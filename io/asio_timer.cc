// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* asio_timer.cc                                                   -*- C++ -*-
   Jeremy Barnes, 20 June 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include <boost/asio.hpp>
#include "asio_timer.h"
#include "mldb/watch/watch_impl.h"
#include "mldb/io/event_loop_impl.h"
#include <thread>
#include "mldb/arch/backtrace.h"
#include "mldb/types/basic_value_descriptions.h"

using namespace std;


namespace MLDB {


void
StrandHolder::
init(void * strand, const std::type_info * type)
{
    if (type != &typeid(boost::asio::strand))
        throw MLDB::Exception("StrandHolder initialized from " + demangle(type->name()) + " not boost::asio::strand");
    this->strand = strand;
}

namespace {
static constexpr int NO_HANDLER = 0;
static constexpr int HANDLER_QUEUED = 1;
static constexpr int IN_HANDLER = 2;
} // file scope


/*****************************************************************************/
/* ASIO TIMER                                                                */
/*****************************************************************************/

struct AsioTimer::Impl {    
    /** The state is behind a shared pointer so that the object can be
        destroyed from within its handler but its internal state can live
        until the call chain is unwound.
    */
    struct State {
        State(boost::asio::io_service & ioService)
            : timer(ioService), strand(nullptr),
              firingThread(std::thread::id()),
              shutdown(false),
              handlerStatus(NO_HANDLER), firings(0),
              pin(new int(0))
        {
        }

        State(boost::asio::strand & strand)
            :  timer(strand.get_io_service()), strand(&strand),
               firingThread(std::thread::id()),
               shutdown(false),
               handlerStatus(NO_HANDLER), firings(0),
               pin(new int(0))
        {
        }

        // According to the ASIO timer documentation, we can't call methods
        // on the timer from multiple threads.  This mutex protects it against
        // such calls.  It needs to be recursive as ASIO will run the callback
        // directly in some circumstances.
        std::recursive_mutex timerMutex;
        boost::asio::basic_waitable_timer<std::chrono::system_clock> timer;
        boost::asio::strand * strand;
        WatchesT<Date> watches;
        Date nextExpiry;
        double period;
        std::thread::id firingThread;
        std::atomic<bool> shutdown;
        std::atomic<int> handlerStatus;
        std::atomic<int> firings;
        std::shared_ptr<int> pin;
    };

    const std::shared_ptr<State> state;

    Impl(boost::asio::io_service & ioService)
        : state(new State(ioService))
    {
    }

    Impl(boost::asio::strand & strand)
        : state(new State(strand))
    {
    }

    ~Impl()
    {
        //cerr << "timer impl destroy" << endl;
        state->shutdown = true;
        disarm();
        //cerr << "timer impl destroy done" << endl;
    }

    void arm(Date nextExpiry, double period)
    {
        ExcAssertEqual(state->handlerStatus, NO_HANDLER);

        armImpl(nextExpiry, period, state);
    }

    static void armImpl(Date nextExpiry, double period,
                        std::shared_ptr<State> state)
    {
        std::unique_lock<std::recursive_mutex> guard(state->timerMutex);

        if (state->shutdown) {
            state->handlerStatus = NO_HANDLER;
            return;
        }

        state->nextExpiry = nextExpiry;
        state->period = period;

        size_t numCancelled = state->timer.expires_at(nextExpiry.toStd());
        ExcAssertEqual(numCancelled, 0);
    
        auto bound = std::bind(&Impl::onFire, std::placeholders::_1,
                               nextExpiry, period, state,
                               state->pin);
    
        state->handlerStatus = HANDLER_QUEUED;

        try {
            if (state->strand)
                state->timer.async_wait(state->strand->wrap(bound));
            else state->timer.async_wait(bound);
        } catch (...) {
            state->handlerStatus = NO_HANDLER;
            throw;
        }
    }

    /** There are three cases of disarm we need to deal with:
        1.  Normal destruction of the timer, synchronously with no
            possibility of it firing during destruction.
        2.  Recursive destruction of the timer from within its onFired
            handler
        3.  Asynchronous cancellation or destruction of the timer
            that is unsynchronized with the timer.  In this case we
            have to make sure that there are no race conditions with
            the timer firing.
    */
    void disarm()
    {
        std::unique_lock<std::recursive_mutex> guard(state->timerMutex);

        int numCancelled = state->timer.cancel();
        if (numCancelled)
            state->handlerStatus = NO_HANDLER;

        if (state->shutdown) {
            // If we're shutting down, we need to return straight away.
            // If we have another timer in our call chain with the same
            // strand, our handler will never trigger as the strand will
            // be locked.  We need to exit it to allow the handler to
            // be called; it will discover that we're shutting down and
            // therefore do nothing.
            //
            // Note that the case of calling disarm() instead of shutting
            // down is not well handled; for this reason disarm() is a
            // private method.
            return; 
        }

        bool doneBacktrace = false;

        // Spin until handler is out
        for (unsigned i = 0;  state->handlerStatus != NO_HANDLER;  ++i) {
            if (state->firingThread == std::this_thread::get_id()
                && state->handlerStatus == IN_HANDLER)
                break;
            std::this_thread::yield();
            if (i && i % 1000000 == 0) {
                cerr << "stuck in timer disarm; status "
                     << state->handlerStatus << " numCancelled " << numCancelled
                     << " shutdown " << state->shutdown
                     << " nextExpiry " << state->nextExpiry
                     << " period " << state->period
                     << " in " << Date::now().secondsUntil(state->nextExpiry)
                     << " firings " << state->firings
                     << " thread " << std::hash<std::thread::id>()(state->firingThread)
                     << " us " << std::hash<std::thread::id>()(std::this_thread::get_id())
                     << " none " << std::hash<std::thread::id>()(std::thread::id())
                     << " use count " << state.use_count()
                     << " pin " << state->pin.use_count()
                     << " stopped " << state->timer.get_io_service().stopped()
                     << " strand " << state->strand
                     << endl;
                if (!doneBacktrace) {
                    backtrace();
                    doneBacktrace = true;
                }
            }
        }
    }

    static void onFire(const boost::system::error_code & error,
                       Date nextExpiry, double period,
                       std::shared_ptr<State> state,
                       std::shared_ptr<int> pin)
    {
        ++state->firings;
        if (error == boost::system::errc::operation_canceled
            || error == boost::asio::error::operation_aborted) {
            state->handlerStatus = NO_HANDLER;
            return;
        }

        if (state->shutdown) {
            state->handlerStatus = NO_HANDLER;
            return;
        }

        if (error) {
            // NOTE: this should return an error in a more sensible manner,
            // ie through the watch
            throw boost::system::system_error(error, "AsioTimer onFire");
        }
        
        state->handlerStatus = IN_HANDLER;
        state->firingThread = std::this_thread::get_id();

        // WARNING: the trigger operation could lead to this object being
        // destroyed.  In that case we need to immediately exit (shutdown
        // will be true in that case).  Note that the state will still be
        // alive in this case as we receive it via a shared pointer, not
        // via a this pointer.
        state->watches.trigger(nextExpiry);

        state->firingThread = std::thread::id();

        if (period <= 0 || state->shutdown) {
            state->handlerStatus = NO_HANDLER;
            return;
        }
        
        nextExpiry.addSeconds(period);

        armImpl(nextExpiry, period, state);
    }
};

AsioTimer::
AsioTimer(Date nextExpiry, double period,
          boost::asio::io_service & ioService)
    : impl(new Impl(ioService))
{
    arm(nextExpiry, period);
}

AsioTimer::
AsioTimer(Date nextExpiry, double period,
          const StrandHolder & strand)
    : impl(new Impl(*reinterpret_cast<boost::asio::strand *>(strand.strand)))
{
    arm(nextExpiry, period);
}

AsioTimer::
AsioTimer(boost::asio::io_service & ioService)
    : impl(new Impl(ioService))
{
}

AsioTimer::
AsioTimer(const StrandHolder & strand)
    : impl(new Impl(*reinterpret_cast<boost::asio::strand *>(strand.strand)))
{
}

AsioTimer::
~AsioTimer()
{
}

void
AsioTimer::
arm(Date nextExpiry, double period)
{
    impl->arm(nextExpiry, period);
}

void
AsioTimer::
disarm()
{
    impl->disarm();
}

WatchT<Date>
AsioTimer::
watch()
{
    return impl->state->watches.add();
}

WatchT<Date> getTimer(Date nextExpiry,
                      double period,
                      EventLoop & eventLoop,
                      std::function<void (Date)> toBind)
{
    return getTimer(nextExpiry, period, eventLoop.impl().ioService(), toBind);
}

WatchT<Date> getTimer(Date nextExpiry,
                      double period,
                      boost::asio::io_service & ioService,
                      std::function<void (Date)> toBind)
{
    auto timer = std::make_shared<AsioTimer>(ioService);
    auto watch = timer->watch();
    if (toBind)
        watch.bind(std::move(toBind));
    watch.piggyback = timer;
    timer->arm(nextExpiry, period);
    return watch;
}

WatchT<Date> getTimer(Date nextExpiry,
                      double period,
                      const StrandHolder & strand,
                      std::function<void (Date)> toBind)
{
    auto timer = std::make_shared<AsioTimer>(strand);
    auto watch = timer->watch();
    if (toBind)
        watch.bind(std::move(toBind));
    watch.piggyback = timer;
    timer->arm(nextExpiry, period);
    return watch;
}

template class WatchT<Date>;
template class WatchesT<Date>;

} // namespace MLDB

