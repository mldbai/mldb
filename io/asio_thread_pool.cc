// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* asio_thread_pool.h                                              -*- C++ -*-
   Jeremy Barnes, 20 June 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Thread pool for ASIO workers.
*/

#include "asio_thread_pool.h"
#include "asio_timer.h"
#include <boost/asio.hpp>
#include "mldb/watch/watch_impl.h"
#include "mldb/types/structure_description.h"
#include "mldb/io/event_loop_impl.h"
#include <thread>


using namespace std;


namespace MLDB {


/*****************************************************************************/
/* ASIO THREAD POOL                                                          */
/*****************************************************************************/

struct AsioThreadPool::Impl {
    Impl(boost::asio::io_context & ioContext,
         double probeIntervalSeconds = 0.1)
        : ioContext(ioContext),
          work(boost::asio::make_work_guard(ioContext)),  
          shutdown(false)
    {
        threads.emplace_back([=,this] () { this->run(0); });

        lastProbe = Date::now();
        lastLatency = 0;

        timer = getTimer(Date::now().plusSeconds(probeIntervalSeconds),
                         probeIntervalSeconds,
                         ioContext,
                         std::bind(&Impl::onProbe,
                                   this,
                                   std::placeholders::_1));
        numIdles = 0;
    }

    ~Impl()
    {
        shutdown = true;
        ioContext.stop();
        work.reset();
        for (auto & t: threads) {
            boost::asio::post(ioContext, [] () {});
            t.join();
        }
        ioContext.stop();
    }

    void ensureThreads(int minNumThreads)
    {
        std::unique_lock<std::mutex> guard(threadsLock);
        for (size_t i = threads.size(); i < minNumThreads; i++) {
            threads.emplace_back([=,this] () { this->run(i); });
        }
    }
    
    void run(int threadNum)
    {
        //cerr << "starting thread " << threadNum << endl;
        Date after = Date::now();

        Date lastCheck = after;
        int consecutiveIdles = 0;

        for (;;) {
            if (shutdown)
                return;

            if (true) {
                int res = ioContext.run();
                if (res == 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    //cerr << "ioContext has stopped" << endl;
                }
                continue;
            }
            
            Date before = Date::now();

            double timeSpentBefore = before.secondsSince(after);
            nanosSleeping += timeSpentBefore * 1000000000;

            numHandlers += 1;
            size_t numDone = (threadNum == 0 ? ioContext.run_one() : ioContext.run());
            ++numWakeups;
            numHandlers -= 1;

            after = Date::now();
            
            double timeSpent = after.secondsSince(before);
            nanosProcessing += timeSpent * 1000000000;

            if (numDone == 0) {
                ++numIdles;
                ++consecutiveIdles;
                if (consecutiveIdles < 10)
                    std::this_thread::yield();
                else {
                    // TODO: smarter sleeping than this, or allow thread 0 to
                    // wake up others when it has work to do
                    if (threadNum != 0)
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
                // TODO: sleep for maximum latency
            }
            else {
                numEvents += numDone;
                consecutiveIdles = 0;
            }

            if (threadNum == 0 && after > lastCheck.plusSeconds(0.1)) {
                Date now = Date::now();
                int64_t sleeping = nanosSleeping, processing = nanosProcessing;
                nanosSleeping = 0;
                nanosProcessing = 0;
                int wakeups = numWakeups, idles = numIdles, events = numEvents;
                numWakeups = 0;
                numIdles = 0;
                numEvents = 0;
                
                double duty = 1.0 * processing / (processing + sleeping);
                double threadFactor = processing / (lastCheck.secondsUntil(now)) / 1000000000.0;
                
                if (false) {
                    cerr << "sleeping " << sleeping << " processing " << processing
                         << " duty " << duty * 100.0 << "%" << endl;
                    cerr << "threads required " << threadFactor << endl;
                    cerr << "idles = " << idles << " wakeups = " << wakeups << " events "
                         << events << endl;
                    cerr << "threads = " << threads.size() << endl;
                }

                Stats stats;
                stats.duty = duty;
                stats.numThreadsRequired = threadFactor;
                stats.numThreadsRunning = threads.size();
                stats.latency = lastLatency;
                
                statsWatches.trigger(stats);
                
                lastCheck = now;

                if (idles == 0) {
                    int threadNum = threads.size();
                    cerr << "starting up thread " << threadNum << endl;
                    std::unique_lock<std::mutex> guard(threadsLock);
                    threads.emplace_back([=,this] () { this->run(threadNum); });
                }
                else if (threadFactor + 0.2 < threads.size()) {
                    // Threads are over-provisioned.  Need to signal the thread
                    // to exit and then join it.
                }
            }
        }
    }

    void onProbe(Date date)
    {
        Date now = Date::now();
        double latency = now.secondsSince(date);
        lastLatency = latency;
        //cerr << "latency = " << latency * 1000.0 << "ms" << endl;
    }

    boost::asio::io_context & ioContext;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work;

    std::atomic<int64_t> nanosSleeping;
    std::atomic<int64_t> nanosProcessing;
    std::atomic<int> numEvents;
    std::atomic<int> numWakeups;
    std::atomic<int> numIdles;
    std::atomic<int> numHandlers;
    //std::atomic<double> lastLatency;
    double lastLatency;

    std::mutex threadsLock;
    std::vector<std::thread> threads;
    WatchT<Date> timer;
    WatchesT<Stats> statsWatches;
    Date lastProbe;
    std::atomic<bool> shutdown;
};

AsioThreadPool::
AsioThreadPool(EventLoop & eventLoop, double probeIntervalSeconds)
    : impl(new Impl(eventLoop.impl().ioContext(), probeIntervalSeconds))
{
}

AsioThreadPool::
~AsioThreadPool()
{
}

void
AsioThreadPool::
shutdown()
{
    impl.reset();
}

void
AsioThreadPool::
ensureThreads(int numThreads)
{
    impl->ensureThreads(numThreads);
}

WatchT<AsioThreadPool::Stats>
AsioThreadPool::
watchStats()
{
    return impl->statsWatches.add();
}

DEFINE_STRUCTURE_DESCRIPTION_NAMED(AsioThreadPoolStatsDescription, AsioThreadPool::Stats);

AsioThreadPoolStatsDescription::
AsioThreadPoolStatsDescription()
{
    /*
    struct Stats {
        double duty;
        int numThreadsRunning;
        double numThreadsRequired;
        double latency;
    };
    */
}


} // namespace MLDB
