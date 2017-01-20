// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* watch_impl.h                                                    -*- C++ -*-
   Jeremy Barnes, 21 May 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Implementation of watch methods.
*/

#pragma once

#include "mldb/watch/watch.h"
#include "mldb/arch/futex.h"
#include <thread>
#include <atomic>
#include <list>
#include "mldb/types/value_description.h"
#include "mldb/types/tuple_description.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/date.h" // TODO: shouldn't need this

namespace MLDB {


/*****************************************************************************/
/* WATCH DATA                                                                */
/*****************************************************************************/

/** Abstract class that implements the data being held for a watch. */
struct WatchData {
    WatchData(Watches * owner,
              const std::type_info * boundType = nullptr,
              Any info = nullptr);

    virtual ~WatchData();

    virtual void setHolder(Watch & newHolder);

    virtual void transferHolder(Watch && oldHolder, Watch & newHolder);

    virtual void releaseHolder(Watch & holder);

    virtual void releaseHolderForDestroy(Watch & holder);

    virtual void bindGeneric(const std::function<void (const Any &) > & fn,
                             WatchErrorHandler errorFn) = 0;

    virtual bool tryBindNonRecursiveGeneric(const std::function<void (const Any &) > & fn,
                                            WatchErrorHandler errorFn) = 0;

    virtual bool bound() const = 0;

    virtual void unbind() = 0;

    /** Trigger from a Any containing a std::tuple of the types
        necessary to make the call.
    */
    virtual void triggerGeneric(const Any & vals) = 0;

    /** Trigger from a Any containing a std::tuple of the types
        necessary to make the call.

        This is used for re-entrant calls, where the object is already
        locked.
    */
    virtual void triggerGenericLocked(const Any & vals) = 0;

    /** Trigger the watch. */
    virtual void error(const WatchError & error) = 0;

    /** Trigger the watch, with the lock already taken. */
    virtual void errorLocked(const WatchError & error) = 0;


    virtual bool any() const = 0;

    virtual int count() const = 0;

    /** Wait synchronously for an event, or throw an exception if none
        found in that time.
    */
    Any waitGeneric(double timeToWait = INFINITY);

    /** Try to wait synchronously for the event.  Returns true in the first
        element and the returned value if successful, or false and a null
        tuple if not successful.
    */
    virtual std::pair<bool, Any>
    tryWaitGeneric(double timeToWait = INFINITY) = 0;

    /** Try to wait synchronously for the event.

        Return values:
        - If there was a timeout, return a null maybe (result.isNull() == true)
        - If an error was received, return it in the maybe error
          (result.isNull() == false, result == false, result.err() has result)
        - If a value was received, return it in the maybe value
          (result.isNull() == false, result == true, result.val() has value)
    */
    virtual MaybeT<Any, WatchError>
    tryWaitMaybeGeneric(double timeToWait = INFINITY) = 0;

    /** Cause the other watch to send its events to this one. */
    virtual void multiplex(Watch && other) = 0;

    /** Return the type we're bound into. */
    const std::type_info * boundType() const
    {
        return boundType_;
    }

    std::shared_ptr<const ValueDescription>
    boundValueDescription() const;

    void waitForInProgressEvents() const;

    Watch * holder;
    Watches * owner;
    const std::type_info * boundType_;

    Any info;  ///< Information set by the watcher
    int triggers;   ///< Number of times triggered
    int errors;     ///< Number of errors

private:
    /// Structure to track ownership of the lock object as sometimes
    /// the WatchData can be destroyed with the lock held, and we need
    /// to be able to clean up.
    struct LockInfo {
        std::recursive_mutex mutex;
    };

    /// Private to force derived classes to use lock()
    std::shared_ptr<LockInfo> lockInfo;

protected:
    /** Object that acts as a guard but also takes ownership of the LockInfo
        so that if the object is destroyed when locked we don't have
        invalid memory accesses when unlocking.
    */
    struct Locker : public std::unique_lock<std::recursive_mutex> {
        Locker(std::shared_ptr<LockInfo> w)
            : std::unique_lock<std::recursive_mutex>(w->mutex),
              w(w)
        {
        }

       Locker(Locker && other)
            : std::unique_lock<std::recursive_mutex>(std::move(other)),
              w(std::move(other.w))
        {
            other.w = nullptr;
        }

        Locker & operator = (Locker && other)
        {
            swap(other);
            return *this;
        }

        void swap(Locker & other)
        {
            std::unique_lock<std::recursive_mutex>::swap(other);
            std::swap(w, other.w);
        }

        void unlock()
        {
            std::unique_lock<std::recursive_mutex>::unlock();
            w = nullptr;
        }

        ~Locker()
        {
            if (!w)
                return;
            unlock();
        }

        std::shared_ptr<LockInfo> w;
    };

    Locker lock() const
    {
        return Locker(lockInfo);
    }
};


/*****************************************************************************/
/* UNKNOWN TYPE WATCH DATA                                                   */
/*****************************************************************************/

/** Watch data used when the type of the watch isn't known until it is
    bound or used.
*/
struct UnknownTypeWatchData: public WatchData {

    UnknownTypeWatchData(Watches * owner,
                         std::function<bool (const Any &)> filterFn,
                         Any info);
    
    virtual void triggerGeneric(const Any & vals);

    virtual void triggerGenericLocked(const Any & vals);

    /** Trigger the watch. */
    virtual void error(const WatchError & error);

    /** Trigger the watch, with the lock already taken. */
    virtual void errorLocked(const WatchError & error);

    virtual bool any() const
    {
        auto l = lock();
        return numSaved;
    }

    virtual int count() const
    {
        auto l = lock();
        return numSaved;
    }

    virtual std::pair<bool, Any>
    tryWaitGeneric(double timeToWait = INFINITY);

    virtual MaybeT<Any, WatchError>
    tryWaitMaybeGeneric(double timeToWait = INFINITY);

    virtual void bindGeneric(const std::function<void (const Any &) > & fn,
                             WatchErrorHandler errorFn);

    virtual bool tryBindNonRecursiveGeneric(const std::function<void (const Any &) > & fn,
                                            WatchErrorHandler errorFn);

    virtual bool bound() const;

    virtual void unbind();

    virtual void multiplex(Watch && other);

    typedef MaybeT<Any, WatchError> Maybe;

    /// Events that have occurred but not been consumed yet
    std::list<Maybe> saved;

    /// Number of saved events.  Used as a futex.
    volatile int numSaved;

    /// Function that gets the triggers
    std::function<void (const Any &)> boundFn;

    /// Function that gets the errors
    WatchErrorHandler errorFn;

    /// Function that does the filtering
    std::function<bool (const Any &)> filterFn;

    /// Multiplexed watches
    std::vector<Watch> multiplexed;
};


/*****************************************************************************/
/* TYPED WATCH DATA                                                          */
/*****************************************************************************/

template<typename... T>
struct WatchDataT: public WatchData {
    WatchDataT(Watches * owner,
               std::function<bool (const T &...)> filterFn,
               Any info);

    /** Bind an asynchronous callback. */
    void bind(const std::function<void (const T &...)> & boundFn,
              WatchErrorHandler errorFn);

    bool tryBindNonRecursive(const std::function<void (const T &...)> & fn,
                             WatchErrorHandler errorFn);

    virtual void bindGeneric(const std::function<void (const Any &) > & fn,
                             WatchErrorHandler errorFn);

    virtual bool tryBindNonRecursiveGeneric(const std::function<void (const Any &) > & fn,
                                            WatchErrorHandler errorFn);

    virtual bool bound() const;

    /** Unbind an asynchronous callback. */
    virtual void unbind();

    virtual void multiplex(Watch && other);

    virtual bool any() const
    {
        auto l = lock();
        return numSaved;
    }

    virtual int count() const
    {
        auto l = lock();
        return numSaved;
    }
    
    /** Type returned from wait.  For a single type, it's just that type.
        For multiple types, it's a tuple.
    */
    typedef typename std::decay<decltype(detuplize(std::declval<std::tuple<T...> >()))>::type WaitReturnType;
    
    /** Wait synchronously for an event, or throw an exception if none
        found in that time.
    */
    WaitReturnType
    wait(double timeToWait = INFINITY);

    WaitReturnType
    pop()
    {
        return wait(0.0);
    }

    std::tuple<T...>
    waitTuple(double timeToWait = INFINITY);

    /** Try to wait synchronously for the event.  Returns true in the first
        element and the returned value if successful, or false and a null
        tuple if not successful.
    */

    std::pair<bool, WaitReturnType>
    tryWait(double timeToWait = INFINITY);

    std::pair<bool, std::tuple<T...> >
    tryWaitTuple(double timeToWait = INFINITY);

    MaybeT<std::tuple<T...>, WatchError>
    tryWaitMaybe(double timeToWait = INFINITY);

    /** Trigger the watch. */
    void trigger(const T &... args);

    /** Trigger the watch. */
    void triggerLocked(const T &... args);

    /** Trigger the watch from a generic value */
    virtual void triggerGeneric(const Any & vals);

    /** Trigger the watch from a generic value */
    virtual void triggerGenericLocked(const Any & vals);

    /** Trigger the watch. */
    virtual void error(const WatchError & error);

    /** Trigger the watch, with the lock already taken. */
    virtual void errorLocked(const WatchError & error);

    /** Try to wait synchronously for the event.  Returns true in the first
        element and the returned value if successful, or false and a null
        tuple if not successful.
    */
    virtual std::pair<bool, Any>
    tryWaitGeneric(double timeToWait = INFINITY);

    virtual MaybeT<Any, WatchError>
    tryWaitMaybeGeneric(double timeToWait = INFINITY);

    typedef MaybeT<std::tuple<T...>, WatchError> Maybe;

    /// Events that have occurred but not been consumed yet
    std::list<Maybe> saved;

    /// Number of saved events.  Used as a futex.
    volatile int numSaved;

    /// Function that does the watching
    std::function<void (const T &...)> boundFn;

    /// Function that gets the errors
    WatchErrorHandler errorFn;

    /// Function that does filtering
    std::function<bool (const T &...)> filterFn;

    /// Set of multiplexed watches
    std::vector<WatchT<T...> > multiplexed;
};


/*****************************************************************************/
/* WATCHT                                                                    */
/*****************************************************************************/

template<typename... T>
WatchT<T...>::
WatchT(WatchDataT<T...> * data)
    : Watch(data)
{
}

template<typename... T>
WatchT<T...> &
WatchT<T...>::
operator = (WatchT && other)
{
    if (other.data)
        other.data->transferHolder(std::move(other), *this);
    else if (this->data) {
        this->data->releaseHolder(*this);
    }
    this->piggyback = std::move(other.piggyback);
    return *this;
}

template<typename... T>
WatchT<T...> &
WatchT<T...>::
operator = (Watch && other)
{
    if (other.boundType()
        && other.boundType() != &typeid(std::tuple<T...>))
        throwException(WATCH_ERR_TYPE,
                       "Attempt to bind watch of type '%s' from watch of type '%s'",
                       MLDB::type_name<std::tuple<T...> >().c_str(),
                       demangle(*other.boundType()).c_str());


    if (other.data)
        other.data->transferHolder(std::move(other), *this);
    else if (this->data) {
        this->data->releaseHolder(*this);
    }
    this->piggyback = std::move(other.piggyback);
    return *this;
}

template<typename... T>
void
WatchT<T...>::
bind(const std::function<void (const T &...)> & fn,
     WatchErrorHandler errorFn)
{
    ExcAssert(data);
    auto d = dataT();
    if (d)
        d->bind(fn, std::move(errorFn));
    else {
        // Generic watch; need to unpack it
        auto bindFn = [=] (const Any & any)
            {
                auto tpl = any.as<std::tuple<T...> >();
                callFromTuple(fn, tpl);
            };
        data->bindGeneric(bindFn, std::move(errorFn));
    }
}

template<typename... T>
bool
WatchT<T...>::
tryBindNonRecursive(const std::function<void (const T &...)> & fn,
                    WatchErrorHandler errorFn)
{
    ExcAssert(data);
    auto d = dataT();
    if (d)
        return d->tryBindNonRecursive(fn, std::move(errorFn));
    else {
        // Generic watch; need to unpack it
        auto bindFn = [=] (const Any & any)
            {
                auto tpl = any.as<std::tuple<T...> >();
                callFromTuple(fn, tpl);
            };
        return data->tryBindNonRecursiveGeneric(bindFn, std::move(errorFn));
    }
}

template<typename... T>
void
WatchT<T...>::
bindNonRecursive(const std::function<void (const T &...)> & fnToBind,
                 const std::function<void (const T &...)> & fnRecursive,
                 WatchErrorHandler errorFnToBind,
                 const WatchErrorHandler & errorFnRecursive)
{
    while (!tryBindNonRecursive(fnToBind, errorFnToBind)) {
        auto val = popMaybe();
        if (val) {
            callFromTuple(fnRecursive, val.val());
        }
        else errorFnRecursive(val.err());
    }
}

/** Unbind an asynchronous callback function from the watch. */
template<typename... T>
void
WatchT<T...>::
unbind()
{
    data->unbind();
}

template<typename... T>
typename WatchT<T...>::WaitReturnType
WatchT<T...>::
wait(double timeToWait)
{
    return detuplize(waitTuple(timeToWait));
}

template<typename... T>
std::tuple<T...>
WatchT<T...>::
waitTuple(double timeToWait)
{
    ExcAssert(data);
    auto d = dataT();
    if (d)
        return d->waitTuple(timeToWait);
        
    auto a = data->waitGeneric(timeToWait);
    if (&a.type() != &typeid(std::tuple<T...>))
        throwException(WATCH_ERR_TYPE,
                       "waiting for %s but got %s",
                       MLDB::type_name<std::tuple<T...> >().c_str(),
                       demangle(a.type().name()).c_str());
        
    return a.as<std::tuple<T...> >();
}

template<typename... T>
std::pair<bool, typename WatchT<T...>::WaitReturnType>
WatchT<T...>::
tryWait(double timeToWait)
{
    auto res = tryWaitTuple(timeToWait);
    return std::make_pair(res.first, detuplize(res.second));
}

template<typename... T>
std::pair<bool, std::tuple<T...> >
WatchT<T...>::
tryWaitTuple(double timeToWait)
{
    auto d = dataT();
    if (d)
        return d->tryWaitTuple(timeToWait);
    else {
        auto res = data->tryWaitGeneric(timeToWait);
        if (!res.first)
            return std::make_pair(false, std::tuple<T...>());
        return std::make_pair(true, res.second.as<std::tuple<T...> >());
    }
}

template<typename... T>
std::pair<bool, std::tuple<T...> >
WatchT<T...>::
doTryWaitTuple(WatchData * data, double timeToWait)
{
    // Try to do it directly
    auto * cast = dynamic_cast<WatchDataT<T...> *>(data);
    //ExcAssertEqual(&typeid(*data), &typeid(WatchDataT<T...>));
    if (cast)
        return cast->tryWaitTuple(timeToWait);

    // Do it generically and cast the result back
    auto res = data->tryWaitGeneric(timeToWait);
    if (!res.first)
        return std::make_pair(false, std::tuple<T...>());
    return std::make_pair(true, res.second.as<std::tuple<T...> >());
}

template<typename... T>
typename WatchT<T...>::Maybe
WatchT<T...>::
waitMaybe(double timeToWait)
{
    Maybe result = tryWaitMaybe(timeToWait);
    if (result.isNull())
        throwException(WATCH_ERR_TIMEOUT, "Watch timed out");
    return result;
}

template<typename... T>
typename WatchT<T...>::Maybe
WatchT<T...>::
tryWaitMaybe(double timeToWait)
{
    // Try to do it directly
    auto * cast = dynamic_cast<WatchDataT<T...> *>(data);
    if (cast)
        return cast->tryWaitMaybe(timeToWait);

    // Do it generically and cast the result back
    auto res = data->tryWaitMaybeGeneric(timeToWait);
    if (!res)
        return res.err();

    // Convert the returned any to the right type
    return res.val().as<std::tuple<T...> >();
}
    
template<typename... T>
void
WatchT<T...>::
trigger(T... args)
{
    auto d = dataT();
    if (d)
        d->trigger(std::forward<T>(args)...);
    else data->triggerGeneric(std::tuple<T...>(args...));
}

template<typename... T>
WatchDataT<T...> *
WatchT<T...>::
dataT()
{
    ExcAssert(data);
    return dynamic_cast<WatchDataT<T...> *>(this->data);
}

template<typename... T>
const WatchDataT<T...> *
WatchT<T...>::
dataT() const
{
    ExcAssert(data);
    return dynamic_cast<const WatchDataT<T...> *>(this->data);
}


/*****************************************************************************/
/* WATCHEST                                                                  */
/*****************************************************************************/

template<typename... T>
WatchesT<T...>::
WatchesT()
    : Watches(&typeid(std::tuple<T...>),
              maybeGetDefaultDescriptionShared<std::tuple<T...> >())
{
}

template<typename... T>
WatchT<T...>
WatchesT<T...>::
add(Any info,
    std::function<bool (const T &...)> filterFn)
{
    std::unique_lock<std::mutex> guard(mutex);
    watches.emplace_back(new WatchDataT<T...>(this, filterFn, std::move(info)));
    return WatchT<T...>(watches.back().get());
}

template<typename... T>
void
WatchesT<T...>::
trigger(const T &... args)
{
    std::unique_lock<std::mutex> guard(mutex);
    triggerThread = std::this_thread::get_id();

    ++triggers;
    for (auto & w: watches) {
        static_cast<WatchDataT<T...> *>(w.get())
            ->trigger(args...);
    }

    triggerThread = std::thread::id();
}


/*****************************************************************************/
/* WATCH DATA T                                                              */
/*****************************************************************************/

template<typename... T>
WatchDataT<T...>::
WatchDataT(Watches * owner,
           std::function<bool (const T &...)> filterFn,
           Any info)
    : WatchData(owner, &typeid(std::tuple<T...>), std::move(info)),
      numSaved(0),
      filterFn(std::move(filterFn))
{
}

template<typename... T>
void
WatchDataT<T...>::
bind(const std::function<void (const T &...)> & boundFn,
     WatchErrorHandler errorFn)
{
    // TODO: errorFn...

    auto guard(this->lock());

    ExcAssert(!this->boundFn);

    this->boundFn = boundFn;
    this->errorFn = errorFn;

    for (auto & s: saved) {
        if (s)
            callFromTuple(boundFn, std::move(s.val()));
        else {
            errorFn(s.err());
        }
    }

    saved.clear();
    numSaved = 0;
}

template<typename... T>
bool
WatchDataT<T...>::
tryBindNonRecursive(const std::function<void (const T &...)> & boundFn,
                    WatchErrorHandler errorFn)
{
    auto guard(this->lock());
    
    ExcAssert(!this->boundFn);
    if (!saved.empty())
        return false;
    
    this->boundFn = boundFn;
    this->errorFn = errorFn;

    for (auto & s: saved) {
        if (s)
            callFromTuple(boundFn, std::move(s.val()));
        else {
            errorFn(s.err());
        }
    }

    saved.clear();
    numSaved = 0;

    return true;
}

template<typename... T>
void
WatchDataT<T...>::
bindGeneric(const std::function<void (const Any &) > & fn,
            WatchErrorHandler errorFn)
{
    auto realBoundFn = [=] (const T &... args)
        {
            fn(Any(std::tuple<T...>(args...)));
        };

    bind(realBoundFn, std::move(errorFn));
}

template<typename... T>
bool
WatchDataT<T...>::
tryBindNonRecursiveGeneric(const std::function<void (const Any &) > & fn,
                           WatchErrorHandler errorFn)
{
    auto realBoundFn = [=] (const T &... args)
        {
            fn(Any(std::tuple<T...>(args...)));
        };

    return tryBindNonRecursive(realBoundFn, std::move(errorFn));
}

template<typename... T>
bool
WatchDataT<T...>::
bound() const
{
    auto guard(this->lock());
    return this->boundFn.operator bool();
}

/** Unbind an asynchronous callback. */
template<typename... T>
void
WatchDataT<T...>::
unbind()
{
    auto guard(this->lock());
    this->boundFn = nullptr;
    this->errorFn = nullptr;
}

template<typename... T>
void
WatchDataT<T...>::
multiplex(Watch && other)
{
    auto guard(this->lock());
    multiplexed.emplace_back(std::move(other));

    // Bind it in so that we get the events
    auto recursiveTrigger = std::make_shared<bool>(true);
        
    auto onEvent = [=] (const T &... args)
        {
            if (*recursiveTrigger)
                this->triggerLocked(args...);
            else this->trigger(args...);
        };

    auto errorFn = [=] (const WatchError & error)
        {
            if (*recursiveTrigger)
                this->errorLocked(error);
            else this->error(error);
        };
    
    multiplexed.back().bind(onEvent, errorFn);

    *recursiveTrigger = false;
}

template<typename... T>
typename WatchDataT<T...>::WaitReturnType
WatchDataT<T...>::
wait(double timeToWait)
{
    return detuplize(waitTuple(timeToWait));
}

template<typename... T>
std::tuple<T...>
WatchDataT<T...>::
waitTuple(double timeToWait)
{
    bool found;
    std::tuple<T...> res;
        
    std::tie(found, res) = tryWaitTuple(timeToWait);
        
    if (!found)
        owner->throwException(WATCH_ERR_TIMEOUT,
                              "No event found before timeout");

    return std::move(res);
}

template<typename... T>
std::pair<bool, typename WatchDataT<T...>::WaitReturnType>
WatchDataT<T...>::
tryWait(double timeToWait)
{
    auto res = tryWaitTuple(timeToWait);
    return std::make_pair(res.first, detuplize(res.second));
}

template<typename... T>
std::pair<bool, std::tuple<T...> >
WatchDataT<T...>::
tryWaitTuple(double timeToWait)
{
    auto val = tryWaitMaybe(timeToWait);
    if (val.isNull())
        return std::make_pair(false, std::tuple<T...>());
    else if (!val)
        throw WatchException(std::move(val.err()));
    else return std::make_pair(true, std::move(val.val()));
}

template<typename... T>
MaybeT<std::tuple<T...>, WatchError>
WatchDataT<T...>::
tryWaitMaybe(double timeToWait)
{
    if (boundFn)
        owner->throwException(WATCH_ERR_WAIT_BOUND, 
                              "cannot wait on a bound watch");
    
    Date limit = Date::now().plusSeconds(timeToWait);
        
    for (;;) {
        double secondsToWait
            = std::max(0.0, Date::now().secondsUntil(limit));

        if (secondsToWait > 0) {
            ML::futex_wait(numSaved, 0, secondsToWait);
            if (!numSaved)
                continue;
        }
        else {
            if (!numSaved)
                return MaybeT<std::tuple<T...>, WatchError>();
        }

        auto guard(this->lock());
            
        if (numSaved == 0) {
            if (secondsToWait > 0.0)
                continue;
            else
                return MaybeT<std::tuple<T...>, WatchError>();
        }

        ExcAssert(!saved.empty());
            
        auto el = std::move(saved.front());
        saved.pop_front();
        --numSaved;

        return el;
    }
}

template<typename... T>
void
WatchDataT<T...>::
trigger(const T &... args)
{
    auto guard = this->lock();
    triggerLocked(args...);
}

template<typename... T>
void
WatchDataT<T...>::
triggerLocked(const T &... args)
{
    ++this->triggers;

    if (filterFn && !filterFn(args...))
        return;

    // No lock, assume it's already taken
    if (boundFn && saved.empty())
        boundFn(args...);
    else {
        saved.emplace_back(std::tuple<T...>(args...));
        ++numSaved;
        ML::futex_wake(numSaved);
    }
}

template<typename... T>
void
WatchDataT<T...>::
triggerGeneric(const Any & vals)
{
    auto tupl = vals.as<std::tuple<T...> >();
    callFromTuple(std::mem_fn(&WatchDataT::trigger), tupl, this);
}

template<typename... T>
void
WatchDataT<T...>::
triggerGenericLocked(const Any & vals)
{
    auto tupl = vals.as<std::tuple<T...> >();
    callFromTuple(std::mem_fn(&WatchDataT::triggerLocked), tupl, this);
}

template<typename... T>
void
WatchDataT<T...>::
error(const WatchError & error)
{
    auto guard = this->lock();
    errorLocked(error);
}

template<typename... T>
void
WatchDataT<T...>::
errorLocked(const WatchError & error)
{
    ++this->errors;

    if (errorFn) {
        ExcAssert(saved.empty());
        errorFn(error);
    }
    else {
        saved.emplace_back(error);
        ++numSaved;
        ML::futex_wake(numSaved);
    }
}

template<typename... T>
std::pair<bool, Any>
WatchDataT<T...>::
tryWaitGeneric(double timeToWait)
{
    std::pair<bool, std::tuple<T...> > res = tryWaitTuple(timeToWait);
    if (!res.first)
        return std::make_pair(false, Any());
    return std::make_pair(true, Any(res.second));
}

template<typename... T>
MaybeT<Any, WatchError>
WatchDataT<T...>::
tryWaitMaybeGeneric(double timeToWait)
{
    MaybeT<std::tuple<T...>, WatchError> res
        = this->tryWaitMaybe(timeToWait);
    if (res.isNull())
        return MaybeT<Any, WatchError>();  // timeout
    else if (!res)
        return MaybeT<Any, WatchError>(std::move(res.err()));  // error
    else return MaybeT<Any, WatchError>(std::move(res.val()));
}


} // namespace MLDB
