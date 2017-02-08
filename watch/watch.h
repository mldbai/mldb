/* watch.h                                                       -*- C++ -*-
   Jeremy Barnes, 6 April 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Class to deal with watchers.
*/

#pragma once

#include "mldb/types/value_description_fwd.h"
//#include "mldb/types/tuple_description.h"
#include <math.h>
#include <mutex>
#include <thread>
#include <vector>
#include <map>
#include <iostream>
#include "tuple_utils.h"
#include "mldb/types/any.h"
#include "mldb/arch/demangle.h"
#include "mldb/compiler/compiler.h"
#include "mldb/base/exc_assert.h"
#include "maybe.h"

namespace MLDB {


struct ValueDescription;
struct WatchData;
template<typename... T> struct WatchDataT;
template<typename... T> struct WatchT;


/*****************************************************************************/
/* WATCH ERROR                                                               */
/*****************************************************************************/

enum WatchErrorKind {
    WATCH_OK = 0,           ///< No error
    WATCH_DISCONNECTED,     ///< Watch was disconnected
    WATCH_ERR_CREATE,       ///< Couldn't create the watch
    WATCH_ERR_TYPE,
    WATCH_ERR_TIMEOUT,
    WATCH_ERR_DOUBLE_TRIGGER,
    WATCH_ERR_WAIT_BOUND,
    WATCH_ERR_USER = 1024   ///< User error codes start here
};

DECLARE_ENUM_DESCRIPTION(WatchErrorKind);

/** Describes an error returned by a watch.  All watches have the same
    error class to avoid having two type parameters.
*/

struct WatchError {
    WatchError()
        : kind(WATCH_OK)
    {
    }
    
    WatchError(WatchErrorKind kind, const std::string & message)
        : kind(kind), message(message)
    {
    }

    WatchErrorKind kind;
    std::string message;
    Any sender;
    std::map<std::string, Any> details;
};

DECLARE_STRUCTURE_DESCRIPTION(WatchError);

typedef std::function<void (const WatchError & error) > WatchErrorHandler;

/** Watch error handler that will crash the program (abort()) if the
    error is anything but WATCH_DISCONNECTED.  This is the default
    for when no error handler is passed; it essentially means "there should
    never be an error, and if there is my program shouldn't continue".

    This is safe to use on a watch you know may disconnect, but where you
    don't care if it does so.  It allows you to ignore all error handling
    logic, as essentially you are asserting that there will be no errors.
    It is safe because it will check this assertion for you and not swallow
    errors silently.
*/
extern const WatchErrorHandler CRASH_IF_NOT_DISCONNECTED;

/** Watch error handler that crashes the program on any error, including a
    disconnect.  This is safe to use when you know a watch should never have
    an error or disconnect; in other words where you're ignoring error
    handling and you want that to be safe.
*/
extern const WatchErrorHandler CRASH_THE_PROGRAM;

/** Watch error handler that ignores all errors in a watch.  Used when you
    know there could be errors but you don't want to do anything with them;
    if instead you just haven't written handlers you should use one of the
    others.
*/
extern const WatchErrorHandler IGNORE_ERRORS;


/*****************************************************************************/
/* WATCH EXCEPTION                                                           */
/*****************************************************************************/

/** Exception thrown by watches. */
struct WatchException: public std::exception, public WatchError {
    ~WatchException() throw();
    WatchException(WatchError && err);
    const char * what() const throw();
    WatchError err;
    std::string what_;
};


/*****************************************************************************/
/* WATCH                                                                     */
/*****************************************************************************/

struct Watch {

    Watch(WatchData * data = nullptr);

    Watch(Watch && other)
        : data(nullptr)
    {
        *this = std::move(other);
    }

    Watch & operator = (Watch && other);

    /** Destructor.  This will implicitly detach the watch from the event
        source.
    */
    ~Watch();

    /** Returns true iff the watch is currently attached to an event
        source.
    */
    bool attached() const
    {
        return data;
    }

    /** Detach the watch object from its source of events, without
        destroying the object itself.
    */
    void detach();

    /** Bind a generic function to the watch.  Throws if*/
    void bindGeneric(const std::function<void (const Any &) > & fn,
                     WatchErrorHandler errorFn = CRASH_IF_NOT_DISCONNECTED);

    /** Attempt to bind; return false on failure. */
    bool trybindGeneric(const std::function<void (const Any &) > & fn,
                        WatchErrorHandler errorFn = CRASH_IF_NOT_DISCONNECTED);

    /** Returns true iff there is currently a function bound to the
        watch.
    */
    bool bound() const;

    /** Unbind the currently bound function from the watch.  Any events
        will accumulate in the watch.
    */
    void unbind();

    /** Return if there are any outstanding events currently available. */
    bool any() const;

    /** Return the number of outstanding events currently available. */
    int count() const;

    /** Return the info object with which this was added to the watches. */
    Any info() const;

    /** Wait synchronously for an event, or throw an exception if none
        found in that time.

        Returns a tuple if multiple types, or unpacks it if a single type.

        Will throw an exception if the types passed don't match the
        types with which the watch was triggered.
    */
    template<typename... T>
    decltype(detuplize(std::declval<std::tuple<T...> >()))
    wait(double timeToWait = INFINITY)
    {
        return detuplize(waitTuple<T...>(timeToWait));
    }

    /** Wait synchronously for an event, or throw an exception if none 
        found in that time.

        Returns an Any object, which will bound to a tuple containing the
        types that the watch was triggered with.
    */
    Any waitGeneric(double timeToWait = INFINITY)
    {
        bool found;
        Any res;

        std::tie(found, res) = tryWaitGeneric(timeToWait);

        if (!found)
            throwException(WATCH_ERR_TIMEOUT, "No event found before timeout");

        return res;
    }

    /** Pop an event, or throw an exception if the event was not found.
        Typed version.
    */
    template<typename... T>
    decltype(detuplize(std::declval<std::tuple<T...> >()))
    pop()
    {
        return wait<T...>(0.0);
    }

    /** Pop an event, or throw an exception if the event was not found.
        Generic version.
    */
    Any popGeneric()
    {
        return waitGeneric(0.0);
    }

    /** Wait synchronously for an event for the given amount of time.
 
        Returns a tuple (even if there is only one type passed).
    */
    template<typename... T>
    std::tuple<T...>
    waitTuple(double timeToWait = INFINITY)
    {
        bool found;
        std::tuple<T...> res;
        
        std::tie(found, res) = tryWaitTuple<T...>(timeToWait);

        if (!found)
            throwException(WATCH_ERR_TIMEOUT, "No event found before timeout");

        return std::move(res);
    }

    /** Try to wait synchronously for the event.  Returns true in the first
        element and the returned value if successful, or false and a null
        tuple if not successful.
    */
    template<typename... T>
    std::pair<bool, decltype(detuplize(std::declval<std::tuple<T...> >()))>
    tryWait(double timeToWait = INFINITY)
    {
        auto res = tryWaitTuple(timeToWait);
        return std::make_pair(res.first, detuplize(res.second));
    }
    
    template<typename... T>
    std::pair<bool, std::tuple<T...> >
    tryWaitTuple(double timeToWait = INFINITY);

    std::pair<bool, Any>
    tryWaitGeneric(double timeToWait = INFINITY);


    /** Multiplex the other watch so that this one will return its events
        too.  Takes ownership of and destroys the other watch.
    */
    void multiplex(Watch && w);

    /** The type that is bound into the watch. */
    const std::type_info * boundType() const;

    /** The value description for the bound type, if available when the
        watch was created.
    */
    std::shared_ptr<const ValueDescription>
    boundValueDescription() const;

    /** Wait for any in-progress events on the watch to finish.  Once this
        is done, it is guaranteed that all bound events are done.
    */
    void waitForInProgressEvents() const;

    /** Return the number of times that this watch has been triggered. */
    int getTriggerCount() const;

    /** Return the number of errors that this watch has had. */
    int getErrorCount() const;

    int getNumQueuedEvents() const;

    // Data piggybacking along for the ride with the watch
    std::shared_ptr<void> piggyback;

    // Throw the given exception with the given message
    void throwException(WatchErrorKind kind,
                        const char * message, ...) const MLDB_NORETURN;

protected:
    void release(WatchData * data);

    friend class WatchData;
    friend class Watches;
    template<typename... T> friend class WatchDataT;
    template<typename... T> friend class WatchT;
    WatchData * data;
};


/*****************************************************************************/
/* WATCHES                                                                   */
/*****************************************************************************/

struct Watches {
    
    Watches(const std::type_info * boundType = nullptr,
            std::shared_ptr<const ValueDescription> valueDescription = nullptr);

    ~Watches();

    /** Release a watch. */
    void release(Watch & watch);

    /** Release all watches */
    void clear();

    Watch add(Any info = nullptr,
              std::function<bool (const Any &)> filterFn = nullptr);

    template<typename... T>
    void trigger(const T &... vals)
    {
        std::tuple<T...> tupl(vals...);

        // Check for compatible types if we have a bound type
        if (boundType()
            && &typeid(tupl) != boundType()) {
            throwException(WATCH_ERR_TYPE,
                           "attempt to trigger watch bound to type '%s' "
                           "with parameters of type '%s'",
                           demangle(*boundType()).c_str(),
                           MLDB::type_name<std::tuple<T...> >().c_str());
        }

        return this->triggerGeneric(Any(std::move(tupl)));
    }

    void triggerGeneric(const Any & val);

    /** Tell the watchers that there is an error. */
    void error(const WatchError & error);

    /** Return true if and only if there is at least one attached watch. */
    bool empty() const
    {
        std::unique_lock<std::mutex> guard(mutex);
        return watches.empty();
    }

    /** Return the number of attached watches. */
    size_t size() const
    {
        std::unique_lock<std::mutex> guard(mutex);
        return watches.size();
    }

    /** Return a list of all current watches and their info.  This allows
        a watched object to introspect who is watching it.
    */
    std::vector<std::pair<Watch *, Any> > active() const;
    
    /** Return the type that a triggered watch will return. */
    const std::type_info * boundType() const { return boundType_; }

    /** Return the value description for the type that a triggered
        watch will return.
    */
    std::shared_ptr<const ValueDescription> boundValueDescription() const
    {
        return boundValueDescription_;
    }

    /** Bound a type and value description in to the watch.  This will only
        succeed if none were already bound, which in practice means that
        Watches was created as a naked object, not via a WatchesT
        subtype.

        If boundType is nullptr, then the type carried by boundDesc will
        be used instead.
    */
    void bindType(std::shared_ptr<const ValueDescription> boundDesc,
                  const std::type_info * boundType = nullptr);
    
    
    /** Function called when a watch is released from the other end.  Used to
        give the owner visibility into state changes and to allow it to
        clean up afterwards.

        The watch itself, and its watch info, are passed in.
    */
    std::function<void (Watch &, Any &&)> onRelease;

    /** Return the number of times that these watches have been triggered
        since the object was created.
    */
    int getTriggerCount() const
    {
        return triggers;
    }

    /** Return the number of errors that have been triggered on these watches
        since they have been created.
    */
    int getErrorCount() const
    {
        return errors;
    }

    /** Return an error handler that will when called pass the error through
        to these watches.
    */
    WatchErrorHandler passError() const;

    // Throw the given exception with the given message
    void throwException(WatchErrorKind kind,
                        const char * message, ...) const MLDB_NORETURN;

protected:
    /// Mutex.  We handle recursion in via triggerThread, so this is a straight
    /// mutex.
    mutable std::mutex mutex;

    /// The set of watches we're dealing with, along with their info
    std::vector<std::unique_ptr<WatchData> > watches;

    /// This is the concrete type that is bound into the watches
    const std::type_info * boundType_;

    /// Value description for the bound type
    std::shared_ptr<const ValueDescription> boundValueDescription_;

    /// If true, we are shutting down.  We ignore requests to release
    /// watch data.
    bool shuttingDown;

    /// Number of times it's triggered
    int triggers;

    /// Number of times it's had an error
    int errors;

    friend class WatchData;
    template<typename... T>  friend class WatchDataT;

    /// Internal logic to release the watch with the given watch data
    void releaseWatchWithData(WatchData * data);

    /// Thread which is currently triggering, to allow for watches to be
    /// removed from a trigger without deadlock
    std::thread::id triggerThread;

};


/*****************************************************************************/
/* TYPED WATCH                                                               */
/*****************************************************************************/

template<typename... T>
struct WatchT: public Watch {
    WatchT(WatchDataT<T...> * data = nullptr);

    WatchT(WatchT && other)
    {
        *this = std::move(other);
    }

    WatchT(Watch && other)
    {
        *this = std::move(other);
    }
  
    WatchT & operator = (WatchT && other);

    WatchT & operator = (Watch && other);

    /** Bind an asynchronous callback function to the watch. */
    void bind(const std::function<void (const T &...)> & fn,
              WatchErrorHandler errorFn = CRASH_IF_NOT_DISCONNECTED);

    /** Bind the function.  If one is already bound, return false instead of
        throwing. */
    bool tryBind(const std::function<void (const T &...)> & fn,
                 WatchErrorHandler errorFn = CRASH_IF_NOT_DISCONNECTED);

    /** Attempt to atomically bind a callback that cannot be called recursively
        to the watch.  In the case of failure (ie, there are stored callbacks
        that would normally trigger) it returns false; those callbacks can be
        dealt with synchronously.

        This is useful where the callback function will deadlock if called
        recursively.

        Example (which is exactly how tryBindNonRecursive is implemented):

        while (!watch.tryBindNonRecursive(cb)) {
            handleRecursively(watch.pop());
        }
    */
    bool tryBindNonRecursive(const std::function<void (const T &...)> & fn,
                             WatchErrorHandler errorFn = CRASH_IF_NOT_DISCONNECTED);

    /** Bind a callback that cannot be called recursively to the watch.  Any
        queued values (that would normally call recursively) will be passed
        to an alternative callback.

        This is useful where the normal callback function will deadlock if called
        recursively.
    */
    void bindNonRecursive(const std::function<void (const T &...)> & fnToBind,
                          const std::function<void (const T &...)> & fnRecursive,
                          WatchErrorHandler errorFnToBind = CRASH_IF_NOT_DISCONNECTED,
                          const WatchErrorHandler & errorFnRecursive = CRASH_IF_NOT_DISCONNECTED);

    /** Unbind an asynchronous callback function from the watch. */
    void unbind();

    /** Type returned from wait.  For a single type, it's just that type.
        For multiple types, it's a tuple.
    */
    typedef typename std::decay<decltype(detuplize(std::declval<std::tuple<T...> >()))>::type WaitReturnType;

    typedef MaybeT<std::tuple<T...>, WatchError> Maybe;

    /** Wait synchronously for an event, or throw an exception if none
        found in that time.
    */
    WaitReturnType
    wait(double timeToWait = INFINITY);

    /** Wait synchonously for an event or an error, or throw an exception
        if none found in that time.
    */
    Maybe waitMaybe(double timeToWait = INFINITY);

    WaitReturnType pop()
    {
        return wait(0.0);
    }

    Maybe popMaybe()
    {
        return waitMaybe(0.0);
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

    /** Wait for a value or an error for the given time.  If nothing was
        found within the time limit, then return a null maybe.
    */
    Maybe tryWaitMaybe(double timeToWait = INFINITY);
    
    /** Trigger the watch.  Normally this will not be done directly, but
        this is useful for things like catching up.
    */
    void trigger(T... args);

private:
    WatchDataT<T...> * dataT();
    const WatchDataT<T...> * dataT() const;
    
    WatchT(const WatchT & other) = delete;
    void operator = (const WatchT & other) = delete;

    friend class Watch;
    
    static std::pair<bool, std::tuple<T...> >
    doTryWaitTuple(WatchData * data, double timeToWait);
};

/** Watches on the entire set of events. */
template<typename... T>
struct WatchesT : public Watches {

    WatchesT();

    /** Add a watch */
    WatchT<T...> add(Any info = nullptr,
                     std::function<bool (const T &...)> filterFn = nullptr);

    /** Trigger the watches, causing all bound handlers to trigger. */
    void trigger(const T &... args);
};


/*****************************************************************************/
/* FREE FUNCTIONS                                                            */
/*****************************************************************************/

/** Transform one type of watch into another, using a free function to
    perform the transformation.
*/
template<typename... OtherT, typename... T, typename Fn>
WatchT<OtherT...>
transform(WatchT<T...> && watch,
          Fn fn)
{
    struct Data {
        WatchesT<OtherT...> watches;
        WatchT<T...> elementWatch;
    };

    auto data = std::make_shared<Data>();

    Data * dataPtr = data.get();

    // Make sure that onRelease gets done properly
    data->watches.onRelease = [=] (Watch & w, Any && info) -> void
        {
            dataPtr->elementWatch.detach();
        };

    // 1.  Create a watch on the elements
    data->elementWatch = std::move(watch);

    // 2.  Fuction that passes it along, transforming through the function
    auto onElement = [=] (const T &... args)
        {
            std::tuple<OtherT...> transformed
                = tuplize(fn(args...));
            callFromTuple(std::mem_fn(&WatchesT<OtherT...>::trigger),
                          transformed,
                          &data->watches);
        };

    // 2.  Piggyback the data so that it's lifecycle is tied to this watch
    WatchT<OtherT...> res = data->watches.add();
    res.piggyback = data;

    // 4.  Bind the function in
    data->elementWatch.bind(onElement, data->watches.passError());

    // 5.  Return the watch
    return res;
}

template<typename... OtherT, typename Fn>
WatchT<OtherT...>
transform(Watch && watch,
          Fn fn)
{
    struct Data {
        WatchesT<OtherT...> watches;
        Watch elementWatch;
    };

    auto data = std::make_shared<Data>();

    Data * dataPtr = data.get();

    // Make sure that onRelease gets done properly
    data->watches.onRelease = [=] (Watch & w, Any && info)
        {
            dataPtr->elementWatch.detach();
        };

    // 1.  Create a watch on the elements
    data->elementWatch = std::move(watch);

    // 2.  Fuction that passes it along, transforming through the function
    auto onElement = [=] (const Any & args)
        {
            std::tuple<OtherT...> transformed
                = tuplize(fn(args));
            callFromTuple(std::mem_fn(&WatchesT<OtherT...>::trigger),
                          transformed,
                          &data->watches);
        };

    // 2.  Piggyback the data so that it's lifecycle is tied to this watch
    WatchT<OtherT...> res = data->watches.add();
    res.piggyback = data;

    // 4.  Bind the function in
    data->elementWatch.bindGeneric(onElement);

    // 5.  Return the watch
    return res;
}

template<typename... T, typename Fn>
Watch
transformGeneric(WatchT<T...> && watch,
                 Fn fn,
                 const std::type_info * returnedType = nullptr)
{
    struct Data {
        Data(const std::type_info * returnedType)
            : watches(returnedType)
        {
        }

        Watches watches;
        WatchT<T...> elementWatch;
    };
    
    auto data = std::make_shared<Data>(returnedType);

    Data * dataPtr = data.get();

    // Make sure that onRelease gets done properly
    data->watches.onRelease = [=] (Watch & w, Any && info)
        {
            dataPtr->elementWatch.detach();
        };

    // 1.  Create a watch on the elements
    data->elementWatch = std::move(watch);

    // 2.  Fuction that passes it along, transforming through the function
    auto onElement = [=] (const T &... args)
        {
            Any transformed = fn(args...);
            data->watches.triggerGeneric(transformed);
        };

    // 2.  Piggyback the data so that it's lifecycle is tied to this watch
    Watch res = data->watches.add();
    res.piggyback = data;

    // 4.  Bind the function in
    data->elementWatch.bind(onElement, data->watches.passError());

    // 5.  Return the watch
    return res;
}

/** Transform one type of watch into another, using implicit conversion to
    perform the transformation.
*/
template<typename... OtherT, typename... T>
WatchT<OtherT...>
transform(WatchT<T...> && watch)
{
    struct Data {
        WatchesT<OtherT...> watches;
        WatchT<T...> elementWatch;
    };

    auto data = std::make_shared<Data>();

    Data * dataPtr = data.get();

    // Make sure that onRelease gets done properly
    data->watches.onRelease = [=] (Watch & w, Any && info) -> void
        {
            dataPtr->elementWatch.detach();
        };

    // 1.  Create a watch on the elements
    data->elementWatch = std::move(watch);
    
    // 2.  Fuction that passes it along, transforming through the function
    auto onElement = [=] (const T &... args)
        {
            std::tuple<OtherT...> transformed(args...);
            callFromTuple(std::mem_fn(&WatchesT<OtherT...>::trigger),
                          transformed,
                          &data->watches);
        };

    // 3.  Piggyback the data so that it's lifecycle is tied to this watch
    WatchT<OtherT...> res = data->watches.add();
    res.piggyback = data;
    
    // 4.  Bind the function in
    data->elementWatch.bind(onElement, data->watches.passError());

    // 5.  Return the watch
    return res;
}

/** Filter a watch, using a free function to implement the filtering. */
template<typename... T, typename Fn>
WatchT<T...>
filter(WatchT<T...> && watch, Fn fn)
{
    struct Data {
        WatchesT<T...> watches;
        WatchT<T...> elementWatch;
    };

    auto data = std::make_shared<Data>();
    
    Data * dataPtr = data.get();

    // Make sure that onRelease gets done properly
    data->watches.onRelease = [=] (Watch & w, Any && info)
        {
            dataPtr->elementWatch.detach();
        };

    // 1.  Create a watch on the elements
    data->elementWatch = std::move(watch);

    // 2.  Fuction that passes it along, transforming through the function
    auto onElement = [=] (const T &... args)
        {
            if (!fn(args...))
                return;

            data->watches.trigger(args...);
        };

    // 3.  Bind the function in
    data->elementWatch.bind(onElement, data->watches.passError());
    
    // 4.  Piggyback the data so that it's lifecycle is tied to this watch
    WatchT<T...> res = data->watches.add();
    res.piggyback = data;

    // 5.  Return the watch
    return res;
}

struct AllOutput: public std::vector<Any> {
    std::shared_ptr<const ValueDescription> desc;
};

ValueDescriptionT<AllOutput> * getDefaultDescription(AllOutput * = 0);


struct WatchAllData {
    WatchAllData()
        : numDone(0)
    {
    }

    WatchesT<AllOutput> watchOutput;
    AllOutput output;
    std::vector<Watch> elementWatches;
    std::mutex mutex;
    int numDone;

    void addWatch(Watch && watch)
    {
        ExcAssertEqual(elementWatches.size(),
                       output.size());

        elementWatches.emplace_back(std::move(watch));
        output.emplace_back(Any());
    }

    void activate()
    {
        // Once all are done, we bind them so that they can start
        // triggering
        for (unsigned i = 0;  i < elementWatches.size();  ++i) {
            // clang doesn't like std::bind when used with g++6 headers
            elementWatches[i].bindGeneric
                ([this,i](const Any & val) { this->onElementTrigger(val, i); });
        }
    }

    void onElementTrigger(const Any & val, int elementNum)
    {
        std::unique_lock<std::mutex> guard(mutex);

        if (!output[elementNum].empty())
            watchOutput.throwException(WATCH_ERR_DOUBLE_TRIGGER,
                                       "double-trigger of watch on all");
        output[elementNum] = val;
        
        ++numDone;

        if (numDone == elementWatches.size()) {
            watchOutput.trigger(output);
            std::fill(output.begin(), output.end(), Any());
            numDone = 0;
        }
    }
};

inline void addWatchesToAll(WatchAllData & data)
{
}

template<typename... RestOfWatches>
inline void addWatchesToAll(WatchAllData & data, Watch && watch, RestOfWatches&&... rest)
{
    data.addWatch(std::move(watch));
    addWatchesToAll(data, std::forward<RestOfWatches>(rest)...);
}

template<typename... Watches>
WatchT<AllOutput> all(Watches&&... watches)
{
    auto data = std::make_shared<WatchAllData>();

    auto result = data->watchOutput.add();
    result.piggyback = data;

    addWatchesToAll(*data, std::forward<Watches>(watches)...);

    // Now activate it to send any existing results through
    data->activate();

    return result;
}


template<int start, int end>
struct UnpackToTuple {
    template<typename Tuple>
    static void go(const AllOutput & vec, Tuple & tuple)
    {
        typedef decltype(tuplize(std::declval<typename std::tuple_element<start, Tuple>::type>())) elType;
        try {
            std::get<start>(tuple)
                = detuplize(vec[start].as<elType>());
        } catch (const std::exception & exc) {
            using namespace std;
            cerr << "trying to unpack element " << start << " of tuple "
                 << "with type " << demangle(vec[start].type())
                 << " to type " << MLDB::type_name<elType>()
                 << endl;
            throw;
        }
        UnpackToTuple<start + 1, end>::go(vec, tuple);
    }

    template<typename Tuple>
    static void go(AllOutput && vec, Tuple & tuple)
    {
        typedef decltype(tuplize(std::declval<typename std::tuple_element<start, Tuple>::type>())) elType;
        try {
            std::get<start>(tuple) = detuplize(vec[start].as<elType>());
        } catch (const std::exception & exc) {
            using namespace std;
            cerr << "trying to unpack element " << start << " of tuple "
                 << "with type " << demangle(vec[start].type())
                 << " to type " << MLDB::type_name<elType>()
                 << endl;
            throw;
        }
        UnpackToTuple<start + 1, end>::go(vec, tuple);
    }

};

template<int n>
struct UnpackToTuple<n, n> {

    template<typename Tuple>
    static void go(const AllOutput & vec, Tuple & tuple)
    {
    }
};

/** Unpack the output of the all() watch into the individual components.

    Note that the components must match the values of the actual tuples.
*/
template<typename... Outputs>
std::tuple<decltype(detuplize(std::declval<Outputs>()))...>
unpackAll(const AllOutput & vec)
{
    std::tuple<decltype(detuplize(std::declval<Outputs>()))...> result;
    ExcAssertGreaterEqual(vec.size(), sizeof...(Outputs));
    UnpackToTuple<0, sizeof...(Outputs)>::go(vec, result);
    return result;
}

template<typename... Outputs>
std::tuple<decltype(detuplize(std::declval<Outputs>()))...>
unpackAll(AllOutput && vec)
{
    std::tuple<decltype(detuplize(std::declval<Outputs>()))...> result;
    ExcAssertGreaterEqual(vec.size(), sizeof...(Outputs));
    UnpackToTuple<0, sizeof...(Outputs)>::go(std::move(vec), result);
    return result;
}


/*****************************************************************************/
/* METHODS                                                                   */
/*****************************************************************************/

template<typename... T>
std::pair<bool, std::tuple<T...> >
Watch::
tryWaitTuple(double timeToWait)
{
    ExcAssert(data);

    if (boundType() && boundType() != &typeid(std::tuple<T...>))
        throwException(WATCH_ERR_TYPE,
                       "waiting for %s but got %s",
                       demangle(*boundType()).c_str(),
                       MLDB::type_name<std::tuple<T...> >().c_str());

    

    return WatchT<T...>::doTryWaitTuple(data, timeToWait);
}

} // namespace MLDB
