/* rest_collection.h                                               -*- C++ -*-
   Jeremy Barnes, 21 January 2014
   Copyright (C) 2014 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Helpers for creating a collection of REST objects.
*/

#pragma once

#include "rest_entity.h"
#include "mldb/rest/rest_request_router.h"
#include "mldb/rest/rest_request_params.h"
#include "mldb/watch/watch.h"
#include "link.h"
#include <map>
#include <atomic>


namespace MLDB {


template<typename Object, int Index>
inline static std::function<Object &
                            (RestConnection & connection,
                             const RestRequest & request,
                             const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const ObjectExtractor<Object, Index> & obj,
                         void *);

template<typename Key, class Value>
struct RestCollection;

struct CollectionConfigStore;

/*****************************************************************************/
/* REST COLLECTION BASE                                                      */
/*****************************************************************************/

struct RestCollectionBase : public RestEntity {
    RestCollectionBase(const Utf8String & nounSingular,
                       const Utf8String & nounPlural,
                       RestEntity * parent)
        : nounSingular(nounSingular),
          nounPlural(nounPlural),
          parent(parent)
    {
    }

    Utf8String nounSingular;
    Utf8String nounPlural;

    /** Function called once a background task has made some progress. */
    typedef std::function<bool (const Json::Value &)> OnProgress;

    virtual bool isCollection() const
    {
        return true;
    }

    virtual Utf8String getDescription() const
    {
        return "Operations on " + nounPlural;
    }

    virtual Utf8String getName() const
    {
        return nounPlural;
    }

    /** Get our parent. */
    virtual RestEntity * getParent() const
    {
        return parent;
    }

    /// Parent entity for the collection
    RestEntity * parent;
};


/*****************************************************************************/
/* ELEMENT COLLECTION BASE                                                   */
/*****************************************************************************/

    /** Event that a watch on the elements will return. */
template<typename Key, class Value>
struct RestCollectionChildEvent: public RestEntityChildEvent {
    RestCollection<Key, Value> * parentCollection;
    Key key;
    std::shared_ptr<Value> value;
};



// Instantiated in constructor
template<typename Key, class Value>
ValueDescriptionT<RestCollectionChildEvent<Key, Value> > *
getDefaultDescription(RestCollectionChildEvent<Key, Value> * = 0);


/*****************************************************************************/
/* BACKGROUND TASK BASE                                                      */
/*****************************************************************************/

/** Base class for background tasks. */

struct BackgroundTaskBase {
    /// INITIALIZING and EXECUTING are transient states while 
    /// CANCELLED, FINISHED and ERROR are final states 
    enum State {INITIALIZING, EXECUTING, CANCELLED, FINISHED, ERROR};

    BackgroundTaskBase();

    /** When destroying a background task, we make sure it's cancelled or
        finished so that there is nothing outstanding.
    */
    virtual ~BackgroundTaskBase();

    Json::Value getProgress() const;

    /** Cancel the task.  This is synchronous, and will block until the
        cancellation is finished or the task has completed.  May be called
        from multiple threads.

        Return false if the task was already cancelled and true otherwise.
    */
    bool cancel() noexcept;

    /** Set state to State::ERROR */
    void setError(std::exception_ptr exc = nullptr);

    /** Set state to State::EXECUTING */
    void setProgress(const Json::Value & progress);

    /** Set state to State::FINISHED */
    void setFinished();

    /** Set the handle of the thread that's doing this task, so we know what
        to cancel when we set up a hard cancellation.
    */
    void setHandle(int64_t handle)
    {
        std::unique_lock<std::mutex> guard(mutex);
        // std::cerr << "new task with handle " << handle << std::endl;
        this->handle = handle;
    }

    Utf8String getState() const;

    typedef std::function<bool (const Json::Value &)> OnProgress;

    /** A task is running until it is CANCELLED, FINISHED or in ERROR state */
    std::atomic<bool>  running;
    std::atomic<State> state;
    WatchesT<bool> cancelledWatches;
    
    /// Everything below here is protected by this mutex
    mutable std::mutex mutex;
    std::exception_ptr exc;
    Json::Value progress;
    std::vector<OnProgress> onProgressFunctions;
    int64_t handle;  ///< Handle of the thread running task
};


/*****************************************************************************/
/* BACKGROUND TASK                                                           */
/*****************************************************************************/

template<typename Value>
struct BackgroundTaskT: public BackgroundTaskBase {
    /** Function called once a background task is done. */
    typedef std::function<void (std::shared_ptr<Value>)> OnDone;

    std::shared_ptr<Value> value;
    std::vector<OnDone> onDoneFunctions;
    Any config;   /// Configuration of object being created with task
};


/*****************************************************************************/
/* REST COLLECTION                                                           */
/*****************************************************************************/

/** Something that is a homogeneous collection of REST objects, each one with
    its own key and value.
*/
template<typename Key, class Value>
struct RestCollection : public RestCollectionBase {
    RestCollection(const Utf8String & nounSingular,
                   const Utf8String & nounPlural,
                   RestEntity * parent);

    RestCollection(RestCollection && other) = delete;
    RestCollection(const RestCollection & other) = delete;

    RestCollection & operator = (RestCollection && other) = delete;
    void operator = (const RestCollection & other) = delete;

    //void swap(RestCollection & other);

    ~RestCollection();

    void shutdown();

    /** Clear the children.  Only makes sense for collections. */
    virtual void clearChildren()
    {
        this->clear();
    }

    struct RouteManager: public RestRouteManager {
        // Function that will extract the current collection from a currently
        // parsed REST request.
        typedef std::function<RestCollection *
                              (const RestRequestParsingContext & context)>
        GetCollection;

        RouteManager(RestRouteManager & parentManager,
                     RestRequestRouter & parentNode,
                     int resourceElementsMatched,
                     GetCollection getCollection,
                     const Utf8String & nounSingular,
                     const Utf8String & nounPlural)
            : RestRouteManager(parentManager, parentNode,
                               resourceElementsMatched),
              getCollection(getCollection),
              nounSingular(nounSingular),
              nounPlural(nounPlural),
              collectionNode(0), valueNode(0)
        {
        }

        RouteManager(RestRequestRouter & parentNode,
                     int resourceElementsMatched,
                     GetCollection getCollection,
                     const Utf8String & nounSingular,
                     const Utf8String & nounPlural)
            : RestRouteManager(parentNode, resourceElementsMatched),
              getCollection(getCollection),
              nounSingular(nounSingular),
              nounPlural(nounPlural),
              collectionNode(0), valueNode(0)
        {
        }

        GetCollection getCollection;
        Utf8String nounSingular;
        Utf8String nounPlural;
        RestRequestRouter * collectionNode;
        RestRequestRouter * valueNode;

        // Function that will extract the key from a currently
        // parsed REST request.
        typedef std::function<Key (const RestRequestParsingContext & context)>
        GetKey;

        GetKey getKey;
    };

    /** Initialize just the collection node, so that routes can be added
        before initRoutes is called.
    */
    static void initNodes(RouteManager & result);

    /** Initialize all routes, including the collection node if not already
        done.
    */
    static void initRoutes(RouteManager & result);
    
    virtual RestRequestMatchResult
    handleGetValue(Key key,
                   RestConnection & connection,
                   const RestRequest & request,
                   const RestRequestParsingContext & context);

    /** Handler for the get on /.  Default gets a list of all items in the
        collection.
    */
    virtual std::vector<Key> getKeys() const;

    /** Return the number of elements in the collection at the current moment. */
    size_t size() const;

    /** Is the colection empty? */
    bool empty() const;

    /** Handler for the get of a given key that already exists.  Default will
        simply call getExistingValue and return the result.
    */
    virtual RestRequestMatchResult
    handleGetExistingValue(Key key,
                           const Value & value,
                           RestConnection & connection,
                           const RestRequest & request,
                           const RestRequestParsingContext & context) const;

    virtual Json::Value getExistingValue(Key key,
                                         const Value & value) const;

    /** Function called once a background task is done. */
    typedef std::function<void (std::shared_ptr<Value>)> OnDone;

    typedef BackgroundTaskT<Value> BackgroundTask;

    typedef std::function<std::shared_ptr<Value>(const OnProgress & onProgress,
                                                 WatchT<bool> cancel)>
        BackgroundThreadFunction;

    /** Causes the given background job to be started in a thread.  The given
        progress function will be added to those updated on progress.  Once the
        job is finished, the given onDone function will be called unless null.

        If there was an error, the onDone function will be called with a null
        pointer.  The error message will be stored in the background task.

        If mustBeNewEntry is false, then it will throw if the entry is not newly
        added to the list of objects.
        
        Guarantees that:
        - the onDone function is called exactly once
    */
    void addBackgroundJobInThread(Key key,
                                  const BackgroundThreadFunction & fn,
                                  const OnProgress & onProgress = nullptr,
                                  const OnDone & onDone = nullptr,
                                  bool mustBeNewEntry = false,
                                  Any config = Any());

    void finishedBackgroundJob(Key key,
                               std::shared_ptr<BackgroundTask> task,
                               bool mustBeNewEntry = false);

    bool addEntry(Key key,
                  std::shared_ptr<Value> value,
                  bool mustBeNewEntry = true)
    {
        std::atomic<BackgroundTaskBase::State> state(BackgroundTaskBase::State::FINISHED);
        return addEntryItl(key, std::move(value), mustBeNewEntry, state);
    }

    bool replaceEntry(Key key,
                      std::shared_ptr<Value> value,
                      bool mustAlreadyExist = true)
    {
        std::atomic<BackgroundTaskBase::State> state(BackgroundTaskBase::State::FINISHED);
        return replaceEntryItl(key, std::move(value), mustAlreadyExist, state);
    }

    std::pair<std::shared_ptr<Value>,
              std::shared_ptr<BackgroundTask> >
    getEntry(Key key) const;

    std::pair<std::shared_ptr<Value>,
              std::shared_ptr<BackgroundTask> >
    tryGetEntry(Key key) const;

    std::shared_ptr<Value>
    getExistingEntry(Key key) const;

    std::shared_ptr<Value>
    tryGetExistingEntry(Key key) const;

    /** Peform the given action on each entry, without taking the lock for
        too long.

        If the function returns false, then the loop will be terminated.
    */
    bool forEachEntry(const std::function<bool (Key key, Value & value)> & fn);

    /** Peform the given action on each entry, without taking the lock for
        too long.

        If the function returns false, then the loop will be terminated.
    */
    bool forEachEntry(const std::function<bool (Key key, const Value & value)> & fn) const;

    /** Remove the given entry.  Returns the entry removed. */
    std::shared_ptr<Value> deleteEntry(Key key);

    /** Remove all entries. */
    void clear();

    /** Watch for children.  This will fail at run time unless the children
        are derived from RestEntity.
    */
    virtual WatchT<RestEntityChildEvent>
    watchChildren(const Utf8String & spec, bool catchUp, Any info);

    typedef RestCollectionChildEvent<Key, Value> ChildEvent;
    
    /** More detailed watch on the collection.  Returns full information even
        if not derived from RestEntity.
    */
    virtual WatchT<ChildEvent>
    watchElements(const Utf8String & spec, bool catchUp, Any info);

    /** More detailed watch on the collection.  Returns full information even
        if not derived from RestEntity.
    */
    virtual WatchT<Utf8String>
    watchNames(const Utf8String & spec, bool catchUp, Any info);
    
    virtual Watch
    watchChannel(const Utf8String & chanel, const Utf8String & filter,
                 bool catchUp, Any info);

    virtual std::pair<const std::type_info *,
                      std::shared_ptr<const ValueDescription> >
    getWatchBoundType(const ResourceSpec & spec);

    std::vector<Any>
    getChildWatches() const;

    virtual std::shared_ptr<EntityLinkToken>
    acceptLink(const std::vector<Utf8String> & sourcePath,
               const std::vector<Utf8String> & targetPath,
               const std::string & linkType,
               Any linkParams);

protected:
    bool addEntryItl(Key key,
                     std::shared_ptr<Value> value,
                     bool mustBeNewEntry,
                     std::atomic<BackgroundTaskBase::State> & state);

    bool replaceEntryItl(Key key,
                         std::shared_ptr<Value> value,
                         bool mustAlreadyExist,
                         std::atomic<BackgroundTaskBase::State> & state);
    struct Entry {
        std::shared_ptr<BackgroundTask> underConstruction;
        std::shared_ptr<Value> value;
    };

    typedef std::map<Key, Entry> Entries;

    struct Impl;
    std::unique_ptr<Impl> impl;

    void throwEntryAlreadyExists(const Key & key) const MLDB_NORETURN;
    void throwEntryNotReady(const Key & key) const MLDB_NORETURN;
    void throwEntryDoesntExist(const Key & key) const MLDB_NORETURN;
    void throwEntryNotObtained(const Key & key) const MLDB_NORETURN;
    void throwEntryNotOverwritten(const Key & key) const MLDB_NORETURN;
};


/*****************************************************************************/
/* REST CONFIGURABLE COLLECTION                                              */
/*****************************************************************************/

/** A collection of REST objects that are each configurable and have a status.

    This can be used as a combination collection and factory to instantiate
    elements as their configuration is known.
*/

template<typename Key, typename Value,
         typename Config, typename Status>
struct RestConfigurableCollection: public RestCollection<Key, Value> {

    RestConfigurableCollection(const Utf8String & nounSingular,
                               const Utf8String & nounPlural,
                               RestEntity * parent);

    RestConfigurableCollection(const RestConfigurableCollection &) = delete;
    RestConfigurableCollection(RestConfigurableCollection &&) = delete;

    void operator = (const RestConfigurableCollection &) = delete;
    void operator = (RestConfigurableCollection &&) = delete;

    ~RestConfigurableCollection();

    typedef RestCollection<Key, Value> Base;
    typedef typename Base::OnProgress OnProgress;
    typedef typename Base::OnDone OnDone;
    typedef typename Base::BackgroundTask BackgroundTask;
    typedef typename Base::ChildEvent ChildEvent;

    /** Ensures that any config manipulations will be persisted to the given
        config store.
     */
    virtual void attachConfig(std::shared_ptr<CollectionConfigStore> configStore);

    /** Load up any existing entities, getting them and their configuration from
        the store configured via attachConfig.
    */
    virtual void loadConfig();

    /** Return whether this object has required persistence.  Default
        implementation returns true.
    */
    virtual bool objectIsPersistent(const Key & key, const Config & config) const;

    virtual RestRequestMatchResult
    handleGetValue(Key key,
                   RestConnection & connection,
                   const RestRequest & request,
                   const RestRequestParsingContext & context);

    /* Creation of a new object using a PUT request. 
       Set mustBeNew to true to prevent a PUT request to overwrite an existing object.
       Note that even if mustBeNew is set to false (the default) this call may fail
       if the object is still under construction.
    */
    virtual Status handlePut(Key key, Config config, bool mustBeNew = false);
    virtual Status handlePutSync(Key key, Config config, bool mustBeNew = false);

    virtual Status handlePost(Key key, Config config, bool mustBeNew = false);
    virtual Status handlePostSync(Key key, Config config, bool mustBeNew = false);

    virtual void handleDelete(Key key);

    struct RouteManager: public RestCollection<Key, Value>::RouteManager {
        // Function that will extract the current collection from a currently
        // parsed REST request.
        typedef std::function<RestConfigurableCollection *
                              (const RestRequestParsingContext & context)>
        GetCollection;


        RouteManager(RestRouteManager & parentManager,
                     RestRequestRouter & parentNode,
                     int resourceElementsMatched,
                     GetCollection getCollection,
                     const Utf8String & nounSingular,
                     const Utf8String & nounPlural)
            : RestCollection<Key, Value>::RouteManager
              (parentManager, parentNode, resourceElementsMatched,
               getCollection, nounSingular, nounPlural),
              getCollection(getCollection)
        {
        }

        GetCollection getCollection;

        void addPutRoute();
        void addPostRoute();
        void addDeleteRoute();
    };

#if 0
    /** Add a route that allows a PUT to the collection. */
    virtual void addPutRoute();

    /** Add a route that allows a POST to the collection. */
    virtual void addPostRoute();

    /** Add a route that allows a DELETE from the collection. */
    virtual void addDeleteRoute();
#endif

    virtual Status getStatus(Key key) const;

    virtual Status getStatusLoading(Key key, const BackgroundTask & task) const;

    virtual Status getStatusFinished(Key key, const Value & value) const = 0;

    /** Return the configuration.  Default throws. */
    virtual std::shared_ptr<Config>
    getConfig(Key key, const Value & value) const;

    virtual Key getKey(Config & config) = 0;

    /** Used for a put to update the config object.  Default calls getKey. */
    virtual void setKey(Config & config, Key key)
    {
        auto key2 = getKey(config);
        if (key != key2)
            throw MLDB::Exception("attempt to put under the wrong name (passed '%s', "
                                "should be '%s'",
                                restEncode(key).rawData(),
                                restEncode(key2).rawData());
    }

    /** Obtain methods get an existing entity or create a new one
        if it doesn't already existing.
    */
    virtual Key obtainAsync(Config config,
                            const OnProgress & onProgress,
                            const OnDone & onDone);
    virtual std::shared_ptr<Value>
    obtainSync(Config config,
               const OnProgress & onProgress);
    /** Like obtainSync, but will throw an exception if it can't be obtained
        for any reason, and so is guaranteed to never return a null pointer.
    */
    virtual std::shared_ptr<Value>
    mustObtainSync(Config config,
                   const OnProgress & onProgress);
    /** Like obtainSync, but will throw an exception if it already exist
        and overwrite is false (default). 
    */
    virtual std::shared_ptr<Value>
    mustCreateSync(Config config, const OnProgress & onProgress, bool overwrite = false);
 
    virtual std::shared_ptr<Value>
    constructCancellable(Config config,
                         const OnProgress & onProgress,
                         WatchT<bool> cancelled) const;

    /** NOTE: This method should be pure virtual, except that we want
        to be able to call it during destruction and for that to
        perform a no-op.
    */
    virtual std::shared_ptr<Value>
    construct(Config config,
              const OnProgress & onProgress) const;

    virtual WatchT<Key, std::shared_ptr<Config> >
    watchConfig(const Utf8String & spec, bool catchUp, Any info);
    
    virtual WatchT<Key, std::shared_ptr<Status> >
    watchStatus(const Utf8String & spec, bool catchUp, Any info);
    
    virtual Watch
    watchChannel(const Utf8String & chanel, const Utf8String & filter,
                 bool catchUp, Any info);

    virtual WatchT<std::vector<Utf8String>, Any>
    watchWithPath(const ResourceSpec & spec, bool catchUp, Any info = nullptr,
                  const std::vector<Utf8String> & currentPath
                       = std::vector<Utf8String>());

    virtual std::pair<const std::type_info *,
                      std::shared_ptr<const ValueDescription> >
    getWatchBoundType(const ResourceSpec & spec);

    /// Notify that a child's configuration changed
    void notifyChildChangedConfig(Key key,
                                  std::shared_ptr<Value> value);

    /// Notify that a child's status changed
    void notifyChildChangedStatus(Key key,
                                  std::shared_ptr<Value> value);

    /// Make sure that the child watch handler is present
    void ensureChildWatchPresent();

    /// Function bound in to the childWatch
    void onChildChange(const ChildEvent & event);

    /// Watches set on the configuration of all objects
    WatchesT<Key, std::shared_ptr<Config> > configWatches;

    /// Watches set on the status of all objects
    WatchesT<Key, std::shared_ptr<Status> > statusWatches;

    /// Child watch to allow changes in config to be reported
    WatchT<ChildEvent> childWatch;

    /// Is the child watch active?
    std::atomic<bool> childWatchActive;

    std::shared_ptr<CollectionConfigStore> configStore;

protected:
    bool backgroundCreate;
    /* return true if the object is being created in a background thread */
    bool handlePutItl(Key key, Config config, const OnDone & onDone, bool mustBeNew);
};

#define REST_COLLECTION_INSTANTIATIONS_IMPL(extern, Key, Value, Config, Status) \
extern template class BackgroundTaskT<Value>;                       \
extern template class RestCollection<Key, Value>; \
extern template class RestConfigurableCollection<Key, Value, Config, Status>; \
extern template class WatchT<Key, std::shared_ptr<Status> >; \
extern template class WatchT<Key, std::shared_ptr<Config> >; \
extern template class WatchesT<Key, std::shared_ptr<Config> >; \
extern template class WatchesT<Key, std::shared_ptr<Status> >

#define DECLARE_REST_COLLECTION_INSTANTIATIONS(Key, Value, Config, Status) \
    REST_COLLECTION_INSTANTIATIONS_IMPL(extern, Key, Value, Config, Status)

#define DEFINE_REST_COLLECTION_INSTANTIATIONS(Key, Value, Config, Status) \
    REST_COLLECTION_INSTANTIATIONS_IMPL( ,Key, Value, Config, Status)

} // namespace MLDB
