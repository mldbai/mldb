/** rest_collection_impl.h                                         -*- C++ -*-
    Jeremy Barnes, 21 January 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/types/value_description.h"
#include "rest_collection.h"
#include "mldb/watch/watch_impl.h"
#include "mldb/arch/rcu_protected.h"
#include <future>
#include "collection_config_store.h"
#include "mldb/types/utility_descriptions.h"
#include "mldb/types/vector_description.h"
#include "mldb/rest/cancellation_exception.h"


namespace MLDB {

// Defined in rest_request_binding.h.  Used to validate that request
// parameters match those exposed in the help.
std::function<bool
              (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)>
createRequestValidater(const Json::Value & argHelp,
                       std::set<Utf8String> ignore);


/*****************************************************************************/
/* REST COLLECTION                                                           */
/*****************************************************************************/

template<typename Key, class Value>
ValueDescriptionT<RestCollectionChildEvent<Key, Value> > *
getDefaultDescription(RestCollectionChildEvent<Key, Value> *)
{
    return new PureValueDescription<RestCollectionChildEvent<Key, Value> >();
}

template<typename T>
RestEntity *
getChildEntity(T * val,
               typename std::enable_if<std::is_convertible<T *, RestEntity *>::value,
                                       void>::type * = 0)
{
    return val;
}

template<typename T>
RestEntity *
getChildEntity(T * val,
               typename std::enable_if<!std::is_convertible<T *, RestEntity *>::value,
                                       void>::type * = 0)
{
    return nullptr;
}

template<typename Key, class Value>
struct RestCollection<Key, Value>::Impl {
    Impl()
        : entries(entriesLock)
    {
    }

    mutable GcLock entriesLock;
    RcuProtected<Entries> entries;

    /// Watches on the children
    WatchesT<ChildEvent> childWatches;

    typedef Spinlock MutateMutex;

    /** Mutex on mutation operations.  This is required so that the watch
        operations are serializable.  Could be turned off when there are no
        watches.

        Despite this mutex, read operations remain lock-free and are
        protected by the RCU lock.
    */
    MutateMutex mutateMutex; // not mutable on purpose
};


template<typename Key, class Value>
RestCollection<Key, Value>::
RestCollection(const Utf8String & nounSingular,
               const Utf8String & nounPlural,
               RestEntity * parent)
    : RestCollectionBase(nounSingular, nounPlural, parent),
      impl(new Impl())
{
}

template<typename Key, class Value>
RestCollection<Key, Value>::
~RestCollection()
{
    shutdown();
}

#if 0
template<typename Key, class Value>
RestCollection<Key, Value>::
RestCollection(RestCollection && other)
    : RestCollectionBase(other.nounSingular, other.nounPlural, other.parent),
      valueNode(0),
      impl(std::move(other.impl))
{
}

template<typename Key, class Value>
RestCollection<Key, Value> &
RestCollection<Key, Value>::
operator = (RestCollection && other)
{
    swap(other);
}

template<typename Key, class Value>
void
RestCollection<Key, Value>::
swap(RestCollection & other)
{
    std::swap(impl, other.impl);
    std::swap(valueNode, other.valueNode);
}
#endif

template<typename Key, class Value>
void
RestCollection<Key, Value>::
shutdown()
{
    clear();
}

template<typename Key, class Value>
void
RestCollection<Key, Value>::
clear()
{
    std::unique_lock<typename Impl::MutateMutex> mutateGuard(impl->mutateMutex);

    using namespace std;
    //cerr << "clearing collection with " << size() << " entries" << endl;

    auto cleanup = impl->entries.replaceCustomCleanup(new Entries());
    impl->entriesLock.visibleBarrier();

    // TODO CONCURRENCY: all of these watches should complete before we
    // make any other notifications, to avoid events from other events
    // being interleaved.  This will allow us to weaken the locking
    // requirements of the mutateMutex above, which will improve perf.
    // Not doing for the moment, since focus is on stability not the last
    // 5% of performance.
    // auto exclusiveLock = impl->childWatches.getExclusiveAccess();

    for (auto & e: *cleanup) {
        // First, wait until it has actually be cleaned up
        if (e.second.underConstruction) {
            // Stop it, and wait for it to finish
            e.second.underConstruction->cancel();
            ExcAssert(!e.second.underConstruction->running);

            // Note that even if there is a race condition where the
            // construction finished after we swapped the collections,
            // it will be blocked on the mutateMutex in addEntryItl,
            // and once this routine exits (and the mutex is released),
            // it will see that it was cancelled and the add will abort.
        }
        else {
            // Let the watch know it's being deleted
            if (!impl->childWatches.empty()) {
                ChildEvent event;
                event.event = CE_DELETED;
                event.parent = event.parentCollection = this;
                event.key = e.first;
                event.name = restEncode(event.key);
                event.object = e.second.value.get();
                event.entity = getChildEntity(e.second.value.get());
                event.value = e.second.value;

                //cerr << "triggering child watch for delete on " << event.name << endl;

                impl->childWatches.trigger(event);
            }
        }
    }
}

template<typename Key, class Value>
void
RestCollection<Key, Value>::
initNodes(RouteManager & result)
{
    if (result.collectionNode)
        return;

    ExcAssert(result.parentNode);
    ExcAssert(result.getCollection);

    const Utf8String & nounSingular = result.nounSingular;
    const Utf8String & nounPlural = result.nounPlural;

    RestRequestRouter & parentNode = *result.parentNode;

    int keyResourceIndex = result.resourceElementsMatched;

    result.getKey = [keyResourceIndex] (const RestRequestParsingContext & context)
        {
            //using namespace std;
            //cerr << "context.resources = " << jsonEncode(context.resources) << endl;
            //cerr << "keyResourceIndex = " << keyResourceIndex << endl;
            return restDecode(context.resources.at(keyResourceIndex), (Key *)0);
        };

    /* This is called after the entity name has been parsed.  It looks it up in the
       collection, puts it in the context, and causes an error if it didn't exist
       and we weren't doing a PUT.
    */
    auto getCollection = [=] (RestConnection & connection,
                              const RestRequest & request,
                              RestRequestParsingContext & context)
        {
            context.addObject(result.getCollection(context));
        };

    RestRequestRouter & collectionNode
        = parentNode.addSubRouter("/" + nounPlural,
                                  "Operations on collection of " + nounPlural,
                                  getCollection);

    result.collectionNode = &collectionNode;

    /* This is called after the entity name has been parsed.  It looks it up in the
       collection, puts it in the context, and causes an error if it didn't exist
       and we weren't doing a PUT.
    */
    auto getElement = [=] (RestConnection & connection,
                           const RestRequest & request,
                           RestRequestParsingContext & context)
        {
            auto collection = result.getCollection(context);

            Utf8String resource = context.resources.back();
            Key key = restDecode(resource, (Key *)0);

            std::shared_ptr<Value> ptr;
            std::shared_ptr<BackgroundTask> task;

            std::tie(ptr, task) = collection->tryGetEntry(key);

            if (!ptr) {
                if (context.remaining.empty()
                    && (request.verb == "PUT"
                        || request.verb == "GET"
                        || request.verb == "DELETE")) {
                    /* If we have a PUT, or we are doing a GET on the entry
                       itself (for its status), or we are doing a DELETE, then
                       we can continue without a pointer.  Otherwise we need to bail.
                    */
                    return;
                } 
                else if (context.remaining == "/state"
                         && (request.verb == "PUT"
                             || request.verb == "GET")) {
                    /* Trying to change the state of an object being created.
                       We are most likely trying to cancel the operation.  We
                       can continue without a pointer.
                    */
                    return;
                }

                Json::Value error;
                error["kind"] = nounSingular;
                error["resource"] = resource;

                if (task) {
                    error["state"] = task->getState();
                    error["progress"] = task->getProgress();
                    if (task->state == BackgroundTaskBase::State::ERROR) {
                        error["error"]
                            = nounSingular + " entry '"
                            + resource + "' not available due to error in creation";
                    }
                    else if (task->state == BackgroundTaskBase::State::CANCELLED) {
                        error["error"]
                            = nounSingular + " entry '"
                            + resource + "' not available due to cancellation";
                    } else {
                        error["error"]
                            = nounSingular + " entry '"
                            + resource + "' not available as it is still being created";
                    }
                }
                else {
                    error["error"]
                        = nounSingular + " entry '"
                        + resource + "' does not exist or has been deleted";
                }
                connection.sendResponse(404, error);
            }
            else {
                context.addSharedPtr(ptr);
            }
        };

    auto & valueNode
        = collectionNode.addSubRouter(Rx("/([^/]*)", "/<" + nounSingular + ">"),
                                      "operations on an individual " + nounSingular,
                                      getElement);

    result.valueNode = &valueNode;
}

template<typename Key, class Value>
void
RestCollection<Key, Value>::
initRoutes(RouteManager & result)
{
    initNodes(result);

    ExcAssert(result.parentNode);
    ExcAssert(result.getCollection);

    const Utf8String & nounSingular = result.nounSingular;
    const Utf8String & nounPlural = result.nounPlural;

    Json::Value help;
    help["result"] = nounPlural + " in the collection";

    auto validater = createRequestValidater(help, {});

    auto getCollectionRoute = [=] (RestConnection & connection,
                                   const RestRequest & req,
                                   const RestRequestParsingContext & cxt)
        {
            try {
                if (!validater(connection, req, cxt))
                    return RestRequestRouter::MR_ERROR;

                auto collection = result.getCollection(cxt);

                MLDB_TRACE_EXCEPTIONS(false);
                auto resultStr = jsonEncodeStr(collection->getKeys());
                connection.sendHttpResponse(200, resultStr,
                                            "application/json", {});
                return RestRequestRouter::MR_YES;
            } catch (const std::exception & exc) {
                return sendExceptionResponse(connection, exc);
            }
        };

    result.collectionNode->addRoute("", { "GET" },
                                    "Get a list of " + nounPlural
                                    + " in the collection",
                                    getCollectionRoute, help);

    help["result"] = nounPlural + " in the collection";

    validater = createRequestValidater(help, {});


    auto getValueRoute = [=] (RestConnection & connection,
                              const RestRequest & req,
                              const RestRequestParsingContext & cxt)
        {
            try {
                if (!validater(connection, req, cxt))
                    return RestRequestRouter::MR_ERROR;

                auto collection = result.getCollection(cxt);
                auto key = result.getKey(cxt);

                MLDB_TRACE_EXCEPTIONS(false);
                collection->handleGetValue(key, connection, req, cxt);
                return RestRequestRouter::MR_YES;
            } catch (const std::exception & exc) {
                return sendExceptionResponse(connection, exc);
            }
        };

    result.valueNode->addRoute("", { "GET" },
                               "Get the status of a " + nounSingular,
                               getValueRoute, help);
}

template<typename Key, class Value>
RestRequestMatchResult
RestCollection<Key, Value>::
handleGetValue(Key key,
               RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)
{
    auto found = this->getEntry(key);

    if (found.second) {
        // under construction
        connection.sendResponse(200, found.second->progress);
        return RestRequestRouter::MR_YES;
    }
    else {
        return handleGetExistingValue(key, *found.first, connection, request, context);
    }
}

template<typename Key, class Value>
std::vector<Key>
RestCollection<Key, Value>::
getKeys() const
{
    std::vector<Key> result;

    // NOTE: Should not be necessary... investigation needed
    GcLock::SharedGuard guard(impl->entriesLock);

    auto es = impl->entries.getImmutable();
    for (auto & e: *es) {
        result.push_back(e.first);
    }
    return result;
}

template<typename Key, class Value>
size_t
RestCollection<Key, Value>::
size() const
{
    // NOTE: Should not be necessary... investigation needed
    GcLock::SharedGuard guard(impl->entriesLock);

    auto es = impl->entries.getImmutable();
    return es->size();
}

template<typename Key, class Value>
bool
RestCollection<Key, Value>::
empty() const
{
    // NOTE: Should not be necessary... investigation needed
    GcLock::SharedGuard guard(impl->entriesLock);

    auto es = impl->entries.getImmutable();
    return es->empty();
}

template<typename Key, class Value>
void
RestCollection<Key, Value>::
throwEntryAlreadyExists(const Key & key) const
{
    throw HttpReturnException(409,
                              nounSingular + " entry '"
                              + restEncode(key) + "' already exists",
                              "collection", this->nounPlural,
                              "entry", key);
}

template<typename Key, class Value>
void
RestCollection<Key, Value>::
throwEntryNotReady(const Key & key) const
{
    throw HttpReturnException(409,
                              nounSingular + " entry '"
                              + restEncode(key) + "' exists but is not ready",
                              "collection", this->nounPlural,
                              "entry", key);
}

template<typename Key, class Value>
void
RestCollection<Key, Value>::
throwEntryDoesntExist(const Key & key) const
{
    throw HttpReturnException(404,
                              nounSingular + " entry '"
                              + restEncode(key) + "' doesn't exist",
                              "collection", this->nounPlural,
                              "entry", key);
}

template<typename Key, class Value>
void
RestCollection<Key, Value>::
throwEntryNotObtained(const Key & key) const
{
    throw HttpReturnException(409,
                              nounSingular + " entry '"
                              + restEncode(key) + "' could not be obtained",
                              "collection", this->nounPlural,
                              "entry", key);
}

template<typename Key, class Value>
void
RestCollection<Key, Value>::
throwEntryNotOverwritten(const Key & key) const
{
    throw HttpReturnException(409,
                              nounSingular + " entry '"
                              + restEncode(key) + "' could not be overwritten",
                              "collection", this->nounPlural,
                              "entry", key);
}


template<typename Key, class Value>
void
RestCollection<Key, Value>::
addBackgroundJobInThread(Key key,
                         const BackgroundThreadFunction & fn,
                         const OnProgress & onProgress,
                         const OnDone & onDone,
                         bool mustBeNewEntry,
                         Any config)
{
    using namespace std;

    // NOTE: Should not be necessary... investigation needed
    GcLock::SharedGuard guard(impl->entriesLock);

    for (;;) {
        auto oldEntries = impl->entries();
        std::unique_ptr<Entries> newEntries(new Entries(*oldEntries));

        auto it = newEntries->find(key);
        if (it != newEntries->end()) {
            if (mustBeNewEntry) {
                throwEntryAlreadyExists(key);
            }
            auto entry = it->second;

            if (!entry.value) {
                // cerr << "under construction" << endl;

                auto task = entry.underConstruction;

                ExcAssert(task);

                std::unique_lock<std::mutex> guard(task->mutex);

                // MLDB-748 - bail out if the task is not completed
                if (task->running) {
                    throw HttpReturnException(409, "entry '"
                                              +  restEncode(key)
                                              + "' in "
                                              + this->nounSingular
                                              + " could not be overwritten because it is being constructed");
                }
            }
        }

        // cerr << "creating job for " << restEncode(key) << endl;

        // Set up the task, without starting it yet
        auto task = std::make_shared<BackgroundTask>();
        task->config = config;

        auto onProgressFn = [=] (const Json::Value & progress)
            {
                std::unique_lock<std::mutex> guard(task->mutex);
                task->setProgress(progress);
                // if (task->state == BackgroundTaskBase::State::CANCELLED)
                //    cerr << "calling progress when cancelled" << endl;
                return !(task->state == BackgroundTaskBase::State::CANCELLED);
            };


        std::shared_ptr<WatchT<bool> > cancelledPtr
            (new WatchT<bool>(task->cancelledWatches.add()));

        auto toRun = [=] ()
            {
                MLDB_TRACE_EXCEPTIONS(false);
                try {
                    WatchT<bool> cancelled = std::move(*cancelledPtr);
                    task->value = fn(onProgressFn, std::move(cancelled));
                    task->setFinished();
                }
                catch (const CancellationException & exc) {
                    // throwing CancellationException when the task
                    // state was not set to Cancelled
                    ExcAssertEqual(task->state, BackgroundTaskBase::State::CANCELLED);
                    task->setFinished();
                }
                catch (const std::exception & exc) {
                    // MDLB-1863 progress must be a json object
                    ExcAssert(task->progress.type() == Json::nullValue ||
                              task->progress.type() == Json::objectValue);

                    task->progress["exception"] = extractException(exc, 500);
                    task->setError(std::current_exception());
                    task->setFinished();
                }

                auto taskCopy = task;
                Key keyCopy = key;
                this->finishedBackgroundJob(std::move(keyCopy), std::move(taskCopy), mustBeNewEntry);
            };

        auto & entry = (*newEntries)[key];
        entry.underConstruction = task;

        if (onDone)
            task->onDoneFunctions.push_back(onDone);

        std::atomic_thread_fence(std::memory_order_release);

        if (impl->entries.cmp_xchg(oldEntries, newEntries, true)) {
            // Now we can start the task, since the commit succeeded
            task->running = true;
            std::thread thread(toRun);

            auto handle = thread.native_handle();

            task->setHandle(handle);

            // The thread runs independently and cleans itself up
            thread.detach();
            return;
        }

        // RCU failed because something raced us to update.  Try again.
    }
}

template<typename Key, class Value>
void
RestCollection<Key, Value>::
finishedBackgroundJob(Key key,
                      std::shared_ptr<BackgroundTask> task,
                      bool mustBeNewEntry)
{
    using namespace std;
    //cerr << "finished background job " << restEncode(key) << endl;

    std::unique_lock<std::mutex> guard(task->mutex);

    if (task->state == BackgroundTaskBase::State::CANCELLED ||
        task->state == BackgroundTaskBase::State::ERROR) {

        // there is no object created
        for (auto & f: task->onDoneFunctions)
            f(nullptr);
    }
    else {

        // If this check triggers, we've somehow destroyed our object
        // without waiting for background tasks to finish.
        ExcAssert(impl);
        //ExcAssertEqual(task->state, BackgroundTaskBase::State::FINISHED);
        
        // Only promote it if it's not in the error state
        // If we're currently clearing, then we shouldn't add anything
        // at all.
        if (task->value) {
            if (mustBeNewEntry)
                this->addEntryItl(key, task->value, true /* must add */, task->state);
            else
                this->replaceEntry(key, task->value, task->state);
        }
        
        for (auto & f: task->onDoneFunctions)
            f(task->value);
    }
}

template<typename Key, class Value>
bool
RestCollection<Key, Value>::
addEntryItl(Key key,
            std::shared_ptr<Value> val,
            bool mustBeNewEntry,
            std::atomic<BackgroundTaskBase::State> & state)
{
    if (!val)
        return false;  // should only happen when being destroyed

    std::unique_lock<typename Impl::MutateMutex> mutateGuard(impl->mutateMutex);

    // In the case of an entry that's created and then removed before
    // it can be added, it may be cancelled before it can obtain the
    // mutate guard.  In that case, we don't add it.
    if (state == BackgroundTaskBase::State::CANCELLED)
        return false;

    // NOTE: Should not be necessary... investigation needed
    GcLock::SharedGuard guard(impl->entriesLock);

    for (;;) {
        auto oldEntries = impl->entries();

        auto it = oldEntries->find(key);
        if (it != oldEntries->end() && it->second.value) {
            if (mustBeNewEntry)
                throwEntryAlreadyExists(key);
            else return false;
        }

        std::unique_ptr<Entries> newEntries(new Entries(*oldEntries));

        auto & entry = (*newEntries)[key];
        if (entry.value) {
            if (mustBeNewEntry)
                throwEntryAlreadyExists(key);
            else return false;
        }
        entry.underConstruction.reset();
        entry.value = val;

        std::atomic_thread_fence(std::memory_order_release);

        if (impl->entries.cmp_xchg(oldEntries, newEntries, true)) {

            if (!impl->childWatches.empty()) {
                ChildEvent event;
                event.event = CE_NEW;
                event.parent = event.parentCollection = this;
                event.key = key;
                event.name = restEncode(key);
                event.object = val.get();
                event.entity = getChildEntity(val.get());
                event.value = val;

                impl->childWatches.trigger(event);
            }
            //onNewEntry(key, *val);
            ExcAssert(!newEntries.get());
            return true;
        }

        ExcAssert(newEntries.get());
        // RCU failed because something raced us to update.  Try again.
    }
}

template<typename Key, class Value>
bool
RestCollection<Key, Value>::
replaceEntryItl(Key key,
                std::shared_ptr<Value> val,
                bool mustAlreadyExist,
                std::atomic<BackgroundTaskBase::State> & state)
{
    std::unique_lock<typename Impl::MutateMutex> mutateGuard(impl->mutateMutex);

    // In the case of an entry that's created and then removed before
    // it can be added, it may be cancelled before it can obtain the
    // mutate guard.  In that case, we don't add it.
    if (state == BackgroundTaskBase::State::CANCELLED)
        return false;

    // NOTE: Should not be necessary... investigation needed
    GcLock::SharedGuard guard(impl->entriesLock);

    for (;;) {
        auto oldEntries = impl->entries();

        std::unique_ptr<Entries> newEntries(new Entries(*oldEntries));

        auto it = newEntries->find(key);
        if (it == newEntries->end()) {
            if (mustAlreadyExist) {
                throwEntryDoesntExist(key);
            }
            (*newEntries)[key].underConstruction.reset();
            (*newEntries)[key].value = val;
        }
        else {
            it->second.underConstruction.reset();
            it->second.value = val;
        }

        std::atomic_thread_fence(std::memory_order_release);

        if (impl->entries.cmp_xchg(oldEntries, newEntries, true)) {

            if (!impl->childWatches.empty()) {
                ChildEvent event;
                event.event = CE_UPDATED;
                event.parent = event.parentCollection = this;
                event.key = key;
                event.name = restEncode(key);
                event.object = val.get();
                event.entity = getChildEntity(val.get());
                event.value = val;

                impl->childWatches.trigger(event);
            }
            //onNewEntry(key, *val);
            ExcAssert(!newEntries.get());
            return true;
        }

        ExcAssert(newEntries.get());
        // RCU failed because something raced us to update.  Try again.
    }
}

template<typename Key, class Value>
std::shared_ptr<Value>
RestCollection<Key, Value>::
deleteEntry(Key key)
{
    std::unique_lock<typename Impl::MutateMutex> mutateGuard(impl->mutateMutex);

    // NOTE: Should not be necessary... investigation needed
    GcLock::SharedGuard guard(impl->entriesLock);

    for (;;) {
        auto oldEntries = impl->entries();

        auto it = oldEntries->find(key);
        if (it == oldEntries->end())
            return nullptr;

        std::unique_ptr<Entries> newEntries(new Entries(*oldEntries));

        auto entry = (*newEntries)[key];
        newEntries->erase(key);

        //ExcAssertEqual(newEntries->size(), 20);

        std::atomic_thread_fence(std::memory_order_release);

        if (impl->entries.cmp_xchg(oldEntries, newEntries, true /* defer */)) {
            using namespace std;

            // Make sure children are cleared before we get rid of the
            // parent.  Otherwise recursive watches won't work properly.
            RestEntity * entity = getChildEntity(entry.value.get());
            if (entity)
                entity->clearChildren();

            if (!impl->childWatches.empty()) {
                ChildEvent event;
                event.event = CE_DELETED;
                event.parent = event.parentCollection = this;
                event.key = key;
                event.name = restEncode(key);
                //cerr << "successfully deleted entry " << event.name << endl;
                event.object = entry.value.get();
                event.entity = getChildEntity(entry.value.get());
                event.value = entry.value;

                impl->childWatches.trigger(event);
            }

            std::shared_ptr<Value> result = entry.value;

            // Now it's no longer visible we can get rid of it
            if (entry.underConstruction)
                entry.underConstruction->cancel();

            if (entry.value)
                entry.value.reset();

            return result;
        }

        // RCU failed because something raced us to update.  Try again.
    }
}

template<typename Key, class Value>
std::pair<std::shared_ptr<Value>,
          std::shared_ptr<typename RestCollection<Key, Value>::BackgroundTask> >
RestCollection<Key, Value>::
getEntry(Key key) const
{
    auto v = tryGetEntry(key);

    if (v.first || v.second)
        return v;

    Utf8String error_message;
    if (restEncode(key).empty()) {
        error_message = MLDB::format("Collection URLs do not contain a trailing slash: try /v1/%1$s instead of /v1/%1$s/",
                                   this->nounPlural.rawData());
        throw HttpReturnException(404, error_message);
    }
    else
        throwEntryDoesntExist(key);
}

template<typename Key, class Value>
std::pair<std::shared_ptr<Value>,
          std::shared_ptr<typename RestCollection<Key, Value>::BackgroundTask> >
RestCollection<Key, Value>::
tryGetEntry(Key key) const
{
    // NOTE: Should not be necessary... investigation needed
    GcLock::SharedGuard guard(impl->entriesLock);

    auto es = impl->entries.getImmutable();

    auto it = es->find(key);
    if (it != es->end())
        return make_pair(it->second.value, it->second.underConstruction);

    return { nullptr, nullptr };
}

template<typename Key, class Value>
std::shared_ptr<Value>
RestCollection<Key, Value>::
getExistingEntry(Key key) const
{
    // NOTE: Should not be necessary... investigation needed
    GcLock::SharedGuard guard(impl->entriesLock);

    auto es = impl->entries.getImmutable();

    auto it = es->find(key);
    if (it != es->end() && it->second.value)
        return it->second.value;

    if (it != es->end())
        this->throwEntryNotReady(key);
    else
        this->throwEntryDoesntExist(key);
}

template<typename Key, class Value>
std::shared_ptr<Value>
RestCollection<Key, Value>::
tryGetExistingEntry(Key key) const
{
    // NOTE: Should not be necessary... investigation needed
    GcLock::SharedGuard guard(impl->entriesLock);

    auto es = impl->entries.getImmutable();

    auto it = es->find(key);
    if (it != es->end())
        return it->second.value;

    return nullptr;
}

template<typename Key, class Value>
RestRequestMatchResult
RestCollection<Key, Value>::
handleGetExistingValue(Key key,
                       const Value & value,
                       RestConnection & connection,
                       const RestRequest & request,
                       const RestRequestParsingContext & context) const
{
    connection.sendResponse(200, getExistingValue(key, value));
    return RestRequestRouter::MR_YES;
}

template<typename Key, class Value>
Json::Value
RestCollection<Key, Value>::
getExistingValue(Key key,
                 const Value & value) const
{
    Json::Value result;
    result["state"] = "ok";
    return result;
}


template<typename Key, class Value>
bool
RestCollection<Key, Value>::
forEachEntry(const std::function<bool (Key key, Value & value)> & fn)
{
    // NOTE: Should not be necessary... investigation needed
    GcLock::SharedGuard guard(impl->entriesLock);

    auto es = impl->entries.getImmutable();

    size_t sz = es->size();

    for (auto & e: *es) {
        ExcAssertEqual(es->size(), sz);
        if (e.second.value && !fn(e.first, *e.second.value))
            return false;
    }

    ExcAssertEqual(es->size(), sz);

    return true;
}

template<typename Key, class Value>
bool
RestCollection<Key, Value>::
forEachEntry(const std::function<bool (Key key, const Value & value)> & fn) const
{
    // NOTE: Should not be necessary... investigation needed
    GcLock::SharedGuard guard(impl->entriesLock);

    auto es = impl->entries.getImmutable();
    for (auto & e: *es) {
        if (e.second.value && !fn(e.first, *e.second.value))
            return false;
    }

    return true;
}

template<typename Key, class Value>
WatchT<RestEntityChildEvent>
RestCollection<Key, Value>::
watchChildren(const Utf8String & spec, bool catchUp, Any info)
{
    using namespace std;
    return transform<RestEntityChildEvent>
        (watchElements(spec, catchUp, std::move(info)),
         [] (const ChildEvent & event) {  return event; });

    //return transform<RestEntityChildEvent>
    //    (watchElements(spec, catchUp));
}

template<typename Key, class Value>
WatchT<Utf8String>
RestCollection<Key, Value>::
watchNames(const Utf8String & spec, bool catchUp, Any info)
{
    using namespace std;
    return transform<Utf8String>
        (watchElements(spec, catchUp, std::move(info)),
         [] (const ChildEvent & event) -> Utf8String
         {
             if (event.event == CE_NEW)
                 return "+" + event.name;
             else if (event.event == CE_DELETED)
                 return "-" + event.name;
             else return "unknown " + event.name;
         });
}

template<typename Key, class Value>
WatchT<typename RestCollection<Key, Value>::ChildEvent>
RestCollection<Key, Value>::
watchElements(const Utf8String & spec, bool catchUp, Any info)
{
    using namespace std;
    //cerr << "restCollection watchElements spec = " << spec
    //     << " entity " << MLDB::type_name(*this) << " " << catchUp
    //     << " info " << jsonEncodeStr(info) << endl;

    // Filter that controls which events are seen by this watch
    auto filter = [=] (const ChildEvent & event) -> bool
        {
            //cerr << "filtering " << event.name << " with filter "
            //     << spec << endl;
            if (spec == "*")
                return true;
            return event.name == spec;
        };

    if (!catchUp)
        return impl->childWatches.add(std::move(info), filter);

    // Take an exclusive lock on mutation so that nothing can interleave
    // events with our catchup events.
    std::unique_lock<typename Impl::MutateMutex> mutateGuard(impl->mutateMutex);

    // We create this after we take the lock so that nothing can squeeze
    // in an event between when it was created and when it was caught up
    auto res = impl->childWatches.add(std::move(info), filter);

    // NOTE: the || true here is due to watching for the non-existance of an
    // entry causing problems.  We need a better way to ask for this.
    if (spec == "*" || true) {

        GcLock::SharedGuard guard(impl->entriesLock);
        auto es = impl->entries.getImmutable();

        for (auto & e: *es) {
            if (e.second.value) {
                ChildEvent event;
                event.event = CE_NEW;
                event.parent = event.parentCollection = this;
                event.key = e.first;
                event.name = restEncode(e.first);
                event.object = e.second.value.get();
                event.entity = getChildEntity(e.second.value.get());
                event.value = e.second.value;
                using namespace std;
                //cerr << "catching up ChildEvent " << event.name << endl;
                res.trigger(event);
            }
        }
    }
    else {
        // A single entry.  We specifically identify if it is present or not.
        Key key = restDecode(spec, (Key *)0);
        auto val = tryGetExistingEntry(key);

        ChildEvent event;
        event.parent = event.parentCollection = this;
        event.key = key;
        event.name = spec;

        if (val) {
            event.event = CE_NEW;
            event.object = val.get();
            event.entity = getChildEntity(val.get());
            event.value = val;
        }
        else {
            event.event = CE_DELETED;
        }

        res.trigger(event);
    }
    return res;
}

template<typename Key, class Value>
Watch
RestCollection<Key, Value>::
watchChannel(const Utf8String & channel,
             const Utf8String & filter,
             bool catchUp, Any info)
{
    if (channel == "children")
        return watchChildren(filter, catchUp, std::move(info));
    else if (channel == "elements")
        return watchElements(filter, catchUp, std::move(info));
    else if (channel == "names")
        return watchNames(filter, catchUp, std::move(info));
    else throw HttpReturnException(400,
                                   MLDB::format("type %s doesn't have channel named '%s'",
                                              MLDB::type_name(*this).c_str(),
                                              channel.rawData()));
}

template<typename Key, class Value>
std::vector<Any>
RestCollection<Key, Value>::
getChildWatches() const
{
    std::vector<Any> result;
    for (auto & w: impl->childWatches.active())
        result.push_back(w.second);
    return result;
}

// A pair of classes that allow getWatchBoundType() to be synthesized for
// any type which has a getBoundWatchTypeGeneric static method, using the
// sfinae pattern.

// This one for when there is no getBoundWatchTypeGeneric method...
template<typename T, typename Enable = void>
struct ChildHasGenericWatchType : public std::false_type {
    static std::pair<const std::type_info *,
                     std::shared_ptr<const ValueDescription> >
    get(const ResourceSpec & spec)
    {
        throw HttpReturnException(400,
                                  MLDB::format("need to override getWatchBoundType() or implement "
                                             "getWatchBoundTypeGeneric() "
                                             "for type %s to know about value watch types",
                                             MLDB::type_name<T>().c_str()));
    }
};

// Specialization for when there is a getBoundWatchTypeGeneric method
template<typename T>
struct ChildHasGenericWatchType<T, typename std::enable_if<std::is_function<decltype(T::getWatchBoundTypeGeneric)>::value>::type> : public std::true_type {
    static std::pair<const std::type_info *,
                     std::shared_ptr<const ValueDescription> >
    get(const ResourceSpec & spec)
    {
        return T::getWatchBoundTypeGeneric(spec);
    }
};

template<typename Key, class Value>
std::pair<const std::type_info *,
          std::shared_ptr<const ValueDescription> >
RestCollection<Key, Value>::
getWatchBoundType(const ResourceSpec & spec)
{
    if (spec.empty())
        throw HttpReturnException(400, "no type for empty spec");
    else if (spec.size() > 1) {
        if (spec[0].channel == "children") {
            ResourceSpec childSpec(spec.begin() + 1, spec.end());
            return ChildHasGenericWatchType<Value>::get(childSpec);
        }
        else throw HttpReturnException(400,
                                       MLDB::format("type %s doesn't have channel named '%s'",
                                                  MLDB::type_name(*this).c_str(),
                                                  spec[0].channel.rawData()));
    }
    else if (spec[0].channel == "children")
        return make_pair(&typeid(std::tuple<RestEntityChildEvent>), nullptr);
    else if (spec[0].channel == "elements")
        return make_pair(&typeid(std::tuple<ChildEvent>), nullptr);
    else if (spec[0].channel == "names")
        return make_pair(&typeid(std::tuple<Utf8String>), nullptr);
    else throw HttpReturnException(400,
                                   MLDB::format("type %s doesn't have channel named '%s'",
                                              MLDB::type_name(*this).c_str(),
                                              spec[0].channel.rawData()));
}

template<typename Key, class Value>
std::shared_ptr<EntityLinkToken>
RestCollection<Key, Value>::
acceptLink(const std::vector<Utf8String> & sourcePath,
           const std::vector<Utf8String> & targetPath,
           const std::string & linkType,
           Any linkParams)
{
    if (targetPath.empty())
        throw HttpReturnException(400, "RestCollection cannot accept links");

    std::vector<Utf8String> path = targetPath;
    Utf8String element = path.front();
    path.erase(path.begin());

    Key key = restDecode(element, (Key *)0);

    auto val = this->tryGetExistingEntry(key);
    if (!val)
        this->throwEntryDoesntExist(key);

    auto el = getChildEntity(val.get());
    if (!el)
        throw HttpReturnException(400, "Collection elements are not RestEntities");

    return el->acceptLink(sourcePath, path, linkType, linkParams);
}


/*****************************************************************************/
/* REST CONFIGURABLE COLLECTION                                              */
/*****************************************************************************/

template<typename Key, typename Value,
         typename Config, typename Status>
RestConfigurableCollection<Key, Value, Config, Status>::
RestConfigurableCollection(const Utf8String & nounSingular,
                           const Utf8String & nounPlural,
                           RestEntity * parent)
    : Base(nounSingular, nounPlural, parent),
      childWatchActive(false), backgroundCreate(true)
{
    this->childWatch = this->watchElements("*", true /* catchUp */,
                                           Utf8String("internal child watch"));
    childWatch.bind(std::bind(&RestConfigurableCollection::onChildChange,
                              this,
                              std::placeholders::_1));
}

template<typename Key, typename Value,
         typename Config, typename Status>
RestConfigurableCollection<Key, Value, Config, Status>::
~RestConfigurableCollection()
{
    this->shutdown();
}

template<typename Key, typename Value,
         typename Config, typename Status>
Key
RestConfigurableCollection<Key, Value, Config, Status>::
obtainAsync(Config config,
            const OnProgress & onProgress,
            const OnDone & onDone)
{
    Key key = getKey(config);

    auto value = this->tryGetExistingEntry(key);
    if (value) {
        onDone(value);
        return key;
    }

    // If it's key we know about then reload the full config to make sure we can
    // properly construct it.
    if (configStore) {
        auto restKey = restEncode(key);
        for (const auto& configKey : configStore->keys()) {
            if (restKey == configKey) {
                config = jsonDecode<Config>(configStore->get(restKey));
                break;
            }
        }
    }

    auto fn = std::bind(
            &RestConfigurableCollection::construct,
            this, config, std::placeholders::_1);

    this->addBackgroundJobInThread(key, fn, onProgress, onDone, false /* must be new */, config);

    return key;
}

template<typename Key, typename Value,
         typename Config, typename Status>
std::shared_ptr<Value>
RestConfigurableCollection<Key, Value, Config, Status>::
obtainSync(Config config,
           const OnProgress & onProgress)
{
    using namespace std;
    std::mutex done;
    done.lock();

    std::shared_ptr<Value> result;

    auto onDone = [&] (std::shared_ptr<Value> entry)
        {
            //cerr << "obtainSync done for " << jsonEncodeStr(config) << endl;
            result = std::move(entry);
            done.unlock();
        };

    //cerr << "obtainSync config = " << jsonEncode(config) << " this = " << this
    //     << endl;

    auto key = obtainAsync(config, onProgress, onDone);

    done.lock();

    //using namespace std;
    //cerr << "obtainAsync for " << restEncode(key) << " returned" << endl;

    return result;
}

template<typename Key, typename Value,
         typename Config, typename Status>
std::shared_ptr<Value>
RestConfigurableCollection<Key, Value, Config, Status>::
mustObtainSync(Config config,
               const OnProgress & onProgress)
{
    auto key = getKey(config);
    auto result = obtainSync(std::move(config), onProgress);
    if (!result) {
        auto entry = this->getEntry(key);
        if (entry.second) {
            if (entry.second->exc)
                std::rethrow_exception(entry.second->exc);
        }

        this->throwEntryDoesntExist(key);
    }

    return result;
}

template<typename Key, typename Value,
         typename Config, typename Status>
std::shared_ptr<Value>
RestConfigurableCollection<Key, Value, Config, Status>::
mustCreateSync(Config config,
               const OnProgress & onProgress,
               bool overwrite /* = false*/)
{
    auto key = getKey(config);

    // TODO: this causes a deadlock, but according to my mental model is
    // required to avoid a race condition.  Need to revisit; it has been
    // stress tested for a week without any errors with serializability or
    // deadlocks, once the lock below was added.  We may have some
    // opportunity here to improve performance by better understanding
    // what is going on, but it's been tested enough to have a reasonable
    // probability that it's correct and it's certainly not a performance
    // bottleneck under any observed situation.
    //std::unique_lock<typename Base::Impl::MutateMutex>
    //    mutateGuard(this->impl->mutateMutex);

    {
        // NOTE: Should not be necessary... investigation needed.  See
        // the comment above.
        GcLock::SharedGuard guard(this->impl->entriesLock);

        auto es = this->impl->entries.getImmutable();

        if (es->count(key)) {
            if (!overwrite)
                this->throwEntryAlreadyExists(key);
            this->deleteEntry(key);
        }
    }

    auto result = obtainSync(std::move(config), onProgress);
    if (!result) {
        auto entry = this->getEntry(key);
        if (entry.second) {
            if (entry.second->exc)
                std::rethrow_exception(entry.second->exc);
        }

        this->throwEntryNotObtained(key);
    }

    return result;
}

template<typename Key, typename Value,
         typename Config, typename Status>
std::shared_ptr<Value>
RestConfigurableCollection<Key, Value, Config, Status>::
constructCancellable(Config config,
                     const OnProgress & onProgress,
                     WatchT<bool> cancelled) const
{
    // NOTE: if you get a "pure virtual function call" here,
    // it's because you didn't call shutdown() from the destructor
    // of the most derived class in your hierarchy.
    return construct(config, onProgress);
}

template<typename Key, typename Value,
         typename Config, typename Status>
std::shared_ptr<Value>
RestConfigurableCollection<Key, Value, Config, Status>::
construct(Config config,
          const OnProgress & onProgress) const
{
    // This should ONLY be called when we are destroying a
    // collection with items just added into it.
    return nullptr;
}

template<typename Key, typename Value,
         typename Config, typename Status>
RestRequestMatchResult
RestConfigurableCollection<Key, Value, Config, Status>::
handleGetValue(Key key,
               RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)
{
    connection.sendResponse(200, jsonEncodeStr(getStatus(key)),
                            "application/json");
    return RestRequestRouter::MR_YES;
}

template<typename Key, typename Value,
         typename Config, typename Status>
Status
RestConfigurableCollection<Key, Value, Config, Status>::
getStatus(Key key) const
{
    auto entries = this->getEntry(key);

    if (entries.second)
        return getStatusLoading(key, *entries.second);
    else return getStatusFinished(key, *entries.first);
}

template<typename Key, typename Value,
         typename Config, typename Status>
Status
RestConfigurableCollection<Key, Value, Config, Status>::
getStatusLoading(Key key, const BackgroundTask & task) const
{
    throw HttpReturnException(400, "getStatusLoading needs to be overridden");
}

// in rest_collection.cc.  Checks that the payload isn't empty and throws
// an appropriate exception if it is.
void validatePayloadForPut(const RestRequest & req,
                           const Utf8String & nounPlural);

template<typename Key, typename Value,
         typename Config, typename Status>
void
RestConfigurableCollection<Key, Value, Config, Status>::RouteManager::
addPutRoute()
{
    ExcAssert(this->getKey);

    Json::Value help;
    help["result"] = this->nounSingular + " status after creation";

    Json::Value & v = help["jsonParams"];
    Json::Value & v2 = v[0];
    v2["description"] = "Configuration of new " + this->nounSingular;
    v2["cppType"] = MLDB::type_name<Config>();
    v2["encoding"] = "JSON";
    v2["location"] = "Request Body";

    auto validater = createRequestValidater(help, {});


    RestRequestRouter::OnProcessRequest putAsyncRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               const RestRequestParsingContext & cxt)
        {
            try {
                if (!validater(connection, req, cxt))
                    return RestRequestRouter::MR_ERROR;

                validatePayloadForPut(req, this->nounPlural);

                auto collection = getCollection(cxt);
                Key key = this->getKey(cxt);

                MLDB_TRACE_EXCEPTIONS(false);
                auto config = jsonDecodeStr<Config>(req.payload);
                Status status = collection->handlePut(key, config);

                ResourcePath path = collection->getPath();
                path.push_back(encodeUriComponent(restEncode(key)));
                Utf8String uri = collection->getUriForPath(path);

                RestParams headers = {
                    { "Location", uri, },
                    { "EntityPath", jsonEncodeStr(path) }
                };

                connection.sendHttpResponse(202, jsonEncodeStr(status),
                                            "application/json", headers);

                return RestRequestRouter::MR_YES;
            } catch (const std::exception & exc) {
                return sendExceptionResponse(connection, exc);
            }
        };

    this->valueNode->addRoute("", { "PUT", "header:async=true" },
                              "Create a new " + this->nounSingular + " asynchronously",
                              putAsyncRoute, help);

    RestRequestRouter::OnProcessRequest putSyncRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               const RestRequestParsingContext & cxt)
        {
            try {
                if (!validater(connection, req, cxt))
                    return RestRequestRouter::MR_ERROR;

                auto collection = getCollection(cxt);
                Key key = this->getKey(cxt);

                validatePayloadForPut(req, this->nounPlural);

                MLDB_TRACE_EXCEPTIONS(false);
                auto config = jsonDecodeStr<Config>(req.payload);
                Status status = collection->handlePutSync(key, config);

                ResourcePath path = collection->getPath();
                path.push_back(encodeUriComponent(restEncode(key)));
                Utf8String uri = collection->getUriForPath(path);

                RestParams headers = {
                    { "Location", uri, },
                    { "EntityPath", jsonEncodeStr(path) }
                };

                connection.sendHttpResponse(201, jsonEncodeStr(status),
                                            "application/json", headers);

                return RestRequestRouter::MR_YES;
            } catch (const std::exception & exc) {
                return sendExceptionResponse(connection, exc);
            }
        };

    this->valueNode->addRoute("", { "PUT" },
                              "Create a new " + this->nounSingular
                              + " and wait for it to appear",
                              putSyncRoute, help);
}

// in rest_collection.cc.  Checks that the payload isn't empty and throws
// an appropriate exception if it is.
void validatePayloadForPost(const RestRequest & req,
                            const Utf8String & nounPlural);

template<typename Key, typename Value,
         typename Config, typename Status>
void
RestConfigurableCollection<Key, Value, Config, Status>::RouteManager::
addPostRoute()
{
    Json::Value help;
    help["result"] = this->nounSingular + " status after creation";

    Json::Value & v = help["jsonParams"];
    Json::Value & v2 = v[0];
    v2["description"] = "Configuration of new " + this->nounSingular;
    v2["cppType"] = MLDB::type_name<Config>();
    v2["encoding"] = "JSON";
    v2["location"] = "Request Body";

    auto validater = createRequestValidater(help, {});

    RestRequestRouter::OnProcessRequest postSyncRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               const RestRequestParsingContext & cxt)
        {
            try {
                if (!validater(connection, req, cxt))
                    return RestRequestRouter::MR_ERROR;

                validatePayloadForPost(req, this->nounPlural);

                auto collection = getCollection(cxt);

                MLDB_TRACE_EXCEPTIONS(false);
                auto config = jsonDecodeStr<Config>(req.payload);
                Key key = collection->getKey(config);
                Status status = collection->handlePostSync(key, config);

                ResourcePath path = collection->getPath();
                path.push_back(encodeUriComponent(restEncode(key)));
                Utf8String uri = collection->getUriForPath(path);

                RestParams headers = {
                    { "Location", uri, },
                    { "EntityPath", jsonEncodeStr(path) }
                };

                connection.sendHttpResponse(201, jsonEncodeStr(status),
                                            "application/json", headers);

                return RestRequestRouter::MR_YES;
            } catch (const std::exception & exc) {
                return sendExceptionResponse(connection, exc);
            }
        };

    RestRequestRouter::OnProcessRequest postAsyncRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               const RestRequestParsingContext & cxt)
        {
            try {
                if (!validater(connection, req, cxt))
                    return RestRequestRouter::MR_ERROR;

                validatePayloadForPost(req, this->nounPlural);

                auto collection = getCollection(cxt);

                MLDB_TRACE_EXCEPTIONS(false);
                auto config = jsonDecodeStr<Config>(req.payload);
                Key key = collection->getKey(config);
                Status status = collection->handlePost(key, config);

                ResourcePath path = collection->getPath();
                path.push_back(encodeUriComponent(restEncode(key)));
                Utf8String uri = collection->getUriForPath(path);

                RestParams headers = {
                    { "Location", uri, },
                    { "EntityPath", jsonEncodeStr(path) }
                };

                connection.sendHttpResponse(201, jsonEncodeStr(status),
                                            "application/json", headers);

                return RestRequestRouter::MR_YES;
            } catch (const std::exception & exc) {
                return sendExceptionResponse(connection, exc);
            }
        };

    this->collectionNode->addRoute("", { "POST", "header:async=true" },
                                   "Create a new " + this->nounSingular,
                                   postAsyncRoute, help);

    this->collectionNode->addRoute("", { "POST" },
                                   "Create a new " + this->nounSingular
                                   + " and wait for it to appear",
                                   postSyncRoute, help);
}

template<typename Key, typename Value,
         typename Config, typename Status>
void
RestConfigurableCollection<Key, Value, Config, Status>::RouteManager::
addDeleteRoute()
{
    Json::Value help;
    help["description"] = "Delete the given " + this->nounSingular;

    auto validater = createRequestValidater(help, {});

    RestRequestRouter::OnProcessRequest deleteRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               const RestRequestParsingContext & cxt)
        {
            try {
                if (!validater(connection, req, cxt))
                    return RestRequestRouter::MR_ERROR;

                auto collection = getCollection(cxt);
                auto key = this->getKey(cxt);

                MLDB_TRACE_EXCEPTIONS(false);
                collection->handleDelete(key);

                connection.sendHttpResponse(204, "", "", {});

                return RestRequestRouter::MR_YES;
            } catch (const std::exception & exc) {
                return sendExceptionResponse(connection, exc);
            }
        };


    this->valueNode->addRoute("", { "DELETE" },
                              "Delete the " + this->nounSingular,
                              deleteRoute, help);
}

template<typename Key, typename Value,
         typename Config, typename Status>
void
RestConfigurableCollection<Key, Value, Config, Status>::
attachConfig(std::shared_ptr<CollectionConfigStore> configStore)
{
    this->configStore = configStore;
}

template<typename Key, typename Value,
         typename Config, typename Status>
void
RestConfigurableCollection<Key, Value, Config, Status>::
loadConfig()
{
    if (!configStore) return;

    for (const auto & key_config: configStore->getAll()) {
        Key key = restDecode(key_config.first, (Key *)0);
        Config config = jsonDecode<Config>(key_config.second);
        handlePut(key, config, false);
    }
}

template<typename Key, typename Value,
         typename Config, typename Status>
bool
RestConfigurableCollection<Key, Value, Config, Status>::
objectIsPersistent(const Key & key, const Config & config) const
{
    return true;
}


template<typename Key, typename Value,
         typename Config, typename Status>
bool
RestConfigurableCollection<Key, Value, Config, Status>::
handlePutItl(Key key, Config config,  const OnDone & onDone, bool mustBeNew)
{
   setKey(config, key);

    auto savedConfig = jsonEncode(config);

    if (backgroundCreate) {
        auto fn = std::bind(
                &RestConfigurableCollection::constructCancellable,
                this, std::move(config), std::placeholders::_1, std::placeholders::_2);
        this->addBackgroundJobInThread(key, fn, nullptr, onDone, mustBeNew, savedConfig);
    }
    else {
        WatchT<bool> cancelled;
        std::atomic<BackgroundTaskBase::State> state(BackgroundTaskBase::State::FINISHED);
        this->addEntryItl(key, constructCancellable(std::move(config), nullptr,
                                                    std::move(cancelled)),
                          true /* must add */,
                          state);
    }

    bool isPersistent = objectIsPersistent(key, config);

    if (isPersistent) {
        if (this->configStore) {
            this->configStore->set(restEncode(key), savedConfig);
        }
    }

    return backgroundCreate;
}

template<typename Key, typename Value,
         typename Config, typename Status>
Status
RestConfigurableCollection<Key, Value, Config, Status>::
handlePut(Key key, Config config, bool mustBeNew /* = false */)
{
    handlePutItl(key, config, nullptr, mustBeNew);
    return getStatus(key);
}


template<typename Key, typename Value,
         typename Config, typename Status>
Status
RestConfigurableCollection<Key, Value, Config, Status>::
handlePutSync(Key key, Config config, bool mustBeNew /* = false */)
{
    std::shared_ptr<std::promise<bool> > done(new std::promise<bool>());
    if (!handlePutItl(key,
                      config,
                      [=] (std::shared_ptr<Value> val)
                      {
                          done->set_value(!!val);
                      },
                      mustBeNew))
        done->set_value(true);

    std::future<bool> future = done->get_future();

    bool success = future.get();

    if (success) {
        return getStatus(key);
    }
    else {
        auto entry = this->getEntry(key);
        if (entry.second) {
            if (entry.second->exc)
                std::rethrow_exception(entry.second->exc);
        }

        this->throwEntryNotObtained(key);
    }
}

template<typename Key, typename Value,
         typename Config, typename Status>
void
RestConfigurableCollection<Key, Value, Config, Status>::
handleDelete(Key key)
{
    if (this->deleteEntry(key)) {
        if (this->configStore)
            this->configStore->erase(restEncode(key));
    }
}

template<typename Key, typename Value,
         typename Config, typename Status>
Status
RestConfigurableCollection<Key, Value, Config, Status>::
handlePost(Key key, Config config, bool mustBeNew)
{
    return handlePut(key, std::move(config), mustBeNew);
}

template<typename Key, typename Value,
         typename Config, typename Status>
Status
RestConfigurableCollection<Key, Value, Config, Status>::
handlePostSync(Key key, Config config, bool mustBeNew)
{
    return handlePutSync(key, std::move(config), mustBeNew);
}

template<typename Key, typename Value,
         typename Config, typename Status>
void
RestConfigurableCollection<Key, Value, Config, Status>::
ensureChildWatchPresent()
{
    childWatchActive = true;
}

template<typename Key, typename Value,
         typename Config, typename Status>
void
RestConfigurableCollection<Key, Value, Config, Status>::
onChildChange(const ChildEvent & event)
{
    if (!childWatchActive)
        return;

    using namespace std;
    cerr << "got child event for "
         << jsonEncode(this->getConfig(event.key, *event.value))
         << endl;
    if (event.event == CE_NEW) {
        if (!configWatches.empty())
            configWatches.trigger(event.key,
                                  this->getConfig(event.key, *event.value));
        if (!statusWatches.empty())
            statusWatches.trigger(event.key, std::make_shared<Status>(std::move(this->getStatus(event.key))));

    }
    else if (event.event == CE_DELETED) {
        if (!configWatches.empty())
            configWatches.trigger(event.key, nullptr);
    }
    else throw HttpReturnException(400, "unknown child watch");
}

template<typename Key, typename Value,
         typename Config, typename Status>
WatchT<Key, std::shared_ptr<Config> >
RestConfigurableCollection<Key, Value, Config, Status>::
watchConfig(const Utf8String & spec, bool catchUp, Any info)
{
    using namespace std;

    // Make sure the value description is there
    auto desc = getDefaultDescriptionShared((std::tuple<std::shared_ptr<Config> > *)0);

    ensureChildWatchPresent();

    // Filter that controls which events are seen by this watch
    auto filter = [=] (const Key & key, const std::shared_ptr<Config> & value) -> bool
        {
            cerr << "configWatch filtering with spec " << spec << " key "
                 << restEncode(key) << endl;
            if (spec == "*")
                return true;
            return restEncode(key) == spec;
        };

    if (!catchUp)
        return configWatches.add(std::move(info), filter);

    // Take an exclusive lock on mutation so that nothing can interleave
    // events with our catchup events.
    std::unique_lock<typename Base::Impl::MutateMutex> mutateGuard(this->impl->mutateMutex);

    // We create this after we take the lock so that nothing can squeeze
    // in an event between when it was created and when it was caught up
    auto res = configWatches.add(std::move(info), filter);

    GcLock::SharedGuard guard(this->impl->entriesLock);

    auto es = this->impl->entries.getImmutable();
    for (auto & e: *es) {
        if (e.second.value) {
            res.trigger(e.first, getConfig(e.first, *e.second.value));
        }
    }

    return res;
}

template<typename Key, typename Value,
         typename Config, typename Status>
WatchT<Key, std::shared_ptr<Status> >
RestConfigurableCollection<Key, Value, Config, Status>::
watchStatus(const Utf8String & spec, bool catchUp, Any info)
{
    using namespace std;

    // Make sure the value description is there
    auto desc = getDefaultDescriptionShared((std::tuple<std::shared_ptr<Status> > *)0);

    ensureChildWatchPresent();

    // Filter that controls which events are seen by this watch
    auto filter = [=] (const Key & key, const std::shared_ptr<Status> & value) -> bool
        {
            return true;
            cerr << "statusWatch filtering with spec " << spec << " key "
                 << restEncode(key) << endl;
            if (spec == "*")
                return true;
            return restEncode(key) == spec;
        };

    if (!catchUp)
        return statusWatches.add(std::move(info), filter);

    // Take an exclusive lock on mutation so that nothing can interleave
    // events with our catchup events.
    std::unique_lock<typename Base::Impl::MutateMutex> mutateGuard(this->impl->mutateMutex);

    // We create this after we take the lock so that nothing can squeeze
    // in an event between when it was created and when it was caught up
    auto res = statusWatches.add(std::move(info), filter);

    GcLock::SharedGuard guard(this->impl->entriesLock);

    auto es = this->impl->entries.getImmutable();
    for (auto & e: *es) {
        if (e.second.value) {
            res.trigger(e.first, std::make_shared<Status>(std::move(getStatus(e.first))));
        }
    }

    return res;
}

template<typename Key, typename Value,
         typename Config, typename Status>
Watch
RestConfigurableCollection<Key, Value, Config, Status>::
watchChannel(const Utf8String & channel,
             const Utf8String & filter,
             bool catchUp, Any info)
{
    if (channel == "config")
        return watchConfig(filter, catchUp, std::move(info));
    else if (channel == "status")
        return watchStatus(filter, catchUp, std::move(info));
    return Base::watchChannel(channel, filter, catchUp, std::move(info));
}

template<typename Key, typename Value,
         typename Config, typename Status>
WatchT<std::vector<Utf8String>, Any>
RestConfigurableCollection<Key, Value, Config, Status>::
watchWithPath(const ResourceSpec & spec, bool catchUp, Any info,
              const std::vector<Utf8String> & currentPath)
{
    using namespace std;
    if (spec.size() == 2
        && spec[0].channel == "children"
        && spec[1].channel == "state") {

        Utf8String filter = spec[1].filter;

        auto onStatus = [=] (Key key, const std::shared_ptr<Status> & status)
            {
                cerr << "onStatus " << jsonEncode(status) << endl;
                vector<Utf8String> path = currentPath;
                path.push_back(restEncode(key));

                string state = jsonEncode(status)["state"].asString();

                return std::make_tuple(path, Any(state));
            };

        return transform<vector<Utf8String>, Any>(this->watchStatus(spec[1].filter, true, info), onStatus);
    }
    return Base::watchWithPath(spec, catchUp, std::move(info), currentPath);
}

template<typename Key, typename Value,
         typename Config, typename Status>
std::pair<const std::type_info *,
          std::shared_ptr<const ValueDescription> >
RestConfigurableCollection<Key, Value, Config, Status>::
getWatchBoundType(const ResourceSpec & spec)
{
    //using namespace std;
    //cerr << "getWatchBoundType: spec " << jsonEncode(spec) << endl;
    if (spec.size() == 1) {
        if (spec[0].channel == "config")
            return std::make_pair(&typeid(std::tuple<Key, std::shared_ptr<Config> >), nullptr);
        else if (spec[0].channel == "status")
            return std::make_pair(&typeid(std::tuple<Key, std::shared_ptr<Status> >), nullptr);
    }
    else if (spec.size() == 2
             && spec[0].channel == "children"
             && spec[1].channel == "state") {
        return std::make_pair(&typeid(std::tuple<Utf8String>), nullptr);
    }
    return Base::getWatchBoundType(spec);
}

template<typename Key, typename Value,
         typename Config, typename Status>
std::shared_ptr<Config>
RestConfigurableCollection<Key, Value, Config, Status>::
getConfig(Key key, const Value & value) const
{
    throw HttpReturnException(400,
                              MLDB::format("type '%s' needs to override getConfig",
                                         MLDB::type_name(*this).c_str()));
}

} // namespace MLDB
