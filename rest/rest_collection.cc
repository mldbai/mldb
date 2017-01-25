// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* rest_collection.cc
   Jeremy Barnes, 23 March 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "rest_collection.h"
#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/arch/demangle.h"
#include "mldb/arch/backtrace.h"
#include "mldb/watch/watch_impl.h"
#include "mldb/http/http_exception.h"
#include <thread>
#include <chrono>
#include "mldb/types/structure_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/vector_description.h"


using namespace std;


namespace MLDB {

/*****************************************************************************/
/* VALUE DESCRIPTIONS                                                        */
/*****************************************************************************/

DEFINE_ENUM_DESCRIPTION(CollectionEvent);

CollectionEventDescription::
CollectionEventDescription()
{
    addValue("NEW", CE_NEW, "New value was added to the collection");
    addValue("DELETED", CE_DELETED, "Value was deleted from the collection");
    addValue("CONNECTED", CE_CONNECTED,
             "Value was discovered due to new connection");
    addValue("DISCONNECTED", CE_DISCONNECTED,
             "Value was lost due to lost connection");
}

DEFINE_STRUCTURE_DESCRIPTION(ResourceSpecEntry);

ResourceSpecEntryDescription::
ResourceSpecEntryDescription()
{
    addField("channel", &ResourceSpecEntry::channel,
             "Channel watched");
    addField("filter", &ResourceSpecEntry::filter,
             "Filter on the channel");
}

DEFINE_STRUCTURE_DESCRIPTION(EntityLinkStatus);

EntityLinkStatusDescription::
EntityLinkStatusDescription()
{
    addField("remoteAddress", &EntityLinkStatus::remoteAddress,
             "Address of remote end of link");
    addField("end", &EntityLinkStatus::end,
             "Which end of the link we have");
    addField("connectParam", &EntityLinkStatus::connectParam,
             "Parameters passed for connection");
    addField("acceptParam", &EntityLinkStatus::acceptParam,
             "Parameters returned on acceptance");
    addField("state", &EntityLinkStatus::state,
             "State of link");
    addField("messagesSent", &EntityLinkStatus::messagesSent,
             "Number of messages sent");
    addField("messagesReceived", &EntityLinkStatus::messagesReceived,
             "Number of messages received");
}

/*****************************************************************************/
/* ENTITY LINK TOKEN                                                         */
/*****************************************************************************/

EntityLinkStatus
EntityLinkToken::
getStatus() const
{
    EntityLinkStatus result;
    result.remoteAddress = getRemoteAddress();
    result.end = getEnd();
    result.connectParam = connectParams();
    result.acceptParam = acceptParams();
    result.state = getState();
    result.messagesSent = messagesSent();
    result.messagesReceived = messagesReceived();
    return result;
}


/*****************************************************************************/
/* REST ENTITY                                                               */
/*****************************************************************************/

struct RestEntityWatchData {
    std::mutex lock;
    Watches watches;
    WatchesT<std::vector<Utf8String>, Any> pathWatches;
    WatchT<RestEntityChildEvent> childWatch;
    std::map<RestEntity *, Watch> childEntityWatches;
    std::vector<Utf8String> currentPath;
    ResourceSpec childPath;
    bool catchUp;
    Any info;

    RestEntityWatchData(std::pair<const std::type_info *,
                                  std::shared_ptr<const ValueDescription> >
                        boundType)
        : watches(boundType.first, std::move(boundType.second))
    {
    }

    ~RestEntityWatchData()
    {
        //cerr << "**** Destroying RestEntityWatchData" << endl;
        //ML::backtrace();

        release();
    }

    void release(bool clearWatches = true)
    {
        std::unique_lock<std::mutex> guard(this->lock);

        watches.onRelease = nullptr;

        for (auto & w: childEntityWatches)
            w.second.detach();
 
        if (clearWatches) {
            watches.clear();
            pathWatches.clear();
        }
    }

    void onChild(const RestEntityChildEvent & child)
    {
        std::unique_lock<std::mutex> guard(this->lock);
            
        // The collection that has the child is not a collection of
        // RestEntity descendents, and so we don't know how to watch
        // it.  Instead, that collection should either a) have child
        // items that derive from RestEntity or b) implement watch()
        // directly.

        if (!child.entity)
            throw HttpReturnException(400, "generic RestEntity::watch() no good for "
                                      "non-RestElement collections");

        //cerr << "triggered onChild for " << child.name << endl;

        switch (child.event) {
        case CE_NEW: {
            // Get the watch for the channel of this object

            vector<Utf8String> fullPath = currentPath;
            fullPath.push_back(child.name);

            auto watch = child.entity
                ->watchWithPath(this->childPath, catchUp,
                                string("Internal REST Entity child watch"),
                                fullPath);

            // Function called when it triggers.  This simply passes it
            // on to the output watch.
            auto onVal = [&] (const std::vector<Utf8String> & path,
                              const Any & val)
                {
                    //cerr << "Triggered child watch" << endl;
                    this->watches.triggerGeneric(val);
                    this->pathWatches.trigger(path, val);
                };

            // Bind the trigger function so that the watch returned by the
            // main function will be triggered by this call
            watch.bind(onVal);

            ExcAssert(!this->childEntityWatches.count(child.entity));
            this->childEntityWatches[child.entity] = std::move(watch);

            break;
        }
        case CE_DELETED: {
            auto it = this->childEntityWatches.find(child.entity);
            if (it == this->childEntityWatches.end())
                throw HttpReturnException(400, "deleted entity wasn't being watched");
            this->childEntityWatches.erase(it);
                
            break;
        }
        case CE_UPDATED: {
            throw HttpReturnException(400, "not handling CE_UPDATED although we probably should");
            break;
        }
        case CE_CONNECTED:
            throw HttpReturnException(400, "not handling CE_CONNECTED");
        case CE_DISCONNECTED:
            throw HttpReturnException(400, "not handling CE_DISCONNECTED");
        }
    };
};
    

Watch
RestEntity::
watch(const ResourceSpec & spec,
      bool catchUp,
      Any info)
{
    auto w = watchWithPath(spec, catchUp, info, {});

    auto types = getWatchBoundType(spec);

    return transformGeneric(std::move(w),
                            [&] (const std::vector<Utf8String> & path,
                                 const Any & val)
                            {
                                return val;
                            },
                            types.first);
#if 0
    

    //cerr << "watching " << spec << " in entity "
    //     << MLDB::type_name(*this) << " with catchUp " << catchUp
    //     << endl;

    if (spec.empty())
        throw HttpReturnException(400, "cannot watch empty object");

    if (spec.size() == 1)
        return watchChannel(spec[0].channel, spec[0].filter, catchUp,
                            std::move(info));

    if (spec[0].channel != "children")
        throw HttpReturnException(400, "only children accepted for non-terminal levels");

    // TODO WARNING THIS MEMORY WILL NEVER BE FREED
    // Currently there is a circular reference in data: it contains an entry
    // pathWatch, which is bound to a function onDescendent, which contains
    // a reference to data.  Before using in production we will need to
    // resolve this circular reference.
    // NOTE: no longer true... should make sure
    auto data = std::make_shared<RestEntityWatchData>(getWatchBoundType(spec));
    data->childPath.insert(data->childPath.end(),
                           spec.begin() + 1, spec.end());
    data->catchUp = catchUp;
    data->info = info;

    // 1.  Watch the path, so that we know of any entities that come or
    //     go on the path.
    data->childWatch
        = std::move(watchChildren(spec[0].filter, true /* catchup */, info));

    ExcAssert(!data->childWatch.bound());

    // 2.  Return our watch
    auto res = data->watches.add(info);

    // 3.  When we get a new descendent, we need to watch its channel and
    //     add that to the watch.  Note that this bind has to happen AFTER
    //     the watch is added, as otherwise the catchup events will be
    //     swallowed.
    data->childWatch.bind(std::bind(&RestEntityWatchData::onChild,
                                    data.get(),
                                    std::placeholders::_1));

    // Make sure it's typed
    ExcAssert(res.boundType());

    // Get a bare pointer to avoid taking ownership
    //auto dataPtr = data.get();

    // 4.  When the watch is released, we need to unwind all of this
    auto onRelease = [=] (Watch & watch, Any && info)
        {
            //cerr << "CHILD WATCH RELEASED" << endl;
            data->release(false);
        };

    data->pathWatches.onRelease = onRelease;

    res.piggyback = data;
    return res;
#endif
}

/** Transform a watch to return its any with an extra path element. */
WatchT<std::vector<Utf8String>, Any>
addPath(Watch && w, const std::vector<Utf8String> & currentPath)
{
    return transform<std::vector<Utf8String>, Any>(std::move(w),
                     [=] (const Any & val) ->std::tuple<std::vector<Utf8String>, Any>
                     {
                         return std::make_tuple(currentPath, val);
                     });
}

WatchT<std::vector<Utf8String>, Any>
RestEntity::
watchWithPath(const ResourceSpec & spec,
              bool catchUp,
              Any info,
              const std::vector<Utf8String> & currentPath)
{
    //cerr << "watching " << spec << " in entity "
    //     << MLDB::type_name(*this) << " with catchUp " << catchUp
    //     << endl;

    if (spec.empty())
        throw HttpReturnException(400, "cannot watch empty object");

    if (spec.size() == 1)
        return addPath(watchChannel(spec[0].channel, spec[0].filter, catchUp,
                                    std::move(info)),
                       currentPath);

    if (spec[0].channel != "children")
        throw HttpReturnException(400, "only children accepted for non-terminal levels");

    // TODO WARNING THIS MEMORY WILL NEVER BE FREED
    // Currently there is a circular reference in data: it contains an entry
    // pathWatch, which is bound to a function onDescendent, which contains
    // a reference to data.  Before using in production we will need to
    // resolve this circular reference.
    // NOTE: no longer true... should make sure
    auto data = std::make_shared<RestEntityWatchData>(getWatchBoundType(spec));
    data->childPath.insert(data->childPath.end(),
                           spec.begin() + 1, spec.end());
    data->catchUp = catchUp;
    data->info = info;
    data->currentPath = currentPath;
    
    // 1.  Watch the path, so that we know of any entities that come or
    //     go on the path.
    data->childWatch
        = watchChildren(spec[0].filter, true /* catchup */, info);

    ExcAssert(!data->childWatch.bound());

    // 2.  Return our watch
    auto res = data->pathWatches.add(info);

    // 3.  When we get a new descendent, we need to watch its channel and
    //     add that to the watch.  Note that this bind has to happen AFTER
    //     the watch is added, as otherwise the catchup events will be
    //     swallowed.
    data->childWatch.bind(std::bind(&RestEntityWatchData::onChild,
                                    data.get(),
                                    std::placeholders::_1));

    // Make sure it's typed
    ExcAssert(res.boundType());

    // 4.  When the watch is released, we need to unwind all of this
    auto onRelease = [=] (Watch & watch, Any && info)
        {
            data->release(false);
        };

    data->pathWatches.onRelease = onRelease;

    res.piggyback = data;
    return res;
}

std::pair<const std::type_info *,
          std::shared_ptr<const ValueDescription> >
RestEntity::
getWatchBoundType(const ResourceSpec & spec)
{
    throw HttpReturnException(400, "entity does not support watching");
}

WatchT<RestEntityChildEvent>
RestEntity::
watchChildren(const Utf8String & spec, bool catchUp, Any info)
{
    throw HttpReturnException(400, "entity '" + MLDB::type_name(*this) + "' does not support watching children: spec " + spec);
}

Watch
RestEntity::
watchChannel(const Utf8String & channel,
             const Utf8String & filter,
             bool catchUp,
             Any info)
{
    throw HttpReturnException(400, "entity does not support watching channels");
}

WatchT<Date>
RestEntity::
getTimer(Date nextExpiry, double period, std::function<void (Date)> toBind)
{
    return getParent()->getTimer(nextExpiry, period, toBind);
}

std::vector<Utf8String>
RestEntity::
getPath() const
{
    if (getParent() == this)
        return {};
    auto result = getParent()->getPath();
    result.push_back(getName());
    return result;
}

Utf8String
RestEntity::
getUriForPath(ResourcePath path)
{
    if (getParent() == this)
        throw HttpReturnException(400, "need to override getUriForPath for base entity");
    return getParent()->getUriForPath(path);
}

std::shared_ptr<EntityLinkToken>
RestEntity::
createLink(const std::vector<Utf8String> & remotePath,
           const std::string & linkType,
           Any linkParams)
{
    return doCreateLink(this, remotePath, linkType, linkParams);
}

std::shared_ptr<EntityLinkToken>
RestEntity::
doCreateLink(RestEntity * forWho,
             const std::vector<Utf8String> & remotePath,
             const std::string & linkType,
             Any linkParams)
{
    auto p = getParent();
    if (!p || p == this)
        throw HttpReturnException(400, "doCreateLink needs to be overridden somewhere");

    return p->doCreateLink(forWho, remotePath, linkType, std::move(linkParams));
}

std::shared_ptr<EntityLinkToken>
RestEntity::
acceptLink(const std::vector<Utf8String> & sourcePath,
           const std::vector<Utf8String> & targetPath,
           const std::string & linkType,
           Any linkParams)
{
    throw HttpReturnException(400, "acceptLink needs to be overridden for class " + MLDB::type_name(*this));
}


/*****************************************************************************/
/* REST DIRECTORY                                                            */
/*****************************************************************************/

void
RestDirectory::
addEntity(Utf8String name, RestEntity & entity)
{
    entities[name].reset(new EntityEntry(ML::make_unowned_sp(entity)));
}

void
RestDirectory::
addEntity(Utf8String name, std::shared_ptr<RestEntity> entity)
{
    entities[name].reset(new EntityEntry(entity));
}

WatchT<RestEntityChildEvent>
RestDirectory::
watchChildren(const Utf8String & spec, bool catchUp, Any info)
{
    //cerr << "watchChildren for '" << spec << "' in directory" << endl;

    if (spec == "*")
        throw HttpReturnException(400, "wildcards not supported for a directory");

    if (!catchUp)
        throw HttpReturnException(400, "A watch on a directory entity with no catchup "
                            "is useless");

    auto it = entities.find(spec);
    if (it == entities.end())
        throw HttpReturnException(400, "child '" + spec + "' not found in RestDirectory");
    
    auto result = it->second->watches.add(info);

    RestEntityChildEvent child;
    child.parent = this;
    child.name = spec;
    child.entity = it->second->entity.get();
    child.event = CE_NEW;

    result.trigger(child);
    
    return result;
}

Watch
RestDirectory::
watchChannel(const Utf8String & channel, const Utf8String & filter,
             bool catchUp, Any info)
{
    if (channel == "children")
        return watchChildren(filter, catchUp, std::move(info));

    throw HttpReturnException(400, "RestDirectory::watchChannel(): "
                              "only children channel exists for a directory");
}

std::pair<const std::type_info *,
          std::shared_ptr<const ValueDescription> >
RestDirectory::
getWatchBoundType(const ResourceSpec & spec)
{
    if (spec.empty())
        throw HttpReturnException(400, "no type for empty spec");
    if (spec[0].channel != "children")
        throw HttpReturnException(400, "RestDirectory '" + MLDB::type_name(*this)
                                  + "' has only a children channel: "
                                  + jsonEncodeUtf8(spec));

    if (spec.size() == 1)
        return make_pair(&typeid(std::tuple<RestEntityChildEvent>), nullptr);

    auto it = entities.find(spec[0].filter);
    if (it == entities.end())
        throw HttpReturnException(400, "child '" + spec[0].filter + "' not found in RestDirectory");

    return it->second->entity
        ->getWatchBoundType(ResourceSpec(spec.begin() + 1, spec.end()));
}

std::shared_ptr<EntityLinkToken>
RestDirectory::
acceptLink(const std::vector<Utf8String> & sourcePath,
           const std::vector<Utf8String> & targetPath,
           const std::string & linkType,
           Any linkParams)
{
    //cerr << "RestDirectory acceptLink " << jsonEncodeStr(sourcePath)
    //     << " " << jsonEncodeStr(targetPath) << " " << linkType << endl;

    if (targetPath.empty())
        throw HttpReturnException(400, "no links accepted from a RestDirecory");

    vector<Utf8String> path = targetPath;
    const Utf8String & element = path.front();
    path.erase(path.begin());

    auto it = entities.find(element);
    if (it == entities.end())
        throw HttpReturnException(400, "child '" + element + "' not found in RestDirectory");
    
    return it->second->entity
        ->acceptLink(sourcePath, path, linkType, linkParams);
}

void
RestDirectory::
initNodes(RouteManager & manager)
{
    ExcAssert(manager.parentNode);
    ExcAssert(manager.getCollection);

    if (!manager.collectionNode) {
    
        RestRequestRouter & parentNode = *manager.parentNode;

        //int keyResourceIndex = manager.resourceElementsMatched;

        Utf8String nounSingular = manager.nounSingular;
        Utf8String nounPlural = manager.nounPlural;

        /* This is called after the entity name has been parsed.  It looks it up in the
           collection, puts it in the context, and causes an error if it didn't exist
           and we weren't doing a PUT.
        */
        auto getCollection = [=] (RestConnection & connection,
                                  const RestRequest & request,
                                  RestRequestParsingContext & context)
            {
                context.addObject(manager.getCollection(context));
            };
    
        RestRequestRouter & collectionNode
            = parentNode.addSubRouter("/" + nounPlural,
                                      "Operations on collection of " + nounPlural,
                                      getCollection);
    
        manager.collectionNode = &collectionNode;
    }
}

void
RestDirectory::
initRoutes(RouteManager & manager)
{
    initNodes(manager);

    ExcAssert(manager.parentNode);
    ExcAssert(manager.getCollection);

    const Utf8String & nounPlural = manager.nounPlural;

    RestRequestRouter & collectionNode = *manager.collectionNode;
    
    auto getCollectionRoute = [=] (RestConnection & connection,
                                   const RestRequest & req,
                                   const RestRequestParsingContext & cxt)
        {
            try {
                auto collection = manager.getCollection(cxt);

                MLDB_TRACE_EXCEPTIONS(false);
                auto managerStr = jsonEncodeStr(collection->getKeys());
                connection.sendHttpResponse(200, managerStr,
                                            "application/json", {});
                return RestRequestRouter::MR_YES;
            } catch (const std::exception & exc) {
                return sendExceptionResponse(connection, exc);
            }
        };

    Json::Value help;
    help["result"] = nounPlural + " in the collection";

    collectionNode.addRoute("", { "GET" },
                           "Get a list of " + nounPlural
                            + " in the collection",
                            getCollectionRoute, help);
    
}


/*****************************************************************************/
/* BACKGROUND TASK BASE                                                      */
/*****************************************************************************/

BackgroundTaskBase::
BackgroundTaskBase()
    : running(false), state(State::INITIALIZING)
{
}

BackgroundTaskBase::
~BackgroundTaskBase()
{
    if (running) {
        cancel();
    }
}

Json::Value
BackgroundTaskBase::
getProgress() const
{
    std::unique_lock<std::mutex> guard(mutex);
    return progress;
}

bool
BackgroundTaskBase::
cancel() noexcept
{
    auto old_state = state.exchange(State::CANCELLED);
    if (old_state != State::CANCELLED && old_state != State::FINISHED) {
        try {
            cancelledWatches.trigger(true);
        }
        catch (...) {
            std::terminate();
        }
    }
    // cerr << "state is now CANCELLED " << handle << endl;

#if 0
    // Give it one second to stop
    struct timespec timeToWait = { 1, 0 };
    int res = pthread_timedjoin_np(handle, nullptr, timeToWait);
    // check res...

    if (res == -1 && errno == ETIMEDOUT) {
        // Now cancel it
        res = pthread_cancel(handle, nullptr);
        // check res...

        res = pthread_timedjoin_np(handle, nullptr, timeToWait);
    }
#endif
            
    //thread->join();

    while (running) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    // note that the old state of the entity and its associated task
    // can be ERROR or event FINISHED meaning that the cancellation
    // could not occurred before the task encounter an error or was
    // completed
    return old_state != State::CANCELLED;
}

void
BackgroundTaskBase::
setError(std::exception_ptr exc)
{
    auto old_state = state.exchange(State::ERROR);
    // cerr << "state is now ERROR " << handle << endl;

    // the entity and the task to create it cannot be
    // in ERROR and FINISHED,  however, it can be in
    // CANCELLED state meaning that the error occurred
    // before the task could be cancelled
    ExcAssertNotEqual(old_state, State::FINISHED);
    this->exc = std::move(exc);
}

void
BackgroundTaskBase::
setFinished()
{
    running = false;
    State oldState = state.load();
    if (oldState != State::CANCELLED &&
        oldState != State::ERROR) {
        // cerr << "state is now FINISHED " << handle << " was " << oldState << endl;
        state = State::FINISHED;
    }
}

void
BackgroundTaskBase::
setProgress(const Json::Value & _progress)
{
    ExcAssert(running);
    
    State oldState = state.load();
    if (oldState != State::CANCELLED &&
        oldState != State::ERROR) {
        // cerr << "state is now EXECUTING " << handle << endl;
        state = State::EXECUTING;

        auto type = _progress.type();
        if (type == Json::nullValue  ||  
            type == Json::arrayValue  || 
            type == Json::objectValue)
            progress.clear();

        ExcAssert(type != Json::stringValue);

        if(!_progress.isNull()) {
            progress = _progress;
            for (auto & f: onProgressFunctions) {
                f(progress);
            }
        }
    }
}

Utf8String
BackgroundTaskBase::
getState() const
{
    switch (state.load()) {
    case State::CANCELLED:
        return L"cancelled";
    case State::ERROR:
        return L"error";
    case State::FINISHED:
        return L"finished";
    case State::INITIALIZING:
        return L"initializing";
    case State::EXECUTING:
        return L"executing";
    default:
        // you get this assert most likely because you have
        // added a new state and did not update this method
        ExcAssert(!"update the BackgroundTaskBase::getState method");
        return L"unknown state";
    }
}

void validatePayloadForPut(const RestRequest & req,
                           const Utf8String & nounPlural)
{
    if (req.payload.empty()) {
        MLDB_TRACE_EXCEPTIONS(false);
        throw HttpReturnException
            (400, "PUT to collection '" + nounPlural + "' with empty payload.  "
             "Pass a JSON body of your request containing the "
             "parameters of the entity to create.  If there really "
             "are no parameters, pass an empty JSON object {}.",
             "collection", nounPlural,
             "request", req);
    }
}

void validatePayloadForPost(const RestRequest & req,
                            const Utf8String & nounPlural)
{
    if (req.payload.empty()) {
        MLDB_TRACE_EXCEPTIONS(false);
        throw HttpReturnException
            (400, "POST to collection '" + nounPlural + "' with empty payload.  "
             "Pass a JSON body of your request containing the "
             "parameters of the entity to create.",
             "collection", nounPlural,
             "request", req);
    }
}

} // namespace MLDB
