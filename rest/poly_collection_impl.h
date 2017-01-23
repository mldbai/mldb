/** poly_collection_impl.h                                         -*- C++ -*-
    Jeremy Barnes, 22 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Implementation of polymorphic collections.
*/

#pragma once

#include "poly_collection.h"
#include "mldb/types/meta_value_description_impl.h"
#include "mldb/types/pointer_description.h"

namespace MLDB {

/*****************************************************************************/
/* POLYMORPHIC COLLECTION                                                    */
/*****************************************************************************/

template<typename Entity>
PolyCollection<Entity>::
PolyCollection(const Utf8String & nounSingular,
               const Utf8String & nounPlural,
               RestDirectory * server)
    : PolyCollectionBase(nounSingular, nounPlural, server)
{
}

template<typename Entity>
PolyCollection<Entity>::
~PolyCollection()
{
    this->shutdown();
}

template<typename Entity>
std::shared_ptr<PolyConfig>
PolyCollection<Entity>::
getConfig(Utf8String key, const PolyEntity & value) const
{
    return value.config_;
}

template<typename Entity>
PolyStatus
PolyCollection<Entity>::
getStatusFinished(Utf8String key, const PolyEntity & value) const
{
    PolyStatus result;
    result.id = key;
    result.state = "ok";
    result.config = getConfig(key, value);
    result.status = getEntityStatus(static_cast<const Entity &>(value));
    result.type = value.type_;
    return result;
}

#if 0
template<typename T, typename Q = void>
struct HasGetStatusSwitch: public std::false_type {
};

template<typename T>
struct HasGetStatusSwitch<T, decltype(((const T *)0)->getStatus())>
    : public std::true_type {
};

template<typename Entity>
Any getEntityStatusSwitch(const Entity & entity,
                          typename std::enable_if<HasGetStatusSwitch<Entity>::value>::type * = 0)
{
    using namespace std;
    cerr << "*** getStatus with method" << endl;
    return entity.getStatus();
}

template<typename Entity>
Any getEntityStatusSwitch(const Entity & entity,
                          typename std::enable_if<!HasGetStatusSwitch<Entity>::value>::type * = 0)
{
    using namespace std;
    cerr << "*** getStatus without method" << endl;
    cerr << "value = " << HasGetStatusSwitch<Entity>::value << endl;
    return Any();
}
#endif


template<typename Entity>
Any
PolyCollection<Entity>::
getEntityStatus(const Entity & entity) const
{
    //return getEntityStatusSwitch(entity);
    return Any();
}

template<typename Entity>
std::shared_ptr<PolyEntity>
PolyCollection<Entity>::
construct(PolyConfig config, const OnProgress & onProgress) const
{
    if (config.type.empty()) {
        throw HttpReturnException(400, "Attempt to refer to nonexistant " + nounSingular
                                  + " with id " + config.id,
                                  config);
    }

    Utf8String typeSaved = config.type;
    
    auto result = doConstruct(server, config, onProgress);
    if (result->type_.empty())
        result->type_ = typeSaved;

    if (!result->config_)
        result->config_.reset(new PolyConfig(std::move(config)));
    return result;
}

template<typename Entity>
std::shared_ptr<Entity>
PolyCollection<Entity>::
obtainEntitySync(PolyConfig config,
                 const OnProgress & onProgress)
{
    if (config.type.empty() && config.id.empty())
        throw HttpReturnException(400, "Attempt to obtain " + nounSingular
                                  + " without setting 'id' (to refer to existing) "
                                  + "or 'type' and 'params' (to create new)",
                                  "entityKind", nounSingular,
                                  "configPassedIn", config);
    return std::static_pointer_cast<Entity>
        (PolyCollectionBase::mustObtainSync(config, onProgress));
}

template<typename Entity>
std::shared_ptr<Entity>
PolyCollection<Entity>::
createEntitySync(PolyConfig config,
                 const OnProgress & onProgress,
                 bool overwrite)
{
    if (config.type.empty())
        throw HttpReturnException(400, "Attempt to create " + nounSingular
                                  + " without type being set",
                                  "config", config);
    
    return std::static_pointer_cast<Entity>
        (PolyCollectionBase::mustCreateSync(config, onProgress, overwrite));
}

template<typename Entity>
std::shared_ptr<Entity>
PolyCollection<Entity>::
getExistingEntity(const Utf8String & key) const
{
    return std::static_pointer_cast<Entity>
        (PolyCollectionBase::getExistingEntry(key));
}

template<typename Entity>
std::shared_ptr<Entity>
PolyCollection<Entity>::
tryGetExistingEntity(const Utf8String & key) const
{
    return std::static_pointer_cast<Entity>
        (PolyCollectionBase::tryGetExistingEntry(key));
}

template<typename Entity>
struct PolyCollection<Entity>::Registry {
    mutable std::recursive_mutex mutex;

    struct Entry {
        Utf8String description;
        CreateEntity create;
        TypeCustomRouteHandler docRoute;
        TypeCustomRouteHandler customRoute;
        std::shared_ptr<const ValueDescription> configValueDescription;
        std::set<std::string> flags;
    };

    std::map<Utf8String, Entry> registry;
    WatchesT<Utf8String> watches;

    void insert(const Utf8String & name,
                const Utf8String & description,
                const CreateEntity & createEntity,
                TypeCustomRouteHandler docRoute,
                TypeCustomRouteHandler customRoute,
                std::shared_ptr<const ValueDescription> config,
                std::set<std::string> registryFlags)
    {
        std::unique_lock<std::recursive_mutex> guard(mutex);
        if (!registry.insert(std::make_pair(name, Entry{ description, createEntity, docRoute, customRoute, config, registryFlags })).second) {
            throw HttpReturnException(400, "double-registering type " + name);
        }
        watches.trigger(name);
    }
    
    const CreateEntity & lookup(const Utf8String & type)
    {
        std::unique_lock<std::recursive_mutex> guard(mutex);
        auto it = registry.find(type);
        if (it == registry.end())
            throw HttpReturnException(400, "couldn't find type '" + type
                                      + "' in registry");
        return it->second.create;
    }

    RestRequestMatchResult
    handleDocRequest(RestDirectory * server,
                     const Utf8String & type,
                     RestConnection & connection,
                     const RestRequest & req,
                     const RestRequestParsingContext & cxt)
    {
        std::unique_lock<std::recursive_mutex> guard(mutex);

        using namespace std;
        //cerr << "cxt.resources = " << cxt.resources << endl;

        auto it = registry.find(type);
        if (it == registry.end())
            throw HttpReturnException(400, "couldn't find type '"
                                      + type + "' in registry");
        guard.unlock();

        if (!it->second.docRoute) {
            connection.sendErrorResponse(404, "type " + type + " has no documentation registered");
            return RestRequestRouter::MR_YES;
        }
        return it->second.docRoute(server, connection, req, const_cast<RestRequestParsingContext &>(cxt));
    }

    Json::Value getTypeInfo(const Utf8String & nounPlural,
                            const Utf8String & type) const
    {
        try {
            std::unique_lock<std::recursive_mutex> guard(mutex);

            auto it = registry.find(type);
            if (it == registry.end())
                throw HttpReturnException(400, "couldn't find type '" + type
                                          + "' in registry");
            guard.unlock();

            Json::Value result;
        
            if (it->second.configValueDescription) {

                static ValueDescriptionConstPtrDescription desc(true /* detailed */);
                Json::Value configType;
                StructuredJsonPrintingContext context(configType);
                desc.printJson(&it->second.configValueDescription, context);
                result["configType"] = configType;
            }

            result["docRoute"] = "/v1/types/" + nounPlural + "/" + type + "/doc";
            result["description"] = it->second.description;
            result["flags"] = Json::Value(it->second.flags.begin(), it->second.flags.end());

            return result;
        } MLDB_CATCH_ALL {
            rethrowHttpException(500, "Error getting information for type '" + type
                                 + "' in collection " + nounPlural + ": "
                                 + getExceptionString(),
                                 "type", type,
                                 "nounPlural", nounPlural);
        }
    }

    RestRequestMatchResult
    handleInfoRequest(RestDirectory * server,
                      const Utf8String & nounPlural,
                      const Utf8String & type,
                      RestConnection & connection,
                      const RestRequest & req,
                      const RestRequestParsingContext & cxt)
    {
        Json::Value result = getTypeInfo(nounPlural, type);
        connection.sendResponse(200, result);
        return RestRequestRouter::MR_YES;
    }

    RestRequestMatchResult
    handleCustomRequest(RestDirectory * server,
                        const Utf8String & type,
                        RestConnection & connection,
                        const RestRequest & req,
                        const RestRequestParsingContext & cxt)
    {
        std::unique_lock<std::recursive_mutex> guard(mutex);
        
        using namespace std;
        //cerr << "cxt.resources = " << cxt.resources << endl;
        
        auto it = registry.find(type);
        if (it == registry.end())
            throw HttpReturnException(400, "couldn't find type '" + type
                                      + "' in registry");
        guard.unlock();

        if (!it->second.customRoute) {
            connection.sendErrorResponse(404, "type " + type + " has no custom route handler registered");
            return RestRequestRouter::MR_YES;
        }
        return it->second.customRoute(server, connection, req, const_cast<RestRequestParsingContext &>(cxt));
    }

    std::vector<Utf8String> keys()
    {
        std::unique_lock<std::recursive_mutex> guard(mutex);
        std::vector<Utf8String> result;
        for (auto & r: registry)
            result.push_back(r.first);
        return result;
    }

    WatchT<Utf8String> watch(bool catchUp)
    {
        std::unique_lock<std::recursive_mutex> guard(mutex);
        auto result = watches.add();
        if (catchUp) {
            for (auto & k: keys())
                result.trigger(k);
        }
        return std::move(result);
    }
};

template<typename Entity>
typename PolyCollection<Entity>::Registry &
PolyCollection<Entity>::
getRegistry()
{
    static Registry result;
    return result;
}

template<typename Entity>
std::shared_ptr<EntityType<Entity> >
PolyCollection<Entity>::
registerType(const Package & package,
             const Utf8String & name,
             const Utf8String & description,
             CreateEntity createEntity,
             TypeCustomRouteHandler docRoute,
             TypeCustomRouteHandler customRoute,
             std::shared_ptr<const ValueDescription> config,
             std::set<std::string> registryFlags)
{
    getRegistry().insert(name, description, createEntity, docRoute, customRoute, config, registryFlags);
    return nullptr;
}

template<typename Entity>
std::shared_ptr<Entity>
PolyCollection<Entity>::
doConstruct(RestDirectory * owner, PolyConfig config, const OnProgress & onProgress)
{
    return std::shared_ptr<Entity>(getRegistry().lookup(config.type)
                                   (owner, config, onProgress));
}

template<typename Entity>
std::vector<Utf8String>
PolyCollection<Entity>::
getRegisteredTypes()
{
    return getRegistry().keys();
}

template<typename Entity>
WatchT<Utf8String>
PolyCollection<Entity>::
watchTypes(bool catchUp)
{
    return getRegistry().watch(catchUp);
}

template<typename Entity>
RestRequestMatchResult
PolyCollection<Entity>::
handleDocRequest(RestDirectory * server,
                 const Utf8String & type,
                 RestConnection & connection,
                 const RestRequest & req,
                 const RestRequestParsingContext & cxt)
{
    return getRegistry().handleDocRequest(server, type, connection, req, cxt);
}

template<typename Entity>
RestRequestMatchResult
PolyCollection<Entity>::
handleInfoRequest(RestDirectory * server,
                  const Utf8String & nounPlural,
                  const Utf8String & type,
                  RestConnection & connection,
                  const RestRequest & req,
                  const RestRequestParsingContext & cxt)
{
    return getRegistry().handleInfoRequest(server, nounPlural, type,
                                           connection, req, cxt);
}

template<typename Entity>
Json::Value
PolyCollection<Entity>::
getTypeInfo(const Utf8String & nounPlural,
            const Utf8String & type)
{
    return getRegistry().getTypeInfo(nounPlural, type);
}

template<typename Entity>
RestRequestMatchResult
PolyCollection<Entity>::
handleCustomRequest(RestDirectory * server,
                    const Utf8String & type,
                    RestConnection & connection,
                    const RestRequest & req,
                    const RestRequestParsingContext & cxt)
{
    return getRegistry().handleCustomRequest(server, type, connection, req, cxt);
}

template<typename Collection>
std::shared_ptr<Collection>
createCollection(int numResourcesProducedByPathSpec,
                 const Utf8String & nounSingular,
                 const Utf8String & nounPlural,
                 RestDirectory * server, RestRouteManager & routeManager,
                 std::shared_ptr<CollectionConfigStore> configStore = nullptr)
{
    auto result = std::make_shared<Collection>(Collection::EntityT::getOwner(server));
    result->init(configStore);

    auto getCollection = [=] (const RestRequestParsingContext & context)
        {
            return result.get();
        };

    auto collectionRouteManager
        = std::make_shared<typename Collection::RouteManager>
        (routeManager, *routeManager.parentNode,
         routeManager.resourceElementsMatched + numResourcesProducedByPathSpec,
         getCollection, nounSingular, nounPlural);
    Collection::initRoutes(*collectionRouteManager);
    
    // Save our child route
    routeManager.childRoutes[nounPlural] = collectionRouteManager;

    server->addEntity(nounPlural, *result);
    return result;
}


} // namespace MLDB

