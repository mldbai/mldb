/** poly_collection.h                                              -*- C++ -*-
    Jeremy Barnes, 22 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Collection of polymorphic objects.
*/

#include "mldb/types/any.h"
#include "poly_entity.h"
#include "mldb/rest/rest_collection.h"

#pragma once

namespace MLDB {

struct RestDirectory;

/*****************************************************************************/
/* POLYMORPHIC COLLECTION                                                    */
/*****************************************************************************/

/** Base class for a polymorphoic collection of objects all derived from
    PolyEntity.
*/

struct PolyCollectionBase
    : public RestConfigurableCollection<Utf8String, PolyEntity, PolyConfig,
                                        PolyStatus> {

    PolyCollectionBase(const Utf8String & nounSingular,
                       const Utf8String & nounPlural,
                       RestDirectory * server);
    virtual ~PolyCollectionBase();
    
    RestDirectory * server;
        
    static void initRoutes(RouteManager & manager);

    void init(std::shared_ptr<CollectionConfigStore> config);

    virtual Utf8String getKey(PolyConfig & config);

    virtual void setKey(PolyConfig & config, Utf8String key);

    virtual bool
    objectIsPersistent(const Utf8String & key, const PolyConfig & config) const;

    virtual PolyStatus
    getStatusLoading(Utf8String key, const BackgroundTask & task) const;

    virtual std::shared_ptr<PolyConfig>
    getConfig(Utf8String key, const PolyEntity & value) const;

    virtual PolyStatus
    getStatusFinished(Utf8String key, const PolyEntity & value) const = 0;

    virtual std::shared_ptr<PolyEntity>
    construct(PolyConfig config, const OnProgress & onProgress) const = 0;

};


/*****************************************************************************/
/* POLYMORPHIC COLLECTION                                                    */
/*****************************************************************************/

template<typename Entity>
struct PolyCollection: public PolyCollectionBase {

    typedef Entity EntityT;

    PolyCollection(const Utf8String & nounSingular,
                   const Utf8String & nounPlural,
                   RestDirectory * server);

    virtual ~PolyCollection();

    virtual std::shared_ptr<PolyConfig>
    getConfig(Utf8String key, const PolyEntity & value) const;

    virtual std::shared_ptr<PolyEntity>
    construct(PolyConfig config, const OnProgress & onProgress) const;

    virtual PolyStatus
    getStatusFinished(Utf8String key, const PolyEntity & value) const;

    virtual Any getEntityStatus(const Entity & entity) const;

    std::shared_ptr<Entity>
    obtainEntitySync(PolyConfig config,
                     const OnProgress & onProgress);

    std::shared_ptr<Entity>
    createEntitySync(PolyConfig config,
                     const OnProgress & onProgress, bool overwrite = false);

    std::shared_ptr<Entity>
    getExistingEntity(const Utf8String & key) const;

    /** Try to get an entity, but return nullptr rather than throwing if
        it's not found.
    */
    std::shared_ptr<Entity>
    tryGetExistingEntity(const Utf8String & key) const;

private:
    /*************************************************************************/
    /* REGISTRY                                                              */
    /*************************************************************************/

    struct Registry;
    static Registry & getRegistry();
public:

    typedef std::function<Entity * (RestDirectory *,
                                    PolyConfig,
                                    const std::function<bool (const Json::Value)> &)>
    CreateEntity;

    static std::shared_ptr<Entity>
    doConstruct(RestDirectory * owner, PolyConfig config,
                const OnProgress & onProgress);

    static std::shared_ptr<EntityType<Entity> >
    registerType(const Package & package,
                 const Utf8String & name,
                 const Utf8String & description,
                 CreateEntity createEntity,
                 TypeCustomRouteHandler docRoute,
                 TypeCustomRouteHandler customRoute = nullptr,
                 std::shared_ptr<const ValueDescription> config = nullptr,
                 std::set<std::string> registryFlags = {});

    template<typename T>
    static std::shared_ptr<EntityType<Entity> >
    registerType(const Package & package,
                 const Utf8String & name,
                 const Utf8String & description,
                 TypeCustomRouteHandler docRoute,
                 TypeCustomRouteHandler customRoute = nullptr,
                 std::shared_ptr<const ValueDescription> config = nullptr,
                 std::set<std::string> registryFlags = {})
    {
        return registerType(package, name, description,
                            [] (RestDirectory * peer,
                                PolyConfig config,
                                const OnProgress & onProgress)
                            { return new T(Entity::getOwner(peer), config, onProgress); },
                            docRoute,
                            customRoute,
                            config,
                            registryFlags);
    }

    template<typename T, typename Config>
    struct RegisterType {
        RegisterType(const Package & package,
                     const Utf8String & name,
                     const Utf8String & description,
                     TypeCustomRouteHandler docRoute,
                     TypeCustomRouteHandler customRoute = nullptr,
                     std::set<std::string> registryFlags = {})
        {
            handle = PolyCollection<Entity>::registerType<T>
                (package, name, description, docRoute, customRoute,
                 getDefaultDescriptionSharedT<Config>(), registryFlags);
        }

        std::shared_ptr<EntityType<Entity> > handle;
    };

    template<typename T>
    struct RegisterType<T, void> {
        RegisterType(const Package & package,
                     const Utf8String & name,
                     const Utf8String & description,
                     TypeCustomRouteHandler docRoute,
                     TypeCustomRouteHandler customRoute = nullptr,
                     std::set<std::string> registryFlags = {})
        {
            PolyCollection<Entity>
                ::registerType<T>(package, name, description,
                                  docRoute, customRoute,
                                  nullptr /* config description */,
                                  registryFlags);
        }
    };

    static RestRequestMatchResult
    handleDocRequest(RestDirectory * server,
                     const Utf8String & type,
                     RestConnection & connection,
                     const RestRequest & req,
                     const RestRequestParsingContext & cxt);
                     
    /** Return information about the given type. */
    static Json::Value getTypeInfo(const Utf8String & nounPlural,
                                   const Utf8String & type);
    
    static RestRequestMatchResult
    handleInfoRequest(RestDirectory * server,
                      const Utf8String & nounPlural,
                      const Utf8String & type,
                      RestConnection & connection,
                      const RestRequest & req,
                      const RestRequestParsingContext & cxt);

    static RestRequestMatchResult
    handleCustomRequest(RestDirectory * server,
                        const Utf8String & type,
                        RestConnection & connection,
                        const RestRequest & req,
                        const RestRequestParsingContext & cxt);
                     
    static std::vector<Utf8String> getRegisteredTypes();

    /** Return a watch that will be triggered whenever a new type is added.

        If catchUp is true, each of the existing types will be sent to the
        watch.
    */
    static WatchT<Utf8String> watchTypes(bool catchUp);
};

DECLARE_REST_COLLECTION_INSTANTIATIONS(Utf8String, PolyEntity, PolyConfig, PolyStatus);

} // namespace MLDB
