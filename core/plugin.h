/** plugin.h                                                       -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Interface for plugins into MLDB.
*/

#include "mldb/types/value_description_fwd.h"
#include "mldb/core/mldb_entity.h"
#include <set>

// NOTE TO MLDB DEVELOPERS: This is an API header file.  No includes
// should be added, especially value_description.h.


#pragma once



struct RestRequest;
struct RestConnection;
struct RestRequestParsingContext;

namespace MLDB {

struct MldbEngine;
struct Plugin;

typedef EntityType<Plugin> PluginType;


/*****************************************************************************/
/* PLUGIN                                                                    */
/*****************************************************************************/

struct Plugin: MldbEntity {
    Plugin(MldbEngine * engine);

    virtual ~Plugin();

    MldbEngine * engine;
    
    virtual std::string getKind() const
    {
        return "plugin";
    }

    virtual Any getStatus() const;
    
    virtual Any getVersion() const;

    /** Method to overwrite to handle a request.  By default, the plugin
        will return that it can't handle any requests.
    */
    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

    /** Method to respond to a route under /v1/plugins/xxx/doc, which
        should serve up the documentation.  Default implementation
        says no documentation is available.
    */
    virtual RestRequestMatchResult
    handleDocumentationRoute(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

    /** Method to respond to a route under /v1/plugins/xxx/static, which
        should serve up static resources for the plugin.  Default implementation
        says no static resources are available.
    */
    virtual RestRequestMatchResult
    handleStaticRoute(RestConnection & connection,
                      const RestRequest & request,
                      RestRequestParsingContext & context) const;
};


/*****************************************************************************/
/* UTILITIES                                                                 */
/*****************************************************************************/

std::shared_ptr<Plugin>
obtainPlugin(MldbEngine * engine,
             const PolyConfig & config,
             const std::function<bool (const Json::Value & progress)> & onProgress
                 = nullptr);

std::shared_ptr<Plugin>
createPlugin(MldbEngine * engine,
             const PolyConfig & config,
             const std::function<bool (const Json::Value & progress)> & onProgress
                 = nullptr);

std::shared_ptr<PluginType>
registerPluginType(const Package & package,
                   const Utf8String & name,
                   const Utf8String & description,
                   std::function<Plugin * (RestDirectory *,
                                           PolyConfig,
                                           const std::function<bool (const Json::Value)> &)>
                   createEntity,
                   TypeCustomRouteHandler docRoute,
                   TypeCustomRouteHandler customRoute,
                   std::shared_ptr<const ValueDescription> config,
                   std::set<std::string> registryFlags);

/** Register a new plugin kind.  This takes care of registering everything behind
    the scenes.
*/
template<typename PluginT, typename Config>
std::shared_ptr<PluginType>
registerPluginType(const Package & package,
                        const Utf8String & name,
                        const Utf8String & description,
                        const Utf8String & docRoute,
                        TypeCustomRouteHandler customRoute = nullptr,
                        std::set<std::string> flags = {})
{
    return registerPluginType
        (package, name, description,
         [] (RestDirectory * server,
             PolyConfig config,
             const std::function<bool (const Json::Value)> & onProgress)
         {
             std::shared_ptr<spdlog::logger> logger = MLDB::getMldbLog<PluginT>();
             auto plugin = new PluginT(PluginT::getOwner(server), config, onProgress);
             plugin->logger = std::move(logger); // noexcept
             return plugin;
         },
         makeInternalDocRedirect(package, docRoute),
         customRoute,
         getDefaultDescriptionSharedT<Config>(),
         flags);
}

template<typename PluginT, typename Config>
struct RegisterPluginType {
    RegisterPluginType(const Package & package,
                       const Utf8String & name,
                       const Utf8String & description,
                       const Utf8String & docRoute,
                       TypeCustomRouteHandler customRoute = nullptr,
                       std::set<std::string> registryFlags = {})
    {
        handle = registerPluginType<PluginT, Config>
            (package, name, description, docRoute, customRoute,
             registryFlags);
    }

    std::shared_ptr<PluginType> handle;
};

} // namespace MLDB

