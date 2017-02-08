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

struct MldbServer;
struct Plugin;

typedef EntityType<Plugin> PluginType;


/*****************************************************************************/
/* PLUGIN                                                                    */
/*****************************************************************************/

struct Plugin: MldbEntity {
    Plugin(MldbServer * server);

    virtual ~Plugin();

    MldbServer * server;
    
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
/* SHARED LIBRARY PLUGIN                                                     */
/*****************************************************************************/

/** Plugin that operates by exposing itself as a shared libary. */

struct SharedLibraryConfig {
    std::string address;        ///< Directory for the plugin
    std::string library;        ///< Library to load
    std::string doc;            ///< Documentation path to be served static
    std::string staticAssets;   ///< Path to static assets to be served
    std::string version;        ///< Version of plugin
    std::string apiVersion;     ///< Version of the API we require
    bool allowInsecureLoading;  ///< Must be set to true
};

DECLARE_STRUCTURE_DESCRIPTION(SharedLibraryConfig);

struct SharedLibraryPlugin: public Plugin {
    SharedLibraryPlugin(MldbServer * server,
                        PolyConfig config,
                        std::function<bool (const Json::Value & progress)> onProgress);

    ~SharedLibraryPlugin();

    virtual Any getStatus() const;

    virtual Any getVersion() const;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

    virtual RestRequestMatchResult
    handleDocumentationRoute(RestConnection & connection,
                             const RestRequest & request,
                             RestRequestParsingContext & context) const;

    virtual RestRequestMatchResult
    handleStaticRoute(RestConnection & connection,
                      const RestRequest & request,
                      RestRequestParsingContext & context) const;

    // Entry point.  It will attempt to call this when initializing
    // the plugin.  It is OK for the function to return a null pointer;
    // if not the given object will be used for the status, version
    // and request calls and for the documentation and static route calls
    // if no directory is configured in the configuration.
    typedef Plugin * (*MldbPluginEnterV100) (MldbServer * server);
private:
    struct Itl;

    std::unique_ptr<Itl> itl;
};


/*****************************************************************************/
/* UTILITIES                                                                 */
/*****************************************************************************/

std::shared_ptr<Plugin>
obtainPlugin(MldbServer * server,
             const PolyConfig & config,
             const std::function<bool (const Json::Value & progress)> & onProgress
                 = nullptr);

std::shared_ptr<Plugin>
createPlugin(MldbServer * server,
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

