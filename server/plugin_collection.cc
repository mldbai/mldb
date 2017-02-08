// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** plugin_collection.cc
    Jeremy Barnes, 24 November 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Collection of plugins.
*/

#include "mldb/server/plugin_collection.h"
#include "mldb/rest/poly_collection_impl.h"
#include "mldb/server/mldb_server.h"

using namespace std;



namespace MLDB {

std::shared_ptr<PluginCollection>
createPluginCollection(MldbServer * server, RestRouteManager & routeManager)
{
    return createCollection<PluginCollection>(2, L"plugin", L"plugins",
                                              server, routeManager);
}

std::shared_ptr<Plugin>
obtainPlugin(MldbServer * server,
             const PolyConfig & config,
             const MldbServer::OnProgress & onProgress)
{
    return server->plugins->obtainEntitySync(config, onProgress);
}

std::shared_ptr<Plugin>
createPlugin(MldbServer * server,
             const PolyConfig & config,
             const std::function<bool (const Json::Value & progress)> & onProgress)
{
    return server->plugins->createEntitySync(config, onProgress);
}

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
                        std::set<std::string> registryFlags)
{
    return PluginCollection::registerType(package, name, description, createEntity,
                                          docRoute, customRoute,
                                          config, registryFlags);
}

/*****************************************************************************/
/* PLUGIN COLLECTION                                                         */
/*****************************************************************************/

PluginCollection::
PluginCollection(MldbServer * server)
    : PolyCollection<Plugin>("plugin", "plugins", server)
{
}

void
PluginCollection::
initRoutes(RouteManager & manager)
{
    PolyCollection<Plugin>::initRoutes(manager);

    // Get the actual plugin we're asking for
    auto getPlugin = [=] (const RestRequestParsingContext & context)
        -> Plugin *
        {
            // Get the parent collection
            auto collection = manager.getCollection(context);

            // Get the key
            auto key = manager.getKey(context);

            // Look up the value
            auto plugin = static_cast< Plugin * > (collection->getExistingEntry(key).get());

            return plugin;
        };

    // Make the plugin handle a route
    RestRequestRouter::OnProcessRequest handlePluginRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               RestRequestParsingContext & cxt)
        {
            Plugin * plugin = getPlugin(cxt);
            auto key = manager.getKey(cxt);

            try {
                return plugin->handleRequest(connection, req, cxt);
            }
            catch (const HttpReturnException & exc) {
                return sendExceptionResponse(connection, exc);
            }
        };


    RestRequestRouter & subRouter
        = manager.valueNode->addSubRouter("/routes", "Plugin routes");

    subRouter.rootHandler = handlePluginRoute;


    // Make the plugin handle the version route
    RestRequestRouter::OnProcessRequest handleGetVersionRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               RestRequestParsingContext & cxt)
        {
            Plugin * plugin = getPlugin(cxt);
            auto key = manager.getKey(cxt);

            try {
                connection.sendResponse(200, jsonEncode(plugin->getVersion()));
                return RestRequestRouter::MR_YES;
            }
            catch (const HttpReturnException & exc) {
                return sendExceptionResponse(connection, exc);
            }
        };

    manager.valueNode->addRoute("/version", {"GET"},
                "Get current version of plugin",
                handleGetVersionRoute, Json::Value());


    RestRequestRouter::OnProcessRequest handleGetDocumentationRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               RestRequestParsingContext & cxt)
        {
            Plugin * plugin = getPlugin(cxt);
            auto key = manager.getKey(cxt);

            try {
                return plugin->handleDocumentationRoute(connection, req, cxt);
            }
            catch (const HttpReturnException & exc) {
                return sendExceptionResponse(connection, exc);
            }
        };

    manager.valueNode->addRoute(Rx("/doc/(.*)", "<resource>"), {"GET"},
                "Get documentation of instantiated plugin",
                handleGetDocumentationRoute, Json::Value());

    RestRequestRouter::OnProcessRequest handleStaticRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               RestRequestParsingContext & cxt)
        {
            Plugin * plugin = getPlugin(cxt);
            auto key = manager.getKey(cxt);

            try {
                return plugin->handleStaticRoute(connection, req, cxt);
            }
            catch (const HttpReturnException & exc) {
                return sendExceptionResponse(connection, exc);
            }
        };

    manager.valueNode->addRoute(Rx("/static/(.*)", "<resource>"), {"GET"},
                                "Get static files of of instantiated plugin",
                                handleStaticRoute, Json::Value());
}

Any
PluginCollection::
getEntityStatus(const Plugin & plugin) const
{
    return plugin.getStatus();
}

template class PolyCollection<Plugin>;

} // namespace MLDB



