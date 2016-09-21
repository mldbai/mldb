/** sensor_collection.cc
    Jeremy Barnes, 24 November 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Collection of sensors.
*/

#include "mldb/server/sensor_collection.h"
#include "mldb/rest/poly_collection_impl.h"
#include "mldb/server/mldb_server.h"

using namespace std;


namespace Datacratic {
namespace MLDB {

std::shared_ptr<SensorCollection>
createSensorCollection(MldbServer * server, RestRouteManager & routeManager)
{
    return createCollection<SensorCollection>(2, L"sensor", L"sensors",
                                              server, routeManager);
}

std::shared_ptr<Sensor>
obtainSensor(MldbServer * server,
             const PolyConfig & config,
             const MldbServer::OnProgress & onProgress)
{
    return server->sensors->obtainEntitySync(config, onProgress);
}

std::shared_ptr<Sensor>
createSensor(MldbServer * server,
             const PolyConfig & config,
             const std::function<bool (const Json::Value & progress)> & onProgress)
{
    return server->sensors->createEntitySync(config, onProgress);
}

std::shared_ptr<SensorType>
registerSensorType(const Package & package,
                        const Utf8String & name,
                        const Utf8String & description,
                        std::function<Sensor * (RestDirectory *,
                                                PolyConfig,
                                                const std::function<bool (const Json::Value)> &)>
                        createEntity,
                        TypeCustomRouteHandler docRoute,
                        TypeCustomRouteHandler customRoute,
                        std::shared_ptr<const ValueDescription> config,
                        std::set<std::string> registryFlags)
{
    return SensorCollection::registerType(package, name, description, createEntity,
                                          docRoute, customRoute,
                                          config, registryFlags);
}

/*****************************************************************************/
/* SENSOR COLLECTION                                                         */
/*****************************************************************************/

SensorCollection::
SensorCollection(MldbServer * server)
    : PolyCollection<Sensor>("sensor", "sensors", server)
{
}

void
SensorCollection::
initRoutes(RouteManager & manager)
{
    PolyCollection<Sensor>::initRoutes(manager);

    // Get the actual sensor we're asking for
    auto getSensor = [=] (const RestRequestParsingContext & context)
        -> Sensor *
        {
            // Get the parent collection
            auto collection = manager.getCollection(context);

            // Get the key
            auto key = manager.getKey(context);

            // Look up the value
            auto sensor = static_cast< Sensor * > (collection->getExistingEntry(key).get());

            return sensor;
        };

    // Make the sensor handle a route
    RestRequestRouter::OnProcessRequest handleSensorRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               RestRequestParsingContext & cxt)
        {
            Sensor * sensor = getSensor(cxt);
            auto key = manager.getKey(cxt);

            try {
                return sensor->handleRequest(connection, req, cxt);
            }
            catch (const HttpReturnException & exc) {
                return sendExceptionResponse(connection, exc);
            }
        };


    RestRequestRouter & subRouter
        = manager.valueNode->addSubRouter("/routes", "Sensor routes");

    subRouter.rootHandler = handleSensorRoute;


    // Make the sensor handle the version route
    RestRequestRouter::OnProcessRequest handleGetVersionRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               RestRequestParsingContext & cxt)
        {
            Sensor * sensor = getSensor(cxt);
            auto key = manager.getKey(cxt);

            try {
                connection.sendResponse(200, jsonEncode(sensor->getVersion()));
                return RestRequestRouter::MR_YES;
            }
            catch (const HttpReturnException & exc) {
                return sendExceptionResponse(connection, exc);
            }
        };

    manager.valueNode->addRoute("/version", {"GET"},
                "Get current version of sensor",
                handleGetVersionRoute, Json::Value());


    RestRequestRouter::OnProcessRequest handleGetDocumentationRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               RestRequestParsingContext & cxt)
        {
            Sensor * sensor = getSensor(cxt);
            auto key = manager.getKey(cxt);

            try {
                return sensor->handleDocumentationRoute(connection, req, cxt);
            }
            catch (const HttpReturnException & exc) {
                return sendExceptionResponse(connection, exc);
            }
        };

    manager.valueNode->addRoute(Rx("/doc/(.*)", "<resource>"), {"GET"},
                "Get documentation of instantiated sensor",
                handleGetDocumentationRoute, Json::Value());

    RestRequestRouter::OnProcessRequest handleStaticRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               RestRequestParsingContext & cxt)
        {
            Sensor * sensor = getSensor(cxt);
            auto key = manager.getKey(cxt);

            try {
                return sensor->handleStaticRoute(connection, req, cxt);
            }
            catch (const HttpReturnException & exc) {
                return sendExceptionResponse(connection, exc);
            }
        };

    manager.valueNode->addRoute(Rx("/static/(.*)", "<resource>"), {"GET"},
                                "Get static files of of instantiated sensor",
                                handleStaticRoute, Json::Value());
}

Any
SensorCollection::
getEntityStatus(const Sensor & sensor) const
{
    return sensor.getStatus();
}

} // namespace MLDB

template class PolyCollection<MLDB::Sensor>;

} // namespace Datacratic
