// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** procedure_collection.cc
    Jeremy Barnes, 24 November 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Collection of procedures.
*/

#include "mldb/types/value_description.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/rest/poly_collection_impl.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/procedure_collection.h"
#include "procedure_run_collection.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "types/any.h"
#include "types/any_impl.h"
#include <future>

using namespace std;


namespace MLDB {

std::shared_ptr<ProcedureCollection>
createProcedureCollection(MldbServer * server, RestRouteManager & routeManager)
{
    return createCollection<ProcedureCollection>(2, "procedure", "procedures",
                                                 server, routeManager);
}

std::shared_ptr<Procedure>
obtainProcedure(MldbServer * server,
                const PolyConfig & config,
                const MldbServer::OnProgress & onProgress)
{
    return server->procedures->obtainEntitySync(config, onProgress);
}

std::shared_ptr<Procedure>
createProcedure(MldbServer * server,
                const PolyConfig & config,
                const std::function<bool (const Json::Value & progress)> & onProgress,
                bool overwrite)
{
    return server->procedures->createEntitySync(config, onProgress, overwrite);
}

std::shared_ptr<ProcedureType>
registerProcedureType(const Package & package,
                      const Utf8String & name,
                      const Utf8String & description,
                      std::function<Procedure * (RestDirectory *,
                                                 PolyConfig,
                                                 const std::function<bool (const Json::Value)> &)>
                      createEntity,
                      TypeCustomRouteHandler docRoute,
                      TypeCustomRouteHandler customRoute,
                      std::shared_ptr<const ValueDescription> config,
                      std::set<std::string> registryFlags)
{
    return ProcedureCollection
        ::registerType(package, name, description, createEntity,
                       docRoute, customRoute, config, registryFlags);
}


/*****************************************************************************/
/* PROCEDURE COLLECTION                                                       */
/*****************************************************************************/

ProcedureCollection::
ProcedureCollection(MldbServer * server)
    : PolyCollection<Procedure>("procedure", "procedures", server),
      mldb(server)
{
}

void
ProcedureCollection::
initRoutes(RouteManager & manager)
{
    PolyCollection<Procedure>::initRoutes(manager);

    auto getRunCollection = [=] (const RestRequestParsingContext & context)
        -> ProcedureRunCollection *
        {
#if 1
            // Get the parent collection
            auto collection = manager.getCollection(context);

            // Get the key
            auto key = manager.getKey(context);

            // Look up the value
            auto procedure = static_cast<Procedure *>(collection->getExistingEntry(key).get());
#else
            auto procedure = static_cast<Procedure *>(&context.getObjectAs<PolyEntity>());
#endif
            return procedure->runs.get();
        };

    auto runManager
        = std::make_shared<ProcedureRunCollection::RouteManager>
        (manager, *manager.valueNode, manager.resourceElementsMatched + 3,
         getRunCollection, L"run", L"runs");
    ProcedureRunCollection::initRoutes(*runManager);

    manager.childRoutes["runs"] = runManager;

  RestRequestRouter::OnProcessRequest getLatestRun =
      [=] (RestConnection & connection, const RestRequest & req,
           const RestRequestParsingContext & context)
    {
        // load current procedure
        Utf8String redirect("/v1/procedures/");
        auto collection = manager.getCollection(context);
        auto key = manager.getKey(context);
        auto procedure = static_cast<Procedure *>(
            collection->getExistingEntry(key).get());
        redirect += encodeUriComponent(restEncode(key));

        // load all runs
        auto runs = procedure->runs.get();
        auto keys = runs->getKeys();
        if (keys.empty()) {
            throw HttpReturnException(404, "not found");
        }

        // Find the latest run
        Date latestRunDate = Date::negativeInfinity();
        Utf8String winningKey("");
        for (const auto & key: keys) {
            auto * currentProc = runs->getExistingEntry(key).get();
            if (latestRunDate < currentProc->runStarted) {
                latestRunDate = currentProc->runStarted;
                winningKey = key;
            }
        }
        redirect += "/runs/" + encodeUriComponent(restEncode(winningKey));
        connection.sendRedirect(307, redirect.rawString());
        return RestRequestRouter::MR_YES;
    };

    Json::Value help;
    auto & latestrun =
        manager.valueNode->addSubRouter("/latestrun", "");
    latestrun.addRoute("", { "GET" }, "Return latest run", getLatestRun, help);
}

Any
ProcedureCollection::
getEntityStatus(const Procedure & procedure) const
{
    return procedure.getStatus();
}


PolyStatus
ProcedureCollection::
handlePut(Utf8String key, PolyConfig config, bool mustBeNew)
{
    return handlePutWithFirstRun(key, config, mustBeNew, true);
}

PolyStatus
ProcedureCollection::
handlePutSync(Utf8String key, PolyConfig config, bool mustBeNew)
{
    return handlePutWithFirstRun(key, config, mustBeNew, false);
}


PolyStatus
ProcedureCollection::
handlePutWithFirstRun(Utf8String key, PolyConfig config, bool mustBeNew, bool async)
{
    // create the procedure
    PolyStatus polyStatus = PolyCollectionBase::handlePutSync(key, config, mustBeNew);

    // check if we need to make a first run
    auto procConfig = config.params.convert<ProcedureConfig>();

    if (procConfig.runOnCreation) {
        InProcessRestConnection connection;
        HttpHeader header;
        header.verb = "POST";
        header.resource = "/v1/procedures/" + key.rawString() + "/runs";
        header.headers["async"] = async ? "true" : "false";
        RestRequest request(header, "{}");
        mldb->handleRequest(connection, request);

        Json::Value runResponse;
        Json::Reader reader;
        if (!reader.parse(connection.response, runResponse, false)) {
            throw HttpReturnException(500, "failed to create the initial run",
                                      "entry", key,
                                      "runError", "could not parse the run response");
        }

        if (connection.responseCode == 201) {
            Json::Value status = polyStatus.status.asJson();
            if (!status.isObject()) {
                throw HttpReturnException(500,
                                          "Initial run did not return a valid object as status",
                                          "status", status);
            }
            status["firstRun"] = runResponse;
            polyStatus.status = status;
        }
        else {
            throw HttpReturnException(connection.responseCode, "failed to create the initial run",
                                      "entry", key,
                                      "runError", runResponse);
        }
    }

    return polyStatus;
}

template class PolyCollection<Procedure>;

} // namespace MLDB


