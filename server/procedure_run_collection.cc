// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** procedure_run_collection.cc
    Jeremy Barnes, 24 November 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Collection of procedure trainings.
*/

#include "procedure_run_collection.h"
#include "mldb/rest/rest_collection_impl.h"
#include "mldb/rest/service_peer.h"
#include "mldb/utils/json_utils.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/server/procedure_collection.h"


using namespace std;


namespace MLDB {


/*****************************************************************************/
/* PROCEDURE TRAINING COLLECTION                                              */
/*****************************************************************************/

ProcedureRunCollection::
ProcedureRunCollection(ServicePeer * server, Procedure * procedure)
    : RestConfigurableCollection<Utf8String, ProcedureRun,
                                 ProcedureRunConfig, ProcedureRunStatus>
      ("run", "runs", procedure),
      server(server),
      procedure(procedure)
{
}

void
ProcedureRunCollection::
initRoutes(RouteManager & manager)
{
    RestConfigurableCollection<Utf8String, ProcedureRun,
                               ProcedureRunConfig, ProcedureRunStatus>
        ::initRoutes(manager);

    ExcAssert(manager.getKey);

    manager.addPutRoute();
    manager.addPostRoute();
    manager.addDeleteRoute();

    RestRequestRouter::OnProcessRequest getRunState
        = [=] (RestConnection & connection,
               const RestRequest & req,
               const RestRequestParsingContext & cxt)
        {
            auto collection = manager.getCollection(cxt);
            Utf8String key = manager.getKey(cxt);
            
            ProcedureRunState runState;
            auto runEntry = collection->getEntry(key);
            if (runEntry.second) {
                runState.state = runEntry.second->getState();
            } else {
                runState.state = "finished";
            }
            
            connection.sendHttpResponse(200, jsonEncodeStr(runState),
                                        "application/json", RestParams());
            return RestRequestRouter::MR_YES;
        };

    RestRequestRouter::OnProcessRequest setRunState
        = [=] (RestConnection & connection,
               const RestRequest & req,
               const RestRequestParsingContext & cxt)
        {
            auto collection = manager.getCollection(cxt);
            Utf8String key = manager.getKey(cxt);
            
            MLDB_TRACE_EXCEPTIONS(false);
            auto config = jsonDecodeStr<ProcedureRunState>(req.payload);
            
            if (config.state == "cancelled") {
                auto runEntry = collection->getEntry(key);
                if (runEntry.second) {
                    runEntry.second->cancel();
                }
                
                ResourcePath path = collection->getPath();
                path.push_back(encodeUriComponent(restEncode(key)));
                Utf8String uri = collection->getUriForPath(path);
                
                RestParams headers = {
                    { "Location", uri, },
                    { "EntityPath", jsonEncodeStr(path) }
                };
                
                ProcedureRunStatus status;
                connection.sendHttpResponse(200, jsonEncodeStr(status),
                                            "application/json", headers);
                return RestRequestRouter::MR_YES;
            }
            return RestRequestRouter::MR_YES;
        };


    Json::Value help;
    help["result"] = manager.nounSingular + " status after creation";

    Json::Value & v = help["jsonParams"];
    Json::Value & v2 = v[0];
    v2["description"] = "Configuration of new " + manager.nounSingular;
    v2["cppType"] = type_name<ProcedureRunConfig>();
    v2["encoding"] = "JSON";
    v2["location"] = "Request Body";
    
    std::function<Procedure * (const RestRequestParsingContext & cxt) > getProcedure = [=] (const RestRequestParsingContext & cxt)
        -> Procedure *
        {
            return static_cast<Procedure *>(cxt.getSharedPtrAs<PolyEntity>(2).get());
        };

    std::function<ProcedureRun * (const RestRequestParsingContext & cxt) > getRun = [] (const RestRequestParsingContext & cxt)
        -> ProcedureRun *
        {
            return static_cast<ProcedureRun *>(cxt.getSharedPtrAs<ProcedureRun>(4).get());
        };
    
    manager.valueNode->addRoute("/state", { "GET" },
                           "Get the state of the run",
                                // "JSON containing the state of the run",
                           getRunState,
                           help);

    manager.valueNode->addRoute("/state", { "PUT" },
                              "Change the state of the run",
                              // "JSON containing the modified state of the run",
                           setRunState,
                           help);


    addRouteSyncJsonReturn(*manager.valueNode, "/details", { "GET" },
                           "Get the details about the run's output",
                           "Run-specific JSON output",
                           &Procedure::getRunDetails,
                           getProcedure,
                           getRun);
    
#if 0
    // Allow a per-run route
    RestRequestRouter::OnProcessRequest handlePluginRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               RestRequestParsingContext & cxt)
        {
            ProcedureRun * run = manager.getEntity(cxt);
            auto key = manager.getKey(cxt);

            try {
                return dataset->handleRequest(connection, req, cxt);
            }
            catch (const HttpReturnException & exc) {
                return sendExceptionResponse(connection, exc);
            } catch (const std::exception & exc) {
                return sendExceptionResponse(connection, exc);
            } MLDB_CATCH_ALL {
                connection.sendErrorResponse(400, "Unknown exception was thrown");
                return RestRequestRouter::MR_ERROR;
            }
        };

    RestRequestRouter & subRouter
        = manager.valueNode->addSubRouter("/routes", "Dataset type-specific routes");

    subRouter.rootHandler = handlePluginRoute;
#endif
}


void
ProcedureRunCollection::
init(std::shared_ptr<CollectionConfigStore> configStore)
{
    if (configStore)
        this->attachConfig(configStore);
}

Utf8String
ProcedureRunCollection::
getKey(ProcedureRunConfig & config)
{
    if (!config.id.empty())
        return config.id;

    // Add a disambiguating element to distinguish between different things that
    // try to get the same key.
    // 1.  Newly seeded random number based on current time
    // 2.  Thread ID
    Utf8String disambig
        = MLDB::format("%d-%d", random())
        + Date::now().print(9)
        + std::to_string(std::hash<std::thread::id>()(std::this_thread::get_id()));
    
    // Create an auto hash that is cleary identified as one
    return config.id = MLDB::format("%s-%016llx",
                                  Date::now().printIso8601(6).c_str(),
                                  (unsigned long long)jsonHash(jsonEncode(config)));
}

void
ProcedureRunCollection::
setKey(ProcedureRunConfig & config, Utf8String key)
{
    if (!config.id.empty() && config.id != key) {
        Json::Value details;
        details["valueInUri"] = key;
        details["valueInConfig"] = config.id;
        throw HttpReturnException(400, "Ambiguous names between route and config "
                                  "for procedure run PUT", details);
    }
    
    config.id = key;
}

ProcedureRunStatus
ProcedureRunCollection::
getStatusLoading(Utf8String key, const BackgroundTask & task) const
{
    ProcedureRunStatus result;
    result.id = key;
    result.state = task.getState();
    result.progress = task.getProgress();
    return result;
}

std::shared_ptr<ProcedureRunConfig>
ProcedureRunCollection::
getConfig(Utf8String key, const ProcedureRun & value) const
{
    return value.config;
}

ProcedureRunStatus
ProcedureRunCollection::
getStatusFinished(Utf8String key, const ProcedureRun & value) const
{
    // TODO this needs to be fixed to return the output of the trainign
    ProcedureRunStatus result;
    result.id = key;
    result.state = "finished";
    result.status = value.results;
    result.runStarted = value.runStarted;
    result.runFinished = value.runFinished;
    return result;
}

std::shared_ptr<ProcedureRun>
ProcedureRunCollection::
construct(ProcedureRunConfig config, const OnProgress & onProgress) const
{
    return std::make_shared<ProcedureRun>(procedure, config, onProgress);
}

DEFINE_REST_COLLECTION_INSTANTIATIONS(Utf8String, ProcedureRun,
                                      ProcedureRunConfig,
                                      ProcedureRunStatus);

} // namespace MLDB



