// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** function_collection.cc
    Jeremy Barnes, 24 November 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    Collection of functions.
*/

#include "mldb/server/function_collection.h"
#include "mldb/rest/poly_collection_impl.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/server/mldb_server.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/types/meta_value_description.h"
#include "mldb/server/dataset_context.h"
#include "mldb/types/map_description.h"



using namespace std;


namespace Datacratic {
namespace MLDB {

std::shared_ptr<FunctionCollection>
createFunctionCollection(MldbServer * server, RestRouteManager & routeManager,
                       std::shared_ptr<CollectionConfigStore> configStore)
{
    return createCollection<FunctionCollection>(2, "function", "functions",
                                              server, routeManager,
                                              configStore);
}

std::shared_ptr<Function>
obtainFunction(MldbServer * server,
            const PolyConfig & config,
            const MldbServer::OnProgress & onProgress)
{
    return server->functions->obtainEntitySync(config, onProgress);
}

std::shared_ptr<Function>
createFunction(MldbServer * server,
              const PolyConfig & config,
              const std::function<bool (const Json::Value & progress)> & onProgress,
              bool overwrite)
{
    return server->functions->createEntitySync(config, onProgress, overwrite);
}

std::shared_ptr<FunctionType>
registerFunctionType(const Package & package,
                     const Utf8String & name,
                     const Utf8String & description,
                     std::function<Function * (RestDirectory *,
                                               PolyConfig,
                                               const std::function<bool (const Json::Value)> &)>
                     createEntity,
                     TypeCustomRouteHandler docRoute,
                     TypeCustomRouteHandler customRoute,
                     std::shared_ptr<const ValueDescription> config,
                     std::set<std::string> registryFlags)
{
    return FunctionCollection::registerType(package, name, description, createEntity,
                                            docRoute, customRoute,
                                            config, registryFlags);
}


/*****************************************************************************/
/* FUNCTION                                                                  */
/*****************************************************************************/

FunctionOutput
Function::
call(const std::map<Utf8String, ExpressionValue> & input) const
{
    SqlExpressionMldbContext outerContext(MldbEntity::getOwner(this->server));
    
    auto info = this->getFunctionInfo();

    //cerr << "function info is " << jsonEncode(info) << endl;

    FunctionContext inputContext;

    // 2.  Extract the seed values for the context
    for (auto & p: input) {
        const Utf8String & name(p.first);
        const ExpressionValue & v(p.second);
        const ExpressionValueInfo * valueInfo = nullptr;
        
        auto it = info.input.values.find(name);

        try {
            JML_TRACE_EXCEPTIONS(false);

            // skip unknown values
            if (it == info.input.values.end())
                continue;

            // Save the expected value type to put it in an error message later
            valueInfo = it->second.valueInfo.get();
        
            inputContext.set(name, v);
        } catch (const std::exception & exc) {
            Json::Value details;
            details["valueName"] = name;
            details["value"] = jsonEncode(v);
            details["functionName"] = this->config_->id;
            details["functionType"] = this->type_;
            if (valueInfo)
                details["valueExpectedType"] = jsonEncode(it->second.valueInfo);

            rethrowHttpException(400, "Parsing value '" + name + "' with value '"
                                 + jsonEncodeStr(v) + "' for function '"
                                 + this->config_->id + "': " + exc.what(),
                                 details);
        }
    }

    //cerr << "inputContext = " << jsonEncode(inputContext) << endl;

    auto applier = this->bind(outerContext, info.input);
    
    return applier->apply(inputContext);
}

/*****************************************************************************/
/* FUNCTION COLLECTION                                                       */
/*****************************************************************************/

FunctionCollection::
FunctionCollection(MldbServer * server)
    : PolyCollection<Function>("function", "functions", server)
{
}

void
FunctionCollection::
applyFunction(const Function * function,
              const std::map<Utf8String, ExpressionValue> & input,
              const std::vector<Utf8String> & keepValues,
              RestConnection & connection) const
{
    FunctionOutput output = function->call(input);

    //cerr << "output = " << jsonEncode(output) << endl;

    FunctionOutput result;

    if (!keepValues.empty()) {
        for (auto & p: keepValues)
            result.values[p] = std::move(output.values[p]);
    }
    else {
        result = std::move(output);
    }

    static auto valDesc = getExpressionValueDescriptionNoTimestamp();

    std::ostringstream stream;
    StreamJsonPrintingContext context(stream);

    context.startObject();
    context.startMember("output");
    context.startObject();
    for (auto & p: result.values) {
        context.startMember(p.first.rawString());
        valDesc->printJsonTyped(&p.second, context);
    }
    context.endObject();
    context.endObject();
    connection.sendResponse(200, stream.str(), "application/json");
}

void
FunctionCollection::
initRoutes(RouteManager & manager)
{
    PolyCollection<Function>::initRoutes(manager);

    std::function<Function * (const RestRequestParsingContext & cxt) > getFunction
        = [] (const RestRequestParsingContext & cxt)
        {
            return static_cast<Function *>(cxt.getSharedPtrAs<PolyEntity>(2).get());
        };

    typedef std::map<Utf8String, ExpressionValue> MapType;

    auto mapDesc = std::make_shared<MapDescription<Utf8String, ExpressionValue> >
        (getExpressionValueDescriptionNoTimestamp());

    addRouteAsync(*manager.valueNode, "/application", { "GET" },
                  "Apply a function to a given set of input values and return the output",
                  //"Output of all values or those selected in the keepValues parameter",
                  &FunctionCollection::applyFunction,
                  manager.getCollection,
                  getFunction,
                  RestParamJson<std::map<Utf8String, ExpressionValue> >("input", "Object with input values", JsonStrCodec<MapType>(mapDesc)),
                  RestParamJsonDefault<std::vector<Utf8String> >
                  ("keepValues", "Keep only these values for the output", {}),
                  PassConnectionId());
    
    addRouteSyncJsonReturn(*manager.valueNode, "/info", { "GET" },
                           "Return information about the values and metadata of the function",
                           "Function information structure",
                           &Function::getFunctionInfo,
                           getFunction);

    addRouteSyncJsonReturn(*manager.valueNode, "/details", { "GET" },
                           "Return details about the function's internal state",
                           "Function-specific JSON output",
                           &Function::getDetails,
                           getFunction);

    // Make the plugin handle a route
    RestRequestRouter::OnProcessRequest handlePluginRoute
        = [=] (RestConnection & connection,
               const RestRequest & req,
               RestRequestParsingContext & cxt)
        {
            Function * function = getFunction(cxt);
            auto key = manager.getKey(cxt);

            try {
                return function->handleRequest(connection, req, cxt);
            }
            catch (const HttpReturnException & exc) {
                return sendExceptionResponse(connection, exc);
            } catch (const std::exception & exc) {
                return sendExceptionResponse(connection, exc);
            } JML_CATCH_ALL {
                connection.sendErrorResponse(400, "Unknown exception was thrown");
                return RestRequestRouter::MR_ERROR;
            }
        };

    RestRequestRouter & subRouter
        = manager.valueNode->addSubRouter("/routes", "Function type-specific routes");
    
    subRouter.rootHandler = handlePluginRoute;
}

Any
FunctionCollection::
getEntityStatus(const Function & function) const
{
    return function.getStatus();
}

} // namespace MLDB

template class PolyCollection<MLDB::Function>;

} // namespace Datacratic
