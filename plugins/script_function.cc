// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** script_functions.cc
    Francois Maillet, 14 juillet 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "script_function.h"
#include "mldb/server/mldb_server.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "mldb/types/any_impl.h"
#include "mldb/utils/log.h"

using namespace std;



namespace MLDB {



/*****************************************************************************/
/* SCRIPT FUNCTION CONFIG                                                    */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(ScriptFunctionConfig);

ScriptFunctionConfigDescription::
ScriptFunctionConfigDescription()
{
    addField("language", &ScriptFunctionConfig::language, 
            "Script language (python or javascript)");
    addField("scriptConfig", &ScriptFunctionConfig::scriptConfig, 
            "Script resource configuration");
}


/*****************************************************************************/
/* SCRIPT FUNCTION                                                           */
/*****************************************************************************/
                      
ScriptFunction::
ScriptFunction(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner, config)
{
    functionConfig = config.params.convert<ScriptFunctionConfig>();

    // Preload the script code in case it's remotly hosted. This was we won't
    // have to download it each time the function is called
    switch(parseScriptLanguage(functionConfig.language)) {
        case PYTHON:        runner = "python";      break;
        case JAVASCRIPT:    runner = "javascript";  break;
        default:
            throw MLDB::Exception("unknown script language");
    }

    LoadedPluginResource loadedResource(parseScriptLanguage(functionConfig.language),
                        LoadedPluginResource::ScriptType::SCRIPT,
                        "ND", functionConfig.scriptConfig.toPluginConfig());

    cachedResource.source = loadedResource.getScript(PackageElement::MAIN);
}

Any
ScriptFunction::
getStatus() const
{
    return Any();
}

ExpressionValue
ScriptFunction::
apply(const FunctionApplier & applier,
      const ExpressionValue & context) const
{
    string resource = "/v1/types/plugins/" + runner + "/routes/run";

    // make it so that if the params parameter contains an args key, we move
    // its contents to the args parameter of the script
    ScriptResource copiedSR(cachedResource);

    ExpressionValue args = context.getColumn(PathElement("args"));
    //Json::Value val = { args.extractJson(), jsonEncode(args.getEffectiveTimestamp()) };
    Json::Value val = jsonEncode(args);
    copiedSR.args = val;

    DEBUG_MSG(logger) << "script args = " << jsonEncode(copiedSR.args);

    RestRequest request("POST", resource, RestParams(),
                        
                        jsonEncode(copiedSR).toString());
    InProcessRestConnection connection;
    
    // TODO. this should not always be true. need to get this from the context
    // somehow
    request.header.headers.insert(make_pair("__mldb_child_call", "true"));

    server->handleRequest(connection, request);

    // TODO. better exception message
    if(connection.responseCode != 200) {
        throw HttpReturnException(400, "responseCode != 200 for function",
                                  Json::parse(connection.response));
    }

    Json::Value result = Json::parse(connection.response)["result"];
    
    vector<tuple<PathElement, ExpressionValue>> vals;
    if(!result.isArray()) {
        throw MLDB::Exception("Function should return array of arrays.");
    }

    for(const Json::Value & elem : result) {
        if(!elem.isArray() || elem.size() != 3)
            throw MLDB::Exception("elem should be array of size 3");

        vals.push_back(make_tuple(PathElement(elem[0].asString()),
                                  ExpressionValue(elem[1],
                                                  Date::parseIso8601DateTime(elem[2].asString()))));
    }

    StructValue sresult;
    sresult.emplace_back("return", std::move(vals));

    return std::move(sresult);
}

FunctionInfo
ScriptFunction::
getFunctionInfo() const
{
    FunctionInfo result;

    std::vector<KnownColumn> inputColumns, outputColumns;
    inputColumns.emplace_back(PathElement("args"), std::make_shared<UnknownRowValueInfo>(),
                              COLUMN_IS_DENSE, 0);
    outputColumns.emplace_back(PathElement("return"), std::make_shared<UnknownRowValueInfo>(),
                               COLUMN_IS_DENSE, 0);
    
    result.input.emplace_back(new RowValueInfo(inputColumns, SCHEMA_CLOSED));
    result.output.reset(new RowValueInfo(outputColumns, SCHEMA_CLOSED));
    
    return result;
}

static RegisterFunctionType<ScriptFunction, ScriptFunctionConfig>
regScriptFunction(builtinPackage(),
                  "script.apply",
                  "Run a JS or Python script as a function",
                  "functions/ScriptApplyFunction.md.html",
                            nullptr /* static route */,
                            { MldbEntity::INTERNAL_ENTITY });


} // namespace MLDB

