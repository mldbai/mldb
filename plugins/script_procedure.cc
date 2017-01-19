// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** script.cc
    Francois Maillet, 10 juillet 2015
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Script procedure
*/

#include "script_procedure.h"
#include "mldb/server/script_output.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "mldb/types/any_impl.h"

using namespace std;


namespace MLDB {


/*****************************************************************************/
/* SCRIPT PROCEDURE CONFIG                                                   */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(ScriptProcedureConfig);

ScriptProcedureConfigDescription::
ScriptProcedureConfigDescription()
{
    addField("language", &ScriptProcedureConfig::language,
            "Script language (python or javascript)");
    addField("scriptConfig", &ScriptProcedureConfig::scriptConfig,
            "Script resource configuration");
    addField("args", &ScriptProcedureConfig::args,
            "Arguments to be passed to the script");
    addParent<ProcedureConfig>();
}



/*****************************************************************************/
/* SCRIPT PROCEDURE                                                          */
/*****************************************************************************/

ScriptProcedure::
ScriptProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->scriptProcedureConfig = config.params.convert<ScriptProcedureConfig>();
}

Any
ScriptProcedure::
getStatus() const
{
    return Any();
}

RunOutput
ScriptProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(scriptProcedureConfig, run);

    string runner;
    switch(parseScriptLanguage(runProcConf.language)) {
        case PYTHON:        runner = "python";      break;
        case JAVASCRIPT:    runner = "javascript";  break;
        default:
            throw MLDB::Exception("unknown script language");
    }

    string resource = "/v1/types/plugins/" + runner + "/routes/run";

    // make it so that if the params parameter contains an args key, we move
    // its contents to the args parameter of the script
    ScriptResource copiedSR(runProcConf.scriptConfig);
    if(!runProcConf.args.empty())
        copiedSR.args = runProcConf.args;

    RestRequest request("POST", resource, RestParams(),
                        jsonEncodeStr(copiedSR));
    InProcessRestConnection connection;

    server->handleRequest(connection, request);

    Json::Value details;
    details["statusCode"] = connection.responseCode;

    if (!connection.contentType.empty())
        details["contentType"] = connection.contentType;
    if (!connection.headers.empty()) {
        Json::Value headers(Json::ValueType::arrayValue);
        for(const std::pair<Utf8String, Utf8String> & h : connection.headers) {
            Json::Value elem(Json::ValueType::arrayValue);
            elem.append(h.first);
            elem.append(h.second);
            headers.append(elem);
        }
        details["headers"] = headers;
    }

    if (!connection.response.empty()) {
        ScriptOutput parsed = jsonDecodeStr<ScriptOutput>(connection.response);
        if (!parsed.result.isNull())
            details["result"] = parsed.result;
        if (!parsed.logs.empty())
            details["logs"] = jsonEncode(parsed.logs);
        if (!parsed.extra.empty())
            details["extra"] = jsonEncode(parsed.extra);
        if (parsed.exception)
            details["exception"] = jsonEncode(parsed.exception);
    }

    Json::Value result = Json::parse(connection.response);

    return RunOutput(result["result"], details);
}


namespace {

RegisterProcedureType<ScriptProcedure, ScriptProcedureConfig>
regScript(builtinPackage(),
          "Run a script",
          "procedures/ScriptProcedure.md.html",
                            nullptr /* static route */,
                            { MldbEntity::INTERNAL_ENTITY });

} // file scope

} // namespace MLDB

