// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** external_python_procedure.cc                                                     -*- C++ -*-
    Francois Maillet, 31 aout 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    External python procedure
*/

#include "external_python_procedure.h"
#include "mldb/server/mldb_server.h"
#include "mldb/jml/utils/guard.h"
#include "mldb/base/parallel.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/utils/runner.h"
#include <boost/filesystem.hpp>
#include "mldb/types/any_impl.h"


using namespace std;

namespace fs = boost::filesystem;


namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(ExternalPythonProcedureConfig);

ExternalPythonProcedureConfigDescription::
ExternalPythonProcedureConfigDescription()
{
    addParent<ProcedureConfig>();
    addField("scriptConfig", &ExternalPythonProcedureConfig::scriptConfig,
            "Script resource configuration");
    addField("stdInData", &ExternalPythonProcedureConfig::stdInData,
            "What to send on the stdin of the python process");
}


/*****************************************************************************/
/* EXTERNAL PYTHON PROCEDURE                                                 */
/*****************************************************************************/

ExternalPythonProcedure::
ExternalPythonProcedure(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    procedureConfig = config.params.convert<ExternalPythonProcedureConfig>();
    procedureConfig.scriptConfig.writeSourceToFile = true;

    // Use LoadedPluginResource so that it can take care of acquiring the
    // source and cleaning it up for us automatically
    pluginRes = make_shared<LoadedPluginResource>(
                        ScriptLanguage::PYTHON,
                        LoadedPluginResource::ScriptType::PLUGIN,
                        "externalProc-"+config.id,
                        procedureConfig.scriptConfig.toPluginConfig());
}

Any
ExternalPythonProcedure::
getStatus() const
{
    return Any();
}


RunOutput
ExternalPythonProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    ExternalPythonProcedureConfig newProcConf =
        applyRunConfOverProcConf(procedureConfig, run);

    auto output_stdout = make_shared<std::stringstream>();
    auto output_stderr = make_shared<std::stringstream>();

    auto stdout_sink = make_shared<OStreamInputSink>(output_stdout.get());
    auto stderr_sink = make_shared<OStreamInputSink>(output_stderr.get());

    if(!pluginRes)
        throw MLDB::Exception("ScriptRessource not loaded");
    if(!pluginRes->packageElementExists(MAIN))
        throw MLDB::Exception("Main script element does not exist");

    // TODO fix this. MLDB-874
    string python_executable;
    // we're running in docker
    if(fs::exists(fs::path("/mldb_data/"))) {
        python_executable = "/usr/bin/env python";
    }
    // not in docker; probably a test
    else {
        python_executable = "./virtualenv/bin/python";
    }

    string cmd = python_executable + " " + pluginRes->getElementLocation(MAIN);
    RunResult runRes = execute(ML::split(cmd, ' '), stdout_sink,
                                stderr_sink, newProcConf.stdInData);

    cout << runRes.state << endl;
    cout << runRes.returnCode << endl;

    stdout_sink->notifyClosed();
    stderr_sink->notifyClosed();

    string stdout_str = output_stdout->str();

    // python's print will add a \n at the end of the string. check if this is
    // the case and remove it for convinience
    if(stdout_str.back() == '\n') {
        stdout_str.erase(stdout_str.size() -1);
    }

    Json::Value jsRes;

    // check if the last line of the stdout if parsable as json. we parse it
    // for the user and put in the `return` key of the json blob
    size_t found = stdout_str.find_last_of("\n");
    if(found == string::npos)
        found = 0;

    // try to parse the last line as json
    Json::Value root;
    Json::Reader reader;
    if(reader.parse(stdout_str.substr(found), root)) {
        jsRes["return"] = root;

        // remove the last line
        stdout_str.erase(found);
    }

    // assemble the rest of the results json
    jsRes["stdout"] = stdout_str;
    jsRes["stderr"] = output_stderr->str();
    jsRes["runResult"] = jsonEncode(runRes);


    return RunOutput(jsRes);
}

static RegisterProcedureType<ExternalPythonProcedure, ExternalPythonProcedureConfig>
regExternalPipeline(builtinPackage(),
                    "Run an external script as a procedure",
                    "procedures/ExternalPythonProcedure.md.html",
                    nullptr,
                    { MldbEntity::INTERNAL_ENTITY });


} // namespace MLDB

