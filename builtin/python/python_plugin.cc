/** python_plugin_loader.cc
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    
    Plugin loader for Python plugins.
*/

#include <Python.h>

#include "mldb/core/mldb_engine.h"
#include "mldb/core/dataset.h"
#include "mldb/core/plugin.h"
#include "mldb/core/procedure.h"
#include "mldb/base/scope.h"
#include "mldb/builtin/plugin_resource.h"

#include "nanobind/nanobind.h"
#include "nanobind/stl/shared_ptr.h"
#include <frameobject.h>

#include "python_converters.h"
#include "from_python_converter.h"
#include "callback.h"

#include "python_plugin_context.h"
#include "python_entities.h"
#include "mldb/types/annotated_exception.h"

#include "mldb/rest/rest_request_binding.h"
#include "mldb/rest/rest_entity.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/dtoa.h"

#include "mldb/arch/file_functions.h"
#include "mldb/logging/logging.h"
#include "mldb_python_converters.h"
#include "mldb/types/any_impl.h"
#include "mldb/utils/lexical_cast.h"

#include <signal.h>
#include <fcntl.h>

using namespace std;
using namespace MLDB::Python;


namespace MLDB {


/*****************************************************************************/
/* PYTHON PLUGIN                                                             */
/*****************************************************************************/

struct PythonPlugin: public Plugin {
    PythonPlugin(MldbEngine * engine,
                 PolyConfig config,
                 std::function<bool (const Json::Value & progress)> onProgress);
    
    ~PythonPlugin();

    ScriptOutput getLastOutput() const;
    
    virtual Any getStatus() const;

    virtual RestRequestMatchResult
    handleDocumentationRoute(RestConnection & connection,
                         const RestRequest & request,
                         RestRequestParsingContext & context) const;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

    virtual Any getVersion() const;

    // Setup the interpreter's import path to include the plugin directory
    // so that we can find modules that are part of the plugin.
    void addPluginPathToEnv(const EnterThreadToken & token);

    static RestRequestMatchResult
    handleTypeRoute(RestDirectory * server, RestConnection & conn,
                    const RestRequest & request,
                    RestRequestParsingContext & context);

    std::shared_ptr<PythonPluginContext> pluginCtx;
    std::shared_ptr<MldbPythonContext> mldbPyCtx;

    mutable bool initialGetStatus;

    // This is protected by the Python GIL ?!
    mutable ScriptOutput last_output;

    std::shared_ptr<MldbPythonInterpreter> interpreter;
};


PythonPlugin::
PythonPlugin(MldbEngine * engine,
             PolyConfig config,
             std::function<bool (const Json::Value & progress)> onProgress)
    : Plugin(engine), initialGetStatus(true)
{
    PluginResource res = config.params.convert<PluginResource>();
    try {
        pluginCtx.reset(new PythonPluginContext(config.id, this->engine,
                std::make_shared<LoadedPluginResource>
                                      (PYTHON,
                                       LoadedPluginResource::PLUGIN,
                                       config.id, res)));
    }
    catch(const std::exception & exc) {
        throw AnnotatedException(400, MLDB::format("Exception opening plugin: %s", exc.what()));
    }

    addRouteSyncJsonReturn(pluginCtx->router, "/lastoutput", {"GET"},
            "Return the output of the last call to the plugin",
            "Output of the last call to the plugin",
            &PythonPlugin::getLastOutput,
            this);


    mldbPyCtx.reset(new MldbPythonContext(pluginCtx));

    Utf8String scriptSource = pluginCtx->pluginResource->getScript(PackageElement::MAIN);
    Utf8String scriptUri = pluginCtx->pluginResource->getScriptUri(PackageElement::MAIN);

    //cerr << "creating new interpreter" << endl;
    interpreter.reset(new MldbPythonInterpreter(pluginCtx));
    
    auto enterMainThread = interpreter->mainThread().enter();

    addPluginPathToEnv(*enterMainThread);

    RestRequest request;
    RestRequestParsingContext context(request);
    auto connection = InProcessRestConnection::create();
    
    last_output = interpreter->runPythonScript
        (*enterMainThread,
         scriptSource,
         scriptUri,
         interpreter->main_namespace,
         interpreter->main_namespace);

    if (last_output.exception) {
        MLDB_TRACE_EXCEPTIONS(false);
        string context = "Exception executing Python initialization script";
        enterMainThread.reset();
        interpreter->destroy();
        throw AnnotatedException(400, context, last_output);
    }
}

PythonPlugin::
~PythonPlugin()
{
    interpreter->destroy();
}

void
PythonPlugin::
addPluginPathToEnv(const EnterThreadToken & token)
{
    // now time to insert the plugin working directory into the python path
    char key[] = "path";
    PyObject* sysPath = PySys_GetObject(key);
    ExcAssert(sysPath);

    string pluginDir = pluginCtx->pluginResource->getPluginDir().string();
    PyObject * pluginDirPy = PyUnicode_FromString(pluginDir.c_str());
    ExcAssert(pluginDirPy);
    
    PyList_Insert( sysPath, 0, pluginDirPy );
}

ScriptOutput PythonPlugin::
getLastOutput() const
{
    // This is protected by the GIL, so we need to enter the main thread
    // to get it.
    auto enterMainThread = interpreter->mainThread().enter();
    return last_output;
}

Any PythonPlugin::
getVersion() const
{
    return pluginCtx->pluginResource->version;
}

Any
PythonPlugin::
getStatus() const
{
#if 0
    if (itl->getStatus) {
        PythonInterpreter *interpreter;

        Any rtn;
        try {
            rtn = pluginCtx->getStatus();
        } catch (const nanobind::error_already_set & exc) {
            ScriptException pyexc = convertException(*interpreter, exc, "PyPlugin get status");

            {
                std::unique_lock<std::mutex> guard(pluginCtx->logMutex);
                LOG(pluginCtx->loader) << jsonEncode(pyexc) << endl;
            }

            MLDB_TRACE_EXCEPTIONS(false);
            string context = "Exception in Python status call";
            ScriptOutput result = exceptionToScriptOutput(
                *interpreter, pyexc, context);
            throw AnnotatedException(400, context, result);
        }


        if(!initialGetStatus) {
            last_output = ScriptOutput();
            getOutputFromPy(*interpreter, last_output, !initialGetStatus);
            initialGetStatus = false;
        }
        return rtn;
    }
#endif
    return Any();
}

RestRequestMatchResult
PythonPlugin::
handleDocumentationRoute(RestConnection & connection,
                         const RestRequest & request,
                         RestRequestParsingContext & context) const
{
    if (pluginCtx->handleDocumentation) {
        return pluginCtx->handleDocumentation(connection, request, context);
    }

    return RestRequestRouter::MR_NO;
}

RestRequestMatchResult
PythonPlugin::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    // First, check for a route
    auto res = pluginCtx->router.processRequest(connection, request, context);
    if (res != RestRequestRouter::MR_NO) {
        return res;
    }

    // Second, check for a generic request handler
    if(pluginCtx->hasRequestHandler) {
        MLDB_TRACE_EXCEPTIONS(false);

        Utf8String scriptSource
            = pluginCtx->pluginResource->getScript(PackageElement::ROUTES);
        Utf8String scriptUri
            = pluginCtx->pluginResource->getScriptUri(PackageElement::ROUTES);
        auto thread = interpreter->mainThread().enter();

        auto pyRestRequest
            = std::make_shared<PythonRestRequest>(request, context);
        
        nanobind::dict locals;
        locals["request"] = pyRestRequest;
//            = nanobind::object(nanobind::ptr(pyRestRequest.get()));
        
        last_output = interpreter->runPythonScript
            (*thread,
             scriptSource, scriptUri,
             interpreter->main_namespace,
             locals);
        
        if (last_output.exception) {
            connection.sendJsonResponse(last_output.getReturnCode(),
                                        jsonEncode(last_output));
        }
        else {
            last_output.result = std::move(pyRestRequest->returnValue);
        
            if (pyRestRequest->returnCode <= 0) {
                throw AnnotatedException
                    (500,
                     "Return value is required for route handlers but not set");
            }
            
            last_output.setReturnCode(pyRestRequest->returnCode);

            connection.sendJsonResponse(last_output.getReturnCode(),
                                        jsonEncode(last_output.result));
        }
        
        return MR_YES;
    }

    // Otherwise we simply don't handle it
    return RestRequestRouter::MR_NO;
}

RestRequestMatchResult
PythonPlugin::
handleTypeRoute(RestDirectory * engine,
                RestConnection & conn,
                const RestRequest & request,
                RestRequestParsingContext & context)
{
    if (context.resources.back() == "run") {

        auto scriptConfig =
            jsonDecodeStr<ScriptResource>(request.payload).toPluginConfig();

        auto pluginRez =
            std::make_shared<LoadedPluginResource>(PYTHON,
                                                   LoadedPluginResource::SCRIPT,
                                                   "", scriptConfig);
        std::shared_ptr<PythonScriptContext> scriptCtx
            = std::make_shared<PythonScriptContext>
            ("script runner", dynamic_cast<MldbEngine *>(engine), pluginRez);

        // We need a brand new interpreter for this script, as we don't
        // want to allow for any interference with other things
        MldbPythonInterpreter interpreter(scriptCtx);
        //auto & interpreter = MldbPythonInterpreter::mainInterpreter();
        //cerr << "WARNING: Python scripts aren't isolated when using NanoBind" << endl;
        //cerr << "DO NOT REMOVE THIS MESSAGE" << endl;

        Utf8String scriptSource
            = pluginRez->getScript(PackageElement::MAIN);
        Utf8String scriptUri
            = pluginRez->getScriptUri(PackageElement::MAIN);
        
        ScriptOutput output;
        {
            // Now we have our new interpreter, we can enter into its main
            // thread.
            auto enterThread = interpreter.mainThread().enter();

            // Make sure that nanobind is initialized
            nanobind::detail::init(nullptr);
            nanobind::module_::import_("_mldb");

            try {
                auto pyRestRequest = std::make_shared<PythonRestRequest>(request, context);
                interpreter.main_namespace["request"] = pyRestRequest;
    //                = nanobind::object(nanobind::ptr(pyRestRequest.get()));
                
                output = interpreter
                    .runPythonScript(*enterThread,
                                    scriptSource,
                                    scriptUri,
                                    interpreter.main_namespace,
                                    interpreter.main_namespace);

                if (!output.exception) {
                    output.result = std::move(pyRestRequest->returnValue);
                    
                    if (pyRestRequest->returnCode <= 0) {
                        pyRestRequest->returnCode = 200;
                    }
                    output.setReturnCode(pyRestRequest->returnCode);
                }
            } catch (const std::exception & exc) {
                cerr << "exception setting up script: " << getExceptionString() << endl;
                throw;
            }
        }
        
        // Copy log messages over
        for (auto & l: scriptCtx->logs) {
            output.logs.emplace_back(std::move(l));
        }
        std::stable_sort(output.logs.begin(), output.logs.end());
        
        conn.sendJsonResponse(output.getReturnCode(),
                              jsonEncode(output));
        
        interpreter.destroy();
        
        return RestRequestRouter::MR_YES;
    }

    return RestRequestRouter::MR_NO;
}

struct AtInit {
    AtInit()
    {
        registerPluginType<PythonPlugin, PluginResource>
            (builtinPackage(),
             "python",
             "Load plugins or run scripts written in the Python language",
             "lang/Python.md.html",
             &PythonPlugin::handleTypeRoute);

    }
} atInit;

} // namespace MLDB

