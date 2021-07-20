/** python_plugin_loader.cc
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    
    Plugin loader for Python plugins.
*/

// Python includes aren't ready for c++17 which doesn't support register
#define register 

#define BOOST_BIND_GLOBAL_PLACEHOLDERS

#include <Python.h>

#include "mldb/core/mldb_engine.h"
#include "mldb/core/dataset.h"
#include "mldb/core/plugin.h"
#include "mldb/core/procedure.h"
#include "mldb/base/scope.h"
#include "mldb/builtin/plugin_resource.h"

#include "pointer_fix.h" // must come before boost/python
#include <boost/python.hpp>
#include <boost/python/return_value_policy.hpp>
#include <frameobject.h>

#include "python_converters.h"
#include "from_python_converter.h"
#include "callback.h"
#include <boost/python/to_python_converter.hpp>
#include <boost/python/raw_function.hpp>

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

#include "datetime.h"

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
        } catch (const boost::python::error_already_set & exc) {
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
        
        boost::python::dict locals;
        locals["request"]
            = boost::python::object(boost::python::ptr(pyRestRequest.get()));
        
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

        Utf8String scriptSource
            = pluginRez->getScript(PackageElement::MAIN);
        Utf8String scriptUri
            = pluginRez->getScriptUri(PackageElement::MAIN);
        
        ScriptOutput output;
        {
            // Now we have our new interpreter, we can enter into its main
            // thread.
            auto enterThread = interpreter.mainThread().enter();

            auto pyRestRequest
                = std::make_shared<PythonRestRequest>(request, context);
            interpreter.main_namespace["request"]
                = boost::python::object(boost::python::ptr(pyRestRequest.get()));
            
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

namespace {

std::string pyObjectToString(PyObject * pyObj)
{
    namespace bp = boost::python;

    if(PyLong_Check(pyObj)) {
        return boost::lexical_cast<std::string>(bp::extract<long>(pyObj));
    }
    else if(PyFloat_Check(pyObj)) {
        return boost::lexical_cast<std::string>(bp::extract<float>(pyObj));
    }
    else if(PyBytes_Check(pyObj)) {
        return bp::extract<std::string>(pyObj);
    }
    else if(PyUnicode_Check(pyObj)) {
        PyObject* from_unicode = PyUnicode_AsASCIIString(pyObj);
        std::string tmpStr = bp::extract<std::string>(from_unicode);

        // not returned so needs to be garbage collected
        Py_DECREF(from_unicode);

        return tmpStr;
    }

    PyObject* str_obj = PyObject_Str(pyObj);
    std::string str_rep = "<Unable to create str representation of object>";
    if(str_obj) {
        str_rep = bp::extract<std::string>(str_obj);
    }
    Py_DECREF(str_obj);
    return str_rep;
};

boost::python::object
logArgs(boost::python::tuple args, boost::python::dict kwargs)
{
    namespace bp = boost::python;

    if(len(args) < 1) {
        return bp::object();
    }
    
    string str_accum; 
    for(int i = 1; i < len(args); ++i) {
        if(i > 1) str_accum += " ";
        str_accum += pyObjectToString(bp::object(args[i]).ptr());
    }

    MldbPythonContext* pymldb = bp::extract<MldbPythonContext*>(bp::object(args[0]).ptr());
    pymldb->log(str_accum);

    return bp::object();
}

// At startup, initialize all of this fun stuff

void pythonLoaderInit(const EnterThreadToken & thread)
{
    namespace bp = boost::python;

    PyDateTime_IMPORT;
    from_python_converter< RestParams, RestParamsConverter>();
    bp::to_python_converter< RestParams, RestParamsConverter>();

    bp::class_<PythonRestRequest, std::shared_ptr<PythonRestRequest>, boost::noncopyable>("rest_request", bp::no_init)
        .add_property("remaining",
                      make_getter(&PythonRestRequest::remaining,
                                  bp::return_value_policy<bp::return_by_value>()))
        .add_property("verb",
                      make_getter(&PythonRestRequest::verb,
                                  bp::return_value_policy<bp::return_by_value>()))
        .add_property("resource",
                      make_getter(&PythonRestRequest::resource,
                                  bp::return_value_policy<bp::return_by_value>()))
        .add_property("rest_params",
                      make_getter(&PythonRestRequest::restParams,
                                  bp::return_value_policy<bp::return_by_value>()))
        .add_property("payload",
                      make_getter(&PythonRestRequest::payload,
                                  bp::return_value_policy<bp::return_by_value>()))
        .add_property("content_type",
                      make_getter(&PythonRestRequest::contentType,
                                  bp::return_value_policy<bp::return_by_value>()))
        .add_property("content_length",
                      make_getter(&PythonRestRequest::contentLength,
                                  bp::return_value_policy<bp::return_by_value>()))
        .add_property("headers",
                      make_getter(&PythonRestRequest::headers,
                                  bp::return_value_policy<bp::return_by_value>()))
        .def("set_return", &PythonRestRequest::setReturnValue)
        .def("set_return", &PythonRestRequest::setReturnValue1);
        ;

    bp::class_<PythonPluginContext,
               std::shared_ptr<PythonPluginContext>,
               boost::noncopyable>
        plugin("Plugin", bp::no_init);
    bp::class_<PythonScriptContext,
               std::shared_ptr<PythonScriptContext>,
               boost::noncopyable>
        script("Script", bp::no_init);
    bp::class_<MldbPythonContext,
               std::shared_ptr<MldbPythonContext>,
               boost::noncopyable>
        mldb("Mldb", bp::no_init);

    script.add_property("args", &PythonScriptContext::getArgs);
    plugin.add_property("args", &PythonPluginContext::getArgs);

    plugin.def("serve_static_folder",
               &PythonPluginContext::serveStaticFolder);
    plugin.def("serve_documentation_folder",
               &PythonPluginContext::serveDocumentationFolder);
    plugin.def("get_plugin_dir",
               &PythonPluginContext::getPluginDirectory);

    mldb.def("log", bp::raw_function(logArgs, 1));
    mldb.def("log", &MldbPythonContext::logUnicode);
    mldb.def("log", &MldbPythonContext::logJsVal);
    mldb.def("perform", &MldbPythonContext::perform); // for 5 args
    mldb.def("perform", &MldbPythonContext::perform4); // for 4 args
    mldb.def("perform", &MldbPythonContext::perform3); // for 3 args
    mldb.def("perform", &MldbPythonContext::perform2); // for 2 args
    mldb.def("read_lines", &MldbPythonContext::readLines);
    mldb.def("read_lines", &MldbPythonContext::readLines1);
    mldb.def("ls", &MldbPythonContext::ls);
    mldb.def("get_http_bound_address", &MldbPythonContext::getHttpBoundAddress);


    mldb.def("create_dataset",
             &DatasetPy::createDataset,
             bp::return_value_policy<bp::manage_new_object>());
    mldb.def("create_procedure", &PythonProcedure::createPythonProcedure);
             mldb.def("create_function", &PythonFunction::createPythonFunction);

    mldb.add_property("script", &MldbPythonContext::getScript);
    mldb.add_property("plugin", &MldbPythonContext::getPlugin);

    mldb.def("debugSetPathOptimizationLevel",
             &MldbPythonContext::setPathOptimizationLevel);

    /****
     *  Functions
     *  **/

}

// Arrange for the above function to be run at the appropriate moment
// when there is a proper python environment set up.  There is no
// proper environment on shared initialization, so it can't be run
// from AtInit.

RegisterPythonInitializer regMe(&pythonLoaderInit);

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


} // file scope
} // namespace MLDB

