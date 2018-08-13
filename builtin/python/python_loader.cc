/** python_plugin_loader.cc
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    
    Plugin loader for Python plugins.
*/

// Python includes aren't ready for c++17 which doesn't support register
#define register 

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

#include "mldb/utils/file_functions.h"
#include "mldb/logging/logging.h"
#include "mldb_python_converters.h"
#include "mldb/types/any_impl.h"

#include "datetime.h"

#include <signal.h>
#include <fcntl.h>

using namespace std;
using namespace MLDB::Python;
//namespace bp = boost::python;


namespace MLDB {

// NOTE: copied from exec.cpp in Boost, available under the Boost license
// Copyright Stefan Seefeld 2005 - http://www.boost.org/LICENSE_1_0.txt
boost::python::object
pyExec(Utf8String code,
       Utf8String filename,
       boost::python::object global = boost::python::object(),
       boost::python::object local = boost::python::object())
{
    // We create a pipe so that we don't need to open a temporary
    // file.  Unfortunately Python only allows us to pass a filename
    // for error messages if we have a FILE *, so we need to arrange
    // to have one.  The easiest way is to create an fd on a pipe,
    // and then to use fopenfd() to open it.

    int fds[2];
    int res = pipe2(fds, 0 /* flags */);

    if (res == -1)
        throw AnnotatedException(500, "Python evaluation pipe: "
                                  + string(strerror(errno)));
    
    Scope_Exit(if (fds[0] != -1) ::close(fds[0]); if (fds[1] != -1) ::close(fds[1]));

    res = fcntl(fds[1], F_SETFL, O_NONBLOCK);
    if (res == -1) {
        auto errno2 = errno;
        throw AnnotatedException(500, "Python evaluation fcntl: "
                                  + string(strerror(errno2)));
    }

    std::atomic<int> finished(0);
    std::thread t;

    // Write as much as we can.  In Linux, the default pipe buffer is
    // 64k, which is enough for most scripts.
    ssize_t written = write(fds[1], code.rawData(), code.rawLength());
    
    if (written == -1) {
        // Error writing.  Bail out.
        throw AnnotatedException
            (500, "Error writing to pipe for python evaluation: "
             + string(strerror(errno)));
    }
    else if (written == code.rawLength()) {
        // We wrote the whole script to the pipe.  We can close the
        // write end of the file.
        ::close(fds[1]);
        fds[1] = -1;
    }
    else {
        // We weren't able to write the whole thing to the pipe.
        // We need to set up a thread to push more in once the
        // Python part has read some.

        // Turn off non-blocking.  We don't want to busy loop.  The
        // thread won't deadlock, since the FD will be closed if
        // we need to exit before all of the code is written.
        res = fcntl(fds[1], F_SETFL, 0);
        if (res == -1) {
            auto errno2 = errno;
            throw AnnotatedException(500, "Python evaluation fcntl: "
                                      + string(strerror(errno2)));
        }

        // Set up a thread to continue writing code to the pipe until
        // all of the code has been passed to the function
        t = std::thread([&] ()
            {
                while (!finished) {
                    ssize_t done = write(fds[1],
                                         code.rawData() + written,
                                         code.rawLength() - written);
                    if (finished)
                        return;
                    if (done == -1) {
                        if (errno == EAGAIN || errno == EINTR) {
                            continue;
                        }
                        cerr << "Error writing to Python source pipe: "
                             << strerror(errno) << endl;
                        std::terminate();
                    }
                    else {
                        written += done;
                        if (written == code.rawLength()) {
                            close(fds[1]);
                            fds[1] = -1;
                            return;
                        }
                    }
                }
            });
    }

    // Finally, we have our fd.  Turn it into a FILE * that Python wants.
    FILE * file = fdopen(fds[0], "r");

    if (!file) {
        throw AnnotatedException
            (500, "Error creating fd for python evaluation: "
             + string(strerror(errno)));
    }
    Scope_Exit(::fclose(file));
    fds[0] = -1;  // stop the fd guard from closing it, since now we have a guard

    using namespace boost::python;
    // From here on in is copied from the Boost version
    // Set suitable default values for global and local dicts.
    object none;
    if (global.ptr() == none.ptr()) {
        if (PyObject *g = PyEval_GetGlobals())
            global = object(boost::python::detail::borrowed_reference(g));
        else
            global = dict();
    }
    if (local.ptr() == none.ptr()) local = global;
    
    // should be 'char const *' but older python versions don't use 'const' yet.
    PyObject* result = PyRun_File(file, filename.rawData(), Py_file_input,
                                  global.ptr(), local.ptr());

    // Clean up no matter what (make sure our writing thread exits).  If it
    // was blocked on writing, the closing of the fd will unblock it.
    finished = 1;
    if (fds[1] != -1) {
        ::close(fds[1]);
        fds[1] = -1;
    }
    if (t.joinable())
        t.join();

    if (!result) throw_error_already_set();
    return boost::python::object(boost::python::detail::new_reference(result));
}



/*****************************************************************************/
/* PYTHON PLUGIN                                                             */
/*****************************************************************************/

struct PythonPlugin: public Plugin {
    PythonPlugin(MldbEngine * engine,
                     PolyConfig config,
                     std::function<bool (const Json::Value & progress)> onProgress);
    
    ~PythonPlugin();

    void addPluginPathToEnv(PythonSubinterpreter & pyControl) const;

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

    static ScriptOutput
    runPythonScript(std::shared_ptr<PythonContext> pyCtx,
                    PackageElement elementToRun,
                    const RestRequest & request,
                    RestRequestParsingContext & context,
                    PythonSubinterpreter & pyControl);
    
    static ScriptOutput
    runPythonScript(std::shared_ptr<PythonContext> pyCtx,
                    PackageElement elementToRun,
                    PythonSubinterpreter & pyControl);

    static RestRequestMatchResult
    handleTypeRoute(RestDirectory * server, RestConnection & conn,
                    const RestRequest & request,
                    RestRequestParsingContext & context);

    static void injectMldbWrapper(PythonSubinterpreter & pyControl);

    std::shared_ptr<PythonPluginContext> pluginCtx;
    std::shared_ptr<MldbPythonContext> mldbPyCtx;

    mutable std::mutex routeHandlingMutex;

    mutable bool initialGetStatus;
    mutable ScriptOutput last_output;
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
                                       config.id, res), routeHandlingMutex));
    }
    catch(const std::exception & exc) {
        throw AnnotatedException(400, MLDB::format("Exception opening plugin: %s", exc.what()));
    }

    addRouteSyncJsonReturn(pluginCtx->router, "/lastoutput", {"GET"},
            "Return the output of the last call to the plugin",
            "Output of the last call to the plugin",
            &PythonPlugin::getLastOutput,
            this);


    mldbPyCtx.reset(new MldbPythonContext());
    mldbPyCtx->setPlugin(pluginCtx);

    Utf8String scriptSource = pluginCtx->pluginResource->getScript(PackageElement::MAIN);
    Utf8String scriptUri = pluginCtx->pluginResource->getScriptUri(PackageElement::MAIN);

    // initialize interpreter. if the plugin is being constructed from a python plugin 
    // perform call, the __mldb_child_call will be set so we can engage child mode
    bool isChild = false;
    if(!res.args.empty() && res.args.is<Json::Value>()) {
        auto jsVal = res.args.as<Json::Value>();
        isChild = jsVal.isObject() && 
            res.args.as<Json::Value>().isMember("__mldb_child_call");
    }

    PythonSubinterpreter pyControl(isChild);
    addPluginPathToEnv(pyControl);

    try {
        MLDB_TRACE_EXCEPTIONS(false);
        pyControl.main_namespace["mldb"] =
            boost::python::object(boost::python::ptr(mldbPyCtx.get()));
    } catch (const boost::python::error_already_set & exc) {
        ScriptException pyexc = convertException(pyControl, exc, "PyPlugin init");

        {
            std::unique_lock<std::mutex> guard(pluginCtx->logMutex);
            LOG(pluginCtx->loader) << jsonEncode(pyexc) << endl;
        }

        MLDB_TRACE_EXCEPTIONS(false);
        throw AnnotatedException(500, "Exception creating Python context", pyexc);

    }

    try {
        injectMldbWrapper(pyControl);
        boost::python::object obj = pyExec(scriptSource,
                                           scriptUri,
                                           pyControl.main_namespace);
    } catch (const boost::python::error_already_set & exc) {
        ScriptException pyexc = convertException(pyControl, exc, "Running PyPlugin script");

        {
            std::unique_lock<std::mutex> guard(pluginCtx->logMutex);
            LOG(pluginCtx->loader) << jsonEncode(pyexc) << endl;
        }

        MLDB_TRACE_EXCEPTIONS(false);

        string context = "Exception executing Python initialization script";
        ScriptOutput result = exceptionToScriptOutput(pyControl, pyexc, context);
        throw AnnotatedException(400, context, result);
    }

    last_output = ScriptOutput();
    getOutputFromPy(pyControl, last_output);

}

PythonPlugin::
~PythonPlugin()
{
}
    
void PythonPlugin::
addPluginPathToEnv(PythonSubinterpreter & pyControl) const
{
    pyControl.acquireGil();
    
    // now time to insert the plugin working directory into the python path
    char key[] = "path";
    PyObject* sysPath = PySys_GetObject(key);
    ExcAssert(sysPath);

    string pluginDir = pluginCtx->pluginResource->getPluginDir().string();
    PyObject * pluginDirPy = PyUnicode_FromString(pluginDir.c_str());
    ExcAssert(pluginDirPy);

    PyList_Insert( sysPath, 0, pluginDirPy );

    // change working dir to script dir
//     PyRun_SimpleString(MLDB::format("import os\nos.chdir(\"%s\")", pluginDir).c_str());
}

ScriptOutput PythonPlugin::
getLastOutput() const
{
    std::unique_lock<std::mutex> guard(routeHandlingMutex);
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
//     if (itl->getStatus) {
//         PythonSubinterpreter pyControl;
// 
//         Any rtn;
//         try {
//             rtn = pluginCtx->getStatus();
//         } catch (const boost::python::error_already_set & exc) {
//             ScriptException pyexc = convertException(pyControl, exc, "PyPlugin get status");
// 
//             {
//                 std::unique_lock<std::mutex> guard(pluginCtx->logMutex);
//                 LOG(pluginCtx->loader) << jsonEncode(pyexc) << endl;
//             }
// 
//             MLDB_TRACE_EXCEPTIONS(false);
//             string context = "Exception in Python status call";
//             ScriptOutput result = exceptionToScriptOutput(
//                 pyControl, pyexc, context);
//             throw AnnotatedException(400, context, result);
//         }
// 
// 
//         if(!initialGetStatus) {
//             std::unique_lock<std::mutex> guard(routeHandlingMutex);
//             last_output = ScriptOutput();
//             getOutputFromPy(pyControl, last_output, !initialGetStatus);
//             initialGetStatus = false;
//         }
//         return rtn;
//     }
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

    // check if
    bool isChild = request.header.headers.find("__mldb_child_call") != request.header.headers.end();
    PythonSubinterpreter pyControl(isChild);
    addPluginPathToEnv(pyControl);

    // Second, check for a generic request handler
    if(pluginCtx->hasRequestHandler) {
        try {
            RestRequestMatchResult rtn;
            {
                MLDB_TRACE_EXCEPTIONS(false);

                pyControl.releaseGil();

                pluginCtx->restRequest = make_shared<PythonRestRequest>(request, context);
                pluginCtx->resetReturnValue();
                auto jsonResult = runPythonScript(pluginCtx,
                        PackageElement::ROUTES, request, context, pyControl);

                last_output = ScriptOutput();

                getOutputFromPy(pyControl, last_output, false);
                if(jsonResult.exception) {
                    connection.sendResponse(400, jsonEncodeStr(jsonResult), "application/json");
                }

                connection.sendResponse(jsonResult.getReturnCode(), jsonResult.result);
                return RestRequestRouter::MR_YES;
            }

            return rtn;
        } catch (const boost::python::error_already_set & exc) {
            ScriptException pyexc = convertException(pyControl, exc, "Handling PyPlugin route");

            {
                std::unique_lock<std::mutex> guard(pluginCtx->logMutex);
                LOG(pluginCtx->loader) << jsonEncode(pyexc) << endl;
            }

            MLDB_TRACE_EXCEPTIONS(false);
            string context = "Exception in Python request handler";
            ScriptOutput result = exceptionToScriptOutput(
                    pyControl, pyexc, context);
            throw AnnotatedException(400, context, result);
        }
   
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

        std::shared_ptr<PythonScriptContext> scriptCtx;
        auto pluginRez =
            std::make_shared<LoadedPluginResource>(PYTHON,
                                                   LoadedPluginResource::SCRIPT,
                                                   "", scriptConfig);
        try {
            scriptCtx = std::make_shared<PythonScriptContext>(
                "script runner", dynamic_cast<MldbEngine *>(engine), pluginRez);
        }
        catch(const std::exception & exc) {
            conn.sendResponse(
                400,
                jsonEncodeStr(MLDB::format("Exception opening script: %s", exc.what())),
                "application/json");
        }

        bool isChild = request.header.headers.find("__mldb_child_call") != request.header.headers.end();
        PythonSubinterpreter pyControl(isChild);
        auto result = runPythonScript(scriptCtx,
                                      PackageElement::MAIN,
                                      pyControl);
        conn.sendResponse(result.exception ? 400 : 200,
                          jsonEncodeStr(result), "application/json");

        return RestRequestRouter::MR_YES;
    }

    return RestRequestRouter::MR_NO;
}

ScriptOutput
PythonPlugin::
runPythonScript(std::shared_ptr<PythonContext> pyCtx,
        PackageElement elementToRun,
        PythonSubinterpreter & pyControl)
{
    RestRequest request;
    RestRequestParsingContext context(request);

    return runPythonScript(pyCtx, elementToRun, request, context, pyControl);
}

ScriptOutput
PythonPlugin::
runPythonScript(std::shared_ptr<PythonContext> pyCtx,
                PackageElement elementToRun,
                const RestRequest & request,
                RestRequestParsingContext & context,
                PythonSubinterpreter & pyControl)
{
    auto mldbPyCtx = std::make_shared<MldbPythonContext>();
    bool isScript = pyCtx->pluginResource->scriptType == LoadedPluginResource::ScriptType::SCRIPT;
    if(isScript) {
        mldbPyCtx->setScript(static_pointer_cast<PythonScriptContext>(pyCtx));
    }
    else {
        mldbPyCtx->setPlugin(static_pointer_cast<PythonPluginContext>(pyCtx));
    }

    Utf8String scriptSource = pyCtx->pluginResource->getScript(elementToRun);
    Utf8String scriptUri = pyCtx->pluginResource->getScriptUri(elementToRun);

    pyControl.acquireGil();

    try {
        MLDB_TRACE_EXCEPTIONS(false);
        pyControl.main_namespace["mldb"] = boost::python::object(boost::python::ptr(mldbPyCtx.get()));
        injectMldbWrapper(pyControl);

    } catch (const boost::python::error_already_set & exc) {
        ScriptException pyexc = convertException(pyControl, exc, "PyRunner init");

        {
            std::unique_lock<std::mutex> guard(pyCtx->logMutex);
            LOG(pyCtx->loader) << jsonEncode(pyexc) << endl;
        }

        MLDB_TRACE_EXCEPTIONS(false);

        ScriptOutput result;

        result.exception = std::make_shared<ScriptException>(std::move(pyexc));
        result.exception->context.push_back("Initializing Python script");
        return result;
    }

    ScriptOutput result;

    auto pySetArgv = [] {
        wchar_t argv1[] = L"mldb-boost-python";
        wchar_t *argv[] = {argv1};
        int argc = sizeof(argv[0]) / sizeof(wchar_t *);
        PySys_SetArgv(argc, argv);
    };

    // if we're simply executing the body of the script
    try {
        if(elementToRun == PackageElement::MAIN) {
            MLDB_TRACE_EXCEPTIONS(false);
            pySetArgv();
            boost::python::object obj =
                pyExec(scriptSource,
                       scriptUri,
                       pyControl.main_namespace);

            getOutputFromPy(pyControl, result);

            if(isScript) {
                auto scriptCtx = static_pointer_cast<PythonScriptContext>(pyCtx);
                result.result = scriptCtx->rtnVal;
                // python scripts don't need to set a return code
                result.setReturnCode(
                    scriptCtx->getRtnCode() == 0 ? 200 : scriptCtx->getRtnCode());
                for (auto & l: scriptCtx->logs) {
                    result.logs.emplace_back(std::move(l));
                }
                std::stable_sort(result.logs.begin(), result.logs.end());
            }

            return result;
        }
        // if we need to call the routes function
        else if(elementToRun == PackageElement::ROUTES) {

            pySetArgv();
            boost::python::object obj
                = pyExec(scriptSource, scriptUri, pyControl.main_namespace);
            if (pyCtx->getRtnCode() == 0) {
                 throw AnnotatedException(
                         500, "The route did not set a return code");
            }
            result.result = pyCtx->rtnVal;
            result.setReturnCode(pyCtx->getRtnCode());
            return result;
        }
        else {
            throw MLDB::Exception("Unknown element to run!!");
        }
    } catch (const boost::python::error_already_set & exc) {
        ScriptException pyexc = convertException(pyControl, exc, "Running PyRunner script");

        {
            std::unique_lock<std::mutex> guard(pyCtx->logMutex);
            LOG(pyCtx->loader) << jsonEncode(pyexc) << endl;
        }

        getOutputFromPy(pyControl, result);
        result.exception = std::make_shared<ScriptException>(std::move(pyexc));
        result.exception->context.push_back("Executing Python script");
        result.setReturnCode(500);
        return result;
    };
}

extern "C" {
    extern const char mldb_wrapper_start;
    extern const char mldb_wrapper_end;
    extern const size_t mldb_wrapper_size;
};

void
PythonPlugin::
injectMldbWrapper(PythonSubinterpreter & pyControl)
{
    std::string code(&mldb_wrapper_start, &mldb_wrapper_end);

#if 0    
    boost::python::dict d(pyControl.main_namespace);

    cerr << "main namespace values" << endl;
    for (boost::python::stl_input_iterator<std::string> it(d.keys()), end;  it != end;  ++it) {
            cerr << *it << endl;
    }

    boost::python::dict builtins(d["__builtins__"].attr("__dict__"));
    
    cerr << "builtin values" << endl;
    for (boost::python::stl_input_iterator<std::string> it(builtins.keys()), end;  it != end;  ++it) {
            cerr << *it << endl;
    }
#endif
    
    PyObject * out = PyRun_String(code.c_str(), Py_file_input, pyControl.main_namespace.ptr(), pyControl.main_namespace.ptr());
    
    if (!out) {
        PyErr_Print();
        throw boost::python::error_already_set();
    }
    else {
        Py_DECREF(out);
        cerr << "succeeded running string" << endl;
    }


#if 0    
    
    auto pyCode = boost::python::str(code);
    
    boost::python::exec(pyCode, pyControl.main_namespace);
#endif
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

struct AtInit {
    AtInit()
    {
        Py_Initialize();

        PyEval_InitThreads();
        mainThreadState = PyEval_SaveThread();
        PyEval_AcquireThread(mainThreadState);

        signal(SIGINT, SIG_DFL);

        namespace bp = boost::python;

        PyDateTime_IMPORT;
        from_python_converter< Date, DateFromPython >();

        from_python_converter< std::string, StringFromPyUnicode>();
        from_python_converter< Utf8String,  Utf8StringPyConverter>();
        bp::to_python_converter< Utf8String, Utf8StringPyConverter>();

        from_python_converter< RowPath, StrConstructableIdFromPython<RowPath> >();
        from_python_converter< ColumnPath, StrConstructableIdFromPython<ColumnPath> >();
        from_python_converter< CellValue, CellValueConverter >();

        from_python_converter< RowCellTuple,
                               Tuple3ElemConverter<ColumnPath, CellValue, Date> >();

        from_python_converter< std::vector<RowCellTuple>,
                               VectorConverter<RowCellTuple>>();

        from_python_converter< std::pair<RowPath, std::vector<RowCellTuple> >,
                               PairConverter<RowPath, std::vector<RowCellTuple> > >();

        from_python_converter< std::vector<std::pair<RowPath, std::vector<RowCellTuple> > >,
                               VectorConverter<std::pair<RowPath, std::vector<RowCellTuple> > > >();

        from_python_converter< ColumnCellTuple,
                               Tuple3ElemConverter<RowPath, CellValue, Date> >();

        from_python_converter< std::vector<ColumnCellTuple>,
                               VectorConverter<ColumnCellTuple>>();

        from_python_converter< std::pair<ColumnPath, std::vector<ColumnCellTuple> >,
                               PairConverter<ColumnPath, std::vector<ColumnCellTuple> > >();

        from_python_converter< std::vector<std::pair<ColumnPath, std::vector<ColumnCellTuple> > >,
                               VectorConverter<std::pair<ColumnPath, std::vector<ColumnCellTuple> > > >();

        from_python_converter<std::pair<string, string>,
                        PairConverter<string, string> >();

        bp::to_python_converter<std::pair<string, string>,
                        PairConverter<string, string> >();
        
        from_python_converter< Path, PathConverter>();

        from_python_converter< RestParams, RestParamsConverter>();
        bp::to_python_converter< RestParams, RestParamsConverter>();

        from_python_converter< Json::Value, JsonValueConverter>();
        bp::to_python_converter< Json::Value, JsonValueConverter> ();

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
        ;

        bp::class_<DatasetPy>("dataset", bp::no_init)
            .def("record_row", &DatasetPy::recordRow)
            .def("record_rows", &DatasetPy::recordRows)
            .def("record_column", &DatasetPy::recordColumn)
            .def("record_columns", &DatasetPy::recordColumns)
            .def("commit", &DatasetPy::commit);

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

        script.add_property("args", &PythonContext::getArgs);
        script.def("set_return", &PythonScriptContext::setReturnValue1);

        plugin.add_property("args", &PythonContext::getArgs);
        plugin.add_property("rest_params", &PythonPluginContext::getRestRequest);
        plugin.def("serve_static_folder",
                   &PythonPluginContext::serveStaticFolder);
        plugin.def("serve_documentation_folder",
                   &PythonPluginContext::serveDocumentationFolder);
        plugin.def("get_plugin_dir",
                   &PythonPluginContext::getPluginDirectory);
        plugin.def("set_return", &PythonPluginContext::setReturnValue);
        plugin.def("set_return", &PythonPluginContext::setReturnValue1);


        mldb.def("set_return", &PythonContext::setReturnValue1);
        mldb.def("log", bp::raw_function(logArgs, 1));
        mldb.def("log", &MldbPythonContext::logUnicode);
        mldb.def("log", &MldbPythonContext::logJsVal);
        mldb.def("perform", perform); // for 5 args
        mldb.def("perform", perform4); // for 4 args
        mldb.def("perform", perform3); // for 3 args
        mldb.def("perform", perform2); // for 2 args
        mldb.def("read_lines", readLines);
        mldb.def("read_lines", readLines1);
        mldb.def("ls", ls);
        mldb.def("get_http_bound_address", getHttpBoundAddress);
        mldb.def("create_dataset",
                   &DatasetPy::createDataset,
                   bp::return_value_policy<bp::manage_new_object>());
        mldb.def("create_procedure", &PythonProcedure::createPythonProcedure);
//         mldb.def("create_function", &PythonFunction::createPythonFunction);

        mldb.add_property("script", &MldbPythonContext::getScript);
        mldb.add_property("plugin", &MldbPythonContext::getPlugin);

        mldb.def("debugSetPathOptimizationLevel",
                 &MldbPythonContext::setPathOptimizationLevel);

        /****
         *  Functions
         *  **/

        bp::class_<MLDB::Any, boost::noncopyable>("any", bp::no_init)
            .def("as_json",   &MLDB::Any::asJson)
            ;

        bp::class_<FunctionInfo, boost::noncopyable>("function_info", bp::no_init)
            ;
        
        auto main_module = boost::python::import("__main__"); 
        auto main_namespace = main_module.attr("__dict__");

        PyEval_ReleaseLock();

        registerPluginType<PythonPlugin, PluginResource>
            (builtinPackage(),
             "python",
             "Load plugins or run scripts written in the Python language",
             "lang/Python.md.html",
             &PythonPlugin::handleTypeRoute);

    }

    ~AtInit() {
        PyThreadState_Swap(mainThreadState);
        Py_Finalize();
    }

    //in main thread
    PyThreadState * mainThreadState;

} atInit;


} // file scope

} // namespace MLDB

