/** python_plugin_loader.cc
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
    
    Plugin loader for Python plugins.
*/

#include <Python.h>

#include "mldb/server/mldb_server.h"
#include "mldb/core/dataset.h"
#include "mldb/core/plugin.h"
#include "mldb/core/procedure.h"
#include "mldb/server/plugin_resource.h"

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
#include "mldb/http/http_exception.h"

#include "mldb/rest/rest_request_binding.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/dtoa.h"

#include "mldb/jml/utils/file_functions.h"
#include "mldb/http/logs.h"
#include "mldb_python_converters.h"
#include "mldb/types/any_impl.h"

#include "datetime.h"

using namespace std;
using namespace Datacratic::Python;
//namespace bp = boost::python;


namespace Datacratic {
namespace MLDB {




/*****************************************************************************/
/* PYTHON PLUGIN                                                             */
/*****************************************************************************/

struct PythonPlugin: public Plugin {
    PythonPlugin(MldbServer * server,
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
    runPythonScript(std::shared_ptr<PythonContext> itl,
                    PackageElement elementToRun,
                    const RestRequest & request,
                    RestRequestParsingContext & context,
                    PythonSubinterpreter & pyControl);
    
    static ScriptOutput
    runPythonScript(std::shared_ptr<PythonContext> itl,
                    PackageElement elementToRun,
                    PythonSubinterpreter & pyControl);

    static RestRequestMatchResult
    handleTypeRoute(RestDirectory * server, RestConnection & conn,
                    const RestRequest & request,
                    RestRequestParsingContext & context);

    static void injectMldbWrapper(PythonSubinterpreter & pyControl);

    std::shared_ptr<PythonPluginContext> itl;
    std::shared_ptr<MldbPythonContext> mldbPy;

    mutable std::mutex routeHandlingMutex;

    mutable bool initialGetStatus;
    mutable ScriptOutput last_output;
};


PythonPlugin::
PythonPlugin(MldbServer * server,
             PolyConfig config,
             std::function<bool (const Json::Value & progress)> onProgress)
    : Plugin(server), initialGetStatus(true)
{

    PluginResource res = config.params.convert<PluginResource>();
    try {
        itl.reset(new PythonPluginContext(config.id, this->server,
                std::make_shared<LoadedPluginResource>
                                      (PYTHON,
                                       LoadedPluginResource::PLUGIN,
                                       config.id, res), routeHandlingMutex));
    }
    catch(const std::exception & exc) {
        throw HttpReturnException(400, ML::format("Exception opening plugin: %s", exc.what()));
    }

    addRouteSyncJsonReturn(itl->router, "/lastoutput", {"GET"},
            "Return the output of the last call to the plugin",
            "Output of the last call to the plugin",
            &PythonPlugin::getLastOutput,
            this);


    mldbPy.reset(new MldbPythonContext());
    mldbPy->setPlugin(itl);

    Utf8String scriptSource = itl->pluginResource->getScript(PackageElement::MAIN);

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
        JML_TRACE_EXCEPTIONS(false);
        pyControl.main_namespace["mldb"] =
            boost::python::object(boost::python::ptr(mldbPy.get()));
    } catch (const boost::python::error_already_set & exc) {
        ScriptException pyexc = convertException(pyControl, exc, "PyPlugin init");

        {
            std::unique_lock<std::mutex> guard(itl->logMutex);
            LOG(itl->loader) << jsonEncode(pyexc) << endl;
        }

        JML_TRACE_EXCEPTIONS(false);
        throw HttpReturnException(500, "Exception creating Python context", pyexc);

    }

    try {
        boost::python::object obj = boost::python::exec(boost::python::str(scriptSource.rawString()),
                                                        pyControl.main_namespace);
    } catch (const boost::python::error_already_set & exc) {
        ScriptException pyexc = convertException(pyControl, exc, "Running PyPlugin script");

        {
            std::unique_lock<std::mutex> guard(itl->logMutex);
            LOG(itl->loader) << jsonEncode(pyexc) << endl;
        }

        JML_TRACE_EXCEPTIONS(false);

        string context = "Exception executing Python initialization script";
        ScriptOutput result = exceptionToScriptOutput(pyControl, pyexc, context);
        throw HttpReturnException(400, context, result);
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

    string pluginDir = itl->pluginResource->getPluginDir().string();
    PyObject * pluginDirPy = PyString_FromString(pluginDir.c_str());
    ExcAssert(pluginDirPy);

    PyList_Insert( sysPath, 0, pluginDirPy );

    // change working dir to script dir
//     PyRun_SimpleString(ML::format("import os\nos.chdir(\"%s\")", pluginDir).c_str());
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
    return itl->pluginResource->version;
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
//             rtn = itl->getStatus();
//         } catch (const boost::python::error_already_set & exc) {
//             ScriptException pyexc = convertException(pyControl, exc, "PyPlugin get status");
// 
//             {
//                 std::unique_lock<std::mutex> guard(itl->logMutex);
//                 LOG(itl->loader) << jsonEncode(pyexc) << endl;
//             }
// 
//             JML_TRACE_EXCEPTIONS(false);
//             string context = "Exception in Python status call";
//             ScriptOutput result = exceptionToScriptOutput(
//                 pyControl, pyexc, context);
//             throw HttpReturnException(400, context, result);
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
    if (itl->handleDocumentation)
        return itl->handleDocumentation(connection, request, context);

    return RestRequestRouter::MR_NO;
}

RestRequestMatchResult
PythonPlugin::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    // First, check for a route
    auto res = itl->router.processRequest(connection, request, context);
    if (res != RestRequestRouter::MR_NO)
        return res;

    // check if 
    bool isChild = request.header.headers.find("__mldb_child_call") != request.header.headers.end();
    PythonSubinterpreter pyControl(isChild);
    addPluginPathToEnv(pyControl);

    // Second, check for a generic request handler
    if(itl->hasRequestHandler) {
        try {
            RestRequestMatchResult rtn;
            {
                JML_TRACE_EXCEPTIONS(false);

                pyControl.releaseGil();

                itl->restRequest = make_shared<PythonRestRequest>(request, context);
                itl->setReturnValue(Json::Value());
                auto jsonResult = runPythonScript(itl,
                        PackageElement::ROUTES, request, context, pyControl);

                last_output = ScriptOutput();

                getOutputFromPy(pyControl, last_output, false);
                if(jsonResult.exception) {
                    connection.sendResponse(400, jsonEncodeStr(jsonResult), "application/json");
                }

                if(!jsonResult.result) {
                    return RestRequestRouter::MR_NO;
                }

                connection.sendResponse(jsonResult.getReturnCode(), jsonResult.result);
                return RestRequestRouter::MR_YES;
            }

            return rtn;
        } catch (const boost::python::error_already_set & exc) {
            ScriptException pyexc = convertException(pyControl, exc, "Handling PyPlugin route");

            {
                std::unique_lock<std::mutex> guard(itl->logMutex);
                LOG(itl->loader) << jsonEncode(pyexc) << endl;
            }

            JML_TRACE_EXCEPTIONS(false);
            string context = "Exception in Python request handler";
            ScriptOutput result = exceptionToScriptOutput(
                    pyControl, pyexc, context);
            throw HttpReturnException(400, context, result);
        }
   
    }
    // Otherwise we simply don't handle it
    return RestRequestRouter::MR_NO;
}

RestRequestMatchResult
PythonPlugin::
handleTypeRoute(RestDirectory * server,
                RestConnection & conn,
                const RestRequest & request,
                RestRequestParsingContext & context)
{
    if (context.resources.back() == "run") {
        auto scriptConfig = jsonDecodeStr<ScriptResource>(request.payload).toPluginConfig();

        std::shared_ptr<PythonScriptContext> titl;
        auto pluginRez = 
            std::make_shared<LoadedPluginResource>(PYTHON,
                                                   LoadedPluginResource::SCRIPT,
                                                   "", scriptConfig);
        try {
            titl = std::make_shared<PythonScriptContext>("script runner",
                                                     static_cast<MldbServer *>(server),
                                                     pluginRez);
        }
        catch(const std::exception & exc) {
            conn.sendResponse(400,
                          jsonEncodeStr(ML::format("Exception opening script: %s", exc.what())),
                          "application/json");
        }

        bool isChild = request.header.headers.find("__mldb_child_call") != request.header.headers.end();
        PythonSubinterpreter pyControl(isChild);
        auto result = runPythonScript(titl, PackageElement::MAIN, pyControl);
        conn.sendResponse(result.exception ? 400 : 200,
                          jsonEncodeStr(result), "application/json");

        return RestRequestRouter::MR_YES;
    }

    return RestRequestRouter::MR_NO;
}

ScriptOutput
PythonPlugin::
runPythonScript(std::shared_ptr<PythonContext> titl, 
        PackageElement elementToRun,
        PythonSubinterpreter & pyControl)
{
    RestRequest request;
    RestRequestParsingContext context(request);

    return runPythonScript(titl, elementToRun, request, context, pyControl);
}

ScriptOutput
PythonPlugin::
runPythonScript(std::shared_ptr<PythonContext> titl, 
        PackageElement elementToRun,
        const RestRequest & request,
        RestRequestParsingContext & context,
        PythonSubinterpreter & pyControl)
{
    auto mldbPy = std::make_shared<MldbPythonContext>();
    bool isScript = titl->pluginResource->scriptType == LoadedPluginResource::ScriptType::SCRIPT;
    if(isScript) {
        mldbPy->setScript(static_pointer_cast<PythonScriptContext>(titl));
    }
    else {
        mldbPy->setPlugin(static_pointer_cast<PythonPluginContext>(titl));
    }

    Utf8String scriptSource = titl->pluginResource->getScript(elementToRun);

    pyControl.acquireGil();

    try {
        JML_TRACE_EXCEPTIONS(false);
        pyControl.main_namespace["mldb"] = boost::python::object(boost::python::ptr(mldbPy.get()));
        injectMldbWrapper(pyControl);

    } catch (const boost::python::error_already_set & exc) {
        ScriptException pyexc = convertException(pyControl, exc, "PyRunner init");

        {
            std::unique_lock<std::mutex> guard(titl->logMutex);
            LOG(titl->loader) << jsonEncode(pyexc) << endl;
        }

        JML_TRACE_EXCEPTIONS(false);

        ScriptOutput result;

        result.exception = std::make_shared<ScriptException>(std::move(pyexc));
        result.exception->context.push_back("Initializing Python script");
        return result;
    }

    ScriptOutput result;
    auto scriptSourceStr = boost::python::str(scriptSource.rawString());

    auto pySetArgv = [] {
        char argv1[] = "mldb-boost-python";
        char *argv[] = {argv1};
        int argc = sizeof(argv[0]) / sizeof(char *);
        PySys_SetArgv(argc, argv);
    };

    // if we're simply executing the body of the script
    try {
        if(elementToRun == PackageElement::MAIN) {
            JML_TRACE_EXCEPTIONS(false);
            pySetArgv();
            boost::python::object obj =
                boost::python::exec(scriptSourceStr, pyControl.main_namespace);

            getOutputFromPy(pyControl, result);

            if(isScript) {
                auto ctitl = static_pointer_cast<PythonScriptContext>(titl);
                result.result = ctitl->rtnVal;
                result.setReturnCode(ctitl->rtnCode);
                for (auto & l: ctitl->logs)
                    result.logs.emplace_back(std::move(l));
                std::stable_sort(result.logs.begin(), result.logs.end());
            }

            return result;
        }
        // if we need to call the routes function
        else if(elementToRun == PackageElement::ROUTES) {

            pySetArgv();
            boost::python::object obj
                = boost::python::exec(scriptSourceStr, pyControl.main_namespace);

            result.result = titl->rtnVal;
            result.setReturnCode(titl->rtnCode);
            return result;
        }
        else {
            throw ML::Exception("Unknown element to run!!");
        }
    } catch (const boost::python::error_already_set & exc) {
        ScriptException pyexc = convertException(pyControl, exc, "Running PyRunner script");

        {
            std::unique_lock<std::mutex> guard(titl->logMutex);
            LOG(titl->loader) << jsonEncode(pyexc) << endl;
        }

        getOutputFromPy(pyControl, result);
        result.exception = std::make_shared<ScriptException>(std::move(pyexc));
        result.exception->context.push_back("Executing Python script");
        return result;
    };
}

void
PythonPlugin::
injectMldbWrapper(PythonSubinterpreter & pyControl)
{
    std::string code = R"code(


class mldb_wrapper(object):

    import json as jsonlib

    @staticmethod
    def wrap(mldb):
        return Wrapped(mldb)

    class ResponseException(Exception):
        def __init__(self, response):
            self.response = response

        def __str__(self):
            return ('Response status code: {r.status_code}. '
                    'Response text: {r.text}'.format(r=self.response))

    class Response(object):

        def __init__(self, url, raw_response):
            self.url         = url
            self.headers     = {k: v for k, v in
                                raw_response.get('headers', {})}
            self.status_code = raw_response['statusCode']
            self.text        = raw_response.get('response', '')
            self.raw         = raw_response

            self.apparent_encoding = 'unimplemented'
            self.close             = 'unimplemented'
            self.conection         = 'unimplemented'
            self.elapsed           = 'unimplemented'
            self.encoding          = 'unimplemented'
            self.history           = 'unimplemented'
            self.iter_content      = 'unimplemented'
            self.iter_lines        = 'unimplemented'
            self.links             = 'unimplemented'
            self.ok                = 'unimplemented'
            self.raise_for_status  = 'unimplemented'
            self.reason            = 'unimplemented'
            self.request           = 'unimplemented'

        def json(self):
            return mldb_wrapper.jsonlib.loads(self.text)

    class wrap(object):
        def __init__(self, mldb):
            self._mldb = mldb
            import functools
            self.post = functools.partial(self._post_put, 'POST')
            self.put = functools.partial(self._post_put, 'PUT')
            self.post_async = functools.partial(self._post_put, 'POST',
                                                async=True)
            self.put_async = functools.partial(self._post_put, 'PUT',
                                               async=True)
            self.create_dataset = self._mldb.create_dataset

        def _perform(self, method, url, *args, **kwargs):
            raw_res = self._mldb.perform(method, url, *args, **kwargs)
            response = mldb_wrapper.Response(url, raw_res)
            if response.status_code < 200 or response.status_code >= 400:
                raise mldb_wrapper.ResponseException(response)
            return response

        def log(self, thing):
            if type(thing) in [dict, list]:
                thing = mldb_wrapper.jsonlib.dumps(thing, indent=4)
            self._mldb.log(str(thing))

        @property
        def script(self):
            return self._mldb.script

        def get(self, url, **kwargs):
            query_string = []
            for k, v in kwargs.iteritems():
                if type(v) in [list, dict]:
                    v = mldb_wrapper.jsonlib.dumps(v)
                query_string.append([unicode(k), unicode(v)])
            return self._perform('GET', url, query_string)

        def _post_put(self, verb, url, data=None, async=False):
            if async:
                return self._perform(verb, url, [], data, [['async', 'true']])
            return self._perform(verb, url, [], data)

        def delete(self, url):
            return self._perform('DELETE', url)

        def delete_async(self, url):
            return self._perform('DELETE', url, [], {}, [['async', 'true']])

        def query(self, query):
            return self._perform('GET', '/v1/query', [
                ['q', query],
                ['format', 'table']
            ]).json()

        def run_tests(self):
            import unittest
            if self.script.args:
                assert type(self.script.args) is list
                if self.script.args[0]:
                    argv = ['python'] + self.script.args
                else:
                    # avoid the only one empty arg issue
                    argv = None
            else:
                argv = None

            res = unittest.main(exit=False, argv=argv).result
            self.log(res)
            got_err = False
            for err in res.errors + res.failures:
                got_err = True
                self.log(str(err[0]) + "\n" + err[1])

            if not got_err:
                self.script.set_return("success")

    )code"; //this is python code

    auto pyCode = boost::python::str(code);
    boost::python::exec(pyCode, pyControl.main_namespace);
}


namespace {

// The set function in FunctionOutput and FunctionContext are both overloaded due to
// the UTF8String variants. This the helpers used to provide the required
// casting type to extract the function pointer of one of the overloaded types.
template<typename T>
struct SetFn
{
    typedef void (FunctionContext::* CtxType)(const std::string&, const T&, Date);
    typedef void (FunctionOutput::* OutputType)(const std::string&, const T&, Date);
};


std::string pyObjectToString(PyObject * pyObj)
{
    namespace bp = boost::python;

    if(PyInt_Check(pyObj)) {
        return boost::lexical_cast<std::string>(bp::extract<int>(pyObj));
    }
    else if(PyFloat_Check(pyObj)) {
        return boost::lexical_cast<std::string>(bp::extract<float>(pyObj));
    }
    else if(PyString_Check(pyObj)) {
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

        from_python_converter< RowName, StrConstructableIdFromPython<RowName> >();
        from_python_converter< ColumnName, StrConstructableIdFromPython<ColumnName> >();
        from_python_converter< CellValue, CellValueConverter >();

        from_python_converter< RowCellTuple,
                               Tuple3ElemConverter<ColumnName, CellValue, Date> >();

        from_python_converter< std::vector<RowCellTuple>,
                               VectorConverter<RowCellTuple>>();

        from_python_converter< std::pair<RowName, std::vector<RowCellTuple> >,
                               PairConverter<RowName, std::vector<RowCellTuple> > >();

        from_python_converter< std::vector<std::pair<RowName, std::vector<RowCellTuple> > >,
                               VectorConverter<std::pair<RowName, std::vector<RowCellTuple> > > >();

        from_python_converter< ColumnCellTuple,
                               Tuple3ElemConverter<RowName, CellValue, Date> >();

        from_python_converter< std::vector<ColumnCellTuple>,
                               VectorConverter<ColumnCellTuple>>();

        from_python_converter< std::pair<ColumnName, std::vector<ColumnCellTuple> >,
                               PairConverter<ColumnName, std::vector<ColumnCellTuple> > >();

        from_python_converter< std::vector<std::pair<ColumnName, std::vector<ColumnCellTuple> > >,
                               VectorConverter<std::pair<ColumnName, std::vector<ColumnCellTuple> > > >();

        from_python_converter<std::pair<string, string>,
                        PairConverter<string, string> >();

        bp::to_python_converter<std::pair<string, string>,
                        PairConverter<string, string> >();

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
        mldb.def("create_dataset",
                   &DatasetPy::createDataset,
                   bp::return_value_policy<bp::manage_new_object>());
        mldb.def("create_procedure", &PythonProcedure::createPythonProcedure);
//         mldb.def("create_function", &PythonFunction::createPythonFunction);

        mldb.add_property("script", &MldbPythonContext::getScript);
        mldb.add_property("plugin", &MldbPythonContext::getPlugin);

        /****
         *  Functions
         *  **/

        bp::class_<Datacratic::Any, boost::noncopyable>("any", bp::no_init)
               .def("as_json",   &Datacratic::Any::asJson)
            ;

        bp::class_<FunctionInfo, boost::noncopyable>("function_info", bp::no_init)
            ;
        
        bp::class_<FunctionOutput, boost::noncopyable>("function_output", bp::init<>())
            .def("set_str", (SetFn<std::string>::OutputType) &FunctionOutput::setT<std::string>)
            ;

        bp::class_<FunctionApplier, boost::noncopyable>("function_applier", bp::no_init)
            ;


//         const Datacratic::Any& (FunctionContext::*functionContextGetStr)(const std::string &) = &FunctionContext::get;

        bp::class_<FunctionContext, boost::noncopyable>("FunctionContext", bp::no_init)
//                .def("get",      functionContextGetStr)
//                .def("getStr",   &FunctionContext::get<std::string>)
            .def("setStr",   (SetFn<std::string>::CtxType) &FunctionContext::setT<std::string>)
            .def("setInt",   (SetFn<int>::CtxType) &FunctionContext::setT<int>)
            .def("setFloat", (SetFn<float>::CtxType) &FunctionContext::setT<float>)
            ;

//         bp::class_<FunctionInfo, std::shared_ptr<FunctionInfo>, boost::noncopyable>("", bp::no_init)
//             .def("subject_count", &BehaviourDomain::subjectCount)
//             ;

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
} // namespace Datacratic
