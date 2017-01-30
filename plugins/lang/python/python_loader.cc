/** python_plugin_loader.cc
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    
    Plugin loader for Python plugins.
*/

#include <Python.h>

#include "mldb/server/mldb_server.h"
#include "mldb/core/dataset.h"
#include "mldb/core/plugin.h"
#include "mldb/core/procedure.h"
#include "mldb/base/scope.h"
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
#include "mldb/logging/logging.h"
#include "mldb_python_converters.h"
#include "mldb/types/any_impl.h"

#include "datetime.h"

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
        throw HttpReturnException(500, "Python evaluation pipe: "
                                  + string(strerror(errno)));
    
    Scope_Exit(if (fds[0] != -1) ::close(fds[0]); if (fds[1] != -1) ::close(fds[1]));

    res = fcntl(fds[1], F_SETFL, O_NONBLOCK);
    if (res == -1) {
        auto errno2 = errno;
        throw HttpReturnException(500, "Python evaluation fcntl: "
                                  + string(strerror(errno2)));
    }

    std::atomic<int> finished(0);
    std::thread t;

    // Write as much as we can.  In Linux, the default pipe buffer is
    // 64k, which is enough for most scripts.
    ssize_t written = write(fds[1], code.rawData(), code.rawLength());
    
    if (written == -1) {
        // Error writing.  Bail out.
        throw HttpReturnException
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
            throw HttpReturnException(500, "Python evaluation fcntl: "
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
        throw HttpReturnException
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
PythonPlugin(MldbServer * server,
             PolyConfig config,
             std::function<bool (const Json::Value & progress)> onProgress)
    : Plugin(server), initialGetStatus(true)
{

    PluginResource res = config.params.convert<PluginResource>();
    try {
        pluginCtx.reset(new PythonPluginContext(config.id, this->server,
                std::make_shared<LoadedPluginResource>
                                      (PYTHON,
                                       LoadedPluginResource::PLUGIN,
                                       config.id, res), routeHandlingMutex));
    }
    catch(const std::exception & exc) {
        throw HttpReturnException(400, MLDB::format("Exception opening plugin: %s", exc.what()));
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
        throw HttpReturnException(500, "Exception creating Python context", pyexc);

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

    string pluginDir = pluginCtx->pluginResource->getPluginDir().string();
    PyObject * pluginDirPy = PyString_FromString(pluginDir.c_str());
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
        auto scriptConfig =
            jsonDecodeStr<ScriptResource>(request.payload).toPluginConfig();

        std::shared_ptr<PythonScriptContext> scriptCtx;
        auto pluginRez =
            std::make_shared<LoadedPluginResource>(PYTHON,
                                                   LoadedPluginResource::SCRIPT,
                                                   "", scriptConfig);
        try {
            scriptCtx = std::make_shared<PythonScriptContext>(
                "script runner", static_cast<MldbServer *>(server), pluginRez);
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
        char argv1[] = "mldb-boost-python";
        char *argv[] = {argv1};
        int argc = sizeof(argv[0]) / sizeof(char *);
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
                 throw HttpReturnException(
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

void
PythonPlugin::
injectMldbWrapper(PythonSubinterpreter & pyControl)
{
    std::string code = R"code(

import unittest

class mldb_wrapper(object):

    import json as jsonlib

    class MldbBaseException(Exception):
        pass

    class ResponseException(MldbBaseException):
        def __init__(self, response):
            self.response = response

        def __str__(self):
            return ('Response status code: {r.status_code}. '
                    'Response text: {r.text}'.format(r=self.response))

    class TooManyRedirectsException(Exception):
        pass

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

        def __str__(self):
            return self.text

    class StepsLogger(object):

        def __init__(self, mldb):
            self.done_steps = set()
            self.log = mldb.log

        def log_progress_steps(self, progress_steps):
            from datetime import datetime
            from dateutil.tz import tzutc
            from dateutil.parser import parse as parse_date
            now = datetime.now(tzutc())
            for step in progress_steps:
                if 'ended' in step:
                    if step['name'] in self.done_steps:
                        continue
                    self.done_steps.add(step['name'])
                    ran_in = parse_date(step['ended']) - parse_date(step['started'])
                    self.log("{} completed in {} seconds - {} {}"
                            .format(step['name'], ran_in.total_seconds(),
                                    step['type'], step['value']))
                elif 'started' in step:
                    running_since = now - parse_date(step['started'])
                    self.log("{} running since {} seconds - {} {}"
                            .format(step['name'], running_since.total_seconds(),
                                    step['type'], step['value']))

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

        def _follow_redirect(self, url, counter):
            # somewhat copy pasted from _perform, but gives a nicer stacktrace
            # on failure
            if counter == 0:
                raise mldb_wrapper.TooManyRedirectsException()

            raw_res = self._mldb.perform('GET', url)
            response = mldb_wrapper.Response(url, raw_res)
            if response.status_code < 200 or response.status_code >= 400:
                raise mldb_wrapper.ResponseException(response)
            if response.status_code >= 300:
                return self._follow_redirect(response.headers['location'],
                                             counter - 1)
            return response

        def _perform(self, method, url, *args, **kwargs):
            raw_res = self._mldb.perform(method, url, *args, **kwargs)
            response = mldb_wrapper.Response(url, raw_res)
            if response.status_code < 200 or response.status_code >= 400:
                raise mldb_wrapper.ResponseException(response)
            if response.status_code >= 300:
                # 10 is the maximum count of redirects to follow
                return self._follow_redirect(response.headers['location'], 10)
            return response

        def log(self, thing):
            if type(thing) in [dict, list]:
                thing = mldb_wrapper.jsonlib.dumps(thing, indent=4,
                                                   ensure_ascii=False)
            if isinstance(thing, (str, unicode)):
                self._mldb.log(thing)
            else:
                self._mldb.log(str(thing))

        @property
        def script(self):
            return self._mldb.script

        @property
        def plugin(self):
            return self._mldb.plugin

        def get_http_bound_address(self):
            return self._mldb.get_http_bound_address()

        def get(self, url, data=None, **kwargs):
            query_string = []
            for k, v in kwargs.iteritems():
                if type(v) in [list, dict]:
                    v = mldb_wrapper.jsonlib.dumps(v)
                query_string.append([unicode(k), unicode(v)])
            return self._perform('GET', url, query_string, data)

        def _post_put(self, verb, url, data=None, async=False):
            if async:
                return self._perform(verb, url, [], data, [['async', 'true']])
            return self._perform(verb, url, [], data)

        def delete(self, url):
            return self._perform('DELETE', url)

        def delete_async(self, url):
            return self._perform('DELETE', url, [], {}, [['async', 'true']])

        def query(self, query):
            return self._perform('GET', '/v1/query', [], {
                'q' : query,
                'format' : 'table'
            }).json()

        def run_tests(self):
            import StringIO
            io_stream = StringIO.StringIO()
            runner = unittest.TextTestRunner(stream=io_stream, verbosity=2,
                                             buffer=True)
            if self.script.args:
                assert type(self.script.args) is list
                if self.script.args[0]:
                    argv = ['python'] + self.script.args
                else:
                    # avoid the only one empty arg issue
                    argv = None
            else:
                argv = None

            res = unittest.main(exit=False, argv=argv,
                                testRunner=runner).result
            self.log(io_stream.getvalue())

            if res.wasSuccessful():
                self.script.set_return("success")

        def post_run_and_track_procedure(self, payload, refresh_rate_sec=10):
            import threading

            if 'params' not in payload:
                payload['params'] = {}
            payload['params']['runOnCreation'] = False

            res = self.post('/v1/procedures', payload).json()
            proc_id = res['id']
            event = threading.Event()

            def monitor_progress():
                # wrap everything in a try/except because exceptions are not passed to
                # mldb.log by themselves.
                try:
                    # find run id
                    run_id = None
                    sl = mldb_wrapper.StepsLogger(self)
                    while not event.wait(refresh_rate_sec):
                        if run_id is None:
                            res = self.get('/v1/procedures/{}/runs'.format(proc_id)).json()
                            if res:
                                run_id = res[0]
                            else:
                                continue

                        res = self.get('/v1/procedures/{}/runs/{}'.format(proc_id, run_id)).json()
                        if res['state'] == 'executing':
                            sl.log_progress_steps(res['progress']['steps'])
                        else:
                            break

                except Exception as e:
                    self.log(str(e))
                    import traceback
                    self.log(traceback.format_exc())

            t = threading.Thread(target=monitor_progress)
            t.start()

            try:
                return self.post('/v1/procedures/{}/runs'.format(proc_id), {})
            except mldb_wrapper.ResponseException as e:
                return e.response
            finally:
                event.set()
                t.join()



class MldbUnitTest(unittest.TestCase):
    import json

    longMessage = True # Appends the user message to the normal message

    class _AssertMldbRaisesContext(object):
        """A context manager used to implement TestCase.assertRaises* methods.
        Inspired from python unittests.
        """

        def __init__(self, test_case, expected_regexp=None, status_code=None):
            self.failureException = test_case.failureException
            self.expected_regexp = expected_regexp
            self.status_code = status_code

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, tb):
            if exc_type is None:
                raise self.failureException(
                    "{0} not raised".format(mldb_wrapper.MldbBaseException))
            if not issubclass(exc_type, mldb_wrapper.MldbBaseException):
                # let unexpected exceptions pass through
                return False
            self.exception = exc_value # store for later retrieval
            if self.expected_regexp:
                import re
                expected_regexp = self.expected_regexp
                if isinstance(expected_regexp, basestring):
                    expected_regexp = re.compile(expected_regexp)
                if not expected_regexp.search(str(exc_value.response.text)):
                    raise self.failureException('"%s" does not match "%s"' %
                        (expected_regexp.pattern,
                         str(exc_value.response.text)))
            if self.status_code:
                if exc_value.response.status_code != self.status_code:
                    raise self.failureException(
                        "Status codes are not equal: {} != {}".format(
                            exc_value.response.status_code, self.status_code))
            return True

    def _get_base_msg(self, res, expected):
        return '{line}Result: {res}{line}Expected: {expected}'.format(
            line='\n' + '*' * 10 + '\n',
            res=MldbUnitTest.json.dumps(res, indent=4),
            expected=MldbUnitTest.json.dumps(expected, indent=4))

    def assertTableResultEquals(self, res, expected, msg=""):
        msg += self._get_base_msg(res, expected)
        self.assertEqual(len(res), len(expected), msg)
        self.assertNotEqual(len(res), 0, msg)
        res_keys = sorted(res[0])
        expected_keys = sorted(expected[0])
        self.assertEqual(res_keys, expected_keys, msg)

        # we'll make the order of `res` match the order of `expected`
        # this is a map that gives us the index in `res` of a column name
        colname_to_idx = {
            colname: index for index, colname in enumerate(res[0])}
        # this gives us the permutation we need to apply to `res`
        res_perm = [colname_to_idx[colname]
            for i, colname in enumerate(expected[0])]

        ordered_res = [[row[i] for i in res_perm] for row in res[1:]]

        for res_row, expected_row in zip(ordered_res, expected[1:]):
            self.assertEqual(res_row, expected_row)

    def assertFullResultEquals(self, res, expected, msg=""):
        msg += self._get_base_msg(res, expected)
        self.assertEqual(len(res), len(expected), msg)
        for res_row, expected_row in zip(res, expected):
            self.assertEqual(res_row["rowName"], expected_row["rowName"], msg)
            res_columns = sorted(res_row["columns"])
            expected_columns = sorted(expected_row["columns"])
            self.assertEqual(res_columns, expected_columns, msg)

    def assertMldbRaises(self, expected_regexp=None, status_code=None):
        return MldbUnitTest._AssertMldbRaisesContext(self, expected_regexp,
                                                     status_code)

    )code"; //this is python code

    auto pyCode = boost::python::str(code);
    boost::python::exec(pyCode, pyControl.main_namespace);
}


namespace {

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

