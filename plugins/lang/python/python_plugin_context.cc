/** python_plugin_context.cc
    Francois Maillet, 6 mars 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "python_plugin_context.h"
#include "mldb/server/static_content_handler.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/plugins/for_each_line.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/base/optimized_path.h"
#include "mldb/utils/log.h"
#include <boost/regex.hpp>
#include <boost/algorithm/string.hpp>
#include <memory>


using namespace std;

namespace fs = boost::filesystem;


namespace MLDB {

/****************************************************************************/
/* PythonSubinterpreter                                                     */
/****************************************************************************/

std::mutex PythonSubinterpreter::youShallNotPassMutex;

PythonSubinterpreter::
PythonSubinterpreter(bool isChild) : isChild(isChild)
{
    if(!isChild) {
        lock = std::unique_ptr<std::lock_guard<std::mutex>>(
                new std::lock_guard<std::mutex>(PythonSubinterpreter::youShallNotPassMutex));
    }

    // acquire gilles
    PyEval_AcquireLock();
    hasGil = true;

    // Create the sub interpreter
    interpState = Py_NewInterpreter();
    threadState = PyThreadState_New(interpState->interp);

    // change current thread state
    savedThreadState = PyThreadState_Swap(threadState);

    main_module = boost::python::import("__main__");
    main_namespace = main_module.attr("__dict__");

    injectOutputLoggingCode();
}

PythonSubinterpreter::
~PythonSubinterpreter()
{
    acquireGil();

    // release thread state
    PyThreadState_Swap(NULL);

    PyThreadState_Clear(threadState);
    PyThreadState_Delete(threadState);

    // destroy the interpreter
    PyThreadState_Swap(interpState);
    Py_EndInterpreter(interpState);

    PyThreadState_Swap(savedThreadState);

    // release gilles
    PyEval_ReleaseLock();
}

void PythonSubinterpreter::
acquireGil()
{
    if(hasGil) return;

    // acquire gilles
    PyEval_AcquireLock();
    hasGil = true;

    // change current thread state
    PyThreadState_Swap(threadState);
}

void PythonSubinterpreter::
releaseGil()
{
    if(!hasGil) return;

    // release thread state
    PyThreadState_Swap(NULL);

    // release gilles
    PyEval_ReleaseLock();
    hasGil = false;
}

ScriptException
convertException(PythonSubinterpreter & pyControl,
        const boost::python::error_already_set & exc2,
        const std::string & context)
{
    using namespace boost::python;
    using namespace boost;

    pyControl.acquireGil();

    PyObject *exc,*val,*tb;
    object formatted_list, formatted;
    PyErr_Fetch(&exc,&val,&tb);
    handle<> hexc(exc),hval(allow_null(val)),htb(allow_null(tb));
    object traceback(import("traceback"));

    ScriptException result;

    if(htb) {
        object tbb(htb);
        result.lineNumber = extract<long>(tbb.attr("tb_lineno"));
    }

    // why is this not always working? for plugins it doesn't look like it is...
    if(val && PyString_Check(val))
        result.message = Utf8String(extract<string>(val));

    if (!tb) {
        object format_exception_only(traceback.attr("format_exception_only"));
        formatted_list = format_exception_only(hexc,hval);
    } else {
        object format_exception(traceback.attr("format_exception"));
        formatted_list = format_exception(hexc,hval,htb);
    }

    boost::python::ssize_t n = boost::python::len(formatted_list);
    result.stack.reserve(n);

    for (boost::python::ssize_t i = 0; i < n; ++i) {
        string str = extract<string>(formatted_list[i])();
        ScriptStackFrame frame;
        frame.where = str;
        result.stack.push_back(frame);
    }

    // TODO. this is a pretty horrible hack to get the line number of a syntax error exception
    // for some reason the usual way to get the info does not work for that specific exception
    // should revisit this
    if(result.lineNumber == -1 && result.stack.size() == 1 &&
       boost::starts_with(result.stack[0].where.rawString(), "SyntaxError")) {

        // SyntaxError: ('invalid syntax', ('<string>', 2, 3, 'a b\\n'))
        boost::regex pattern("SyntaxError: \\('invalid syntax', \\('.*', ([\\d]+), ([\\d]+), '(.*)'\\)\\)\n");

        boost::smatch what;
        if(boost::regex_match(result.stack[0].where.rawString(),
                              what, pattern, boost::match_extra)) {
            result.lineNumber = std::stoi(what[1]);
            result.columnStart = std::stoi(what[2]);
            result.lineContents = what[3];
        }
    }

    result.context = {context};

    return result;

}


/*****************************************************************************/
/* PYTHON STDOUT/ERR EXTRACTION CODE                                         */
/*****************************************************************************/

void injectOutputLoggingCode()
{
    std::string stdOutErr = R"foo(
class CatchOutContainer:
    def __init__(self):
        import json as _json
        self._json = _json

        import datetime as _datetime
        self._datetime = _datetime

        self.value = []
    def write(self, txt, method):
        if len(txt.strip()) == 0: return    # ignore whitespaces
        self.value.append(self._json.dumps(
                    [self._datetime.datetime.now().isoformat(), method, txt]))

class CatchOutErr:
    def __init__(self, catchOut, method):
        self.catchOut=catchOut
        self.method=method
    def write(self, txt):
        self.catchOut.write(txt, self.method)
    def flush(self):
        pass


catchOut = CatchOutContainer()
catctOutErr = CatchOutErr(catchOut, "stderr")
catctOutOut = CatchOutErr(catchOut, "stdout")

import sys as _sys
_sys.stdout = catctOutOut
_sys.stderr = catctOutErr

)foo"; //this is python code to redirect stdouts/stderr

    int res = PyRun_SimpleString(stdOutErr.c_str()); //invoke code to redirect
    if (res) {
        PyErr_Print(); //make python print any errors, unfortunately to console
        throw HttpReturnException
            (500, "Couldn't inject Python code (see error message on console). "
             "Have you installed python_dependenies (json and datetime) and "
             "properly set up your virtual environment?");
    }
}

void getOutputFromPy(PythonSubinterpreter & pyControl,
                     ScriptOutput & result,
                     bool reset)
{
    pyControl.acquireGil();

    PyObject *outCatcher = PyObject_GetAttrString(pyControl.main_module.ptr(),"catchOut"); //get our catchOutErr created above

    // Until we figure out WTF is going on here...
    if (!outCatcher) {
        throw HttpReturnException
            (500, "Couldn't extract output from injected Python code.  Look for "
             "an earlier error message on the console.");
        return;
    }
    
    PyErr_Print(); //make python print any errors
    PyObject *outOutput = PyObject_GetAttrString(outCatcher,"value"); //get the stdout and stderr from our catchOutErr object
    
    if(outOutput) {
        boost::python::list lst = boost::python::extract<boost::python::list>(outOutput);
        for(int i = 0; i < len(lst); i++) {
            boost::python::object obj = boost::python::object(lst[i]);
            if(obj.ptr() == Py_None) continue;
            auto p = Json::parse(PyString_AsString(obj.ptr()));
            if(!p.isArray()) continue;

            vector<Utf8String> parts;
            for(int i=0; i<p.size(); i++)
                parts.emplace_back(p[i].asString());

            ExcAssertEqual(parts.size(), 3);

            Date ts = Date::parseIso8601DateTime(parts[0].rawString() + "Z");
            std::string stream = parts[1].rawString();

            result.logs.emplace_back(ts, stream, std::move(parts[2]));
        }
    }

    Py_DecRef(outOutput);
    Py_DecRef(outCatcher);

    // reset logging code
    if(reset) {
        injectOutputLoggingCode();
    }

};

ScriptOutput exceptionToScriptOutput(PythonSubinterpreter & pyControl,
                                           ScriptException & exc,
                                           const string & context)
{
    ScriptOutput result;

    result.exception = std::make_shared<ScriptException>(std::move(exc));
    result.exception->context.push_back(context);

    getOutputFromPy(pyControl, result);

    return result;
}

/****************************************************************************/
/* PythonRestRequest                                                        */
/****************************************************************************/

PythonRestRequest::
PythonRestRequest(const RestRequest & request,
                  RestRequestParsingContext & context)
{
    remaining = context.remaining;
    verb = request.verb;
    resource = request.resource;
    payload = request.payload;
    contentType = request.header.contentType;
    contentLength = request.header.contentLength;

    for(const std::pair<Utf8String, Utf8String> & p : request.params) {
        boost::python::list inner_list;
        inner_list.append(p.first);
        inner_list.append(p.second);
        restParams.append(inner_list);
    }

    for(auto it = request.header.headers.begin();
            it != request.header.headers.end(); it++) {
        headers[it->first] = it->second;
    }
}


/****************************************************************************/
/* HELPER FUNCTION                                                          */
/****************************************************************************/
Json::Value
perform2(MldbPythonContext * mldbCon,
        const std::string & verb,
        const std::string & resource)
{
    return perform(mldbCon, verb, resource);
}


Json::Value
perform3(MldbPythonContext * mldbCon,
        const std::string & verb,
        const std::string & resource,
        const RestParams & params)
{
    return perform(mldbCon, verb, resource, params);
}

Json::Value
perform4(MldbPythonContext * mldbCon,
        const std::string & verb,
        const std::string & resource,
        const RestParams & params,
        Json::Value payload)
{
    return perform(mldbCon, verb, resource, params, payload);
}


Json::Value
perform(MldbPythonContext * mldbCon,
        const std::string & verb,
        const std::string & resource,
        const RestParams & params,
        Json::Value payload,
        const RestParams & headers)
{
    HttpHeader header;
    header.verb = verb;
    header.resource = resource;
    header.queryParams = params;
    for (auto & h: headers)
        header.headers.insert({h.first.toLower().extractAscii(), h.second.extractAscii()});

    RestRequest request(header, payload.toString());
    InProcessRestConnection connection;

    // add magic token to notify the receiver that this is a child call
    if(resource.find("/plugins/") != std::string::npos) {
        // if it's a python plugin creation call
        if(payload.get("type", Json::Value()).asString() == "python") {
            auto confParams = payload.get("params", Json::Value());
            auto argsParams = confParams.get("args", Json::Value());
            argsParams["__mldb_child_call"] = "true";
            confParams["args"] = argsParams;
            payload["params"] = confParams;
            request.payload = payload.toString();
        }
        else {
            request.header.headers.insert(make_pair("__mldb_child_call", "true"));
        }
    }


    // save current thread state and release lock
    PyThreadState* threadState = PyThreadState_Get();
    PyThreadState_Swap(NULL);
    PyEval_ReleaseLock();

    mldbCon->getPyContext()->server->handleRequest(connection, request);

    // relock and restore thread state
    PyEval_AcquireLock();
    PyThreadState_Swap(threadState);

    Json::Value result;
    result["statusCode"] = connection.responseCode;

    if (!connection.contentType.empty())
        result["contentType"] = connection.contentType;
    if (!connection.headers.empty()) {
        Json::Value headers(Json::ValueType::arrayValue);
        for(const pair<Utf8String, Utf8String> & h : connection.headers) {
            Json::Value elem(Json::ValueType::arrayValue);
            elem.append(h.first);
            elem.append(h.second);
            headers.append(elem);
        }
        result["headers"] = headers;
    }
    if (!connection.response.empty())
        result["response"] = connection.response;

    return result;
}

Json::Value
readLines1(MldbPythonContext * mldbCon,
          const std::string & path)
{
    return readLines(mldbCon, path);
}

Json::Value
readLines(MldbPythonContext * mldbCon,
          const std::string & path, int maxLines)
{
    filter_istream stream(path);

    Json::Value lines(Json::arrayValue);
    auto onLine = [&] (const char * line,
                       size_t length,
                       int64_t lineNum)
        {
            lines.append(line);
        };

    auto logger = getMldbLog("python");
    forEachLine(stream, onLine, logger, 1 /* numThreads */, false /* ignore exc */,
                    maxLines);

    return lines;
}

Json::Value
ls(MldbPythonContext * mldbCon,
   const std::string & dir)
{
    std::vector<std::string> dirs;
    std::map<std::string, FsObjectInfo> objects;

    auto onSubdir = [&] (const std::string & dirName,
                         int depth)
        {
            dirs.push_back(dirName);
            return false;
        };

    auto onObject = [&] (const std::string & uri,
                         const FsObjectInfo & info,
                         const OpenUriObject & open,
                         int depth)
        {
            objects[uri] = info;
            return true;
        };

    forEachUriObject(dir, onObject, onSubdir);

    Json::Value result;
    result["dirs"] = jsonEncode(dirs);
    result["objects"] = jsonEncode(objects);

    return result;
}

string
getHttpBoundAddress(MldbPythonContext * mldbCon)
{
    return mldbCon->getPyContext()->server->httpBoundAddress;
}


/****************************************************************************/
/* PYTHON CONTEXT                                                           */
/****************************************************************************/

void PythonContext::
log(const std::string & message)
{
    LOG(category) << message << endl;
    logs.emplace_back(Date::now(), "log", Utf8String(message));
}


Json::Value PythonContext::
getArgs() const
{
    return jsonEncode(pluginResource->args);
}

void PythonContext::
setReturnValue(const Json::Value & rtn, unsigned returnCode)
{
    if (returnCode == 0) {
        throw MLDB::Exception("Cannot set return code to 0");
    }
    rtnVal = rtn;
    rtnCode = returnCode;
}

void PythonContext::
setReturnValue1(const Json::Value & rtn)
{
    setReturnValue(rtn);
}

void PythonContext::
resetReturnValue()
{
    rtnCode = 0;
}

/****************************************************************************/
/* PYTHON PLUGIN CONTEXT                                                    */
/****************************************************************************/

// TODO probably some python rtn object
void PythonPluginContext::
setStatusHandler(PyObject * callback)
{
    if(!callback)
        throw MLDB::Exception("Must specify handler function");

    auto localsPlugin = boost::python::object(boost::python::ptr(mldbContext));
    getStatus = [=] ()
        {
            return boost::python::call<Json::Value>(callback, localsPlugin);
        };
}

void PythonPluginContext::
serveStaticFolder(const std::string & route, const std::string & dir)
{
    if(route.empty() || dir.empty()) {
        throw MLDB::Exception("Route and static directory cannot be empty "
                "for serving static folder");
    }

    fs::path fullDir(fs::path(getPluginDirectory()) / fs::path(dir));
    if(!fs::exists(fullDir)) {
        throw MLDB::Exception("Cannot serve static folder for path that does "
                "not exist: " + fullDir.string());
    }

    string route_pattern = "/" + boost::replace_all_copy(route, "/", "") + "/(.*)";
    router.addRoute(Rx(route_pattern, "<resource>"),
                    "GET", "Static content",
                    getStaticRouteHandler("file://" + fullDir.string(), server),
                    Json::Value());
}

void PythonPluginContext::
serveDocumentationFolder(const std::string & dir)
{
    if(dir.empty()) {
        throw MLDB::Exception("Documentation directory cannot be empty");
    }

    fs::path fullDir(fs::path(getPluginDirectory()) / fs::path(dir));
    if(!fs::exists(fullDir)) {
        throw MLDB::Exception("Cannot serve documentation folder for path that does "
                "not exist: " + fullDir.string());
    }

    handleDocumentation = getStaticRouteHandler("file://" + fullDir.string(), server);
}

std::string PythonPluginContext::
getPluginDirectory() const
{
    return pluginResource->getPluginDir().string();
}

std::shared_ptr<PythonRestRequest> PythonPluginContext::
getRestRequest() const
{
    if(!restRequest) cout << "WANRING!! got restRequest pointer but it is nullz!" << endl;
    return restRequest;
}


/****************************************************************************/
/* MLDB PYTHON CONTEXT                                                      */
/****************************************************************************/

void MldbPythonContext::
log(const std::string & message)
{
    getPyContext()->log(message);
}

void MldbPythonContext::
logJsVal(const Json::Value & jsVal)
{
    if(jsVal.isObject() || jsVal.isArray()) {
        getPyContext()->log(jsVal.toStyledString());
    }
    else if(jsVal.isIntegral()) {
        getPyContext()->log(std::to_string(jsVal.asInt()));
    }
    else if(jsVal.isDouble()) {
        getPyContext()->log(jsVal.toStringNoNewLine());
    }
    else {
        getPyContext()->log(jsVal.asString());
    }
}

void MldbPythonContext::
logUnicode(const Utf8String & msg)
{
    getPyContext()->log(msg.rawString());
}

PythonContext* MldbPythonContext::
getPyContext()
{
    if(script && plugin)
        throw MLDB::Exception("Both script and plugin are defined!!");

    if(script) return script.get();
    if(plugin) return plugin.get();
    throw MLDB::Exception("Neither script or plugin is defined!");
}

void MldbPythonContext::
setPlugin(std::shared_ptr<PythonPluginContext> pluginCtx) {
    plugin = pluginCtx;
    plugin->mldbContext = this;
}

void MldbPythonContext::
setScript(std::shared_ptr<PythonScriptContext> scriptCtx) {
    script = scriptCtx;
    script->mldbContext = this;
}

std::shared_ptr<PythonPluginContext> MldbPythonContext::
getPlugin()
{
    if(plugin) {
        return plugin;
    }
    throw MLDB::Exception("Cannot call the plugin object in this context");

}

std::shared_ptr<PythonScriptContext> MldbPythonContext::
getScript()
{
    if(script)
        return script;

    throw MLDB::Exception("Cannot call the script object in this context");
}

void MldbPythonContext::
setPathOptimizationLevel(const std::string & val)
{
    std::string valLc;
    for (auto c: val)
        valLc += tolower(c);
    int level = -1;
    if (valLc == "always") {
        level = OptimizedPath::ALWAYS;
    }
    else if (valLc == "never") {
        level = OptimizedPath::NEVER;
    }
    else if (valLc == "sometimes") {
        level = OptimizedPath::SOMETIMES;
    }
    else throw MLDB::Exception("Couldn't parse path optimization level '"
                             + val + "': accepted are 'always', 'never' "
                             "and 'sometimes'");

    OptimizedPath::setDefault(level);
}

} // namespace MLDB


