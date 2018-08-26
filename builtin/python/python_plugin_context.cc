/** python_plugin_context.cc
    Francois Maillet, 6 mars 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "python_plugin_context.h"
#include "mldb/server/static_content_handler.h"
#include "mldb/utils/string_functions.h"
#include "mldb/builtin/for_each_line.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/base/optimized_path.h"
#include "mldb/utils/log.h"
#include "mldb/base/scope.h"
#include <regex>
#include <boost/algorithm/string.hpp>
#include <memory>
#include "frameobject.h"
#include "pointer_fix.h"

using namespace std;

namespace fs = std::filesystem;


namespace MLDB {

MldbPythonInterpreter::
MldbPythonInterpreter(std::shared_ptr<PythonContext> context)
    : context(context)
{
    auto enterThread = mainThread().enter();

    mldb = std::make_shared<MldbPythonContext>(context);
    
    main_module = boost::python::import("__main__");
    main_namespace = main_module.attr("__dict__");

    main_namespace["mldb"]
        = boost::python::object(boost::python::ptr(mldb.get()));
    
    injectMldbWrapper(*enterThread);
    injectOutputLoggingCode(*enterThread);
}

MldbPythonInterpreter::
~MldbPythonInterpreter()
{
}

void
MldbPythonInterpreter::
destroy()
{
    {
        auto enterGuard = mainThread().enter();

        main_module = boost::python::object();
        main_namespace = boost::python::object();

#if 0
        // Uninstall the output logging, since it causes problems when
        // called from the interpreter shutdown as 
        
        PyObject * oldstdout = PySys_GetObject("oldStdOut");  // borrowed
        if (oldstdout) {
            PySys_SetObject("stdout", oldstdout);
        }
        
        PyObject * oldstderr = PySys_GetObject("oldStdErr");  // borrowed
        if (oldstderr) {
            PySys_SetObject("stderr", oldstderr);
        }
#endif
    }
    
    PythonInterpreter::destroy();
}

ScriptException
MldbPythonInterpreter::
convertException(const EnterThreadToken & threadToken,
                 const boost::python::error_already_set & exc2,
                 const std::string & context)
{
    try {
        PyFrameObject* frame = PyEval_GetFrame();

        PyThreadState *tstate = PyThreadState_GET();

        if (NULL != tstate && NULL != tstate->frame) {
            frame = tstate->frame;
        }
    
        ScriptException result;

        using namespace boost::python;
        using namespace boost;

        PyObject *exc,*val,*tb;
        object formatted_list, formatted;
        PyErr_Fetch(&exc,&val,&tb);

        if(val && PyUnicode_Check(val)) {
            result.message = Utf8String(extract<string>(val));
        }

        PyErr_NormalizeException(&exc, &val, &tb);

        handle<> hexc(exc),hval(allow_null(val)),htb(allow_null(tb));

        // Attempt to extract the type name
        {
            PyObject * repr = PyObject_Repr(exc);
            Scope_Exit(Py_DECREF(repr));
            std::string reprUtf8 = PyUnicode_AsUTF8(repr);
        
            static std::regex typePattern("<class '(.*)'>");
            std::smatch what;
            if (std::regex_match(reprUtf8, what, typePattern)) {
                result.type = what[1];
            }
        }        

        if(val && PyUnicode_Check(val)) {
            result.message = Utf8String(extract<string>(val));
        }
        else if (val) {
            PyObject * str = PyObject_Str(val);
            Scope_Exit(Py_DECREF(str));
            result.message = PyUnicode_AsUTF8(str);
        }
            

        if(htb) {
            object tbb(htb);
            result.lineNumber = extract<long>(tbb.attr("tb_lineno"));

            PyTracebackObject * ptb = (PyTracebackObject*)tb;
            while (ptb) {
                auto frame = ptb->tb_frame;
                long lineno = PyFrame_GetLineNumber(frame);
                PyObject *filename = frame->f_code->co_filename;
                const char * fn = PyUnicode_AsUTF8(filename);
                const char * func = PyUnicode_AsUTF8(frame->f_code->co_name);

                ScriptStackFrame sframe;
                sframe.scriptUri = fn;
                sframe.functionName = func;
                sframe.lineNumber = lineno;
                sframe.where = Utf8String("File \"") + fn + "\", line "
                    + std::to_string(lineno) + ", in " + func;
                result.stack.push_back(sframe);
                
                ptb = ptb->tb_next;
            }
        }

        if (result.type == "SyntaxError" && hval) {
            // Extra fixups required to parse the syntax error fields
            object oval(hval);
            result.lineNumber = boost::python::extract<long>(oval.attr("lineno"));
            result.scriptUri = boost::python::extract<std::string>(oval.attr("filename"));
            result.lineContents = boost::python::extract<std::string>(oval.attr("text"));
            result.columnStart = boost::python::extract<long>(oval.attr("offset"));
            PyObject * str = PyObject_Str(val);
            Scope_Exit(Py_DECREF(str));
            result.message = PyUnicode_AsUTF8(str);
        }
        else if (!result.stack.empty()) {
            result.where = result.stack.back().where;
            result.scriptUri = result.stack.back().scriptUri;
            result.lineNumber = result.stack.back().lineNumber;
            result.columnStart = result.stack.back().columnStart;
        }
        
        result.context = {context};

        return result;
    } catch (const boost::python::error_already_set & exc) {
        PyErr_Print();
        throw;
    }
}


/*****************************************************************************/
/* PYTHON MLDB WRAPPER                                                       */
/*****************************************************************************/
extern "C" {
    extern const char mldb_wrapper_start;
    extern const char mldb_wrapper_end;
    extern const size_t mldb_wrapper_size;
};

void
MldbPythonInterpreter::
injectMldbWrapper(const EnterThreadToken & threadToken)
{
    static const Utf8String
        code(std::string(&mldb_wrapper_start, &mldb_wrapper_end));
    boost::python::object out
        = PythonThread::exec(threadToken, code,
                             "mldb_wrapper.py",
                             this->main_namespace);
}

/*****************************************************************************/
/* PYTHON STDOUT/ERR EXTRACTION CODE                                         */
/*****************************************************************************/

extern "C" {
    extern const char output_logging_start;
    extern const char output_logging_end;
    extern const size_t output_logging_size;
};

void
MldbPythonInterpreter::
injectOutputLoggingCode(const EnterThreadToken & threadToken)
{
    static const Utf8String
        code(std::string(&output_logging_start, &output_logging_end));
    int res = PyRun_SimpleString(code.rawData()); //invoke code to redirect
    if (res) {
        PyErr_Print(); //make python print any errors, unfortunately to console
        throw AnnotatedException
            (500, "Couldn't inject Python code (see error message on console). "
             "Have you installed python_dependenies (json and datetime) and "
             "properly set up your virtual environment?");
    }
}

void
MldbPythonInterpreter::
getOutputFromPy(const EnterThreadToken & threadToken,
                ScriptOutput & result,
                bool reset)
{
    PyObject *outCatcher = PyObject_GetAttrString(main_module.ptr(),"catchOut"); //get our catchOutErr created above

    // Until we figure out WTF is going on here...
    if (!outCatcher) {
        return; //... TODO: this is a hack for testing, must be removed
        throw AnnotatedException
            (500, "Couldn't extract output from injected Python code.  Look for "
             "an earlier error message on the console.");
    }
    
    PyErr_Print(); //make python print any errors
    PyObject *outOutput = PyObject_GetAttrString(outCatcher,"value"); //get the stdout and stderr from our catchOutErr object
    
    if(outOutput) {
        boost::python::list lst = boost::python::extract<boost::python::list>(outOutput);
        for(int i = 0; i < len(lst); i++) {
            boost::python::object obj = boost::python::object(lst[i]);
            if(obj.ptr() == Py_None) continue;

            auto p = Json::parse(boost::python::extract<std::string>(obj));
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
        injectOutputLoggingCode(threadToken);
    }
};

ScriptOutput
MldbPythonInterpreter::
exceptionToScriptOutput(const EnterThreadToken & thread,
                        ScriptException & exc,
                        const string & context)
{
    ScriptOutput result;

    result.exception = std::make_shared<ScriptException>(std::move(exc));
    result.exception->context.push_back(context);

    getOutputFromPy(thread, result);

    return result;
}

void
MldbPythonInterpreter::
runPythonScript(const EnterThreadToken & threadToken,
                Utf8String scriptSource,
                Utf8String scriptUri,
                bool useLocals,
                bool mustProvideOutput,
                bool isScript,
                ScriptOutput * output)
{
    RestRequest request;
    RestRequestParsingContext context(request);
    auto connection = InProcessRestConnection::create();
    
    runPythonScript(threadToken, std::move(scriptSource), std::move(scriptUri),
                    request, context,
                    *connection, useLocals, mustProvideOutput, output);

    connection->waitForResponse();
}

void
MldbPythonInterpreter::
runPythonScript(const EnterThreadToken & threadToken,
                Utf8String scriptSource,
                Utf8String scriptUri,
                const RestRequest & request,
                RestRequestParsingContext & context,
                RestConnection & connection,
                bool useLocals,
                bool mustProvideOutput,
                bool isScript,
                ScriptOutput * output)
{
    ScriptOutput result;

    try {
        auto pyRestRequest
            = std::make_shared<PythonRestRequest>(request, context);

        boost::python::dict locals;

        if (useLocals) {
            auto keys = boost::python::dict(main_namespace).keys();
            for (size_t i = 0;  i < boost::python::len(keys);  ++i) {
                const auto & key = keys[i];
                locals[key] = main_namespace[key];
            }
            
            locals["request"]
                = boost::python::object(boost::python::ptr(pyRestRequest.get()));
        }
        else {
            main_namespace["request"]
                = boost::python::object(boost::python::ptr(pyRestRequest.get()));
        }
        
        MLDB_TRACE_EXCEPTIONS(false);
        
        boost::python::object obj =
            PythonThread
            ::exec(threadToken,
                   scriptSource,
                   scriptUri,
                   main_namespace,
                   useLocals ? locals: boost::python::object());
        
        getOutputFromPy(threadToken, result);

        result.result = std::move(pyRestRequest->returnValue);

        if (pyRestRequest->returnCode <= 0) {
            if (mustProvideOutput) {
                throw AnnotatedException
                    (500,
                     "Return value is required but not set");
            }
            else {
                pyRestRequest->returnCode = 200;
            }
        }
        
        result.setReturnCode(pyRestRequest->returnCode);

        if (isScript) {
#if 0
            auto scriptCtx = static_pointer_cast<PythonScriptContext>(pyCtx);

            // Copy log messages over
            for (auto & l: scriptCtx->logs) {
                result.logs.emplace_back(std::move(l));
            }
            std::stable_sort(result.logs.begin(), result.logs.end());
#endif
            connection.sendResponse(result.getReturnCode(),
                                     jsonEncode(result));
        }
        else {
            connection.sendResponse(result.getReturnCode(),
                                    jsonEncode(result.result));
        }
    }
    catch (const boost::python::error_already_set & exc) {
        ScriptException pyexc
            = convertException(threadToken, exc,
                               "Running python script");

        {
            std::unique_lock<std::mutex> guard(this->context->logMutex);
            LOG(this->context->loader) << jsonEncode(pyexc) << endl;
        }
        
        getOutputFromPy(threadToken, result);
        result.exception = std::make_shared<ScriptException>(std::move(pyexc));
        result.exception->context.push_back("Executing Python script");
        result.setReturnCode(400);

        connection.sendResponse(result.getReturnCode(),
                                jsonEncode(result));
    }

    if (output) {
        *output = std::move(result);
    }
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

void
PythonRestRequest::
setReturnValue(const Json::Value & rtnVal, unsigned returnCode)
{
    this->returnValue = rtnVal;
    this->returnCode = returnCode;
}

void
PythonRestRequest::
setReturnValue1(const Json::Value & rtnVal)
{
    setReturnValue(rtnVal, 200);
}


/****************************************************************************/
/* PYTHON CONTEXT                                                           */
/****************************************************************************/

PythonContext::
PythonContext(const Utf8String &  name, MldbEngine * engine)
    : categoryName(name + " plugin"),
      loaderName(name + " loader"),
      category(categoryName.rawData()),
      loader(loaderName.rawData()),
      engine(engine)
{
}

PythonContext::
~PythonContext()
{
}

void
PythonContext::
log(const std::string & message)
{
    LOG(category) << message << endl;
    logs.emplace_back(Date::now(), "log", Utf8String(message));
}

/****************************************************************************/
/* PYTHON PLUGIN CONTEXT                                                    */
/****************************************************************************/

PythonPluginContext::
PythonPluginContext(const Utf8String & pluginName,
                    MldbEngine * engine,
                    std::shared_ptr<LoadedPluginResource> pluginResource)
    : PythonContext(pluginName, engine),
      hasRequestHandler(false),
      pluginResource(pluginResource)
{
    hasRequestHandler =
        pluginResource->packageElementExists(PackageElement::ROUTES);
}

PythonPluginContext::
~PythonPluginContext()
{
}

Json::Value
PythonPluginContext::
getArgs() const
{
    return jsonEncode(pluginResource->args);
}

void
PythonPluginContext::
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
                    engine->getStaticRouteHandler("file://" + fullDir.string()),
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

    handleDocumentation = engine->getStaticRouteHandler("file://" + fullDir.string());
}

std::string PythonPluginContext::
getPluginDirectory() const
{
    return pluginResource->getPluginDir().string();
}


/****************************************************************************/
/* PYTHON SCRIPT CONTEXT                                                    */
/****************************************************************************/

PythonScriptContext::
PythonScriptContext(const std::string & pluginName, MldbEngine * engine,
                    std::shared_ptr<LoadedPluginResource> pluginResource)
    : PythonContext(pluginName, engine),
      pluginResource(std::move(pluginResource))
{
}

PythonScriptContext::
~PythonScriptContext()
{
}

Json::Value
PythonScriptContext::
getArgs() const
{
    return jsonEncode(pluginResource->args);
}


/****************************************************************************/
/* MLDB PYTHON CONTEXT                                                      */
/****************************************************************************/

MldbPythonContext::
MldbPythonContext(std::shared_ptr<PythonContext> context)
{
    bool isScript
        = dynamic_pointer_cast<PythonScriptContext>(context)
        != nullptr;
    
    // Perform a downcast depending upon the context
    if(isScript) {
        setScript(static_pointer_cast<PythonScriptContext>(context));
    }
    else {
        setPlugin(static_pointer_cast<PythonPluginContext>(context));
    }
}

void
MldbPythonContext::
log(const std::string & message)
{
    getPyContext()->log(message);
}

void
MldbPythonContext::
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
}

void MldbPythonContext::
setScript(std::shared_ptr<PythonScriptContext> scriptCtx) {
    script = scriptCtx;
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

void
MldbPythonContext::
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

Json::Value
MldbPythonContext::
perform2(const std::string & verb,
         const std::string & resource)
{
    return perform(verb, resource);
}


Json::Value
MldbPythonContext::
perform3(const std::string & verb,
         const std::string & resource,
         const RestParams & params)
{
    return perform(verb, resource, params);
}

Json::Value
MldbPythonContext::
perform4(const std::string & verb,
         const std::string & resource,
         const RestParams & params,
         Json::Value payload)
{
    return perform(verb, resource, params, payload);
}

Json::Value
MldbPythonContext::
perform(const std::string & verb,
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
    auto connection = InProcessRestConnection::create();

    {
        auto noGil = releaseGil();
        this->getPyContext()->engine->handleRequest(*connection, request);
    }

    connection->waitForResponse();
    
    Json::Value result;
    result["statusCode"] = connection->responseCode();

    if (!connection->contentType().empty())
        result["contentType"] = connection->contentType();
    if (!connection->headers().empty()) {
        Json::Value headers(Json::ValueType::arrayValue);
        for(const pair<Utf8String, Utf8String> & h : connection->headers()) {
            Json::Value elem(Json::ValueType::arrayValue);
            elem.append(h.first);
            elem.append(h.second);
            headers.append(elem);
        }
        result["headers"] = headers;
    }
    if (!connection->response().empty())
        result["response"] = connection->response();

    return result;
}

Json::Value
MldbPythonContext::
readLines1(const std::string & path)
{
    return readLines(path);
}

Json::Value
MldbPythonContext::
readLines(const std::string & path, int maxLines)
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
MldbPythonContext::
ls(const std::string & dir)
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
MldbPythonContext::
getHttpBoundAddress()
{
    return this->getPyContext()->engine->getHttpBoundAddress();
}


} // namespace MLDB
