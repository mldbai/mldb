/** python_plugin_context.cc
    Francois Maillet, 6 mars 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "python_plugin_context.h"
#include "mldb/engine/static_content_handler.h"
#include "mldb/utils/string_functions.h"
#include "mldb/utils/for_each_line.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/base/optimized_path.h"
#include "mldb/utils/log.h"
#include "mldb/base/scope.h"
#include <regex>
#include "mldb/utils/split.h"
#include "mldb/utils/replace_all.h"
#include <memory>
#include "frameobject.h"
#include "capture_stream.h"
#include "nanobind/stl/string.h"

using namespace std;

namespace fs = std::filesystem;


namespace MLDB {

// Once the MLDB runtime is loaded, this replaces the weak findEnvironmentImpl in
// find_mldb_environment.cc with this one, which looks in the MldbPythonInterpreter
// for the right context for the current interpreter.
std::shared_ptr<MldbPythonContext>
findEnvironmentImpl()
{
    cerr << "findEnvironmentImpl strong version" << endl;
    return MldbPythonInterpreter::findEnvironment();
}

namespace {

// Protected by the GIL.  Functions that manipulate must only be called with it held.
std::unordered_map<PyInterpreterState *, std::weak_ptr<MldbPythonContext> > environments;

} // file scope

// Used by modules to find the MLDB environment associated with our interpreter
std::shared_ptr<MldbPythonContext>
MldbPythonInterpreter::
findEnvironment()
{
    cerr << "findEnvironment" << endl;
    PyThreadState * st = PyThreadState_Get();
    ExcAssert(st);

    PyInterpreterState * interp = st->interp;

    cerr << "looking for interpreter in " << environments.size() << " environments" << endl;
    auto it = environments.find(interp);
    if (it == environments.end())
        return nullptr;

    return it->second.lock();
}

MldbPythonInterpreter::
MldbPythonInterpreter(std::shared_ptr<PythonContext> context)
    : context(context)
{
    auto enterThread = mainThread().enter();

    try {

        
        mldb = std::make_shared<MldbPythonContext>(context);
        environments[interpState.get()->interp] = mldb;

        nanobind::module_::import_("mldb");

        main_module = nanobind::module_::import_("__main__");
        main_namespace = main_module.attr("__dict__");

#if 0
        main_namespace["__mldb_environment__"] = mldb;
#endif

        injectOutputLoggingCode(*enterThread);

    } catch (...) {
        cerr << "got exception " << getExceptionString() << " initializing python interpreter" << endl;
        environments.erase(interpState.get()->interp);
        throw;
    }
}

MldbPythonInterpreter::
~MldbPythonInterpreter()
{
    if (stdOutCapture) {
        cerr << "MldbPythonInterpreter: destroy() not called "
             << "before destruction after initialization" << endl;
        abort();
    }
}

void
MldbPythonInterpreter::
destroy()
{
    {
        auto enterGuard = mainThread().enter();

        main_module = nanobind::object();
        main_namespace = nanobind::object();

        stdOutCapture.reset();
        stdErrCapture.reset();

        environments.erase(interpState.get()->interp);
    }
    
    PythonInterpreter::destroy();
}

ScriptException
MldbPythonInterpreter::
convertException(const EnterThreadToken & threadToken,
                 const nanobind::python_error & exc,
                 const std::string & context)
{
    try {
#if 0
        PyThreadState *tstate = PyThreadState_GET();

        PyFrameObject * frame = nullptr;

        if (NULL != tstate) {
            frame = PyThreadState_GetFrame(tstate);
        }

        Scope_Exit(if (frame) Py_DECREF(frame));
#endif

        ScriptException result;


        auto val = exc.value();
        cerr << "val is " << nanobind::repr(val).c_str() << endl;
        if(val && PyUnicode_Check(val.ptr())) {
            result.message = Utf8String(nanobind::cast<string>(val));
        }
        else {
            result.message = "Cannot extract exception message from non-unicode Python string";
        }

        // Attempt to extract the type name
        nanobind::handle type = exc.type();
        if (type) {
            std::string reprUtf8 = nanobind::repr(type).c_str();
            cerr << "exception repr is " << reprUtf8 << endl;
            static std::regex typePattern("<class '(.*)'>");
            std::smatch what;
            if (std::regex_match(reprUtf8, what, typePattern)) {
                result.type = what[1];
            }
        }        

        nanobind::object traceback = exc.traceback();

        if(traceback) {
            result.lineNumber = nanobind::cast<long>(traceback.attr("tb_lineno"));

            PyTracebackObject * ptb = (PyTracebackObject*)traceback.ptr();;
            while (ptb) {
                auto frame = ptb->tb_frame;
                long lineno = PyFrame_GetLineNumber(frame);
                auto code = PyFrame_GetCode(frame);
                Scope_Exit(Py_DECREF(code));
                PyObject *filename = code->co_filename;
                const char * fn = PyUnicode_AsUTF8(filename);
                const char * func = PyUnicode_AsUTF8(code->co_name);

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

        if (result.type == "SyntaxError" && val) {
            // Extra fixups required to parse the syntax error fields
            result.lineNumber = nanobind::cast<long>(val.attr("lineno"));
            result.scriptUri = nanobind::cast<std::string>(val.attr("filename"));
            if (nanobind::hasattr(val, "text")) {
                result.lineContents = nanobind::cast<std::string>(val.attr("text"));
            }
            
            result.columnStart = nanobind::cast<long>(val.attr("offset"));
            PyObject * str = PyObject_Str(val.ptr());
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
    } catch (const nanobind::python_error & exc) {
        PyErr_Print();
        throw;
    }
}


/*****************************************************************************/
/* PYTHON STDOUT/ERR EXTRACTION CODE                                         */
/*****************************************************************************/

void
MldbPythonInterpreter::
injectOutputLoggingCode(const EnterThreadToken & threadToken)
{
    stdOutCapture.reset();
    stdErrCapture.reset();

    stdOutCapture
        = setStdStream(threadToken,
                       [this] (const EnterThreadToken & threadToken,
                               std::string message)
                       {
                           this->logMessage(threadToken, "stdout",
                                            std::move(message));
                       },
                       "stdout");
    
    stdErrCapture
        = setStdStream(threadToken,
                       [this] (const EnterThreadToken & threadToken,
                               std::string message)
                       {
                           this->logMessage(threadToken, "stderr",
                                            std::move(message));
                       },
                       "stderr");
}

void
MldbPythonInterpreter::
logMessage(const EnterThreadToken & threadToken,
           const char * stream, std::string message)
{
    Date ts = Date::now();
    
    if (message != "\n") {
        context->logToStream(stream, message);
    }

    BufferState & buffer = buffers[stream];

    // Just a newline?  Flush it out
    if (message == "\n") {
        if (!buffer.empty) {
            logs.emplace_back(buffer.ts, stream, std::move(buffer.message));
            buffer.message = std::string();
            buffer.empty = true;
        }
        return;
    }

    // Message with a newline at the end?  Print it including the buffer
    // contents
    if (!message.empty() && message[message.length() - 1] == '\n') {
        message = std::string(message, 0, message.length() - 1);
        if (!buffer.empty) {
            message = buffer.message + message;
            ts = buffer.ts;
            buffer.empty = true;
            buffer.message = std::string();
        }

        logs.emplace_back(ts, stream, std::move(message));
    }
    else {
        // No newline.  Buffer until we get one.
        if (buffer.empty) {
            buffer.ts = ts;
            buffer.message = std::move(message);
            buffer.empty = false;
        }
        else {
            buffer.message += message;
        }
    }
}

void
MldbPythonInterpreter::
getOutputFromPy(const EnterThreadToken & threadToken,
                ScriptOutput & result,
                bool reset)
{
    ExcAssert(reset);

    // Flush the buffers
    for (auto & p: buffers) {
        const auto & stream = p.first;
        BufferState & buf = p.second;

        if (!buf.empty) {
            logs.emplace_back(buf.ts, stream, std::move(buf.message));
            buf.empty = true;
            buf.message = std::string();
        }
    }

    result.logs.insert(result.logs.end(),
                       std::make_move_iterator(logs.begin()),
                       std::make_move_iterator(logs.end()));

    logs.clear();
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

ScriptOutput
MldbPythonInterpreter::
runPythonScript(const EnterThreadToken & threadToken,
                Utf8String scriptSource,
                Utf8String scriptUri,
                nanobind::object globals,
                nanobind::object locals)
{
    ScriptOutput result;

    try {
        MLDB_TRACE_EXCEPTIONS(false);
        
        nanobind::object obj =
            PythonThread
            ::exec(threadToken,
                   scriptSource,
                   scriptUri,
                   globals,
                   locals);
        
        getOutputFromPy(threadToken, result);
    }
    catch (const nanobind::python_error & exc) {
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
    }

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
    restParams = request.params;
    headers = request.header.headers;
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
    : category((name + " plugin").rawString().c_str()),
      loader("loader", category),
      stdout("stdout", category),
      stderr("stderr", category),
      engine(engine)
{
    ExcAssert(engine);
}

PythonContext::
~PythonContext()
{
}

void
PythonContext::
log(const std::string & message)
{
    std::unique_lock<std::mutex> guard(logMutex);
    LOG(category) << message << endl;
    logs.emplace_back(Date::now(), "log", Utf8String(message));
}

void
PythonContext::
logToStream(const char * stream,
            const std::string & message)
{
    std::unique_lock<std::mutex> guard(logMutex);
    if (strcmp(stream, "stdout")) {
        LOG(stdout) << message << endl;
    }
    else if (strcmp(stream, "stderr")) {
        LOG(stderr) << message << endl;
    }
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

    string route_pattern = "/" + MLDB::replace_all_copy(route, "/", "") + "/(.*)";
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
    return plugin;
}

std::shared_ptr<PythonScriptContext> MldbPythonContext::
getScript()
{
    return script;
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
perform(const std::string & verb,
        const std::string & resource,
        const RestParamsBase & params,
        Json::Value payload,
        const RestParamsBase & headers)
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
    std::vector<Utf8String> dirs;
    std::map<Utf8String, FsObjectInfo> objects;

    auto onSubdir = [&] (const Utf8String & dirName,
                         int depth)
        {
            dirs.push_back(dirName);
            return false;
        };

    auto onObject = [&] (const Utf8String & uri,
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

string
MldbPythonContext::
getPythonExecutable()
{
    return this->getPyContext()->engine->getPythonExecutable();
}


} // namespace MLDB
