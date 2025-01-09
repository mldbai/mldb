/** python_plugin_context.h                                        -*- C++ -*-
    Francois Maillet, 6 mars 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "python_interpreter.h"
#include "nanobind/nanobind.h"
#include <frameobject.h>

#include "python_converters.h"
#include "from_python_converter.h"
#include "callback.h"

#include "mldb/builtin/plugin_resource.h"
#include "mldb/core/plugin.h"
#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/mldb_engine.h"
#include "mldb/builtin/script_output.h"

#include "mldb/types/basic_value_descriptions.h"
#include "mldb/logging/logging.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "mldb/rest/rest_request_router.h"

namespace MLDB {

struct PythonContext;
struct MldbPythonContext;


/*****************************************************************************/
/* MLDB PYTHON INTERPRETER                                                   */
/*****************************************************************************/

/** Version of PythonInterpreter that initializes itself with all of
    the machinery to set up MLDB inside, including logging, the mldb
    object, and package paths.
*/

struct MldbPythonInterpreter: public PythonInterpreter {
    MldbPythonInterpreter(std::shared_ptr<PythonContext> context);

    // Destroy must be called before the destructor.
    ~MldbPythonInterpreter();

    // Destroy the interpreter's state at a moment just before the
    // destructor that we know the GIL isn't held.
    void destroy();

    // GIL must be held
    ScriptException
    convertException(const EnterThreadToken & threadToken,
                     const nanobind::python_error & exc2,
                     const std::string & context);

    // GIL must be held
    void getOutputFromPy(const EnterThreadToken & threadToken,
                         ScriptOutput & result,
                         bool reset=true);

    // GIL must be held
    ScriptOutput exceptionToScriptOutput(const EnterThreadToken & threadToken,
                                         ScriptException & exc,
                                         const std::string & context);

    // GIL must be held
    ScriptOutput
    runPythonScript(const EnterThreadToken & threadToken,
                    Utf8String scriptSource,
                    Utf8String scriptUri,
                    nanobind::object globals,
                    nanobind::object locals);

    std::shared_ptr<PythonContext> context;
    std::shared_ptr<MldbPythonContext> mldb;
    
    nanobind::object main_module;
    nanobind::object main_namespace;

    static std::shared_ptr<MldbPythonContext>
    findEnvironment();
    
private:    
    // GIL must be held
    void injectOutputLoggingCode(const EnterThreadToken & threadToken);

    // GIL must be held
    void logMessage(const EnterThreadToken & threadToken,
                    const char * stream, std::string message);

    // All this is protected by the GIL
    std::shared_ptr<const void> stdOutCapture;
    std::shared_ptr<const void> stdErrCapture;
    struct BufferState {
        Date ts;
        std::string message;
        bool empty = true;
    };
    std::map<std::string, BufferState> buffers;
    std::vector<ScriptLogEntry> logs;
};


/****************************************************************************/
/* PythonRestRequest                                                        */
/****************************************************************************/

/** Python-ized version of the RestRequest class. */

struct PythonRestRequest {

    PythonRestRequest(const RestRequest & request,
                      RestRequestParsingContext & context);

    Utf8String remaining;
    std::string verb;
    std::string resource;
    std::vector<std::pair<Utf8String, Utf8String>> restParams;
    Json::Value payload;
    std::string contentType;
    int contentLength;
    std::map<std::string, std::string> headers;

    // Must hold GIL; only called from Python so automatically true
    void setReturnValue(const Json::Value & rtnVal, unsigned returnCode=200);

    // Must hold GIL; only called from Python so automatically true
    void setReturnValue1(const Json::Value & rtnVal);
    
    // These two are protected by the GIL
    Json::Value returnValue;
    int returnCode = -1;
};


/****************************************************************************/
/* PYTHON CONTEXT                                                           */
/****************************************************************************/

struct PythonContext {
    PythonContext(const Utf8String &  name, MldbEngine * engine);

    virtual ~PythonContext();
    
    void log(const std::string & message);

    void logToStream(const char * stream,
                     const std::string & message);
    
    std::mutex logMutex;  /// protects the categories below
    Logging::Category category, loader, stdout, stderr;

    MldbEngine * engine;
    RestRequestRouter router;

    std::mutex guard;
    std::vector<ScriptLogEntry> logs;
};


/****************************************************************************/
/* PYTHON PLUGIN CONTEXT                                                    */
/****************************************************************************/

struct PythonPluginContext: public PythonContext  {
    PythonPluginContext(const Utf8String & pluginName,
                        MldbEngine * engine,
                        std::shared_ptr<LoadedPluginResource> pluginResource);

    virtual ~PythonPluginContext();
    
    Json::Value getArgs() const;

    //void setStatusHandler(PyObject * callback);
    void serveStaticFolder(const std::string & route, const std::string & dir);
    void serveDocumentationFolder(const std::string & dir);

    std::string getPluginDirectory() const;

    std::function<Json::Value ()> getStatus;
    
    RestRequestRouter::OnProcessRequest handleDocumentation;

    bool hasRequestHandler;
    std::string requestHandlerSource;

    std::shared_ptr<LoadedPluginResource> pluginResource;
};


/****************************************************************************/
/* PYTHON SCRIPT CONTEXT                                                    */
/****************************************************************************/

struct PythonScriptContext: public PythonContext  {
    PythonScriptContext(const std::string & pluginName, MldbEngine * engine,
                        std::shared_ptr<LoadedPluginResource> pluginResource);

    virtual ~PythonScriptContext();

    Json::Value getArgs() const;

    std::shared_ptr<LoadedPluginResource> pluginResource;
};


/****************************************************************************/
/* MLDB PYTHON CONTEXT  
 * this just holds pointers to the plugin/script contexts. this is the mldb
 * object that is exposed                                                   */
/****************************************************************************/

struct MldbPythonContext {

    MldbPythonContext(std::shared_ptr<PythonContext> context);
    
    std::shared_ptr<PythonPluginContext> getPlugin();
    std::shared_ptr<PythonScriptContext> getScript();

    void log(const std::string & message);
    void logJsVal(const Json::Value & jsVal);
    void logUnicode(const Utf8String & msg);

    PythonContext* getPyContext();

    /** Set the path optimization level.  See base/optimized_path.h. */
    void setPathOptimizationLevel(const std::string & level);

    std::shared_ptr<PythonPluginContext> plugin;
    std::shared_ptr<PythonScriptContext> script;

    Json::Value
    perform(const std::string & verb,
            const std::string & resource,
            const RestParamsBase & params=RestParamsBase(),
            Json::Value payload=Json::Value(),
            const RestParamsBase & header=RestParamsBase());

    Json::Value
    readLines(const std::string & path,
              int maxLine = -1);

    Json::Value
    ls(const std::string & path);

    std::string getHttpBoundAddress();
    std::string getPythonExecutable();

private:
    void setPlugin(std::shared_ptr<PythonPluginContext> pluginCtx);
    void setScript(std::shared_ptr<PythonScriptContext> scriptCtx);
};

} // namespace mldb

