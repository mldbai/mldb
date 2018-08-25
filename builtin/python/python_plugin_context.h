/** python_plugin_context.h                                        -*- C++ -*-
    Francois Maillet, 6 mars 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "python_interpreter.h"
#include <boost/python.hpp>
#include <boost/python/return_value_policy.hpp>
#include <frameobject.h>

#include "python_converters.h"
#include "from_python_converter.h"
#include "callback.h"
#include <boost/python/to_python_converter.hpp>

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

/** Version of PythonInterpreter that initializes itself with all of
    the machinery to set up MLDB inside, including logging, the mldb
    object, and package paths.
*/

struct MldbPythonInterpreter: public PythonInterpreter {
    MldbPythonInterpreter(MldbEngine * engine);

    // Destroy must be called before the destructor.
    ~MldbPythonInterpreter();

    // Destroy the interpreter's state at a moment just before the
    // destructor that we know the GIL isn't held.
    void destroy();

    // GIL must be held
    ScriptException
    convertException(const EnterThreadToken & threadToken,
                     const boost::python::error_already_set & exc2,
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
    void
    runPythonScript(const EnterThreadToken & threadToken,
                    std::shared_ptr<PythonContext> pyCtx,
                    PackageElement elementToRun,
                    bool useLocals,
                    bool mustSetOutput,
                    ScriptOutput * output = nullptr);

    // GIL must be held
    void
    runPythonScript(const EnterThreadToken & threadToken,
                    std::shared_ptr<PythonContext> pyCtx,
                    PackageElement elementToRun,
                    const RestRequest & request,
                    RestRequestParsingContext & context,
                    RestConnection & connection,
                    bool useLocals,
                    bool mustSetOutput,
                    ScriptOutput * output = nullptr);

private:    
    void injectMldbWrapper(const EnterThreadToken & threadToken);
    void injectOutputLoggingCode(const EnterThreadToken & threadToken);

};


/****************************************************************************/
/* PythonRestRequest                                                        */
/****************************************************************************/

/** Python-ized version of the RestRequest class. */

struct PythonRestRequest {

    PythonRestRequest(const RestRequest & request,
                      RestRequestParsingContext & context,
                      std::shared_ptr<RestConnection> connection);

    Utf8String remaining;
    std::string verb;
    std::string resource;
    boost::python::list restParams;
    Json::Value payload;
    std::string contentType;
    int contentLength;
    boost::python::dict headers;

    std::shared_ptr<RestConnection> connection;
    
    // Must hold GIL
    void setReturnValue(const Json::Value & rtnVal, unsigned returnCode=200);

    // Must hold GIL
    void setReturnValue1(const Json::Value & rtnVal);

    bool hasReturnValue() const;
    
    // These two are protected by the GIL
    Json::Value returnValue;
    int returnCode = -1;
};


/****************************************************************************/
/* PYTHON CONTEXT                                                           */
/****************************************************************************/

struct MldbPythonContext;

struct PythonContext {
    PythonContext(const Utf8String &  name, MldbEngine * engine,
            std::shared_ptr<LoadedPluginResource> pluginResource)
    : categoryName(name + " plugin"),
      loaderName(name + " loader"),
      category(categoryName.rawData()),
      loader(loaderName.rawData()),
      engine(engine),
      pluginResource(pluginResource)
    {
        using namespace std;
        cerr << "new python context for " << name << " at " << this << endl;
    }

    ~PythonContext()
    {
        using namespace std;
        cerr << "destroyed python context for " << categoryName << " at " << this << endl;
        magic = 987654321;
    }
    
    void log(const std::string & message);
    
    Json::Value getArgs() const;

    Utf8String categoryName, loaderName;

    //Json::Value rtnVal;

    std::mutex logMutex;  /// protects the categories below
    Logging::Category category, loader;

    MldbEngine * engine;
    RestRequestRouter router;

    std::mutex guard;
    std::vector<ScriptLogEntry> logs;

    std::shared_ptr<LoadedPluginResource> pluginResource;

    MldbPythonContext* mldbContext;

    int magic = 1234567;
    
    //unsigned getRtnCode() {
    //    return rtnCode;
    //}

    //private:
    //    unsigned rtnCode;
};


/****************************************************************************/
/* HELPER FUNCTION                                                          */
/****************************************************************************/

Json::Value
perform2(MldbPythonContext * mldbCon,
        const std::string & verb,
        const std::string & resource);

Json::Value
perform3(MldbPythonContext * mldbCon,
        const std::string & verb,
        const std::string & resource,
        const RestParams & params);

Json::Value
perform4(MldbPythonContext * mldbCon,
        const std::string & verb,
        const std::string & resource,
        const RestParams & params,
        Json::Value payload);

Json::Value
perform(MldbPythonContext * mldbCon,
        const std::string & verb,
        const std::string & resource,
        const RestParams & params=RestParams(),
        Json::Value payload=Json::Value(),
        const RestParams & header=RestParams());

Json::Value
readLines1(MldbPythonContext * mldbCon,
          const std::string & path);

Json::Value
readLines(MldbPythonContext * mldbCon,
          const std::string & path,
          int maxLine = -1);

Json::Value
ls(MldbPythonContext * mldbCon,
    const std::string & path);

std::string
getHttpBoundAddress(MldbPythonContext * mldbCon);



/****************************************************************************/
/* PYTHON PLUGIN CONTEXT                                                    */
/****************************************************************************/

struct PythonPluginContext: public PythonContext  {
    PythonPluginContext(const Utf8String & pluginName,
                        MldbEngine * engine,
                        std::shared_ptr<LoadedPluginResource> pluginResource,
                        std::mutex & routeHandlingMutex)
        : PythonContext(pluginName, engine, pluginResource),
          hasRequestHandler(false),
          routeHandlingMutex(routeHandlingMutex)
    {
        hasRequestHandler =
            pluginResource->packageElementExists(
                    PackageElement::ROUTES);
    }

    void setStatusHandler(PyObject * callback);
    void serveStaticFolder(const std::string & route, const std::string & dir);
    void serveDocumentationFolder(const std::string & dir);

    std::string getPluginDirectory() const;

    //std::shared_ptr<PythonRestRequest> getRestRequest() const;
    //std::shared_ptr<PythonRestRequest> restRequest;

    std::function<Json::Value ()> getStatus;
    
    RestRequestRouter::OnProcessRequest handleDocumentation;

    bool hasRequestHandler;
    std::string requestHandlerSource;

    std::mutex & routeHandlingMutex;
};


/****************************************************************************/
/* PYTHON SCRIPT CONTEXT                                                    */
/****************************************************************************/

struct PythonScriptContext: public PythonContext  {
    PythonScriptContext(const std::string & pluginName, MldbEngine * engine,
            std::shared_ptr<LoadedPluginResource> pluginResource)
    : PythonContext(pluginName, engine, pluginResource)
    {
    }
};


/****************************************************************************/
/* MLDB PYTHON CONTEXT  
 * this just holds pointers to the plugin/script contexts. this is the mldb
 * object that is exposed                                                   */
/****************************************************************************/

struct MldbPythonContext {

    std::shared_ptr<PythonPluginContext> getPlugin();
    std::shared_ptr<PythonScriptContext> getScript();

    void log(const std::string & message);
    void logJsVal(const Json::Value & jsVal);
    void logUnicode(const Utf8String & msg);

    PythonContext* getPyContext();

    void setPlugin(std::shared_ptr<PythonPluginContext> pluginCtx);
    void setScript(std::shared_ptr<PythonScriptContext> scriptCtx);

    /** Set the path optimization level.  See base/optimized_path.h. */
    void setPathOptimizationLevel(const std::string & level);

    std::shared_ptr<PythonPluginContext> plugin;
    std::shared_ptr<PythonScriptContext> script;

};

} // namespace mldb

