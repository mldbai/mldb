/** python_plugin_context.h                                        -*- C++ -*-
    Francois Maillet, 6 mars 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <boost/python.hpp>
#include <boost/python/return_value_policy.hpp>
#include <frameobject.h>

#include "python_converters.h"
#include "from_python_converter.h"
#include "callback.h"
#include <boost/python/to_python_converter.hpp>

#include "mldb/server/plugin_resource.h"
#include "mldb/core/plugin.h"
#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/script_output.h"

#include "mldb/types/basic_value_descriptions.h"
#include "mldb/logging/logging.h"
#include "mldb/rest/in_process_rest_connection.h"


//using namespace Python;


namespace MLDB {


/****************************************************************************/
/* PythonSubinterpreter                                                     */
/****************************************************************************/

struct PythonSubinterpreter {

    PythonSubinterpreter(bool isChild=false);
    ~PythonSubinterpreter();

    void acquireGil();
    void releaseGil();
    
    PyThreadState* savedThreadState;
    
    PyThreadState* interpState;
    PyThreadState* threadState;
    
    boost::python::object main_module;
    boost::python::object main_namespace;

    bool hasGil;
    bool isChild;

    static std::mutex youShallNotPassMutex;
    std::unique_ptr<std::lock_guard<std::mutex>> lock;
};

ScriptException
convertException(PythonSubinterpreter & pyControl,
        const boost::python::error_already_set & exc2,
        const std::string & context);



/*****************************************************************************/
/* PYTHON STDOUT/ERR EXTRACTION CODE                                         */
/*****************************************************************************/

void injectOutputLoggingCode();
void getOutputFromPy(PythonSubinterpreter & pyControl,
                     ScriptOutput & result,
                     bool reset=true);

ScriptOutput exceptionToScriptOutput(PythonSubinterpreter & pyControl,
                                     ScriptException & exc,
                                     const std::string & context);



/****************************************************************************/
/* PythonRestRequest                                                        */
/****************************************************************************/

struct PythonRestRequest {

    PythonRestRequest(const RestRequest & request,
                      RestRequestParsingContext & context);

    Utf8String remaining;
    std::string verb;
    std::string resource;
    boost::python::list restParams;
    Json::Value payload;
    std::string contentType;
    int contentLength;
    boost::python::dict headers;
};


/****************************************************************************/
/* PYTHON CONTEXT                                                           */
/****************************************************************************/

struct MldbPythonContext;

struct PythonContext {
    PythonContext(const Utf8String &  name, MldbServer * server,
            std::shared_ptr<LoadedPluginResource> pluginResource)
    : categoryName(name + " plugin"),
      loaderName(name + " loader"),
      category(categoryName.rawData()),
      loader(loaderName.rawData()),
      server(server),
      pluginResource(pluginResource)
    {
    }

    void log(const std::string & message);
    
    Json::Value getArgs() const;
    void setReturnValue(const Json::Value & rtnVal, unsigned returnCode=200);
    void setReturnValue1(const Json::Value & rtnVal);
    void resetReturnValue();

    Utf8String categoryName, loaderName;

    Json::Value rtnVal;

    std::mutex logMutex;  /// protects the categories below
    Logging::Category category, loader;

    MldbServer * server;
    RestRequestRouter router;

    std::mutex guard;
    std::vector<ScriptLogEntry> logs;

    std::shared_ptr<LoadedPluginResource> pluginResource;

    MldbPythonContext* mldbContext;

    unsigned getRtnCode() {
        return rtnCode;
    }

    private:
        unsigned rtnCode;
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
                        MldbServer * server,
                        std::shared_ptr<LoadedPluginResource> pluginResource,
                        std::mutex & routeHandlingMutex)
        : PythonContext(pluginName, server, pluginResource),
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

    std::shared_ptr<PythonRestRequest> getRestRequest() const;
    std::shared_ptr<PythonRestRequest> restRequest;

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
    PythonScriptContext(const std::string & pluginName, MldbServer * server,
            std::shared_ptr<LoadedPluginResource> pluginResource)
    : PythonContext(pluginName, server, pluginResource)
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

