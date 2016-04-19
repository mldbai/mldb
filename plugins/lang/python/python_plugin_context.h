/** python_plugin_context.h                                        -*- C++ -*-
    Francois Maillet, 6 mars 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
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
#include "mldb/http/logs.h"
#include "mldb/rest/in_process_rest_connection.h"


//using namespace Datacratic::Python;

namespace Datacratic {
namespace MLDB {

std::shared_ptr<void> enterPython();

struct EnterPython {
    EnterPython()
        : handle(enterPython())
    {
    }
    std::shared_ptr<void> handle;
};

#if 0
/****************************************************************************/
/* PythonSubinterpreter                                                     */
/****************************************************************************/

struct PythonSubinterpreter {

    PythonSubinterpreter();
    ~PythonSubinterpreter();

    PythonSubinterpreter(const PythonSubinterpreter & other) = delete;
    void operator = (const PythonSubinterpreter & other) = delete;

    /// Enter into this interpreter.  This will take the GIL and swap the
    /// current thread onto the Python execution thread.  Once the return
    /// value is released, the operation will be undone.
    std::shared_ptr<void> enter();

    boost::python::object main_module;
    boost::python::object main_namespace;

private:
    PyThreadState* threadState;
};
#endif

ScriptException
convertException(const boost::python::error_already_set & exc2,
                 const std::string & context);



/*****************************************************************************/
/* PYTHON STDOUT/ERR EXTRACTION CODE                                         */
/*****************************************************************************/

void injectOutputLoggingCode();
void getOutputFromPy(ScriptOutput & result,
                     bool reset=true);

ScriptOutput exceptionToScriptOutput(ScriptException & exc,
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

struct PythonPluginContext;
struct PythonScriptContext;

struct PythonContext: public std::enable_shared_from_this<PythonContext> {

protected:
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

public:
    virtual ~PythonContext()
    {
    }
    
    void log(const std::string & message);
    
    Json::Value getArgs() const;
    void setReturnValue(const Json::Value & rtnVal, unsigned returnCode=200);
    void setReturnValue1(const Json::Value & rtnVal);

    Utf8String categoryName, loaderName;

    unsigned rtnCode;
    Json::Value rtnVal;

    std::mutex logMutex;  /// protects the categories below
    Logging::Category category, loader;

    MldbServer * server;
    RestRequestRouter router;

    std::mutex guard;
    std::vector<ScriptLogEntry> logs;

    std::shared_ptr<LoadedPluginResource> pluginResource;

    boost::python::object scope;

    ScriptOutput runScript(PackageElement elementToRun);

    ScriptOutput runScript(PackageElement elementToRun,
                           const RestRequest & request,
                           RestRequestParsingContext & context);

    void logJsVal(const Json::Value & jsVal);
    void logUnicode(const Utf8String & msg);
  
    std::shared_ptr<PythonPluginContext> getPlugin();
    std::shared_ptr<PythonScriptContext> getScript();
};


/****************************************************************************/
/* HELPER FUNCTION                                                          */
/****************************************************************************/

Json::Value
perform2(PythonContext * mldbCon,
        const std::string & verb,
        const std::string & resource);

Json::Value
perform3(PythonContext * mldbCon,
        const std::string & verb,
        const std::string & resource,
        const RestParams & params);

Json::Value
perform4(PythonContext * mldbCon,
        const std::string & verb,
        const std::string & resource,
        const RestParams & params,
        Json::Value payload);

Json::Value
perform(PythonContext * mldbCon,
        const std::string & verb,
        const std::string & resource,
        const RestParams & params=RestParams(),
        Json::Value payload=Json::Value(),
        const RestParams & header=RestParams());

Json::Value
readLines1(PythonContext * mldbCon,
          const std::string & path);

Json::Value
readLines(PythonContext * mldbCon,
          const std::string & path,
          int maxLine = -1);

Json::Value
ls(PythonContext * mldbCon,
    const std::string & path);

std::string
getHttpBoundAddress(PythonContext * mldbCon);



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

#if 0
/****************************************************************************/
/* MLDB PYTHON CONTEXT  
 * this just holds pointers to the plugin/script contexts. this is the mldb
 * object that is exposed                                                   */
/****************************************************************************/

struct MldbPythonContext {

    PythonPluginContext * getPlugin();
    PythonScriptContext * getScript();

    void log(const std::string & message);
    void logJsVal(const Json::Value & jsVal);
    void logUnicode(const Utf8String & msg);

    PythonContext* getPyContext();

    void setPlugin(PythonPluginContext * plug);
    void setScript(PythonScriptContext * scrp);

    PythonPluginContext * plugin;
    PythonScriptContext * script;

};
#endif

} // namespace mldb
} // namespace datacratic
