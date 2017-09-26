/** opencl.cc
    Jeremy Barnes, 21 September 2017
    Copyright (c) 2017 Element AI Inc.  All rights reserved.

*/

#include "mldb/core/plugin.h"
#include "mldb/server/plugin_resource.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/types/basic_value_descriptions.h"

#include <CL/cl_platform.h>
#include <CL/cl.h>

using namespace std;

namespace MLDB {





/*****************************************************************************/
/* JAVASCRIPT PLUGIN                                                         */
/*****************************************************************************/

struct OpenCLPlugin: public Plugin {
    OpenCLPlugin(MldbServer * server,
                     PolyConfig config,
                     std::function<bool (const Json::Value & progress)> onProgress);
    
    ~OpenCLPlugin();
    
    virtual Any getStatus() const;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

#if 0
    static ScriptOutput
    runOpenCLScript(MldbServer * server,
                        const PluginResource & scriptConfig);
#endif

    static RestRequestMatchResult
    handleTypeRoute(RestDirectory * server, RestConnection & conn,
                    const RestRequest & request,
                    RestRequestParsingContext & context);

    //std::unique_ptr<JsPluginContext> itl;
};

OpenCLPlugin::
OpenCLPlugin(MldbServer * server,
             PolyConfig config,
             std::function<bool (const Json::Value & progress)> onProgress)
    : Plugin(server)
{
    cl_uint platformIdCount = 0;
    clGetPlatformIDs (0, nullptr, &platformIdCount);

    std::vector<cl_platform_id> platformIds (platformIdCount);
    clGetPlatformIDs (platformIdCount, platformIds.data (), nullptr);

    cerr << platformIds.size() << " OpenCL platforms available" << endl;

#if 0
    PluginResource res = config.params.convert<PluginResource>();
    itl.reset(new JsPluginContext(config.id, this->server,
                                  std::make_shared<LoadedPluginResource>
                                  (OPENCL,
                                   LoadedPluginResource::PLUGIN,
                                   config.id, res)));
    
    Utf8String jsFunctionSource = itl->pluginResource->getScript(PackageElement::MAIN);


    v8::Locker locker(itl->isolate.isolate);
    v8::Isolate::Scope isolate(itl->isolate.isolate);

    HandleScope handle_scope(itl->isolate.isolate);
    Context::Scope context_scope(itl->context.Get(itl->isolate.isolate));
    
    // Create a string containing the Opencl source code.
    Handle<String> source
        = String::NewFromUtf8(itl->isolate.isolate, jsFunctionSource.rawData());

    // Compile the source code.
    TryCatch trycatch;
    trycatch.SetVerbose(true);

    auto script = Script::Compile
        (source,
         v8::String::NewFromUtf8(itl->isolate.isolate,
                                 itl->pluginResource->getFilenameForErrorMessages().c_str()));
    
    if (script.IsEmpty()) {  
        auto rep = convertException(trycatch, "Compiling plugin script");

        {
            std::unique_lock<std::mutex> guard(itl->logMutex);
            LOG(itl->loader) << jsonEncode(rep) << endl;
        }

        MLDB_TRACE_EXCEPTIONS(false);
        throw HttpReturnException(400, "Exception compiling plugin script", rep);
    }

    itl->script.Reset(itl->isolate.isolate, script);

    // Run the script to get the result.
    Handle<Value> result = itl->script.Get(itl->isolate.isolate)->Run();

    if (result.IsEmpty()) {  
        auto rep = convertException(trycatch, "Running plugin script");
        
        {
            std::unique_lock<std::mutex> guard(itl->logMutex);
            LOG(itl->loader) << jsonEncode(rep) << endl;
        }
        
        MLDB_TRACE_EXCEPTIONS(false);
        throw HttpReturnException(400, "Exception running plugin script", rep);
    }
    
    cerr << "script returned " << JS::cstr(result) << endl;
#endif
}
    
OpenCLPlugin::
~OpenCLPlugin()
{
}
    
Any
OpenCLPlugin::
getStatus() const
{
    return Any();
}

RestRequestMatchResult
OpenCLPlugin::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
#if 0
    // First, check for a route
    auto res = itl->router.processRequest(connection, request, context);
    if (res != RestRequestRouter::MR_NO)
        return res;

    // Second, check for a generic request handler
    if (itl->handleRequest)
        return itl->handleRequest(connection, request, context);
#endif

    // Otherwise we simply don't handle it
    return RestRequestRouter::MR_NO;
}

RestRequestMatchResult
OpenCLPlugin::
handleTypeRoute(RestDirectory * server,
                RestConnection & conn,
                const RestRequest & request,
                RestRequestParsingContext & context)
{
#if 0
    //cerr << "context.remaining = " << context.remaining << endl;

    if (context.resources.back() == "run") {
        //cerr << "request.payload = " << request.payload << endl;
        
        auto scriptConfig = jsonDecodeStr<ScriptResource>(request.payload).toPluginConfig();
        
        auto result = runOpenCLScript(static_cast<MldbServer *>(server),
                                          scriptConfig);
        conn.sendResponse(result.exception ? 400 : 200,
                          jsonEncodeStr(result), "application/json");
        return RestRequestRouter::MR_YES;
    }
#endif
    return RestRequestRouter::MR_NO;
}

#if 0
ScriptOutput
OpenCLPlugin::
runOpenCLScript(MldbServer * server,
                const PluginResource & scriptConfig)
{
    using namespace v8;

    std::unique_ptr<JsPluginContext> itl
        (new JsPluginContext
         ("script runner", server,
          std::make_shared<LoadedPluginResource>
          (OPENCL,
           LoadedPluginResource::SCRIPT,
           "", scriptConfig)));

    Utf8String jsFunctionSource = itl->pluginResource->getScript(PackageElement::MAIN);


    v8::Locker locker(itl->isolate.isolate);
    v8::Isolate::Scope isolate(itl->isolate.isolate);

    HandleScope handle_scope(itl->isolate.isolate);
    Context::Scope context_scope(itl->context.Get(itl->isolate.isolate));
        
    v8::Local<v8::Object> globalPrototype
        = itl->context.Get(itl->isolate.isolate)
        ->Global()->GetPrototype().As<v8::Object>();
    globalPrototype->Set(String::NewFromUtf8(itl->isolate.isolate, "args"),
                         JS::toJS(jsonEncode(scriptConfig.args)));

    // Create a string containing the Opencl source code.
    auto source
        = String::NewFromUtf8(itl->isolate.isolate, jsFunctionSource.rawData());
    
    // Compile the source code.
    TryCatch trycatch;
    trycatch.SetVerbose(true);

    auto script = Script::Compile(source, v8::String::NewFromUtf8(itl->isolate.isolate, scriptConfig.address.c_str()));
    
    ScriptOutput result;
            
    if (script.IsEmpty()) {  
        auto rep = convertException(trycatch, "Compiling plugin script");

        {
            std::unique_lock<std::mutex> guard(itl->logMutex);
            LOG(itl->loader) << jsonEncode(rep) << endl;
        }

        result.exception.reset(new ScriptException(std::move(rep)));
    }
    else {
        itl->script.Reset(itl->isolate.isolate, script);
            
        // Run the script to get the result.
        Handle<Value> scriptResult
            = itl->script.Get(itl->isolate.isolate)->Run();
            
        if (scriptResult.IsEmpty()) {  
            auto rep = convertException(trycatch, "Running script");
                
            {
                std::unique_lock<std::mutex> guard(itl->logMutex);
                LOG(itl->loader) << jsonEncode(rep) << endl;
            }
                
            result.exception.reset(new ScriptException(std::move(rep)));
        }
        else {
            result.result = JS::fromJS(scriptResult);
                
            {
                std::unique_lock<std::mutex> guard(itl->logMutex);
                LOG(itl->loader) << jsonEncode(result.result) << endl;
            }
        }
    }

    result.logs = std::move(itl->logs);

    return result;
}
#endif

/*****************************************************************************/
/* REGISTRY                                                                  */
/*****************************************************************************/

RegisterPluginType<OpenCLPlugin, PluginResource>
regOpenCL(builtinPackage(),
          "opencl",
          "OpenCL plugin loader",
          "lang/OpenCL.md.html",
          &OpenCLPlugin::handleTypeRoute);

struct AtInitCL {
    AtInitCL()
    {
        cerr << "AT INIT CL" << endl;
        cerr << "bonus" << endl;
    }

} atInitCL;

} // namespace MLDB
