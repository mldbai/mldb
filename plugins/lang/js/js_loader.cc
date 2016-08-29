/** js_plugin_loader.cc
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Plugin loader for Javascript plugins.
*/

#include "mldb/types/basic_value_descriptions.h"
#include "mldb/core/plugin.h"
#include "mldb/core/dataset.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/plugin_resource.h"
#include "mldb/server/static_content_handler.h"
#include "mldb/sql/cell_value.h"
#include "mldb/http/http_exception.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/jml/utils/file_functions.h"
#include "mldb/types/any_impl.h"

#include "js_common.h"
#include "mldb_js.h"
#include "dataset_js.h"
#include "function_js.h"
#include "procedure_js.h"
#include <v8.h>

#include "mldb/types/string.h"

#include <boost/algorithm/string.hpp>

using namespace std;


namespace Datacratic {
namespace MLDB {


/****************************************************************************/
/* JS PLUGIN                                                                */
/****************************************************************************/

struct JsPluginContext;
struct ScriptOutput;

struct JavascriptPlugin: public Plugin {
    JavascriptPlugin(MldbServer * server,
                     PolyConfig config,
                     std::function<bool (const Json::Value & progress)> onProgress);
    
    ~JavascriptPlugin();
    
    virtual Any getStatus() const;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

    static ScriptOutput
    runJavascriptScript(MldbServer * server,
                        const PluginResource & scriptConfig);

    static RestRequestMatchResult
    handleTypeRoute(RestDirectory * server, RestConnection & conn,
                    const RestRequest & request,
                    RestRequestParsingContext & context);

    std::unique_ptr<JsPluginContext> itl;
};


/*****************************************************************************/
/* JS PLUGIN                                                                 */
/*****************************************************************************/

struct JsPluginJS {

    static v8::Local<v8::ObjectTemplate>
    registerMe()
    {
        using namespace v8;

        HandleScope scope;

        v8::Local<v8::ObjectTemplate> result(ObjectTemplate::New());

        result->SetInternalFieldCount(2);

        result->Set(String::New("log"),
                                   FunctionTemplate::New(log));
        result->Set(String::New("setStatusHandler"),
                    FunctionTemplate::New(setStatusHandler));
        result->Set(String::New("setRequestHandler"),
                    FunctionTemplate::New(setRequestHandler));
        result->Set(String::New("serveStaticFolder"),
                    FunctionTemplate::New(serveStaticFolder));
        result->Set(String::New("serveDocumentationFolder"),
                    FunctionTemplate::New(serveDocumentationFolder));
        result->Set(String::New("getPluginDir"),
                    FunctionTemplate::New(getPluginDir));
        
        return scope.Close(result);
    }

    static JsPluginContext * getShared(const v8::Handle<v8::Object> & val)
    {
        return reinterpret_cast<JsPluginContext *>
        (v8::Handle<v8::External>::Cast
         (val->GetInternalField(0))->Value());
    }

    static v8::Handle<v8::Value>
    log(const v8::Arguments & args)
    {
        using namespace v8;
        auto itl = getShared(args.This());

        try {
            Utf8String line;

            for (unsigned i = 0;  i < args.Length();  ++i) {
                Utf8String printed;

                if (args[i]->IsString() || args[i]->IsStringObject()) {
                    printed = JS::utf8str(args[i]);
                }
                else if (args[i]->IsObject() || args[i]->IsArray()) {
                    Json::Value val = JS::fromJS(args[i]);
                    printed = val.toStyledString();
                }
                else {
                    printed = JS::utf8str(args[i]);
                }
                
                if (i != 0)
                    line += " ";
                line += printed;
            }

            line += "\n";

            {
                std::unique_lock<std::mutex> guard(itl->logMutex);
                LOG(itl->category) << line;
                itl->logs.emplace_back(Date::now(), "logs", Utf8String(line));
            }

            return args.This();
        } HANDLE_JS_EXCEPTIONS;
    }

    static v8::Handle<v8::Value>
    setStatusHandler(const v8::Arguments & args)
    {
        try {
            if (args.Length() < 1)
                throw ML::Exception("Need a handler");

            JsPluginContext * itl = getShared(args.This());

            cerr << JS::cstr(args[0]) << endl;

            struct Args {
                v8::Persistent<v8::Function> fn;
                v8::Persistent<v8::Object> self;
            };

            std::shared_ptr<Args> shargs(new Args());

            shargs->fn = v8::Persistent<v8::Function>::New(v8::Handle<v8::Function>(v8::Function::Cast(*args[0])));
            shargs->self = v8::Persistent<v8::Object>::New(args.This());
            
            auto getStatus = [itl,shargs] () -> Json::Value
                {
                    v8::Locker locker(itl->isolate.isolate);
                    v8::Isolate::Scope isolate(itl->isolate.isolate);
                    v8::HandleScope handle_scope;
                    v8::Context::Scope context_scope(itl->context);

                    v8::TryCatch trycatch;
                    trycatch.SetVerbose(true);
                    auto result = shargs->fn->Call(shargs->self, 0, nullptr);

                    if (result.IsEmpty()) {
                        ScriptException exc = convertException(trycatch,
                                                              "JS Plugin Status callback");

                        {
                            std::unique_lock<std::mutex> guard(itl->logMutex);
                            LOG(itl->loader) << jsonEncode(exc) << endl;
                        }

                        JML_TRACE_EXCEPTIONS(false);
                        throw HttpReturnException(400, "Exception in JS status function", exc);
                    }

                    Json::Value jsonResult = JS::fromJS(result);
                    return jsonResult;
                };

            cerr << "setting status" << endl;

            itl->getStatus = getStatus;

            cerr << "done setting status" << endl;

            return args.This();
        } HANDLE_JS_EXCEPTIONS;
    }

    static v8::Handle<v8::Value>
    serveStaticFolder(const v8::Arguments & args)
    {
        try {
            if (args.Length() < 1)
                throw ML::Exception("Need a handler");

            JsPluginContext * itl = getShared(args.This());

            auto route = JS::getArg<std::string>(args, 0, "route");
            auto dir = JS::getArg<std::string>(args, 1, "dir");

            itl->router.addRoute(Rx(route + "/(.*)", "<resource>"),
                                 "GET", "Static content",
                                 getStaticRouteHandler(dir, itl->server),
                                 Json::Value());

            return args.This();
        } HANDLE_JS_EXCEPTIONS;
    }

    static v8::Handle<v8::Value>
    serveDocumentationFolder(const v8::Arguments & args)
    {
        try {
            if (args.Length() < 1)
                throw ML::Exception("Need a handler");

            JsPluginContext * itl = getShared(args.This());

            auto route = JS::getArg<std::string>(args, 0, "route");
            auto dir = JS::getArg<std::string>(args, 1, "dir");

            serveDocumentationDirectory(itl->router, route, dir, itl->server);

            return args.This();
        } HANDLE_JS_EXCEPTIONS;
    }

    static v8::Handle<v8::Value>
    getPluginDir(const v8::Arguments & args)
    {
        try {
            JsPluginContext * itl = getShared(args.This());

            return JS::toJS(itl->pluginResource->getPluginDir().string());

        } HANDLE_JS_EXCEPTIONS;
    }

    static v8::Handle<v8::Value>
    setRequestHandler(const v8::Arguments & args)
    {
        try {
            if (args.Length() < 1)
                throw ML::Exception("Need a handler");

            JsPluginContext * itl = getShared(args.This());

            cerr << JS::cstr(args[0]) << endl;

            struct Args {
                v8::Persistent<v8::Function> fn;
                v8::Persistent<v8::Object> self;
            };

            std::shared_ptr<Args> shargs(new Args());

            shargs->fn = v8::Persistent<v8::Function>::New(v8::Handle<v8::Function>(v8::Function::Cast(*args[0])));
            shargs->self = v8::Persistent<v8::Object>::New(args.This());
            
            auto handleRequest = [itl,shargs] (RestConnection & connection,
                                                  const RestRequest & request,
                                                  RestRequestParsingContext & context)
                -> RestRequestMatchResult
                {
                    v8::Locker locker(itl->isolate.isolate);
                    v8::Isolate::Scope isolate(itl->isolate.isolate);
                    v8::HandleScope handle_scope;
                    v8::Context::Scope context_scope(itl->context);

                    // Create some context
                    v8::Handle<v8::Value> args[8] = {
                        JS::toJS(context.remaining),
                        JS::toJS(request.verb),
                        JS::toJS(request.resource),
                        JS::toJS(request.params),
                        JS::toJS(request.payload),
                        JS::toJS(request.header.contentType),
                        JS::toJS(request.header.contentLength),
                        JS::toJS(request.header.headers)
                    };
                        

                    v8::TryCatch trycatch;
                    trycatch.SetVerbose(true);
                    auto result = shargs->fn->Call(shargs->self, 8, args);

                    if (result.IsEmpty()) {
                        ScriptException exc = convertException(trycatch,
                                                              "JS Plugin request callback");
                        {
                            std::unique_lock<std::mutex> guard(itl->logMutex);
                            LOG(itl->loader) << jsonEncode(exc) << endl;
                        }

                        connection.sendResponse(400, jsonEncode(exc));
                        return RestRequestRouter::MR_YES;
                    }

                    Json::Value jsonResult = JS::fromJS(result);

                    connection.sendResponse(200, jsonResult);

                    return RestRequestRouter::MR_YES;
                };

            itl->handleRequest = handleRequest;

            return args.This();
        } HANDLE_JS_EXCEPTIONS;
    }
};


/*****************************************************************************/
/* JS PLUGIN CONTEXT                                                         */
/*****************************************************************************/

JsPluginContext::
JsPluginContext(const Utf8String & pluginName,
                MldbServer * server,
                std::shared_ptr<LoadedPluginResource> pluginResource)
    : isolate(false /* for this thread only */),
      categoryName(pluginName.rawString() + " plugin"),
      loaderName(pluginName.rawString() + " loader"),
      category(categoryName.c_str()),
      loader(loaderName.c_str()),
      server(server),
      pluginResource(pluginResource)
{
    using namespace v8;

    v8::Locker locker(this->isolate.isolate);
    v8::Isolate::Scope isolate(this->isolate.isolate);

    HandleScope handle_scope;

    // Create a new context.
    context = v8::Persistent<v8::Context>::New(Context::New());

    // Enter the created context for compiling and
    // running the hello world script. 
    Context::Scope context_scope(context);
    
    // This is how we set it
    // https://code.google.com/p/v8/issues/detail?id=54
    v8::Local<v8::Object> globalPrototype
        = v8::Local<v8::Object>::Cast(context->Global()->GetPrototype());

    auto plugin = JsPluginJS::registerMe()->NewInstance();
    plugin->SetInternalField(0, v8::External::New(this));
    plugin->SetInternalField(1, v8::External::New(this));
    if (pluginResource)
        plugin->Set(String::New("args"), JS::toJS(jsonEncode(pluginResource->args)));
    globalPrototype->Set(String::New("plugin"), plugin);

    auto mldb = MldbJS::registerMe()->NewInstance();
    mldb->SetInternalField(0, v8::External::New(this->server));
    mldb->SetInternalField(1, v8::External::New(this));
    globalPrototype->Set(String::New("mldb"), mldb);

    Stream = v8::Persistent<v8::FunctionTemplate>::New(StreamJS::registerMe());
    Dataset = v8::Persistent<v8::FunctionTemplate>::New(DatasetJS::registerMe());
    Function = v8::Persistent<v8::FunctionTemplate>::New(FunctionJS::registerMe());
    Procedure = v8::Persistent<v8::FunctionTemplate>::New(ProcedureJS::registerMe());
    CellValue = v8::Persistent<v8::FunctionTemplate>::New(CellValueJS::registerMe());
    RandomNumberGenerator = v8::Persistent<v8::FunctionTemplate>::New(RandomNumberGeneratorJS::registerMe());
}

JsPluginContext::
~JsPluginContext()
{
}



/*****************************************************************************/
/* JS PLUGIN                                                                 */
/*****************************************************************************/

JavascriptPlugin::
JavascriptPlugin(MldbServer * server,
                 PolyConfig config,
                 std::function<bool (const Json::Value & progress)> onProgress)
    : Plugin(server)
{
    using namespace v8;

    static V8Init v8Init;

    PluginResource res = config.params.convert<PluginResource>();
    itl.reset(new JsPluginContext(config.id, this->server,
                                  std::make_shared<LoadedPluginResource>
                                  (JAVASCRIPT,
                                   LoadedPluginResource::PLUGIN,
                                   config.id, res)));
    
    Utf8String jsFunctionSource = itl->pluginResource->getScript(PackageElement::MAIN);


    v8::Locker locker(itl->isolate.isolate);
    v8::Isolate::Scope isolate(itl->isolate.isolate);

    HandleScope handle_scope;
    Context::Scope context_scope(itl->context);
    
    // Create a string containing the JavaScript source code.
    Handle<String> source = String::New(jsFunctionSource.rawData(),
                                        jsFunctionSource.rawLength());

    // Compile the source code.
    TryCatch trycatch;
    trycatch.SetVerbose(true);

    auto script = Script::Compile(source, v8::String::New(itl->pluginResource->getFilenameForErrorMessages().c_str()));
    
    if (script.IsEmpty()) {  
        auto rep = convertException(trycatch, "Compiling plugin script");

        {
            std::unique_lock<std::mutex> guard(itl->logMutex);
            LOG(itl->loader) << jsonEncode(rep) << endl;
        }

        JML_TRACE_EXCEPTIONS(false);
        throw HttpReturnException(400, "Exception compiling plugin script", rep);
    }

    itl->script = v8::Persistent<v8::Script>::New(script);

    // Run the script to get the result.
    Handle<Value> result = itl->script->Run();

    if (result.IsEmpty()) {  
        auto rep = convertException(trycatch, "Running plugin script");
        
        {
            std::unique_lock<std::mutex> guard(itl->logMutex);
            LOG(itl->loader) << jsonEncode(rep) << endl;
        }
        
        JML_TRACE_EXCEPTIONS(false);
        throw HttpReturnException(400, "Exception running plugin script", rep);
    }
    
    cerr << "script returned " << JS::cstr(result) << endl;
}
    
JavascriptPlugin::
~JavascriptPlugin()
{
}
    
Any
JavascriptPlugin::
getStatus() const
{
    if (itl->getStatus) {
        try {
            return itl->getStatus();
        } catch (const JsException & exc) {
            throw HttpReturnException(400, "Exception getting plugin status", exc.rep);
        }
    }
    else return Any();
}

RestRequestMatchResult
JavascriptPlugin::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    // First, check for a route
    auto res = itl->router.processRequest(connection, request, context);
    if (res != RestRequestRouter::MR_NO)
        return res;

    // Second, check for a generic request handler
    if (itl->handleRequest)
        return itl->handleRequest(connection, request, context);

    // Otherwise we simply don't handle it
    return RestRequestRouter::MR_NO;
}

RestRequestMatchResult
JavascriptPlugin::
handleTypeRoute(RestDirectory * server,
                RestConnection & conn,
                const RestRequest & request,
                RestRequestParsingContext & context)
{
    //cerr << "context.remaining = " << context.remaining << endl;

    if (context.resources.back() == "run") {
        //cerr << "request.payload = " << request.payload << endl;
        
        auto scriptConfig = jsonDecodeStr<ScriptResource>(request.payload).toPluginConfig();
        
        auto result = runJavascriptScript(static_cast<MldbServer *>(server),
                                          scriptConfig);
        conn.sendResponse(result.exception ? 400 : 200,
                          jsonEncodeStr(result), "application/json");
        return RestRequestRouter::MR_YES;
    }

    return RestRequestRouter::MR_NO;
}

ScriptOutput
JavascriptPlugin::
runJavascriptScript(MldbServer * server,
                    const PluginResource & scriptConfig)
{
    using namespace v8;

    std::unique_ptr<JsPluginContext> itl
        (new JsPluginContext
         ("script runner", server,
          std::make_shared<LoadedPluginResource>
          (JAVASCRIPT,
           LoadedPluginResource::SCRIPT,
           "", scriptConfig)));

    Utf8String jsFunctionSource = itl->pluginResource->getScript(PackageElement::MAIN);


    v8::Locker locker(itl->isolate.isolate);
    v8::Isolate::Scope isolate(itl->isolate.isolate);

    HandleScope handle_scope;
    Context::Scope context_scope(itl->context);
        
    v8::Local<v8::Object> globalPrototype
        = v8::Local<v8::Object>::Cast(itl->context->Global()->GetPrototype());
    globalPrototype->Set(String::New("args"), JS::toJS(jsonEncode(scriptConfig.args)));

    // Create a string containing the JavaScript source code.
    Handle<String> source = String::New(jsFunctionSource.rawData(),
                                        jsFunctionSource.rawLength());

    // Compile the source code.
    TryCatch trycatch;
    trycatch.SetVerbose(true);

    auto script = Script::Compile(source, v8::String::New(scriptConfig.address.c_str()));
    
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
        itl->script = v8::Persistent<v8::Script>::New(script);
            
        // Run the script to get the result.
        Handle<Value> scriptResult = itl->script->Run();
            
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


RegisterPluginType<JavascriptPlugin, PluginResource>
regJavascript(builtinPackage(),
              "javascript",
              "Javascript plugin loader",
              "lang/Javascript.md.html",
              &JavascriptPlugin::handleTypeRoute);


} // namespace MLDB
} // namespace Datacratic
