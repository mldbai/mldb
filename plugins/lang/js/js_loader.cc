/** js_plugin_loader.cc
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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
#include "mldb/ext/v8-cross-build-output/include/v8.h"

#include "mldb/types/string.h"

#include <boost/algorithm/string.hpp>

using namespace std;

namespace fs = boost::filesystem;


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
        v8::Isolate* isolate = v8::Isolate::GetCurrent();
        using namespace v8;

        EscapableHandleScope scope(isolate);

        v8::Local<v8::ObjectTemplate> result(ObjectTemplate::New(isolate));

        result->SetInternalFieldCount(2);

        result->Set(String::NewFromUtf8(isolate, "log"),
                    FunctionTemplate::New(isolate, log));
        result->Set(String::NewFromUtf8(isolate, "setStatusHandler"),
                    FunctionTemplate::New(isolate, setStatusHandler));
        result->Set(String::NewFromUtf8(isolate, "setRequestHandler"),
                    FunctionTemplate::New(isolate, setRequestHandler));
        result->Set(String::NewFromUtf8(isolate, "serveStaticFolder"),
                    FunctionTemplate::New(isolate, serveStaticFolder));
        result->Set(String::NewFromUtf8(isolate, "serveDocumentationFolder"),
                    FunctionTemplate::New(isolate, serveDocumentationFolder));
        result->Set(String::NewFromUtf8(isolate, "getPluginDir"),
                    FunctionTemplate::New(isolate, getPluginDir));
        
        return scope.Escape(result);
    }

    static JsPluginContext * getShared(const v8::Handle<v8::Object> & val)
    {
        return reinterpret_cast<JsPluginContext *>
        (v8::Handle<v8::External>::Cast
         (val->GetInternalField(0))->Value());
    }

    static void
    log(const v8::FunctionCallbackInfo<v8::Value> & args)
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

            args.GetReturnValue().Set(args.This());
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    setStatusHandler(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            if (args.Length() < 1)
                throw MLDB::Exception("Need a handler");

            JsPluginContext * itl = getShared(args.This());

            cerr << JS::cstr(args[0]) << endl;

            struct Args {
                v8::Persistent<v8::Function> fn;
                v8::Persistent<v8::Object> self;
            };

            std::shared_ptr<Args> shargs(new Args());

            shargs->fn.Reset(itl->isolate.isolate,
                             args[0].As<v8::Function>());
            shargs->self.Reset(itl->isolate.isolate,
                               args.This());
            
            auto getStatus = [itl,shargs] () -> Json::Value
                {
                    v8::Locker locker(itl->isolate.isolate);
                    v8::Isolate::Scope isolate(itl->isolate.isolate);
                    v8::HandleScope handle_scope(itl->isolate.isolate);
                    v8::Context::Scope context_scope(itl->context.Get(itl->isolate.isolate));

                    v8::TryCatch trycatch;
                    trycatch.SetVerbose(true);
                    auto result = shargs->fn.Get(itl->isolate.isolate)
                        ->Call(shargs->self.Get(itl->isolate.isolate),
                               0, nullptr);

                    if (result.IsEmpty()) {
                        ScriptException exc = convertException(trycatch,
                                                              "JS Plugin Status callback");

                        {
                            std::unique_lock<std::mutex> guard(itl->logMutex);
                            LOG(itl->loader) << jsonEncode(exc) << endl;
                        }

                        MLDB_TRACE_EXCEPTIONS(false);
                        throw HttpReturnException(400, "Exception in JS status function", exc);
                    }

                    Json::Value jsonResult = JS::fromJS(result);
                    return jsonResult;
                };

            cerr << "setting status" << endl;

            itl->getStatus = getStatus;

            cerr << "done setting status" << endl;

            args.GetReturnValue().Set(args.This());
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    serveStaticFolder(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            if (args.Length() < 1)
                throw MLDB::Exception("Need a handler");

            JsPluginContext * itl = getShared(args.This());

            auto route = JS::getArg<std::string>(args, 0, "route");
            auto dir = JS::getArg<std::string>(args, 1, "dir");

            fs::path fullDir(fs::path(itl->pluginResource->getPluginDir().string()) / fs::path(dir));
            if(!fs::exists(fullDir)) {
                throw MLDB::Exception("Cannot serve static folder for path that does "
                        "not exist: " + fullDir.string());
            }

            itl->router.addRoute(Rx(route + "/(.*)", "<resource>"),
                                 "GET", "Static content",
                                 getStaticRouteHandler(fullDir.string(), itl->server),
                                 Json::Value());

            args.GetReturnValue().Set(args.This());
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    serveDocumentationFolder(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            if (args.Length() < 1)
                throw MLDB::Exception("Need a handler");

            JsPluginContext * itl = getShared(args.This());

            auto route = JS::getArg<std::string>(args, 0, "route");
            auto dir = JS::getArg<std::string>(args, 1, "dir");

            serveDocumentationDirectory(itl->router, route, dir, itl->server);

            args.GetReturnValue().Set(args.This());
        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    getPluginDir(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            JsPluginContext * itl = getShared(args.This());

            args.GetReturnValue().Set(JS::toJS(itl->pluginResource->getPluginDir().string()));

        } HANDLE_JS_EXCEPTIONS(args);
    }

    static void
    setRequestHandler(const v8::FunctionCallbackInfo<v8::Value> & args)
    {
        try {
            if (args.Length() < 1)
                throw MLDB::Exception("Need a handler");

            JsPluginContext * itl = getShared(args.This());

            cerr << JS::cstr(args[0]) << endl;

            struct Args {
                v8::Persistent<v8::Function> fn;
                v8::Persistent<v8::Object> self;
            };

            std::shared_ptr<Args> shargs(new Args());

            shargs->fn.Reset(args.GetIsolate(),
                             args[0].As<v8::Function>());
            shargs->self.Reset(args.GetIsolate(), args.This());
            
            auto handleRequest
                = [itl,shargs] (RestConnection & connection,
                                const RestRequest & request,
                                RestRequestParsingContext & context)
                -> RestRequestMatchResult
                {
                    v8::Locker locker(itl->isolate.isolate);
                    v8::Isolate::Scope isolate(itl->isolate.isolate);
                    v8::HandleScope handle_scope(itl->isolate.isolate);
                    v8::Context::Scope context_scope
                        (itl->context.Get(itl->isolate.isolate));

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
                    auto result = shargs->fn.Get(itl->isolate.isolate)
                        ->Call(shargs->self.Get(itl->isolate.isolate), 8, args);

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

            args.GetReturnValue().Set(args.This());
        } HANDLE_JS_EXCEPTIONS(args);
    }
};


/*****************************************************************************/
/* JS PLUGIN CONTEXT                                                         */
/*****************************************************************************/

JsPluginContext::
JsPluginContext(const Utf8String & pluginName,
                MldbServer * server,
                std::shared_ptr<LoadedPluginResource> pluginResource)
    : categoryName(pluginName.rawString() + " plugin"),
      loaderName(pluginName.rawString() + " loader"),
      category(categoryName.c_str()),
      loader(loaderName.c_str()),
      server(server),
      pluginResource(pluginResource)
{
    using namespace v8;

    static V8Init v8Init(server);

    isolate.init(false /* for this thread only */);

    v8::Locker locker(this->isolate.isolate);
    v8::Isolate::Scope isolate(this->isolate.isolate);

    HandleScope handle_scope(this->isolate.isolate);

    // Create a new context.
    context.Reset(this->isolate.isolate,
                  Context::New(this->isolate.isolate));
    
    // Enter the created context for compiling and
    // running the hello world script. 
    Context::Scope context_scope(context.Get(this->isolate.isolate));
    
    // This is how we set it
    // https://code.google.com/p/v8/issues/detail?id=54
    v8::Local<v8::Object> globalPrototype
        = context.Get(this->isolate.isolate)->Global()
        ->GetPrototype().As<v8::Object>();
    
    auto plugin = JsPluginJS::registerMe()->NewInstance();
    plugin->SetInternalField(0, v8::External::New(this->isolate.isolate, this));
    plugin->SetInternalField(1, v8::External::New(this->isolate.isolate, this));
    if (pluginResource)
        plugin->Set(String::NewFromUtf8(this->isolate.isolate, "args"), JS::toJS(jsonEncode(pluginResource->args)));
    globalPrototype->Set(String::NewFromUtf8(this->isolate.isolate, "plugin"), plugin);

    auto mldb = MldbJS::registerMe()->NewInstance();
    mldb->SetInternalField(0, v8::External::New(this->isolate.isolate, this->server));
    mldb->SetInternalField(1, v8::External::New(this->isolate.isolate, this));
    globalPrototype->Set(String::NewFromUtf8(this->isolate.isolate, "mldb"), mldb);

    Stream.Reset(this->isolate.isolate, StreamJS::registerMe());
    Dataset.Reset(this->isolate.isolate, DatasetJS::registerMe());
    Function.Reset(this->isolate.isolate, FunctionJS::registerMe());
    Procedure.Reset(this->isolate.isolate, ProcedureJS::registerMe());
    CellValue.Reset(this->isolate.isolate, CellValueJS::registerMe());
    RandomNumberGenerator.Reset(this->isolate.isolate,
                                RandomNumberGeneratorJS::registerMe());
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

    PluginResource res = config.params.convert<PluginResource>();
    itl.reset(new JsPluginContext(config.id, this->server,
                                  std::make_shared<LoadedPluginResource>
                                  (JAVASCRIPT,
                                   LoadedPluginResource::PLUGIN,
                                   config.id, res)));
    
    Utf8String jsFunctionSource = itl->pluginResource->getScript(PackageElement::MAIN);


    v8::Locker locker(itl->isolate.isolate);
    v8::Isolate::Scope isolate(itl->isolate.isolate);

    HandleScope handle_scope(itl->isolate.isolate);
    Context::Scope context_scope(itl->context.Get(itl->isolate.isolate));
    
    // Create a string containing the JavaScript source code.
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

    HandleScope handle_scope(itl->isolate.isolate);
    Context::Scope context_scope(itl->context.Get(itl->isolate.isolate));
        
    v8::Local<v8::Object> globalPrototype
        = itl->context.Get(itl->isolate.isolate)
        ->Global()->GetPrototype().As<v8::Object>();
    globalPrototype->Set(String::NewFromUtf8(itl->isolate.isolate, "args"),
                         JS::toJS(jsonEncode(scriptConfig.args)));

    // Create a string containing the JavaScript source code.
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


RegisterPluginType<JavascriptPlugin, PluginResource>
regJavascript(builtinPackage(),
              "javascript",
              "Javascript plugin loader",
              "lang/Javascript.md.html",
              &JavascriptPlugin::handleTypeRoute);


} // namespace MLDB

