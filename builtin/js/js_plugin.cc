/** js_plugin_loader.cc
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Plugin loader for Javascript plugins.
*/

#include "js_plugin.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/core/plugin.h"
#include "mldb/core/dataset.h"
#include "mldb/core/mldb_engine.h"
#include "mldb/builtin/plugin_resource.h"
#include "mldb/engine/static_content_handler.h"
#include "mldb/sql/cell_value.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/arch/file_functions.h"
#include "mldb/types/any_impl.h"
#include "mldb/rest/rest_entity.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/rest/in_process_rest_connection.h"

#include "js_common.h"
#include "mldb_js.h"
#include "dataset_js.h"
#include "function_js.h"
#include "procedure_js.h"
#include "sensor_js.h"
#include "v8.h"
#include "mldb/engine/static_content_macro.h"

#include "mldb/types/string.h"

#include "mldb/utils/split.h"

using namespace std;

namespace fs = std::filesystem;


namespace MLDB {


/*****************************************************************************/
/* JS PLUGIN JS                                                              */
/*****************************************************************************/

/** This what the "plugin" object (which represents itself) looks like to a
    JS plugin.  It's distinct from the PluginJS, which is what a generic
    MLDB plugin looks like.
*/

v8::Local<v8::ObjectTemplate>
JsPluginJS::
registerMe()
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    using namespace v8;

    EscapableHandleScope scope(isolate);

    v8::Local<v8::ObjectTemplate> result(ObjectTemplate::New(isolate));

    result->SetInternalFieldCount(2);

    result->Set(isolate, "log", FunctionTemplate::New(isolate, log));
    result->Set(isolate, "setStatusHandler",
                FunctionTemplate::New(isolate, setStatusHandler));
    result->Set(isolate, "setRequestHandler",
                FunctionTemplate::New(isolate, setRequestHandler));
    result->Set(isolate, "serveStaticFolder",
                FunctionTemplate::New(isolate, serveStaticFolder));
    result->Set(isolate, "serveDocumentationFolder",
                FunctionTemplate::New(isolate, serveDocumentationFolder));
    result->Set(isolate, "getPluginDir",
                FunctionTemplate::New(isolate, getPluginDir));
        
    return scope.Escape(result);
}

JsPluginContext *
JsPluginJS::
getShared(const v8::Handle<v8::Object> & val)
{
    return reinterpret_cast<JsPluginContext *>
        (v8::Handle<v8::External>::Cast
         (val->GetInternalField(0))->Value());
}

void
JsPluginJS::
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

void
JsPluginJS::
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

                v8::TryCatch trycatch(itl->isolate.isolate);
                trycatch.SetVerbose(true);
                auto result = shargs->fn.Get(itl->isolate.isolate)
                ->Call(itl->getLocalContext(), shargs->self.Get(itl->isolate.isolate),
                       0, nullptr);

                if (result.IsEmpty()) {
                    ScriptException exc = convertException(trycatch,
                                                           "JS Plugin Status callback");

                    {
                        std::unique_lock<std::mutex> guard(itl->logMutex);
                        LOG(itl->loader) << jsonEncode(exc) << endl;
                    }

                    MLDB_TRACE_EXCEPTIONS(false);
                    throw AnnotatedException(400, "Exception in JS status function", exc);
                }

                Json::Value jsonResult = JS::fromJS(result.ToLocalChecked());
                return jsonResult;
            };

        cerr << "setting status" << endl;

        itl->getStatus = getStatus;

        cerr << "done setting status" << endl;

        args.GetReturnValue().Set(args.This());
    } HANDLE_JS_EXCEPTIONS(args);
}

void
JsPluginJS::
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
                             itl->engine->getStaticRouteHandler(fullDir.string()),
                             Json::Value());

        args.GetReturnValue().Set(args.This());
    } HANDLE_JS_EXCEPTIONS(args);
}

void
JsPluginJS::
serveDocumentationFolder(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        if (args.Length() < 1)
            throw MLDB::Exception("Need a handler");

        JsPluginContext * itl = getShared(args.This());

        auto route = JS::getArg<std::string>(args, 0, "route");
        auto dir = JS::getArg<std::string>(args, 1, "dir");

        serveDocumentationDirectory(itl->router, route, dir, itl->engine);

        args.GetReturnValue().Set(args.This());
    } HANDLE_JS_EXCEPTIONS(args);
}

void
JsPluginJS::
getPluginDir(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        JsPluginContext * itl = getShared(args.This());

        args.GetReturnValue().Set(JS::toJS(itl->pluginResource->getPluginDir().string()));

    } HANDLE_JS_EXCEPTIONS(args);
}

void
JsPluginJS::
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
                        

                v8::TryCatch trycatch(itl->isolate.isolate);
                trycatch.SetVerbose(true);
                auto result = shargs->fn.Get(itl->isolate.isolate)
                ->Call(itl->getLocalContext(), shargs->self.Get(itl->isolate.isolate), 8, args);

                if (result.IsEmpty()) {
                    ScriptException exc = convertException(trycatch,
                                                           "JS Plugin request callback");
                    {
                        std::unique_lock<std::mutex> guard(itl->logMutex);
                        LOG(itl->loader) << jsonEncode(exc) << endl;
                    }

                    connection.sendJsonResponse(400, jsonEncode(exc));
                    return RestRequestRouter::MR_YES;
                }

                Json::Value jsonResult = JS::fromJS(result.ToLocalChecked());

                connection.sendJsonResponse(200, jsonResult);

                return RestRequestRouter::MR_YES;
            };

        itl->handleRequest = handleRequest;

        args.GetReturnValue().Set(args.This());
    } HANDLE_JS_EXCEPTIONS(args);
}


/*****************************************************************************/
/* JS PLUGIN                                                                 */
/*****************************************************************************/

JavascriptPlugin::
JavascriptPlugin(MldbEngine * engine,
                 PolyConfig config,
                 std::function<bool (const Json::Value & progress)> onProgress)
    : Plugin(engine)
{
    using namespace v8;

    PluginResource res = config.params.convert<PluginResource>();
    itl.reset(new JsPluginContext(config.id, this->engine,
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
    Handle<String> source = JS::createString(itl->isolate.isolate, jsFunctionSource);

    // Compile the source code.
    TryCatch trycatch(itl->isolate.isolate);
    trycatch.SetVerbose(true);

    auto origin = JS::createScriptOrigin(itl->isolate.isolate, 
				     itl->pluginResource->getFilenameForErrorMessages());
    auto script = Script::Compile(itl->getLocalContext(), source, &origin);
    
    if (script.IsEmpty()) {  
        auto rep = convertException(trycatch, "Compiling plugin script");

        {
            std::unique_lock<std::mutex> guard(itl->logMutex);
            LOG(itl->loader) << jsonEncode(rep) << endl;
        }

        MLDB_TRACE_EXCEPTIONS(false);
        throw AnnotatedException(400, "Exception compiling plugin script", rep);
    }

    itl->script.Reset(itl->isolate.isolate, script.ToLocalChecked());

    // Run the script to get the result.
    Handle<Value> result = JS::toLocalChecked(itl->script.Get(itl->isolate.isolate)->Run(itl->getLocalContext()));

    if (result.IsEmpty()) {  
        auto rep = convertException(trycatch, "Running plugin script");
        
        {
            std::unique_lock<std::mutex> guard(itl->logMutex);
            LOG(itl->loader) << jsonEncode(rep) << endl;
        }
        
        MLDB_TRACE_EXCEPTIONS(false);
        throw AnnotatedException(400, "Exception running plugin script", rep);
    }
    
    //cerr << "script returned " << JS::cstr(result) << endl;
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
            throw AnnotatedException(400, "Exception getting plugin status", exc.rep);
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
handleTypeRoute(RestDirectory * engine,
                RestConnection & conn,
                const RestRequest & request,
                RestRequestParsingContext & context)
{
    // resources will be ["/v1","/types","/plugins","/javascript","javascript","/routes/...","..."]
    cerr << "context.resources = " << jsonEncodeStr(context.resources) << endl;
    
    if (context.resources.size() == 7 && context.resources[6] == "run") {
        //cerr << "request.payload = " << request.payload << endl;
        
        auto scriptConfig = jsonDecodeStr<ScriptResource>(request.payload).toPluginConfig();
        
        auto result = runJavascriptScript(dynamic_cast<MldbEngine *>(engine),
                                          scriptConfig);
        conn.sendResponse(result.exception ? 400 : 200,
                          jsonEncodeStr(result), "application/json");
        return RestRequestRouter::MR_YES;
    }
    else if (context.resources.size() == 7
             && context.resources[6] == "modules") {
        using namespace v8;
        MldbEngine * mldb = dynamic_cast<MldbEngine *>(engine);
        ExcAssert(mldb);
        std::unique_ptr<JsPluginContext> itl
            (new JsPluginContext("modules", mldb, nullptr));

        // List all modules
        auto modules = itl->knownModules();

        conn.sendResponse(200, jsonEncodeStr(modules), "application/json");
        
        return RestRequestRouter::MR_YES;
    }
    else if (context.resources.size() == 7
             && context.resources[6].startsWith("modules/")) {

        MldbEngine * mldb = dynamic_cast<MldbEngine *>(engine);
        ExcAssert(mldb);
        std::unique_ptr<JsPluginContext> itl
            (new JsPluginContext("modules", mldb, nullptr));

        Utf8String moduleName = context.resources[6];
        moduleName.removePrefix("modules/");

        bool isDocHtml = moduleName.removeSuffix("/doc.html");
        bool isMarkdown = moduleName.removeSuffix("/doc.md");

        Utf8String symbolName;
        if (request.params.hasValue("symbol")) {
            symbolName = request.params.getValue("symbol");
        }
        
        using namespace v8;
        
        //cerr << "module name " << moduleName << endl;
        
        v8::Locker locker(itl->isolate.isolate);
        v8::Isolate::Scope isolate(itl->isolate.isolate);
        
        HandleScope handle_scope(itl->isolate.isolate);
        Context::Scope context_scope(itl->context.Get(itl->isolate.isolate));

        auto module = itl->getModule(moduleName);

        // Return the JSON that describes a given exported symbol
        auto getSymbolData = [&] (const Utf8String & symbolName)
            -> Json::Value
            {
                Json::Value result;
                auto context = itl->getLocalContext();

                v8::Handle<v8::Value> val
                    = JS::toLocalChecked(module->Get(context, JS::createString(itl->isolate.isolate, symbolName)));
                
                if (!val->IsObject()) {
                    result["type"] = "value";
                    result["value"] = (Json::Value)JS::fromJS(val);
                }
                else {
                    auto object = JS::toObject(val);
                    
                    if (val->IsFunction()) {
                        auto fn = JS::toFunction(val);
                        result["type"] = "function";
                        result["source"] = JS::utf8str(fn);
                    }
                    else {
                        result["type"]
                            = JS::utf8str(object->GetConstructorName());
                    }
                    
                    auto summary = object->Get(context, JS::createString(itl->isolate.isolate, "__summary"));
                    if (!summary.IsEmpty()) {
                        result["summary"] = JS::utf8str(summary.ToLocalChecked());
                    }

                    auto markdown = object->Get
                        (context, JS::createString(itl->isolate.isolate, "__markdown"));
                    if (!markdown.IsEmpty()) {
                        result["markdown"] = JS::utf8str(markdown.ToLocalChecked());
                    }
                }

                return result;
            };
       
        
        if (!symbolName.empty()) {

            Json::Value symbolData = getSymbolData(symbolName);
            const Json::Value & markdown = symbolData["markdown"];
            
            if (isDocHtml || isMarkdown) {
                if (markdown.isNull()) {
                    conn.sendResponse(404,
                                      ("Symbol " + symbolName
                                      + "in module " + moduleName
                                       + " has no __markdown attribute").rawString(),
                                      "text/plain");
                    
                    return MR_YES;
                }
                Utf8String markdownText = markdown.asStringUtf8();

                if (isDocHtml) {
                    StandaloneMacroContext context(mldb);
                    context.writeMarkdown(markdownText);
                    Utf8String html = context.getHtmlPage();
                    conn.sendResponse(200, html.rawString(), "text/html");
                    return RestRequestRouter::MR_YES;
                }
                else {
                    conn.sendResponse(200, markdownText.rawString(), "text/markdown");
                    return RestRequestRouter::MR_YES;
                }
            }
            else {
                conn.sendJsonResponse(200, symbolData);
                return RestRequestRouter::MR_YES;
            }
        }
        
        if (isDocHtml || isMarkdown) {

            Utf8String markdownText;
            // Look for the "__markdown" string, and if it's there render it
            auto md = module->Get(itl->getLocalContext(), JS::createString(itl->isolate.isolate, "__markdown"));
            if (!md.IsEmpty()) {
                markdownText = JS::utf8str(md.ToLocalChecked());
            }
            else {
                conn.sendResponse(404,
                                  "Module " + moduleName
                                  + " has no __markdown attribute",
                                  "text/plain");
                
                return MR_YES;
            }

            if (isDocHtml) {
                StandaloneMacroContext context(mldb);
                context.writeMarkdown(markdownText);
                Utf8String html = context.getHtmlPage();
                conn.sendResponse(200, html.rawString(), "text/html");
                return RestRequestRouter::MR_YES;
            }
            else {
                conn.sendResponse(200, markdownText.rawString(), "text/markdown");
                return RestRequestRouter::MR_YES;
            }
        }
        else {
            auto props = JS::toLocalChecked(module->GetOwnPropertyNames(itl->getLocalContext()));

            Json::Value result;
            
            for (size_t i = 0;  i < props->Length();  ++i) {
                Utf8String prop = JS::utf8str(props->Get(itl->getLocalContext(), i));
                if (prop.startsWith("__"))
                    continue;

                auto symbolData = getSymbolData(prop);

                result[prop] = symbolData;
            }

            conn.sendResponse(200, jsonEncodeStr(result),
                              "application/json");
            return RestRequestRouter::MR_YES;
        }
        
        return RestRequestRouter::MR_YES;
    }
        
    return RestRequestRouter::MR_NO;
}

ScriptOutput
JavascriptPlugin::
runJavascriptScript(MldbEngine * engine,
                    const PluginResource & scriptConfig)
{
    using namespace v8;

    std::unique_ptr<JsPluginContext> itl
        (new JsPluginContext
         ("script runner", engine,
          std::make_shared<LoadedPluginResource>
          (JAVASCRIPT,
           LoadedPluginResource::SCRIPT,
           "", scriptConfig)));

    Utf8String jsFunctionSource = itl->pluginResource->getScript(PackageElement::MAIN);


    v8::Locker locker(itl->isolate.isolate);
    v8::Isolate::Scope isolate(itl->isolate.isolate);

    HandleScope handle_scope(itl->isolate.isolate);
    Context::Scope context_scope(itl->getLocalContext());
        
    v8::Local<v8::Object> globalPrototype
        = itl->getLocalContext()
        ->Global()->GetPrototype().As<v8::Object>();
    JS::check(globalPrototype->Set(itl->getLocalContext(), JS::createString(itl->isolate.isolate, "args"),
                                   JS::toJS(jsonEncode(scriptConfig.args))));

    // Create a string containing the JavaScript source code.
    auto source = JS::createString(itl->isolate.isolate, jsFunctionSource);
    
    // Compile the source code.
    TryCatch trycatch(itl->isolate.isolate);
    trycatch.SetVerbose(true);

    auto origin = JS::createScriptOrigin(itl->isolate.isolate, scriptConfig.address);
    auto script = Script::Compile(itl->getLocalContext(), source, &origin);
    
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
        itl->script.Reset(itl->isolate.isolate, script.ToLocalChecked());
        
        // Run the script to get the result.
        auto scriptResult
            = itl->script.Get(itl->isolate.isolate)->Run(itl->getLocalContext());
            
        if (scriptResult.IsEmpty()) {  
            auto rep = convertException(trycatch, "Running script");
                
            {
                std::unique_lock<std::mutex> guard(itl->logMutex);
                LOG(itl->loader) << jsonEncode(rep) << endl;
            }
                
            result.exception.reset(new ScriptException(std::move(rep)));
        }
        else {
            result.result = JS::fromJS(scriptResult.ToLocalChecked());
                
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


/*****************************************************************************/
/* DOCUMENTATION MACROS                                                      */
/*****************************************************************************/

/** Documentation macro for Javascript functions. */

void jsFunctionDocumentationMacro(MacroContext & context,
                                  const std::string & macroName,
                                  const Utf8String & args)
{
    auto space = args.find(' ');
    if (space == args.end()) {
        throw AnnotatedException(400, "jsfunction macro takes two arguments");
    }

    Utf8String moduleName(args.begin(), space);
    ++space;
    Utf8String functionName(space, args.end());
    
    RestRequest request("GET", "/v1/types/plugins/javascript/routes/modules/" + moduleName.rawString(),
                        {{ "symbol", functionName }}, "");
    auto connection = InProcessRestConnection::create();
    context.engine->handleRequest(*connection, request);
    connection->waitForResponse();
    
    // TODO. better exception message
    if(connection->responseCode() != 200) {
        throw AnnotatedException(400, "responseCode != 200 for GET modules: "
                                 + connection->response());
    }

    auto symbol = jsonDecodeStr<Json::Value>(connection->response());

    context.writeHtml("<h2>");
    context.writeText(functionName);
    context.writeText(" function (Javascript)");
    context.writeHtml("</h2>");
    context.writeMarkdown(symbol["markdown"].asStringUtf8());
}

void jsFunctionsDocumentationMacro(MacroContext & context,
                                 const std::string & macroName,
                                 const Utf8String & args)
{
    RestRequest request("GET", "/v1/types/plugins/javascript/routes/modules/" + args.rawString(),
                        {}, "");
    auto connection = InProcessRestConnection::create();
    context.engine->handleRequest(*connection, request);
    connection->waitForResponse();
    
    // TODO. better exception message
    if(connection->responseCode() != 200) {
        throw AnnotatedException(400, "responseCode != 200 for GET modules: "
                                 + connection->response());
    }

    auto module = jsonDecodeStr<Json::Value>
        (connection->response());

    context.writeHtml("<table class=\"params table\" width='100%'><tr><th align='left'>Export</th><th>Description or value</th></tr>\n");

    for (auto & symbol: module.getMemberNamesUtf8()) {
        const Json::Value & val = module[symbol];
        context.writeHtml("<tr><td>");
        if (val["type"] == "function") {
            context.writeText("function ");
            context.writeHtml("<br><code>");
            context.writeInternalLink("/v1/types/plugins/javascript/routes/modules/" + args.rawString() + "/doc.html?symbol=" + symbol,
                                      symbol,
                                      true /* follow internal redirect */);
            context.writeHtml("</code></br>");
            context.writeHtml("</td><td>");
            context.writeMarkdown(val["summary"].asStringUtf8());
        }
        else if (val["type"] == "value") {
            context.writeText("value " + symbol);
            context.writeHtml("</td><td>");
            context.writeText(val["value"].asStringUtf8());
        }
        context.writeHtml("</td></tr>\n");
    }
    context.writeHtml("</table>");
}

void jsModuleDocumentationMacro(MacroContext & context,
                                const std::string & macroName,
                                const Utf8String & args)
{
    RestRequest request("GET", "/v1/types/plugins/javascript/routes/modules/" + args.rawString() + "/doc.md",
                        {}, "");
    auto connection = InProcessRestConnection::create();
    context.engine->handleRequest(*connection, request);
    connection->waitForResponse();
    
    // TODO. better exception message
    if(connection->responseCode() != 200) {
        throw AnnotatedException(400, "responseCode != 200 for GET modules: "
                                 + connection->response());
    }

    context.writeMarkdown(connection->response());
}

void jsModulesDocumentationMacro(MacroContext & context,
                                 const std::string & macroName,
                                 const Utf8String & args)
{
    RestRequest request("GET", "/v1/types/plugins/javascript/routes/modules",
                        {}, "");
    auto connection = InProcessRestConnection::create();
    context.engine->handleRequest(*connection, request);
    connection->waitForResponse();
    
    // TODO. better exception message
    if(connection->responseCode() != 200) {
        throw AnnotatedException(400, "responseCode != 200 for GET modules: "
                                 + connection->response());
    }

    auto modules = jsonDecodeStr<std::vector<Utf8String> >
        (connection->response());
    
    context.writeHtml("<ul>");
    for (auto & m: modules) {
        context.writeHtml("<li><a href=\"/v1/types/plugins/javascript/routes/modules/" + m + "/doc.html\">");
        context.writeText(m);
        context.writeHtml("</a></li>");
    }
    context.writeHtml("</ul>");
}

namespace {

// Add the ![](jsmodule <moduleName>) macro to document the given module
auto regJsModule = RegisterMacro("jsmodule", jsModuleDocumentationMacro);

// Add the ![](jsfunction <moduleName> <functionName>) macro
auto regJsFunction = RegisterMacro("jsfunction", jsFunctionDocumentationMacro);

// Add the ![](jsfunction <moduleName> <functionName>) macro
auto regJsFunctions = RegisterMacro("jsfunctions", jsFunctionsDocumentationMacro);

// Add the ![](jsfunction <moduleName> <functionName>) macro to list all modules
auto regJsModules = RegisterMacro("jsmodules", jsModulesDocumentationMacro);

} // file scope

} // namespace MLDB

