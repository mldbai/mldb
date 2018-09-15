/** js_plugin.h                                                    -*- C++ -*-
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Plugin loader for Javascript plugins.
*/

#pragma once

#include "mldb/core/plugin.h"
#include "mldb/core/mldb_engine.h"
#include "mldb/builtin/plugin_resource.h"
#include "js_common.h"

using namespace std;


namespace MLDB {


/****************************************************************************/
/* JS PLUGIN                                                                */
/****************************************************************************/

/** MLDB plugin that is implemented in Javascript. */

struct JsPluginContext;
struct ScriptOutput;

struct JavascriptPlugin: public Plugin {
    JavascriptPlugin(MldbEngine * engine,
                     PolyConfig config,
                     std::function<bool (const Json::Value & progress)> onProgress);
    
    ~JavascriptPlugin();
    
    virtual Any getStatus() const;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

    static ScriptOutput
    runJavascriptScript(MldbEngine * engine,
                        const PluginResource & scriptConfig);

    static RestRequestMatchResult
    handleTypeRoute(RestDirectory * engine, RestConnection & conn,
                    const RestRequest & request,
                    RestRequestParsingContext & context);

    std::unique_ptr<JsPluginContext> itl;
};


/*****************************************************************************/
/* JS PLUGIN JS                                                                 */
/*****************************************************************************/

struct JsPluginJS {

    static v8::Local<v8::ObjectTemplate>
    registerMe();

    static JsPluginContext * getShared(const v8::Handle<v8::Object> & val);

    static void
    log(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    setStatusHandler(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    serveStaticFolder(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    serveDocumentationFolder(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    getPluginDir(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    setRequestHandler(const v8::FunctionCallbackInfo<v8::Value> & args);
};


} // namespace MLDB
