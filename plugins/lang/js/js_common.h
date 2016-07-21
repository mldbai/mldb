/** js_common.h                                                    -*- C++ -*-
    Jeremy Barnes, 12 June 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Common code for JS handling.
*/

#include <v8.h>
#include "mldb/plugins/lang/js/js_utils.h"
#include "mldb/types/value_description.h"
#include "mldb/logging/logging.h"
#include "mldb/server/script_output.h"
#include <mutex>
#include "mldb/rest/rest_request_router.h"
#include "mldb/sql/path.h"

#pragma once


namespace Datacratic {

struct RestRequestRouter;

namespace MLDB {

struct CellValue;
struct ExpressionValue;
struct MldbServer;
struct LoadedPluginResource;

extern Logging::Category mldbJsCategory;

struct JsIsolate {
    JsIsolate(bool forThisThreadOnly)
    {
        init(forThisThreadOnly);
    }

    /** Initialize the isolate.  If forThisThreadOnly is true, then this isolate
        is a special isolate for running functions only in this thread, and
        consequently will be locked to this thread and pushed to the bottom of
        the isolate stack so it's always entered and can thus be switched to
        very efficiently.
    */
    void init(bool forThisThreadOnly);

    ~JsIsolate()
    {
        locker.reset();
        isolate->Dispose();
    }

    v8::Isolate * isolate;
    std::shared_ptr<v8::Locker> locker;

    static JsIsolate * getIsolateForMyThread()
    {
        static __thread JsIsolate * result = 0;

        if (!result) {
            result = new JsIsolate(true);
        }

        return result;
    }
};

struct V8Init {
    V8Init();
};

void to_js(JS::JSValue & value, const CellValue & val);

CellValue from_js(const JS::JSValue & value, CellValue * = 0);

CellValue from_js_ref(const JS::JSValue & value, CellValue * = 0);

void to_js(JS::JSValue & value, const PathElement & val);

PathElement from_js(const JS::JSValue & value, PathElement * = 0);

PathElement from_js_ref(const JS::JSValue & value, PathElement * = 0);

void to_js(JS::JSValue & value, const Path & val);

Path from_js(const JS::JSValue & value, Path * = 0);

Path from_js_ref(const JS::JSValue & value, Path * = 0);

void to_js(JS::JSValue & value, const ExpressionValue & val);

ExpressionValue from_js(const JS::JSValue & value, ExpressionValue * = 0);

ExpressionValue from_js_ref(const JS::JSValue & value, ExpressionValue * = 0);

/** Convert an exception to its representation. */
ScriptException convertException(const v8::TryCatch & trycatch,
                                 const Utf8String & context);

struct JsException: public ML::Exception {
    JsException(const ScriptException & exc)
        : ML::Exception(exc.where.rawString()), rep(exc)
    {
    }

    ~JsException() throw ()
    {
    }

    ScriptException rep;
};


/*****************************************************************************/
/* JS THREAD CONTEXT                                                         */
/*****************************************************************************/

struct JsThreadContext {
    JsThreadContext(JsIsolate & isolate,
                    MldbServer * server,
                    const Utf8String & pluginName);

    JsIsolate & isolate;
    v8::Persistent<v8::Context> context;

    std::string categoryName, loaderName;
    std::mutex logMutex;  /// protects the categories below
    Logging::Category category;
    Logging::Category loader;

    std::vector<ScriptLogEntry> logs;

    MldbServer * server;

    // These are the function templates for all of the builtin objects
    v8::Persistent<v8::FunctionTemplate> Plugin;
    v8::Persistent<v8::FunctionTemplate> Mldb;
    v8::Persistent<v8::FunctionTemplate> Stream;
    v8::Persistent<v8::FunctionTemplate> CellValue;
    v8::Persistent<v8::FunctionTemplate> ExpressionValue;
    v8::Persistent<v8::FunctionTemplate> PathElement;
    v8::Persistent<v8::FunctionTemplate> Path;
    v8::Persistent<v8::FunctionTemplate> Dataset;
    v8::Persistent<v8::FunctionTemplate> Function;
    v8::Persistent<v8::FunctionTemplate> Procedure;
    v8::Persistent<v8::FunctionTemplate> RandomNumberGenerator;
};


/*****************************************************************************/
/* JS PLUGIN CONTEXT                                                         */
/*****************************************************************************/

struct JsPluginContext: public JsIsolate, public JsThreadContext {
    
    /** Create a JS plugin context.  Note that pluginResource may be
        a null pointer if the context is for a JS function rather than
        an actual plugin.
    */
    JsPluginContext(const Utf8String & pluginName, MldbServer * server,
                    std::shared_ptr<LoadedPluginResource> pluginResource);
    ~JsPluginContext();

    using JsThreadContext::isolate;
    v8::Persistent<v8::Script> script;
    std::function<Json::Value ()> getStatus;
    RestRequestRouter router;
    RestRequestRouter::OnProcessRequest handleRequest;

    std::shared_ptr<LoadedPluginResource> pluginResource;

};


/*****************************************************************************/
/* JS CONTEXT SCOPE                                                          */
/*****************************************************************************/

/** Thread-local stack of JS contexts used to allow free functions (like
    type converters) that require the current context to obtain it.
*/

struct JsContextScope {
    JsContextScope(JsThreadContext * context);
    JsContextScope(const v8::Handle<v8::Object> & val);
    ~JsContextScope();

    JsContextScope(const JsContextScope & other) = delete;
    void operator = (const JsContextScope & other) = delete;
    JsContextScope(JsContextScope && other) = delete;
    void operator = (JsContextScope && other) = delete;
    
    static JsThreadContext * current();

private:    
    static void enter(JsThreadContext * context);
    static void exit(JsThreadContext * context);

    JsThreadContext * context;
};


/*****************************************************************************/
/* JS OBJECT BASE                                                            */
/*****************************************************************************/

class JsObjectBase {
public:
    JsObjectBase()
    {
    }
    
    virtual ~JsObjectBase();

    template <class T>
    static inline T * unwrap(const v8::Handle<v8::Object> & handle)
    {
        ExcAssert(!handle.IsEmpty());
        ExcAssertEqual(handle->InternalFieldCount(), 2);
        return static_cast<T*>(v8::Handle<v8::External>::Cast
                               (handle->GetInternalField(0))->Value());
    }

    static JsThreadContext * getContext(const v8::Handle<v8::Object> & val);

    v8::Persistent<v8::Object> js_object_;

    // Underlying C++ object
    std::shared_ptr<void> cpp_object_;

    /** Set up the object by making handle contain an external reference
        to the given object. */
    void wrap(v8::Handle<v8::Object> handle, JsThreadContext * context);

    /** Set this object up to be garbage collected once there are no more
        references to it in the javascript. */
    void registerForGarbageCollection();
    
    static v8::Handle<v8::Value>
    NoConstructor(const v8::Arguments & args);

    static v8::Handle<v8::FunctionTemplate>
    CreateFunctionTemplate(const char * name,
                           v8::InvocationCallback constructor = NoConstructor);
    
private:
    // Called back once an object is garbage collected.
    static void garbageCollectionCallback(v8::Persistent<v8::Value> value, void *data);
};

} // namespace MLDB


namespace JS {

RestParams
from_js(const JSValue & val, const RestParams *);

Json::Value
fromJsForRestParams(const JSValue & val);

} // namespace JS

} // namespace Datacratic
