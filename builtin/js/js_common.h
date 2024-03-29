/** js_common.h                                                    -*- C++ -*-
    Jeremy Barnes, 12 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Common code for JS handling.
*/

#include "v8.h"
#include "js_utils.h"
#include "mldb/types/value_description.h"
#include "mldb/logging/logging.h"
#include "mldb/builtin/script_output.h"
#include <mutex>
#include "mldb/rest/rest_request_router.h"
#include "mldb/types/path.h"

#pragma once


namespace MLDB {

struct RestRequestRouter;
struct CellValue;
struct ExpressionValue;
struct MldbEngine;
struct LoadedPluginResource;
struct FsObjectInfo;

extern Logging::Category mldbJsCategory;

struct JsIsolate {
    JsIsolate()
        : isolate(nullptr)
    {
        // not initialized
    }

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
        if (isolate)
            isolate->Dispose();
    }

    v8::Isolate * isolate = nullptr;
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
    V8Init(MldbEngine * engine);
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

struct JsException: public MLDB::Exception {
    JsException(const ScriptException & exc)
        : MLDB::Exception(exc.where.rawString()), rep(exc)
    {
    }

    ~JsException() throw ()
    {
    }

    ScriptException rep;
};


/*****************************************************************************/
/* JS PLUGIN CONTEXT                                                         */
/*****************************************************************************/

struct JsPluginContext {

    /** Create a JS plugin context.  Note that pluginResource may be
        a null pointer if the context is for a JS function rather than
        an actual plugin.
    */
    JsPluginContext(const Utf8String & pluginName, MldbEngine * engine,
                    std::shared_ptr<LoadedPluginResource> pluginResource);
    ~JsPluginContext();

    JsIsolate isolate;
    v8::Persistent<v8::Context> context;
    v8::Local<v8::Context> getLocalContext() const { return context.Get(isolate.isolate); }
    v8::Persistent<v8::Script> script;

    std::string categoryName, loaderName;
    std::mutex logMutex;  /// protects the categories below
    Logging::Category category;
    Logging::Category loader;

    std::vector<ScriptLogEntry> logs;

    std::function<Json::Value ()> getStatus;
    RestRequestRouter router;
    RestRequestRouter::OnProcessRequest handleRequest;
    MldbEngine * engine;

    std::shared_ptr<LoadedPluginResource> pluginResource;

    // These are the function templates for all of the builtin objects
    v8::Persistent<v8::FunctionTemplate> Plugin;
    v8::Persistent<v8::FunctionTemplate> Mldb;
    v8::Persistent<v8::FunctionTemplate> Stream;
    v8::Persistent<v8::FunctionTemplate> CellValue;
    v8::Persistent<v8::FunctionTemplate> PathElement;
    v8::Persistent<v8::FunctionTemplate> Path;
    v8::Persistent<v8::FunctionTemplate> Dataset;
    v8::Persistent<v8::FunctionTemplate> Function;
    v8::Persistent<v8::FunctionTemplate> Sensor;
    v8::Persistent<v8::FunctionTemplate> Procedure;
    v8::Persistent<v8::FunctionTemplate> RandomNumberGenerator;

    /// This is the actual implemention of the MLDB object
    v8::Persistent<v8::Object> mldb;
    
    /// This implements the require function, which loads other code and
    /// objects.
    static void require(const v8::FunctionCallbackInfo<v8::Value> & args);

    /// List set of available modules for require
    std::vector<Utf8String> knownModules();
    
    /// Return the module object for a given module (cached or by loading
    /// it).
    v8::Handle<v8::Object> getModule(const Utf8String & module);

    /// Return the source code, source filename and source filename object
    /// info for this module
    std::tuple<Utf8String,
               Utf8String,
               FsObjectInfo>
    findModuleSource(const Utf8String & moduleName);
};


/*****************************************************************************/
/* JS CONTEXT SCOPE                                                          */
/*****************************************************************************/

/** Thread-local stack of JS contexts used to allow free functions (like
    type converters) that require the current context to obtain it.
*/

struct JsContextScope {
    JsContextScope(JsPluginContext * context);
    JsContextScope(const v8::Handle<v8::Object> & val);
    ~JsContextScope();

    JsContextScope(const JsContextScope & other) = delete;
    void operator = (const JsContextScope & other) = delete;
    JsContextScope(JsContextScope && other) = delete;
    void operator = (JsContextScope && other) = delete;
    
    static JsPluginContext * current();

private:    
    static void enter(JsPluginContext * context);
    static void exit(JsPluginContext * context);

    JsPluginContext * context;
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

    // Create a wrapper for a given wrapped object of the Wrapper type, which must have a
    // constructor from a shared pointer to the wrapped object.
    template<typename Wrapper, typename Wrapped>
    static v8::Local<v8::Object>
    doCreateWrapper(Wrapped&& wrapped, JsPluginContext * pluginContext,
                    const v8::Persistent<v8::FunctionTemplate> & tmpl)
    {
        ExcAssert(pluginContext);
        v8::Isolate* isolate = pluginContext->isolate.isolate;
        auto context = isolate->GetCurrentContext();
        auto obj = JS::toLocalChecked(JS::toLocalChecked(tmpl.Get(isolate)->GetFunction(context))->NewInstance(context));
        auto * wrapper = new Wrapper(std::forward<Wrapped>(wrapped));
        wrapper->wrap(obj, pluginContext);
        return obj;
    }


    template <class T>
    static inline T * unwrap(const v8::Handle<v8::Object> & handle)
    {
        ExcAssert(!handle.IsEmpty());
        ExcAssertEqual(handle->InternalFieldCount(), 2);
        return static_cast<T*>(v8::Handle<v8::External>::Cast
                               (handle->GetInternalField(0))->Value());
    }

    static JsPluginContext * getContext(const v8::Handle<v8::Object> & val);

    v8::Persistent<v8::Object> js_object_;

    // Underlying C++ object
    std::shared_ptr<void> cpp_object_;

    /** Set up the object by making handle contain an external reference
        to the given object. */
    void wrap(v8::Handle<v8::Object> handle, JsPluginContext * context);

    /** Set this object up to be garbage collected once there are no more
        references to it in the javascript. */
    void registerForGarbageCollection();
    
    static void
    NoConstructor(const v8::FunctionCallbackInfo<v8::Value> & args);

    static v8::Handle<v8::FunctionTemplate>
    CreateFunctionTemplate(const char * name,
                           v8::FunctionCallback constructor = NoConstructor);
    
private:
    // Called back once an object is garbage collected.
    static void
    garbageCollectionCallback(const v8::WeakCallbackInfo<JsObjectBase> & info);
};

namespace JS {

RestParams
from_js(const JSValue & val, const RestParams *);

Json::Value
fromJsForRestParams(const JSValue & val);

} // namespace JS

} // namespace MLDB


