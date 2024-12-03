/* js_utils.cc
   Jeremy Barnes, 21 July 2010
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Implementation of Javascript utility functions.
*/

#include "js_utils.h"
#include "js_value.h"
#include <cxxabi.h>
#include "mldb/arch/demangle.h"
#include "mldb/arch/exception_internals.h"
#include "mldb/arch/backtrace.h"
#include "mldb/utils/string_functions.h"
#include "mldb/compiler/compiler.h"
#include "mldb/types/annotated_exception.h"
#include "mldb_v8_platform.h"

using namespace std;
using namespace v8;


namespace MLDB {

__thread BacktraceInfo * current_backtrace = nullptr;

namespace JS {

//v8::Local<v8::String> createString(v8::Isolate * isolate, const std::string & ascii);
//toLocalChecked(v8::String::NewFromUtf8(isolate, "stack"))));
//v8::Local<v8::String> createString(v8::Isolate * isolate, const char * ascii);
//v8::Local<v8::String> createString(v8::Isolate * isolate, const UTF8String & str);

/*****************************************************************************/
/* UTILITIES                                                                 */
/*****************************************************************************/

v8::Local<v8::String> createString(v8::Isolate * isolate, const std::string & ascii)
{
    return JS::toLocalChecked(v8::String::NewFromUtf8(isolate, ascii.data(), v8::NewStringType::kNormal, ascii.length()));
}

v8::Local<v8::String> createString(v8::Isolate * isolate, const char * ascii)
{
    return JS::toLocalChecked(v8::String::NewFromUtf8(isolate, ascii, v8::NewStringType::kNormal));
}

v8::Local<v8::String> createString(v8::Isolate * isolate, const Utf8String & str)
{
    return JS::toLocalChecked(v8::String::NewFromUtf8(isolate, str.rawData(), v8::NewStringType::kNormal, str.rawLength()));
}

void set(v8::Isolate * isolate, v8::Local<v8::Object> obj, const char * str, const JSValue & val)
{
    auto context = isolate->GetCurrentContext();
    check(obj->Set(context, createString(isolate, str), val));
}

void setReturnValue(const v8::FunctionCallbackInfo<v8::Value> & args, const JSValue & val)
{
    args.GetReturnValue().Set(val);
}

static inline int toGetKey(v8::Local<v8::Context> context, int key)
{
    return key;
}

static inline v8::Local<v8::Value> toGetKey(v8::Local<v8::Context> context, v8::Local<v8::Value> key)
{
    return key;
}

template<typename T, typename Enable = std::enable_if_t<!std::is_integral_v<T> && !std::is_convertible_v<T, JSValue>>>
v8::Local<v8::String> toGetKey(v8::Local<v8::Context> context, T&& key)
{
    return createString(context->GetIsolate(), key);
}

template<typename Key>
JSValue getImpl(v8::Local<v8::Context> context, const JSObject & obj, Key&& key)
{
    return JS::toLocalChecked(obj->Get(context, toGetKey(context, std::forward<Key>(key))));
}

JSValue get(v8::Local<v8::Context> context, const JSObject & obj, int el)
{
    return getImpl(context, obj, el);
}

JSValue get(v8::Local<v8::Context> context, const JSObject & obj, const std::string & el)
{
    return getImpl(context, obj, el);
}

JSValue get(v8::Local<v8::Context> context, const JSObject & obj, const Utf8String & el)
{
    return getImpl(context, obj, el);
}

JSValue get(v8::Local<v8::Context> context, const JSObject & obj, const char * el)
{
    return getImpl(context, obj, el);
}

JSValue get(v8::Local<v8::Context> context, const JSObject & obj, const v8::Local<v8::String> & el)
{
    return getImpl(context, obj, el);
}

JSValue get(v8::Local<v8::Context> context, const JSObject & obj, const JSValue & el)
{
    return getImpl(context, obj, el);
}

template<typename Key>
JSObject getObjectImpl(v8::Local<v8::Context> context, const JSObject & obj, Key&& key)
{
    return JS::toLocalChecked(obj->Get(context, toGetKey(context, std::forward<Key>(key)))).template As<v8::Object>();
}

JSObject getObject(v8::Local<v8::Context> context, const JSObject & obj, int el)
{
    return getObjectImpl(context, obj, el);
}

JSObject getObject(v8::Local<v8::Context> context, const JSObject & obj, const std::string & el)
{
    return getObjectImpl(context, obj, el);
}

JSObject getObject(v8::Local<v8::Context> context, const JSObject & obj, const Utf8String & el)
{
    return getObjectImpl(context, obj, el);
}

JSObject getObject(v8::Local<v8::Context> context, const JSObject & obj, const char * el)
{
    return getObjectImpl(context, obj, el);
}

JSObject getObject(v8::Local<v8::Context> context, const JSObject & obj, const v8::Local<v8::String> & el)
{
    return getObjectImpl(context, obj, el);
}

JSObject getObject(v8::Local<v8::Context> context, const JSObject & obj, const JSValue & el)
{
    return getObjectImpl(context, obj, el);
}

void addMethod(v8::Isolate * isolate, v8::Local<v8::ObjectTemplate> obj, const char * str, const v8::Local<v8::FunctionTemplate> & val)
{
    obj->Set(isolate, str /*createString(isolate, str)*/, val);
}

#if 0
void addMethod(v8::Isolate * isolate, v8::Local<v8::Object> obj, const char * str, const v8::Local<v8::FunctionTemplate> & val)
{
    auto context = isolate->GetCurrentContext();
    check(obj->Set(context, createString(isolate, str), val));
}
#endif

std::string cstr(const std::string & str)
{
    return str;
}

std::string cstr(const JSValue & val)
{
    return from_js(val, (string *)0);
}

Utf8String utf8str(const JSValue & val)
{
    return from_js(val, (Utf8String *)0);
}

v8::ScriptOrigin createScriptOrigin(v8::Isolate * isolate, const Utf8String & address)
{
#if V8_MAJOR_VERSION <= 11
    return v8::ScriptOrigin(isolate,
                            JS::createString(isolate, address));
#else
    return v8::ScriptOrigin(/* isolate, */
                            JS::createString(isolate, address));
#endif
}

v8::Handle<v8::Value>
injectBacktrace(v8::Handle<v8::Value> value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    if (value.IsEmpty())
        throw MLDB::Exception("no object passed for backtrace injection");

    auto obj = value.As<v8::Object>();

    if (obj.IsEmpty())
        throw MLDB::Exception("can't inject backtrace");

    auto context = isolate->GetCurrentContext();

    v8::Handle<v8::Value> jsStack
        = toLocalChecked(obj->Get(context, createString(isolate, "stack")));

    vector<string> jsStackElements = split(cstr(jsStack), '\n');

    // Frames to skip:
    // at [C++] MLDB::backtrace(int)
    // at [C++] MLDB::JS::injectBacktrace(v8::Handle<v8::Value>)
    // at [C++] MLDB::JS::mapException(MLDB::Exception const&)
    // at [C++] MLDB::JS::translateCurrentException()
    int num_frames_to_skip = 4;

    vector<BacktraceFrame> backtrace;
    if (current_backtrace && abi::__cxa_current_exception_type()) {
        // Skip:
        backtrace = MLDB::backtrace(*current_backtrace, num_frames_to_skip);
        delete current_backtrace;
        current_backtrace = 0;
    }
    else backtrace = MLDB::backtrace(num_frames_to_skip);

    v8::Handle<v8::Array> nativeStack
        (v8::Array::New(isolate, backtrace.size() + jsStackElements.size()));
    int n = 0;
    for (unsigned i = 0;  i < backtrace.size();  ++i, ++n) {
        check(nativeStack->Set(context,
                               v8::Uint32::New(isolate, n),
                               createString(isolate, ("[C++]     at " + backtrace[i].print_for_trace()))));
    }
    
    for (int i = jsStackElements.size() - 1;  i >= 0;  --i, ++n) {
        check(nativeStack->Set(context,
                               v8::Uint32::New(isolate, n),
                               createString(isolate, ("[JS]  " + jsStackElements[i]))));
    }

    check(obj->Set(context, createString(isolate, "backtrace"), nativeStack));

    return obj;
}

v8::Handle<Value>
mapException(const std::exception & exc)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    return isolate->ThrowException
        (injectBacktrace
         (v8::Exception::Error(createString(isolate, (type_name(exc) + ": " + exc.what()).c_str()))));
}

v8::Handle<Value>
mapException(const MLDB::Exception & exc)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    //cerr << "mapping MLDB::Exception " << exc.what() << endl;

    return isolate->ThrowException
        (injectBacktrace
         (v8::Exception::Error(createString(isolate, exc.what()))));
}

v8::Handle<Value>
mapException(const AnnotatedException & exc)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    auto context = isolate->GetCurrentContext();
    v8::Handle<v8::Value> error
        = injectBacktrace(v8::Exception::Error(createString(isolate, exc.what())));

    auto obj = error.As<v8::Object>();

    if (obj.IsEmpty())
        throw MLDB::Exception("can't inject backtrace");
    
    check(obj->Set(context, createString(isolate, "httpCode"),
                   v8::Integer::New(isolate, exc.httpCode)));
    check(obj->Set(context, createString(isolate, "details"),
                   toJS(exc.details.asJson())));
    check(obj->Set(context, createString(isolate, "error"),
                   toJS(exc.what())));
    
    return isolate->ThrowException(error);
}

v8::Handle<v8::Value>
translateCurrentException()
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();

    if (!std::current_exception()) {
        throw MLDB::Exception("no exception");
    }

    try {
        throw;
    }
    catch(const JSPassException&) {
        return v8::Handle<v8::Value>();
    }
    catch (const AnnotatedException & ex) {
        return mapException(ex);
    }
    catch(const MLDB::Exception& ex) {
        return mapException(ex);
    }
    catch(const std::exception& ex) {
        return mapException(ex);
    }
    MLDB_CATCH_ALL {
        std::string msg = "unknown exception type";
        auto error = v8::Exception::Error(createString(isolate, msg.c_str()));
        return isolate->ThrowException(injectBacktrace(error));
    }
}

void passJsException(const v8::TryCatch & tc);

ValuePromise getArg(const JSArgs & args, int argnum,
                    const std::string & name)
{
    if (args.Length() <= argnum)
        throw MLDB::Exception("argument %d (%s) must be present",
                            argnum, name.c_str());

    ValuePromise arg;
    arg.value  = args[argnum];

    if (arg.value->IsUndefined() || arg.value->IsNull())
        throw MLDB::Exception("argument %d (%s) was %s",
                            argnum, name.c_str(), cstr(arg.value).c_str());

    arg.argnum = argnum;
    arg.name   = name;

    return arg;
}


string getArg(const JSArgs & args, int argnum, const string & defvalue,
         const std::string & name)
{

    return getArg<string>(args, argnum, defvalue, name);
}

#if 0
/** Convert the given value into a persistent v8 function. */
v8::Persistent<v8::Function>
from_js(const JSValue & val, v8::Persistent<v8::Function> *)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::Local<v8::Function> fn = val.As<v8::Function>();
    if (fn.IsEmpty() || !fn->IsFunction()) {
        //cerr << "fn = " << cstr(fn) << endl;
        //cerr << "fn.IsEmpty() = " << fn.IsEmpty() << endl;
        //cerr << "val->IsFunction() = " << val->IsFunction() << endl;
        throw MLDB::Exception("expected a function; instead we got " + cstr(val));
    }
    
    return v8::Persistent<v8::Function>(isolate, fn);
}
#endif

v8::Local<v8::Function>
from_js(const JSValue & val, v8::Local<v8::Function> *)
{
    auto fn = val.As<v8::Function>();
    if (fn.IsEmpty() || !fn->IsFunction()) {
        //cerr << "fn = " << cstr(fn) << endl;
        //cerr << "fn.IsEmpty() = " << fn.IsEmpty() << endl;
        //cerr << "val->IsFunction() = " << val->IsFunction() << endl;
        throw MLDB::Exception("expected a function; instead we got " + cstr(val));
    }

    return fn;
}

v8::Handle<v8::Array>
from_js(const JSValue & val, v8::Handle<v8::Array> *)
{
    auto arr = val.As<v8::Array>();
    if (arr.IsEmpty() || !arr->IsArray())
        throw MLDB::Exception("expected an array; instead we got " + cstr(val));

    return arr;
}

v8::Handle<v8::Function>
getFunction(const std::string & script_source)
{
    using namespace v8;
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    EscapableHandleScope scope(isolate);

    auto context = isolate->GetCurrentContext();

    Handle<String> source = createString(isolate, script_source);

    TryCatch tc(isolate);
    
    // Compile the source code.
    Handle<Script> script = toLocalChecked(Script::Compile(context, source));

    if (script.IsEmpty() && tc.HasCaught())
        throw MLDB::Exception("got exception compiling: "
                            + JS::cstr(tc.Exception()));
    if (script.IsEmpty())
        throw MLDB::Exception("compilation returned nothing");
    
    // Run the script to get the result (which should be a function)
    auto result = script->Run(context);

    if (result.IsEmpty() && tc.HasCaught())
        throw MLDB::Exception("got exception compiling: "
                            + JS::cstr(tc.Exception()));
    if (result.IsEmpty())
        throw MLDB::Exception("compilation returned nothing");
    if (!result.ToLocalChecked()->IsFunction())
        throw MLDB::Exception("result of script isn't a function");
    
    auto fnresult = result.ToLocalChecked().As<v8::Function>();

    return scope.Escape(fnresult);
}

} // namespace JS

} // namespace MLDB
