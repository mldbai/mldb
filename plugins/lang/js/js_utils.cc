// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* js_utils.cc
   Jeremy Barnes, 21 July 2010
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.

   Implementation of Javascript utility functions.
*/

#include "js_utils.h"
#include "js_value.h"
#include <cxxabi.h>
#include "mldb/arch/demangle.h"
#include "mldb/arch/exception_internals.h"
#include "mldb/arch/backtrace.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/compiler/compiler.h"
#include "mldb/http/http_exception.h"

using namespace std;
using namespace v8;
using namespace ML;


namespace MLDB {

__thread BacktraceInfo * current_backtrace = nullptr;

namespace JS {


/*****************************************************************************/
/* UTILITIES                                                                 */
/*****************************************************************************/

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

v8::Handle<v8::Value>
injectBacktrace(v8::Handle<v8::Value> value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    if (value.IsEmpty())
        throw MLDB::Exception("no object passed for backtrace injection");

    auto obj = value.As<v8::Object>();

    if (obj.IsEmpty())
        throw MLDB::Exception("can't inject backtrace");

    v8::Handle<v8::Value> jsStack
        = obj->Get(v8::String::NewFromUtf8(isolate, "stack"));

    vector<string> jsStackElements = split(cstr(jsStack), '\n');

    // Frames to skip:
    // at [C++] ML::backtrace(int)
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
        nativeStack->Set(v8::Uint32::New(isolate, n),
                         v8::String::NewFromUtf8(isolate, ("[C++]     at " + backtrace[i].print_for_trace()).c_str()));
    }
    
    for (int i = jsStackElements.size() - 1;  i >= 0;  --i, ++n) {
        nativeStack->Set(v8::Uint32::New(isolate, n),
                         v8::String::NewFromUtf8(isolate, ("[JS]  " + jsStackElements[i]).c_str()));
    }

    obj->Set(v8::String::NewFromUtf8(isolate, "backtrace"), nativeStack);

    return obj;
}

v8::Handle<Value>
mapException(const std::exception & exc)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    return isolate->ThrowException
        (injectBacktrace
         (v8::Exception::Error(v8::String::NewFromUtf8(isolate, (type_name(exc)
                                                + ": " + exc.what()).c_str()))));
}

v8::Handle<Value>
mapException(const MLDB::Exception & exc)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    //cerr << "mapping MLDB::Exception " << exc.what() << endl;

    return isolate->ThrowException
        (injectBacktrace
         (v8::Exception::Error(v8::String::NewFromUtf8(isolate, exc.what()))));
}

v8::Handle<Value>
mapException(const HttpReturnException & exc)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::Handle<v8::Value> error
        = injectBacktrace(v8::Exception::Error(v8::String::NewFromUtf8(isolate, exc.what())));

    auto obj = error.As<v8::Object>();

    if (obj.IsEmpty())
        throw MLDB::Exception("can't inject backtrace");
    
    obj->Set(v8::String::NewFromUtf8(isolate, "httpCode"),
             v8::Integer::New(isolate, exc.httpCode));
    obj->Set(v8::String::NewFromUtf8(isolate, "details"),
             toJS(exc.details.asJson()));
    obj->Set(v8::String::NewFromUtf8(isolate, "error"),
             toJS(exc.what()));
    
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
    catch (const HttpReturnException & ex) {
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
        auto error = v8::Exception::Error(v8::String::NewFromUtf8(isolate, msg.c_str()));
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
    Handle<String> source = String::NewFromUtf8(isolate, script_source.c_str());

    TryCatch tc;
    
    // Compile the source code.
    Handle<Script> script = Script::Compile(source);

    if (script.IsEmpty() && tc.HasCaught())
        throw MLDB::Exception("got exception compiling: "
                            + JS::cstr(tc.Exception()));
    if (script.IsEmpty())
        throw MLDB::Exception("compilation returned nothing");
    
    // Run the script to get the result (which should be a function)
    Handle<Value> result = script->Run();

    if (result.IsEmpty() && tc.HasCaught())
        throw MLDB::Exception("got exception compiling: "
                            + JS::cstr(tc.Exception()));
    if (result.IsEmpty())
        throw MLDB::Exception("compilation returned nothing");
    if (!result->IsFunction())
        throw MLDB::Exception("result of script isn't a function");
    
    auto fnresult = result.As<v8::Function>();

    return scope.Escape(fnresult);
}

} // namespace JS

} // namespace MLDB
