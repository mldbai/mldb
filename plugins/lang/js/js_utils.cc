// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* js_utils.cc
   Jeremy Barnes, 21 July 2010
   Copyright (c) 2010 Datacratic.  All rights reserved.

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

namespace ML {

__thread BacktraceInfo * current_backtrace = nullptr;

} // namespace ML

namespace Datacratic {
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
    if (value.IsEmpty())
        throw ML::Exception("no object passed for backtrace injection");

    v8::Handle<v8::Object> obj(v8::Object::Cast(*value));

    if (obj.IsEmpty())
        throw ML::Exception("can't inject backtrace");

    v8::Handle<v8::Value> jsStack = obj->Get(v8::String::NewSymbol("stack"));

    vector<string> jsStackElements = split(cstr(jsStack), '\n');

    // Frames to skip:
    // at [C++] ML::backtrace(int)
    // at [C++] Datacratic::JS::injectBacktrace(v8::Handle<v8::Value>)
    // at [C++] Datacratic::JS::mapException(ML::Exception const&)
    // at [C++] Datacratic::JS::translateCurrentException()
    int num_frames_to_skip = 4;

    vector<ML::BacktraceFrame> backtrace;
    if (current_backtrace && abi::__cxa_current_exception_type()) {
        // Skip:
        backtrace = ML::backtrace(*current_backtrace, num_frames_to_skip);
        delete current_backtrace;
        current_backtrace = 0;
    }
    else backtrace = ML::backtrace(num_frames_to_skip);

    v8::Handle<v8::Array> nativeStack(v8::Array::New(backtrace.size() + jsStackElements.size()));
    int n = 0;
    for (unsigned i = 0;  i < backtrace.size();  ++i, ++n) {
        nativeStack->Set(v8::Uint32::New(n),
                         v8::String::New(("[C++]     at " + backtrace[i].print_for_trace())
                                         .c_str()));
    }
    
    for (int i = jsStackElements.size() - 1;  i >= 0;  --i, ++n) {
        nativeStack->Set(v8::Uint32::New(n),
                         v8::String::New(("[JS]  " + jsStackElements[i]).c_str()));
    }

    obj->Set(v8::String::NewSymbol("backtrace"), nativeStack);

    return obj;
}

v8::Handle<Value>
mapException(const std::exception & exc)
{
    return v8::ThrowException
        (injectBacktrace
         (v8::Exception::Error(v8::String::New((type_name(exc)
                                                + ": " + exc.what()).c_str()))));
}

v8::Handle<Value>
mapException(const ML::Exception & exc)
{
    //cerr << "mapping ML::Exception " << exc.what() << endl;

    return v8::ThrowException
        (injectBacktrace
         (v8::Exception::Error(v8::String::New(exc.what()))));
}

v8::Handle<Value>
mapException(const HttpReturnException & exc)
{
    v8::Handle<v8::Value> error
        = injectBacktrace(v8::Exception::Error(v8::String::New(exc.what())));

    v8::Handle<v8::Object> obj(v8::Object::Cast(*error));

    if (obj.IsEmpty())
        throw ML::Exception("can't inject backtrace");
    
    obj->Set(v8::String::NewSymbol("httpCode"),
             v8::Integer::New(exc.httpCode));
    obj->Set(v8::String::NewSymbol("details"),
             toJS(exc.details.asJson()));
    obj->Set(v8::String::NewSymbol("error"),
             toJS(exc.what()));
    
    return v8::ThrowException(error);
}

v8::Handle<v8::Value>
translateCurrentException()
{
    if (!std::current_exception()) {
        throw ML::Exception("no exception");
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
    catch(const ML::Exception& ex) {
        return mapException(ex);
    }
    catch(const std::exception& ex) {
        return mapException(ex);
    }
    JML_CATCH_ALL {
        std::string msg = "unknown exception type";
        auto error = v8::Exception::Error(v8::String::New(msg.c_str()));
        return v8::ThrowException(injectBacktrace(error));
    }
}

void passJsException(const v8::TryCatch & tc);

ValuePromise getArg(const JSArgs & args, int argnum,
                    const std::string & name)
{
    if (args.Length() <= argnum)
        throw ML::Exception("argument %d (%s) must be present",
                            argnum, name.c_str());

    ValuePromise arg;
    arg.value  = args[argnum];

    if (arg.value->IsUndefined() || arg.value->IsNull())
        throw ML::Exception("argument %d (%s) was %s",
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

/** Convert the given value into a persistent v8 function. */
v8::Persistent<v8::Function>
from_js(const JSValue & val, v8::Persistent<v8::Function> *)
{
    v8::Handle<v8::Function> fn(v8::Function::Cast(*val));
    if (fn.IsEmpty() || !fn->IsFunction()) {
        //cerr << "fn = " << cstr(fn) << endl;
        //cerr << "fn.IsEmpty() = " << fn.IsEmpty() << endl;
        //cerr << "val->IsFunction() = " << val->IsFunction() << endl;
        throw ML::Exception("expected a function; instead we got " + cstr(val));
    }
    
    return v8::Persistent<v8::Function>::New(fn);
}

v8::Local<v8::Function>
from_js(const JSValue & val, v8::Local<v8::Function> *)
{
    v8::Local<v8::Function> fn(v8::Function::Cast(*val));
    if (fn.IsEmpty() || !fn->IsFunction()) {
        //cerr << "fn = " << cstr(fn) << endl;
        //cerr << "fn.IsEmpty() = " << fn.IsEmpty() << endl;
        //cerr << "val->IsFunction() = " << val->IsFunction() << endl;
        throw ML::Exception("expected a function; instead we got " + cstr(val));
    }

    return fn;
}

v8::Handle<v8::Function>
from_js(const JSValue & val, v8::Handle<v8::Function> *)
{
    v8::Handle<v8::Function> fn(v8::Function::Cast(*val));
    if (fn.IsEmpty() || !fn->IsFunction()) {
        //cerr << "fn = " << cstr(fn) << endl;
        //cerr << "fn.IsEmpty() = " << fn.IsEmpty() << endl;
        //cerr << "val->IsFunction() = " << val->IsFunction() << endl;
        throw ML::Exception("expected a function; instead we got " + cstr(val));
    }

    return fn;
}

v8::Handle<v8::Array>
from_js(const JSValue & val, v8::Handle<v8::Array> *)
{
    v8::Handle<v8::Array> arr(v8::Array::Cast(*val));
    if (arr.IsEmpty() || !arr->IsArray())
        throw ML::Exception("expected an array; instead we got " + cstr(val));

    return arr;
}

v8::Handle<v8::Function>
getFunction(const std::string & script_source)
{
    using namespace v8;

    HandleScope scope;
    Handle<String> source = String::New(script_source.c_str());

    TryCatch tc;
    
    // Compile the source code.
    Handle<Script> script = Script::Compile(source);

    if (script.IsEmpty() && tc.HasCaught())
        throw ML::Exception("got exception compiling: "
                            + JS::cstr(tc.Exception()));
    if (script.IsEmpty())
        throw ML::Exception("compilation returned nothing");
    
    // Run the script to get the result (which should be a function)
    Handle<Value> result = script->Run();

    if (result.IsEmpty() && tc.HasCaught())
        throw ML::Exception("got exception compiling: "
                            + JS::cstr(tc.Exception()));
    if (result.IsEmpty())
        throw ML::Exception("compilation returned nothing");
    if (!result->IsFunction())
        throw ML::Exception("result of script isn't a function");
    
    v8::Local<v8::Function> fnresult(v8::Function::Cast(*result));

    return scope.Close(fnresult);
}

} // namespace JS
} // namespace Datacratic
