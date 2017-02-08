/* js_utils.h                                                      -*- C++ -*-
   Jeremy Barnes, 21 July 2010
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Utility functions for js.
*/

#pragma once

#include "mldb/ext/v8-cross-build-output/include/v8.h"
#include <string>
#include "mldb/arch/exception.h"
#include "mldb/base/exc_assert.h"
#include "mldb/compiler/compiler.h"
#include "mldb/arch/demangle.h"
#include "mldb/arch/format.h"
#include "js_value.h"
#include <iostream>
#include <set>
#include <unordered_map>
#include <tuple>
#include <array>
#include <type_traits>
#include <functional>
#include <vector>
#include <map>
#include "mldb/types/string.h"


namespace MLDB {

struct HttpReturnException;

template<typename T, size_t I, typename Sz, bool Sf, typename P, class A>
struct compact_vector;

namespace JS {


/*****************************************************************************/
/* UTILITIES                                                                 */
/*****************************************************************************/

std::string cstr(const std::string & str);

std::string cstr(const JSValue & val);

Utf8String utf8str(const JSValue & val);

template<typename T>
std::string cstr(const v8::Handle<T> & str)
{
    return cstr(JSValue(str));
}

struct JSPassException : public std::exception {
//    v8::Persistent<v8::Value> jsException;
//    v8::Persistent<v8::Value> jStackTrace;
//    v8::Persistent<v8::Message> jsMessage;
};

/** Throws a JsPassException object recording the given values. */
void passJsException(const v8::TryCatch & tc);

/** A function that will translate the current exception into a v8::Value
    that represents the exception in Javascript.

    This function should be called from within a catch(...) statement.  It
    uses magic to actually figure out what the exception is and how to
    translate it into a JS exception.  It works for all kinds of exceptions,
    not only those that are known about; unknown exceptions will generate a
    message with the type name of the exception.
*/
v8::Handle<v8::Value> translateCurrentException();

/** Modify the given exception object to include a C++ backtrace. */
v8::Handle<v8::Value> injectBacktrace(v8::Handle<v8::Value> value);

/** Macro to use after a try { block to catch and translate javascript
    exceptions. */
#define HANDLE_JS_EXCEPTIONS(args)                              \
    catch (...) {                                               \
        args.GetReturnValue().Set(MLDB::JS::translateCurrentException()); \
        return;                                                         \
    }

#define HANDLE_JS_EXCEPTIONS_SETTER                             \
    catch (...) {                                               \
        MLDB::JS::translateCurrentException();                  \
        return;                                                 \
    }

/** Convert a MLDB::Exception into a Javascript exception. */
v8::Handle<v8::Value>
mapException(const MLDB::Exception & exc);

/** Convert a std::exception into a Javascript exception. */
v8::Handle<v8::Value>
mapException(const std::exception & exc);

/** Convert the Value handle to an Object handle.  Will throw if it isn't an
    object.
*/
inline v8::Handle<v8::Object>
toObject(v8::Handle<v8::Value> handle)
{
    ExcAssert(!handle.IsEmpty());
    if (!handle->IsObject())
        throw MLDB::Exception("value " + cstr(handle) + " is not an object");
    v8::Handle<v8::Object> object = handle->ToObject();
    if (object.IsEmpty())
        throw MLDB::Exception("value " + cstr(handle) + " is not an object");
    return object;
}

/** Convert the Value handle to an Array handle.  Will throw if it isn't an
    object.
*/
inline v8::Handle<v8::Array>
toArray(v8::Handle<v8::Value> handle)
{
    ExcAssert(!handle.IsEmpty());
    if (!handle->IsArray())
        throw MLDB::Exception("value " + cstr(handle) + " is not an array");
    auto array = handle.As<v8::Array>();
    if (array.IsEmpty())
        throw MLDB::Exception("value " + cstr(handle) + " is not an array");
    return array;
}

template<typename T>
v8::Handle<v8::Value>
toJS(const T & t)
{
    JSValue val;
    to_js(val, t);
    return val;
}

template<typename T>
void to_js(JSValue & val, const std::vector<T> & v)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::EscapableHandleScope scope(isolate);
    v8::Handle<v8::Array> arr(v8::Array::New(isolate, v.size()));
    for (unsigned i = 0;  i < v.size();  ++i)
        arr->Set(v8::Uint32::New(isolate, i), toJS(v[i]));
    val = scope.Escape(arr);
}

template<typename T, std::size_t SIZE>
void to_js(JSValue & val, const std::array<T, SIZE> & v)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::EscapableHandleScope scope(isolate);
    v8::Local<v8::Array> arr(v8::Array::New(isolate, v.size()));
    for (unsigned i = 0;  i < v.size();  ++i)
        arr->Set(v8::Uint32::New(isolate, i), toJS(v[i]));
    val = scope.Escape(arr);
}

template<typename T>
void to_js(JSValue & val, const std::set<T> & s)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::EscapableHandleScope scope(isolate);
    v8::Local<v8::Array> arr(v8::Array::New(s.size()));
    int count = 0;
    for(auto i = s.begin(); i != s.end(); ++i)
    {
        arr->Set(v8::Uint32::New(isolate, count), toJS(*i));
        count++;
    }
    val = scope.Escape(arr);
}

template<typename T>
void to_js(JSValue & val, const std::map<std::string, T> & s)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::EscapableHandleScope scope(isolate);
    v8::Local<v8::Object> obj= v8::Object::New(isolate);
    for(auto i = s.begin(); i != s.end(); ++i)
    {
        obj->Set(v8::String::NewFromUtf8(isolate, i->first.c_str()),
                 toJS(i->second));
    }
    val = scope.Escape(obj);
}

template<typename T>
std::map<std::string, T>
from_js(const JSValue & val, const std::map<std::string, T> * = 0)
{
    if(!val->IsObject()) {
        throw MLDB::Exception("invalid JSValue for map extraction");
    }
    
    std::map<std::string, T> result;

    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::HandleScope scope(isolate);

    auto objPtr = v8::Object::Cast(*val);

    v8::Local<v8::Array> properties = objPtr->GetOwnPropertyNames();

    for(int i=0; i<properties->Length(); ++i)
    {
        v8::Local<v8::Value> key = properties->Get(i);
        v8::Local<v8::Value> val = objPtr->Get(key);
        T val2 = from_js(JSValue(val), (T *)0);
        result[cstr(key)] = val2;
    }

    return result;
}

template<typename T, typename Str>
void to_js(JSValue & val, const std::map<Utf8String, T> & s)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::EscapableHandleScope scope(isolate);
    v8::Local<v8::Object> obj= v8::Object::New(isolate);
    for(auto i = s.begin(); i != s.end(); ++i) {
        obj->Set(v8::String::NewFromUtf8(i->first.rawData(),
                                         i->first.rawLength()),
                 toJS(i->second));
    }
    val = scope.Escape(obj);
}

template<typename T>
std::map<Utf8String, T>
from_js(const JSValue & val, const std::map<Utf8String, T> * = 0)
{
    if(!val->IsObject()) {
        throw MLDB::Exception("invalid JSValue for map extraction");
    }
    
    std::map<Utf8String, T> result;

    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::HandleScope scope(isolate);

    auto objPtr = v8::Object::Cast(*val);

    v8::Local<v8::Array> properties = objPtr->GetOwnPropertyNames();

    for(int i=0; i<properties->Length(); ++i)
    {
        v8::Local<v8::Value> key = properties->Get(i);
        v8::Local<v8::Value> val = objPtr->Get(key);
        T val2 = from_js(JSValue(val), (T *)0);
        result[utf8str(key)] = val2;
    }

    return result;
}

template<typename T>
void to_js(JSValue & val, const std::unordered_map<std::string, T> & s)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::EscapableHandleScope scope(isolate);
    v8::Local<v8::Object> obj= v8::Object::New(isolate);
    for(auto i = s.begin(); i != s.end(); ++i)
    {
        obj->Set(v8::String::NewFromUtf8(i->first.c_str()), toJS(i->second));
    }
    val = scope.Escape(obj);
}

template<typename K, typename V, typename H>
void to_js(JSValue & val, const std::unordered_map<K, V, H> & s)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::EscapableHandleScope scope(isolate);
    v8::Local<v8::Object> obj= v8::Object::New(isolate);
    for(auto i = s.begin(); i != s.end(); ++i)
    {
        obj->Set(toJS(i->first), toJS(i->second));
    }
    val = scope.Escape(obj);
}

template<typename K, typename V, typename H>
std::map<K, V, H>
from_js(const JSValue & val, const std::map<K, V, H> * = 0)
{
    if(!val->IsObject()) {
        throw MLDB::Exception("invalid JSValue for map extraction");
    }
    
    std::map<K, V, H> result;

    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::HandleScope scope(isolate);

    auto objPtr = v8::Object::Cast(*val);

    v8::Local<v8::Array> properties = objPtr->GetOwnPropertyNames();

    for(int i=0; i<properties->Length(); ++i)
    {
        v8::Local<v8::Value> key = properties->Get(i);
        v8::Local<v8::Value> val = objPtr->Get(key);
        K key2 = from_js(JSValue(key), (K *)0);
        V val2 = from_js(JSValue(val), (V *)0);
        result[key2] = val2;
    }

    return result;
}

template<typename T, typename V>
void to_js(JSValue & val, const std::tuple<T, V> & v)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::EscapableHandleScope scope(isolate);
    v8::Local<v8::Array> arr(v8::Array::New(isolate, 2));
    arr->Set(v8::Uint32::New(isolate, 0), toJS(v.template get<0>()));
    arr->Set(v8::Uint32::New(isolate, 1), toJS(v.template get<1>()));
    val = scope.Escape(arr);
}

template<typename T, typename V>
void to_js(JSValue & val, const std::pair<T, V> & v)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::EscapableHandleScope scope(isolate);
    v8::Local<v8::Array> arr(v8::Array::New(isolate, 2));
    arr->Set(v8::Uint32::New(isolate, 0), toJS( v.first  ));
    arr->Set(v8::Uint32::New(isolate, 1), toJS( v.second ));
    val = scope.Escape(arr);
}

template<typename T, typename V>
std::pair<T,V>
from_js(const JSValue & val, const std::pair<T,V> * = 0)
{
    if(!val->IsArray()) {
        throw MLDB::Exception("invalid JSValue for pair extraction");
    }

    auto arrPtr = v8::Array::Cast(*val);
    if(arrPtr->Length() != 2) {
        throw MLDB::Exception("invalid length for pair extraction");
    }

    return std::make_pair(from_js(JSValue(arrPtr->Get(0)),(T *) 0),
            from_js(JSValue(arrPtr->Get(1)),(V *) 0));
}

template<class Tuple, int Arg, int Size>
struct TupleOpsJs {

    static void unpack(v8::Isolate * isolate,
                       v8::Local<v8::Array> & arr,
                       const Tuple & tuple)
    {
        JSValue val;
        to_js(val, std::get<Arg>(tuple));
        arr->Set(v8::Uint32::New(isolate, Arg), val);
        TupleOpsJs<Tuple, Arg + 1, Size>::unpack(isolate, arr, tuple);
    }

    static void pack(v8::Array & array,
                     Tuple & tuple)
    {
        if (Arg >= array.Length()) return;
        auto & el = std::get<Arg>(tuple);
        el = from_js(JSValue(array.Get(Arg)), &el);
        TupleOpsJs<Tuple, Arg + 1, Size>::pack(array, tuple);
    }
};

template<class Tuple, int Size>
struct TupleOpsJs<Tuple, Size, Size> {

    static void unpack(v8::Isolate * isolate,
                       v8::Local<v8::Array> & array,
                       const Tuple & tuple)
    {
    }

    static void pack(v8::Array & array,
                     Tuple & tuple)
    {
    }
};

template<typename... Args>
void to_js(JSValue & val, const std::tuple<Args...> & v)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::EscapableHandleScope scope(isolate);
    v8::Local<v8::Array> arr(v8::Array::New(isolate, sizeof...(Args)));
    TupleOpsJs<std::tuple<Args...>, 0, sizeof...(Args)>
        ::unpack(isolate, arr, v);
    val = scope.Escape(arr);
}

template<typename... Args>
std::tuple<Args...>
from_js(const JSValue & val, const std::tuple<Args...> * v = 0)
{
    if (!val->IsArray())
        throw MLDB::Exception("invalid JSValue for tuple extraction");

    auto arrPtr = v8::Array::Cast(*val);

    std::tuple<Args...> result;
    TupleOpsJs<std::tuple<Args...>, 0, sizeof...(Args)>::pack(*arrPtr, result);
    return result;
}

/** Class to deal with passing JS arguments, either from an Arguments
    structure or from an array of values.
*/
struct JSArgs {
    JSArgs(const v8::FunctionCallbackInfo<v8::Value> & args)
        : This(args.This()), args1(&args), args2(0), argc(args.Length())
    {
    }

    JSArgs(const v8::Handle<v8::Object> & This,
           int argc, const v8::Handle<v8::Value> * argv)
        : This(This), args1(0), args2(argv), argc(argc)
    {
    }

    v8::Handle<v8::Value> operator [] (unsigned index) const
    {
        v8::Isolate* isolate = v8::Isolate::GetCurrent();
        if (index >= argc)
            return v8::Undefined(isolate);

        if (args1) return (*args1)[index];
        else return args2[index];
    }

    unsigned Length() const { return argc; }

    v8::Handle<v8::Object> Holder() const
    {
        if (args1) return args1->Holder();
        return This;
    }

    v8::Handle<v8::Function> Callee() const
    {
        if (args1) return args1->Callee();
        return v8::Handle<v8::Function>();
    }

    v8::Handle<v8::Object> This;
    const v8::FunctionCallbackInfo<v8::Value> * args1;
    const v8::Handle<v8::Value> * args2;
    unsigned argc;
};

/** Convert the given value into a persistent v8 function. */
v8::Persistent<v8::Function>
from_js(const JSValue & val, v8::Persistent<v8::Function> * = 0);

/** Same, but for a local version */
v8::Local<v8::Function>
from_js(const JSValue & val, v8::Local<v8::Function> * = 0);

v8::Handle<v8::Array>
from_js(const JSValue & val, v8::Handle<v8::Array> * = 0);

template<typename T>
std::vector<T>
from_js(const JSValue & val, const std::vector<T> * = 0)
{
    if(!val->IsArray()) {
        throw MLDB::Exception("invalid JSValue for vector extraction");
    }

    std::vector<T> result;
    auto arrPtr = v8::Array::Cast(*val);
    for(int i=0; i<arrPtr->Length(); ++i)
    {
        result.push_back( from_js(JSValue(arrPtr->Get(i)), (T *)0) );
    }
    return result;
}

template<typename T>
std::set<T>
from_js(const JSValue & val, const std::set<T> * = 0)
{
    if(!val->IsArray()) {
        throw MLDB::Exception("invalid JSValue for set extraction");
    }

    std::set<T> result;
    auto arrPtr = v8::Array::Cast(*val);
    for(int i=0; i<arrPtr->Length(); ++i)
    {
        result.insert( from_js(JSValue(arrPtr->Get(i)), (T *)0) );
    }
    return result;
}

template<typename T>
void from_js(const JSValue & jsval, const T * value,
             typename std::enable_if<std::is_same<T, void>::value, void *>::type = 0)
{
}

template<typename T>
void from_js(const JSValue & jsval, T * value,
             typename std::enable_if<std::is_same<T, void>::value, void *>::type = 0)
{
}

template<typename V8Value>
void to_js(JSValue & val, const v8::Local<V8Value> & val2)
{
    val = val2;
}

template<typename V8Value>
void to_js(JSValue & val, const v8::Persistent<V8Value> & val2)
{
    val = val2;
}

template<typename T, size_t I, typename Sz, bool Sf, typename P, class A>
void to_js(JSValue & val, const compact_vector<T, I, Sz, Sf, P, A> & v)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::EscapableHandleScope scope(isolate);

    v8::Local<v8::Array> arr(v8::Array::New(v.size()));
    for (unsigned i = 0;  i < v.size();  ++i)
        arr->Set(v8::Uint32::New(isolate, i), toJS(v[i]));
    val = scope.Escape(arr);
}

//template<typename T>
//void to_js(JSValue & val, T * const &)
//{
//    to_js(val, (T *)0);
//}

//void from_js(JSValue & jsval, const void * = 0);
//void from_js(const JSValue & jsval, void * = 0);

struct ValuePromise {
    ValuePromise()
        : argnum(-1)
    {
    }

    ValuePromise(const JSValue & value)
        : value(value), argnum(-1)
    {
    }

    ValuePromise(const JSValue & value,
                 const std::string & name,
                 int argnum)
        : value(value), name(name), argnum(argnum)
    {
    }

    JSValue value;
    std::string name;
    int argnum;

    template<typename T>
    operator T () const
    {
        try {
            return from_js(this->value, (T *)0);
        } catch (const std::exception & exc) {
            if (argnum == -1)
                throw MLDB::Exception("value \"%s\" could not be "
                                    "converted to a %s: %s",
                                    cstr(this->value).c_str(),
                                    MLDB::type_name<T>().c_str(),
                                    exc.what());
            throw MLDB::Exception("argument %d (%s): value \"%s\" could not be "
                                "converted to a %s: %s",
                                this->argnum, this->name.c_str(),
                                cstr(this->value).c_str(),
                                MLDB::type_name<T>().c_str(),
                                exc.what());
        }
    }

    template<typename T>
    decltype(from_js_ref(*(JSValue *)0, (T *)0)) getRef() const
    {
        try {
            return from_js_ref(this->value, (T *)0);
        } catch (const std::exception & exc) {
            if (argnum == -1)
                throw MLDB::Exception("value \"%s\" could not be "
                                    "converted to a %s: %s",
                                    cstr(this->value).c_str(),
                                    MLDB::type_name<T>().c_str(),
                                    exc.what());
            throw MLDB::Exception("argument %d (%s): value \"%s\" could not be "
                                "converted to a %s: %s",
                                this->argnum, this->name.c_str(),
                                cstr(this->value).c_str(),
                                MLDB::type_name<T>().c_str(),
                                exc.what());
        }
    }
};

ValuePromise getArg(const JSArgs & args, int argnum,
                    const std::string & name);


std::string
getArg(const JSArgs & args, int argnum, const std::string & defvalue,
       const std::string & name);

template<typename T, typename A>
T getArg(const JSArgs & args, int argnum,
         const std::string & name,
         T (*fn) (A))
{
    ValuePromise vp = getArg(args, argnum, name);
    return fn(vp.value);
}

template<typename T>
T getArg(const JSArgs & args, int argnum, const T & defvalue,
         const std::string & name)
{
    if (args.Length() <= argnum)
        return defvalue;

    return getArg(args, argnum, name);
}

template<typename T>
typename std::remove_reference<T>::type
getArg(const JSArgs & args, int argnum, const std::string & name,
       typename std::enable_if<std::is_abstract<typename std::remove_reference<T>::type>::value>::type * = 0)
{
    return getArg(args, argnum, name)
        .operator typename std::remove_reference<T>::type ();
}

template<typename T>
decltype(from_js_ref(*(JSValue *)0, (T *)0))
getArg(const JSArgs & args, int argnum, const std::string & name,
       typename std::enable_if<!std::is_abstract<typename std::remove_reference<T>::type>::value>::type * = 0)
{
    return getArg(args, argnum, name).getRef<T>();
}

inline ValuePromise
fromJS(const v8::Handle<v8::Value> & value)
{
    return ValuePromise(value);
}

/** Turn a string containing Javascript source into a function.  The source
    file has to be of the following form:

    fn = getFunction("function f1(val) { return val; };  f1;");

    Note the extra "f1" at the end, which provides the return value of the
    script (which is copied into the function).

    An exception will be thrown on any error; on success the returned
    handle is guaranteed to be a valid handle to a function.

    Note that this function can only run within a v8 context.
*/
v8::Handle<v8::Function>
getFunction(const std::string & script_source);

/** Turn a string containing Javascript source into a function.  The source
    file has to be of the following form:

    fn = getFunction("function f1(val) { return val; };  f1;");

    Note the extra "f1" at the end, which provides the return value of the
    script (which is copied into the function).

    An exception will be thrown on any error; on success the returned
    handle is guaranteed to be a valid handle to a function.

    Note that this function can only run within a v8 context.

    The given global object is used.
*/
v8::Handle<v8::Function>
getFunction(const std::string & script_source,
            v8::Handle<v8::Object> global);


// We make from_js_ref make a temporary copy of anything that's just a POD
// field; anything else needs to a) have a from_js_ref specialization, or
// b) be extractable as a pointer to avoid copying
template<typename T>
T
from_js_ref(const JSValue & val, T *,
            typename std::enable_if<std::is_pod<T>::value>::type * = 0)
{
    return from_js(val, (T*)0);
}

// Allow std::string to be passed by value as well

inline std::string
from_js_ref(const JSValue & val, std::string *)
{
    return from_js(val, (std::string *)0);
}

inline Utf8String
from_js_ref(const JSValue & val, Utf8String *)
{
    return from_js(val, (Utf8String *)0);
}

// And vectors

template<typename T>
inline std::vector<T>
from_js_ref(const JSValue & val, std::vector<T> *)
{
    return from_js(val, (std::vector<T> *)0);
}

// And maps

template<typename K, typename V, class C, class A>
inline std::map<K, V, C, A>
from_js_ref(const JSValue & val, std::map<K, V, C, A> *)
{
    return from_js(val, (std::map<K, V, C, A> *)0);
}

// Anything else we require that we can extract a pointer to a real object
// to avoid copying complex objects; this can be overridden by defining a
// function like for std::string above
template<typename T>
const T &
from_js_ref(const JSValue & val, T *,
            typename std::enable_if<!std::is_pod<T>::value>::type * = 0)
{
    return *from_js(val, (T**)0);
}

} // namespace JS

} // namespace MLDB
