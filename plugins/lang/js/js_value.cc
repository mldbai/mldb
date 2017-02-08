// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* js_value.cc
   Jeremy Barnes, 21 July 2010
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.

   Javascript value handling.
*/

#include "js_value.h"
#include "js_utils.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/arch/demangle.h"
#include "mldb/arch/backtrace.h"
#include "mldb/types/date.h"
#include "mldb/types/string.h"
#include <boost/lexical_cast.hpp>
#include <boost/foreach.hpp>
#include "mldb/ext/jsoncpp/json.h"

#include <cxxabi.h>
using namespace std;
using namespace ML;

namespace MLDB {

namespace JS {


/*****************************************************************************/
/* JSVALUE                                                                   */
/*****************************************************************************/

JSValue::operator v8::Handle<v8::Object>() const
{
    return toObject(*this);
}


/*****************************************************************************/
/* JSOBJECT                                                                  */
/*****************************************************************************/

void
JSObject::
initialize()
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    *this = v8::Object::New(isolate);
}

void
JSObject::
add(const std::string & key, const std::string & value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    (*this)->Set(v8::String::NewFromUtf8(isolate, key.c_str()),
                 v8::String::NewFromUtf8(isolate, value.c_str()));
}

void
JSObject::
add(const std::string & key, const JSValue & value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    (*this)->Set(v8::String::NewFromUtf8(isolate, key.c_str()),
                 value);
}


/*****************************************************************************/
/* CONVERSIONS                                                               */
/*****************************************************************************/

void to_js(JSValue & jsval, signed int value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    jsval = v8::Integer::New(isolate, value);
}

void to_js(JSValue & jsval, unsigned int value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    jsval = v8::Integer::NewFromUnsigned(isolate, value);
}

void to_js(JSValue & jsval, signed long value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    if (value <= INT_MAX && value >= INT_MIN)
        jsval = v8::Integer::New(isolate, value);
    else jsval = v8::Number::New(isolate, (double) value);
}

void to_js(JSValue & jsval, unsigned long value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    if (value <= UINT_MAX)
        jsval = v8::Integer::NewFromUnsigned(isolate, value);
    else jsval = v8::Number::New(isolate, (double) value);
}

void to_js(JSValue & jsval, signed long long value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    if (value <= INT_MAX && value >= INT_MIN)
        jsval = v8::Integer::New(isolate, value);
    else jsval = v8::Number::New(isolate, (double) value);
}

void to_js(JSValue & jsval, unsigned long long value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    if (value <= UINT_MAX)
        jsval = v8::Integer::NewFromUnsigned(isolate, value);
    else jsval = v8::Number::New(isolate, (double) value);
}

void to_js(JSValue & jsval, float value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    jsval = v8::Number::New(isolate, value);
}

void to_js(JSValue & jsval, double value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    jsval = v8::Number::New(isolate, value);
}

void to_js_bool(JSValue & jsval, bool value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    jsval = v8::Boolean::New(isolate, value);
}

void to_js(JSValue & jsval, const std::string & value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    bool isAscii = true;
    for (unsigned i = 0;  i < value.size() && isAscii;  ++i)
        if (value[i] == 0 || value[i] > 127)
            isAscii = false;
    if (isAscii)
        jsval = v8::String::NewFromUtf8(isolate, value.c_str());
    else {
        // Assume utf-8
        jsval = v8::String::NewFromUtf8(isolate, value.c_str());
    }
}

void to_js(JSValue & jsval, const Utf8String & value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    jsval = v8::String::NewFromUtf8(isolate, value.rawData());
}

void to_js(JSValue & jsval, const char * value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    jsval = v8::String::NewFromUtf8(isolate, value);
}

void to_js(JSValue & jsval, const Json::Value & value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    switch(value.type()) {
    case Json::objectValue:
        {
            v8::EscapableHandleScope scope(isolate);
            v8::Local<v8::Object> obj = v8::Object::New(isolate);
            BOOST_FOREACH(string key, value.getMemberNames()) {
                JSValue member;
                to_js(member, value[key]);
                obj->Set(v8::String::NewFromUtf8(isolate, key.c_str()),
                         member);
            }
            jsval = scope.Escape(obj);
        }
        break;
    case Json::arrayValue:
        {
            v8::EscapableHandleScope scope(isolate);
            v8::Local<v8::Array> arr = v8::Array::New(isolate);
            for(int i=0;i< value.size(); ++i)
                {
                    JSValue elem;
                    to_js(elem, value[i]);
                    arr->Set(i, elem);
                }
            jsval = scope.Escape(arr);
        }
        break;
    case Json::realValue:
        to_js(jsval, value.asDouble());
        break;
    case Json::stringValue:
        to_js(jsval, value.asString());
        break;
    case Json::intValue:
        to_js(jsval, value.asInt());
        break;
    case Json::uintValue:
        to_js(jsval, value.asUInt());
        break;
    case Json::booleanValue:
        to_js(jsval, value.asBool());
        break;
    case Json::nullValue:
        jsval = v8::Null(isolate);
        break;
    default:
        throw MLDB::Exception("Can't convert from JsonCpp to JSValue");
        break;
    }
}

void to_js(JSValue & jsval, Date value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    jsval = v8::Date::New(isolate, value.secondsSinceEpoch() * 1000.0);
}

namespace {

int64_t check_to_int2(const JSValue & val)
{
    //cerr << "check_to_int " << cstr(val) << endl;

    int64_t ival = val->IntegerValue();
    double dval = val->NumberValue();

    //cerr << "  ival = " << ival << endl;
    //cerr << "  dval = " << dval << endl;

    if (ival != 0 && ival == dval) return ival;

    if (dval > std::numeric_limits<uint64_t>::max()
        || dval < std::numeric_limits<uint64_t>::min())
        throw MLDB::Exception("Cannot fit " + cstr(val) + " into an integer");
        
    v8::Local<v8::Number> num;

    if (val->IsArray())
        throw Exception("cannot convert array to integer");

    //bool debug = val->IsArray();
    //if (debug) cerr << "is array" << endl;

    if (val->IsNumber() || v8::Number::Cast(*val)) {
        //if (debug)
        //cerr << "is number" << endl;
        double d = val->NumberValue();
        if (!isfinite(d))
            throw Exception("cannot convert double value "
                            + cstr(val) + " to integer");
        return d;
    }
    if (val->IsString()) {
        //if (debug)
        //cerr << "is string" << endl;

        int64_t ival = val->IntegerValue();
        if (ival != 0) return ival;
        string s = ML::lowercase(cstr(val));
        try {
            return boost::lexical_cast<int64_t>(s);
        } catch (const boost::bad_lexical_cast & error) {
            throw Exception("cannot convert string value \""
                            + cstr(val) + "\" (\"" + s + "\") to integer");
        }
    }

#define try_type(x) if (val->x()) cerr << #x << endl;

    try_type(IsUndefined);
    try_type(IsNull);
    try_type(IsTrue);
    try_type(IsFalse);
    try_type(IsString);
    try_type(IsFunction);
    try_type(IsArray);
    try_type(IsObject);
    try_type(IsBoolean);
    try_type(IsNumber);
    try_type(IsExternal);
    try_type(IsInt32);
    try_type(IsDate);

    if (val->IsObject()) {
        cerr << "object: " << cstr(val->ToObject()->ObjectProtoToString())
             << endl;
        cerr << "val->NumberValue() = " << val->NumberValue() << endl;
    }

    backtrace();

    throw Exception("cannot convert value \""
                    + cstr(val) + "\" to integer");
}

template<typename T>
T check_to_int(const JSValue & val)
{
    if (val.IsEmpty())
        throw Exception("from_js: value is empty");

    

    int64_t result1 = check_to_int2(val);
    T result2 = result1;
    if (result1 != result2)
        throw Exception("value " + cstr(val) + " does not fit in type "
                        + MLDB::type_name<T>());
    return result2;
}

} // file scope

signed int from_js(const JSValue & val, signed int *)
{
    //cerr << "from_js signed int" << endl;
    return check_to_int<signed int>(val);
}

unsigned int from_js(const JSValue & val, unsigned *)
{
    //cerr << "from_js unsigned int" << endl;
    return check_to_int<unsigned int>(val);
}

signed long from_js(const JSValue & val, signed long *)
{
    //cerr << "from_js signed long" << endl;
    return check_to_int<signed long>(val);
}

unsigned long from_js(const JSValue & val, unsigned long *)
{
    //cerr << "from_js unsigned long" << endl;
    return check_to_int<unsigned long>(val);
}

signed long long from_js(const JSValue & val, signed long long *)
{
    //cerr << "from_js signed long" << endl;
    return check_to_int<signed long long>(val);
}

unsigned long long from_js(const JSValue & val, unsigned long long *)
{
    //cerr << "from_js unsigned long" << endl;
    return check_to_int<unsigned long long>(val);
}

float from_js(const JSValue & val, float *)
{
    //cerr << "from_js float" << endl;
    return from_js(val, (double *)0);
}

double from_js(const JSValue & val, double *)
{
    //cerr << "from_js double" << endl;
    const double result = val->NumberValue();
    if (std::isnan(result)) {
        if (val->IsNumber()) return result;
        if (val->IsString()) {
            string s = ML::lowercase(cstr(val));
            if (s == "nan" || s == "-nan")
                return result;
            throw MLDB::Exception("string value \"%s\" is not converible to "
                                "floating point",
                                s.c_str());
        }
        throw Exception("value \"%s\" not convertible to floating point",
                        cstr(val).c_str());
    }
    return result;
}

bool from_js(const JSValue & val, bool *)
{
    bool result = val->BooleanValue();
    return result;
}

std::string from_js(const JSValue & val, std::string *)
{
    v8::String::Utf8Value utf8Str(val);
    return std::string(*utf8Str, *utf8Str + utf8Str.length());
}

Json::Value from_js(const JSValue & val, Json::Value *)
{
    if (val.IsEmpty())
        throw MLDB::Exception("empty val");

    //cerr << cstr(val) << endl;

    if(val->IsObject())
    {
        if(v8::Date::Cast(*val)->IsDate())
        {
            return from_js(val, (Date*)(0)).secondsSinceEpoch();
        }
        if(val->IsArray())
        {
            Json::Value result (Json::arrayValue);

            auto arrPtr = v8::Array::Cast(*val);
            for(int i=0; i<arrPtr->Length(); ++i)
            {
                result[i] = from_js(arrPtr->Get(i), (Json::Value *)0);
            }

            return result;
        }
        else
        {
            v8::Isolate* isolate = v8::Isolate::GetCurrent();
            v8::HandleScope handleScope(isolate);
            Json::Value result (Json::objectValue);
            auto objPtr = v8::Object::Cast(*val);
            v8::Handle<v8::Array> prop_names = objPtr->GetPropertyNames();

            for (unsigned i = 0;  i < prop_names->Length();  ++i)
            {
                v8::Handle<v8::String> key
                    = prop_names->Get(v8::Uint32::New(isolate, i))->ToString();
                if (!objPtr->HasOwnProperty(key)) continue;
                result[from_js(key, (Utf8String *)0)] =
                        from_js(objPtr->Get(key), (Json::Value *)0);
            }

            return result;
        }
    }
    if(val->IsBoolean())
    {
        return from_js(val, (bool *)0);
    }
    if(val->IsString())
    {
       	return from_js(val, (Utf8String *)0);
    }
    if(val->IsInt32())
    {
        return from_js(val, (int32_t *)0);
    }
    if(val->IsUint32())
    {
        return from_js(val, (uint32_t *)0);
    }
    if(val->IsNumber())
    {
        return from_js(val, (double *)0);
    }
    if (val->IsNull() || val->IsUndefined())
        return Json::Value();
    throw MLDB::Exception("can't convert from JSValue %s to Json::Value",
                        cstr(val).c_str());
}

Date from_js(const JSValue & val, Date *)
{
    if(!v8::Date::Cast(*val)->IsDate())
        throw MLDB::Exception("Couldn't convert from " + cstr(val) + " to MLDB::Date");
    return Date::fromSecondsSinceEpoch(v8::Date::Cast(*val)->NumberValue()
                                       / 1000.0);
}

Utf8String from_js(const JSValue & val, Utf8String *)
{
    v8::String::Utf8Value valStr(val);
    return Utf8String(*valStr, valStr.length(), false /* check */) ;
}

Json::Value from_js_ref(const JSValue & val, Json::Value *)
{
    return from_js(val, (Json::Value *)0);
}


} // namespace JS

} // namespace MLDB
