/* js_value.cc
   Jeremy Barnes, 21 July 2010
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Javascript value handling.
*/

#include "js_value.h"
#include "js_utils.h"
#include "mldb/utils/string_functions.h"
#include "mldb/arch/demangle.h"
#include "mldb/arch/backtrace.h"
#include "mldb/types/date.h"
#include "mldb/types/string.h"
#include <boost/lexical_cast.hpp>
#include <boost/foreach.hpp>
#include "mldb/ext/jsoncpp/json.h"
#include "mldb/types/basic_value_descriptions.h"

#include <cxxabi.h>
using namespace std;

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
    auto context = isolate->GetCurrentContext();
    check((*this)->Set(context,
                       createString(isolate, key),
                       createString(isolate, value)));
}

void
JSObject::
add(const std::string & key, const JSValue & value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    auto context = isolate->GetCurrentContext();
    check((*this)->Set(context, createString(isolate, key),
                 value));
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
        jsval = createString(isolate, value);
    else {
        // Assume utf-8
        jsval = createString(isolate, Utf8String(value));
    }
}

void to_js(JSValue & jsval, const Utf8String & value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    jsval = createString(isolate, value);
}

void to_js(JSValue & jsval, const char * value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    jsval = createString(isolate, value);
}

void to_js(JSValue & jsval, const Json::Value & value)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    auto context = isolate->GetCurrentContext();
    switch(value.type()) {
    case Json::objectValue:
        {
            v8::EscapableHandleScope scope(isolate);
            v8::Local<v8::Object> obj = v8::Object::New(isolate);
            BOOST_FOREACH(string key, value.getMemberNames()) {
                JSValue member;
                to_js(member, value[key]);
                check(obj->Set(context, createString(isolate, key),
                         member));
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
                    check(arr->Set(context, i, elem));
                }
            jsval = scope.Escape(arr);
        }
        break;
    case Json::realValue:
        to_js(jsval, value.asDouble());
        break;
    case Json::stringValue:
        to_js(jsval, value.asStringUtf8());
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
    auto context = isolate->GetCurrentContext();
    jsval = v8::Date::New(context, value.secondsSinceEpoch() * 1000.0);
}

namespace {

int64_t check_to_int2(const JSValue & val)
{
    //cerr << "check_to_int " << cstr(val) << endl;

    auto context = v8::Isolate::GetCurrent()->GetCurrentContext();

    v8::Maybe<int64_t> ival = val->IntegerValue(context);
    v8::Maybe<double> dval = val->NumberValue(context);

    if (ival.IsJust() && check(ival) == check(dval)) return check(ival);

    if (check(dval) > (double)std::numeric_limits<uint64_t>::max()
        || check(dval) < (double)std::numeric_limits<uint64_t>::min())
        throw MLDB::Exception("Cannot fit " + cstr(val) + " into an integer");
        
    if (val->IsArray())
        throw Exception("cannot convert array to integer");

    //bool debug = val->IsArray();
    //if (debug) cerr << "is array" << endl;

    if (val->IsNumber() || v8::Number::Cast(*val)) {
        //if (debug)
        //cerr << "is number" << endl;
        double d = check(val->NumberValue(context));
        if (!isfinite(d))
            throw Exception("cannot convert double value "
                            + cstr(val) + " to integer");
        return d;
    }
    if (val->IsString()) {
        //if (debug)
        //cerr << "is string" << endl;

        string s = MLDB::lowercase(cstr(val));
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
        cerr << "object: " << cstr(toLocalChecked(toLocalChecked(val->ToObject(context))->ObjectProtoToString(context)))
             << endl;
        cerr << "val->NumberValue(context) = "
             << (val->NumberValue(context).IsJust() ? std::to_string(check(val->NumberValue(context))) : "NULL")
             << endl;
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
    auto context = v8::Isolate::GetCurrent()->GetCurrentContext();
    auto result = val->NumberValue(context);
    if (result.IsNothing()) {
        if (val->IsString()) {
            string s = MLDB::lowercase(cstr(val));
            if (s == "nan")
                return NAN;
            else if (s == "-nan")
                return -NAN;
            throw MLDB::Exception("string value \"%s\" is not converible to "
                                "floating point",
                                s.c_str());
        }
        throw Exception("value \"%s\" not convertible to floating point",
                        cstr(val).c_str());
    }
    return check(result);
}

// Small structure to pass an isolate or context, to paper over some of
// the evolution in the v8 API.
struct IsolateOrContext {
    v8::Isolate * isolate = nullptr;
    operator v8::Isolate * () const { return isolate; }
    operator v8::Local<v8::Context> () const { return isolate->GetCurrentContext(); }
};

IsolateOrContext ioc(v8::Isolate * isolate)
{
    return { isolate };
}

bool from_js(const JSValue & val, bool *)
{
    auto isolate = v8::Isolate::GetCurrent();
    bool result = check(val->BooleanValue(ioc(isolate)));
    return result;
}

std::string from_js(const JSValue & val, std::string *)
{
    auto isolate = v8::Isolate::GetCurrent();
    v8::String::Utf8Value utf8Str(isolate, val);
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
            v8::Isolate* isolate = v8::Isolate::GetCurrent();
            auto context = isolate->GetCurrentContext();

            for(int i=0; i<arrPtr->Length(); ++i)
            {
                result[i] = from_js(arrPtr->Get(context, i), (Json::Value *)0);
            }

            return result;
        }
        else
        {
            v8::Isolate* isolate = v8::Isolate::GetCurrent();
            auto context = isolate->GetCurrentContext();
            v8::HandleScope handleScope(isolate);
            Json::Value result (Json::objectValue);
            auto objPtr = v8::Object::Cast(*val);
            v8::Handle<v8::Array> prop_names = toLocalChecked(objPtr->GetPropertyNames(context));

            for (unsigned i = 0;  i < prop_names->Length();  ++i)
            {
                v8::Handle<v8::String> key
                    = toLocalChecked(toLocalChecked(prop_names->Get(context, v8::Uint32::New(isolate, i)))->ToString(context));
                if (!check(objPtr->HasOwnProperty(context, key))) continue;
                result[from_js(key, (Utf8String *)0)] =
                        from_js(objPtr->Get(context, key), (Json::Value *)0);
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
    if(v8::Date::Cast(*val)->IsDate()) {
        v8::Isolate* isolate = v8::Isolate::GetCurrent();
        v8::HandleScope scope(isolate);
        auto context = isolate->GetCurrentContext();
        return Date::fromSecondsSinceEpoch(check(v8::Date::Cast(*val)->NumberValue(context))
                                           / 1000.0);
    }
    if (val->IsString()) {
        return jsonDecode<Date>(cstr(val));
    }
    throw MLDB::Exception("Couldn't convert from " + cstr(val) + " to MLDB::Date");
}

Utf8String from_js(const JSValue & val, Utf8String *)
{
    auto isolate = v8::Isolate::GetCurrent();
    v8::String::Utf8Value valStr(isolate, val);
    return Utf8String(*valStr, valStr.length(), false /* check */) ;
}

Json::Value from_js_ref(const JSValue & val, Json::Value *)
{
    return from_js(val, (Json::Value *)0);
}


} // namespace JS

} // namespace MLDB
