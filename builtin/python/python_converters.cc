/** python_converters.cc
    Jeremy Barnes, 7 December 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "python_converters.h"
#include "python_interpreter.h"
#include "from_python_converter.h"
#include "mldb_python_converters.h"
#include "Python.h"
#include "datetime.h"
#include <iostream>
#include "nanobind/stl/string.h"

using namespace std;

namespace MLDB {

namespace Python {

#if 0
/******************************************************************************/
/* DATE TO DATE TIME                                                          */
/******************************************************************************/

PyObject* 
DateToPython::
convert(const Date& date)
{
    tm time = date.toTm();
    // no incref required because returning it transfers ownership
    return PyDateTime_FromDateAndTime(
                                      time.tm_year + 1900,
                                      time.tm_mon + 1,
                                      time.tm_mday,
                                      time.tm_hour,
                                      time.tm_min,
                                      time.tm_sec,
                                      0.
                                      );
}


/******************************************************************************/
/* DATE FROM PYTHON                                                           */
/******************************************************************************/

void*
DateFromPython::
convertible(PyObject* obj_ptr)
{
    //ExcAssert(&PyDateTime_DateTimeType);

    if (!obj_ptr)
        return nullptr;
    if (!PyDateTime_Check(obj_ptr) && !PyFloat_Check(obj_ptr) && 
        !PyLong_Check(obj_ptr) && !PyUnicode_Check(obj_ptr)) {
        return nullptr;
    }
    return obj_ptr;
}

void
DateFromPython::
construct(PyObject* obj_ptr, void* storage)
{
    if (PyFloat_Check(obj_ptr)) {
        Date x = Date::fromSecondsSinceEpoch(PyFloat_AS_DOUBLE(obj_ptr));
        new (storage) Date(x);
    } else if (PyLong_Check(obj_ptr)) {
        Date x = Date::fromSecondsSinceEpoch(PyLong_AS_LONG(obj_ptr));
        new (storage) Date(x);
    } else if (PyUnicode_Check(obj_ptr)) {
        std::string str = nanobind::cast<std::string>(obj_ptr)();
        Date x = Date::parseIso8601DateTime(str);
        new (storage) Date(x);
    } else {
        new (storage) Date(
                           PyDateTime_GET_YEAR(obj_ptr),
                           PyDateTime_GET_MONTH(obj_ptr),
                           PyDateTime_GET_DAY(obj_ptr),
                           PyDateTime_DATE_GET_HOUR(obj_ptr),
                           PyDateTime_DATE_GET_MINUTE(obj_ptr),
                           PyDateTime_DATE_GET_SECOND(obj_ptr),
                           static_cast<double>(
                                               PyDateTime_DATE_GET_MICROSECOND(obj_ptr)) / 1000000.);
    }
}


/******************************************************************************/
/* DATE TO DOUBLE                                                             */
/******************************************************************************/

PyObject*
DateToPyFloat::
convert(const Date& date)
{
    return PyFloat_FromDouble(date.secondsSinceEpoch());
}


/******************************************************************************/
/* LONG FROM PYTHON                                                           */
/******************************************************************************/

void*
StringFromPyUnicode::
convertible(PyObject* obj_ptr)
{
    if (!PyUnicode_Check(obj_ptr)){
        return 0;
    }
    return obj_ptr;
}

void
StringFromPyUnicode::
construct(PyObject* obj_ptr, void* storage)
{
    PyObject* from_unicode;
    if (PyUnicode_Check(obj_ptr)){
        from_unicode = PyUnicode_AsUTF8String(obj_ptr);
    }
    else {
        from_unicode = obj_ptr;
    }
    ExcCheck(from_unicode!=nullptr, "Error converting unicode to ASCII");

    new (storage) std::string(
                              nanobind::cast<std::string>(from_unicode)());
    Py_XDECREF(from_unicode);
}

void*
Utf8StringPyConverter::
convertible(PyObject* obj_ptr)
{
    if (!PyUnicode_Check(obj_ptr)){
        return 0;
    }
    return obj_ptr;
}

void
Utf8StringPyConverter::
construct(PyObject* obj_ptr, void* storage)
{
    PyObject* from_unicode;
    if (PyUnicode_Check(obj_ptr)){
        from_unicode = PyUnicode_AsUTF8String(obj_ptr);
    }
    else {
        from_unicode = obj_ptr;
    }
    ExcCheck(from_unicode!=nullptr, "Error converting unicode to ASCII");

    new (storage) Utf8String(
                             nanobind::cast<std::string>(from_unicode)());
    Py_XDECREF(from_unicode);
}

PyObject*
Utf8StringPyConverter::
convert(const Utf8String & str)
{
    return PyUnicode_FromStringAndSize(str.rawData(), str.rawLength());
}
#endif

/******************************************************************************/
/* Json::Value CONVERTER                                                      */
/******************************************************************************/

#if 0
Json::Value
JsonValueConverter::
construct_recur(PyObject * pyObj)
{
    Json::Value val;
    if (PyBool_Check(pyObj)) {
        bool b = nanobind::cast<bool>(pyObj);
        val = b;
    }
    else if(PyLong_Check(pyObj)) {
        int64_t i = nanobind::cast<int64_t>(pyObj);
        val = i;
    }
    else if(PyFloat_Check(pyObj)) {
        float flt = nanobind::cast<float>(pyObj);
        val = flt;
    }
    else if(PyUnicode_Check(pyObj)) {
        // https://docs.python.org/2/c-api/unicode.html#c.PyUnicode_AsUTF8String
        PyObject* from_unicode = PyUnicode_AsUTF8String(pyObj);
        if(!from_unicode) {
            PyObject* str_obj = PyObject_Str(pyObj);
            std::string str_rep = "<Unable to create str esentation of object>";
            if(str_obj) {
                str_rep = nanobind::cast<std::string>(str_obj);
            }
            // not returned so needs to be garbage collected
            Py_DECREF(str_obj);

            throw MLDB::Exception("Unable to encode unicode to UTF-8"
                                  "Str representation: "+str_rep);
        }
        else {
            std::string str = nanobind::cast<std::string>(from_unicode);
            val = Utf8String(str);

            // not returned so needs to be garbage collected
            Py_DECREF(from_unicode);
        }
    }
    //else if(PyDateTime_Check(pyObj)) {
    //    throw MLDB::Exception("do datetime!!");
    //}
    else if(PyTuple_Check(pyObj)) {
        val = construct_lst<nanobind::tuple>(pyObj);
    }
    else if(PyList_Check(pyObj)) {
        val = construct_lst<nanobind::list>(pyObj);
    }
    else if(PyDict_Check(pyObj)) {
        val = Json::objectValue;
        PyDict pyDict = nanobind::cast<PyDict>(pyObj)();
        nanobind::list keys = pyDict.keys();
        for(int i = 0; i < len(keys); i++) {
            //                 if(!PyString_Check(keys[i]))
            //                     throw MLDB::Exception("PyDict to JsVal only supports string keys");
            std::string key = nanobind::cast<std::string>(keys[i]);

            nanobind::object obj = nanobind::object(pyDict[keys[i]]);
            val[key] = construct_recur(obj.ptr());
        }
    }
    else if(pyObj == Py_None) {
        // nothing to do. leave Json::Value empty 
    }
    else {
        // try to create a string reprensetation of object for a better error msg
        PyObject* str_obj = PyObject_Str(pyObj);
        PyObject* rep_obj = PyObject_Repr(pyObj);
        std::string str_rep = "<Unable to create str representation of object>";
        if(str_obj && rep_obj) {
            str_rep = nanobind::cast<std::string>(rep_obj);
            str_rep += " ";
            str_rep += nanobind::cast<std::string>(str_obj);
        }
        // not returned so needs to be garbage collected
        Py_DECREF(str_obj);
        Py_DECREF(rep_obj);

        throw MLDB::Exception("Unknown type in PyDict to JsVal converter. "
                              "Str representation: "+str_rep);
    }

    return val;
}

void
JsonValueConverter::
construct(PyObject* obj, void* storage)
{
    Json::Value* js = new (storage) Json::Value();
    (*js) = construct_recur(obj);
}

nanobind::object*
JsonValueConverter::
convert_recur(const Json::Value & js)
{
    if(js.isIntegral()) {
        //TODO this is pretty ugly. find the right way to do this
        int i = js.asInt();
        auto int_as_str = MLDB::lexical_cast<std::string>(i);
        PyObject* pyobj = PyLong_FromString(const_cast<char*>(int_as_str.c_str()), NULL, 10);

        // When you want a nanobind::object to manage a pointer to PyObject* pyobj one does: 
        // nanobind::object o(nanobind::handle<>(pyobj));
        //  In this case, the o object, manages the pyobj, it won’t increase the reference count on construction. 
        return new nanobind::object(nanobind::handle<>(pyobj));

        // using the code below will always return a long 
        //             return new nanobind::int(js.asInt());
    }
    else if(js.isDouble()) {
        return new nanobind::object(js.asDouble());
    }
    else if(js.isString()) {
        return new nanobind::str(js.asString());
    }
    else if(js.isArray()) {
        nanobind::list* lst = new nanobind::list();
        for(int i=0; i<js.size(); i++) {
            lst->append(js[i]);
        }
        return lst;
    }
    else if(js.isObject()) {
        PyDict * dict = new PyDict();
        for (const std::string & id : js.getMemberNames()) {
            (*dict)[id.c_str()] = js[id];
        }
        return dict;
    }
    else {
        throw MLDB::Exception("Unknown type is JsVal to PyDict converter");
    }
}
#endif

std::string srepr(nanobind::handle obj)
{
    std::string result;
    nanobind::str type_repr(nanobind::steal(PyObject_Repr(obj.type().ptr())));
    result = type_repr.c_str() + string(" = ");
    nanobind::str rep_obj(nanobind::steal(PyObject_Repr(obj.ptr())));
    result += rep_obj.c_str();
    return result;
}

static Json::Value
json_from_python_recursive(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup)
{
    //cerr << "json_from_python_recursive" << endl;
    //cerr << srepr(src) << endl;
#if 0
    nanobind::str rep_obj(nanobind::steal(PyObject_Repr(src.ptr())));
    //cerr << "str_obj = " << nanobind::cast<std::string>(str_obj) << endl;
    cerr << "rep = " << rep_obj.c_str() << endl;
    nanobind::str type_obj(nanobind::steal(PyObject_Repr(src.type().ptr())));
    cerr << "type = " << type_obj.c_str() << endl;
#endif

    Json::Value val;
    if (PyBool_Check(src.ptr())) {
        bool b = nanobind::cast<bool>(src);
        val = b;
    }
    else if(PyLong_Check(src.ptr())) {
        //cerr << "long long" << endl;
        long long i = nanobind::cast<long long>(src);
        //cerr << "cast long " << i << endl;
        val = i;
    }
    else if(PyFloat_Check(src.ptr())) {
        double flt = nanobind::cast<double>(src);
        val = flt;
    }
    else if(PyUnicode_Check(src.ptr())) {
        std::string str = nanobind::str(src).c_str();
        val = Utf8String(str);
    }
    else if(PyDateTime_Check(src.ptr())) {
        throw MLDB::Exception("do datetime!!");
    }
    else if(PyTuple_Check(src.ptr()) || PyList_Check(src.ptr())) {
        Json::Value val(Json::ValueType::arrayValue);
        auto len = nanobind::len(src);
        for(size_t i = 0; i < len; ++i) {
            nanobind::object obj = src[i];
            val.append(json_from_python_recursive(obj, flags, cleanup));
        }
        return val;
    }
    else if(PyDict_Check(src.ptr())) {
        //cerr << "dict" << endl;
        val = Json::objectValue;
        nanobind::dict pyDict(src.ptr(), nanobind::detail::borrow_t{});
        //cerr << "got pyDict" << endl;
        //nanobind::dict pyDict = nanobind::cast<nanobind::dict>(src);
        nanobind::list keys = pyDict.keys();
        //cerr << "got keys" << endl;
        for(int i = 0; i < len(keys); i++) {
            //cerr << "key " << i << endl;
            //cerr << srepr(keys[i]) << endl;
            //                 if(!PyString_Check(keys[i]))
            //                     throw MLDB::Exception("PyDict to JsVal only supports string keys");
            Utf8String key = nanobind::str(keys[i]).c_str(); // todo: don't need to check...
            nanobind::object obj = nanobind::object(pyDict[keys[i]]);
            val[key] = json_from_python_recursive(obj, flags, cleanup);
        }
    }
    else if(src.is_none()) {
        // nothing to do. leave Json::Value empty 
    }
    else {
        // try to create a string reprensetation of object for a better error msg
        nanobind::str str_obj(nanobind::steal(PyObject_Str(src.ptr())));
        nanobind::str rep_obj(nanobind::steal(PyObject_Repr(src.ptr())));
        std::string str_rep = "<Unable to create str representation of object>";
        if(str_obj && rep_obj) {
            str_rep = nanobind::cast<std::string>(rep_obj);
            str_rep += " ";
            str_rep += nanobind::cast<std::string>(str_obj);
        }

        throw MLDB::Exception("Unknown type in PyDict to JsVal converter. "
                              "Str representation: "+str_rep);
    }

    //cerr  << "returning val" << val << endl;
    return val;
}

bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, Json::Value * result) noexcept
{
    try {
        cerr << "JSON from_python " << srepr(src) << endl;
        *result = json_from_python_recursive(src, flags, cleanup);
        return true;
    } catch (const std::exception & e) {
        cerr << "Caught exception " << e.what() << endl;
        return false;
    } MLDB_CATCH_ALL {
        cerr << "Caught exception " << getExceptionString() << endl;
        return false;
    }
}

nanobind::object json_from_cpp_recursive(const Json::Value & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup)
{
    if (value.isIntegral()) {
        return nanobind::int_(value.asInt());
    }
    else if (value.isDouble()) {
        return nanobind::float_(value.asDouble());
    }
    else if (value.isString()) {
        return nanobind::str(value.asString().c_str());
    }
    else if (value.isArray()) {
        nanobind::list lst;
        for (const auto & v: value) {
            lst.append(json_from_cpp_recursive(v, policy, cleanup));
        }
        return lst;
    }
    else if (value.isObject()) {
        nanobind::dict dict;
        //cerr << "dict repr " << srepr(dict) << endl;
        for (const auto & id: value.getMemberNames()) {
            //cerr << "converting " << value[id].toString() << endl;
            auto val = json_from_cpp_recursive(value[id], policy, cleanup);
            //cerr << "got val " << srepr(val) << endl;
            //cerr << "inserting to " << id.c_str() << endl;
            dict[id.c_str()] = val;
        }
        return dict;
    }
    else {
        throw MLDB::Exception("Unknown type is JsVal to PyDict converter");
    }
}

nanobind::handle from_cpp(const Json::Value & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup)
{
    return json_from_cpp_recursive(value, policy, cleanup).release();
}

bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, Utf8String * result) noexcept
{
    try {
        // TODO: don't copy the string if it's long
        *result = Utf8String(nanobind::str(src).c_str(), false /* check valid UTF-8 */);
        return true;
    } catch (const std::exception & e) {
        cerr << "Caught exception converting Utf8String" << e.what() << endl;
        return false;
    } MLDB_CATCH_ALL {
        cerr << "Caught exception converting Utf8String" << getExceptionString() << endl;
        return false;
    }
}

nanobind::handle from_cpp(const Utf8String & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup)
{
    return nanobind::str(value.c_str()).release();
}

void pythonConvertersInit(const EnterThreadToken & thread)
{
    PyDateTime_IMPORT;

#if 0
    from_python_converter< Date, DateFromPython >();

    from_python_converter< std::string, StringFromPyUnicode>();
    from_python_converter< Utf8String,  Utf8StringPyConverter>();
    nanobind::to_python_converter< Utf8String, Utf8StringPyConverter>();

    from_python_converter< Path, StrConstructableIdFromPython<Path> >();
    from_python_converter< CellValue, CellValueConverter >();

    from_python_converter<std::pair<string, string>,
                          PairConverter<string, string> >();

    nanobind::to_python_converter<std::pair<string, string>,
                            PairConverter<string, string> >();
        
    from_python_converter< Path, PathConverter>();

    from_python_converter< Json::Value, JsonValueConverter>();
    nanobind::to_python_converter< Json::Value, JsonValueConverter> ();

    nanobind::class_<MLDB::Any>("any", nanobind::no_init)
        .def("as_json",   &MLDB::Any::asJson)
        ;
#endif
}

RegisterPythonInitializer regMe(&pythonConvertersInit);

} // namespace Python
} // namespace MLDB

#if 0
NAMESPACE_BEGIN(NB_NAMESPACE)
NAMESPACE_BEGIN(detail)

bool
type_caster<Json::Value, int>::
from_python(handle src, uint8_t flags, cleanup_list *cleanup) noexcept
{
    return false;
}

handle
type_caster<Json::Value, int>::
from_cpp(const Json::Value & value, rv_policy policy, cleanup_list *cleanup)
{
    return {};
}

NAMESPACE_END(detail)
NAMESPACE_END(NB_NAMESPACE)
#endif