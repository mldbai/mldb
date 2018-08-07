/** python_converters.cc
    Jeremy Barnes, 7 December 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "python_converters.h"
#include "Python.h"
#include "datetime.h"
#include <iostream>

using namespace std;

namespace bp = boost::python;

namespace MLDB {

namespace Python {


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
        !PyInt_Check(obj_ptr) && !PyString_Check(obj_ptr)) {
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
    } else if (PyInt_Check(obj_ptr)) {
        Date x = Date::fromSecondsSinceEpoch(PyInt_AS_LONG(obj_ptr));
        new (storage) Date(x);
    } else if (PyString_Check(obj_ptr)) {
        std::string str = bp::extract<std::string>(obj_ptr)();
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
                              bp::extract<std::string>(from_unicode)());
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
                             bp::extract<std::string>(from_unicode)());
    Py_XDECREF(from_unicode);
}

PyObject*
Utf8StringPyConverter::
convert(const Utf8String & str)
{
    return PyUnicode_FromStringAndSize(str.rawData(), str.rawLength());
}


/******************************************************************************/
/* Json::Value CONVERTER                                                      */
/******************************************************************************/

void*
JsonValueConverter::
convertible(PyObject* obj)
{
    return obj;
}

template<typename LstType>
Json::Value
construct_lst(PyObject * pyObj)
{
    Json::Value val(Json::ValueType::arrayValue);
    LstType tpl = bp::extract<LstType>(pyObj);
    for(int i = 0; i < len(tpl); i++) {
        bp::object obj = bp::object(tpl[i]);
        val.append(JsonValueConverter::construct_recur(obj.ptr()));
    }
    return val;
}

Json::Value
JsonValueConverter::
construct_recur(PyObject * pyObj)
{
    Json::Value val;
    if PyBool_Check(pyObj) {
        bool b = bp::extract<bool>(pyObj);
        val = b;
    }
    else if(PyInt_Check(pyObj)) {
        int64_t i = bp::extract<int64_t>(pyObj);
        val = i;
    }
    else if(PyFloat_Check(pyObj)) {
        float flt = bp::extract<float>(pyObj);
        val = flt;
    }
    else if(PyString_Check(pyObj)) {
        std::string str = bp::extract<std::string>(pyObj);
        val = str;
    }
    else if(PyUnicode_Check(pyObj)) {
        // https://docs.python.org/2/c-api/unicode.html#c.PyUnicode_AsUTF8String
        PyObject* from_unicode = PyUnicode_AsUTF8String(pyObj);
        if(!from_unicode) {
            PyObject* str_obj = PyObject_Str(pyObj);
            std::string str_rep = "<Unable to create str representation of object>";
            if(str_obj) {
                str_rep = bp::extract<std::string>(str_obj);
            }
            // not returned so needs to be garbage collected
            Py_DECREF(str_obj);

            throw MLDB::Exception("Unable to encode unicode to UTF-8"
                                  "Str representation: "+str_rep);
        }
        else {
            std::string str = bp::extract<std::string>(from_unicode);
            val = Utf8String(str);

            // not returned so needs to be garbage collected
            Py_DECREF(from_unicode);
        }
    }
    //else if(PyDateTime_Check(pyObj)) {
    //    throw MLDB::Exception("do datetime!!");
    //}
    else if(PyTuple_Check(pyObj)) {
        val = construct_lst<bp::tuple>(pyObj);
    }
    else if(PyList_Check(pyObj)) {
        val = construct_lst<bp::list>(pyObj);
    }
    else if(PyDict_Check(pyObj)) {
        val = Json::objectValue;
        PyDict pyDict = bp::extract<PyDict>(pyObj)();
        bp::list keys = pyDict.keys();
        for(int i = 0; i < len(keys); i++) {
            //                 if(!PyString_Check(keys[i]))
            //                     throw MLDB::Exception("PyDict to JsVal only supports string keys");
            std::string key = bp::extract<std::string>(keys[i]);

            bp::object obj = bp::object(pyDict[keys[i]]);
            val[key] = construct_recur(obj.ptr());
        }
    }
    else if(pyObj == Py_None) {
        // nothing to do. leave Json::Value empty 
    }
    else {
        // try to create a string reprensetation of object for a better error msg
        PyObject* str_obj = PyObject_Str(pyObj);
        std::string str_rep = "<Unable to create str representation of object>";
        if(str_obj) {
            str_rep = bp::extract<std::string>(str_obj);
        }
        // not returned so needs to be garbage collected
        Py_DECREF(str_obj);

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

bp::object*
JsonValueConverter::
convert_recur(const Json::Value & js)
{
    if(js.isIntegral()) {
        //TODO this is pretty ugly. find the right way to do this
        int i = js.asInt();
        auto int_as_str = boost::lexical_cast<std::string>(i);
        PyObject* pyobj = PyInt_FromString(const_cast<char*>(int_as_str.c_str()), NULL, 10);

        // When you want a bp::object to manage a pointer to PyObject* pyobj one does: 
        // bp::object o(bp::handle<>(pyobj));
        //  In this case, the o object, manages the pyobj, it won’t increase the reference count on construction. 
        return new bp::object(bp::handle<>(pyobj));

        // using the code below will always return a long 
        //             return new bp::int(js.asInt());
    }
    else if(js.isDouble()) {
        return new bp::object(js.asDouble());
    }
    else if(js.isString()) {
        return new bp::str(js.asString());
    }
    else if(js.isArray()) {
        bp::list* lst = new bp::list();
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

PyObject*
JsonValueConverter::
convert(const Json::Value & js)
{
    return bp::incref(convert_recur(js)->ptr());
}

static struct AtInit {
    AtInit()
    {
        PyDateTime_IMPORT;
    }
} atInit;

} // namespace Python

} // namespace MLDB
