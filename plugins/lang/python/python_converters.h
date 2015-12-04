// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** python_converters.h                                 -*- C++ -*-
    Rémi Attab, 13 Dec 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Python converters for common types.

    \todo Add a helper function to initialize the converters for the various
    types. These should only be initialized once so use std::call_once.

*/

#pragma once

#include <boost/python.hpp>
#include "mldb/types/id.h"
#include "mldb/types/date.h"
#include "datetime.h"
#include <vector>
#include <memory>
#include "mldb/base/exc_check.h"
#include "mldb/ext/jsoncpp/value.h"


namespace Datacratic {
namespace Python {

/******************************************************************************/
/*   PAIR CONVERTER                                                           */
/******************************************************************************/

/** To and from convertion of std::pair<T, T>.

    Note that for this to work a to_python or a from_python converter must be
    defined for T. Also, to convert from_python every elements must be
    convertible to T.
 */
template<typename T1, typename T2>
struct PairConverter
{

    typedef boost::python::list PyTuple;

    /** PyTuple ?-> std::pair<T1, T2> */
    static void* convertible(PyObject* obj)
    { 
        //std::cout << "convertible called" << std::endl;
        boost::python::extract<PyTuple> tupleExtract(obj);
        if (!tupleExtract.check()) return nullptr;
        PyTuple tuple = tupleExtract();
        if (boost::python::len(tuple) != 2) return nullptr;

        // We may also want to make sure that every element is convertible.
        // Otherwise we can just let it fail during construction.
        boost::python::extract<T1> extract1(tuple[0]);
        if (!extract1.check())
        {
            return nullptr;
        }
        boost::python::extract<T2> extract2(tuple[1]);
        if (!extract2.check())
        {
            return nullptr;
        }
        //std::cout << "convertible returning" << std::endl;
        return obj;
    }


    /** PyTuple -> std::pair<T1, T2> */
    static void construct(PyObject* obj, void* storage)
    {
        //std::cout << "construct called" << std::endl;
        PyTuple tuple = boost::python::extract<PyTuple>(obj)();
        std::pair<T1, T2>* p = new (storage) std::pair<T1, T2>();

        p->first = boost::python::extract<T1>(tuple[0])();
        p->second = boost::python::extract<T2>(tuple[1])();
        //std::cout << "construct returning" << std::endl;
    }


    /** std::pair<T1, T2> -> PyTuple */
    static PyObject* convert(const std::pair<T1, T2>& p)
    {
        PyTuple* tuple = new PyTuple;

        tuple->append(p.first);
        tuple->append(p.second);

        return boost::python::incref(tuple->ptr());
    }
};

/******************************************************************************/
/*   3 ELEMENT TUPLE CONVERTER                                                */
/******************************************************************************/

/** To and from convertion of std::tuple<T, T, T>.

    Note that for this to work a to_python or a from_python converter must be
    defined for T. Also, to convert from_python every elements must be
    convertible to T.

    TODO
    This should be generalised to any number of elements
 */
template<typename T1, typename T2, typename T3>
struct Tuple3ElemConverter
{

    typedef boost::python::list PyTuple;

    /** PyTuple ?-> std::tuple<T1, T2, T3> */
    static void* convertible(PyObject* obj)
    { 
        //std::cout << "tuple convertible called" << std::endl;
        boost::python::extract<PyTuple> tupleExtract(obj);
        if (!tupleExtract.check()) return nullptr;
        PyTuple tuple = tupleExtract();
        if (boost::python::len(tuple) != 3) return nullptr;

        // We may also want to make sure that every element is convertible.
        // Otherwise we can just let it fail during construction.
        boost::python::extract<T1> extract1(tuple[0]);
        if (!extract1.check())
        {
            return nullptr;
        }
        boost::python::extract<T2> extract2(tuple[1]);
        if (!extract2.check())
        {
            return nullptr;
        }
        boost::python::extract<T3> extract3(tuple[2]);
        if (!extract3.check())
        {
            return nullptr;
        }
        //std::cout << "convertible returning" << std::endl;
        return obj;
    }


    /** PyTuple -> std::tuple<T1, T2, T3> */
    static void construct(PyObject* obj, void* storage)
    {
        //std::cout << "construct called" << std::endl;
        PyTuple tuple = boost::python::extract<PyTuple>(obj)();
        std::tuple<T1, T2, T3>* p = new (storage) std::tuple<T1, T2, T3>();

        std::get<0>(*p) = boost::python::extract<T1>(tuple[0])();
        std::get<1>(*p) = boost::python::extract<T2>(tuple[1])();
        std::get<2>(*p) = boost::python::extract<T3>(tuple[2])();
        //std::cout << "construct returning" << std::endl;
    }


    /** std::tuple<T1, T2, T3> -> PyTuple */
    static PyObject* convert(const std::tuple<T1, T2, T3>& p)
    {
        PyTuple* tuple = new PyTuple;

        tuple->append(std::get<0>(p));
        tuple->append(std::get<1>(p));
        tuple->append(std::get<2>(p));

        return boost::python::incref(tuple->ptr());
    }
};

/******************************************************************************/
/* VECTOR CONVERTER                                                           */
/******************************************************************************/

/** To and from convertion of std::vector<T>.

    Note that for this to work a to_python or a from_python converter must be
    defined for T. Also, to convert from_python every elements must be
    convertible to T.
 */
template<typename T, bool safe = false>
struct VectorConverter
{

    typedef boost::python::list PyList;

    /** PyList ?-> std::vector<T> */
    static void* convertible(PyObject* obj)
    {
        //std::cout << "vector convertible called" << std::endl;
        boost::python::extract<PyList> listExtract(obj);
        if (!listExtract.check()) return nullptr;

        // We may also want to make sure that every element is convertible.
        // Otherwise we can just let it fail during construction.
        if (safe) {
            PyList pyList = listExtract();
            boost::python::ssize_t n = boost::python::len(pyList);

            for (boost::python::ssize_t i = 0; i < n; ++i) {
                boost::python::extract<T> eleExtract(pyList[i]);
                if (!eleExtract.check())
                {
                    return nullptr;
                }
            }
        }
        return obj;
    }


    /** PyList -> std::vector<T> */
    static void construct(PyObject* obj, void* storage)
    {
        //std::cout << "vector construct" << std::endl;
        PyList pyList = boost::python::extract<PyList>(obj)();
        std::vector<T>* vec = new (storage) std::vector<T>();

        boost::python::ssize_t n = boost::python::len(pyList);
        vec->reserve(n);

        for (boost::python::ssize_t i = 0; i < n; ++i)
            vec->push_back(boost::python::extract<T>(pyList[i])());
    }


    /** std::vector<T> -> PyList */
    static PyObject* convert(const std::vector<T>& vec)
    {
        PyList* list = new PyList;

        for (const T& element : vec)
            list->append(element);

        // incref because of: https://misspent.wordpress.com/2009/09/27/how-to-write-boost-python-converters/
        return boost::python::incref(list->ptr());
    }
};


/******************************************************************************/
/* DATE TO DATE TIME                                                          */
/******************************************************************************/

struct DateToPython
{
    static PyObject* convert(const Date& date)
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
};


/******************************************************************************/
/* DATE FROM PYTHON                                                           */
/******************************************************************************/

struct DateFromPython
{
    // will interpret an int or float from python as seconds since epoch 
    static void* convertible(PyObject* obj_ptr)
    {
        if (!PyDateTime_Check(obj_ptr) && !PyFloat_Check(obj_ptr) && 
                !PyInt_Check(obj_ptr) && !PyString_Check(obj_ptr)) {
            return 0;
        }
        return obj_ptr;
    }

    static void construct(PyObject* obj_ptr, void* storage)
    {
        if (PyFloat_Check(obj_ptr)) {
            Date x = Date::fromSecondsSinceEpoch(PyFloat_AS_DOUBLE(obj_ptr));
            new (storage) Date(x);
        } else if (PyInt_Check(obj_ptr)) {
            Date x = Date::fromSecondsSinceEpoch(PyInt_AS_LONG(obj_ptr));
            new (storage) Date(x);
        } else if (PyString_Check(obj_ptr)) {
            std::string str = boost::python::extract<std::string>(obj_ptr)();
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
};


/******************************************************************************/
/* INT TO PY LONG                                                             */
/******************************************************************************/

// Converts IntWrapper objects into PyLong objects
template<class T>
struct IntToPyLong
#ifndef BOOST_PYTHON_NO_PY_SIGNATURES
    : boost::python::converter::to_python_target_type<T>  //inherits get_pytype
#endif
{
    static PyObject* convert(const T& intwrap)
    {
        return PyLong_FromUnsignedLong(intwrap.index());
    }
};


/******************************************************************************/
/* ID TO INT                                                                  */
/******************************************************************************/

// Converts Ids to boost::python::str objects
struct IdToPython
{
    static PyObject* convert(const Id& id)
    {
        boost::python::str* s = new boost::python::str(id.toString());
        return boost::python::incref(s->ptr());
    }
};



/******************************************************************************/
/* DATE TO DOUBLE                                                             */
/******************************************************************************/

// Converts Date objects into PyFloat objects (timestamp)
// You probably want to use DateToDateTime instead

struct DateToPyFloat
{
    static PyObject* convert(const Date& date)
    {
        return PyFloat_FromDouble(date.secondsSinceEpoch());
    }
};



/******************************************************************************/
/* StrConstructableID FROM PYTHON                                             */
/*   this converter can be used by any class which should be instantiated
 *   with a string                                                            */
/******************************************************************************/

template<typename StrConstructable>
struct StrConstructableIdFromPython
{
    static void* convertible(PyObject* obj_ptr)
    {
        if (!PyString_Check(obj_ptr) && !PyUnicode_Check(obj_ptr)
                && !PyInt_Check(obj_ptr) && !PyFloat_Check(obj_ptr)) {
            return 0;
        }
        return obj_ptr;
    }

    static void construct(PyObject* obj_ptr, void* storage)
    {
        std::string id;
        if (PyInt_Check(obj_ptr)) {
            id = to_string(boost::python::extract<int>(obj_ptr)());
        }
        else if (PyFloat_Check(obj_ptr)) {
            id = to_string(boost::python::extract<float>(obj_ptr)());
        }
        else if (PyUnicode_Check(obj_ptr) || PyString_Check(obj_ptr)) {
            if (PyUnicode_Check(obj_ptr)) {
                obj_ptr = PyUnicode_AsUTF8String(obj_ptr);
                ExcCheck(obj_ptr!=nullptr,
                        "Error converting unicode to ASCII");
            }
            id = boost::python::extract<std::string>(obj_ptr)();
        } else {
            throw ML::Exception("StrConstructableIdFromPython: "
                                "Failed to convert value to id");
        }

        new (storage) StrConstructable(id);
    }
};




/******************************************************************************/
/* LONG FROM PYTHON                                                           */
/******************************************************************************/

template <typename IndexT>
struct IndexFromPython
{
    static void* convertible(PyObject* obj_ptr)
    {
        if (!PyLong_Check(obj_ptr)) return 0;
        return obj_ptr;
    }

    static void construct(PyObject* obj_ptr, void* storage)
    {
        new (storage) IndexT(boost::python::extract<int>(obj_ptr)());
    }
};

struct StringFromPyUnicode
{
    static void* convertible(PyObject* obj_ptr)
    {
        if (!PyUnicode_Check(obj_ptr)){
            return 0;
        }
        return obj_ptr;
    }

    static void construct(PyObject* obj_ptr, void* storage)
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
            boost::python::extract<std::string>(from_unicode)());
        Py_XDECREF(from_unicode);
    }

};

struct Utf8StringPyConverter
{
    static void* convertible(PyObject* obj_ptr)
    {
        if (!PyUnicode_Check(obj_ptr)){
            return 0;
        }
        return obj_ptr;
    }

    static void construct(PyObject* obj_ptr, void* storage)
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
            boost::python::extract<std::string>(from_unicode)());
        Py_XDECREF(from_unicode);
    }

    /** Json::Value -> PyDict */
    static PyObject* convert(const Utf8String & str)
    {
        return PyUnicode_FromStringAndSize(str.rawData(), str.rawLength());
    }
};


/******************************************************************************/
/* Json::Value CONVERTER                                                      */
/******************************************************************************/

/**
 * To and from convertion of Json::Value to PythonDict
 */
struct JsonValueConverter
{
    typedef boost::python::dict PyDict;

    /** PyDict ?-> Json::Value */
    static void* convertible(PyObject* obj)
    {
        return obj;
    }

    static Json::Value construct_recur(PyObject * pyObj)
    {
        namespace bp = boost::python;

        Json::Value val;
        if(PyBool_Check(pyObj)) {
            bool b = bp::extract<bool>(pyObj);
            val = b;
        }
        else if(PyInt_Check(pyObj)) {
            int i = bp::extract<int>(pyObj);
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
                std::cerr << "WARNING! Unable to extract unicode to ascii" << std::endl;
            }
            else {
                std::string str = bp::extract<std::string>(from_unicode);
                val = Utf8String(str);

                // not returned so needs to be garbage collected
                Py_DECREF(from_unicode);
            }
        }
        else if(PyDateTime_Check(pyObj)) {
            throw ML::Exception("do datetime!!");
        }
        else if(PyTuple_Check(pyObj) || PyList_Check(pyObj)) {
            val = Json::Value(Json::ValueType::arrayValue);
            bp::list lst = bp::extract<bp::list>(pyObj);
            for(int i = 0; i < len(lst); i++) {
                bp::object obj = bp::object(lst[i]);
                val.append(construct_recur(obj.ptr()));
            }
        }
        else if(PyDict_Check(pyObj)) {
            PyDict pyDict = bp::extract<PyDict>(pyObj)();
            bp::list keys = pyDict.keys();
            for(int i = 0; i < len(keys); i++) {
//                 if(!PyString_Check(keys[i]))
//                     throw ML::Exception("PyDict to JsVal only supports string keys");
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

            throw ML::Exception("Unknown type in PyDict to JsVal converter. "
                    "Str representation: "+str_rep);
        }

        return val;
    }

    /** PyDict -> Json::Value */
    static void construct(PyObject* obj, void* storage)
    {
        Json::Value* js = new (storage) Json::Value();
        (*js) = construct_recur(obj);
    }

    static boost::python::object* convert_recur(const Json::Value & js)
    {
        namespace bp = boost::python;

        if(js.isIntegral()) {
            //TODO this is pretty ugly. find the right way to do this
            int i = js.asInt();
            const char* int_as_str = boost::lexical_cast<std::string>(i).c_str();
            PyObject* pyobj = PyInt_FromString(const_cast<char*>(int_as_str), NULL, 10);

            // When you want a boost::python::object to manage a pointer to PyObject* pyobj one does: 
            // boost::python::object o(boost::python::handle<>(pyobj));
            //  In this case, the o object, manages the pyobj, it won’t increase the reference count on construction. 
            return new bp::object(boost::python::handle<>(pyobj));

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
            throw ML::Exception("Unknown type is JsVal to PyDict converter");
        }
    }

    /** Json::Value -> PyDict */
    static PyObject* convert(const Json::Value & js)
    {
        return boost::python::incref(convert_recur(js)->ptr());
    }
};


} // namespace Python
} // Datacratic

