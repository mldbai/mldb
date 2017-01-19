/** python_converters.h                                 -*- C++ -*-
    Rémi Attab, 13 Dec 2012
    Copyright (c) 2012 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    
    Python converters for common types.

    \todo Add a helper function to initialize the converters for the various
    types. These should only be initialized once so use std::call_once.

*/

#pragma once

#include <boost/python.hpp>
#include "mldb/types/date.h"
#include <vector>
#include <memory>
#include "mldb/base/exc_check.h"
#include "mldb/ext/jsoncpp/value.h"
#include "pointer_fix.h"

namespace MLDB {

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
    static PyObject* convert(const Date& date);
};


/******************************************************************************/
/* DATE FROM PYTHON                                                           */
/******************************************************************************/

struct DateFromPython
{
    // will interpret an int or float from python as seconds since epoch 
    static void* convertible(PyObject* obj_ptr);

    static void construct(PyObject* obj_ptr, void* storage);
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
/* DATE TO DOUBLE                                                             */
/******************************************************************************/

// Converts Date objects into PyFloat objects (timestamp)
// You probably want to use DateToDateTime instead

struct DateToPyFloat
{
    static PyObject* convert(const Date& date);
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
        using std::to_string;
        std::string id;
        if (PyInt_Check(obj_ptr)) {
            id = to_string(boost::python::extract<int>(obj_ptr)());
        }
        else if (PyFloat_Check(obj_ptr)) {
            id = to_string(boost::python::extract<double>(obj_ptr)());
        }
        else if (PyUnicode_Check(obj_ptr) || PyString_Check(obj_ptr)) {
            if (PyUnicode_Check(obj_ptr)) {
                obj_ptr = PyUnicode_AsUTF8String(obj_ptr);
                ExcCheck(obj_ptr!=nullptr,
                        "Error converting unicode to ASCII");
            }
            id = boost::python::extract<std::string>(obj_ptr)();
        } else {
            throw MLDB::Exception("StrConstructableIdFromPython: "
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
    static void* convertible(PyObject* obj_ptr);

    static void construct(PyObject* obj_ptr, void* storage);
};

struct Utf8StringPyConverter
{
    static void* convertible(PyObject* obj_ptr);

    static void construct(PyObject* obj_ptr, void* storage);

    /** Json::Value -> PyDict */
    static PyObject* convert(const Utf8String & str);
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
    static void* convertible(PyObject* obj);

    static Json::Value construct_recur(PyObject * pyObj);

    /** PyDict -> Json::Value */
    static void construct(PyObject* obj, void* storage);

    static boost::python::object* convert_recur(const Json::Value & js);

    /** Json::Value -> PyDict */
    static PyObject* convert(const Json::Value & js);
};


} // namespace Python

} // namespace MLDB
