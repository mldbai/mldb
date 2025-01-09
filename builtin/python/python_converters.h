/** python_converters.h                                 -*- C++ -*-
    Rémi Attab, 13 Dec 2012
    Copyright (c) 2012 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    
    Python converters for common types.

    \todo Add a helper function to initialize the converters for the various
    types. These should only be initialized once so use std::call_once.

*/

#pragma once

#include "nanobind/nanobind.h"
#include "mldb/types/date.h"
#include <vector>
#include <memory>
#include "mldb/base/exc_check.h"
#include "mldb/ext/jsoncpp/value.h"
#include "mldb/types/string.h"

namespace MLDB {

namespace Python {

#if 0
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

    typedef nanobind::list PyTuple;

    /** PyTuple ?-> std::pair<T1, T2> */
    static void* convertible(PyObject* obj)
    { 
        //std::cout << "convertible called" << std::endl;
        nanobind::cast<PyTuple> tupleExtract(obj);
        if (!tupleExtract.check()) return nullptr;
        PyTuple tuple = tupleExtract();
        if (nanobind::len(tuple) != 2) return nullptr;

        // We may also want to make sure that every element is convertible.
        // Otherwise we can just let it fail during construction.
        nanobind::cast<T1> extract1(tuple[0]);
        if (!extract1.check())
        {
            return nullptr;
        }
        nanobind::cast<T2> extract2(tuple[1]);
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
        PyTuple tuple = nanobind::cast<PyTuple>(obj)();
        std::pair<T1, T2>* p = new (storage) std::pair<T1, T2>();

        p->first = nanobind::cast<T1>(tuple[0])();
        p->second = nanobind::cast<T2>(tuple[1])();
        //std::cout << "construct returning" << std::endl;
    }


    /** std::pair<T1, T2> -> PyTuple */
    static PyObject* convert(const std::pair<T1, T2>& p)
    {
        PyTuple* tuple = new PyTuple;

        tuple->append(p.first);
        tuple->append(p.second);

        return nanobind::incref(tuple->ptr());
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

    typedef nanobind::list PyTuple;

    /** PyTuple ?-> std::tuple<T1, T2, T3> */
    static void* convertible(PyObject* obj)
    { 
        //std::cout << "tuple convertible called" << std::endl;
        nanobind::cast<PyTuple> tupleExtract(obj);
        if (!tupleExtract.check()) return nullptr;
        PyTuple tuple = tupleExtract();
        if (nanobind::len(tuple) != 3) return nullptr;

        // We may also want to make sure that every element is convertible.
        // Otherwise we can just let it fail during construction.
        nanobind::cast<T1> extract1(tuple[0]);
        if (!extract1.check())
        {
            return nullptr;
        }
        nanobind::cast<T2> extract2(tuple[1]);
        if (!extract2.check())
        {
            return nullptr;
        }
        nanobind::cast<T3> extract3(tuple[2]);
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
        PyTuple tuple = nanobind::cast<PyTuple>(obj)();
        std::tuple<T1, T2, T3>* p = new (storage) std::tuple<T1, T2, T3>();

        std::get<0>(*p) = nanobind::cast<T1>(tuple[0])();
        std::get<1>(*p) = nanobind::cast<T2>(tuple[1])();
        std::get<2>(*p) = nanobind::cast<T3>(tuple[2])();
        //std::cout << "construct returning" << std::endl;
    }


    /** std::tuple<T1, T2, T3> -> PyTuple */
    static PyObject* convert(const std::tuple<T1, T2, T3>& p)
    {
        PyTuple* tuple = new PyTuple;

        tuple->append(std::get<0>(p));
        tuple->append(std::get<1>(p));
        tuple->append(std::get<2>(p));

        return nanobind::incref(tuple->ptr());
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

    typedef nanobind::list PyList;

    /** PyList ?-> std::vector<T> */
    static void* convertible(PyObject* obj)
    {
        //std::cout << "vector convertible called" << std::endl;
        nanobind::cast<PyList> listExtract(obj);
        if (!listExtract.check()) return nullptr;

        // We may also want to make sure that every element is convertible.
        // Otherwise we can just let it fail during construction.
        if (safe) {
            PyList pyList = listExtract();
            nanobind::ssize_t n = nanobind::len(pyList);

            for (nanobind::ssize_t i = 0; i < n; ++i) {
                nanobind::cast<T> eleExtract(pyList[i]);
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
        PyList pyList = nanobind::cast<PyList>(obj)();
        std::vector<T>* vec = new (storage) std::vector<T>();

        nanobind::ssize_t n = nanobind::len(pyList);
        vec->reserve(n);

        for (nanobind::ssize_t i = 0; i < n; ++i)
            vec->push_back(nanobind::cast<T>(pyList[i])());
    }


    /** std::vector<T> -> PyList */
    static PyObject* convert(const std::vector<T>& vec)
    {
        PyList* list = new PyList;

        for (const T& element : vec)
            list->append(element);

        // incref because of: https://misspent.wordpress.com/2009/09/27/how-to-write-boost-python-converters/
        return nanobind::incref(list->ptr());
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
    : nanobind::converter::to_python_target_type<T>  //inherits get_pytype
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
        if (!PyBytes_Check(obj_ptr) && !PyUnicode_Check(obj_ptr)
                && !PyLong_Check(obj_ptr) && !PyFloat_Check(obj_ptr)) {
            return 0;
        }
        return obj_ptr;
    }

    static void construct(PyObject* obj_ptr, void* storage)
    {
        using std::to_string;
        std::string id;
        if (PyLong_Check(obj_ptr)) {
            id = to_string(nanobind::cast<long>(obj_ptr)());
        }
        else if (PyFloat_Check(obj_ptr)) {
            id = to_string(nanobind::cast<double>(obj_ptr)());
        }
        else if (PyUnicode_Check(obj_ptr) || PyBytes_Check(obj_ptr)) {
            if (PyUnicode_Check(obj_ptr)) {
                obj_ptr = PyUnicode_AsUTF8String(obj_ptr);
                ExcCheck(obj_ptr!=nullptr,
                        "Error converting unicode to ASCII");
            }
            id = nanobind::cast<std::string>(obj_ptr)();
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
        new (storage) IndexT(nanobind::cast<long>(obj_ptr)());
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
#endif

/******************************************************************************/
/* Utf8String CONVERTER                                                       */
/******************************************************************************/

bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, Utf8String * result) noexcept;
nanobind::handle from_cpp(const Utf8String & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup);

/******************************************************************************/
/* Json::Value CONVERTER                                                      */
/******************************************************************************/

bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, Json::Value * result) noexcept;
nanobind::handle from_cpp(const Json::Value & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup);

} // namespace Python
} // namespace MLDB


NAMESPACE_BEGIN(NB_NAMESPACE)
NAMESPACE_BEGIN(detail)

template<> struct type_caster<Json::Value> {
    NB_TYPE_CASTER(Json::Value, const_name("json"));

    /// Python -> C++ caster, populates cast_value upon success
    bool from_python(handle src, uint8_t flags, cleanup_list *cleanup) noexcept
    {
        return MLDB::Python::from_python(src, flags, cleanup, &value);
    }

    // C++ to Python
    static handle from_cpp(const Json::Value & value, rv_policy policy, cleanup_list *cleanup)
    {
        return MLDB::Python::from_cpp(value, policy, cleanup);
    }
};

template<> struct type_caster<MLDB::Utf8String> {
    NB_TYPE_CASTER(MLDB::Utf8String, const_name("utf8"));

    /// Python -> C++ caster, populates cast_value upon success
    bool from_python(handle src, uint8_t flags, cleanup_list *cleanup) noexcept
    {
        return MLDB::Python::from_python(src, flags, cleanup, &value);
    }

    // C++ to Python
    static handle from_cpp(const MLDB::Utf8String & value, rv_policy policy, cleanup_list *cleanup)
    {
        return MLDB::Python::from_cpp(value, policy, cleanup);
    }
};

NAMESPACE_END(detail)
NAMESPACE_END(NB_NAMESPACE)
