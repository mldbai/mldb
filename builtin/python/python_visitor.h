/* python_visitor.h                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/utils/is_visitable.h"
#include "nanobind/nanobind.h"
#include <iostream>

namespace MLDB {

struct PythonNoneTag {};

template<typename Visitor, typename... ExtraTypes>
auto visit(Visitor && visitor, nanobind::handle) -> typename std::decay_t<Visitor>::return_type;

template<typename Unknown>
struct HandleUnknownPyObject {
    using return_type = std::invoke_result_t<Unknown, nanobind::handle>;
    Unknown unknown;
    HandleUnknownPyObject(Unknown&&unknown) : unknown(std::move(unknown)) {}
};

template<typename Result>
struct ExceptionOnUnknownPyObjectReturning {
    using return_type = Result;
    Result unknown(nanobind::handle val) const
    {
        MLDB_THROW_LOGIC_ERROR("Encountered unknown PyObject type : %s", nanobind::repr(val).c_str());
    }
};

template<typename Result>
struct HandleUnknownPyObject<ExceptionOnUnknownPyObjectReturning<Result>>: ExceptionOnUnknownPyObjectReturning<Result> {
    template<typename T>
    HandleUnknownPyObject(T&&arg): ExceptionOnUnknownPyObjectReturning<Result>(std::forward<T>(arg)) {}
};

template<typename Unknown, class... Ops>
struct PyObjectLambdaVisitor
    : HandleUnknownPyObject<Unknown>, Ops... {
    using visitor_base = PyObjectLambdaVisitor;
    using Ops::operator ()...;
    using HandleUnknownPyObject<Unknown>::unknown;

    template<typename Arg>
    auto visit(Arg&& arg) -> std::invoke_result_t<PyObjectLambdaVisitor, Arg>
    {
        return (*this)(std::forward<Arg>(arg));
    }
};

template<typename Unknown, class... Ts>
PyObjectLambdaVisitor(Unknown, Ts...) -> PyObjectLambdaVisitor<Unknown, Ts...>;

template<typename Visitor, typename... ExtraTypes>
auto visit(Visitor && visitor, nanobind::handle handle) -> typename std::decay_t<Visitor>::return_type
{
    //std::cerr << "visiting python object " << nanobind::repr(handle).c_str() << std::endl;
    using DecayedVisitor = typename std::decay_t<Visitor>;
    using Return = typename DecayedVisitor::return_type;

#   define HANDLE_NANOBIND_TYPE(type, check) \
    else if (isVisitable<DecayedVisitor, Return(nanobind::type)>() && check(handle.ptr())) { \
        /*std::cerr << "  testing against " << #type << std::endl;*/ \
        if constexpr (isVisitable<DecayedVisitor, Return(nanobind::type)>()) { \
            /*std::cerr << "  matched against " << #type << std::endl;*/ \
            return visitor.visit(nanobind::cast<nanobind::type>(handle)); \
        } \
    }

    if (!handle.is_valid()) {
        if constexpr (isVisitable<DecayedVisitor, Return(std::nullptr_t)>()) {
            return visitor.visit(nullptr);
        }
        else MLDB_THROW_LOGIC_ERROR("null Python Object passed to vistor with no nullptr_t handler");
    }
    else if (handle.is_none()) {
        if constexpr (isVisitable<DecayedVisitor, Return(PythonNoneTag)>()) {
            return visitor.visit(PythonNoneTag{});
        }
    }
    else if (isVisitable<DecayedVisitor, Return(Date)>() && PyDateTime_Check(handle.ptr())) {
        if constexpr (isVisitable<DecayedVisitor, Return(Date)>()) {
            Date d(PyDateTime_GET_YEAR            (handle.ptr()),
                   PyDateTime_GET_MONTH           (handle.ptr()),
                   PyDateTime_GET_DAY             (handle.ptr()),
                   PyDateTime_DATE_GET_HOUR       (handle.ptr()),
                   PyDateTime_DATE_GET_MINUTE     (handle.ptr()),
                   PyDateTime_DATE_GET_SECOND     (handle.ptr()),
                   PyDateTime_DATE_GET_MICROSECOND(handle.ptr()) / 1000000.);
            return visitor.visit(d);
        }
    }

    HANDLE_NANOBIND_TYPE(bool_, PyBool_Check)
    HANDLE_NANOBIND_TYPE(int_, PyLong_Check)
    HANDLE_NANOBIND_TYPE(float_, PyFloat_Check)
    HANDLE_NANOBIND_TYPE(str, PyUnicode_Check)
    HANDLE_NANOBIND_TYPE(bytes, PyBytes_Check)
    HANDLE_NANOBIND_TYPE(bytearray, PyBytes_Check)
    HANDLE_NANOBIND_TYPE(tuple, PyTuple_Check)
    HANDLE_NANOBIND_TYPE(list, PyList_Check)
    HANDLE_NANOBIND_TYPE(dict, PyDict_Check)
    HANDLE_NANOBIND_TYPE(set, PySet_Check)
    HANDLE_NANOBIND_TYPE(module_, PyModule_Check)
    //HANDLE_NANOBIND_TYPE(capsule, PyCapsule_Check)
    HANDLE_NANOBIND_TYPE(callable, PyCallable_Check)
    HANDLE_NANOBIND_TYPE(slice, PySlice_Check)
    //HANDLE_NANOBIND_TYPE(ellipsis, PyEllipsis_Check)


    return visitor.unknown(handle);   
}

} // namespace MLDB
