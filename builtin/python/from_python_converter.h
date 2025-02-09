/** from_python_converter.h                                 -*- C++ -*-
    Rémi Attab, 13 Dec 2012
    Copyright (c) 2012 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    boost-python helper to convert python data types to C++ data types.

    Naming style (underscore instead of camel case) is to fit with the
    boost-python naming style.

 */

#pragma once

#include "nanobind/nanobind.h"

namespace MLDB {
namespace Python {

#if 0
/******************************************************************************/
/* FROM_PYTHON_CONVERTER                                                      */
/******************************************************************************/

/** Converts a type T from python using the provided Converter.

    Converter must define the following static functions:

    - void* convertible(PyObject* obj)
      Returns obj if the object is convertible to T. Return 0 otherwise.

   - void construct(PyObject* obj, void* storage)
     Initializes an object T in storage (use placement new) from the python
     object obj.

 */
template<typename T, typename Converter>
struct from_python_converter
{
    from_python_converter()
    {
        nanobind::converter::registry::push_back(
                &convertible,
                &construct,
                nanobind::type_id<T>()
#ifndef BOOST_PYTHON_NO_PY_SIGNATURE
                , &nanobind::converter::expected_from_python_type<T>::get_pytype
#endif
            );

    }

    static void* convertible(PyObject* obj)
    {
        return Converter::convertible(obj);
    }

    static void construct(
            PyObject* obj,
            nanobind::converter::rvalue_from_python_stage1_data* data)
    {
        typedef nanobind::converter::rvalue_from_python_storage<T>
            TypedStorage;

        void* storage = reinterpret_cast<TypedStorage*>(data)->storage.bytes;

        Converter::construct(obj, storage);

        data->convertible = storage;
    }
};
#endif

} // namespace Python
} // namespace MLDB
