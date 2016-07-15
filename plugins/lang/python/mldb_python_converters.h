// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* mldb_python_converters.h                                             -*- C++ -*-
   Sunil Rottoo, 25 March 2015
   Copyright (c) 2015 Datacratic Inc.  All rights reserved.

   
*/
#include <boost/python.hpp>
#include <boost/python/return_value_policy.hpp>
#include "mldb/sql/cell_value.h"
#include "python_converters.h"
#include "from_python_converter.h"
#include "callback.h"
#include <boost/python/to_python_converter.hpp>
#include "mldb/http/http_header.h"
#include "mldb/types/dtoa.h"
#include "mldb/http/http_exception.h"
#include "python_converters.h"

namespace Datacratic {
namespace MLDB {

struct RestParamsConverter
{
    typedef boost::python::list PyList;    
    typedef std::pair<std::string, std::string> RestParamType;

    /** PyList ?-> RestParams */
    static void* convertible(PyObject* obj)
    {
        return Python::VectorConverter<RestParamType>::convertible(obj);
    }

    /** PyList -> RestParams */
    static void construct(PyObject* obj, void* storage)
    {
        namespace bp = boost::python;

        //std::cout << "vector construct" << std::endl;
        PyList pyList = bp::extract<PyList>(obj)();
        RestParams* vec =  new (storage) RestParams();

        bp::ssize_t n = bp::len(pyList);
        vec->reserve(n);
        for (bp::ssize_t i = 0; i < n; ++i)
        {
            bp::extract<std::string> strExtract1(pyList[i][0]);
            bp::extract<std::string> strExtract2(pyList[i][1]);
            // can we extract it as a std::string?
            if(strExtract2.check())
            {
                vec->push_back(std::make_pair(strExtract1(), strExtract2()));
            }
            else
            {
                bp::extract<int> intExtract(pyList[i][1]);
                bp::extract<double> floatExtract(pyList[i][1]);
                bp::extract<Json::Value> jsonExtract(pyList[i][1]);
                if (intExtract.check() )
                {
                    vec->push_back(make_pair(strExtract1(), to_string(intExtract())));
                }
                else if(floatExtract.check())
                {
                    vec->push_back(make_pair(strExtract1(), dtoa(floatExtract())));
                }
                else if(jsonExtract.check())
                {
                    std::string theStr = jsonExtract().toString();
                    vec->push_back(std::make_pair(strExtract1(), theStr));
                }
                else
                { 
                    throw HttpReturnException(400,
                            ML::format("Exception while parsing REST parameter at position: %d", i));
                }
            }
        }
    }
    /** RestParams -> PyList */
    static PyObject* convert(const RestParams& vec)
    {
        return Python::VectorConverter<RestParamType>::convert(vec);
    }
};

struct CellValueConverter 
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
        if (PyInt_Check(obj_ptr)) {
            int val = boost::python::extract<int>(obj_ptr);
//             cerr << "   recording val as INT: " << val << endl;
            new (storage) CellValue(val);
        }
        else if (PyFloat_Check(obj_ptr)) {
            double val = boost::python::extract<double>(obj_ptr);
//             cerr << "   recording val as DOUBLE: " << val << endl;
            new (storage) CellValue(val);
        }
        else if (PyUnicode_Check(obj_ptr) || PyString_Check(obj_ptr)) {
            if (PyUnicode_Check(obj_ptr)) {
                obj_ptr = PyUnicode_AsUTF8String(obj_ptr);
                ExcCheck(obj_ptr!=nullptr,
                         "Error converting unicode");
            }
            std::string val = boost::python::extract<std::string>(obj_ptr);
            //std::cerr << "   recording val as STR: " << val << std::endl;
            new (storage) CellValue(Utf8String(val));
        } else {
            throw ML::Exception("Unsupported value type for CellValue converter");
        }
    }
};

}// namespace MLDB
} // namespace Datacratic

