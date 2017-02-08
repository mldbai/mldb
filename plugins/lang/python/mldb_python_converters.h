/* mldb_python_converters.h                                             -*- C++ -*-
   Sunil Rottoo, 25 March 2015
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include <boost/python.hpp>
#include <boost/python/return_value_policy.hpp>
#include "mldb/sql/cell_value.h"
#include "mldb/sql/path.h"
#include "python_converters.h"
#include "from_python_converter.h"
#include "callback.h"
#include <boost/python/to_python_converter.hpp>
#include "mldb/http/http_header.h"
#include "mldb/types/dtoa.h"
#include "mldb/http/http_exception.h"
#include "python_converters.h"


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
                            MLDB::format("Exception while parsing REST parameter at position: %d", i));
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
            new (storage) CellValue(val);
        }
        else if (PyFloat_Check(obj_ptr)) {
            double val = boost::python::extract<double>(obj_ptr);
            new (storage) CellValue(val);
        }
        else if (PyUnicode_Check(obj_ptr) || PyString_Check(obj_ptr)) {
            if (PyUnicode_Check(obj_ptr)) {
                obj_ptr = PyUnicode_AsUTF8String(obj_ptr);
                ExcCheck(obj_ptr!=nullptr,
                         "Error converting unicode");
            }
            std::string val = boost::python::extract<std::string>(obj_ptr);
            new (storage) CellValue(Utf8String(val));
        } else {
            throw MLDB::Exception("Unsupported value type for CellValue converter");
        }
    }
};


struct PathConverter
{
    typedef boost::python::list PyList;

    static void* convertible(PyObject* obj_ptr)
    {
        // check if it's a basic type, which would be a path with
        // a single element
        if (!PyString_Check(obj_ptr) && !PyUnicode_Check(obj_ptr)
            && !PyInt_Check(obj_ptr) && !PyFloat_Check(obj_ptr)) {

            // if not check if it's a structured path, that should
            // be represented as an array
            boost::python::extract<PyList> listExtract(obj_ptr);
            if (!listExtract.check()) return 0;
        }
        return obj_ptr;
    }

    static void construct(PyObject* obj_ptr, void* storage)
    {
        auto pyToPathElement = [] (PyObject* ptr)
        {
            if (PyInt_Check(ptr)) {
                int val = boost::python::extract<int>(ptr);
                return PathElement(std::to_string(val));
            }
            else if (PyFloat_Check(ptr)) {
                double val = boost::python::extract<double>(ptr);
                return PathElement(std::to_string(val));
            }
            else if (PyUnicode_Check(ptr) || PyString_Check(ptr)) {
                if (PyUnicode_Check(ptr)) {
                    ptr = PyUnicode_AsUTF8String(ptr);
                    ExcCheck(ptr!=nullptr,
                             "Error converting unicode");
                }
                std::string val = boost::python::extract<std::string>(ptr);
                return PathElement(Utf8String(val));
            }

            throw MLDB::Exception("Unsupported value type for Path converter");
        };

        boost::python::extract<PyList> listExtract(obj_ptr);
        // if it's a number of string, return a single element path
        if (!listExtract.check()) {
            new (storage) Path(pyToPathElement(obj_ptr));
            return;
        }

        PyList pyList = boost::python::extract<PyList>(obj_ptr)();
        boost::python::ssize_t n = boost::python::len(pyList);

        std::vector<PathElement> el_container;
        for (boost::python::ssize_t i = 0; i < n; ++i){
            el_container.emplace_back(
                        pyToPathElement(boost::python::object(pyList[i]).ptr()));
        }

        new (storage) Path(el_container.begin(), el_container.end());
    }
};

} // namespace MLDB


