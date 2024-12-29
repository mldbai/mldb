/* mldb_python_converters.h                                             -*- C++ -*-
   Sunil Rottoo, 25 March 2015
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "mldb/sql/cell_value.h"
#include "mldb/types/path.h"
#include "python_converters.h"
#include "from_python_converter.h"
#include "callback.h"
#include "mldb/http/http_header.h"
#include "mldb/types/dtoa.h"
#include "mldb/types/annotated_exception.h"
#include "python_converters.h"


namespace MLDB {

#if 0
struct RestParamsConverter
{
    typedef nanobind::list PyList;    
    typedef std::pair<std::string, std::string> RestParamType;

    /** PyList ?-> RestParams */
    static void* convertible(PyObject* obj)
    {
        return Python::VectorConverter<RestParamType>::convertible(obj);
    }

    /** PyList -> RestParams */
    static void construct(PyObject* obj, void* storage)
    {
        //std::cout << "vector construct" << std::endl;
        PyList pyList = nanobind::cast<PyList>(obj)();
        RestParams* vec =  new (storage) RestParams();

        nanobind::ssize_t n = nanobind::len(pyList);
        vec->reserve(n);
        for (nanobind::ssize_t i = 0; i < n; ++i)
        {
            nanobind::cast<std::string> strExtract1(pyList[i][0]);
            nanobind::cast<std::string> strExtract2(pyList[i][1]);
            // can we extract it as a std::string?
            if(strExtract2.check())
            {
                vec->push_back(std::make_pair(strExtract1(), strExtract2()));
            }
            else
            {
                nanobind::cast<long> intExtract(pyList[i][1]);
                nanobind::cast<double> floatExtract(pyList[i][1]);
                nanobind::cast<Json::Value> jsonExtract(pyList[i][1]);
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
                    throw AnnotatedException(400,
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
        if (!PyBytes_Check(obj_ptr) && !PyUnicode_Check(obj_ptr)
            && !PyLong_Check(obj_ptr) && !PyFloat_Check(obj_ptr)) {
            return 0;
        }
        return obj_ptr;
    }
    static void construct(PyObject* obj_ptr, void* storage)
    {
        if (PyLong_Check(obj_ptr)) {
            int val = nanobind::cast<long>(obj_ptr);
            new (storage) CellValue(val);
        }
        else if (PyFloat_Check(obj_ptr)) {
            double val = nanobind::cast<double>(obj_ptr);
            new (storage) CellValue(val);
        }
        else if (PyUnicode_Check(obj_ptr)) {
            obj_ptr = PyUnicode_AsUTF8String(obj_ptr);
            ExcCheck(obj_ptr!=nullptr,
                     "Error converting unicode");
            std::string val = nanobind::cast<std::string>(obj_ptr);
            new (storage) CellValue(Utf8String(val));
        }
        else if (PyBytes_Check(obj_ptr)) {
            std::string val = nanobind::cast<std::string>(obj_ptr);
            new (storage) CellValue(CellValue::blob(std::move(val)));
        }
        else {
            throw MLDB::Exception("Unsupported value type for CellValue converter");
        }
    }
};


struct PathConverter
{
    typedef nanobind::list PyList;

    static void* convertible(PyObject* obj_ptr)
    {
        // check if it's a basic type, which would be a path with
        // a single element
        if (!PyBytes_Check(obj_ptr) && !PyUnicode_Check(obj_ptr)
            && !PyLong_Check(obj_ptr) && !PyFloat_Check(obj_ptr)) {

            // if not check if it's a structured path, that should
            // be represented as an array
            nanobind::cast<PyList> listExtract(obj_ptr);
            if (!listExtract.check()) return 0;
        }
        return obj_ptr;
    }

    static void construct(PyObject* obj_ptr, void* storage)
    {
        auto pyToPathElement = [] (PyObject* ptr)
        {
            if (PyLong_Check(ptr)) {
                int val = nanobind::cast<long>(ptr);
                return PathElement(std::to_string(val));
            }
            else if (PyFloat_Check(ptr)) {
                double val = nanobind::cast<double>(ptr);
                return PathElement(std::to_string(val));
            }
            else if (PyUnicode_Check(ptr)) {
                ptr = PyUnicode_AsUTF8String(ptr);
                ExcCheck(ptr!=nullptr,
                         "Error converting unicode");
                std::string val = nanobind::cast<std::string>(ptr);
                return PathElement(std::move(val));
            }

            throw MLDB::Exception("Unsupported value type for Path converter");
        };

        nanobind::cast<PyList> listExtract(obj_ptr);
        // if it's a number of string, return a single element path
        if (!listExtract.check()) {
            new (storage) Path(pyToPathElement(obj_ptr));
            return;
        }

        PyList pyList = nanobind::cast<PyList>(obj_ptr)();
        nanobind::ssize_t n = nanobind::len(pyList);

        std::vector<PathElement> el_container;
        for (nanobind::ssize_t i = 0; i < n; ++i){
            el_container.emplace_back(
                        pyToPathElement(nanobind::object(pyList[i]).ptr()));
        }

        new (storage) Path(el_container.begin(), el_container.end());
    }
};
#endif

} // namespace MLDB


