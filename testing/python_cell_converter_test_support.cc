// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** python_cell_converter_test_support.cc                                 -*- C++ -*-
    Sunil Rottoo, 26 mars 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/


#include <Python.h>
#include "mldb/plugins/lang/python/pointer_fix.h" //must come before boost python
#include <boost/python.hpp>

#include <iostream>
#include "mldb/plugins/lang/python/from_python_converter.h"
#include "mldb/plugins/lang/python/python_converters.h"
#include "mldb/plugins/lang/python/callback.h"
#include "mldb/sql/cell_value.h"
#include "mldb/plugins/lang/python/mldb_python_converters.h"

using namespace std;
using namespace boost::python;

using namespace MLDB::Python;
using namespace MLDB;
namespace bp = boost::python;

struct Tester {
    RestParams getRestParamsFromCpp()
    {
        RestParams rp;
        rp.push_back(std::make_pair("patate", "pwel"));
        return rp;
    }

    template<typename T>
    T getAndReturn(T val)
    {
        return val;
    }

    template<typename T>
    bool gotToCpp(T val)
    {
//        std::cout << val.toString() << std::endl;
        return true;
    }
};

BOOST_PYTHON_MODULE(py_cell_conv_test_module) {

    from_python_converter<CellValue, CellValueConverter>();

    from_python_converter< RestParams, RestParamsConverter>();
    bp::to_python_converter< RestParams, RestParamsConverter>();

    class_<Tester, std::shared_ptr<Tester>>
        ("Tester", init<>())
        .def("cellValueToCpp",&Tester::gotToCpp<CellValue>)
        .def("getRestParams",&Tester::getAndReturn<RestParams>)
        .def("getRestParamsFromCpp", &Tester::getRestParamsFromCpp)
       ;
}
