// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** python_cell_converter_test_support.cc                                 -*- C++ -*-
    Sunil Rottoo, 26 mars 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/


// Python includes aren't ready for c++17 which doesn't support register
#define register 
#define BOOST_BIND_GLOBAL_PLACEHOLDERS

#include <Python.h>
#include "nanobind/nanobind.h"

#include <iostream>
#include "mldb/builtin/python/from_python_converter.h"
#include "mldb/builtin/python/python_converters.h"
#include "mldb/builtin/python/callback.h"
#include "mldb/sql/cell_value.h"
#include "mldb/builtin/python/mldb_python_converters.h"
#include "mldb/builtin/python/python_interpreter.h"

using namespace std;
using namespace nanobind;

using namespace MLDB::Python;
using namespace MLDB;
namespace nb = nanobind;

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

NB_MODULE(py_cell_conv_test_module, m) {

    PythonInterpreter::initializeFromModuleInit();

#if 0
    from_python_converter<CellValue, CellValueConverter>();

    from_python_converter< RestParams, RestParamsConverter>();
    nanobind::to_python_converter< RestParams, RestParamsConverter>();
#endif

    class_<Tester>(m, "Tester")
        .def("cellValueToCpp",&Tester::gotToCpp<CellValue>)
        .def("getRestParams",&Tester::getAndReturn<RestParams>)
        .def("getRestParamsFromCpp", &Tester::getRestParamsFromCpp)
       ;
}
