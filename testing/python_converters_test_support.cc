/** python_converters_test_support.cc                                 -*- C++ -*-
    Francois Maillet, 10 mars 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/


// Python includes aren't ready for c++17 which doesn't support register
#define register 
#define BOOST_BIND_GLOBAL_PLACEHOLDERS

#include <Python.h>
#include "nanobind/nanobind.h"
#include "nanobind/stl/string.h"
#include "nanobind/stl/vector.h"
#include "nanobind/stl/pair.h"
#include "nanobind/stl/tuple.h"

#include <iostream>
#include "datetime.h"
#include "mldb/builtin/python/from_python_converter.h"
#include "mldb/builtin/python/python_converters.h"
#include "mldb/builtin/python/callback.h"
#include "mldb/builtin/python/python_interpreter.h"

using namespace std;
using namespace nanobind;

using namespace MLDB::Python;
using namespace MLDB;

namespace nb = nanobind;


struct Tester {
    template<typename T>
    T getAndReturn(T val)
    {
        return val;
    }

    template<typename T>
    bool gotToCpp(T val)
    {
        std::cout << val.toStyledString() << std::endl;
        return true;
    }
};

NB_MODULE(py_conv_test_module, m) {

    PythonInterpreter::initializeFromModuleInit();
    
    PyDateTime_IMPORT;

#if 0
    to_python_converter< std::vector<std::string>, VectorConverter<std::string> >();
    from_python_converter< std::vector<std::string>, VectorConverter<std::string> >();
    
    to_python_converter< Date, DateToPython >();


    from_python_converter< Date, DateFromPython >();
    from_python_converter< string, StringFromPyUnicode >();

    from_python_converter< std::pair<string, int>, PairConverter<string, int> >();
    to_python_converter< std::pair<string, int>, PairConverter<string, int> >();

    //from_python_converter< Json::Value, JsonValueConverter> ();
    //to_python_converter< Json::Value, JsonValueConverter> ();
#endif

    class_<Tester>(m, "Tester")
        .def(nanobind::init<>())
        .def("getString", &Tester::getAndReturn<string>)
        .def("getFloat", &Tester::getAndReturn<float>)
        .def("getInt", &Tester::getAndReturn<int>)
        .def("getVectorString", &Tester::getAndReturn<vector<string>>)
        .def("getPairStringInt", &Tester::getAndReturn<std::pair<string, int>>)
        .def("getJsonVal", &Tester::getAndReturn<Json::Value>)
        .def("jsonValToCpp", &Tester::gotToCpp<Json::Value>)
        //.def("__str__", &toString)
       ;
}
