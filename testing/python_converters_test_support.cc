// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** python_converters_test_support.cc                                 -*- C++ -*-
    Francois Maillet, 10 mars 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/


#include <Python.h>
#include "mldb/plugins/lang/python/pointer_fix.h" //must come before boost python
#include <boost/python.hpp>

#include <iostream>
#include "datetime.h"
#include "mldb/plugins/lang/python/from_python_converter.h"
#include "mldb/plugins/lang/python/python_converters.h"
#include "mldb/plugins/lang/python/callback.h"

using namespace std;
using namespace boost::python;

using namespace MLDB::Python;
using namespace MLDB;

namespace bp = boost::python;


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

BOOST_PYTHON_MODULE(py_conv_test_module) {

    PyDateTime_IMPORT;

    to_python_converter< std::vector<std::string>, VectorConverter<std::string> >();
    from_python_converter< std::vector<std::string>, VectorConverter<std::string> >();
    
    to_python_converter< Date, DateToPython >();


    from_python_converter< Date, DateFromPython >();
    from_python_converter< string, StringFromPyUnicode >();

    from_python_converter< std::pair<string, int>, PairConverter<string, int> >();
    to_python_converter< std::pair<string, int>, PairConverter<string, int> >();

    from_python_converter< Json::Value, JsonValueConverter> ();
    to_python_converter< Json::Value, JsonValueConverter> ();

    class_<Tester, std::shared_ptr<Tester>>
        ("Tester", init<>())
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
