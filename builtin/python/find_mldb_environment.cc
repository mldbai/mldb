/** find_mldb_environment.cc
    Jeremy Barnes, 26 August 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Functionality to find, from a Python interpreter, the active MLDB
    environment and make it available to the rest of the Python machinery.

    In the case that we're importing MLDB from a Python program that runs
    under MLDB, then the environment is MLDB itself.

    In the case that we're importing MLDB directly from Python itself (by
    running a script directly), then we need to create the environment, and
    eventually hook it up to the inbuilt Python functionality for things
    like making network connections, etc.

    The goal is to make all Python code be the same no matter what the
    environment it runs in, and have this code do the work of handling the
    different environments.
*/

// Python includes aren't ready for c++17 which doesn't support register
#define register 

#include <Python.h>
#include "mldb/builtin/python/pointer_fix.h" //must come before boost python
#include <boost/python.hpp>

#include "mldb/builtin/python/python_plugin_context.h"

using namespace MLDB;

BOOST_PYTHON_MODULE(_mldb)
{
    boost::python::def("find_mldb_environment", MldbPythonInterpreter::findEnvironment);
}

