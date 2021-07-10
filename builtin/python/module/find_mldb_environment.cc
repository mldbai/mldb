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
#define BOOST_BIND_GLOBAL_PLACEHOLDERS

#include <Python.h>
#include "mldb/builtin/python/pointer_fix.h" //must come before boost python
#include <boost/python.hpp>
#include "mldb/compiler/compiler.h"
#include <dlfcn.h>

#include "mldb/builtin/python/python_plugin_context.h"

using namespace std;
using namespace MLDB;

namespace MLDB {

/** MLDB environment search functions.
    
    This module exports a single python-callable function: findEnvironment().
    That function is responsible for finding or creating an MLDB environment
    for the currently active Python interpreter; it will always be called
    with the GIL held.  There are two main contexts in which it must work:
    firstly, when called from the mldb_runner executable (a standalone MLDB
    environment) that has an embedded Python runtime, and secondly when called
    from the Python executable that has imported MLDB and therefore needs an
    embedded MLDB runtime.  In other words, in one situation MLDB embeds Python
    and in the other situation Python embeds MLDB, but we want the two to
    appear identical to the Python code.

    That will call the findEnvironmentImpl() function.  If MLDB is already
    in the current process, then the weak version defined here (which always
    finds no environment) will be overridden with a stronger one from
    mldb_python_context.cc, which will look for the MldbPythonInterpreter
    that owns the current Python interpreter and return that.  If MLDB is
    not already in process, then findEnvironmentImpl() will return a null
    pointer; that is the behaviour of the default (weak) implementation here;
    this should only happen when the executable is python rather than mldb.

    In the case that the findEnvironment() function will perform the following
    steps to enable MLDB to be loaded:

    1.  Dynamic load libmldb_platform_python.so, which contains everything
        necessary to create an MldbEngine instance hooked into the Python.
        This will also load the entire set of MLDB runtime libraries.
    2.  Find the MLDB::getPythonEnvironment() function exported by that module.
    3.  Call that function to create an MldbEngine instance and a
        PythonPluginContext instance, which are then returned to the calling
        function.

    Finally, note that the library must be loaded by Python with RTLD_GLOBAL,
    or otherwise the symbols will be duplicated and typecasting, Any conversion
    and exception handling won't work (all of which MLDB relies on extensively).
*/


// This is defined in the MLDB server
extern std::mutex dlopenMutex;

std::shared_ptr<MldbPythonContext>
findEnvironmentImpl() MLDB_WEAK_FN;

std::shared_ptr<MldbPythonContext>
findEnvironmentImpl()
{
    return nullptr;
}

typedef std::shared_ptr<MldbPythonContext> (*FindEnvironmentFn) ();

/// See above for description.
/// Returns a pair with the function pointer and a handle that will unload the library
/// once released.
static std::pair<FindEnvironmentFn, std::shared_ptr<void> >
loadPythonEnvironment()
{
    std::shared_ptr<void> result;

    std::unique_lock<std::mutex> guard(dlopenMutex);

    string path = "libmldb_platform_python.so";
    
    dlerror();  // clear existing error
    
    void * handle = dlopen(path.c_str(), RTLD_NOW | RTLD_GLOBAL);
    if (!handle) {
        char * error = dlerror();
        throw Exception("Error loading python platform library " + path + ": "
                        + error);
    }

    FindEnvironmentFn fn = (FindEnvironmentFn)dlsym(handle, "_ZN4MLDB20getPythonEnvironmentEv");
    result.reset(handle, dlclose);
    if (!result) {
        throw Exception("Error loading python platform library " + path + ": no symbol "
                        " getPythonEnvironment defined");
    }

    return {fn, result};
}

static std::shared_ptr<MldbPythonContext>
findEnvironment()
{
    auto res = findEnvironmentImpl();
    if (res) {
        return res;
    }

    // We need to dlopen a separate library to create and initialize an MLDB instance that
    // is specialized for Python
    static auto env = loadPythonEnvironment();
    return (*env.first)();
}

} // namespace MLDB

BOOST_PYTHON_MODULE(_mldb)
{
    boost::python::def("find_mldb_environment", findEnvironment);
}

