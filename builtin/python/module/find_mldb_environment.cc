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

#include <Python.h>
#include "nanobind/nanobind.h"
#include "nanobind/stl/shared_ptr.h"
#include "nanobind/stl/string.h"
#include "nanobind/stl/vector.h"
#include "nanobind/stl/pair.h"
#include "nanobind/stl/tuple.h"
#include "nanobind/stl/map.h"
#include "mldb/builtin/python/python_converters.h"
#include <dlfcn.h>

#include "mldb/builtin/python/python_plugin_context.h"
#include "mldb/builtin/python/python_entities.h"

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
    //cerr << "findEnvironmentImpl weak version" << endl;
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

namespace {

#if 0
std::string pyObjectToString(PyObject * pyObj)
{
    return nanobind::str(nanobind::borrow(pyObj)).c_str();
#if 0
    if(PyLong_Check(pyObj)) {
        return MLDB::lexical_cast<std::string>(long(nanobind::cast<long>(pyObj)));
    }
    else if(PyFloat_Check(pyObj)) {
        return MLDB::lexical_cast<std::string>(float(nanobind::cast<float>(pyObj)));
    }
    else if(PyBytes_Check(pyObj)) {
        return nanobind::cast<std::string>(pyObj);
    }
    else if(PyUnicode_Check(pyObj)) {
        PyObject* from_unicode = PyUnicode_AsASCIIString(pyObj);
        std::string tmpStr = nanobind::cast<std::string>(from_unicode);

        // not returned so needs to be garbage collected
        Py_DECREF(from_unicode);

        return tmpStr;
    }

    PyObject* str_obj = PyObject_Str(pyObj);
    std::string str_rep = "<Unable to create str representation of object>";
    if(str_obj) {
        str_rep = nanobind::cast<std::string>(str_obj);
    }
    Py_DECREF(str_obj);
    return str_rep;
#endif
};
#endif

void logArgs(nanobind::args args, nanobind::kwargs kwargs)
{
    if(nanobind::len(args) < 1) {
        return;
    }
    
    Utf8String str_accum; 
    for(int i = 1; i < nanobind::len(args); ++i) {
        if(i > 1) str_accum += " ";
        str_accum += nanobind::str(args[i]).c_str();
    }

    MldbPythonContext* pymldb = nanobind::cast<MldbPythonContext*>(args[0]);
    pymldb->logUnicode(str_accum);
}

} // file scope

NB_MODULE(_mldb, m) {
    namespace nb = nanobind;
    using namespace nb::literals;
    nanobind::class_<MldbPythonContext>(m, "Mldb")
        .def("log", logArgs)
        .def("log", &MldbPythonContext::logUnicode, "message"_a)
        .def("log", &MldbPythonContext::logJsVal, "message"_a)
        .def("perform", &MldbPythonContext::perform,
            "verb"_a, "resource"_a,
             nb::arg("params").none() = RestParams(),
             nb::arg("payload").none() = Json::Value(),
             nb::arg("header").none() = RestParams())
        .def("read_lines", &MldbPythonContext::readLines,
             "path"_a, nb::arg("max_lines") = -1)
        .def("ls", &MldbPythonContext::ls)
        .def("get_http_bound_address", &MldbPythonContext::getHttpBoundAddress)
        .def("get_python_executable", &MldbPythonContext::getPythonExecutable)
        .def("create_dataset", &DatasetPy::createDataset)
        .def("create_procedure", &PythonProcedure::createPythonProcedure)
        .def("create_function", &PythonFunction::createPythonFunction)
        .def_prop_ro("plugin", &MldbPythonContext::getPlugin)
        .def_prop_ro("script", &MldbPythonContext::getScript)
        .def("debugSetPathOptimizationLevel", &MldbPythonContext::setPathOptimizationLevel)
        ;

    nanobind::class_<PythonRestRequest>(m, "RestRequest")
        .def_rw("remaining", &PythonRestRequest::remaining)
        .def_ro("verb", &PythonRestRequest::verb)
        .def_ro("resource", &PythonRestRequest::resource)
        .def_ro("rest_params", &PythonRestRequest::restParams)
        .def_ro("payload", &PythonRestRequest::payload)
        .def_ro("content_type", &PythonRestRequest::contentType)
        .def_ro("content_length", &PythonRestRequest::contentLength)
        .def_ro("headers", &PythonRestRequest::headers)
        .def("set_return", &PythonRestRequest::setReturnValue, nb::arg("return_val").none(), nb::arg("return_code") = 200)
        ;

    nanobind::class_<PythonPluginContext>(m, "Plugin")
        .def_prop_ro("args", &PythonPluginContext::getArgs)
        .def("serve_static_folder", &PythonPluginContext::serveStaticFolder)
        .def("serve_documentation_folder", &PythonPluginContext::serveDocumentationFolder)
        .def("get_plugin_dir", &PythonPluginContext::getPluginDirectory)
        ;

    nanobind::class_<PythonScriptContext>(m, "Script")
        .def_prop_ro("args", &PythonScriptContext::getArgs)
        ;

    m.def("find_mldb_environment", findEnvironment);

    nanobind::class_<DatasetPy>(m, "Dataset")
        .def("record_row", &DatasetPy::recordRow)
        .def("record_rows", &DatasetPy::recordRows)
        .def("record_column", &DatasetPy::recordColumn)
        .def("record_columns", &DatasetPy::recordColumns)
        .def("commit", &DatasetPy::commit);
}

} // namespace MLDB
