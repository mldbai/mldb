/** python_plugin_context.h                                        -*- C++ -*-
    Francois Maillet, 6 mars 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <memory>
#include <boost/python.hpp>
#include "mldb/types/string.h"
#include "mldb/compiler/compiler.h"

namespace MLDB {

// This is the main Python thread, which is created for us on
// initialization of Python.  It's important for two reasons:
// a) Creating a new sub-interpreter needs to be done with this
//    thread current; although the documentation for PyInterpreter_New
//    says you can call it without a thread state, you must have the
//    GIL locked and you can only lock the GIL with a thread state...
//    catch 22.  This is the thread state that enables us to manage
//    the situation.
// b) Cleanup of MLDB is done within this thread.
//extern PyThreadState * mainThreadState;

#if 0

struct AcquireGilToken;

/** Acquire the GIL, and return a token that will release the GIL again
    once it goes out of scope.
*/
std::shared_ptr<AcquireGilToken> acquireGil() MLDB_WARN_UNUSED_RESULT;
#endif

struct ReleaseGilToken;

/** Release the GIL temporarily, and return a token that will re-acquire
    the GIL again once it goes out of scope.  Used to wrap calls that
    don't use Python to allow other Python threads to run while the
    calculation happens.
*/
std::shared_ptr<ReleaseGilToken> releaseGil() MLDB_WARN_UNUSED_RESULT;

struct GilAlreadyHeldToken;

/** Assert that the GIL is already held by this thread (for example,
    because we are in a function that is only ever called back from
    the Python interpreter).  This will turn acquireGil into a no-op.

    This is necessary for re-entrancy of code that expects to acquire
    the GIL from a context where the GIL is already held.
*/

std::shared_ptr<GilAlreadyHeldToken>
assertGilAlreadyHeld() MLDB_WARN_UNUSED_RESULT;



/*****************************************************************************/
/* PYTHON THREAD                                                             */
/*****************************************************************************/

struct EnterThreadToken;  // opaque

struct PythonThread {
    ~PythonThread();

    // Destroy the interpreter's state at a moment just before the
    // destructor that we know the GIL isn't held.  This will allow
    // cleanup that acquires the GIL to run.
    void destroy();

    // Enter into the thread, returning a token that proves that we
    // are within it.  This will also acquire the GIL, which must not
    // already be held.  This is the ONLY way that the GIL should be
    // acquired.
    std::shared_ptr<EnterThreadToken> enter() const MLDB_WARN_UNUSED_RESULT;

    /** Execute the given code within this thread.  Note that the
        token must come from a previous call to enter().

        \param code     The Python code to evaluate
        \param filename The filename that the code comes from.  This is used
                        to provide informative error messages.
        \param global   The global object to be used; the default is the
                        globals from the interpreter owning the thread
        \param local    The locals to be defined; the default is empty.

        See the Python documentation for PyRun_SimpleFile for more details.

        The thead must be entered; the threadToken proves that it is.

        The GIL must be held; the gilToken proves that it is.
    */
    static boost::python::object
    exec(const EnterThreadToken & threadToken,
         const Utf8String & code,
         const Utf8String & filename,
         boost::python::object global = boost::python::object(),
         boost::python::object local = boost::python::object());
    
private:
    std::shared_ptr<PyThreadState> st_;
    PythonThread();
    PythonThread(PyThreadState * st, bool manageThreadLifetime);
    void init(PyThreadState * st, bool manageThreadLifetime);
    static void exitThread(EnterThreadToken * tok);
    static void freeThread(PyThreadState * st);
    static void dontFreeThread(PyThreadState * st);
    friend class PythonInterpreter;
};


/****************************************************************************/
/* PYTHON INTERPRETER                                                       */
/****************************************************************************/

/** In the MLDB Python isolation model, we do the following:
    - For each script or plugin, a new Python subinterpreter is created.
      This allows for the scripts and plugins to each be independent of
      each other and avoid polluting each others' namespaces.  The main
      (default) Python interpreter is only used to create new subinterpreters
      and for initialization and finalization; no actual Python code is
      run within it.
    - The main thread of the subinterpreter is used for initialization and
      finalization, as well as for running the script (in the case of
      a script).
    - For each REST request that is handled by a plugin, a new thread is
      created within the plugin's subinterpreter to handle it.

    The PythonInterpreter class encapsulates the ownership of the
    subinterpreter.
*/

struct PythonInterpreter {

    PythonInterpreter();

    // Destroy the interpreter.  Requires that either
    // a) the GIL not be held, or
    // b) a call to destroy() with the GIL not held be made immediately
    //    before calling this method.
    ~PythonInterpreter();

    // Access the main Python interpreter.  This is a process-global
    // object.  This interpreter should only be used for setup,
    // finalization and creation of new subinterpreters.
    //
    // NOTE: this method MAY NOT BE CALLED from static initializers in
    // shared libraries.  This is because those shared libraries need
    // to function in two different contexts:
    // 1) As a part of MLDB, where the Python runtime is embedded
    //    into MLDB.  In that case, calling this function for the first
    //    time will actually create the Python interpreter
    // 2) As a Python module incorporating MLDB, where the Python runtime
    //    is external to MLDB.  When this happens, the shared libraries
    //    for modules are loaded with the Python interpreter in a strange
    //    state; the libraries may be loaded, but they can't do anything
    //    involving the runtime as part of their static initialization.  That
    //    needs to be done as part of the module initialization.
    
    static PythonInterpreter & mainInterpreter();

    // If we're loading MLDB from a Python module, MLDB does not own an
    // embedded Python runtime.  Instead, it needs to hook into the
    // Python runtime from which it's being loaded.
    //
    // This function needs to be called first thing from the module
    // initialization of any module using MLDB.  It causes MLDB to hook
    // into the Python runtime that's calling the module, rather than
    // try to create one of its own.
    //
    // This function must be called BEFORE the first call to mainInterpreter().

    static void initializeFromModuleInit();

    // Are we running as a module imported from Python (true), or as an
    // executable that embeds python (false)?

    static bool isAModule();
    
    // Access the main thread for the subinterpreter
    const PythonThread & mainThread();

    // Create a new thread under the subinterpreter
    PythonThread newThread();

    // Destroy the interpreter's state at a moment just before the
    // destructor that we know the GIL isn't held.
    void destroy();
    
private:
    enum InitializationContext {
        CREATE_MAIN,
        CREATE_SUB
    };
    
    // threadState must be nullptr only on the first call, in which case
    // the main interpreter is initialized.
    PythonInterpreter(InitializationContext context);

    std::shared_ptr<PyThreadState> interpState;
    PythonThread mainThread_;

public:    
    boost::python::object main_module;
    boost::python::object main_namespace;
};

// Cause the initializer function to be run in the context of the
// main Python thread and with the GIL held as soon as Python is
// sufficiently set up to do so.
//
// This is the alternative to static initialization, which doesn't
// work when MLDB is loaded as a Python module.
//
// The returned handle will unregister the initializer when it goes
// out of scope.

std::shared_ptr<void>
registerPythonInitializer(std::function<void (const EnterThreadToken &)>
                          initializer);

// Object version of the above, for ease of static initialization.

struct RegisterPythonInitializer {
    RegisterPythonInitializer(std::function<void (const EnterThreadToken &)>
                              initializer)
        : handle(registerPythonInitializer(std::move(initializer)))
    {
    }

    std::shared_ptr<void> handle;
};

} // namespace MLDB
