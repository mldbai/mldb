/** python_interpreter.h                                           -*- C++ -*-
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

/** Thread state/GIL functions

    Please note that there is no way to manage the GIL and the thread
    state separately.  In MLDB, either a function is within a given
    thread state AND holds the GIL, or neither is true.  It's also not
    currently possible to recursively enter a thread state, though that
    would be possible to implement.

    The reason for these strict invariants is that the locking in Python
    is extremely difficult to get right: the GIL must be acquired
    *before* switching thread state, but will throw an assertion if
    there is no thread state active.
*/

struct EnterThreadToken;  // opaque

struct ReleaseGilToken; // opaque

/** Release the GIL temporarily, and return a token that will re-acquire
    the GIL again once it goes out of scope.  Used to wrap calls that
    don't use Python to allow other Python threads to run while the
    calculation happens.
*/
std::shared_ptr<ReleaseGilToken> releaseGil() MLDB_WARN_UNUSED_RESULT;

/** Assert that the GIL is already held by this thread (for example,
    because we are in a function that is only ever called back from
    the Python interpreter).  This will turn acquireGil into a no-op.

    This is necessary for re-entrancy of code that expects to acquire
    the GIL from a context where the GIL is already held.  It's primarily
    used in the destructor of objects that may be garbage collected by
    Python; not using it will cause a deadlock when the destructors
    attempt to acquire the GIL to do their work.

    The other way to get a token is to of course call enter() on a thread.
*/

std::shared_ptr<EnterThreadToken>
assertGilAlreadyHeld() MLDB_WARN_UNUSED_RESULT;


/** A note on object destruction and thread state.
    
    There are several objects defined here that encapsulate Python
    functionality.  Those which require an EnterThreadToken to
    be called must be called from within the context of a thread; the
    parameter enforces that contract.

    The destructors of these objects must all be called *without* a
    thread state active or the GIL held; they will enter into a thread
    state to do their job.  The only exception is for objects with a
    destroy() method; those objects may be destroyed with a thread state
    active without deadlocking if and only if the destroy() method is
    called without a thread state active immediately before the destructor
    is called.

    These rules are complex, but are the only way that it's possible to
    guarantee cleanup in the context of the Python garbage collector
    for objects that may be co-owned by the Python runtime and MLDB.
*/


/*****************************************************************************/
/* PYTHON THREAD                                                             */
/*****************************************************************************/

/** Encapsulation of a Python thread state.  This is the main interface to
    performing work in Python.

    enter() and exec() are the two primary methods.
*/

struct PythonThread {
    ~PythonThread();

    /** Destroy the interpreter's state at a moment just before the
        destructor that we know the GIL isn't held.  This will allow
        cleanup that acquires the GIL to run.
    */
    void destroy();

    /** Enter into the thread, returning a token that proves that we
        are within it.  This will also acquire the GIL, which must not
        already be held.  This is the ONLY way that the GIL should be
        acquired.
    */
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

        This uses a trick to convince Python to provide informative error
        messages including filenames for VFS objects (eg, http:// or s3://).

        It's important to be aware that the code provided is fully trusted
        and can execute arbitrary commands; executing untrusted code can
        enable the provider of the code to have full control over the
        MLDB process.
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

    /** Destroy the interpreter.  Requires that either
        a) the GIL not be held, or
        b) a call to destroy() with the GIL not held be made immediately
           before calling this method.
    */
    ~PythonInterpreter();

    /** Access the main Python interpreter.  This is a process-global
        object.  This interpreter should only be used for setup,
        finalization and creation of new subinterpreters.
    
        NOTE: this method MAY NOT BE CALLED from static initializers in
        shared libraries.  This is because those shared libraries need
        to function in two different contexts:
        1) As a part of MLDB, where the Python runtime is embedded
          into MLDB.  In that case, calling this function for the first
          time will actually create the Python interpreter
        2) As a Python module incorporating MLDB, where the Python runtime
          is external to MLDB.  When this happens, the shared libraries
          for modules are loaded with the Python interpreter in a strange
          state; the libraries may be loaded, but they can't do anything
          involving the runtime as part of their static initialization.  That
          needs to be done as part of the module initialization.
    */
    
    static PythonInterpreter & mainInterpreter();

    /** If we're loading MLDB from a Python module, MLDB does not own an
        embedded Python runtime.  Instead, it needs to hook into the
        Python runtime from which it's being loaded.
    
        This function needs to be called first thing from the module
        initialization of any module using MLDB.  It causes MLDB to hook
        into the Python runtime that's calling the module, rather than
        try to create one of its own.
    
        This function must be called BEFORE the first call to
        mainInterpreter().
    */
    
    static void initializeFromModuleInit();

    /** Are we running as a module imported from Python (true), or as an
        executable that embeds python (false)?  This can affect how much we
        influence the Python environment.  For example, if we're not a
        module we remove the Python SIGINT handler.
    */

    static bool isAModule();
    
    /** Access the main thread for the subinterpreter. */
    const PythonThread & mainThread();

    /** Create a new thread under the subinterpreter. */
    PythonThread newThread();

    /** Destroy the interpreter's state at a moment just before the
        destructor that we know the GIL isn't held.
    */
    void destroy();
    
private:
    enum InitializationContext {
        CREATE_MAIN,   ///< Create the main interpreter
        CREATE_SUB     ///< Create a sub-interpreter
    };
    
    /** Initialize the PythonInterpreter.  It must be first called exactly
        once with CREATE_MAIN, and then may be called as many times as
        necessary with CREATE_SUB.

        There must be no thread context active.
    */
    PythonInterpreter(InitializationContext context);

    std::shared_ptr<PyThreadState> interpState;
    PythonThread mainThread_;
};


/*****************************************************************************/
/* REGISTRATION                                                              */
/*****************************************************************************/

/** Cause the initializer function to be run in the context of the
    main Python thread and with the GIL held as soon as Python is
    sufficiently set up to do so.
    
    This is the alternative to static initialization, which doesn't
    work when MLDB is loaded as a Python module.

    The returned handle will unregister the initializer when it goes
    out of scope, allowing it to be used in the context of plugins that
    may be loaded and unloaded.
*/

std::shared_ptr<void>
registerPythonInitializer(std::function<void (const EnterThreadToken &)>
                          initializer);

/** Object version of the above, for ease of static initialization.

    Usage:

    void myInitializerFunction(const EnterThreadToken & token)
    {
        // ...
    }

    static RegisterPythonInitializer myInit(myInitializerFunction);
*/

struct RegisterPythonInitializer {
    RegisterPythonInitializer(std::function<void (const EnterThreadToken &)>
                              initializer)
        : handle(registerPythonInitializer(std::move(initializer)))
    {
    }

    std::shared_ptr<void> handle;
};

} // namespace MLDB
