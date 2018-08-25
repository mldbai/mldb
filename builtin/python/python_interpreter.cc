/** python_interpreter.cc
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    
    Plugin loader for Python plugins.
*/

#include <unistd.h>
#include <fcntl.h>
#include <signal.h>

// Python includes aren't ready for c++17 which doesn't support register
#define register 
#include <Python.h>
#undef register

#include "python_interpreter.h"
#include "mldb/base/scope.h"
#include "mldb/base/exc_assert.h"
#include "mldb/types/annotated_exception.h"

#include <thread>
#include <iostream>
#include <mutex>


using namespace std;

namespace MLDB {

struct AcquireGilToken {
};

struct ReleaseGilToken {
};

struct GilAlreadyHeldToken {
};

struct EnterThreadToken {
};

namespace {

// These are so we have something non-null to take the address of
//static AcquireGilToken ACQUIRE_GIL_TOKEN;
//static ReleaseGilToken RELEASE_GIL_TOKEN;
static GilAlreadyHeldToken GIL_ALREADY_HELD_TOKEN;
//static EnterThreadToken ENTER_THREAD_TOKEN;
static thread_local int gilAlreadyHeldCount = 0;

void gilRelease(AcquireGilToken *)
{
    PyEval_ReleaseLock();
}

void noGilRelease(AcquireGilToken *)
{
}

void gilReacquire(ReleaseGilToken * token)
{
    PyThreadState * st = reinterpret_cast<PyThreadState *>(token);
    //cerr << "reacquiring thread state " << st << endl;
    PyEval_AcquireThread(st);
}

void finishAssertGilHeld(GilAlreadyHeldToken *)
{
    ExcAssertGreater(gilAlreadyHeldCount, 0);
    gilAlreadyHeldCount -= 1;
}

} // file scope

#if 0

std::shared_ptr<AcquireGilToken> acquireGil()
{
    if (gilAlreadyHeldCount > 0) {
        return std::shared_ptr<AcquireGilToken>(&ACQUIRE_GIL_TOKEN,
                                                &noGilRelease);
    }

    PyEval_AcquireLock();
    return std::shared_ptr<AcquireGilToken>(&ACQUIRE_GIL_TOKEN, &gilRelease);
}

#endif


#if 1

std::shared_ptr<ReleaseGilToken> releaseGil()
{
    // Once we leave the GIL, there is no guarantee that other things
    // will leave our thread state in place.  So in addition to releasing
    // the GIL, we need to ensure we get back the right thread state.
    // The PyEval_{Release,Acquire}Thread functions are designed for
    // exactly this use.
    
    PyThreadState* threadState = PyThreadState_Get();

    //cerr << "releasing thread state " << threadState << endl;
    
    PyEval_ReleaseThread(threadState);

    return std::shared_ptr<ReleaseGilToken>
        (reinterpret_cast<ReleaseGilToken *>(threadState),
         &gilReacquire);
}

#else

std::shared_ptr<ReleaseGilToken> releaseGil()
{
    PyEval_ReleaseLock();
    PyThreadState * oldState = PyThreadState_Swap(nullptr);

    auto reacquire = [] (ReleaseGilToken * token)
        {
            PyThreadState * st = reinterpret_cast<PyThreadState *>(token);
            PyThreadState_Swap(st);
            PyEval_AcquireLock();
        };

    return std::shared_ptr<ReleaseGilToken>
        (reinterpret_cast<ReleaseGilToken *>(oldState),
         std::move(reacquire));
}

#endif


std::shared_ptr<GilAlreadyHeldToken>
assertGilAlreadyHeld()
{
    gilAlreadyHeldCount += 1;
    return std::shared_ptr<GilAlreadyHeldToken>
        (&GIL_ALREADY_HELD_TOKEN, &finishAssertGilHeld);
}


/*****************************************************************************/
/* PYTHON THREAD                                                             */
/*****************************************************************************/

PythonThread::
PythonThread()
{
}

PythonThread::
PythonThread(PyThreadState * st, bool manageThreadLifetime)
{
    init(st, manageThreadLifetime);
}

PythonThread::
~PythonThread()
{
    // Shared pointer destructor will free the thread
}

void
PythonThread::
freeThread(PyThreadState * st)
{
    //cerr << "enter to free thread " << st << endl;
    auto enterMainThreadGuard
        = PythonInterpreter::mainInterpreter().mainThread().enter();
    PyThreadState_Clear(st);
    PyThreadState_Delete(st);
    //cerr << "exit from free thread " << st << endl;
}

void
PythonThread::
dontFreeThread(PyThreadState * st)
{
}

void
PythonThread::
init(PyThreadState * st, bool manageThreadLifetime)
{
    //cerr << "initialize thread with state " << st << " manage "
    //     << manageThreadLifetime << endl;
    ExcAssert(!st_.get());
    st_.reset(st,
              manageThreadLifetime
              ? &PythonThread::freeThread
              : &PythonThread::dontFreeThread);
}

void
PythonThread::
destroy()
{
    st_.reset();
}

std::shared_ptr<EnterThreadToken>
PythonThread::
enter() const
{
    //cerr << "entering thread " << st_.get()
        //<< " with state " << PyThreadState_Get()
    //     << endl;

    if (gilAlreadyHeldCount > 0) {
        // We already are in a thread, so we don't actually do any
        // acquiring.

        // Assert that it's true
        PyThreadState_Get();

        // Do nothing
        return std::shared_ptr<EnterThreadToken>
            (nullptr, [] (EnterThreadToken *) {});
    }

    PyEval_AcquireThread(st_.get());
    return std::shared_ptr<EnterThreadToken>
        (reinterpret_cast<EnterThreadToken *>(st_.get()),
         &PythonThread::exitThread);
}

void
PythonThread::
exitThread(EnterThreadToken * token)
{
    //cerr << "exiting thread " << token << " with state "
    //     << PyThreadState_Get() << endl;
    PyThreadState * st = reinterpret_cast<PyThreadState *>(token);
    if (PyThreadState_Swap(st) != st) {
        cerr << "warning: got unexpected thread state " << st << endl;
    }
    PyEval_ReleaseThread(st);
}

// NOTE: copied from exec.cpp in Boost, available under the Boost license
// Copyright Stefan Seefeld 2005 - http://www.boost.org/LICENSE_1_0.txt
boost::python::object
PythonThread::
exec(const EnterThreadToken & threadToken,
     const Utf8String & code,
     const Utf8String & filename,
     boost::python::object global,
     boost::python::object local)
{
    // We create a pipe so that we don't need to open a temporary
    // file.  Unfortunately Python only allows us to pass a filename
    // for error messages if we have a FILE *, so we need to arrange
    // to have one.  The easiest way is to create an fd on a pipe,
    // and then to use fopenfd() to open it.

    int fds[2];
    int res = pipe2(fds, 0 /* flags */);

    if (res == -1)
        throw AnnotatedException(500, "Python evaluation pipe: "
                                  + string(strerror(errno)));
    
    Scope_Exit(if (fds[0] != -1) ::close(fds[0]); if (fds[1] != -1) ::close(fds[1]));

    res = fcntl(fds[1], F_SETFL, O_NONBLOCK);
    if (res == -1) {
        auto errno2 = errno;
        throw AnnotatedException(500, "Python evaluation fcntl: "
                                  + string(strerror(errno2)));
    }

    std::atomic<int> finished(0);
    std::thread t;

    // Write as much as we can.  In Linux, the default pipe buffer is
    // 64k, which is enough for most scripts.
    ssize_t written = write(fds[1], code.rawData(), code.rawLength());
    
    if (written == -1) {
        // Error writing.  Bail out.
        throw AnnotatedException
            (500, "Error writing to pipe for python evaluation: "
             + string(strerror(errno)));
    }
    else if (written == code.rawLength()) {
        // We wrote the whole script to the pipe.  We can close the
        // write end of the file.
        ::close(fds[1]);
        fds[1] = -1;
    }
    else {
        // We weren't able to write the whole thing to the pipe.
        // We need to set up a thread to push more in once the
        // Python part has read some.

        // Turn off non-blocking.  We don't want to busy loop.  The
        // thread won't deadlock, since the FD will be closed if
        // we need to exit before all of the code is written.
        res = fcntl(fds[1], F_SETFL, 0);
        if (res == -1) {
            auto errno2 = errno;
            throw AnnotatedException(500, "Python evaluation fcntl: "
                                      + string(strerror(errno2)));
        }

        // Set up a thread to continue writing code to the pipe until
        // all of the code has been passed to the function
        t = std::thread([&] ()
            {
                while (!finished) {
                    ssize_t done = write(fds[1],
                                         code.rawData() + written,
                                         code.rawLength() - written);
                    if (finished)
                        return;
                    if (done == -1) {
                        if (errno == EAGAIN || errno == EINTR) {
                            continue;
                        }
                        cerr << "Error writing to Python source pipe: "
                             << strerror(errno) << endl;
                        std::terminate();
                    }
                    else {
                        written += done;
                        if (written == code.rawLength()) {
                            close(fds[1]);
                            fds[1] = -1;
                            return;
                        }
                    }
                }
            });
    }

    // Finally, we have our fd.  Turn it into a FILE * that Python wants.
    FILE * file = fdopen(fds[0], "r");

    if (!file) {
        throw AnnotatedException
            (500, "Error creating fd for python evaluation: "
             + string(strerror(errno)));
    }
    Scope_Exit(::fclose(file));
    fds[0] = -1;  // stop the fd guard from closing it, since now we have a guard

    using namespace boost::python;
    // From here on in is copied from the Boost version
    // Set suitable default values for global and local dicts.
    object none;
    if (global.ptr() == none.ptr()) {
        if (PyObject *g = PyEval_GetGlobals())
            global = object(boost::python::detail::borrowed_reference(g));
        else
            global = dict();
    }
    if (local.ptr() == none.ptr()) local = global;

    // should be 'char const *' but older python versions don't use 'const' yet.
    PyObject* result = PyRun_File(file, filename.rawData(), Py_file_input,
                                  global.ptr(), local.ptr());

    // Clean up no matter what (make sure our writing thread exits).  If it
    // was blocked on writing, the closing of the fd will unblock it.
    finished = 1;
    if (fds[1] != -1) {
        ::close(fds[1]);
        fds[1] = -1;
    }
    if (t.joinable())
        t.join();

    if (!result) throw_error_already_set();
    return boost::python::object(boost::python::detail::new_reference(result));
}


/*****************************************************************************/
/* PYTHON INITIALIZERS                                                       */
/*****************************************************************************/

namespace {

std::vector<std::function<void (const EnterThreadToken &)> >
pythonInitializers;
size_t initializersDone = 0;
std::recursive_mutex initializersMutex;

bool hasInitializersToRun()
{
    std::unique_lock<std::recursive_mutex> guard(initializersMutex);
    return initializersDone < pythonInitializers.size();
}

void runPythonInitializers(const EnterThreadToken & thread)
{
    std::unique_lock<std::recursive_mutex> guard(initializersMutex);

    while (initializersDone < pythonInitializers.size()) {
        if (pythonInitializers[initializersDone])
            pythonInitializers[initializersDone](thread);
        ++initializersDone;
    }
}

} // file scope

std::shared_ptr<void>
registerPythonInitializer(std::function<void (const EnterThreadToken &)>
                          initializer)
{
    std::unique_lock<std::recursive_mutex> guard(initializersMutex);
    int initializerNum = pythonInitializers.size();
    pythonInitializers.emplace_back(std::move(initializer));
    
    auto clearInitializer = [initializerNum] (void *)
        {
            std::unique_lock<std::recursive_mutex> guard(initializersMutex);
            pythonInitializers[initializerNum] = nullptr;
        };

    // Something not null to return in the token
    static const char * NOT_NULL = "hello";

    return std::shared_ptr<void>((void *)NOT_NULL, clearInitializer);
}


/****************************************************************************/
/* PYTHON INTERPRETER                                                       */
/****************************************************************************/

namespace {

bool mldbIsAModule = false;
std::atomic<bool> mainInterpreterInitialized(false);
PyThreadState * moduleMainThreadState = nullptr; // for a module

} // file scope

void
PythonInterpreter::
initializeFromModuleInit()
{
    if (mainInterpreterInitialized) {
        cerr << "ERROR: MLDB Python initialized before being told it's a module"
             << endl;
        abort();
    }
    //cerr << "we're being called from a module" << endl;

    mldbIsAModule = true;
    moduleMainThreadState = PyThreadState_Swap(nullptr);
    PyThreadState_Swap(moduleMainThreadState);

    auto token = assertGilAlreadyHeld();
    mainInterpreter();
}

bool
PythonInterpreter::
isAModule()
{
    return mldbIsAModule;
}

PythonInterpreter &
PythonInterpreter::
mainInterpreter()
{
    static PythonInterpreter result(CREATE_MAIN);

    mainInterpreterInitialized = true;

    if (hasInitializersToRun()) {
        //cerr << "enter for main initializers" << endl;
        auto enterGuard = result.mainThread().enter();
        runPythonInitializers(*enterGuard);
        //cerr << "exit from main initializers" << endl;
    }

    return result;
}

namespace {

PyThreadState * getNewInterpreter()
{
    auto result = Py_NewInterpreter();
    if (!result) {
        cerr << "bad alloc in Py_NewInterpreter()" << endl;
        throw std::bad_alloc();
    }
    return result;
}

} // file scope

PythonInterpreter::
PythonInterpreter()
    : PythonInterpreter(CREATE_SUB)
{
}

PythonInterpreter::
PythonInterpreter(InitializationContext context)
{
    PyThreadState * st = nullptr;

    //cerr << "creating PythonInterpreter at " << this << endl;

    bool isModule = mldbIsAModule;
    
    if (context == CREATE_MAIN) {
        ExcAssert(!mainInterpreterInitialized);

        //cerr << "main interpreter init" << endl;

        std::function<void (PyThreadState * st)> finalize;

        if (isModule) {
            // Saved from module initialization
            st = moduleMainThreadState;
            
            // No finalization; the calling process takes care of it
            finalize = [] (PyThreadState * st)
                {
                };
        }
        else {
            Py_Initialize();
            PyEval_InitThreads();
            st = PyEval_SaveThread();

            // Undo the sigint handler that the Python initialization puts
            // in place
            signal(SIGINT, SIG_DFL);

            // Clean up the Python interpreter on finalization
            finalize = [] (PyThreadState * st)
                {
                    PyThreadState_Swap(st);
                    Py_Finalize();
                };
        }
        
        this->interpState.reset(st, finalize);
        mainThread_.init(st, false /* don't manage lifecycle */);
    }
    else {
        static std::mutex mutex;
        std::unique_lock<std::mutex> guard(mutex);

        {
            //cerr << "enter for extra initializers" << endl;
            auto enterGuard
                = PythonInterpreter::mainInterpreter().mainThread().enter();
            runPythonInitializers(*enterGuard);
            st = getNewInterpreter();
            //cerr << "exit from extra initializers" << endl;
        }
    

        //cerr << "sub interpreter init " << st->interp << endl;

        auto endSubInterpreter = [] (PyThreadState * interp) {
            //auto enterMainThreadGuard = mainInterpreter().mainThread().enter();
            //cerr << "sub interpreter destroy " << interp << endl;
            
            //auto oldState = PyThreadState_Swap(interp);
            //auto gilGuard = acquireGil();

            //Py_EndInterpreter(interp);

            //PyThreadState_Swap(oldState);
        };
    
        this->interpState.reset(st, endSubInterpreter);
        //mainThread_.init(PyThreadState_New(interpState->interp),
        //                 true /* do manage lifecycle */);
        // If uncommented, the thread state needs to be checked for alloc
        // failure
        mainThread_.init(st, false /* don't manage lifecycle */);
        
        // Now, set up the new interpreter
        //cerr << "enter for modules" << endl;
        auto enterGuard = mainThread().enter();
        main_module = boost::python::import("__main__");
        main_namespace = main_module.attr("__dict__");
    }
}

PythonInterpreter::
~PythonInterpreter()
{
    //cerr << "destructor of PythonInterpreter at " << this << endl;
}

void
PythonInterpreter::
destroy()
{
    //cerr << "destroying PythonInterpreter at " << this << endl;
    {
        //cerr << "enter for interpreter destruction" << endl;
        auto enterGuard = mainThread().enter();
        main_module = boost::python::object();
        main_namespace = boost::python::object();
        //cerr << "exit from interpreter destruction" << endl;
    }
    mainThread_.destroy();
    interpState.reset();
}

const PythonThread &
PythonInterpreter::
mainThread()
{
    return mainThread_;
}

PythonThread
PythonInterpreter::
newThread()
{
    PyThreadState * result = PyThreadState_New(interpState->interp);
    if (!result) {
        cerr << "bad alloc in PyThreadState_New()" << endl;
        throw std::bad_alloc();
    }
    return PythonThread(result, true /* manage lifecycle */);
}

} // namespace MLDB
