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
#include "mldb/types/value_description.h"
#include "mldb/arch/info.h"
#include "mldb/utils/runner.h"
#include "mldb/utils/string_functions.h"

#include <thread>
#include <iostream>
#include <mutex>
#include <atomic>
#include <filesystem>

using namespace std;

namespace MLDB {

struct ReleaseGilToken {
};

struct EnterThreadToken {
};

namespace {

// These are so we have something non-null to take the address of
static EnterThreadToken GIL_ALREADY_HELD_TOKEN;
//static EnterThreadToken ENTER_THREAD_TOKEN;
static thread_local int gilAlreadyHeldCount = 0;

static void gilReacquire(ReleaseGilToken * token)
{
    PyThreadState * st = reinterpret_cast<PyThreadState *>(token);
    //cerr << "reacquiring thread state " << st << endl;
    PyEval_AcquireThread(st);
}

static void finishAssertGilHeld(EnterThreadToken *)
{
    ExcAssertGreater(gilAlreadyHeldCount, 0);
    gilAlreadyHeldCount -= 1;
}

} // file scope

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

std::shared_ptr<EnterThreadToken>
assertGilAlreadyHeld()
{
    gilAlreadyHeldCount += 1;
    return std::shared_ptr<EnterThreadToken>
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
    auto enterMainThreadGuard
        = PythonInterpreter::mainInterpreter().mainThread().enter();
    PyThreadState_Clear(st);
    PyThreadState_Delete(st);
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
    if (gilAlreadyHeldCount > 0) {
        // We already are in a thread, so we don't actually do any
        // acquiring.  We simply swap the thread state to the correct
        // one, which there is no guarantee is already current.

        // Simply swap the state in...
        PyThreadState * oldState = PyThreadState_Swap(st_.get());

        // ... and swap it back out when we exit
        auto recoverOldState = [=] (EnterThreadToken * token)
            {
                PyThreadState_Swap(oldState);
            };
        
        // Do nothing
        return std::shared_ptr<EnterThreadToken>
            (nullptr, std::move(recoverOldState));
    }

    if (_Py_IsFinalizing()) {
        throw MLDB::Exception("Creating PythonThread in finalizing interpreter");
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
    PyThreadState * st = reinterpret_cast<PyThreadState *>(token);
    if (PyThreadState_Swap(st) != st) {
        // This warning ends up happening all the time when we exec.
        // Python code that performs imports.  We need to be
        // robust to any kind of action from the Python code we run,
        // so here we simply switch back in the right thread and then
        // release it.

        //cerr << "warning: somebody switched Python threads on us to "
        //     << st << ": please use facilities in python_interpreter.h to "
        //     << " do so" << endl;
    }
    PyEval_ReleaseThread(st);
    PyThreadState_Swap(nullptr);
}

// NOTE: partially copied from exec.cpp in Boost, available under the Boost
// license.  Copyright Stefan Seefeld 2005
// http://www.boost.org/LICENSE_1_0.txt

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
    int res = pipe(fds);

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
std::atomic<size_t> initializersDone = 0;
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

/// Is MLDB a module?  Must be set before any functionality is called
bool mldbIsAModule = false;

/// Have we already initialized the main interpreter?
std::atomic<bool> mainInterpreterInitialized(false);

/// If we're a module, this is where our main thread state is
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

// This is the main Python interpreter, which is created for us on
// initialization of Python.  It's important for two reasons:
// a) Creating a new sub-interpreter needs to be done with this
//    thread current; although the documentation for PyInterpreter_New
//    says you can call it without a thread state, you must have the
//    GIL locked and you can only lock the GIL with a thread state...
//    catch 22.  This is the thread state that enables us to manage
//    the situation.
// b) Cleanup of MLDB is done within this thread.
//
// Note that if MLDB is loaded as a Python module, there is a different
// initialization setup, as the main interpreter is owned externally and
// we can use it but we don't own it.

PythonInterpreter &
PythonInterpreter::
mainInterpreter()
{
    static PythonInterpreter result(CREATE_MAIN);

    mainInterpreterInitialized = true;

    if (hasInitializersToRun()) {
        auto enterGuard = result.mainThread().enter();
        runPythonInitializers(*enterGuard);
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

    bool isModule = mldbIsAModule;
    
    if (context == CREATE_MAIN) {
        ExcAssert(!mainInterpreterInitialized);

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
            cerr << "*******\n\n INITIALIZING PYTHON\n\n********" << endl;

            PyConfig config;
            PyConfig_InitPythonConfig(&config);
            Scope_Exit(PyConfig_Clear(&config));

            auto checkStatus = [&] (PyStatus status)
                {
                    if (PyStatus_Exception(status)) {
                        cerr << "Error initializing Python: "
                             << status.err_msg << endl;
                        throw MLDB::Exception("Error initializing Python: " + string(status.err_msg));
                    }
                };

            std::filesystem::path exe_path = get_exe_name();



            const char * virtualenv_path = std::getenv("MLDB_VIRTUAL_ENV");
            if (!virtualenv_path)
                virtualenv_path = "";

            // 
            std::basic_string<wchar_t> wideVirtualEnv;
            if (auto venv_path = std::getenv("MLDB_VIRTUAL_ENV")) {
                wideVirtualEnv.append(venv_path, venv_path + strlen(venv_path));
            }

            std::string pythonMajorDotMinor = std::to_string(PY_MAJOR_VERSION) +  "." + std::to_string(PY_MINOR_VERSION);
            std::string pythonMajorMinor = std::to_string(PY_MAJOR_VERSION) + std::to_string(PY_MINOR_VERSION);

            // Contains the PATH that the system Python interpreter uses
            std::vector<std::string> system_python_path;
            if (const char * mldb_python_system_path = std::getenv("MLDB_PYTHON_SYSTEM_PATH")) {
                system_python_path = MLDB::split(mldb_python_system_path, ':');
            }
            else {
                // Find the python executable
                std::string python_executable = "python" + pythonMajorDotMinor;

                // look for it in the path
                std::vector<std::string> system_path;
                if (const char * path = ::getenv("PATH")) {
                    system_path = MLDB::split(path, ':');
                }

                std::string python_executable_full_path;
                for (auto && p: system_path) {
                    struct stat st;
                    std::string this_executable = p + "/" + python_executable;
                    int res = ::stat(this_executable.c_str(), &st);
                    if (res == 0 && S_ISREG(st.st_mode) /*&& S_ISEXEC(st.st_mode)*/) {
                        python_executable_full_path = this_executable;
                        break;
                    }
                }

                if (python_executable_full_path.empty()) {
                    throw MLDB::Exception("Couldn't find Python executable '" + python_executable + "' in PATH");
                }

                // Run the system Python executable so it can tell us what its path is
                std::vector<std::string> cmdline = { python_executable_full_path, "-c", "import sys; print(':'.join(sys.path))" };
                std::ostringstream output;
                auto stdout = std::make_shared<OStreamInputSink>(&output);
                MLDB::RunResult result = MLDB::execute(cmdline, stdout);
                if (result.state != RunResult::RETURNED) {
                    std::string cmdlinestr;
                    for (auto && arg: cmdline) {
                        if (!cmdlinestr.empty())
                            cmdlinestr += " ";
                        cmdlinestr += arg;
                    }
                    throw MLDB::Exception("Error running Python to get path with '" + cmdlinestr + "': " + jsonEncodeStr(result));
                }
                cerr << "result = " << jsonEncodeStr(result) << endl;
                cerr << "output = " << output.str() << endl;
                system_python_path = MLDB::split(output.str(), ':');
            }

            config.module_search_paths_set = 1;

            for (auto && path: system_python_path) {
                std::basic_string<wchar_t> widePath(path.begin(), path.end());
                checkStatus(PyWideStringList_Append(&config.module_search_paths, widePath.c_str()));
            }

            //std::filesystem::path python_path = exe_path.parent_path();
            //python_path.wstring();

            if (auto venv_path = std::getenv("MLDB_VIRTUAL_ENV")) {
                std::wstring venv(venv_path, venv_path + strlen(venv_path));
                venv += L"/bin";
                //venv += L"/lib/python" + std::wstring(pythonMajorMinor.begin(), pythonMajorMinor.end()) + L"/site-packages";
                checkStatus(PyWideStringList_Append(&config.module_search_paths, venv.c_str()));
            }

            //checkStatus(PyConfig_SetString(&config, &config.home, L"/usr"));
            //checkStatus(PyConfig_SetString(&config, &config.executable, L"/usr/bin/python3.12"));

            //checkStatus(PyWideStringList_Append(&config.module_search_paths, (wideVirtualEnv + L"/bin").c_str()));
            //checkStatus(PyWideStringList_Append(&config.module_search_paths, L"/opt/homebrew/bin"));
            //checkStatus(PyWideStringList_Append(&config.module_search_paths, L"build/aarch64/bin"));
            //for (auto && path: {L"/usr/lib/python3.12", L"/usr/lib/python3.12/lib-dynload", L"/usr/local/lib/python3.12/dist-packages", L"/usr/lib/python3/dist-packages"}) {
            //    checkStatus(PyWideStringList_Append(&config.module_search_paths, path));
            //}
            //checkStatus(PyWideStringList_Append(&config.module_search_paths, L"/usr/lib/python3.12"));
            //checkStatus(PyWideStringList_Append(&config.module_search_paths, L"/usr/lib/python3.12/lib-dynload"));

            //['', '/usr/lib/python312.zip', '/usr/lib/python3.12', '/usr/lib/python3.12/lib-dynload', '/usr/local/lib/python3.12/dist-packages', '/usr/lib/python3/dist-packages']            

            //checkStatus(PyConfig_SetString(&config, &config.base_prefix, wideVirtualEnv.c_str()));

            //checkStatus(PyConfig_SetString(&config, &config.base_prefix, wideVirtualEnv.c_str()));
            //checkStatus(PyConfig_SetString(&config, &config.prefix, wideVirtualEnv.c_str()));

            //checkStatus(PyWideStringList_Append(&config.module_search_paths, L"/path/to/stdlib/again"));
            //auto exe_path = get_exe_name();
            //const std::basic_string<wchar_t> wideExePath(exe_path.begin(), exe_path.end());
            //checkStatus(PyConfig_SetString(&config, &config.executable, wideExePath.c_str()));

            checkStatus(Py_InitializeFromConfig(&config));

#if PY_MAJOR_VERSION <= 4 && PY_MINOR_VERSION <= 9
            // Deprecated to a no-op in Python 3.9: https://docs.python.org/3/whatsnew/3.9.html
            PyEval_InitThreads();
#endif
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
            auto enterGuard
                = PythonInterpreter::mainInterpreter().mainThread().enter();
            runPythonInitializers(*enterGuard);
            st = getNewInterpreter();
        }
    
        auto endSubInterpreter = [] (PyThreadState * interp)
        {
            // Yes, sub interpreters can be ended recursively
            // in which case, the GIL is held
            static thread_local int recursionCount = 0;
            ++recursionCount;
            Scope_Exit(--recursionCount);
            // This is tricky.  This must be called without the GIL
            // held.


            if (recursionCount > 1) {
                auto old = PyThreadState_Swap(interp);
                Py_EndInterpreter(interp);
                PyThreadState_Swap(old);
            }
            else {
                // First, we acquire the GIL and switch to the interpreter's
                // thread, as it's only from there that it may be destroyed.
                PyEval_AcquireThread(interp);

                // Now we destroy it.  It will switch to a null thread, but
                // not release the GIL.
                Py_EndInterpreter(interp);

                // So now we release the GIL.  We can't use PyEval_ReleaseThread
                // as that checks that we're currently in a thread, which we're
                // not.
                PyThreadState_Swap(PythonInterpreter::mainInterpreter().mainThread().st_.get());

                PyEval_ReleaseThread(PythonInterpreter::mainInterpreter().mainThread().st_.get());
                //PyEval_ReleaseLock();
            }
        };
    
        this->interpState.reset(st, endSubInterpreter);
        mainThread_.init(st, false /* don't manage lifecycle */);
    }
}

PythonInterpreter::
~PythonInterpreter()
{
}

void
PythonInterpreter::
destroy()
{
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
