// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* exception_handler.cc
   Jeremy Barnes, 26 February 2008
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.

*/

#include <cxxabi.h>
#include <cstring>
#include <fstream>

#include "mldb/compiler/compiler.h"
#include "mldb/utils/environment.h"

#include "backtrace.h"
#include "demangle.h"
#include "exception.h"
#include "exception_hook.h"
#include "format.h"
#include "threads.h"
#include "rtti_utils.h"

#include "exception_internals.h"

using namespace std;


namespace MLDB {

EnvOption<bool> TRACE_EXCEPTIONS("MLDB_TRACE_EXCEPTIONS", true);

__thread bool trace_exceptions = false;
__thread bool trace_exceptions_initialized = false;

void set_default_trace_exceptions(bool val)
{
    TRACE_EXCEPTIONS.set(val);
}

bool get_default_trace_exceptions()
{
    return TRACE_EXCEPTIONS;
}

void set_trace_exceptions(bool trace)
{
    //cerr << "set_trace_exceptions to " << trace << " at " << &trace_exceptions
    //     << endl;
    trace_exceptions = trace;
    trace_exceptions_initialized = true;
}

bool get_trace_exceptions()
{
    if (!trace_exceptions_initialized) {
        //cerr << "trace_exceptions initialized to = "
        //     << trace_exceptions << " at " << &trace_exceptions << endl;
        set_trace_exceptions(TRACE_EXCEPTIONS);
        trace_exceptions_initialized = true;
    }
    
    //cerr << "get_trace_exceptions returned " << trace_exceptions
    //     << " at " << &trace_exceptions << endl;

    return trace_exceptions;
}


static const std::exception *
to_std_exception(void* object, const std::type_info * tinfo)
{
#if 1
    return is_convertible<std::exception>(object, *tinfo);
#elif defined( MLDB_STDLIB_GCC )
    /* Check if its a class.  If not, we can't see if it's a std::exception.
       The abi::__class_type_info is the base class of all types of type
       info for types that are classes (of which std::exception is one).
    */
    const abi::__class_type_info * ctinfo
        = dynamic_cast<const abi::__class_type_info *>(tinfo);

    if (!ctinfo) return nullptr;

    /* The thing thrown was an object.  Now, check if it is derived from
    std::exception. */
    const std::type_info * etinfo = &typeid(std::exception);

    /* See if the exception could catch this.  This is the mechanism
    used internally by the compiler in catch {} blocks to see if
    the exception matches the catch type.

    In the case of success, the object will be adjusted to point to
    the start of the std::exception object.
    */
    void * obj_ptr = object;
    bool can_catch = etinfo->__do_catch(tinfo, &obj_ptr, 0);

    if (!can_catch) return nullptr;

    /* obj_ptr points to a std::exception; extract it and get the
    exception message.
    */
    return (const std::exception *)obj_ptr;
#elif defined (MLDB_STDLIB_LLVM)
    // If there might be an uncaught exception
    using namespace __cxxabiv1;
    __cxa_eh_globals* globals = __cxa_get_globals();
    if (!globals) return nullptr;

    __cxa_exception* exception_header = globals->caughtExceptions;
    // If there is an uncaught exception
    if (!exception_header) return nullptr;

    _Unwind_Exception* unwind_exception =
        reinterpret_cast<_Unwind_Exception*>(exception_header + 1) - 1;

    if (!__isOurExceptionClass(unwind_exception))
        return nullptr;

    void* thrown_object =
        __getExceptionClass(unwind_exception) == kOurDependentExceptionClass ?
            ((__cxa_dependent_exception*)exception_header)->primaryException :
            exception_header + 1;

    const __shim_type_info* thrown_type =
        static_cast<const __shim_type_info*>(exception_header->exceptionType);
    const char* name = thrown_type->name();

    // If the uncaught exception can be caught with std::exception&
    const __shim_type_info* catch_type =
        static_cast<const __shim_type_info*>(&typeid(std::exception));
    if (catch_type->can_catch(thrown_type, thrown_object))
    {
        // Include the what() message from the exception
        const std::exception* e = static_cast<const std::exception*>(thrown_object);
        return e;
    }

    return nullptr;
#endif
}

/** We install this handler for when an exception is thrown. */

void default_exception_tracer(void * object, const std::type_info * tinfo)
{
    if (!get_trace_exceptions()) return;

    //const std::exception * exc = nullptr;
    bool noAlloc = tinfo == &typeid(std::bad_alloc);

#if 0
    std::exception_ptr excp = std::current_exception();
    try {
        MLDB_TRACE_EXCEPTIONS(false);
        std::rethrow_exception(excp);
    } catch (const std::bad_alloc & cexc) {
        noAlloc = true;
        exc = &cexc;
    } catch (const MLDB::SilentException & cexc) {
        // We don't want these exceptions to be printed out.
        return;
    } catch (const std::exception & cexc) {
        exc = &cexc;
    } catch (...) {
    }
#endif
    const std::exception * exc = to_std_exception(object, tinfo);

    if (exc && dynamic_cast<const MLDB::SilentException *>(exc)) return;

    constexpr size_t bufferSize(1024*64);
    char buffer[bufferSize];
    char datetime[128];
    size_t totalWritten(0), written, remaining(bufferSize);

    time_t now;
    time(&now);

    struct tm lt_tm;
    strftime(datetime, sizeof(datetime), "%FT%H:%M:%SZ",
             gmtime_r(&now, &lt_tm));

    const char * demangled;
    char * heapDemangled;
    if (noAlloc) {
        heapDemangled = nullptr;
        demangled = "std::bad_alloc";
    }
    else {
        heapDemangled = char_demangle(tinfo->name());
        demangled = heapDemangled;
    }

    auto pid = getpid();
    auto tid = gettid();

    written = ::snprintf(buffer, remaining,
                         "\n"
                         "--------------------------[Exception thrown]"
                         "---------------------------\n"
                         "time:   %s\n"
                         "type:   %s\n"
                         "pid:    %d; tid: %d\n",
                         datetime, demangled, pid, tid);
    if (heapDemangled) {
        free(heapDemangled);
    }
    if (written >= remaining) {
        goto end;
    }
    totalWritten += written;
    remaining -= written;

    if (exc) {
        written = snprintf(buffer + totalWritten, remaining,
                           "what:   %s\n", exc->what());
        if (written >= remaining) {
            goto end;
        }
        totalWritten += written;
        remaining -= written;
    }

    if (noAlloc) {
        goto end;
    }

    written = snprintf(buffer + totalWritten, remaining, "stack:\n");
    if (written >= remaining) {
        goto end;
    }
    totalWritten += written;
    remaining -= written;

    written = backtrace(buffer + totalWritten, remaining, 3);
    if (written >= remaining) {
        goto end;
    }
    totalWritten += written;

    if (totalWritten < bufferSize - 1) {
        strcpy(buffer + totalWritten, "\n");
    }

end:
    cerr << buffer;

    char const * reports = getenv("ENABLE_EXCEPTION_REPORTS");
    if (!noAlloc && reports) {
        std::string path = MLDB::format("%s/exception-report-%s-%d-%d.log",
                                      reports, datetime, pid, tid);

        std::ofstream file(path, std::ios_base::app);
        if (file) {
            file << getenv("_") << endl;
            backtrace(file, 3);
            file.close();
        }
    }
}

} // namespace MLDB
