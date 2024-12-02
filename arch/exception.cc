// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* exception.cc
   Jeremy Barnes, 7 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
      


   Exception class.
*/

#include "exception.h"
#include "format.h"
#include <string.h>
#include <cxxabi.h>
#include "demangle.h"
#include "mldb/compiler/stdlib.h"

using namespace std;


namespace MLDB {

namespace {
// Ensure that the c_str() of the string can execute, returning a null, without mutating the string behind the scenes
// Some standard libraries will only add the null termination on the first call of c_str() which can cause issues
// with multithreading.
template<typename Char>
inline void ensure_null_termintated_c_str(std::basic_string<Char> & str) { auto res __attribute__((unused)) = str.c_str(); }
} // file scope

Exception::Exception(const std::string & msg)
    : message(msg)
{
    ensure_null_termintated_c_str(message);
}

Exception::Exception(const char * msg, ...)
{
    va_list ap;
    va_start(ap, msg);
    try {
        message = vformat(msg, ap);
        ensure_null_termintated_c_str(message);
        va_end(ap);
    }
    catch (...) {
        va_end(ap);
        throw;
    }
}

Exception::Exception(const char * msg, va_list ap)
{
    message = vformat(msg, ap);
    ensure_null_termintated_c_str(message);
}

Exception::
Exception(int errnum, const std::string & msg, const char * function)
{
    string error = strerror(errnum);

    if (function) {
        message = function;
        message += ": ";
    }

    message += msg;
    message += ": ";

    message += error;

    ensure_null_termintated_c_str(message);
}

Exception::~Exception() throw()
{
}

const char * Exception::what() const throw()
{
    return message.c_str();
}

std::string getExceptionString()
{
    try {
        MLDB_TRACE_EXCEPTIONS(false);
        throw;
    }
    catch (const std::bad_alloc & exc) {
        return "Out of memory (std::bad_alloc)";
    }
    catch (const std::exception & exc) {
        return exc.what();
    }
    catch (...) {
        const std::type_info* t = __cxxabiv1::__cxa_current_exception_type();
        return demangle(t->name());
    }
}


/*****************************************************************************/
/* ASSERTION FAILURE                                                         */
/*****************************************************************************/

AssertionFailure::
AssertionFailure(const std::string & msg)
    : Exception(msg)
{
}

AssertionFailure::
AssertionFailure(const char * msg, ...)
    : Exception(msg)
{
}

AssertionFailure::
AssertionFailure(const char * assertion,
                  const char * function,
                  const char * file,
                  int line)
    : Exception(format("assertion failure: %s at %s:%d in %s",
                    assertion, file, line, function))
{
}



} // namespace MLDB
