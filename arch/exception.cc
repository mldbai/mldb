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
#include <cstdarg>

using namespace std;


namespace MLDB {

Exception::Exception(const std::string & msg)
    : message(msg)
{
    message.c_str();  // make sure we have a null terminator
}

Exception::Exception(std::string && msg)
    : message(std::move(msg))
{
    message.c_str();  // make sure we have a null terminator
}

Exception::Exception(const char * msg, ...)
{
    va_list ap;
    va_start(ap, msg);
    try {
        message = vformat(msg, ap);
        message.c_str();
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
    message.c_str();
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

    message.c_str();
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
    catch (const BadAlloc & exc) {
        return "Out of memory (BadAlloc)";
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

namespace {
thread_local std::va_list exception_args;
} /* file scope */

#define MLDB_IMPLEMENT_EXCEPTION_CLASS(Name)                                        \
Name::                                                                              \
Name(const char * function,                                                         \
                       const char * file,                                           \
                       int line)                                                    \
    : Exception(format(#Name ": %s at %s:%d",                                       \
                    function, file, line))                                          \
{                                                                                   \
}                                                                                   \
                                                                                    \
Name::                                                                              \
Name(const std::string & message)                                                   \
    : Exception(message)                                                            \
{                                                                                   \
}                                                                                   \
                                                                                    \
Name::                                                                              \
Name(const char * function,                                                         \
                       const char * file,                                           \
                       int line,                                                    \
                       const char * msg,                                            \
                       ...)                                                         \
    : Exception(format(#Name ": %s at %s:%d : ", function, file, line)              \
                 + ((va_start(exception_args, msg), format(msg, exception_args))))  \
{                                                                                   \
}                                                                                   \
                                                                                    \
Name::                                                                              \
Name(const char * function,                                                         \
                       const char * file,                                           \
                       int line,                                                    \
                       const std::string msg,                                       \
                       ...)                                                         \
    : Exception(format(#Name ": %s at %s:%d : ", function, file, line)              \
                 + ((va_start(exception_args, msg), format(msg.c_str(), exception_args))))  \
{                                                                                   \
}                                                                                   \
                                                                                    \
void throw##Name(const char * function, const char * file, int line, const char * msg, ...) \
{                                                                                   \
    if (msg) {                                                                      \
        throw Name(format(#Name ": %s at %s:%d : ", function, file, line)           \
                          + ((va_start(exception_args, msg), format(msg, exception_args)))); \
    }                                                                               \
    else {                                                                          \
        throw Name(function, file, line);                                           \
    }                                                                               \
}                                                                                   \
                                                                                    \
void throw##Name(const char * function, const char * file, int line, const std::string msg, ...) \
{                                                                                   \
    throw Name(format(#Name ": %s at %s:%d : ", function, file, line)               \
                        + ((va_start(exception_args, msg), format(msg.c_str(), exception_args)))); \
}                                                                                   \
                                                                                    \

MLDB_FOR_EACH_EXCEPTION_CLASS(MLDB_IMPLEMENT_EXCEPTION_CLASS)

void throwUnimplementedException(const std::type_info & thisType, const char * function, const char * file, int line, const std::string msg, ...)
{
    throw UnimplementedException(format("Unimplemented: type %s does not implement operation at %s:%d : ",
                                        demangle(thisType.name()).c_str(), file, line)
                                 + ((va_start(exception_args, msg), format(msg.c_str(), exception_args))));
}

} // namespace MLDB
