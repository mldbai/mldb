// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

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

using namespace std;


namespace ML {

Exception::Exception(const std::string & msg)
    : message(msg)
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
        JML_TRACE_EXCEPTIONS(false);
        throw;
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

Assertion_Failure::
Assertion_Failure(const std::string & msg)
    : Exception(msg)
{
}

Assertion_Failure::
Assertion_Failure(const char * msg, ...)
    : Exception(msg)
{
}

Assertion_Failure::
Assertion_Failure(const char * assertion,
                  const char * function,
                  const char * file,
                  int line)
    : Exception(format("assertion failure: %s at %s:%d in %s",
                    assertion, file, line, function))
{
}



} // namespace ML
