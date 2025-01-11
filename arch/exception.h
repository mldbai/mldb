/* exception.h                                                     -*- C++ -*-
   Jeremy Barnes, 26 January 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Defines our exception class.
*/

#pragma once

#include <string>
#include <exception>
#include <stdarg.h>
#include "exception_handler.h"

namespace MLDB {

class Utf8String; // All methods that implement this are defined in the types
                  // library, as the exceptions are used by all libraries.

class Exception : public std::exception {
public:
    Exception(const std::string & msg);
    Exception(std::string && msg);
    Exception(const Utf8String & msg);  // implemented in types
    Exception(Utf8String && msg);  // implemented in types
    Exception(const char * msg, ...);
    Exception(const char * msg, va_list ap);
    Exception(int errnum, const std::string & msg, const char * function = 0);
    Exception(int errnum, const Utf8String & msg, const char * function = 0);
    Exception(int errnum, const char * msg, const char * function = 0);
    virtual ~Exception() throw();
    
    virtual const char * what() const throw();
    
private:
    std::string message;
};


/** Exceptions derived from this type will be ignored by the exception handler.
    Effectively, this means that, when thrown, the exception will not be dumped
    to the console.
*/
class SilentException : public virtual std::exception {};

/** Return a string that represents the currently thrown exception. */
std::string getExceptionString();


/*****************************************************************************/
/* ASSERTION FAILURE                                                         */
/*****************************************************************************/

/** Exception thrown when an exception assert is made and fails. */

struct AssertionFailure: public Exception {
    AssertionFailure(const std::string & msg);
    AssertionFailure(const Utf8String & msg);
    AssertionFailure(const char * msg, ...);
    AssertionFailure(const char * assertion,
                      const char * function,
                      const char * file,
                      int line);
};

// Apply the macro to all of the exception classes; if we want another one,
// we add it to this list
#define MLDB_FOR_EACH_EXCEPTION_CLASS(macro) \
    macro(UnimplementedException) \
    macro(RuntimeError) \
    macro(LogicError) \
    macro(BadAlloc) \
    macro(RangeError) \

#define MLDB_DEFINE_EXCEPTION_CLASS(Name)   \
struct Name: public Exception {             \
    Name(const std::string & message);      \
    Name(const char * function,             \
         const char * file,                 \
         int line);                         \
    Name(const char * function,             \
         const char * file,                 \
         int line,                          \
         const char * msg,                  \
         ...);                              \
    Name(const char * function,             \
         const char * file,                 \
         int line,                          \
         const std::string msg,             \
         ...);                              \
};                                          \
[[noreturn]] void throw##Name(const char * function, const char * file, int line, const char * msg = nullptr, ...); \
[[noreturn]] void throw##Name(const char * function, const char * file, int line, const std::string msg, ...); \
[[noreturn]] void throw##Name(const char * function, const char * file, int line, const Utf8String msg, ...);
[[noreturn]] void throwUnimplementedException(const std::type_info & thisType, const char * function, const char * file, int line, const std::string msg, ...);

MLDB_FOR_EACH_EXCEPTION_CLASS(MLDB_DEFINE_EXCEPTION_CLASS)

#define MLDB_THROW_UNIMPLEMENTED(...) do { throwUnimplementedException(__PRETTY_FUNCTION__, __FILE__, __LINE__ __VA_OPT__(,) __VA_ARGS__); } while (false)
#define MLDB_THROW_UNIMPLEMENTED_ON_THIS(...) do { throwUnimplementedException(typeid(*this), __PRETTY_FUNCTION__, __FILE__, __LINE__ __VA_OPT__(,) __VA_ARGS__); } while (false)
#define MLDB_THROW_LOGIC_ERROR(...) do { throwLogicError(__PRETTY_FUNCTION__, __FILE__, __LINE__ __VA_OPT__(,) __VA_ARGS__); } while (false)
#define MLDB_THROW_RUNTIME_ERROR(...) do { throwRuntimeError(__PRETTY_FUNCTION__, __FILE__, __LINE__ __VA_OPT__(,) __VA_ARGS__); } while (false)
#define MLDB_THROW_BAD_ALLOC(...) do { throwBadAlloc(__PRETTY_FUNCTION__, __FILE__, __LINE__ __VA_OPT__(,) __VA_ARGS__); } while (false)
#define MLDB_THROW_RANGE_ERROR(...) do { throwRangeError(__PRETTY_FUNCTION__, __FILE__, __LINE__ __VA_OPT__(,) __VA_ARGS__); } while (false)


} // namespace MLDB
