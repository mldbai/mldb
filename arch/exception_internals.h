// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* exception_internals.h                                           -*- C++ -*-
   Jeremy Barnes, 18 October 2010
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.

   Internals needed to interoperate with the exception handling.  These are
   copied from the libsupc++ sources, but contain no functionality, only
   definitions required for interoperability.
*/

#pragma once

#include <typeinfo>
#include <exception>
#include <cstddef>
#include "cxxabi.h"
#include <unwind.h>
#include <exception>

namespace __cxxabiv1 {

// A C++ exception object consists of a header, which is a wrapper around
// an unwind object header with additional C++ specific information,
// followed by the exception object itself.

typedef void (*unexpected_handler);

struct __cxa_exception
{ 
  // Manage the exception object itself.
  std::type_info *exceptionType;
  void (*exceptionDestructor)(void *); 

  // The C++ standard has entertaining rules wrt calling set_terminate
  // and set_unexpected in the middle of the exception cleanup process.
  unexpected_handler unexpectedHandler;
  std::terminate_handler terminateHandler;

  // The caught exception stack threads through here.
  __cxa_exception *nextException;

  // How many nested handlers have caught this exception.  A negated
  // value is a signal that this object has been rethrown.
  int handlerCount;

  // Cache parsed handler data from the personality routine Phase 1
  // for Phase 2 and __cxa_call_unexpected.
  int handlerSwitchValue;
  const unsigned char *actionRecord;
  const unsigned char *languageSpecificData;
  void * /* _Unwind_Ptr */ catchTemp;
  void *adjustedPtr;

  size_t referenceCount;

  // The generic exception header.  Must be last.
  _Unwind_Exception unwindHeader;
};

struct __cxa_eh_globals
{
  __cxa_exception *caughtExceptions;
  unsigned int uncaughtExceptions;
};

extern "C" __cxa_eh_globals *__cxa_get_globals () throw();
#if defined(MLDB_STDLIB_LLVM )
uint64_t __getExceptionClass  (const _Unwind_Exception*);
bool     __isOurExceptionClass(const _Unwind_Exception*);
#endif

} // __cxxabiv1
