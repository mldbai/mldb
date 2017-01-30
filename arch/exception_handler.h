/* exception_handler.h                                             -*- C++ -*-
   Jeremy Barnes, 2 December 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Interface for the exception handler.
*/

#pragma once

namespace MLDB {

/// Set the default value of tracing exceptions.  New threads will be
/// initialized to this value.
void set_default_trace_exceptions(bool trace);
bool get_default_trace_exceptions();

/// Set whether or not we are tracing exceptions for this thread.
void set_trace_exceptions(bool trace);
bool get_trace_exceptions();

/// Guard object to enable/disable/set tracing exceptions
struct Set_Trace_Exceptions {
    Set_Trace_Exceptions(bool trace)
        : old_trace_exceptions(get_trace_exceptions())
    {
        set_trace_exceptions(trace);
    }

    ~Set_Trace_Exceptions()
    {
        set_trace_exceptions(old_trace_exceptions);
    }

    bool old_trace_exceptions;
};

void default_exception_tracer(void * object, const std::type_info * tinfo);

} // namespace MLDB

#define MLDB_TRACE_EXCEPTIONS(value) \
MLDB::Set_Trace_Exceptions trace_exc__(value);
