/* lisp_lib_impl.h                                                        -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "lisp_lib.h"
#include "lisp_visitor.h"
#include "safe_clamp.h"
#include <limits>


namespace MLDB {
namespace Lisp {

inline double asDouble(const Value & v)
{
    LambdaVisitor visitor {
        ExceptionOnUnknownReturning<double>("asDouble not defined for this value"),
        [] (int64_t i)            { return i; },
        [] (uint64_t i)           { return i; },
        [] (double d)             { return d; },
    };
    return visit(visitor, v);
}

inline uint64_t asUInt(const Value & v)
{
    LambdaVisitor visitor {
        ExceptionOnUnknownReturning<uint64_t>("asUInt not defined for this value"),
        [] (int64_t i)            { if (i < 0) MLDB_THROW_LOGIC_ERROR();  return i; },
        [] (uint64_t i)           { return i; },
        [] (double d)             { return safe_clamp<uint64_t>(d); },
    };
    return visit(visitor, v);
}

inline int64_t asInt(const Value & v)
{
    LambdaVisitor visitor {
        ExceptionOnUnknownReturning<uint64_t>("asInt not defined for this value"),
        [] (int64_t i)            { return i; },
        [] (uint64_t i)           { if (i > std::numeric_limits<int64_t>::max()) MLDB_THROW_LOGIC_ERROR(); return i; },
        [] (double d)             { return safe_clamp<int64_t>(d); },
    };
    return visit(visitor, v);
}

inline Value returnNil(ExecutionScope & scope)
{
    return scope.getContext().list();
}

} // namespace Lisp
} // namespace MLDB
