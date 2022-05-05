/* lisp_lib_predicate.cc                                                        -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "lisp_lib_impl.h"
#include "lisp_visitor.h"
#include <cmath>

using namespace std;

namespace MLDB {
namespace Lisp {

template<typename Pred>
CompiledExpression compilePred(Pred && pred, PathElement name, const CompilationScope & scope, List expr)
{
    if (expr.size() != 2) {
        scope.exception("Predicate expressions require one argument");
    }

    Context & context = scope.getContext();

    CompiledExpression arg = scope.compile(expr[1]);

    Executor exec = [arg = std::move(arg), pred = std::move(pred)](ExecutionScope & scope) -> Value
    {
        bool res = pred(arg(scope));
        return res ? scope.getContext().boolean(true) : scope.getContext().null();
    };

    return { name, std::move(exec), nullptr, &context, "COMPARISON " + name.toUtf8String() };    
}

#define DEFINE_LISP_PREDICATE_COMPILER(Name, Lib, NameStr) \
bool impl_##Lib##_##Name(const Lisp::Value & val); \
DEFINE_LISP_FUNCTION_COMPILER(Name, Lib, NameStr) \
{ \
    return compilePred([](const Lisp::Value & val){ return impl_##Lib##_##Name(val); }, #Name, scope, expr); \
} \
bool impl_##Lib##_##Name(const Lisp::Value & val)


DEFINE_LISP_PREDICATE_COMPILER(oddp, std, "oddp")
{
    LambdaVisitor visitor {
        [&] (const Value & val) -> bool // first is for unmatched values
        {
            MLDB_THROW_RUNTIME_ERROR("oddp is only defined for non-integral arguments");
        },
        [&] (int64_t i)            { return (i & 1) == 1; },
        [&] (uint64_t i)           { return (i & 1) == 1; },
    };

    return visit(visitor, val);
}

DEFINE_LISP_PREDICATE_COMPILER(evenp, std, "evenp")
{
    LambdaVisitor visitor {
        [&] (const Value & val) -> bool // first is for unmatched values
        {
            MLDB_THROW_RUNTIME_ERROR("evenp is only defined for non-integral arguments");
        },
        [&] (int64_t i)            { return (i & 1) == 0; },
        [&] (uint64_t i)           { return (i & 1) == 0; },
    };

    return visit(visitor, val);
}

DEFINE_LISP_PREDICATE_COMPILER(plusp, std, "plusp")
{
    LambdaVisitor visitor {
        [&] (const Value & val) -> bool // first is for unmatched values
        {
            MLDB_THROW_RUNTIME_ERROR("plusp is only defined for numeric arguments");
        },
        [&] (int64_t i)            { return i >= 0; },
        [&] (uint64_t i)           { return true; },
        [&] (double d)             { return std::signbit(d) == false; },
    };

    return visit(visitor, val);
}

DEFINE_LISP_PREDICATE_COMPILER(minusp, std, "minusp")
{
    LambdaVisitor visitor {
        [&] (const Value & val) -> bool // first is for unmatched values
        {
            MLDB_THROW_RUNTIME_ERROR("minusp is only defined for numeric arguments");
        },
        [&] (int64_t i)            { return i < 0; },
        [&] (uint64_t i)           { return false; },
        [&] (double d)             { return std::signbit(d) == true; },
    };

    return visit(visitor, val);
}

DEFINE_LISP_PREDICATE_COMPILER(zerop, std, "zerop")
{
    LambdaVisitor visitor {
        [&] (const Value & val) -> bool // first is for unmatched values
        {
            MLDB_THROW_RUNTIME_ERROR("zerop is only defined for numeric arguments");
        },
        [&] (int64_t i)            { return i == 0; },
        [&] (uint64_t i)           { return i == 0; },
        [&] (double d)             { return d == 0.0; },
    };

    return visit(visitor, val);
}

DEFINE_LISP_PREDICATE_COMPILER(numberp, std, "numberp")
{
    LambdaVisitor visitor {
        [&] (const Value & val) -> bool // first is for unmatched values
        {
            return false;
        },
        [&] (int64_t i)            { return true; },
        [&] (uint64_t i)           { return true; },
        [&] (double d)             { return true; },
    };

    return visit(visitor, val);
}

} // namespace Lisp
} // namespace MLDB
