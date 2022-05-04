/* lisp_lib_comparison.cc                                                        -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "lisp_lib_impl.h"

using namespace std;

namespace MLDB {
namespace Lisp {

template<typename Compare>
CompiledExpression compileComparison(Compare && compare, PathElement name, const CompilationScope & scope, List expr)
{
    if (expr.size() != 3) {
        scope.exception("Comparison expression requires two arguments");
    }

    Context & context = scope.getContext();
    std::array<CompiledExpression, 2> args;

    for (size_t i = 0;  i < 2;  ++i) {
        auto & item = expr[i + 1];
        args[i] = scope.compile(item);
    }

    Executor exec = [args = std::move(args), compare = std::move(compare)]
          (ExecutionScope & scope) -> Value
    {
        return scope.getContext().boolean(compare(args[0](scope), args[1](scope)));
    };

    return { name, std::move(exec), nullptr, &context, "COMPARISON " + name.toUtf8String() };
}

// Implement binary comparison with the underlying comparison operator, specialized to
// the common type
template<template<typename> typename Cmp>
bool compareImpl(const Value & v1, const Value & v2)
{
    //cerr << "calculating " << result.print() << " / " << v2.print() << endl;

    if (v1.is<double>() || v2.is<double>()) {
        // TODO: inexactness
        return Cmp<double>()(asDouble(v1),asDouble(v2));
    }
    else if (v1.is<int64_t>() || v2.is<int64_t>()) {
        return Cmp<uint64_t>()(asInt(v1),asInt(v2));
    }
    else if (v1.is<uint64_t>() || v2.is<uint64_t>()) {
        return Cmp<int64_t>()(asUInt(v1),asUInt(v2));
    }
    else {
        MLDB_THROW_RUNTIME_ERROR("incompatible types for multiplication");
    }
}

DEFINE_LISP_FUNCTION_COMPILER(less, std, "<")
{
    return compileComparison(compareImpl<std::less>, "less", scope, std::move(expr));
}

DEFINE_LISP_FUNCTION_COMPILER(greater, std, ">")
{
    return compileComparison(compareImpl<std::greater>, "greater", scope, std::move(expr));
}

DEFINE_LISP_FUNCTION_COMPILER(equal, std, "=")
{
    return compileComparison(compareImpl<std::equal_to>, "equal", scope, std::move(expr));
}

} // namespace Lisp
} // namespace MLDB
