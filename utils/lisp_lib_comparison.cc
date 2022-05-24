/* lisp_lib_comparison.cc                                                        -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "lisp_lib_impl.h"
#include "lisp_visitor.h"
#include "mldb/types/value_description.h"

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
        bool res = compare(args[0](scope), args[1](scope));
        return res ? scope.getContext().boolean(true) : scope.getContext().null();
    };

    return { name, std::move(exec), nullptr, &context, "COMPARISON " + name.toUtf8String() };
}

// Implement binary comparison with the underlying comparison operator, specialized to
// the common type
template<template<typename> typename Cmp>
bool compareImpl(const Value & v1, const Value & v2)
{
    //cerr << "comparing " << v1.print() << " and " << v2.print() << endl;

    if (v1.isNumeric() && v2.isNumeric()) {
        if (v1.is<double>() || v2.is<double>()) {
            // TODO: inexactness
            //cerr << "comparing as double" << endl;
            return Cmp<double>()(asDouble(v1),asDouble(v2));
        }
        else if (v1.is<int64_t>() || v2.is<int64_t>()) {
            //cerr << "comparing as int64" << endl;
            //cerr << "comparing " << asInt(v1) << " and " << asInt(v2) << endl;
            return Cmp<int64_t>()(asInt(v1),asInt(v2));
        }
        else if (v1.is<uint64_t>() || v2.is<uint64_t>()) {
            //cerr << "comparing as uint64" << endl;
            return Cmp<uint64_t>()(asUInt(v1),asUInt(v2));
        }
        else {
            MLDB_THROW_RUNTIME_ERROR("incompatible types for numeric comparison");
        }
    }
    else if (v1.type() == v2.type()) {
        //cerr << "v1 = " << v1 << " v2 = " << v2 << endl;

        LambdaVisitor visitor {
            ExceptionOnUnknownReturning<bool>("NOT IMPLEMENTED: comparison of this type"),
            [&] (double d)             { return d == v2.as<double>(); },
            [&] (uint64_t i)           { return i == v2.as<uint64_t>(); },
            [&] (int64_t i)            { return i == v2.as<int64_t>(); },
            [&] (bool b)               { return b == v2.as<bool>(); },
            [&] (Null)                 { return true; },
            [&] (const Symbol & s)     { return s.sym == v2.as<Symbol>().sym; }
            // ... LOTS of others...
        };

        return visit(visitor, v1);
    }
    else {
        return Cmp<std::string>()(string(v1.type().name()), string(v2.type().name()));
    }
}

DEFINE_LISP_FUNCTION_COMPILER(less, std, "<")
{
    return compileComparison(compareImpl<std::less>, "less", scope, std::move(expr));
}

DEFINE_LISP_FUNCTION_COMPILER(less_equal, std, "<=")
{
    return compileComparison(compareImpl<std::less_equal>, "less_equal", scope, std::move(expr));
}

DEFINE_LISP_FUNCTION_COMPILER(greater, std, ">")
{
    return compileComparison(compareImpl<std::greater>, "greater", scope, std::move(expr));
}

DEFINE_LISP_FUNCTION_COMPILER(greater_equal, std, ">=")
{
    return compileComparison(compareImpl<std::greater_equal>, "greater_equal", scope, std::move(expr));
}

DEFINE_LISP_FUNCTION_COMPILER(equal, std, "=", "equal")
{
    return compileComparison(compareImpl<std::equal_to>, "equal", scope, std::move(expr));
}

DEFINE_LISP_FUNCTION_COMPILER(not_equal, std, "/=")
{
    return compileComparison(compareImpl<std::not_equal_to>, "not_equal", scope, std::move(expr));
}

} // namespace Lisp
} // namespace MLDB
