/* lisp_lib_basics.cc                                                        -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "lisp_lib_impl.h"
#include <numeric>
#include <cmath>

using namespace std;

namespace MLDB {
namespace Lisp {

// (sqrt x)
DEFINE_LISP_FUNCTION_COMPILER(sqrt, std, "sqrt")
{
    auto & context = scope.getContext();

    if (expr.size() != 2) {
        scope.exception("sqrt function takes exactly one argument");
    }

    CompiledExpression arg = scope.compile(expr[1]);

    Executor exec = [arg] (ExecutionScope & scope) -> Value
    {
        Value input = arg(scope);
        Value result = scope.getContext().f64(std::sqrt(asDouble(input)));
        return result;
    };

    return { "sqrt", std::move(exec), nullptr /* createScope */, &context, "SQRT (...)" };
}

} // namespace Lisp
} // namespace MLDB
