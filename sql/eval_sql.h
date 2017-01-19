/** eval_sql.h                                                     -*- C++ -*-
    Jeremy Barnes, 1 August 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Lambda function written in SQL.
*/

#pragma once

#include "mldb/sql/sql_expression.h"
#include "mldb/sql/expression_value.h"


namespace MLDB {

/** Bind the given SQL expression with the given argument info into a
    function object that can be invoked to evaluate the expression.
*/
BoundSqlExpression
bindSql(SqlBindingScope & scope,
        const Utf8String & expression,
        const std::vector<std::shared_ptr<ExpressionValueInfo> > & argInfo);

/** Parse, bind and evaluate the given SQL expression (not statement)
    with the given arguments and return the result;
*/
ExpressionValue
evalSql(SqlBindingScope & scope,
        const Utf8String & expr,
        const SqlRowScope & rowScope,
        const std::vector<ExpressionValue> & argsVec);

/** Parse, bind and evaluate the given SQL expression (not statement)
    with the given arguments and return the result.

    The arguments my be modified by the call.
*/
ExpressionValue
evalSql(SqlBindingScope & scope,
        const Utf8String & expr,
        const SqlRowScope & rowScope,
        ExpressionValue * argsVec,
        size_t numArgs);

/** Parse, bind and evaluate the given SQL expression (not statement)
    with the given arguments and return the result.
*/
ExpressionValue
evalSql(SqlBindingScope & scope,
        const Utf8String & expr,
        const SqlRowScope & rowScope,
        const ExpressionValue * argsVec,
        size_t numArgs);

/** This is a free function that is used to allow arguments of a
    given type to be passed through evalSql.  For each argument,
    the bindSqlArg function will be called with the argument type.

    By providing specializations, it's possible to allow different
    types to be passed to MLDB.

    This default specialization works for anything that can be
    constructed by passing along with a timestamp to the ExpressionValue
    constructor.
*/
template<typename T>
inline ExpressionValue bindSqlArg(T && val)
{
    return ExpressionValue(std::move(val), Date::notADate());
}

/** Allow an ExpressionValue to be passed through as an argument to
    the evalSql function.
*/
inline ExpressionValue bindSqlArg(ExpressionValue val)
{
    return val;
}

/** Evaluate the given SQL expression, within the given scope (which is
    often an SqlExpressionMldbScope) and return the results.

    The expr is an SQL expression (not a statement) in a string, which
    will be parsed.

    Each argument will be passed through bindSqlArg() to convert to an
    ExpressionValue, and then they will be bound to the $1, $2, ...
    parameters in the expression.

    The binding will be in context of the given rowScope.

    For example, evalSql("$1 + $2", 1, 2).toInt() will return 3.

    Any exceptions will be returned as a return of the function.
*/
template<typename... Args>
ExpressionValue evalSql(SqlBindingScope & scope,
                        const Utf8String & expr,
                        const SqlRowScope & rowScope,
                        Args&&... args)
{
    ExpressionValue argsArray[sizeof...(args)]
        = { bindSqlArg(std::forward<Args>(args))... };
    return evalSql(scope, expr, rowScope, argsArray, sizeof...(args));
}

} // namespace MLDB

