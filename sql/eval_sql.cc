/** eval_sql.cc                                                     -*- C++ -*-
    Jeremy Barnes, 1 August 2016
    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Lambda function written in SQL.
*/

#include "eval_sql.h"
#include "binding_contexts.h"


namespace MLDB {

BoundSqlExpression
bindSql(SqlBindingScope & scope,
        const Utf8String & expression,
        const std::vector<std::shared_ptr<ExpressionValueInfo> > & info)
{
    auto parsed = SqlExpression::parse(expression);

    auto bound = parsed->bind(scope);

    return bound;
}

ExpressionValue
evalSql(SqlBindingScope & scope,
        const Utf8String & expr,
        ExpressionValue * argsVec,
        size_t numArgs)
{
    // TODO: this signature is for when we have a binding scope that
    // knows that we can move arguments out of the way.  For now
    // we forward.

    return evalSql(scope, expr,
                   static_cast<const ExpressionValue *>(argsVec),
                   numArgs);
}

ExpressionValue
evalSql(SqlBindingScope & scope,
        const Utf8String & expr,
        const ExpressionValue * argsVec,
        size_t numArgs)
{
    std::vector<std::shared_ptr<ExpressionValueInfo> > info;
    info.reserve(numArgs);
    for (size_t i = 0;  i < numArgs;  ++i) {
        info.emplace_back(argsVec[i].getSpecializedValueInfo());
    }

    SqlExpressionEvalScope evalScope(scope, info);

    auto bound = bindSql(evalScope, expr, info);

    auto rowScope = evalScope.getRowScope(argsVec, numArgs);
    
    return bound(rowScope, GET_ALL);
}

ExpressionValue evalSql(SqlBindingScope & scope,
                        const Utf8String & expr,
                        const std::vector<ExpressionValue> & argsVec)
{
    return evalSql(scope, expr, argsVec.data(), argsVec.size());
}

} // namespace MLDB

