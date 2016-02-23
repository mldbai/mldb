/** binding_contexts.cc                                              -*- C++ -*-
    Jeremy Barnes, 14 March 2015

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


    Contexts in which to execute scoped SQL expressions.
*/

#include "binding_contexts.h"
#include "http/http_exception.h"

using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* READ THROUGH BINDING CONTEXT                                              */
/*****************************************************************************/

BoundFunction
ReadThroughBindingContext::
doGetFunction(const Utf8String & tableName,
              const Utf8String & functionName,
              const std::vector<std::shared_ptr<SqlExpression> > & args)
{
    auto outerFunction = outer.doGetFunction(tableName, functionName, args);

    BoundFunction result = outerFunction;

    if (!outerFunction)
        return result;

    std::vector<BoundSqlExpression> boundArgs;
    for (auto& arg : args)
    {
        boundArgs.emplace_back(std::move(arg->bind(outer)));
    }

    // Call it with the outer context
    result.exec = [=] (const std::vector<BoundSqlExpression> & args,
                       const SqlRowScope & context)
        {
            //ExcAssert(dynamic_cast<const RowContext *>(&context) != nullptr);
            auto & row = static_cast<const RowContext &>(context);

            //cerr << "rebinding to apply function " << functionName
            //<< ": context type is "
            //<< ML::type_name(context) << " outer type is "
            //<< ML::type_name(row.outer) << endl;

            return outerFunction(boundArgs, row.outer);
        };

    return result;
}

VariableGetter
ReadThroughBindingContext::
doGetVariable(const Utf8String & tableName,
              const Utf8String & variableName)
{
    auto outerImpl = outer.doGetVariable(tableName, variableName);

    return {[=] (const SqlRowScope & context,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                auto & row = static_cast<const RowContext &>(context);
                return outerImpl(row.outer, storage, filter);
            },
            outerImpl.info};
}

GetAllColumnsOutput
ReadThroughBindingContext::
doGetAllColumns(const Utf8String & tableName,
                std::function<Utf8String (const Utf8String &)> keep)
{
    GetAllColumnsOutput result = outer.doGetAllColumns(tableName, keep);
    auto outerFn = result.exec;
    result.exec = [=] (const SqlRowScope & scope)
        {
            auto & row = static_cast<const RowContext &>(scope);
            return outerFn(row.outer);
        };
    return result;
}

VariableGetter
ReadThroughBindingContext::
doGetBoundParameter(const Utf8String & paramName)
{
    auto outerImpl = outer.doGetBoundParameter(paramName);

    return {[=] (const SqlRowScope & context,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                auto & row = static_cast<const RowContext &>(context);
                return outerImpl(row.outer, storage, filter);
            },
            outerImpl.info};
}

std::shared_ptr<Function>
ReadThroughBindingContext::
doGetFunctionEntity(const Utf8String & functionName)
{
    return outer.doGetFunctionEntity(functionName);
}

std::shared_ptr<Dataset>
ReadThroughBindingContext::
doGetDataset(const Utf8String & datasetName)
{
    return outer.doGetDataset(datasetName);
}

std::shared_ptr<Dataset>
ReadThroughBindingContext::
doGetDatasetFromConfig(const Any & datasetConfig)
{
    return outer.doGetDatasetFromConfig(datasetConfig);
}


/*****************************************************************************/
/* COLUMN EXPRESSION BINDING CONTEXT                                         */
/*****************************************************************************/

BoundFunction
ColumnExpressionBindingContext::
doGetFunction(const Utf8String & tableName,
              const Utf8String & functionName,
              const std::vector<std::shared_ptr<SqlExpression> > & args)
{

    if (functionName == "columnName") {
        return {[=] (const std::vector<BoundSqlExpression> & args,
                     const SqlRowScope & context)
                {
                    auto & col = static_cast<const ColumnContext &>(context);
                    return ExpressionValue(col.columnName.toUtf8String(),
                                           Date::negativeInfinity());
                },
                std::make_shared<Utf8StringValueInfo>()};
    }

    auto fn = outer.doGetColumnFunction(functionName);

    if (fn)
    {
         return {[=] (const std::vector<BoundSqlExpression> & args,
                 const SqlRowScope & context)
            {
                auto & col = static_cast<const ColumnContext &>(context);

                // consider changing the signature of the column function 
                // to let them evaluate their args as it is done with builtin
                std::vector<ExpressionValue> evaluatedArgs;
                evaluatedArgs.reserve(args.size());
                for (auto & arg: args)
                    evaluatedArgs.emplace_back(std::move(arg(context)));

                return fn(col.columnName, evaluatedArgs); 
            },
            std::make_shared<Utf8StringValueInfo>()};
    }

    auto sqlfn = SqlBindingScope::doGetFunction(tableName, functionName, args);

    if (sqlfn)
        return sqlfn;

    throw HttpReturnException(400, "Unknown function " + functionName + " in column expression");

}

/*****************************************************************************/
/* SQL EXPRESSION WHEN SCOPE                                                 */
/*****************************************************************************/

BoundFunction
SqlExpressionWhenScope::
doGetFunction(const Utf8String & tableName,
              const Utf8String & functionName,
              const std::vector<std::shared_ptr<SqlExpression> > & args)
{
    if (functionName == "timestamp") {
        isTupleDependent = true;
        return  {[=] (const std::vector<BoundSqlExpression> & args,
                      const SqlRowScope & scope)
                {
                    auto & row = static_cast<const RowScope &>(scope);
                    return ExpressionValue(row.ts, row.ts);
                },
                std::make_shared<TimestampValueInfo>()};
    }

    return ReadThroughBindingContext::doGetFunction(tableName, functionName,
                                                    args);
}


/*****************************************************************************/
/* SQL EXPRESSION PARAM SCOPE                                                */
/*****************************************************************************/

VariableGetter
SqlExpressionParamScope::
doGetBoundParameter(const Utf8String & paramName)
{
    return {[=] (const SqlRowScope & scope,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                
                auto & row = static_cast<const RowScope &>(scope);
                return storage = row.params(paramName);
            },
            std::make_shared<AnyValueInfo>() };
}


} // namespace MLDB
} // namespace Datacratic
