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
              const std::vector<BoundSqlExpression> & args,
              SqlBindingScope & argScope)
{
    std::vector<BoundSqlExpression> outerArgs;		
    for (auto & arg: args) {		
        if (arg.metadata.isConstant)  //don't rebind constant expression since they don't need to access the row
            outerArgs.emplace_back(std::move(arg));		
        else		
            outerArgs.emplace_back(std::move(rebind(arg)));		
    }

    // Get the outer function		
    auto outerFunction = outer.doGetFunction(tableName, functionName, outerArgs, argScope);

    BoundFunction result = outerFunction;

    if (!outerFunction)
        return result;
  
    // Call it with the outer context
    result.exec = [=] (const std::vector<ExpressionValue> & args,
                       const SqlRowScope & context)
        {
            //ExcAssert(dynamic_cast<const RowContext *>(&context) != nullptr);
            auto & row = context.as<RowContext>();

            //cerr << "rebinding to apply function " << functionName
            //<< ": context type is "
            //<< ML::type_name(context) << " outer type is "
            //<< ML::type_name(row.outer) << endl;

            return outerFunction(args, row.outer);
        };

    return result;
}

BoundSqlExpression		
ReadThroughBindingContext::		
rebind(BoundSqlExpression expr)		
{		
    auto outerExec = expr.exec;		
		
    // Call the exec function with the context pivoted to the output context		
    expr.exec = [=] (const SqlRowScope & context,		
                     ExpressionValue & storage,
                     const VariableFilter & filter)		
        -> const ExpressionValue &		
        {		
            auto & row = static_cast<const RowContext &>(context);		
            return outerExec(row.outer, storage, filter);		
        };		
		
    return expr;		
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
                auto & row = context.as<RowContext>();
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
            auto & row = scope.as<RowContext>();
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
                auto & row = context.as<RowContext>();
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
              const std::vector<BoundSqlExpression> & args,
              SqlBindingScope & argScope)
{

    if (functionName == "columnName") {
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & context)
                {
                    auto & col = context.as<ColumnContext>();
                    return ExpressionValue(col.columnName.toUtf8String(),
                                           Date::negativeInfinity());
                },
                std::make_shared<Utf8StringValueInfo>()};
    }

    auto fn = outer.doGetColumnFunction(functionName);

    if (fn)
    {
         return {[=] (const std::vector<ExpressionValue> & evaluatedArgs,
                 const SqlRowScope & context)
            {
                auto & col = context.as<ColumnContext>();
                return fn(col.columnName, evaluatedArgs); 
            },
            std::make_shared<Utf8StringValueInfo>()};
    }

    auto sqlfn = SqlBindingScope::doGetFunction(tableName, functionName, args,
                                                argScope);

    if (sqlfn)
        return sqlfn;

    throw HttpReturnException(400, "Unknown function " + functionName + " in column expression");

}

VariableGetter 
ColumnExpressionBindingContext::
doGetVariable(const Utf8String & tableName, const Utf8String & variableName)
{
    throw HttpReturnException(400, "Cannot read column \"" + variableName + "\" inside COLUMN EXPR.");
}

/*****************************************************************************/
/* SQL EXPRESSION WHEN SCOPE                                                 */
/*****************************************************************************/

BoundFunction
SqlExpressionWhenScope::
doGetFunction(const Utf8String & tableName,
              const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              SqlBindingScope & argScope)
{
    if (functionName == "value_timestamp") {
        isTupleDependent = true;
        return  {[=] (const std::vector<ExpressionValue> & args,
                      const SqlRowScope & scope)
                {
                    auto & row = scope.as<RowScope>();
                    return ExpressionValue(row.ts, row.ts);
                },
                std::make_shared<TimestampValueInfo>()};
    }

    return ReadThroughBindingContext::doGetFunction(tableName, functionName,
                                                    args, argScope);
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
                
                auto & row = scope.as<RowScope>();
                return storage = row.params(paramName);
            },
            std::make_shared<AnyValueInfo>() };
}


} // namespace MLDB
} // namespace Datacratic
