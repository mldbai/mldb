/** binding_contexts.cc                                              -*- C++ -*-
    Jeremy Barnes, 14 March 2015

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


    Contexts in which to execute scoped SQL expressions.
*/

#include "binding_contexts.h"
#include "http/http_exception.h"
#include <unordered_map>

using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* READ THROUGH BINDING CONTEXT                                              */
/*****************************************************************************/

BoundFunction
ReadThroughBindingScope::
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
            //ExcAssert(dynamic_cast<const RowScope *>(&context) != nullptr);
            auto & row = context.as<RowScope>();

            //cerr << "rebinding to apply function " << functionName
            //<< ": context type is "
            //<< ML::type_name(context) << " outer type is "
            //<< ML::type_name(row.outer) << endl;

            return outerFunction(args, row.outer);
        };

    return result;
}

BoundSqlExpression		
ReadThroughBindingScope::		
rebind(BoundSqlExpression expr)		
{		
    auto outerExec = expr.exec;		
		
    // Call the exec function with the context pivoted to the output context		
    expr.exec = [=] (const SqlRowScope & context,		
                     ExpressionValue & storage,
                     const VariableFilter & filter)		
        -> const ExpressionValue &		
        {		
            auto & row = static_cast<const RowScope &>(context);		
            return outerExec(row.outer, storage, filter);		
        };		
		
    return expr;		
}


ColumnGetter
ReadThroughBindingScope::
doGetColumn(const Utf8String & tableName,
            const ColumnName & columnName)
{
    auto outerImpl = outer.doGetColumn(tableName, columnName);

    return {[=] (const SqlRowScope & context,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                auto & row = context.as<RowScope>();
                return outerImpl(row.outer, storage, filter);
            },
            outerImpl.info};
}

GetAllColumnsOutput
ReadThroughBindingScope::
doGetAllColumns(const Utf8String & tableName,
                std::function<ColumnName (const ColumnName &)> keep)
{
    GetAllColumnsOutput result = outer.doGetAllColumns(tableName, keep);
    auto outerFn = result.exec;
    result.exec = [=] (const SqlRowScope & scope)
        {
            auto & row = scope.as<RowScope>();
            return outerFn(row.outer);
        };
    return result;
}

ColumnGetter
ReadThroughBindingScope::
doGetBoundParameter(const Utf8String & paramName)
{
    auto outerImpl = outer.doGetBoundParameter(paramName);

    return {[=] (const SqlRowScope & context,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                auto & row = context.as<RowScope>();
                return outerImpl(row.outer, storage, filter);
            },
            outerImpl.info};
}

std::shared_ptr<Dataset>
ReadThroughBindingScope::
doGetDataset(const Utf8String & datasetName)
{
    return outer.doGetDataset(datasetName);
}

std::shared_ptr<Dataset>
ReadThroughBindingScope::
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

ColumnGetter 
ColumnExpressionBindingContext::
doGetColumn(const Utf8String & tableName, const ColumnName & columnName)
{
    throw HttpReturnException(400, "Cannot read column \"" + columnName.toUtf8String() + "\" inside COLUMN EXPR.");
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

    return ReadThroughBindingScope::doGetFunction(tableName, functionName,
                                                    args, argScope);
}


/*****************************************************************************/
/* SQL EXPRESSION PARAM SCOPE                                                */
/*****************************************************************************/

ColumnGetter
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


/*****************************************************************************/
/* SQL EXPRESSION EXTRACT SCOPE                                              */
/*****************************************************************************/

SqlExpressionExtractScope::
SqlExpressionExtractScope(SqlBindingScope & outer,
                          std::shared_ptr<ExpressionValueInfo> inputInfo)
    : ReadThroughBindingScope(outer),
      inputInfo(ExpressionValueInfo::toRow(inputInfo))
{
    ExcAssert(this->inputInfo);
}

SqlExpressionExtractScope::
SqlExpressionExtractScope(SqlBindingScope & outer)
    : ReadThroughBindingScope(outer)
{
}

void
SqlExpressionExtractScope::
inferInput()
{
    throw HttpReturnException(600, "SqlExpressionExtractScope::inferInput()");
}

ColumnGetter
SqlExpressionExtractScope::
doGetColumn(const Utf8String & tableName,
            const ColumnName & columnName)
{
    ExcAssert(!columnName.empty());
    
    // If we have a table name, we're not looking for a column in the
    // current scope
    if (!tableName.empty()) {
        throw HttpReturnException(400, "Cannot use table names inside an extract");
        return ReadThroughBindingScope::doGetColumn(tableName, columnName);
    }

    // Ask the info about what type it is
    std::shared_ptr<ExpressionValueInfo> info
        = inputInfo->findNestedColumn(columnName);

    if (!info) {
        // Don't know the column.  Is it because it never exists, or because
        // the schema is dynamic?
        if (inputInfo->getSchemaCompleteness() != SCHEMA_CLOSED) {
            // Dynamic columns; be prepared to do either depending upon
            // what we find

            // Set up our doGetColumn for if we need to go to the outer
            // scope.
            //auto outer = ReadThroughBindingScope
            //    ::doGetColumn(tableName, columnName);

            return {[=] (const SqlRowScope & context,
                         ExpressionValue & storage,
                         const VariableFilter & filter)
                    -> const ExpressionValue &
                    {
                        auto & row = context.as<RowScope>();

#if 1 // don't allow reads through to outer scope
                        return storage = row.input.getNestedColumn(columnName, filter);
                        return storage;
#else // do allow reads to outer scope
                        bool found;
                        std::tie(storage, found)
                            = row.input.tryGetNestedColumn(columnName, filter);

                        if (found) {
                            return storage;
                        }
                        else {
                            return outer(row.outer, context, storage);
                        }
#endif
                    },
                    std::make_shared<AnyValueInfo>()};
        }

        // Didn't find the column and schema is closed.  Let it pass through.
        throw HttpReturnException(400, "Couldn't find column in extract");
        return ReadThroughBindingScope::doGetColumn(tableName, columnName);
    }

    // Found the column.  Get it from our context.
    return {[=] (const SqlRowScope & context,
                 ExpressionValue & storage,
                 const VariableFilter & filter)
            -> const ExpressionValue &
            {
                auto & row = context.as<RowScope>();
                return storage = row.input.getNestedColumn(columnName, filter);
            },
            info};
}

GetAllColumnsOutput
SqlExpressionExtractScope::
doGetAllColumns(const Utf8String & tableName,
                std::function<ColumnName (const ColumnName &)> keep)
{
    GetAllColumnsOutput result;

    if (inputInfo->getSchemaCompleteness() != SCHEMA_CLOSED) {
        // Dynamic columns; we filter once we have the value

        result.exec = [=] (const SqlRowScope & scope) -> ExpressionValue
            {
                auto & row = scope.as<RowScope>();

                RowValue output;

                auto onAtom = [&] (Coords columnName,
                                  const Coords & prefix,
                                  CellValue val,
                                  Date ts)
                {
                    ColumnName outputColumnName
                        = keep(prefix + std::move(columnName));
                    if (outputColumnName.empty())
                        return true;
                    output.emplace_back(std::move(outputColumnName),
                                        std::move(val),
                                        ts);
                    return true;
                };

                row.input.forEachAtom(onAtom);
            
                return std::move(output);
            };

        result.info = std::make_shared<UnknownRowValueInfo>();

        return result;
    }

    vector<KnownColumn> inputColumns
        = inputInfo->getKnownColumns();

    vector<KnownColumn> outputColumns;

    // List of input name -> outputName for those to keep
    std::unordered_map<ColumnName, ColumnName> toKeep;

    for (auto & c: inputColumns) {
        ColumnName outputColumnName = keep(c.columnName);
        if (outputColumnName.empty())
            continue;

        toKeep[c.columnName] = outputColumnName;
        c.columnName = outputColumnName;

        outputColumns.emplace_back(std::move(c));
    }

    result.exec = [=] (const SqlRowScope & scope) -> ExpressionValue
        {
            auto & row = scope.as<RowScope>();
            
            RowValue output;

            auto onAtom = [&] (const Coords & columnName,
                               const Coords & prefix,
                               CellValue val,
                               Date ts)
            {
                auto it = toKeep.find(prefix + columnName);
                if (it != toKeep.end()) {
                    output.emplace_back(it->second,
                                        std::move(val),
                                        ts);
                }
                return true;
            };

            row.input.forEachAtom(onAtom);
            
            return std::move(output);
        };

    result.info = std::make_shared<RowValueInfo>(outputColumns, SCHEMA_CLOSED);

    return result;
}

} // namespace MLDB
} // namespace Datacratic
