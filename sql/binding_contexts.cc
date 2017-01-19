/** binding_contexts.cc                                              -*- C++ -*-
    Jeremy Barnes, 14 March 2015

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


    Scopes in which to execute scoped SQL expressions.
*/

#include "binding_contexts.h"
#include "http/http_exception.h"
#include "builtin_functions.h"
#include "mldb/types/basic_value_descriptions.h"
#include <unordered_map>

using namespace std;



namespace MLDB {


/*****************************************************************************/
/* READ THROUGH BINDING SCOPE                                                */
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
        if (arg.info->isConst())  //don't rebind constant expression since they don't need to access the row
            outerArgs.emplace_back(std::move(arg));		
        else		
            outerArgs.emplace_back(rebind(arg));
    }

    // Get function from the outer scope
    auto outerFunction
        = outer.doGetFunction(tableName, functionName, outerArgs, argScope);

    BoundFunction result = outerFunction;

    if (!outerFunction)
        return result;
  
    // Call it with the outer scope
    result.exec = [=] (const std::vector<ExpressionValue> & args,
                       const SqlRowScope & scope)
        -> ExpressionValue
        {
            if (!scope.hasRow()) {
                // We don't normally need a scope for function calls, since their
                // arguments are already evaluated in the outer scope and their
                // value shouldn't depend upon anything but their arguments.
                //
                // If this is the case, we pass through an empty scope into
                // the context.
                return outerFunction(args, scope);
            }

            // Otherwise, we call through.
            auto & row = scope.as<RowScope>();
            return outerFunction(args, row.outer);
        };

    return result;
}

BoundSqlExpression		
ReadThroughBindingScope::		
rebind(BoundSqlExpression expr)		
{		
    auto outerExec = expr.exec;		
		
    // Call the exec function with the scope pivoted to the output scope		
    expr.exec = [=] (const SqlRowScope & scope,		
                     ExpressionValue & storage,
                     const VariableFilter & filter)		
        -> const ExpressionValue &		
        {		
            auto & row = static_cast<const RowScope &>(scope);		
            return outerExec(row.outer, storage, filter);		
        };		
		
    return expr;		
}


ColumnGetter
ReadThroughBindingScope::
doGetColumn(const Utf8String & tableName,
            const ColumnPath & columnName)
{
    auto outerImpl = outer.doGetColumn(tableName, columnName);

    return {[=] (const SqlRowScope & scope,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                auto & row = scope.as<RowScope>();
                return outerImpl(row.outer, storage, filter);
            },
            outerImpl.info};
}

GetAllColumnsOutput
ReadThroughBindingScope::
doGetAllColumns(const Utf8String & tableName,
                const ColumnFilter& keep)
{
    GetAllColumnsOutput result = outer.doGetAllColumns(tableName, keep);
    auto outerFn = result.exec;
    result.exec = [=] (const SqlRowScope & scope, const VariableFilter & filter)
        {
            auto & row = scope.as<RowScope>();
            return outerFn(row.outer, filter);
        };
    return result;
}

ColumnGetter
ReadThroughBindingScope::
doGetBoundParameter(const Utf8String & paramName)
{
    auto outerImpl = outer.doGetBoundParameter(paramName);

    return {[=] (const SqlRowScope & scope,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                auto & row = scope.as<RowScope>();
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
/* COLUMN EXPRESSION BINDING SCOPE                                         */
/*****************************************************************************/

BoundFunction
ColumnExpressionBindingScope::
doGetFunction(const Utf8String & tableName,
              const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              SqlBindingScope & argScope)
{

    if (functionName == "columnName") {
        checkArgsSize(args.size(), 0, "columnName()");
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    auto & col = scope.as<ColumnScope>();
                    return ExpressionValue(col.columnName.toUtf8String(),
                                           Date::negativeInfinity());
                },
                std::make_shared<Utf8StringValueInfo>()};
    }

    if (functionName == "columnPath") {
        checkArgsSize(args.size(), 0, "columnPath()");
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    auto & col = scope.as<ColumnScope>();
                    return ExpressionValue(CellValue(col.columnName),
                                           Date::negativeInfinity());
                },
                std::make_shared<PathValueInfo>()};
    }

    if (functionName == "columnPathElement") {
        checkArgsSize(args.size(), 1, "columnPathElement()");
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    ExcAssertEqual(args.size(), 1);
                    auto elementNum = args[0].getAtom().toInt();
                    auto & col = scope.as<ColumnScope>();
                    size_t index
                        = elementNum < 0
                        ? col.columnName.size() + elementNum
                        : elementNum;

                    if (index >= col.columnName.size()) {
                        return ExpressionValue::null(Date::negativeInfinity());
                    }

                    return ExpressionValue(col.columnName.at(index)
                                           .toUtf8String(),
                                           Date::negativeInfinity());
                },
                std::make_shared<Utf8StringValueInfo>()};
    }

    if (functionName == "columnPathLength") {
        checkArgsSize(args.size(), 0, "columnPathLength()");
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    auto & col = scope.as<ColumnScope>();
                    return ExpressionValue(col.columnName.size(),
                                           Date::negativeInfinity());
                },
                std::make_shared<IntegerValueInfo>()};
    }

    if (functionName == "value") {
        checkArgsSize(args.size(), 0, "value()");
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    auto & col = scope.as<ColumnScope>();
                    if (!col.columnValue)
                        throw HttpReturnException
                            (400,
                             "Evaluation value() in column "
                             "expression without columns");
                    return *col.columnValue;
                },
                std::make_shared<AnyValueInfo>()};
    }

    auto fn = outer.doGetColumnFunction(functionName);

    if (fn)
    {
         return {[=] (const std::vector<ExpressionValue> & evaluatedArgs,
                 const SqlRowScope & scope)
            {
                auto & col = scope.as<ColumnScope>();
                return fn(col.columnName, evaluatedArgs); 
            },
            std::make_shared<Utf8StringValueInfo>()};
    }

    // Look for columnPath() or columnPathElement()
    auto derivedFn = getDatasetDerivedFunction(tableName, functionName, args,
                                               argScope, *this, "column");

    if (derivedFn)
        return derivedFn;

    auto sqlfn = SqlBindingScope::doGetFunction(tableName, functionName, args,
                                                argScope);

    if (sqlfn)
        return sqlfn;

    throw HttpReturnException(400, "Unknown function " + functionName + " in column expression");

}

ColumnGetter 
ColumnExpressionBindingScope::
doGetColumn(const Utf8String & tableName, const ColumnPath & columnName)
{
    throw HttpReturnException(400, "Cannot read column '"
                              + columnName.toUtf8String()
                              + "' inside COLUMN EXPR");
}

GetAllColumnsOutput
ColumnExpressionBindingScope::
doGetAllColumns(const Utf8String & tableName,
                const ColumnFilter& keep)
{
    throw HttpReturnException(400, "Cannot use wildcard inside COLUMN EXPR");
}

ColumnPath
ColumnExpressionBindingScope::
doResolveTableName(const ColumnPath & fullVariableName,
                   Utf8String & tableName) const
{
    throw HttpReturnException
        (400, "Cannot use wildcard '"
         + fullVariableName.toUtf8String() + ".*' inside COLUMN EXPR");
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
/* SQL EXPRESSION EVAL SCOPE                                                 */
/*****************************************************************************/

ColumnGetter
SqlExpressionEvalScope::
doGetBoundParameter(const Utf8String & paramName)
{
    size_t argNum = jsonDecodeStr<size_t>(paramName);

    if (argNum == 0) {
        throw HttpReturnException
            (400, "Arguments start at 1, not 0, in SQL evaluate expression");
    }
    if (argNum > argInfo.size()) {
        throw HttpReturnException
            (400, "Attempt to obtain more arguments than exist when binding "
             "SQL evaluate expression");
    }
        
    return {[=] (const SqlRowScope & scope,
                 ExpressionValue & storage,
                 const VariableFilter & filter)
            -> const ExpressionValue &
            {
                auto & row = scope.as<RowScope>();
                ExcAssertLessEqual(argNum, row.numArgs);
                return storage = row.args[argNum - 1];
            },
            argInfo[argNum - 1]};
}


/*****************************************************************************/
/* SQL EXPRESSION EXTRACT SCOPE                                              */
/*****************************************************************************/

SqlExpressionExtractScope::
SqlExpressionExtractScope(SqlBindingScope & outer,
                          std::shared_ptr<ExpressionValueInfo> inputInfo)
    : outer(outer),
      inputInfo(inputInfo),
      wildcardsInInput(false)
{
    ExcAssert(this->inputInfo);
    this->functionStackDepth = outer.functionStackDepth + 1;
}

SqlExpressionExtractScope::
SqlExpressionExtractScope(SqlBindingScope & outer)
    : outer(outer),
      wildcardsInInput(false)
{
    this->functionStackDepth = outer.functionStackDepth + 1;
}

void
SqlExpressionExtractScope::
inferInput()
{
    std::vector<KnownColumn> knownColumns;

    for (auto & c: inferredInputs) {
        knownColumns.emplace_back(c, std::make_shared<AnyValueInfo>(),
                                  COLUMN_IS_SPARSE);
    }

    inputInfo.reset(new RowValueInfo(std::move(knownColumns),
                                     wildcardsInInput ? SCHEMA_OPEN : SCHEMA_CLOSED));
}

ColumnGetter
SqlExpressionExtractScope::
doGetColumn(const Utf8String & tableName,
            const ColumnPath & columnName)
{
    ExcAssert(!columnName.empty());
    
    // If we have a table name, we're not looking for a column in the
    // current scope
    if (!tableName.empty()) {
        throw HttpReturnException(400, "Cannot use table names inside an extract");
    }

    if (!inputInfo) {
        // We're in recording mode.  Simply record that we need this column.
        inferredInputs.insert(columnName);

        return {[=] (const SqlRowScope & scope,
                     ExpressionValue & storage,
                     const VariableFilter & filter)
                -> const ExpressionValue &
                {
                    auto & row = scope.as<RowScope>();
                    return storage = row.input.getNestedColumn(columnName, filter);
                },
                std::make_shared<AnyValueInfo>()};
    }

    // Ask the info about what type it is
    std::shared_ptr<ExpressionValueInfo> info
        = inputInfo->findNestedColumn(columnName);

    if (!info) {
        // Don't know the column.  Is it because it never exists, or because
        // the schema is dynamic, or because its deeper?
        if (inputInfo->getSchemaCompletenessRecursive() != SCHEMA_CLOSED
            || columnName.size() > 1) {
            // Dynamic columns; be prepared to do either depending upon
            // what we find

            // Set up our doGetColumn for if we need to go to the outer
            // scope.
            //auto outer = ReadThroughBindingScope
            //    ::doGetColumn(tableName, columnName);

            return {[=] (const SqlRowScope & scope,
                         ExpressionValue & storage,
                         const VariableFilter & filter)
                    -> const ExpressionValue &
                    {
                        auto & row = scope.as<RowScope>();
                        return storage = row.input.getNestedColumn(columnName, filter);
                    },
                    std::make_shared<AnyValueInfo>()};
        }

        // Didn't find the column and schema is closed.  Let it pass through.
        throw HttpReturnException
            (400, "Couldn't find column '" + columnName.toUtf8String()
             + "' in extract",
             "columnName", columnName,
             "inputInfo", inputInfo);
    }

    // Found the column.  Get it from our scope.
    return {[=] (const SqlRowScope & scope,
                 ExpressionValue & storage,
                 const VariableFilter & filter)
            -> const ExpressionValue &
            {
                auto & row = scope.as<RowScope>();
                return storage = row.input.getNestedColumn(columnName, filter);
            },
            info};
}

GetAllColumnsOutput
SqlExpressionExtractScope::
doGetAllColumns(const Utf8String & tableName,
                const ColumnFilter& keep)
{
    GetAllColumnsOutput result;

    if (!inputInfo || inputInfo->getSchemaCompletenessRecursive() != SCHEMA_CLOSED) {
        // In recording mode, or with dynamic columns; we filter once we have the
        // value

        // If we're recording our input and we asked for a wildcard, we know that
        // we may match unknown parts of our input value.
        if (!inputInfo)
            wildcardsInInput = true;

        result.exec = [=] (const SqlRowScope & scope, const VariableFilter & filter) -> ExpressionValue
            {
                auto & row = scope.as<RowScope>();

                RowValue output;

                auto onAtom = [&] (Path columnName,
                                  const Path & prefix,
                                  CellValue val,
                                  Date ts)
                {
                    ColumnPath outputColumnName
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
    std::unordered_map<ColumnPath, ColumnPath> toKeep;

    for (auto & c: inputColumns) {
        ColumnPath outputColumnName = keep(c.columnName);
        if (outputColumnName.empty())
            continue;

        toKeep[c.columnName] = outputColumnName;
        c.columnName = outputColumnName;

        outputColumns.emplace_back(std::move(c));
    }

    result.exec = [=] (const SqlRowScope & scope, const VariableFilter & filter) -> ExpressionValue
        {
            auto & row = scope.as<RowScope>();
            
            RowValue output;

            auto onAtom = [&] (const Path & columnName,
                               const Path & prefix,
                               CellValue val,
                               Date ts)
            {
                auto it = toKeep.find(prefix + columnName);
                if (it != toKeep.end()) {
                    output.emplace_back(it->second,
                                        std::move(val),
                                        ts);
                }
                else if (!prefix.empty()){
                    it = toKeep.find(prefix);
                    if (it != toKeep.end()) {
                        output.emplace_back(prefix + columnName,
                                            std::move(val),
                                            ts);
                    }
                }
                return true;
            };

            row.input.forEachAtom(onAtom);
            
            return std::move(output);
        };

    result.info = std::make_shared<RowValueInfo>(outputColumns, SCHEMA_CLOSED);

    return result;
}

BoundFunction
SqlExpressionExtractScope::
doGetFunction(const Utf8String & tableName,
              const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              SqlBindingScope & argScope)
{
    // Get function from the outer scope
    return outer.doGetFunction(tableName, functionName, args, argScope);
}

ColumnPath
SqlExpressionExtractScope::
doResolveTableName(const ColumnPath & fullVariableName,
                   Utf8String & tableName) const
{
    // Let the outer context resolve our table name
    return outer.doResolveTableName(fullVariableName, tableName);
}


/*****************************************************************************/
/* UTILITY FUNCTIONS                                                         */
/*****************************************************************************/

BoundFunction
getDatasetDerivedFunction(const Utf8String & tableName,
                          const Utf8String & functionName,
                          const std::vector<BoundSqlExpression> & args,
                          SqlBindingScope & argScope,
                          SqlBindingScope & datasetScope,
                          const Utf8String & baseFunctionName)
{
    // This will be intercepted if the outer scope doesn't implement the
    // rowPath function.
    if (functionName == baseFunctionName + "Path") {
        if (args.size() != 0)
            throw HttpReturnException
                (400, baseFunctionName + "() function takes no arguments");

        // Get the rowName() function
        BoundFunction rowNameFn = datasetScope
            .doGetFunction(tableName, baseFunctionName + "Name", {}, argScope);
        if (!rowNameFn)
            return BoundFunction();

        // Call it and parse the result
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & context)
                {
                    ExpressionValue rowName = rowNameFn(args, context);
                    return ExpressionValue
                        (CellValue(Path::parse(rowName.toUtf8String())),
                         rowName.getEffectiveTimestamp());
                },
                std::make_shared<PathValueInfo>()
            };
    }

    // This will be intercepted if the outer scope doesn't implement the
    // rowPathElement function.
    if (functionName == baseFunctionName + "PathElement") {
        if (args.size() != 1)
            throw HttpReturnException
                (400, baseFunctionName + "PathElement() function takes "
                 "one argument");

        // Get the rowPath() function
        BoundFunction rowPathFn = datasetScope
            .doGetFunction(tableName, baseFunctionName + "Path", {}, argScope);
        if (!rowPathFn)
            return BoundFunction();

        // Call it and parse the result
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & context)
                {
                    ExcAssertEqual(args.size(), 1);
                    ExpressionValue rowPath = rowPathFn(args, context);
                    Path asPath = rowPath.coerceToPath();
                    int64_t firstElement = args[0].getAtom().toInt();

                    if (firstElement < 0)
                        firstElement = asPath.size() + firstElement;

                    if (firstElement < 0 || firstElement >= asPath.size())
                        return ExpressionValue::null(args[0].getEffectiveTimestamp());

                    return ExpressionValue(asPath.at(firstElement).toUtf8String(),
                                           std::max(rowPath.getEffectiveTimestamp(),
                                                    args[0].getEffectiveTimestamp()));
                },
                std::make_shared<PathValueInfo>()
            };
    }

    return BoundFunction();
}


} // namespace MLDB

