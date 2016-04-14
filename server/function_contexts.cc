/** function_contexts.cc                                              -*- C++ -*-
    Jeremy Barnes, 14 March 2015
    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Contexts in which to execute the WITH and EXTRACT clauses of applying
    functions.
*/

#include "mldb/server/function_contexts.h"
#include "mldb/core/dataset.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/function_collection.h"

using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* EXTRACT CONTEXT                                                           */
/*****************************************************************************/

ExtractContext::
ExtractContext(MldbServer * server,
               FunctionValues values)
    : server(server), values(std::move(values))
{
}

ColumnGetter
ExtractContext::
doGetColumn(const Utf8String & tableName,
            const ColumnName & columnName)
{
    std::shared_ptr<ExpressionValueInfo> valueInfo;
    
    ExcAssert(!columnName.empty());

    // Ask our function info for how to convert the value to an
    // ExpressionValue
    auto it = values.values.find(columnName[0]);
    if (it == values.values.end()) {
        throw HttpReturnException(400, "unknown column "
                                  + columnName[0].toUtf8String() + " looking for "
                                  + columnName.toUtf8String());
    }

    // If we're asking for a one-element path, return it directly
    if (columnName.size() == 1) {
        valueInfo = it->second.valueInfo;

        return {[=] (const SqlRowScope & context,
                     ExpressionValue & storage,
                     const VariableFilter & filter)
                -> const ExpressionValue &
                {
                    ExcAssert(canIgnoreIfExactlyOneValue(filter));

                    auto & row = context.as<RowContext>();
                    return storage = std::move(row.input.get(columnName.toSimpleName()));
                },
                valueInfo};
    }
    
    // Recursively get the info and the value
    ColumnName suffix = columnName.removePrefix();
    valueInfo = it->second.valueInfo->findNestedColumn(suffix);

    return {[=] (const SqlRowScope & context,
                 ExpressionValue & storage,
                 const VariableFilter & filter)
            -> const ExpressionValue &
            {
                ExcAssert(canIgnoreIfExactlyOneValue(filter));

                auto & row = context.as<RowContext>();
                storage = std::move(row.input.get(columnName[0]));
                return storage = storage.getNestedColumn(suffix);
            },
            valueInfo
           };
}

GetAllColumnsOutput
getAllColumnsFromFunctionImpl(const Utf8String & tableName,
                              std::function<ColumnName (const ColumnName &)> keep,
                              const FunctionValues & input,
                              const std::function<ExpressionValue (const SqlRowScope &,
                                                                   const ColumnName &)> &
                              getValue)
{
    vector<KnownColumn> knownColumns;

    // List of input name -> outputName for those to keep
    std::vector<std::pair<ColumnName, ColumnName> > toKeep;

    for (auto & p: input.values) {
        ColumnName outputColumnName = keep(p.first.toCoord());
        if (outputColumnName.empty())
            continue;

        ColumnName inputColumnName(p.first.toCoord());
        toKeep.emplace_back(inputColumnName, outputColumnName);

        const FunctionValueInfo & functionValueInfo = p.second;
        const std::shared_ptr<ExpressionValueInfo> & valueInfo
            = functionValueInfo.valueInfo;


        KnownColumn column(outputColumnName, valueInfo, COLUMN_IS_DENSE);
        knownColumns.emplace_back(std::move(column));
    }

    GetAllColumnsOutput result;

    result.exec = [=] (const SqlRowScope & scope) -> ExpressionValue
        {
            StructValue output;

            for (auto & k: toKeep) {
                const ColumnName & inputColumnName = k.first;
                const ColumnName & outputColumnName = k.second;
                ExpressionValue val = getValue(scope, inputColumnName);
                // toSimpleName() is OK here since we know that functions
                // have only a single nesting level.
                output.emplace_back(outputColumnName.toSimpleName(),
                                    std::move(val));
            };
            
            return std::move(output);
        };

    result.info = std::make_shared<RowValueInfo>(knownColumns, SCHEMA_CLOSED);

    return result;
}

GetAllColumnsOutput
ExtractContext::
doGetAllColumns(const Utf8String & tableName,
                std::function<ColumnName (const ColumnName &)> keep)
{
    auto getValue = [&] (const SqlRowScope & context,
                         const ColumnName & col)

        {
            ExcAssert(!col.empty());
            auto & row = context.as<RowContext>();
            if (col.size() == 1) {
                return row.input.getValueOrNull(col[0]);
            }
            else {
                // Look recursively
                return row.input.getValueOrNull(col.head())
                    .getNestedColumn(col.tail());
            }
        };

    return getAllColumnsFromFunctionImpl(tableName, keep, values, getValue);
}

MldbServer *
ExtractContext::
getMldbServer() const
{
    return server;
}


/*****************************************************************************/
/* FUNCTION EXPRESSION CONTEXT                                               */
/*****************************************************************************/

FunctionExpressionContext::
FunctionExpressionContext(const MldbServer * mldb)
    : knownInput(false), mldb(const_cast<MldbServer *>(mldb))
{    
    ExcAssert(mldb != nullptr);
}

FunctionExpressionContext::
FunctionExpressionContext(const MldbServer * mldb, FunctionValues input, size_t outerFunctionStackDepth)
    : input(std::move(input)), knownInput(true), mldb(const_cast<MldbServer *>(mldb))
{
    functionStackDepth = outerFunctionStackDepth;
    ExcAssert(mldb != nullptr);
}

ColumnGetter
FunctionExpressionContext::
doGetColumn(const Utf8String & tableName,
            const ColumnName & columnName)
{
    std::shared_ptr<ExpressionValueInfo> valueInfo;

    //check if the variable could be in a row with unknown columns
    SchemaCompleteness schemaCompleteness(SCHEMA_CLOSED);

    // If we can't find this column, then we know it's a required input
    if (!findColumnRecursive(columnName, valueInfo, schemaCompleteness)) {
        if (knownInput && schemaCompleteness == SCHEMA_CLOSED) {
            throw HttpReturnException(400, "Required input '"
                                      + columnName.toUtf8String()
                                      + "' was not provided",
                                      "columnName", columnName,
                                      "input", input,
                                      "knownInput", knownInput);
        }

        
        valueInfo = std::make_shared<AnyValueInfo>();

        // TO RESOLVE BEFORE MERGE: toSimpleName()
        input.values.emplace(columnName.toSimpleName(),
                             FunctionValueInfo(valueInfo));
    }

    return {[=] (const SqlRowScope & context,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                ExcAssert(canIgnoreIfExactlyOneValue(filter));

                auto & row = context.as<RowContext>();

                return row.input.mustGet(columnName, storage);
            },
            valueInfo};
}

bool
FunctionExpressionContext::
findColumnRecursive(const ColumnName& columnName,
                    std::shared_ptr<ExpressionValueInfo>& valueInfo,
                    SchemaCompleteness& schemaCompleteness) const
{
    ExcAssert(!columnName.empty());

    // case 1: found directly
    auto it = input.values.find(columnName[0]);    
    if (it == input.values.end()) {
        return false;
    }

    if (columnName.size() == 1) {
        // TODO: check that type is compatible with known type
        valueInfo = it->second.valueInfo;
        return true;
    }
    // case 2: columnName is x.y, and we have x
    auto info = it->second.valueInfo; 
    info = info->findNestedColumn(columnName.removePrefix(columnName[0]));
    if (info) {
        valueInfo = info;
        return true;
    }

#if 0    
    // case 3: columnName is x, and we have x.y, x.z, etc.
    // Match the prefix and return everything as a row
    Utf8String toFind = columnName + ".";
    it = input.values.lower_bound(toFind);

    vector<KnownColumn> knownColumns;

    while (it != input.values.end() && it->first.startsWith(toFind)) {
        Utf8String name;
        name.removePrefix(toFind);
        knownColumns.emplace_back(ColumnName(name), it->second.valueInfo,
                                  COLUMN_IS_DENSE);
        ++it;
    }

    if (!knownColumns.empty()) {
        valueInfo = std::make_shared<RowValueInfo>(knownColumns);
        return true;
    }
#endif

    return false;
}

GetAllColumnsOutput
FunctionExpressionContext::
doGetAllColumns(const Utf8String & tableName,
                std::function<ColumnName (const ColumnName &)> keep)
{
    auto getValue = [&] (const SqlRowScope & context,
                         const ColumnName & col)

        {
            auto & row = context.as<RowContext>();
            // TO RESOLVE BEFORE MERGE: toSimpleName();
            return std::move(row.input.getValueOrNull(col.toSimpleName()));
};

    return getAllColumnsFromFunctionImpl(tableName, keep, input, getValue);
}

std::shared_ptr<Function>
FunctionExpressionContext::
doGetFunctionEntity(const Utf8String & functionName)
{
    return mldb->functions->getExistingEntity(functionName.rawString());
}

ColumnName
FunctionExpressionContext::
doResolveTableName(const ColumnName & fullVariableName, Utf8String &tableName) const
{
    return fullVariableName;
}


} // namespace MLDB
} // namespace Datacratic
