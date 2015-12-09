// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** function_contexts.cc                                              -*- C++ -*-
    Jeremy Barnes, 14 March 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Contexts in which to execute the WITH and EXTRACT clauses of applying
    functions.
*/

#include "mldb/server/function_contexts.h"
#include "mldb/core/dataset.h"
#include "mldb/types/basic_value_descriptions.h"


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

VariableGetter
ExtractContext::
doGetVariable(const Utf8String & tableName,
              const Utf8String & variableName)
{
    std::shared_ptr<ExpressionValueInfo> valueInfo;
    
    // Ask our function info for how to convert the value to an
    // ExpressionValue
    auto it = values.values.find(variableName);
    if (it == values.values.end()) {
        auto jt = variableName.find('.');
        if (jt == variableName.end())
            throw HttpReturnException(400, "unknown value " + variableName);
        
        // It's x.y.  Look for the x;
        Utf8String head(variableName.begin(), jt);
        ++jt;
        it = values.values.find(head);
        if (it == values.values.end())
            throw HttpReturnException(400, "unknown value " + head + " looking for "
                                      + variableName);
        
        Utf8String tail(jt, variableName.end());

        valueInfo = std::make_shared<AnyValueInfo>();
    }
    else valueInfo = it->second.valueInfo;

    return {[=] (const SqlRowScope & context,
                 ExpressionValue & storage,
                 const VariableFilter & filter)
            -> const ExpressionValue &
            {
                ExcAssert(canIgnoreIfExactlyOneValue(filter));

                auto & row = static_cast<const RowContext &>(context);
                return storage = std::move(row.input.get(variableName));
            },
            valueInfo};
}

GetAllColumnsOutput
getAllColumnsFromFunctionImpl(const Utf8String & tableName,
                              std::function<Utf8String (const Utf8String &)> keep,
                              const FunctionValues & input,
                              const std::function<ExpressionValue (const SqlRowScope &,
                                                                   const ColumnName &)> &
                                  getValue)
{
    vector<KnownColumn> knownColumns;

    // List of input name -> outputName for those to keep
    std::vector<std::pair<ColumnName, ColumnName> > toKeep;

    for (auto & p: input.values) {
        Utf8String outputName = keep(p.first.toUtf8String());
        if (outputName.empty())
            continue;

        ColumnName outputColumnName(outputName);
        ColumnName inputColumnName(p.first.toId());
        toKeep.emplace_back(inputColumnName, outputColumnName);

        const FunctionValueInfo & functionValueInfo = p.second;
        const std::shared_ptr<ExpressionValueInfo> & valueInfo = functionValueInfo.valueInfo;

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
                output.emplace_back(outputColumnName, std::move(val));
            };
            
            return std::move(output);
        };

    result.info = std::make_shared<RowValueInfo>(knownColumns, SCHEMA_CLOSED);

    return result;
}

GetAllColumnsOutput
ExtractContext::
doGetAllColumns(const Utf8String & tableName,
                std::function<Utf8String (const Utf8String &)> keep)
{
    auto getValue = [&] (const SqlRowScope & context,
                         const ColumnName & col)

        {
            auto & row = static_cast<const RowContext &>(context);
            return std::move(row.input.getValueOrNull(col.toUtf8String()));
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
FunctionExpressionContext(SqlBindingScope & context)
    : ReadThroughBindingContext(context), knownInput(false)
{
}

FunctionExpressionContext::
FunctionExpressionContext(SqlBindingScope & context, FunctionValues input)
    : ReadThroughBindingContext(context), input(std::move(input)), knownInput(true)
{
}

VariableGetter
FunctionExpressionContext::
doGetVariable(const Utf8String & tableName,
              const Utf8String & variableName)
{
    //cerr << "doGetVariable " << variableName << " on input " << jsonEncode(input) <<  " knownInput " << knownInput << endl;


    std::shared_ptr<ExpressionValueInfo> valueInfo;
    SchemaCompleteness schemaCompleteness(SCHEMA_CLOSED); //check if the variable could be in a row with unknown columns
    if (!findVariableRecursive(variableName, valueInfo, schemaCompleteness))
    {
        if (knownInput && schemaCompleteness == SCHEMA_CLOSED) {
            throw HttpReturnException(400, "Required input '" + variableName + "' was not provided");
        }

        
        valueInfo = std::make_shared<AnyValueInfo>();

        input.values.insert(make_pair(variableName, FunctionValueInfo(valueInfo)));
    }

    return {[=] (const SqlRowScope & context,
                 ExpressionValue & storage,
                 const VariableFilter & filter) -> const ExpressionValue &
            {
                ExcAssert(canIgnoreIfExactlyOneValue(filter));

                auto & row = static_cast<const RowContext &>(context);

                return row.input.mustGet(variableName, storage);
            },
            valueInfo};
}

bool FunctionExpressionContext::
findVariableRecursive(const Utf8String& variableName,
                      std::shared_ptr<ExpressionValueInfo>& valueInfo,
                      SchemaCompleteness& schemaCompleteness) const
{
    // case 1: found directly
    auto it = input.values.find(variableName);    
    if (it != input.values.end()) {
        // TODO: check that type is compatible with known type
        valueInfo = it->second.valueInfo;
        return true;
    }

    // case 2: variableName is x.y, and we have x
    auto jt = variableName.find('.');
    if (jt != variableName.end()) {
        // It's x.y.  Look for the x;
        Utf8String head(variableName.begin(), jt);
        ++jt;
        auto kt = input.values.find(head);

        if (kt != input.values.end()) {
            Utf8String tail(jt, variableName.end());
            auto info = kt->second.valueInfo; 

            info = info->findNestedColumn(tail, schemaCompleteness);

            if (info) {
                valueInfo = info;
                return true;
            }
        }
    }
    
    // case 3: variableName is x, and we have x.y, x.z, etc.
    // Match the prefix and return everything as a row
    Utf8String toFind = variableName + ".";
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

    return false;
}

GetAllColumnsOutput
FunctionExpressionContext::
doGetAllColumns(const Utf8String & tableName,
                std::function<Utf8String (const Utf8String &)> keep)
{
    auto getValue = [&] (const SqlRowScope & context,
                         const ColumnName & col)

        {
            auto & row = static_cast<const RowContext &>(context);
            return std::move(row.input.getValueOrNull(col));
        };

    return getAllColumnsFromFunctionImpl(tableName, keep, input, getValue);
}

} // namespace MLDB
} // namespace Datacratic
