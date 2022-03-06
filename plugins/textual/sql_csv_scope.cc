/** sql_csv_scope.h                                                -*- C++ -*-
    Jeremy Barnes, 27 June 2017
    Copyright (c) 2017 Element AI Inc.  All rights reserved.

*/

#include "sql_csv_scope.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/span_description.h"

using namespace std;


namespace MLDB {

/*****************************************************************************/
/* SQL CSV SCOPE                                                             */
/*****************************************************************************/

/** This allows an SQL expression to be bound to a parsed CSV row, which
    allowing it to find the variables, etc.
*/

SqlCsvScope::
SqlCsvScope(MldbEngine * engine,
            const std::span<const ColumnPath> columnNames,
            Date fileTimestamp, Utf8String dataFileUrl,
            bool canHaveExtra)
    : SqlExpressionMldbScope(engine),
      columnNames(columnNames),
      canHaveExtra(canHaveExtra),
      fileTimestamp(fileTimestamp),
      dataFileUrl(std::move(dataFileUrl))
{
    columnsUsed.resize(columnNames.size(), false);
}

ColumnGetter
SqlCsvScope::
doGetColumn(const Utf8String & tableName,
            const ColumnPath & columnName)
{
    if (!tableName.empty()) {
        throw AnnotatedException(400, "Unknown table name in import procedure",
                                  "tableName", tableName);
    }

    int index = std::find(columnNames.begin(), columnNames.end(), columnName)
        - columnNames.begin();

    if (index != columnNames.size()) {
        // The column name is in the index... extracting it is simply a case of
        // returning the indexed element.
        columnsUsed[index] = true;

        return {[=] (const SqlRowScope & scope,
                    ExpressionValue & storage,
                    const VariableFilter & filter) -> const ExpressionValue &
                {
                    auto & row = scope.as<RowScope>();
                    return storage = ExpressionValue(row.row[index],
                                                    row.ts);
                },
                std::make_shared<AtomValueInfo>()};
    }

    if (!canHaveExtra) {
        throw AnnotatedException(400, "Unknown column name in import procedure",
                                "columnName", columnName,
                                "knownColumnNames", columnNames);
    }

    extraUsed = true;

    // We have to look it up in extra, which is not indexed
    return {[=] (const SqlRowScope & scope,
                ExpressionValue & storage,
                const VariableFilter & filter) -> const ExpressionValue &
            {
                auto & row = scope.as<RowScope>();
                for (size_t i = 0;  i < row.numExtra;  ++i) {
                    auto & [path, value] = row.extra[i];
                    if (path == columnName)
                        return storage = ExpressionValue(value, row.ts);
                }
                return storage = ExpressionValue();
            },
            std::make_shared<AtomValueInfo>()};
}

GetAllColumnsOutput
SqlCsvScope::
doGetAllColumns(const Utf8String & tableName,
                const ColumnFilter& keep)
{
    vector<ColumnPath> toKeep;
    std::vector<KnownColumn> columnsWithInfo;
    size_t numToKeep = 0;

    for (unsigned i = 0;  i < columnNames.size();  ++i) {
        const ColumnPath & columnName = columnNames[i];
        const ColumnPath & outputName(keep(columnName));
        bool keep = !outputName.empty();
        toKeep.emplace_back(outputName);
        if (keep) {
            columnsUsed[i] = true;
            columnsWithInfo.emplace_back(outputName,
                                         std::make_shared<AtomValueInfo>(),
                                         COLUMN_IS_DENSE);
            ++numToKeep;
        }
    }

    // Fill out the offset so we know where it is in the input
    for (size_t i = 0;  i < columnsWithInfo.size();  ++i) {
        columnsWithInfo[i].offset = i;
    }

    if (canHaveExtra)
        extraUsed = true;

    auto exec = [=] (const SqlRowScope & scope, const VariableFilter & filter)
        {
            /* 
               The filter parameter here is not used since this context is
               only used when importing tabular data and there is no way to 
               specify a timestamp for this data.
            */

            ExcAssertEqual(columnNames.size(), toKeep.size());

            auto & row = scope.as<RowScope>();

            RowValue result;
            result.reserve(numToKeep);

            for (unsigned i = 0;  i < columnNames.size();  ++i) {
                if (!toKeep[i].empty())
                    result.emplace_back(columnNames[i], row.row[i], row.ts);
            }

            ExcAssertEqual(result.size(), numToKeep);

            if (canHaveExtra) {
                //cerr << "doing " << row.numExtra << " extra columns" << endl;
                // Filter the extra columns and keep those matching the expression
                for (size_t i = 0;  i < row.numExtra;  ++i) {
                    auto & [path, value] = row.extra[i]; 
                    //cerr << "  name " << path << " value " << value << endl;                   
                    ColumnPath outputName(keep(path));
                    if (!outputName.empty()) {
                        result.emplace_back(path, value, row.ts);
                    }
                }
            }

            return result;
        };

    GetAllColumnsOutput result;
    result.exec = exec;
    result.info = std::make_shared<RowValueInfo>(std::move(columnsWithInfo),
                                                 canHaveExtra ? SCHEMA_OPEN : SCHEMA_CLOSED);
    return result;
}

BoundFunction
SqlCsvScope::
doGetFunction(const Utf8String & tableName,
              const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              SqlBindingScope & argScope)
{
    if (functionName == "lineNumber") {
        lineNumberUsed = true;
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    auto & row = scope.as<RowScope>();
                    return ExpressionValue(row.lineNumber, fileTimestamp);
                },
                std::make_shared<IntegerValueInfo>()
                    };
    }
    else if (functionName == "rowHash") {
        lineNumberUsed = true;
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    auto & row = scope.as<RowScope>();
                    if(!row.rowName) {
                        throw MLDB::Exception("rowHash() not available in this scope");
                    }
                    return ExpressionValue(row.rowName->hash(), fileTimestamp);
                },
                std::make_shared<IntegerValueInfo>()
                    };
    }
    else if (functionName == "fileTimestamp") {
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    return ExpressionValue(fileTimestamp, fileTimestamp);
                },
                std::make_shared<TimestampValueInfo>()
                    };
    }
    else if (functionName == "dataFileUrl") {
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    return ExpressionValue(dataFileUrl, fileTimestamp);
                },
                std::make_shared<Utf8StringValueInfo>()
                    };
    }
    else if (functionName == "lineOffset") {
        return {[=] (const std::vector<ExpressionValue> & args,
                     const SqlRowScope & scope)
                {
                    auto & row = scope.as<RowScope>();
                    return ExpressionValue(row.lineOffset, fileTimestamp);
                },
                std::make_shared<IntegerValueInfo>()
                    };
    }
    return SqlBindingScope::doGetFunction(tableName, functionName, args,
                                          argScope);
}

} // namespace MLDB
