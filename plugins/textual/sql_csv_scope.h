/** sql_csv_scope.h                                                -*- C++ -*-
    Jeremy Barnes, 27 June 2017
    Copyright (c) 2017 Element AI Inc.  All rights reserved.

*/

#pragma once

#include "mldb/engine/dataset_scope.h"


namespace MLDB {


/*****************************************************************************/
/* SQL CSV SCOPE                                                             */
/*****************************************************************************/

/** This allows an SQL expression to be bound to a parsed CSV row, which
    allowing it to find the variables, etc.
*/

struct SqlCsvScope: public SqlExpressionMldbScope {

    struct RowScope: public SqlRowScope {
        RowScope(const CellValue * row, Date ts, int64_t lineNumber,
                 int64_t lineOffset)
            : row(row), ts(ts), lineNumber(lineNumber), lineOffset(lineOffset)
        {
        }

        const CellValue * row;
        Date ts;
        int64_t lineNumber;
        int64_t lineOffset;
        const RowPath * rowName;
    };

    SqlCsvScope(MldbEngine * engine,
                const std::vector<ColumnPath> & columnNames,
                Date fileTimestamp, Utf8String dataFileUrl);

    /// Column names passed in to the scope
    const std::vector<ColumnPath> & columnNames;

    /// Which columns are accessed by the bound expressions?  This is really
    /// a std::vector<bool> as it has one entry per column, rather than listing
    /// the indexes.
    std::vector<int> columnsUsed;

    /// Is the line number required by the bound expression?  Some optimizations
    /// can be turned off if not.
    bool lineNumberUsed;

    /// What is the timestamp for the actual file itself?  This is used as a
    /// default timestamp on values returned.
    Date fileTimestamp;

    /// What is the URI for this file?
    Utf8String dataFileUrl;

    virtual ColumnGetter doGetColumn(const Utf8String & tableName,
                                     const ColumnPath & columnName);

    GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    const ColumnFilter& keep);

    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope);

    static RowScope bindRow(const CellValue * row, Date ts,
                            int64_t lineNumber, int64_t lineOffset)
    {
        return RowScope(row, ts, lineNumber, lineOffset);
    }
};



} // namespace MLDB
