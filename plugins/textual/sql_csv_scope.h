/** sql_csv_scope.h                                                -*- C++ -*-
    Jeremy Barnes, 27 June 2017
    Copyright (c) 2017 Element AI Inc.  All rights reserved.

*/

#pragma once

#include "mldb/core/dataset_scope.h"
#include <span>

namespace MLDB {


/*****************************************************************************/
/* SQL CSV SCOPE                                                             */
/*****************************************************************************/

/** This allows an SQL expression to be bound to a parsed CSV row, which
    allowing it to find the variables, etc.
*/

struct SqlCsvScope: public SqlExpressionMldbScope {

    struct RowScope: public SqlRowScope {
        RowScope(const CellValue * row,
                 uint32_t numValuesInRow,
                 const std::pair<Path, CellValue> * extra,
                 uint32_t numExtra,
                 Date ts, int64_t lineNumber,
                 int64_t lineOffset)
            : row(row), numValuesInRow(numValuesInRow),
              extra(extra), numExtra(numExtra),
              ts(ts), lineNumber(lineNumber), lineOffset(lineOffset)
        {
        }

        const CellValue * row = nullptr;
        uint32_t numValuesInRow = 0;
        const std::pair<Path, CellValue> * extra = nullptr;
        uint32_t numExtra = 0;
        Date ts;
        int64_t lineNumber;
        int64_t lineOffset;
        const RowPath * rowName;
    };

    SqlCsvScope() = default;

    SqlCsvScope(MldbEngine * engine,
                std::span<const ColumnPath> columnNames,
                Date fileTimestamp, Utf8String dataFileUrl,
                bool canHaveExtra);

    /// Column names passed in to the scope
    std::span<const ColumnPath> columnNames;

    /// Which columns are accessed by the bound expressions?  This is really
    /// a std::vector<bool> as it has one entry per column, rather than listing
    /// the indexes.
    std::vector<int> columnsUsed;

    /// Can we have extra columns?  If true, it means that we may (on a row by row
    /// basis) have extra, unexpected values.
    bool canHaveExtra = false;

    /// Are the extra columns used?  If not, we can make extra optimizations.
    bool extraUsed = false;

    /// Is the line number required by the bound expression?  Some optimizations
    /// can be turned off if not.
    bool lineNumberUsed = false;

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

    /*static*/ RowScope bindRow(const CellValue * row, uint32_t numValuesInRow,
                            Date ts,
                            int64_t lineNumber, int64_t lineOffset)
    {
        ExcAssertEqual(numValuesInRow, columnNames.size());
        ExcAssertEqual(numValuesInRow, columnsUsed.size());
        return RowScope(row, numValuesInRow, nullptr /* extra */, 0 /* numExtra */, ts, lineNumber, lineOffset);
    }

    /*static*/ RowScope bindRow(const CellValue * row, uint32_t numValuesInRow,
                            const std::pair<Path, CellValue> * extra, uint32_t numExtra,
                            Date ts, int64_t lineNumber, int64_t lineOffset)
    {
        ExcAssertEqual(numValuesInRow, columnNames.size());
        ExcAssertEqual(numValuesInRow, columnsUsed.size());
        return RowScope(row, numValuesInRow, extra, numExtra, ts, lineNumber, lineOffset);
    }
};



} // namespace MLDB
