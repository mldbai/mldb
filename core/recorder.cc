/** recorder.cc
    Jeremy Barnes, 26 March 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.
*/

#include "recorder.h"


namespace MLDB {


/*****************************************************************************/
/* RECORDER                                                                  */
/*****************************************************************************/

void
Recorder::
recordRowExprDestructive(RowPath rowName,
                         ExpressionValue expr)
{
    recordRowExpr(rowName, expr);
}

void
Recorder::
recordRowDestructive(RowPath rowName,
                     std::vector<std::tuple<ColumnPath, CellValue, Date> > vals)
{
    recordRow(rowName, vals);
}

void
Recorder::
recordRowsDestructive(std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > rows)
{
    recordRows(rows);
}

void
Recorder::
recordRowsExprDestructive(std::vector<std::pair<RowPath, ExpressionValue > > rows)
{
    recordRowsExpr(rows);
}

void
Recorder::
finishedChunk()
{
}

std::function<void (RowPath rowName, Date timestamp,
                    CellValue * vals, size_t numVals,
                    std::vector<std::pair<ColumnPath, CellValue> > extra)>
Recorder::
specializeRecordTabular(const std::vector<ColumnPath> & columnNames)
{
    return [=] (RowPath rowName, Date timestamp,
                CellValue * vals, size_t numVals,
                std::vector<std::pair<ColumnPath, CellValue> > extra)
        {
            recordTabularImpl(std::move(rowName), timestamp,
                              vals, numVals, std::move(extra),
                              columnNames);
        };
}

void
Recorder::
recordTabularImpl(RowPath rowName,
                  Date timestamp,
                  CellValue * vals,
                  size_t numVals,
                  std::vector<std::pair<ColumnPath, CellValue> > extra,
                  const std::vector<ColumnPath> & columnNames)
{
    ExcAssertEqual(columnNames.size(), numVals);
    std::vector<std::tuple<ColumnPath, CellValue, Date> > result;
    result.reserve(numVals + extra.size());

    for (unsigned i = 0;  i < columnNames.size();  ++i) {
        result.emplace_back(columnNames[i], std::move(vals[i]), timestamp);
    }

    for (auto & e: extra) {
        result.emplace_back(std::move(e.first), std::move(e.second), timestamp);
    }

    recordRowDestructive(std::move(rowName), std::move(result));
}

} // namespace MLDB

