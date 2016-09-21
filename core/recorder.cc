/** recorder.cc
    Jeremy Barnes, 26 March 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.
*/

#include "recorder.h"


namespace MLDB {


/*****************************************************************************/
/* RECORDER                                                                  */
/*****************************************************************************/

void
Recorder::
recordRowExprDestructive(RowName rowName,
                         ExpressionValue expr)
{
    recordRowExpr(rowName, expr);
}

void
Recorder::
recordRowDestructive(RowName rowName,
                     std::vector<std::tuple<ColumnName, CellValue, Date> > vals)
{
    recordRow(rowName, vals);
}

void
Recorder::
recordRowsDestructive(std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > rows)
{
    recordRows(rows);
}

void
Recorder::
recordRowsExprDestructive(std::vector<std::pair<RowName, ExpressionValue > > rows)
{
    recordRowsExpr(rows);
}

void
Recorder::
finishedChunk()
{
}

std::function<void (RowName rowName, Date timestamp,
                    CellValue * vals, size_t numVals,
                    std::vector<std::pair<ColumnName, CellValue> > extra)>
Recorder::
specializeRecordTabular(const std::vector<ColumnName> & columnNames)
{
    return [=] (RowName rowName, Date timestamp,
                CellValue * vals, size_t numVals,
                std::vector<std::pair<ColumnName, CellValue> > extra)
        {
            recordTabularImpl(std::move(rowName), timestamp,
                              vals, numVals, std::move(extra),
                              columnNames);
        };
}

void
Recorder::
recordTabularImpl(RowName rowName,
                  Date timestamp,
                  CellValue * vals,
                  size_t numVals,
                  std::vector<std::pair<ColumnName, CellValue> > extra,
                  const std::vector<ColumnName> & columnNames)
{
    ExcAssertEqual(columnNames.size(), numVals);
    std::vector<std::tuple<ColumnName, CellValue, Date> > result;
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

