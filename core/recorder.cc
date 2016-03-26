/** recorder.cc
    Jeremy Barnes, 26 March 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.
*/

#include "recorder.h"

namespace Datacratic {
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

} // namespace MLDB
} // namespace Datacratic
