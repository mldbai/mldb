/** recorder.h                                                     -*- C++ -*-
    Jeremy Barnes, 26 March 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Interface for recording into MLDB.
*/

#include "mldb/sql/dataset_fwd.h"
#include "mldb/sql/dataset_types.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/core/mldb_entity.h"
#include "mldb/sql/cell_value.h"
#include "mldb/sql/expression_value.h"

// NOTE TO MLDB DEVELOPERS: This is an API header file.  No includes
// should be added, especially value_description.h.


#pragma once

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* RECORDER                                                                  */
/*****************************************************************************/

/** A recorder is used to record data into a dataset or another abstraction. */

struct Recorder {
    virtual ~Recorder()
    {
    }

    /** Record an expression value as a row.  This will be flattened by
        datasets that require flattening.
    */
    virtual void
    recordRowExpr(const RowName & rowName,
                  const ExpressionValue & expr) = 0;

    /** See recordRowExpr().  This has the same effect, but takes an rvalue
        which is destroyed by the call.  This may result in performance
        improvements.
    */
    virtual void
    recordRowExprDestructive(RowName rowName,
                             ExpressionValue expr);

    /** Record a pre-flattened row. */
    virtual void
    recordRow(const RowName & rowName,
              const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals) = 0;

    /** See recordRow().  This has the same effect, but takes an rvalue
        which is destroyed by the call.  This may result in performance
        improvements.
    */
    virtual void
    recordRowDestructive(RowName rowName,
                         std::vector<std::tuple<ColumnName, CellValue, Date> > vals);

    /** Record multiple flattened rows in a single transaction.  Default
        implementation forwards to recordRow.
    */
    virtual void
    recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows) = 0;

    /** See recordRows().  This has the same effect, but takes an rvalue
        which is destroyed by the call.  This may result in performance
        improvements.
    */
    virtual void
    recordRowsDestructive(std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > rows);

    /** Record multiple rows from ExpressionValues.  */
    virtual void
    recordRowsExpr(const std::vector<std::pair<RowName, ExpressionValue > > & rows) = 0;

    /** See recordRowsExpr().  This has the same effect, but takes an rvalue
        which is destroyed by the call.  This may result in performance
        improvements.
    */
    virtual
    void recordRowsExprDestructive(std::vector<std::pair<RowName, ExpressionValue > > rows);

    /** Return a function specialized to record the same set of atomic values
        over and over again into this chunk.

        The returned function can be called row by row, without needing to
        mention column names (inferred by the positions).  The values will
        be destroyed on insertion.  numVals must always equal the number of
        columns passed.

        Extra allows for unexpected columns to be recorded, for when the
        dataset contains some fixed columns but also may have dynamic
        columns.
    */
    virtual
    std::function<void (RowName rowName,
                        Date timestamp,
                        CellValue * vals,
                        size_t numVals,
                        std::vector<std::pair<ColumnName, CellValue> > extra)>
    specializeRecordTabular(const std::vector<ColumnName> & columns);


    /** Default implementation of specializeRecordTabular.  It will construct
        a row and call recordRowDestructive.
    */
    void recordTabularImpl(RowName rowName,
                           Date timestamp,
                           CellValue * vals,
                           size_t numVals,
                           std::vector<std::pair<ColumnName, CellValue> > extra,
                           const std::vector<ColumnName> & columnNames);
    

    virtual void finishedChunk();
};


} // namespace MLDB
} // namespace Datacratic
