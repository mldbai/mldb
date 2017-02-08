/** recorder.h                                                     -*- C++ -*-
    Jeremy Barnes, 26 March 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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
    recordRowExpr(const RowPath & rowName,
                  const ExpressionValue & expr) = 0;

    /** See recordRowExpr().  This has the same effect, but takes an rvalue
        which is destroyed by the call.  This may result in performance
        improvements.
    */
    virtual void
    recordRowExprDestructive(RowPath rowName,
                             ExpressionValue expr);

    /** Record a pre-flattened row. */
    virtual void
    recordRow(const RowPath & rowName,
              const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals) = 0;

    /** See recordRow().  This has the same effect, but takes an rvalue
        which is destroyed by the call.  This may result in performance
        improvements.
    */
    virtual void
    recordRowDestructive(RowPath rowName,
                         std::vector<std::tuple<ColumnPath, CellValue, Date> > vals);

    /** Record multiple flattened rows in a single transaction.  Default
        implementation forwards to recordRow.
    */
    virtual void
    recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows) = 0;

    /** See recordRows().  This has the same effect, but takes an rvalue
        which is destroyed by the call.  This may result in performance
        improvements.
    */
    virtual void
    recordRowsDestructive(std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > rows);

    /** Record multiple rows from ExpressionValues.  */
    virtual void
    recordRowsExpr(const std::vector<std::pair<RowPath, ExpressionValue > > & rows) = 0;

    /** See recordRowsExpr().  This has the same effect, but takes an rvalue
        which is destroyed by the call.  This may result in performance
        improvements.
    */
    virtual
    void recordRowsExprDestructive(std::vector<std::pair<RowPath, ExpressionValue > > rows);

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
    std::function<void (RowPath rowName,
                        Date timestamp,
                        CellValue * vals,
                        size_t numVals,
                        std::vector<std::pair<ColumnPath, CellValue> > extra)>
    specializeRecordTabular(const std::vector<ColumnPath> & columns);


    /** Default implementation of specializeRecordTabular.  It will construct
        a row and call recordRowDestructive.
    */
    void recordTabularImpl(RowPath rowName,
                           Date timestamp,
                           CellValue * vals,
                           size_t numVals,
                           std::vector<std::pair<ColumnPath, CellValue> > extra,
                           const std::vector<ColumnPath> & columnNames);
    

    virtual void finishedChunk();
};


} // namespace MLDB

