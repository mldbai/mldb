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

    virtual void
    recordRowExpr(const RowName & rowName,
                  const ExpressionValue & expr) = 0;
    virtual void
    recordRowExprDestructive(RowName rowName,
                             ExpressionValue expr);
    virtual void
    recordRow(const RowName & rowName,
              const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals) = 0;
    virtual void
    recordRowDestructive(RowName rowName,
                         std::vector<std::tuple<ColumnName, CellValue, Date> > vals);
    virtual void
    recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows) = 0;
    virtual void
    recordRowsDestructive(std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > rows);

    virtual void
    recordRowsExpr(const std::vector<std::pair<RowName, ExpressionValue > > & rows) = 0;
    virtual
    void recordRowsExprDestructive(std::vector<std::pair<RowName, ExpressionValue > > rows);

    virtual void finishedChunk();
};


} // namespace MLDB
} // namespace Datacratic
