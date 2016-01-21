// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** joined_dataset.h                                               -*- C++ -*-
    Jeremy Barnes, 27 July 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Dataset that is the joined product of multiple underlying datasets.
*/

#pragma once

#include "mldb/sql/sql_expression.h"
#include "mldb/sql/table_expression_operations.h"
#include "mldb/core/dataset.h"
#include "mldb/types/value_description.h"

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* JOINED DATASET CONFIG                                                     */
/*****************************************************************************/

struct JoinedDatasetConfig {
    std::shared_ptr<TableExpression> left;
    std::shared_ptr<TableExpression> right;
    std::shared_ptr<SqlExpression> on;
    JoinQualification qualification;
};

DECLARE_STRUCTURE_DESCRIPTION(JoinedDatasetConfig);


/*****************************************************************************/
/* JOINED DATASET                                                            */
/*****************************************************************************/

struct JoinedDataset: public Dataset {

    JoinedDataset(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress);
    
    /** Constructor used internally when creating a tree of joined datasets */
    JoinedDataset(MldbServer * owner,
                  JoinedDatasetConfig config);

    virtual ~JoinedDataset();

    virtual Any getStatus() const;
    
    virtual std::shared_ptr<MatrixView> getMatrixView() const;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;
    virtual std::shared_ptr<RowStream> getRowStream() const;

    virtual void getChildAliases(std::vector<Utf8String>&) const;

private:
    JoinedDatasetConfig datasetConfig;
    struct Itl;
    std::shared_ptr<Itl> itl;
};

} // namespace MLDB
} // namespace Datacratic
