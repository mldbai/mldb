/** joined_dataset.h                                               -*- C++ -*-
    Jeremy Barnes, 27 July 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Dataset that is the joined product of multiple underlying datasets.
*/

#pragma once

#include "mldb/sql/sql_expression.h"
#include "mldb/sql/table_expression_operations.h"
#include "mldb/sql/join_utils.h"
#include "mldb/core/dataset.h"
#include "mldb/types/value_description_fwd.h"


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
                  const ProgressFunc & onProgress);
    
    /** Constructor used internally when creating a tree of joined datasets
        where some are already bound.
    */
    JoinedDataset(SqlBindingScope & scope,
                  std::shared_ptr<TableExpression> leftExpr,
                  BoundTableExpression left,
                  std::shared_ptr<TableExpression> rightExpr,
                  BoundTableExpression right,
                  std::shared_ptr<SqlExpression> on,
                  JoinQualification qualification);

    virtual ~JoinedDataset();

    virtual Any getStatus() const;
    
    virtual std::shared_ptr<MatrixView> getMatrixView() const;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;
    virtual std::shared_ptr<RowStream> getRowStream() const;

    virtual void getChildAliases(std::vector<Utf8String>&) const;

    virtual BoundFunction
    overrideFunction(const Utf8String & tableName,
                     const Utf8String & functionName,
                     SqlBindingScope & scope) const;

    virtual RowPath getOriginalRowName(const Utf8String& tableName,
                                       const RowPath & name) const;

    virtual int getChainedJoinDepth() const;

    virtual ExpressionValue getRowExpr(const RowPath & row) const override;

private:

    BoundFunction
    overrideFunctionFromSide(JoinSide tableSide,
                         const Utf8String & tableName,
                         const Utf8String & functionName,
                         SqlBindingScope & scope) const;

    BoundFunction
    overrideFunctionFromChild(JoinSide tableSide,
                         const Utf8String & tableName,
                         const Utf8String & functionName,
                         SqlBindingScope & scope) const;

    JoinedDatasetConfig datasetConfig;
    struct Itl;
    std::shared_ptr<Itl> itl;
};

} // namespace MLDB

