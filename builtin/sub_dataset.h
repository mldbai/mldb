/** sub_dataset.h                                               -*- C++ -*-
    Mathieu Marquis Bolduc, August 28th 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Dataset that is the result of applying a SELECT statement
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/types/value_description_fwd.h"


namespace MLDB {


/*****************************************************************************/
/* SUB DATASET CONFIG                                                        */
/*****************************************************************************/

struct SubDatasetConfig {
    SelectStatement statement;
};

DECLARE_STRUCTURE_DESCRIPTION(SubDatasetConfig);


/*****************************************************************************/
/* SUB DATASET                                                               */
/*****************************************************************************/

struct SubDataset : public Dataset {

    SubDataset(MldbServer * owner,
               PolyConfig config,
               const ProgressFunc & onProgress);

    SubDataset(MldbServer * owner, 
               SubDatasetConfig config,
               const ProgressFunc & onProgress);

    SubDataset(MldbServer * owner,
               std::vector<NamedRowValue> rows);

    virtual ~SubDataset();

    virtual Any getStatus() const override;

    virtual std::shared_ptr<MatrixView> getMatrixView() const override;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const override;
    virtual std::shared_ptr<RowStream> getRowStream() const override;

    virtual std::pair<Date, Date> getTimestampRange() const override;

    virtual KnownColumn getKnownColumnInfo(const ColumnPath & columnName) const override;

    virtual std::vector<ColumnPath> getFlattenedColumnNames() const override;

    virtual size_t getFlattenedColumnCount() const override;

    virtual void recordRowExpr(const RowPath & rowName, const ExpressionValue & expr) override;
    virtual void recordRowsExpr(const std::vector<std::pair<RowPath, ExpressionValue> > & rows) override;
    virtual void recordRowItl(const RowPath & rowName, const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals) override;

    virtual ExpressionValue getRowExpr(const RowPath & row) const override;

private:
    SubDatasetConfig datasetConfig;
    struct Itl;
    std::shared_ptr<Itl> itl;
};

} // namespace MLDB

