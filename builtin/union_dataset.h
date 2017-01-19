/**                                                                 -*- C++ -*-
 * union_dataset.h
 * Mich, 2016-09-14
 * This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
 **/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/types/value_description_fwd.h"

namespace MLDB {


/*****************************************************************************/
/* UNION DATASET CONFIG                                                      */
/*****************************************************************************/

struct UnionDatasetConfig {
    std::vector<PolyConfigT<const Dataset> > datasets;
};

DECLARE_STRUCTURE_DESCRIPTION(UnionDatasetConfig);


/*****************************************************************************/
/* UNION DATASET                                                             */
/*****************************************************************************/

struct UnionDataset: public Dataset {

    UnionDataset(MldbServer * owner,
                 PolyConfig config,
                 const ProgressFunc & onProgress);
    
    /** Constructor used internally when creating a datasets */
    UnionDataset(MldbServer * owner,
                 std::vector<std::shared_ptr<Dataset> > datasetsToMerge);

    virtual ~UnionDataset() override;

    virtual Any getStatus() const override;
    virtual void recordRowItl(const RowPath & rowPath,
        const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals) override
    {
        throw MLDB::Exception("Dataset type doesn't allow recording");
    }

    virtual std::shared_ptr<MatrixView> getMatrixView() const override;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const override;
    virtual std::shared_ptr<RowStream> getRowStream() const override;

    virtual std::pair<Date, Date> getTimestampRange() const override;
    virtual ExpressionValue getRowExpr(const RowPath & rowPath) const override;

private:
    UnionDatasetConfig datasetConfig;
    struct Itl;
    std::shared_ptr<Itl> itl;
};

} // namespace MLDB
