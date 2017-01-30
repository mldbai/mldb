/** transposed_dataset.h                                               -*- C++ -*-
    Jeremy Barnes, 28 February 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/types/value_description_fwd.h"


namespace MLDB {


/*****************************************************************************/
/* TRANSPOSED DATASET CONFIG                                                 */
/*****************************************************************************/

struct TransposedDatasetConfig {
    PolyConfigT<const Dataset> dataset;
};

DECLARE_STRUCTURE_DESCRIPTION(TransposedDatasetConfig);


/*****************************************************************************/
/* TRANSPOSED DATASET                                                        */
/*****************************************************************************/

struct TransposedDataset: public Dataset {

    TransposedDataset(MldbServer * owner,
                      PolyConfig config,
                      const ProgressFunc & onProgress);
    
    /** Constructor used internally when creating a temporary transposition. */
    TransposedDataset(MldbServer * owner,
                      std::shared_ptr<Dataset> dataset);

    virtual ~TransposedDataset();

    virtual Any getStatus() const;

    virtual std::pair<Date, Date> getTimestampRange() const;

    virtual std::shared_ptr<MatrixView> getMatrixView() const;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;
    virtual std::shared_ptr<RowStream> getRowStream() const;
    virtual ExpressionValue getRowExpr(const RowPath & row) const;

private:
    TransposedDatasetConfig datasetConfig;
    struct Itl;
    std::shared_ptr<Itl> itl;
};

} // namespace MLDB

