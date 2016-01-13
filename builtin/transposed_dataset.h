// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** transposed_dataset.h                                               -*- C++ -*-
    Jeremy Barnes, 28 February 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Dataset that is the combination of multiple underlying datasets.  The
    merge is done per row ID; those with the same row names will have the
    columns transposed together.  In this way it's neither a union nor a join,
    but a merge.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/types/value_description.h"

namespace Datacratic {
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
                      const std::function<bool (const Json::Value &)> & onProgress);
    
    /** Constructor used internally when creating a temporary transposition. */
    TransposedDataset(MldbServer * owner,
                      std::shared_ptr<Dataset> dataset);

    virtual ~TransposedDataset();

    virtual Any getStatus() const;

    virtual std::pair<Date, Date> getTimestampRange() const;

    virtual std::shared_ptr<MatrixView> getMatrixView() const;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;
    virtual std::shared_ptr<RowStream> getRowStream() const;

private:
    TransposedDatasetConfig datasetConfig;
    struct Itl;
    std::shared_ptr<Itl> itl;
};

} // namespace MLDB
} // namespace Datacratic
