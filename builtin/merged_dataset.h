/** merged_dataset.h                                               -*- C++ -*-
    Jeremy Barnes, 28 February 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Dataset that is the combination of multiple underlying datasets.  The
    merge is done per row ID; those with the same row names will have the
    columns merged together.  In this way it's neither a union nor a join,
    but a merge.

*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/types/value_description_fwd.h"


namespace MLDB {


/*****************************************************************************/
/* MERGED DATASET CONFIG                                                     */
/*****************************************************************************/

struct MergedDatasetConfig {
    std::vector<PolyConfigT<const Dataset> > datasets;
};

DECLARE_STRUCTURE_DESCRIPTION(MergedDatasetConfig);


/*****************************************************************************/
/* MERGED DATASET                                                            */
/*****************************************************************************/

struct MergedDataset: public Dataset {

    MergedDataset(MldbServer * owner,
                  PolyConfig config,
                  const ProgressFunc & onProgress);
    
    /** Constructor used internally when creating a tree of merged datasets */
    MergedDataset(MldbServer * owner,
                  std::vector<std::shared_ptr<Dataset> > datasetsToMerge);

    virtual ~MergedDataset();

    virtual Any getStatus() const;
    virtual void recordRowItl(const RowPath & rowName,
          const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals)
    {
        throw MLDB::Exception("Dataset type doesn't allow recording");
    }

    virtual std::shared_ptr<MatrixView> getMatrixView() const;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;
    virtual std::shared_ptr<RowStream> getRowStream() const;

    virtual std::pair<Date, Date> getTimestampRange() const;

private:
    MergedDatasetConfig datasetConfig;
    struct Itl;
    std::shared_ptr<Itl> itl;
};

} // namespace MLDB

