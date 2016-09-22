/**                                                                 -*- C++ -*-
 * union_dataset.h
 * Mich, 2016-09-14
 * This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
 **/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/types/value_description_fwd.h"

namespace MLDB {


/*****************************************************************************/
/* MERGED DATASET CONFIG                                                     */
/*****************************************************************************/

struct UnionDatasetConfig {
    std::vector<PolyConfigT<const Dataset> > datasets;
};

DECLARE_STRUCTURE_DESCRIPTION(UnionDatasetConfig);


/*****************************************************************************/
/* MERGED DATASET                                                            */
/*****************************************************************************/

struct UnionDataset: public Dataset {

    UnionDataset(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress);
    
    /** Constructor used internally when creating a tree of merged datasets */
    UnionDataset(MldbServer * owner,
                 std::vector<std::shared_ptr<Dataset> > datasetsToMerge);

    virtual ~UnionDataset();

    virtual Any getStatus() const;
    virtual void recordRowItl(const RowName & rowName,
        const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
    {
        throw ML::Exception("Dataset type doesn't allow recording");
    }

    virtual std::shared_ptr<MatrixView> getMatrixView() const;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;
    virtual std::shared_ptr<RowStream> getRowStream() const;

    virtual std::pair<Date, Date> getTimestampRange() const;

private:
    UnionDatasetConfig datasetConfig;
    struct Itl;
    std::shared_ptr<Itl> itl;
};

} // namespace MLDB
