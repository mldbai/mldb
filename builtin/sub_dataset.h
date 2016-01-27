/** sub_dataset.h                                               -*- C++ -*-
    Mathieu Marquis Bolduc, August 28th 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Dataset that is the result of applying a SELECT statement
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/types/value_description.h"

namespace Datacratic {
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
               const std::function<bool (const Json::Value &)> & onProgress);

    SubDataset(MldbServer * owner, SubDatasetConfig config);

    virtual ~SubDataset();

    virtual Any getStatus() const;

    virtual std::shared_ptr<MatrixView> getMatrixView() const;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;
    virtual std::shared_ptr<RowStream> getRowStream() const;

    virtual std::pair<Date, Date> getTimestampRange() const;

private:
    SubDatasetConfig datasetConfig;
    struct Itl;
    std::shared_ptr<Itl> itl;
};

} // namespace MLDB
} // namespace Datacratic
