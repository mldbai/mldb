/** sampled_dataset.h                                               -*- C++ -*-
    Francois Maillet, 11 janvier 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/types/value_description_fwd.h"


namespace MLDB {


/*****************************************************************************/
/* SAMPLED DATASET CONFIG                                                    */
/*****************************************************************************/
        
struct SampledDatasetConfig {

    SampledDatasetConfig();

    std::shared_ptr<TableExpression> dataset;

    unsigned seed;
    unsigned rows;
    float fraction;
    bool withReplacement;
};

DECLARE_STRUCTURE_DESCRIPTION(SampledDatasetConfig);


/*****************************************************************************/
/* SAMPLED DATASET                                                        */
/*****************************************************************************/

struct SampledDataset: public Dataset {

    SampledDataset(MldbServer * owner,
                      PolyConfig config,
                      const ProgressFunc & onProgress);
    
    /** Constructor used internally when creating a temporary sample */
    SampledDataset(MldbServer * owner,
                   std::shared_ptr<Dataset> dataset,
                   const ExpressionValue & options);

    virtual ~SampledDataset();

    virtual Any getStatus() const;

    virtual std::pair<Date, Date> getTimestampRange() const;

    virtual std::shared_ptr<MatrixView> getMatrixView() const;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;

    static std::string getErrorMsg(const std::string msg);

private:
    
    SampledDatasetConfig datasetConfig;
    BoundTableExpression bondTableExpression;

    struct Itl;
    std::shared_ptr<Itl> itl;
};

} // namespace MLDB

