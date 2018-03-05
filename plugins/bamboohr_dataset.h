/** bamboohr_dataset.h                                           -*- C++ -*-
    Jeremy Barnes, 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Dataset that bamboohrly records and expires on a time window.
*/

#pragma once


#include "mldb/core/dataset.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/server/forwarded_dataset.h"


namespace MLDB {


/*****************************************************************************/
/* BAMBOOHR DATASET CONFIG                                                 */
/*****************************************************************************/

struct BambooHrDatasetConfig {
    std::string apiKey;
    std::string subdomain;
};

DECLARE_STRUCTURE_DESCRIPTION(BambooHrDatasetConfig);


/*****************************************************************************/
/* BAMBOOHR DATASET                                                        */
/*****************************************************************************/

/** This models a read-only version of the BambooHR employee dataset.
*/

struct BambooHrDataset: public ForwardedDataset {

    BambooHrDataset(MldbServer * owner,
                    PolyConfig config,
                    const ProgressFunc & onProgress);
    
    virtual ~BambooHrDataset();

private:
    BambooHrDatasetConfig datasetConfig;
    struct Itl;
    std::shared_ptr<Itl> itl;
};


} // namespace MLDB
