/** hdbscan_procedure.h                                            -*- C++ -*-
    Mathieu Marquis Bolduc, May 19th, 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    HDBSCAN clustering procedure.
    Based on Heterogeneous Density Based Spatial Clustering of Application with Noise
    Peter & Antonysamy, IJCSNS, 2010
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/value_function.h"
#include "matrix.h"
#include "mldb/ml/value_descriptions.h"
#include "metric_space.h"
#include "mldb/types/optional.h"

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* HDBSCAN CONFIG                                                                 */
/*****************************************************************************/

struct HDBSCANConfig : public ProcedureConfig  {
    static constexpr const char * name = "hdbscan_clustering.train";

    HDBSCANConfig()
        : numInputDimensions(-1),
          coreDistance(5)
    {
    }

    InputQuery trainingData;
    Optional<PolyConfigT<Dataset> > output;
    static constexpr char const * defaultOutputDatasetType = "embedding";

    int numInputDimensions;
    int coreDistance; 
};

DECLARE_STRUCTURE_DESCRIPTION(HDBSCANConfig);


/*****************************************************************************/
/* HDBSCAN PROCEDURE                                                              */
/*****************************************************************************/

struct HDBSCANProcedure: public Procedure {

    HDBSCANProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput
    run(const ProcedureRunConfig & run,
        const std::function<bool (const Json::Value &)> & onProgress) const;
    
    virtual Any getStatus() const;

    HDBSCANConfig hdbscanConfig;
};

} // namespace MLDB
} // namespace Datacratic
