/** kmeans.h                                                          -*- C++ -*-
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    K-means algorithm.
*/

#pragma once


#include "mldb/sql/sql_expression.h"
#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "matrix.h"
#include "mldb/types/value_description.h"
#include "mldb/types/optional.h"
#include "metric_space.h"

namespace Datacratic {
namespace MLDB {

struct KmeansConfig : public ProcedureConfig {
    KmeansConfig()
        : numInputDimensions(-1),
          numClusters(10),
          maxIterations(100),
          metric(METRIC_COSINE)
    {
    }

    InputQuery trainingData;
    Url modelFileUrl;
    Optional<PolyConfigT<Dataset> > output;
    Optional<PolyConfigT<Dataset> > centroids;
    static constexpr char const * defaultOutputDatasetType = "embedding";

    int numInputDimensions;
    int numClusters;
    int maxIterations;
    MetricSpace metric;

    Utf8String functionName;
};

DECLARE_STRUCTURE_DESCRIPTION(KmeansConfig);



/*****************************************************************************/
/* KMEANS PROCEDURE                                                           */
/*****************************************************************************/

struct KmeansProcedure: public Procedure {
    
    KmeansProcedure(MldbServer * owner,
                   PolyConfig config,
                   const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    KmeansConfig kmeansConfig;
};


/*****************************************************************************/
/* K-MEANS FUNCTION                                                             */
/*****************************************************************************/

struct KmeansFunctionConfig {
    
    Url modelFileUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(KmeansFunctionConfig);

struct KmeansFunction: public Function {
    KmeansFunction(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual Any getStatus() const;
    
    virtual FunctionOutput apply(const FunctionApplier & applier,
                              const FunctionContext & context) const;
    
    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;
    
    KmeansFunctionConfig functionConfig;

    // holds the dimension of the embedding space
    size_t dimension;

    struct Impl;
    std::unique_ptr<Impl> impl;
};

} // namespace MLDB
} // namespace Datacratic
