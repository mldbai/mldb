// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** kmeans.h                                                          -*- C++ -*-
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    KMEANS algorithm for a dataset.
*/

#pragma once

#include "mldb/server/dataset.h"
#include "mldb/server/procedure.h"
#include "mldb/server/algorithm.h"
#include "mldb/server/function.h"
#include "matrix.h"
#include "mldb/types/value_description.h"
#include "mldb/types/optional.h"
#include "metric_space.h"

namespace Datacratic {
namespace MLDB {

struct KmeansConfig : public ProcedureConfig {
    KmeansConfig()
        : select("*"),
          when(WhenExpression::TRUE),
          where(SqlExpression::TRUE),
          orderBy(ORDER_BY_NOTHING),
          offset(0),
          limit(-1),
          numInputDimensions(-1),
          numClusters(10),
          maxIterations(100),
          metric(METRIC_COSINE)
    {
        centroids.withType("embedding");
    }

    std::shared_ptr<TableExpression> dataset;
    Optional<PolyConfigT<Dataset> > output;
    static constexpr char const * defaultOutputDatasetType = "embedding";

    PolyConfigT<Dataset> centroids;
    SelectExpression select;
    WhenExpression when;
    std::shared_ptr<SqlExpression> where;
    OrderByExpression orderBy;
    ssize_t offset;
    ssize_t limit;
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
    KmeansFunctionConfig()
        : metric(METRIC_COSINE),
          select(SelectExpression::parse("*")),
          when(WhenExpression::TRUE),
          where(SqlExpression::TRUE)
    {
    }
    
    PolyConfigT<Dataset> centroids;        ///< Dataset containing the centroids
    MetricSpace metric;                   ///< Actual metric
    SelectExpression select;               ///< What to select from dataset
    WhenExpression when;  //
    std::shared_ptr<SqlExpression> where;  ///< Which centroids to take
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
    std::vector<ColumnName> columnNames;

    struct Cluster {
        CellValue clusterName;
        ML::distribution<float> centroid;
    };

    std::vector<Cluster> clusters;
};

} // namespace MLDB
} // namespace Datacratic
