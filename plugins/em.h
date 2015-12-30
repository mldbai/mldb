#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "matrix.h"
#include "mldb/ml/value_descriptions.h"
#include "metric_space.h"
#include "mldb/types/optional.h"

#include "mldb/ml/Eigen/Dense"

namespace Datacratic {
namespace MLDB {

struct EMConfig : public ProcedureConfig  {
    EMConfig()
        : numInputDimensions(-1),
          numClusters(10),
          maxIterations(100)
    {
        centroids.withType("embedding");
    }

    InputQuery trainingData;
    Optional<PolyConfigT<Dataset> > output;
    static constexpr char const * defaultOutputDatasetType = "embedding";

    PolyConfigT<Dataset> centroids;
    int numInputDimensions;
    int numClusters;
    int maxIterations;

    Utf8String functionName;
};

DECLARE_STRUCTURE_DESCRIPTION(EMConfig);



/*****************************************************************************/
/* EM PROCEDURE                                                           */
/*****************************************************************************/

struct EMProcedure: public Procedure {
    
    EMProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    EMConfig emConfig;
};


/*****************************************************************************/
/* EMFUNCTION                                                             */
/*****************************************************************************/

struct EMFunctionConfig {
    EMFunctionConfig()
        : select(SelectExpression::parse("*")),
          where(SqlExpression::parse("true"))
    {
    }
    
    PolyConfigT<Dataset> centroids;        ///< Dataset containing the centroids
    SelectExpression select;               ///< What to select from dataset
    std::shared_ptr<SqlExpression> where;  ///< Which centroids to take
};

DECLARE_STRUCTURE_DESCRIPTION(EMFunctionConfig);

struct EMFunction: public Function {
    EMFunction(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual Any getStatus() const;
    
    virtual FunctionOutput apply(const FunctionApplier & applier,
                              const FunctionContext & context) const;
    
    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;
    
    EMFunctionConfig functionConfig;
    std::vector<ColumnName> columnNames;
    int numDim;

     struct Cluster {
        CellValue clusterName;
        ML::distribution<float> centroid;
        Eigen::MatrixXd covarianceMatrix;
    };

    std::vector<Cluster> clusters;
};

} // namespace MLDB
} // namespace Datacratic
