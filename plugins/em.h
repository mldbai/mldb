/** em.h                                                           -*- C++ -*-
    Mathieu Marquis Bolduc, October 28th, 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Gaussian clustering procedure and functions.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "matrix.h"
#include "mldb/ml/value_descriptions.h"
#include "metric_space.h"
#include "mldb/types/optional.h"

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* EM CONFIG                                                                 */
/*****************************************************************************/

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
    Url modelFileUrl;

    Utf8String functionName;
};

DECLARE_STRUCTURE_DESCRIPTION(EMConfig);


/*****************************************************************************/
/* EM PROCEDURE                                                              */
/*****************************************************************************/

struct EMProcedure: public Procedure {
    
    EMProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput
    run(const ProcedureRunConfig & run,
        const std::function<bool (const Json::Value &)> & onProgress) const;
    
    virtual Any getStatus() const;

    EMConfig emConfig;
};


/*****************************************************************************/
/* EM FUNCTION                                                               */
/*****************************************************************************/

struct EMFunctionConfig {
    EMFunctionConfig()      
    {
    }
    
    Url modelFileUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(EMFunctionConfig);

struct EMFunction: public Function {
    EMFunction(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual Any getStatus() const;
    
    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const FunctionValues & input) const;

    virtual FunctionOutput apply(const FunctionApplier & applier,
                                 const FunctionContext & context) const;
    
    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;
    
    EMFunctionConfig functionConfig;
  
     // holds the dimension of the embedding space
    size_t dimension;

    struct Impl;
    std::unique_ptr<Impl> impl;
};

} // namespace MLDB
} // namespace Datacratic
