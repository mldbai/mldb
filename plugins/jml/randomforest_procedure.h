/** randomforest_procedure.h                                       -*- C++ -*-
    Mathieu Marquis Bolduc, 11 Mars 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Procedure to train a random forest binary classifier.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/builtin/matrix.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/plugins/jml/jml/feature_info.h"


namespace MLDB {


struct RandomForestProcedureConfig : public ProcedureConfig {
    static constexpr const char * name = "randomforest.binary.train";

    /// Query to select the training data
    InputQuery trainingData;

    /// Where to save the classifier to
    Url modelFileUrl;

    /// Number of samplings of feature vectors
    int featureVectorSamplings = 5;

    /// Number of samplings of features
    /// Total number of bags is featureVectorSamplings*featureSamplings
    int featureSamplings = 20;

    /// Proportion of feature vectors to sample
    float featureVectorSamplingProp = 0.3;

    /// Proportion of Features to sample
    float featureSamplingProp = 0.3;

    /// Maximum depth of each tree
    int maxDepth = 20;

    /// Do we sample feature vectors?  Mostly for debugging
    bool sampleFeatureVectors = true;
    
    /// Debug Verbosity
    bool verbosity = false;

    /// Function name
    Utf8String functionName;
};

DECLARE_STRUCTURE_DESCRIPTION(RandomForestProcedureConfig);


/*****************************************************************************/
/* CLASSIFIER PROCEDURE                                                       */
/*****************************************************************************/

struct RandomForestProcedure: public Procedure {

    RandomForestProcedure(MldbEngine * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    RandomForestProcedureConfig procedureConfig;
};


} // namespace MLDB
