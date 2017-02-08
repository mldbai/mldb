/** randomforest_procedure.h                                            -*- C++ -*-
    Mathieu Marquis Bolduc, 11 Mars 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Procedure to train a random forest binary classifier.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "matrix.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/ml/jml/feature_info.h"


namespace MLDB {


struct RandomForestProcedureConfig : public ProcedureConfig {
    static constexpr const char * name = "randomforest.binary.train";

    RandomForestProcedureConfig() : featureVectorSamplings(5),
                                    featureSamplings(20),
                                    featureVectorSamplingProp(0.3f),
                                    featureSamplingProp(0.3f),
                                    maxDepth(20),
                                    verbosity(false)
    {
    }

    /// Query to select the training data
    InputQuery trainingData;

    /// Where to save the classifier to
    Url modelFileUrl;

    /// Number of samplings of feature vectors
    int featureVectorSamplings;

    /// Number of samplings of features
    /// Total number of bags is featureVectorSamplings*featureSamplings
    int featureSamplings;

    // Proportion of FV to sample
    float featureVectorSamplingProp;

    // Proportion of Feature to sample
    float featureSamplingProp;

    // Maximum depth of each tree
    int maxDepth;

    // Debug Verbosity
    bool verbosity;

    // Function name
    Utf8String functionName;
};

DECLARE_STRUCTURE_DESCRIPTION(RandomForestProcedureConfig);


/*****************************************************************************/
/* CLASSIFIER PROCEDURE                                                       */
/*****************************************************************************/

struct RandomForestProcedure: public Procedure {

    RandomForestProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    RandomForestProcedureConfig procedureConfig;
};


} // namespace MLDB
