/** classifier.h                                                   -*- C++ -*-
    Jeremy Barnes, 22 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Classifier procedure and functions.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "matrix.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/ml/jml/feature_info.h"

namespace ML {
struct Mutable_Feature_Set;
struct Classifier_Impl;
} // namespace ML


namespace MLDB {


class SqlExpression;

enum ClassifierMode {
    CM_REGRESSION,
    CM_BOOLEAN,
    CM_CATEGORICAL
};

DECLARE_ENUM_DESCRIPTION(ClassifierMode);

struct ClassifierConfig : public ProcedureConfig {
    static constexpr const char * name = "classifier.train";

    ClassifierConfig()
        : equalizationFactor(0.5),
          mode(CM_BOOLEAN)
    {
    }

    /// Query to select the training data
    InputQuery trainingData;

    /// Where to save the classifier to
    Url modelFileUrl;

    /// Configuration of the algorithm.  If empty, the configurationFile
    /// will be used instead.
    Json::Value configuration;

    /// Filename to load algorithm configuration from.  Default is an
    /// inbuilt file with a few basic configurations.
    std::string configurationFile;

    /// Classifier algorithm to use from configuration file.  Default is
    /// the empty string, ie the root object.
    std::string algorithm;

    /// Equalization factor for rare classes.  Affects the weighting.
    double equalizationFactor;

    /// What mode to run in
    ClassifierMode mode;

    // Function name
    Utf8String functionName;
};

DECLARE_STRUCTURE_DESCRIPTION(ClassifierConfig);


/*****************************************************************************/
/* CLASSIFIER PROCEDURE                                                       */
/*****************************************************************************/

struct ClassifierProcedure: public Procedure {

    ClassifierProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    ClassifierConfig procedureConfig;
};


/*****************************************************************************/
/* CLASSIFY FUNCTION                                                         */
/*****************************************************************************/

struct ClassifyFunctionConfig {
    ClassifyFunctionConfig(const Url & modelFileUrl = Url())
        : modelFileUrl(modelFileUrl)
    {
    }

    Url modelFileUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(ClassifyFunctionConfig);

struct ClassifyFunction: public Function {
    ClassifyFunction(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress);

    ~ClassifyFunction();

    virtual Any getStatus() const;

    virtual Any getDetails() const;

    // The classify function needs to be able to bind so an optimized classifier
    // can be produced.
    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const;
    
    virtual ExpressionValue apply(const FunctionApplier & applier,
                              const ExpressionValue & context) const;

    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;

    /** Return the feature set for the given function context.  If
        returnDense is true, then it will attempt to return an optimized
        (dense) feature vector.

        The first result is the optimized (dense) vector.
        The second result is the sparse feature vector (if the first result is
        empty).
        The third result is the timestamp that should apply to the feature
        set as a whole.
    */
    std::tuple<std::vector<float>, std::shared_ptr<ML::Mutable_Feature_Set>, Date>
    getFeatureSet(const ExpressionValue & context, bool returnDense) const;

    //Classifier classifier;
    ClassifyFunctionConfig functionConfig;

    struct Itl;
    std::shared_ptr<Itl> itl;

    bool isRegression;
};

/*****************************************************************************/
/* EXPLAIN CLASSIFY FUNCTION                                                 */
/*****************************************************************************/

struct ExplainFunction: public ClassifyFunction {
    ExplainFunction(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress);

    ~ExplainFunction();

    virtual ExpressionValue apply(const FunctionApplier & applier,
                              const ExpressionValue & context) const;

    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;
};


} // namespace MLDB

