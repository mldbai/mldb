// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** svm.h                                                   -*- C++ -*-
    Mathieu Marquis Bolduc, October 28th, 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Support-Vector Machine procedure and functions.
*/

#pragma once

#include "mldb/server/dataset.h"
#include "mldb/server/procedure.h"
#include "mldb/server/algorithm.h"
#include "mldb/server/function.h"
#include "matrix.h"
#include "mldb/types/value_description_fwd.h"

namespace Datacratic {
namespace MLDB {

enum SVMType {
    SVM_CLASSIFICATION,
    SVM_CLASSIFICATION_NU,
    SVM_ONE_CLASS,
    SVM_REGRESSION_EPSILON,
    SVM_REGRESSION_NU
};

DECLARE_ENUM_DESCRIPTION(SVMType);

struct SVMConfig : public ProcedureConfig {
   SVMConfig()
        : select(SelectExpression::STAR),
          when(WhenExpression::TRUE),
          where(SqlExpression::TRUE),
          orderBy(ORDER_BY_NOTHING),
          offset(0), limit(-1), svmType(SVM_CLASSIFICATION)
    {
    }

    /// Dataset for training data
    std::shared_ptr<TableExpression> dataset;

    /// The SELECT clause to tell us which features to keep
    SelectExpression select;

    /// The WHEN clause for the timespan tuples must belong to
    WhenExpression when;

    /// The WHERE clause for which rows to include from the dataset
    std::shared_ptr<SqlExpression> where;

    /// The expression to generate the label
    std::shared_ptr<SqlExpression> label;

    /// The expression to generate the weight
    std::shared_ptr<SqlExpression> weight;

    /// How to order the rows when using an offset and a limit
    OrderByExpression orderBy;

    /// Where to start running
    ssize_t offset;

    /// Maximum number of rows to use
    ssize_t limit;

    /// Where to save the classifier to
    Url modelFileUrl;

    /// Configuration of the algorithm.  If empty, the configurationFile
    /// will be used instead.
    Json::Value configuration;

    // Function name
    Utf8String functionName;

    //SVM-Specific parameters
    SVMType svmType;
};

DECLARE_STRUCTURE_DESCRIPTION(SVMConfig);

/*****************************************************************************/
/* SVM PROCEDURE     	                                                     */
/*****************************************************************************/

struct SVMProcedure: public Procedure {

    SVMProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    SVMConfig procedureConfig;
};


/*****************************************************************************/
/* SVM FUNCTION                                                              */
/*****************************************************************************/

struct SVMFunctionConfig {
    SVMFunctionConfig(const Url & modelFileUrl = Url())
        : modelFileUrl(modelFileUrl)
    {
    }

    Url modelFileUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(SVMFunctionConfig);

struct SVMFunction: public Function {
    SVMFunction(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress);

    ~SVMFunction();

    virtual Any getStatus() const;

    // The function needs to be able to bind so an optimized classifier
    // can be produced.
    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const FunctionValues & input) const;

    virtual FunctionOutput apply(const FunctionApplier & applier,
                              const FunctionContext & context) const;

    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;

    //Classifier classifier;
    SVMFunctionConfig functionConfig;

    struct Itl;
    std::shared_ptr<Itl> itl;
};

} //MLDB
} //Datacratic
