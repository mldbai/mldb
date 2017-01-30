/** svm.h                                                   -*- C++ -*-
    Mathieu Marquis Bolduc, October 28th, 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Support-Vector Machine procedure and functions.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/value_function.h"
#include "matrix.h"
#include "mldb/types/value_description_fwd.h"


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
    static constexpr const char * name = "svm.train";

    SVMConfig()
        : svmType(SVM_CLASSIFICATION)
    {
    }

    /// Input data for training
    InputQuery trainingData;

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

struct SVMFunctionArgs {
   ExpressionValue embedding; //embedding
};

DECLARE_STRUCTURE_DESCRIPTION(SVMFunctionArgs);

struct SVMExpressionValue {
   ExpressionValue output;
};

DECLARE_STRUCTURE_DESCRIPTION(SVMExpressionValue);

struct SVMFunction: public ValueFunctionT<SVMFunctionArgs, SVMExpressionValue>  {
    SVMFunction(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress);

    ~SVMFunction();

    virtual SVMExpressionValue call(SVMFunctionArgs input) const override; 

    SVMFunctionConfig functionConfig;

    struct Itl;
    std::shared_ptr<Itl> itl;
};

} //MLDB

