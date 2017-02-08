/** probabilizer.h                                                   -*- C++ -*-
    Jeremy Barnes, 22 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Probabilizer procedure and functions.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "matrix.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/ml/jml/feature_info.h"
#include "mldb/ml/value_descriptions.h"

namespace ML {
struct GLZ_Probabilizer;
} // namespace ML


namespace MLDB {


class SqlExpression;


struct ProbabilizerConfig : public ProcedureConfig {
    static constexpr const char * name = "probabilizer.train";

    ProbabilizerConfig()
        : link(ML::LOGIT)
    {
    }

    /// Dataset for training data
    InputQuery trainingData;

    /// Where to save the probabilizer to
    Url modelFileUrl;

    /// Link function to use
    ML::Link_Function link;

    Utf8String functionName;
};

DECLARE_STRUCTURE_DESCRIPTION(ProbabilizerConfig);


/*****************************************************************************/
/* PROBABILIZER PROCEDURE                                                       */
/*****************************************************************************/

struct ProbabilizerProcedure: public Procedure {

    ProbabilizerProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    ProbabilizerConfig probabilizerConfig;
};


/*****************************************************************************/
/* PROBABILIZE FUNCTION                                                            */
/*****************************************************************************/

struct ProbabilizeFunctionConfig {
    ProbabilizeFunctionConfig(const Url & modelFileUrl = Url()) :
        modelFileUrl(modelFileUrl)
    {
    }

    Url modelFileUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(ProbabilizeFunctionConfig);

struct ProbabilizeFunction: public Function {
    ProbabilizeFunction(MldbServer * owner,
                     PolyConfig config,
                     const std::function<bool (const Json::Value &)> & onProgress);

    ProbabilizeFunction(MldbServer * owner, const ML::GLZ_Probabilizer & in);

    ~ProbabilizeFunction();

    virtual Any getStatus() const;

    virtual ExpressionValue apply(const FunctionApplier & applier,
                              const ExpressionValue & context) const;

    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;

    ProbabilizeFunctionConfig functionConfig;

    struct Itl;
    std::shared_ptr<Itl> itl;
};


} // namespace MLDB

