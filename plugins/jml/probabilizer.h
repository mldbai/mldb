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
#include "mldb/builtin/matrix.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/plugins/jml/jml/feature_info.h"
#include "mldb/plugins/jml/value_descriptions.h"

namespace MLDB {
struct GLZ_Probabilizer;
} // namespace MLDB


namespace MLDB {


class SqlExpression;


struct ProbabilizerConfig : public ProcedureConfig {
    static constexpr const char * name = "probabilizer.train";

    ProbabilizerConfig()
        : link(MLDB::LOGIT)
    {
    }

    /// Dataset for training data
    InputQuery trainingData;

    /// Where to save the probabilizer to
    Url modelFileUrl;

    /// Link function to use
    MLDB::Link_Function link;

    Utf8String functionName;
};

DECLARE_STRUCTURE_DESCRIPTION(ProbabilizerConfig);


/*****************************************************************************/
/* PROBABILIZER PROCEDURE                                                       */
/*****************************************************************************/

struct ProbabilizerProcedure: public Procedure {

    ProbabilizerProcedure(MldbEngine * owner,
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
    ProbabilizeFunction(MldbEngine * owner,
                     PolyConfig config,
                     const std::function<bool (const Json::Value &)> & onProgress);

    ProbabilizeFunction(MldbEngine * owner, const MLDB::GLZ_Probabilizer & in);

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

