// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** probabilizer.h                                                   -*- C++ -*-
    Jeremy Barnes, 22 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Probabilizer procedure and functions.
*/

#pragma once

#include "mldb/server/dataset.h"
#include "mldb/server/procedure.h"
#include "mldb/server/algorithm.h"
#include "mldb/server/function.h"
#include "matrix.h"
#include "mldb/types/value_description.h"
#include "mldb/ml/jml/feature_info.h"
#include "mldb/ml/value_descriptions.h"

namespace ML {
struct GLZ_Probabilizer;
} // namespace ML

namespace Datacratic {
namespace MLDB {


class SqlExpression;


struct ProbabilizerConfig : public ProcedureConfig {
    ProbabilizerConfig()
        :  when(WhenExpression::parse("true")),
           where(SqlExpression::parse("true")),
           weight(SqlExpression::parse("1.0")),
           orderBy(OrderByExpression::parse("rowHash()")),
           offset(0), limit(-1),
           link(ML::LOGIT)
    {
    }

    /// Dataset for training data
    std::shared_ptr<TableExpression> dataset;

    /// The SELECT clause to tell us how to calculate the probabilizer score
    std::shared_ptr<SqlExpression> select;

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
    
    // Initiialize programatically from a trained probabilizer
    ProbabilizeFunction(MldbServer * owner,
                     const ML::GLZ_Probabilizer & in);

    ~ProbabilizeFunction();

    virtual Any getStatus() const;

    virtual FunctionOutput apply(const FunctionApplier & applier,
                              const FunctionContext & context) const;

    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;

    ProbabilizeFunctionConfig functionConfig;

    struct Itl;
    std::shared_ptr<Itl> itl;
};


} // namespace MLDB
} // namespace Datacratic
