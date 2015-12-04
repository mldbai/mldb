// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** tsne.h                                                          -*- C++ -*-
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    TSNE algorithm for a dataset.
*/

#pragma once

#include "mldb/server/dataset.h"
#include "mldb/server/procedure.h"
#include "mldb/server/algorithm.h"
#include "mldb/server/function.h"
#include "matrix.h"
#include "mldb/types/value_description.h"

namespace Datacratic {
namespace MLDB {

struct TsneItl;

struct TsneConfig : public ProcedureConfig {
    TsneConfig()
        : select(SelectExpression::STAR),
          when(WhenExpression::TRUE),
          where(SqlExpression::TRUE),
          orderBy(ORDER_BY_NOTHING),
          offset(0), limit(-1),
          numInputDimensions(-1),
          numOutputDimensions(2),
          tolerance(1e-5),
          perplexity(30.0)
    {
        output.withType("embedding");
    }

    std::shared_ptr<TableExpression> dataset;
    PolyConfigT<Dataset> output;   ///< Dataset config to hold the output embedding

    Url modelFileUrl;  ///< URI to save the artifact output by t-SNE training

    SelectExpression select;
    WhenExpression when;
    std::shared_ptr<SqlExpression> where;
    OrderByExpression orderBy;
    ssize_t offset;
    ssize_t limit;

    int numInputDimensions;
    int numOutputDimensions;
    double tolerance;
    double perplexity;

    Utf8String functionName;
};

DECLARE_STRUCTURE_DESCRIPTION(TsneConfig);



/*****************************************************************************/
/* TSNE PROCEDURE                                                             */
/*****************************************************************************/

// Input: a dataset, training parameters
// Output: a version, which has an artifact (TSNE file), a configuration, ...
// the important thing is that it can be deployed as a function, both internally
// or externally
struct TsneProcedure: public Procedure {

    TsneProcedure(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;
    
    virtual Any getStatus() const;

    TsneConfig tsneConfig;
};

struct TsneEmbedConfig {
    TsneEmbedConfig(const Url & modelFileUrl = Url())
        : modelFileUrl(modelFileUrl)
    {
    }

    Url modelFileUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(TsneEmbedConfig);


/*****************************************************************************/
/* TSNE EMBED ROW                                                            */
/*****************************************************************************/

struct TsneEmbed: public Function {
    TsneEmbed(MldbServer * owner,
              PolyConfig config,
              const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual Any getStatus() const;
    
    virtual FunctionOutput apply(const FunctionApplier & applier,
                              const FunctionContext & context) const;

    /** Describe what the input and output is for this configured function. */
    virtual FunctionInfo getFunctionInfo() const;
    
    TsneEmbedConfig functionConfig;
    std::shared_ptr<TsneItl> itl;
};


} // namespace MLDB
} // namespace Datacratic
