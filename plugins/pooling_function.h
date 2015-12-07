/** pooling_function.h                                                   -*- C++ -*-
    Francois Maillet, 30 novembre 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#pragma once

#include "types/value_description.h"
#include "server/mldb_server.h"
#include "server/function.h"
#include "sql/sql_expression.h"
#include "types/optional.h"
#include "mldb/jml/stats/distribution.h"

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* POOLING FUNCTION CONFIG                                                   */
/*****************************************************************************/

struct PoolingFunctionConfig {
    PoolingFunctionConfig() : zeroForUnknown(false), aggregators({"avg"})
    {}

    bool zeroForUnknown;
    std::vector<std::string> aggregators;
    std::shared_ptr<TableExpression> embeddingDataset;
};

DECLARE_STRUCTURE_DESCRIPTION(PoolingFunctionConfig);


/*****************************************************************************/
/* APPLY STOP WORDS FUNCTION                                                 */
/*****************************************************************************/

struct PoolingFunction: public Function {
    PoolingFunction(MldbServer * owner,
                   PolyConfig config,
                   const std::function<bool (const Json::Value &)> & onProgress);
   
    virtual Any getStatus() const;

    virtual FunctionOutput apply(const FunctionApplier & applier,
                              const FunctionContext & context) const;

    virtual FunctionInfo getFunctionInfo() const;

    BoundTableExpression boundEmbeddingDataset;

    PoolingFunctionConfig functionConfig;
    int num_embed_cols;

    SelectExpression select;
};

} // namespace MLDB
} // namespace Datacratic
