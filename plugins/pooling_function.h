/** pooling_function.h                                                   -*- C++ -*-
    Francois Maillet, 30 novembre 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#pragma once

#include "mldb/types/value_description.h"
#include "mldb/server/mldb_server.h"
#include "mldb/core/function.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/types/optional.h"
#include "mldb/plugins/sql_functions.h"

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* POOLING FUNCTION CONFIG                                                   */
/*****************************************************************************/

struct PoolingFunctionConfig {
    PoolingFunctionConfig()
        : aggregators({"avg"})
    {
    }

    std::vector<Utf8String> aggregators;
    std::shared_ptr<TableExpression> embeddingDataset;
};

DECLARE_STRUCTURE_DESCRIPTION(PoolingFunctionConfig);


/*****************************************************************************/
/* POOLING FUNCTION                                                          */
/*****************************************************************************/

struct PoolingFunction: public Function {
    PoolingFunction(MldbServer * owner,
                   PolyConfig config,
                   const std::function<bool (const Json::Value &)> & onProgress);
   
    virtual Any getStatus() const;

    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const FunctionValues & input) const;
    
    virtual FunctionOutput apply(const FunctionApplier & applier,
                              const FunctionContext & context) const;

    virtual FunctionInfo getFunctionInfo() const;

    std::shared_ptr<SqlQueryFunction> queryFunction;

    BoundTableExpression boundEmbeddingDataset;

    PoolingFunctionConfig functionConfig;
    std::vector<ColumnName> columnNames;

    SelectExpression select;
};

} // namespace MLDB
} // namespace Datacratic
