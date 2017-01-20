/** pooling_function.h                                                   -*- C++ -*-
    Francois Maillet, 30 novembre 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#pragma once

#include "mldb/types/value_description_fwd.h"
#include "mldb/server/mldb_server.h"
#include "mldb/core/value_function.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/types/optional.h"
#include "mldb/plugins/sql_functions.h"


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

struct PoolingInput {
    ExpressionValue words;
};

DECLARE_STRUCTURE_DESCRIPTION(PoolingInput);

struct PoolingOutput {
    ExpressionValue embedding;
};

DECLARE_STRUCTURE_DESCRIPTION(PoolingOutput);

struct PoolingFunction: public ValueFunctionT<PoolingInput, PoolingOutput> {
    PoolingFunction(MldbServer * owner,
                   PolyConfig config,
                   const std::function<bool (const Json::Value &)> & onProgress);

    virtual PoolingOutput applyT(const ApplierT & applier, 
                                 PoolingInput input) const override;
    
    virtual std::unique_ptr<FunctionApplierT<PoolingInput, PoolingOutput> >
    bindT(SqlBindingScope & outerContext,
          const std::vector<std::shared_ptr<ExpressionValueInfo> > & input)
        const override;
   
    std::shared_ptr<SqlQueryFunction> queryFunction;

    BoundTableExpression boundEmbeddingDataset;

    PoolingFunctionConfig functionConfig;
    std::vector<ColumnPath> columnNames;

    SelectExpression select;
};

} // namespace MLDB

