/** sql_functions.h                                               -*- C++ -*-
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Functions to deal with datasets.
*/

#pragma once

#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression.h"

namespace Datacratic {
namespace MLDB {

struct SqlExpression;


/*****************************************************************************/
/* SQL QUERY FUNCTION                                                        */
/*****************************************************************************/

/** Function that runs a single-row SQL query against a dataset. */

struct SqlQueryFunctionConfig {
    SqlQueryFunctionConfig();
    
    SelectExpression select;
    std::shared_ptr<TableExpression> from;
    WhenExpression when;
    std::shared_ptr<SqlExpression> where;
    OrderByExpression orderBy;
    TupleExpression groupBy;
    std::shared_ptr<SqlExpression> having;
};

DECLARE_STRUCTURE_DESCRIPTION(SqlQueryFunctionConfig);


struct SqlQueryFunction: public Function {
    SqlQueryFunction(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual Any getStatus() const;
    
    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const FunctionValues & input) const;

    virtual FunctionOutput apply(const FunctionApplier & applier,
                              const FunctionContext & context) const;

    virtual FunctionInfo getFunctionInfo() const;

    SqlQueryFunctionConfig functionConfig;
};


/*****************************************************************************/
/* SQL EXPRESSION FUNCTION                                                   */
/*****************************************************************************/

/** Function that runs an SQL expression. */

struct SqlExpressionFunctionConfig {
    SelectExpression expression;
};

DECLARE_STRUCTURE_DESCRIPTION(SqlExpressionFunctionConfig);


struct SqlExpressionFunction: public Function {
    SqlExpressionFunction(MldbServer * owner,
                    PolyConfig config,
                    const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual Any getStatus() const;
    
    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const FunctionValues & inputInfo) const;

    virtual FunctionOutput apply(const FunctionApplier & applier,
                              const FunctionContext & context) const;

    virtual FunctionInfo getFunctionInfo() const;

    SqlExpressionFunctionConfig functionConfig;
};


/*****************************************************************************/
/* TRANSFORM DATASET                                                         */
/*****************************************************************************/

/** Procedure that applies a transform expression to a dataset, creating another
    dataset with the output.
*/

struct TransformDatasetConfig : ProcedureConfig {

    TransformDatasetConfig();

    /// The dataset to which we apply this function, once per row
    std::shared_ptr<TableExpression> inputDataset;

    /// The output dataset.  Rows will be dumped into here via insertRows.
    PolyConfigT<Dataset> outputDataset;

    /// The SELECT clause telling us what to actually put in the dataset
    SelectExpression select;

    /// The WHEN clause for which timespan tuples must belong to
    WhenExpression when;

    /// The WHERE clause for which rows to include from the dataset
    std::shared_ptr<SqlExpression> where;

    /// The GROUP BY clause for which rows to include from the dataset
    TupleExpression groupBy;

    /// The HAVING clause for which groups to include from the output
    std::shared_ptr<SqlExpression> having;

    /// The ORDER BY clause for which groups to include from the output
    OrderByExpression orderBy;

    /// Offset (how many output rows to skip).
    ssize_t offset;

    /// Limit (the maximum number of rows to output).
    ssize_t limit;

    /// Allow setting of the row name for the output dataset
    std::shared_ptr<SqlExpression> rowName;

    /// Skip rows with no columns
    bool skipEmptyRows;
};


DECLARE_STRUCTURE_DESCRIPTION(TransformDatasetConfig);

struct TransformDataset: public Procedure {

    TransformDataset(MldbServer * owner,
                     PolyConfig config,
                     const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    TransformDatasetConfig procedureConfig;
};



} // namespace MLDB
} // namespace Datacratic

