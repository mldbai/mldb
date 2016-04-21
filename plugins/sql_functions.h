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

// TODO: hide these from the .h file
#include "mldb/server/dataset_context.h"
#include "mldb/server/function_contexts.h"


namespace Datacratic {
namespace MLDB {

struct SqlExpression;


/*****************************************************************************/
/* SQL QUERY FUNCTION                                                        */
/*****************************************************************************/

/** Enum that tells us how we encode the output of an SQL query object.
 */
enum SqlQueryOutput {
    FIRST_ROW = 0,       ///< Take the first row and return it directly
    NAMED_COLUMNS = 1    ///< Each row produces an explicitly named column
};

DECLARE_ENUM_DESCRIPTION(SqlQueryOutput);


/** Function that runs a single-row SQL query against a dataset. */

struct SqlQueryFunctionConfig {
    SqlQueryFunctionConfig()
        : output(FIRST_ROW)
    {
    }

    InputQuery query;
    SqlQueryOutput output;
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
    SqlExpressionFunctionConfig()
        : prepared(false)
    {
    }

    SelectExpression expression;
    bool prepared;
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

    SqlExpressionMldbContext outerScope;
    FunctionExpressionContext innerScope;
    FunctionInfo info;
    BoundSqlExpression bound;
};


/*****************************************************************************/
/* TRANSFORM DATASET                                                         */
/*****************************************************************************/

/** Procedure that applies a transform expression to a dataset, creating another
    dataset with the output.
*/

struct TransformDatasetConfig : ProcedureConfig {
    static constexpr const char * name = "transform";

    TransformDatasetConfig();

    /// The data to which we apply this function, once per row
    InputQuery inputData;

    /// The output dataset.  Rows will be dumped into here via insertRows.
    PolyConfigT<Dataset> outputDataset;

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
