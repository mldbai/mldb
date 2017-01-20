/** sql_functions.h                                               -*- C++ -*-
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Functions to deal with datasets.
*/

#pragma once

#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression.h"


namespace MLDB {

struct SqlExpression;
struct SqlExpressionMldbScope;
struct SqlExpressionExtractScope;


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
    InputQuery query;
    SqlQueryOutput output = FIRST_ROW;
};

DECLARE_STRUCTURE_DESCRIPTION(SqlQueryFunctionConfig);


struct SqlQueryFunction: public Function {
    SqlQueryFunction(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress);

    virtual Any getStatus() const;

    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const;
    
    virtual ExpressionValue apply(const FunctionApplier & applier,
                              const ExpressionValue & context) const;

    virtual FunctionInfo getFunctionInfo() const;

    SqlQueryFunctionConfig functionConfig;
};


/*****************************************************************************/
/* SQL EXPRESSION FUNCTION                                                   */
/*****************************************************************************/

/** Function that runs an SQL expression. */

struct SqlExpressionFunctionConfig {
    SelectExpression expression;
    bool prepared = false;
    bool raw = false;
    bool autoInput = false;
};

DECLARE_STRUCTURE_DESCRIPTION(SqlExpressionFunctionConfig);

struct SqlExpressionFunction: public Function {
    SqlExpressionFunction(MldbServer * owner,
                          PolyConfig config,
                          const std::function<bool (const Json::Value &)> & onProgress);
    ~SqlExpressionFunction();

    virtual Any getStatus() const;

    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const std::vector<std::shared_ptr<ExpressionValueInfo> > & inputInfo)
        const;

    virtual ExpressionValue apply(const FunctionApplier & applier,
                              const ExpressionValue & context) const;

    virtual FunctionInfo getFunctionInfo() const;

    SqlExpressionFunctionConfig functionConfig;

    std::unique_ptr<SqlExpressionMldbScope> outerScope;
    std::unique_ptr<SqlExpressionExtractScope> innerScope;
    FunctionInfo info;
    PathElement preparedAutoInputName;
    BoundSqlExpression bound;

    BoundSqlExpression doBind(SqlExpressionExtractScope & innerScope) const;

    std::tuple<PathElement, std::vector<std::shared_ptr<ExpressionValueInfo> > >
    getAutoInputName(SqlExpressionExtractScope & innerScope) const;
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

