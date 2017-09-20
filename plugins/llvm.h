/* llvm.h                                                          -*- C++ -*-
   Jeremy Barnes, July 25, 2017
   Copyright (c) 2017 mldb.ai inc.  All rights reserved.
   
   This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

   Allows a function to be JIT compiled with LLVM.
*/


#pragma once

#include "mldb/core/function.h"
#include "mldb/sql/sql_expression.h"


namespace MLDB {

struct SqlExpression;
struct SqlExpressionMldbScope;
struct SqlExpressionExtractScope;


/*****************************************************************************/
/* LLVM FUNCTION                                                             */
/*****************************************************************************/

/** Function that runs a single-row SQL query against a dataset. */

struct LlvmFunctionConfig {
    InputQuery query;
};

DECLARE_STRUCTURE_DESCRIPTION(LlvmFunctionConfig);


struct LlvmFunction: public Function {
    LlvmFunction(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress);

    virtual Any getStatus() const;
    
    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const;
    
    virtual ExpressionValue apply(const FunctionApplier & applier,
                                  const ExpressionValue & context) const;
    
    virtual FunctionInfo getFunctionInfo() const;
    
    LlvmFunctionConfig functionConfig;
};

/*****************************************************************************/
/* LLVM EXPRESSION FUNCTION                                                  */
/*****************************************************************************/

/** Function that runs an SQL expression. */

struct LlvmExpressionFunctionConfig {
    SelectExpression expression;
    bool prepared = false;
    bool raw = false;
    bool autoInput = false;
};

DECLARE_STRUCTURE_DESCRIPTION(LlvmExpressionFunctionConfig);

struct LlvmExpressionFunction: public Function {
    LlvmExpressionFunction(MldbServer * owner,
                          PolyConfig config,
                          const std::function<bool (const Json::Value &)> & onProgress);
    ~LlvmExpressionFunction();

    virtual Any getStatus() const;

    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const std::vector<std::shared_ptr<ExpressionValueInfo> > & inputInfo)
        const;

    virtual ExpressionValue apply(const FunctionApplier & applier,
                              const ExpressionValue & context) const;

    virtual FunctionInfo getFunctionInfo() const;

    LlvmExpressionFunctionConfig functionConfig;

    std::unique_ptr<SqlExpressionMldbScope> outerScope;
    std::unique_ptr<SqlExpressionExtractScope> innerScope;
    FunctionInfo info;
    PathElement preparedAutoInputName;
    BoundSqlExpression bound;

    BoundSqlExpression doBind(SqlExpressionExtractScope & innerScope) const;

    std::tuple<PathElement, std::vector<std::shared_ptr<ExpressionValueInfo> > >
    getAutoInputName(SqlExpressionExtractScope & innerScope) const;
};


} // namespace MLDB

