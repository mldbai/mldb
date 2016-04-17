/** binding_contexts.h                                             -*- C++ -*-
    Jeremy Barnes, 15 March 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Binding contexts for dealing with scopes.
*/

#pragma once

#include "sql_expression.h"
#include <unordered_set>


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* READ THROUGH BINDING CONTEXT                                              */
/*****************************************************************************/

/** This context is used to build a read-through-write-local select
    expression on top of.  It allows for functions and variables to
    read through to the outer context.

    It is used as the base for any kind of sub-select operation within
    the expression execution.
*/

struct ReadThroughBindingScope: public SqlBindingScope {
    ReadThroughBindingScope(SqlBindingScope & outer)
        : outer(outer)
    {
        functionStackDepth = outer.functionStackDepth;
    }

    /// Outer context, which we can pass through non-mutating operations to
    SqlBindingScope & outer;
    
    /// RowContex structure. Derived class's row context must derive from this
    struct RowScope: public SqlRowScope {
        RowScope(const SqlRowScope & outer)
            : outer(outer)
        {
        }

        const SqlRowScope & outer;
    };

    /// Rebind a BoundSqlExpression from the outer context to run on our		
    /// context.		
    static BoundSqlExpression rebind(BoundSqlExpression expr);

    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope);

    virtual ColumnGetter doGetColumn(const Utf8String & tableName,
                                       const ColumnName & columnName);

    virtual GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    std::function<ColumnName (const ColumnName &)> keep);

    virtual ColumnGetter doGetBoundParameter(const Utf8String & paramName);
    
    virtual std::shared_ptr<Dataset>
    doGetDataset(const Utf8String & datasetName);

    virtual std::shared_ptr<Dataset>
    doGetDatasetFromConfig(const Any & datasetConfig);

    virtual MldbServer * getMldbServer() const
    {
        return outer.getMldbServer();
    }
};

/*****************************************************************************/
/* COLUMN EXPRESSION BINDING CONTEXT                                         */
/*****************************************************************************/

/** Context to bind a row expression into a dataset, but where we are
    applying to the columns.
*/

struct ColumnExpressionBindingContext: public SqlBindingScope {

    ColumnExpressionBindingContext(SqlBindingScope & outer)
        : outer(outer)
    {
    }

    /// Outer context, which we can pass through non-mutating operations to
    SqlBindingScope & outer;
    
    /// RowContex structure. Derived class's row context must derive from this
    struct ColumnContext: public SqlRowScope {
        ColumnContext(const ColumnName & columnName)
            : columnName(columnName)
        {
        }

        const ColumnName & columnName;
    };

    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope);

    ColumnContext getColumnContext(const ColumnName & columnName)
    {
        return ColumnContext(columnName);
    }

    virtual MldbServer * getMldbServer() const
    {
        return outer.getMldbServer();
    }

    virtual ColumnGetter doGetColumn(const Utf8String & tableName,
                                       const ColumnName & columnName);
};


/*****************************************************************************/
/* SQL EXPRESSION WHEN SCOPE                                                 */
/*****************************************************************************/

/** Context to bind a given record of a row into a dataset. */

struct SqlExpressionWhenScope: public ReadThroughBindingScope {

    SqlExpressionWhenScope(SqlBindingScope & outer)
        : ReadThroughBindingScope(outer), isTupleDependent(false)
    {
    }

    struct RowScope: public ReadThroughBindingScope::RowScope {
        RowScope(const SqlRowScope & outer,
                 Date ts)
            : ReadThroughBindingScope::RowScope(outer), ts(ts)
        {
        }

        Date ts;
    };

    // Override the timestamp() function here 
    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope);

    static RowScope getRowScope(const SqlRowScope & outer,
                                Date ts)
    {
        return RowScope(outer, ts);
    }

    /* 
     * This flag get set whenever an expression which is dependent
     * of each tuple (e.g. timestamp()) is bound.  This serves to
     * optimize the execution of SQL query.
     */
    bool isTupleDependent;
};


/*****************************************************************************/
/* SQL EXPRESSION PARAM SCOPE                                                */
/*****************************************************************************/

/** Scope that only binds parameters, ie entities referenced as $xxx which
    are passed in after binding but are constant for each query execution.
*/

struct SqlExpressionParamScope: public ReadThroughBindingScope {

    SqlExpressionParamScope(SqlBindingScope & outer)
        : ReadThroughBindingScope(outer)
    {
    }

    // This row scope initializes the inner scope with itself; it should
    // never be used unless we are in a correlated sub-select in which
    // case we will need to thread the outer scope through.
    struct RowScope: public ReadThroughBindingScope::RowScope {
        RowScope(const BoundParameters & params)
            : ReadThroughBindingScope::RowScope(*this),
              params(params)
        {
        }

        const BoundParameters & params;
    };
    
    virtual ColumnGetter doGetBoundParameter(const Utf8String & paramName);

    static RowScope getRowScope(const BoundParameters & params)
    {
        return RowScope(params);
    }
};


/*****************************************************************************/
/* SQL EXPRESSION CONSTANT SCOPE                                             */
/*****************************************************************************/

/** Scope that will fail to bind anything apart from built-in function.
    This is used to bind and evaluate constant expressions.
*/

struct SqlExpressionConstantScope: public SqlBindingScope {

    static SqlRowScope getRowScope()
    {
        return SqlRowScope();
    }
};


/*****************************************************************************/
/* SQL EXPRESSION EXTRACT SCOPE                                              */
/*****************************************************************************/

/** This context allows for an expression to be bound to receive its inputs
    from a row, rather than from the outer scope.  The outer scope is still
    present, but it is only used to resolve non-column references (functions,
    etc).

    It has two modes:

    a) Known input mode, where the contents (ExpressionValueInfo) of the input
       row are known at binding time.  In this case, it will merely check that
       the columns referenced are satisfiable by the input row at bind time.
    b) Unknown input mode, where we infer that the ExpressionValueInfo of the
       input row looks like at bind time by resolving references.  In this
       case, at the end of the binding process we know what inputs are
       required.

    It is used in two situations currently: when applying an extract expression
    (rewriting a row), and when applying an sql.query or sql.expression
    function.
*/
    
struct SqlExpressionExtractScope: public ReadThroughBindingScope {

    /** Set up the context with a known set of input values.  In this mode,
        only that input will be used, and references to variables not
        satisfied by the input will result in an error on binding.
    */
    SqlExpressionExtractScope(SqlBindingScope & outer,
                              std::shared_ptr<ExpressionValueInfo> inputInfo);

    /** Set up the context to learn what input it should provide.  In this
        mode, anything that can't be satisfied will be recorded into the
        inferredInput upon binding, such that once binding is done, all
        required expressions are known in inferredInput.
    */
    SqlExpressionExtractScope(SqlBindingScope & outer);


    /// Input variables, for when they are known.  Will be null
    /// when the input is unknown.
    std::shared_ptr<RowValueInfo> inputInfo;

    /// Set of column names that we're inferring
    std::unordered_set<ColumnName> inferredInputs;

    /** Once we're done binding, we call this method to fill in the
        inputInfo from the inferredInputs.  It will modify the inputInfo
        field to reflect what is required as an input.
    */
    void inferInput();

    ColumnGetter doGetColumn(const Utf8String & tableName,
                             const ColumnName & columnName);

    GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    std::function<ColumnName (const ColumnName &)> keep);

    struct RowScope: public ReadThroughBindingScope::RowScope {
        RowScope(const SqlRowScope & outer, const ExpressionValue & input)
            : ReadThroughBindingScope::RowScope(outer), input(input)
        {
        }

        const ExpressionValue & input;
    };

    RowScope getRowScope(const SqlRowScope & outer,
                         const ExpressionValue & input) const
    {
        return RowScope(outer, input);
    }
};





} // namespace MLDB
} // namespace Datacratic
