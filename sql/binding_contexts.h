// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** binding_contexts.h                                             -*- C++ -*-
    Jeremy Barnes, 15 March 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Binding contexts for dealing with scopes.
*/

#pragma once

#include "sql_expression.h"

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* READ THROUGH BINDING CONTEXT                                              */
/*****************************************************************************/

/** This context is used to build a read-through-write-local select
    expression on top of.  It allows for functions and variables to
    read through to the outer context, but mutations are not supported
    (they can be in a subclass).

    It is used as the base for any kind of sub-select operation within
    the expression execution.
*/

struct ReadThroughBindingContext: public SqlBindingScope {
    ReadThroughBindingContext(SqlBindingScope & outer)
        : outer(outer)
    {
    }

    /// Outer context, which we can pass through non-mutating operations to
    SqlBindingScope & outer;
    
    /// RowContex structure. Derived class's row context must derive from this
    struct RowContext: public SqlRowScope {
        RowContext(const SqlRowScope & outer)
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
                  const std::vector<std::shared_ptr<SqlExpression> > & args);

    virtual VariableGetter doGetVariable(const Utf8String & tableName,
                                         const Utf8String & variableName);

    virtual GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    std::function<Utf8String (const Utf8String &)> keep);

    virtual VariableGetter doGetBoundParameter(const Utf8String & paramName);
    
    virtual std::shared_ptr<Function>
    doGetFunctionEntity(const Utf8String & functionName);

    virtual std::shared_ptr<Dataset>
    doGetDataset(const Utf8String & datasetName);

    virtual std::shared_ptr<Dataset>
    doGetDatasetFromConfig(const Any & datasetConfig);

    static RowContext getRowContext(const SqlRowScope & outerRow)
    {
        return RowContext(outerRow);
    }

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
                  const std::vector<std::shared_ptr<SqlExpression> > & args);

    ColumnContext getColumnContext(const ColumnName & columnName)
    {
        return ColumnContext(columnName);
    }

    virtual MldbServer * getMldbServer() const
    {
        return outer.getMldbServer();
    }
};


/*****************************************************************************/
/* SQL EXPRESSION WHEN SCOPE                                                 */
/*****************************************************************************/

/** Context to bind a given record of a row into a dataset. */

struct SqlExpressionWhenScope: public ReadThroughBindingContext {

    SqlExpressionWhenScope(SqlBindingScope & outer)
        : ReadThroughBindingContext(outer), isTupleDependent(false)
    {
    }

    struct RowScope: public ReadThroughBindingContext::RowContext {
        RowScope(const SqlRowScope & outer,
                 Date ts)
            : ReadThroughBindingContext::RowContext(outer), ts(ts)
        {
        }

        Date ts;
    };

    // Override the timestamp() function here 
    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<std::shared_ptr<SqlExpression> > & args);

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




} // namespace MLDB
} // namespace Datacratic
