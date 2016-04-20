/** dataset_context.h                                              -*- C++ -*-
    Jeremy Barnes, 24 February 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
    
    Context for datasets within row expressions.
*/

#pragma once

#include "mldb/sql/sql_expression.h"
#include "mldb/sql/binding_contexts.h"

namespace Datacratic {
namespace MLDB {

struct BoundTableExpression;

/*****************************************************************************/
/* SQL EXPRESSION MLDB CONTEXT                                               */
/*****************************************************************************/

/** Context to bind a row expression into an MLDB instance.  This is normally
    the outer-most scope that is used.

    It brings the following entities into scope:

    - User-defined functions registered into the MLDB server;
    - Datasets that have been created by the MLDB server
    
    It also allows for builtin SQL functions to be accessed via the
    SqlBindingScope it inherits from.
*/

struct SqlExpressionMldbScope: public SqlBindingScope {

    SqlExpressionMldbScope(const MldbServer * mldb);

    MldbServer * mldb;
      
    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope);
    
    virtual std::shared_ptr<Dataset>
    doGetDataset(const Utf8String & datasetName);

    virtual std::shared_ptr<Dataset>
    doGetDatasetFromConfig(const Any & datasetConfig);

    virtual TableOperations
    doGetTable(const Utf8String & tableName);

    virtual MldbServer * getMldbServer() const;
};


/*****************************************************************************/
/* SQL EXPRESSION DATASET CONTEXT                                            */
/*****************************************************************************/

/** Context to bind a row expression into a dataset.  This makes the given
    dataset available to expressions that are bound within it, which means:

    - Columns within the dataset can be accessed by name or by wildcard;
    - Functions that are defined by the dataset can be called;
    
    It also, for historical reasons, allows for parameters to be bound.
*/

struct SqlExpressionDatasetScope: public SqlExpressionMldbScope {

    struct RowScope: public SqlRowScope {
        RowScope(const MatrixNamedRow & row,
                   const BoundParameters * params = nullptr)
            : row(row), params(params)
        {
        }

        const MatrixNamedRow & row;

        /// If set, this tells us how to get the value of a bound parameter
        const BoundParameters * params;

        //const Date date;
    };

    SqlExpressionDatasetScope(std::shared_ptr<Dataset> dataset, const Utf8String& alias);
    SqlExpressionDatasetScope(const Dataset & dataset, const Utf8String& alias);
    SqlExpressionDatasetScope(const BoundTableExpression& boundDataset);

    const Dataset & dataset;
    Utf8String alias;
    std::vector<Utf8String> childaliases;

    virtual ColumnGetter doGetColumn(const Utf8String & tableName,
                                       const ColumnName & columnName);

    GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    std::function<ColumnName (const ColumnName &)> keep);

    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope);

    virtual GenerateRowsWhereFunction
    doCreateRowsWhereGenerator(const SqlExpression & where,
                      ssize_t offset,
                      ssize_t limit);

    virtual ColumnFunction
    doGetColumnFunction(const Utf8String & functionName);

    virtual ColumnGetter
    doGetBoundParameter(const Utf8String & paramName);
    
    static RowScope getRowScope(const MatrixNamedRow & row,
                                    const BoundParameters * params = nullptr)
    {
        return RowScope(row, params);
    }

    virtual ColumnName
    doResolveTableName(const ColumnName & fullColumnName,
                       Utf8String & tableName) const;
    
#if 0
protected:

    // This is for the context where we have several datasets
    // resolve ambiguity of different table names
    // by finding the dataset name that resolves first.
    Utf8String resolveTableName(const Utf8String& columnName) const;
    Utf8String resolveTableName(const Utf8String& columnName,
                                Utf8String& resolvedTableName) const;
#endif
};


/*****************************************************************************/
/* SQL EXPRESSION ORDER BY CONTEXT                                           */
/*****************************************************************************/

/** An SQL expression context, but for where we are processing an order by
    clause.  This has access to all of the input and output columns.
*/

struct SqlExpressionOrderByScope: public ReadThroughBindingScope {

    SqlExpressionOrderByScope(SqlBindingScope & outer)
        : ReadThroughBindingScope(outer)
    {
    }

    struct RowScope: public ReadThroughBindingScope::RowScope {
        RowScope(const SqlRowScope & outer,
                   const NamedRowValue & output)
            : ReadThroughBindingScope::RowScope(outer), output(output)
        {
        }

        const NamedRowValue & output;
    };

    /** An order by clause can read through both what was selected and what
        was in the underlying row.  So we first look in what was selected,
        and then fall back to the underlying row.
    */
    virtual ColumnGetter doGetColumn(const Utf8String & tableName,
                                       const ColumnName & columnName);
    
    RowScope getRowScope(const SqlRowScope & outer,
                             const NamedRowValue & output) const
    {
        return RowScope(outer, output);
    }
};

} // namespace MLDB
} // namespace Datacratic
