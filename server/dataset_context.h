/** dataset_context.h                                              -*- C++ -*-
    Jeremy Barnes, 24 February 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
    
    Context for datasets within row expressions.
*/

#pragma once

#include "mldb/sql/sql_expression.h"
#include "mldb/sql/binding_contexts.h"
#include <unordered_map>

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

    virtual ColumnGetter doGetColumn(const Utf8String & tableName,
                                     const ColumnName & columnName);
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
            : dataset(nullptr), rowToken(nullptr),
              row(&row), expr(nullptr), params(params)
        {
        }

        RowScope(const RowName & rowName,
                 const ExpressionValue & row,
                 const BoundParameters * params = nullptr)
            : dataset(nullptr), rowToken(nullptr),
              row(nullptr), rowName(&rowName), expr(&row), params(params)
        {
        }

        RowScope(const Dataset * dataset,
                 const void * rowToken,
                 const BoundParameters * params = nullptr)
            : dataset(dataset), rowToken(rowToken),
              row(nullptr), rowName(nullptr), expr(nullptr), params(params)
        {
        }

        /// Return the row name of the row being processed
        const RowName & getRowName(RowName & storage) const;

        /// Return the hash of the row name of the row being processed
        RowHash getRowHash() const;

        /// Return the value of the given column.  If knownOffset is set,
        /// then it can be used to immediately find the column within
        /// the structure without doing a lookup.
        const ExpressionValue & getColumn(const ColumnName & columnName,
                                          const VariableFilter & filter,
                                          ExpressionValue & storage,
                                          ssize_t knownOffset = -1) const;

        ExpressionValue
        getColumnCount() const;

        const ExpressionValue & getValue(ExpressionValue & storage) const;
        
        ExpressionValue
        getFilteredValue(const VariableFilter & filter) const;

        ExpressionValue
        getReshaped(const std::unordered_map<ColumnHash, ColumnName> & index,
                    const VariableFilter & filter) const;

        /// Return a version of this row, filtered by the given when
        /// expression.
        RowScope filterWhen(const BoundWhenExpression & when) const;
        
        /// For when initialized with dataset and token
        const Dataset * dataset;
        const void * rowToken;

        /// For when initialized with MatrixNamedRow
        const MatrixNamedRow * row;

        /// For when initialized with rowName and expression
        const RowName * rowName;
        const ExpressionValue * expr;

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

    static RowScope getRowScope(const RowName & rowName,
                                const ExpressionValue & row,
                                const BoundParameters * params = nullptr)
    {
        return RowScope(rowName, row, params);
    }

    static RowScope getRowScope(const Dataset & dataset,
                                const void * rowToken,
                                const BoundParameters * params = nullptr)
    {
        return RowScope(&dataset, rowToken, params);
    }

    virtual ColumnName
    doResolveTableName(const ColumnName & fullColumnName,
                       Utf8String & tableName) const;
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
