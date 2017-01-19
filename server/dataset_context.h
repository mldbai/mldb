/** dataset_context.h                                              -*- C++ -*-
    Jeremy Barnes, 24 February 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    
    Context for datasets within row expressions.
*/

#pragma once

#include "mldb/sql/sql_expression.h"
#include "mldb/sql/binding_contexts.h"
#include <unordered_map>


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
                  SqlBindingScope & argScope) override;
    
    virtual std::shared_ptr<Dataset>
    doGetDataset(const Utf8String & datasetName) override;

    virtual std::shared_ptr<Dataset>
    doGetDatasetFromConfig(const Any & datasetConfig) override;

    virtual TableOperations
    doGetTable(const Utf8String & tableName) override;

    virtual MldbServer * getMldbServer() const override;

    virtual ColumnGetter doGetColumn(const Utf8String & tableName,
                                     const ColumnPath & columnName) override;

    virtual GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    const ColumnFilter& keep) override;

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
        RowScope(const MatrixNamedRow & rowValue,
                 const BoundParameters * params = nullptr)
            : row(&rowValue), rowName(nullptr), expr(nullptr), params(params)
        {
        }

        RowScope(const RowPath & rowName,
                 const ExpressionValue & rowValue,
                 const BoundParameters * params = nullptr)
            : row(nullptr), rowName(&rowName), expr(&rowValue), params(params)
        {
        }

        /** Return a moveable copy of the row name of the row being
            processed. */
        RowPath getRowPath() const;

        /** Return either a reference to the row name, or a reference to
            the row name stored in storage.
        */
        const RowPath & getRowPath(RowPath & storage) const;

        /** Return the hash of the row name of the row being processed.
            INVARIANT: should be equal to RowHash(getRowPath()).
        */
        RowHash getRowHash() const;

        /** Return the value of the given column.  If knownOffset is set,
            then it can be used to immediately find the column within
            the structure without doing a lookup.

            Either returns a reference to the value, or a reference to
            storage which has the value stored inside it.
        */
        const ExpressionValue & getColumn(const ColumnPath & columnName,
                                          const VariableFilter & filter,
                                          ExpressionValue & storage,
                                          ssize_t knownOffset = -1) const;

        /** Returns the number of columns in the row, and the timestamp
            associated with it.
        */
        ExpressionValue getColumnCount() const;
        
        /** Return a reference to the filtered version of the row value.
            Storage is set as in ther other methods.
        */
        const ExpressionValue &
        getFilteredValue(const VariableFilter & filter,
                         ExpressionValue & storage) const;
        
        /** Return a value which has its values renamed according to the
            index, and is filtered according to the filter.

            Each column will be:
            - renamed to the name in index if it appears there;
            - dropped if not
        */
        ExpressionValue
        getReshaped(const std::unordered_map<ColumnHash, ColumnPath> & index,
                    const VariableFilter & filter) const;

        /** If we contain a MatrixNamedRow (legacy), this contains a pointer
            to that value.
        */
        const MatrixNamedRow * row;

        /** If we have an ExpressionValue, this points to the rowName of the
            row.  TODO: don't require a materialized rowName().
        */
        const RowPath * rowName;

        /** If we have an ExpressionValue, this points to the value of the
            row.
        */
        const ExpressionValue * expr;

        /// If set, this tells us how to get the value of a bound parameter
        const BoundParameters * params;
    };

    SqlExpressionDatasetScope(std::shared_ptr<Dataset> dataset, const Utf8String& alias);
    SqlExpressionDatasetScope(const Dataset & dataset, const Utf8String& alias);
    SqlExpressionDatasetScope(const BoundTableExpression& boundDataset);

    const Dataset & dataset;
    Utf8String alias;
    std::vector<Utf8String> childaliases;

    virtual ColumnGetter doGetColumn(const Utf8String & tableName,
                                       const ColumnPath & columnName);

    GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    const ColumnFilter& keep);

    GetAllColumnsOutput
    doGetAllAtoms(const Utf8String & tableName,
                  const ColumnFilter& keep);

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

    static RowScope getRowScope(const RowPath & rowName,
                                const ExpressionValue & row,
                                const BoundParameters * params = nullptr)
    {
        return RowScope(rowName, row, params);
    }

    virtual ColumnPath
    doResolveTableName(const ColumnPath & fullColumnName,
                       Utf8String & tableName) const;

private:
    GetAllColumnsOutput doGetAllColumnsInternal(const Utf8String & tableName, const ColumnFilter& keep, bool atoms);

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
                                       const ColumnPath & columnName);
    
    RowScope getRowScope(const SqlRowScope & outer,
                             const NamedRowValue & output) const
    {
        return RowScope(outer, output);
    }
};

} // namespace MLDB

