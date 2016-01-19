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

/** Context to bind a row expression into an MLDB instance. */

struct SqlExpressionMldbContext: public SqlBindingScope {

    SqlExpressionMldbContext(const MldbServer * mldb);

    MldbServer * mldb;
      
    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<std::shared_ptr<SqlExpression> > & args);
    
    virtual std::shared_ptr<Function>
    doGetFunctionEntity(const Utf8String & functionName);

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

/** Context to bind a row expression into a dataset. */

struct SqlExpressionDatasetContext: public SqlExpressionMldbContext {

    struct RowContext: public SqlRowScope {
        RowContext(const MatrixNamedRow & row,
                   const BoundParameters * params = nullptr)
            : row(row), params(params)
        {
        }

        const MatrixNamedRow & row;

        /// If set, this tells us how to get the value of a bound parameter
        const BoundParameters * params;

        //const Date date;
    };

    SqlExpressionDatasetContext(std::shared_ptr<Dataset> dataset, const Utf8String& alias);
    SqlExpressionDatasetContext(const Dataset & dataset, const Utf8String& alias);
    SqlExpressionDatasetContext(const BoundTableExpression& boundDataset);

    const Dataset & dataset;
    Utf8String alias;
    std::vector<Utf8String> childaliases;

    virtual VariableGetter doGetVariable(const Utf8String & tableName,
                                         const Utf8String & variableName);

    GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    std::function<Utf8String (const Utf8String &)> keep);

    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<std::shared_ptr<SqlExpression> > & args);

    virtual GenerateRowsWhereFunction
    doCreateRowsWhereGenerator(const SqlExpression & where,
                      ssize_t offset,
                      ssize_t limit);

    virtual ColumnFunction
    doGetColumnFunction(const Utf8String & functionName);

    virtual VariableGetter
    doGetBoundParameter(const Utf8String & paramName);
    
    static RowContext getRowContext(const MatrixNamedRow & row,
                                    const BoundParameters * params = nullptr)
    {
        return RowContext(row, params);
    }
    
protected:

    Utf8String removeTableName(const Utf8String & variableName) const;
    Utf8String removeQuotes(const Utf8String & variableName) const;
    Utf8String resolveTableName(const Utf8String& variableName) const;
};


/*****************************************************************************/
/* SQL EXPRESSION ORDER BY CONTEXT                                           */
/*****************************************************************************/

/** An SQL expression context, but for where we are processing an order by
    clause.  This has access to all of the input and output columns.
*/

struct SqlExpressionOrderByContext: public ReadThroughBindingContext {

    SqlExpressionOrderByContext(SqlBindingScope & outer)
        : ReadThroughBindingContext(outer)
    {
    }

    struct RowContext: public ReadThroughBindingContext::RowContext {
        RowContext(const SqlRowScope & outer,
                   const NamedRowValue & output)
            : ReadThroughBindingContext::RowContext(outer), output(output)
        {
        }

        const NamedRowValue & output;
    };

    /** An order by clause can read through both what was selected and what
        was in the underlying row.  So we first look in what was selected,
        and then fall back to the underlying row.
    */
    virtual VariableGetter doGetVariable(const Utf8String & tableName,
                                         const Utf8String & variableName);

    RowContext getRowContext(const SqlRowScope & outer,
                             const NamedRowValue & output) const
    {
        return RowContext(outer, output);
    }
};

} // namespace MLDB
} // namespace Datacratic
