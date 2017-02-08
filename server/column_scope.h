/** column_scope.h                                                 -*- C++ -*-
    Mathieu Marquis Bolduc, 11 Mars 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Scope to bind expression using dataset known columns.
*/

#pragma once

#include "mldb/server/dataset_context.h"


namespace MLDB {

struct Dataset;


/*****************************************************************************/
/* COLUMN SCOPE                                                              */
/*****************************************************************************/

/** Binding scope that can be used to execute a subset of expressions (those
    that access only named columns and extract only atomic values) very
    efficiently on datasets that have a materialized transpose.
*/

struct ColumnScope: public SqlExpressionMldbScope {
    ColumnScope(MldbServer * server, std::shared_ptr<Dataset> dataset);

    std::shared_ptr<Dataset> dataset;

    std::map<ColumnPath, size_t> requiredColumnIndexes;
    std::vector<ColumnPath> requiredColumns;

    struct RowScope: public SqlRowScope {
        // Initialize with a dense column for each input
        RowScope(size_t rowIndex,
                 const std::vector<std::vector<CellValue> > & inputs)
            : rowIndex(rowIndex), inputs(&inputs),
              cellInputs(nullptr), numericInputs(nullptr)
        {
        }

        // Initialize for an already-extracted row of CellValues
        RowScope(const CellValue * cellInputs)
            : rowIndex(-1), inputs(nullptr),
              cellInputs(cellInputs), numericInputs(nullptr)
        {
        }

        // Initialize for an already-extracted row of doubles
        RowScope(const double * numericInputs)
            : rowIndex(-1), inputs(nullptr),
              cellInputs(nullptr), numericInputs(numericInputs)
        {
        }

        size_t rowIndex;
        const std::vector<std::vector<CellValue> > * inputs;
        const CellValue * cellInputs;
        const double * numericInputs;
    };
    
    virtual ColumnGetter
    doGetColumn(const Utf8String & tableName,
                const ColumnPath & columnName);

    /** This will throw, as the ColumnScope can't execute an expression
        with wildcards in it.
    */
    virtual GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    const ColumnFilter& keep);

    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope);

    /** Run the expression, returning a *column* for each of the
        expressions given.  Since this will allocate a vector
        with the same number of elements as the number of rows in
        the dataset for each expression in exprs, the memory
        usage may be extreme; it is usually better to use the
        runIncremental() methods below (which this method calls
        internally).

        See documentation on runIncremental() for more details.
    */
    std::vector<std::vector<CellValue> >
    run(const std::vector<BoundSqlExpression> & exprs) const;

    /** Run the expression over a dataset, calling the given
        lambda for each row that is generated.  This method of
        running a query is extremely efficient for datasets
        that have a materialized transpose (including the Tabular
        dataset) but has several limitations.
        
        If any of the onVal expressions returns false, it will be
        stopped.  Each expression in exprs must return an atom as
        a result; if not an exception will be thrown.

        The expression will be run in parallel, so onVal must accept
        parallel execution.

        Will return false if and only if an onVal call returned false.

        If one or more of the onVal calls throws an exception, then one
        of them will be rethrown by the call.
    */
    bool
    runIncremental(const std::vector<BoundSqlExpression> & exprs,
                   std::function<bool (size_t rowNum,
                                       CellValue * vals)> onVal) const;

    /** Run the expression over a dataset, calling the given
        lambda for each row that is generated.  This method of
        running a query is extremely efficient for datasets
        that have a materialized transpose (including the Tabular
        dataset) but has several limitations.
        
        If any of the onVal expressions returns false, it will be
        stopped.  Each expression in exprs must return a double as
        a result or a null (which will result in the corresponding
        value returning NaN); if not an exception will be thrown.

        The expression will be run in parallel, so onVal must accept
        parallel execution.

        Will return false if and only if an onVal call returned false.

        If one or more of the onVal calls throws an exception, then one
        of them will be rethrown by the call.
    */
    bool
    runIncrementalDouble(const std::vector<BoundSqlExpression> & exprs,
                         std::function<bool (size_t rowNum,
                                             double * vals)> onVal) const;

private:
    template<typename Val>
    bool
    runIncrementalT(const std::vector<BoundSqlExpression> & exprs,
                    std::function<bool (size_t rowNum,
                                        Val * vals)> onVal) const;
};

} // namespace MLDB


