/** randomforest_procedure.h                                            -*- C++ -*-
    Mathieu Marquis Bolduc, 11 Mars 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Scope to bind expression using dataset known columns
*/


#pragma once

#include "mldb/server/dataset_context.h"
#include "mldb/base/parallel.h"
#include "mldb/base/thread_pool.h"
#include "mldb/http/http_exception.h"
#include "mldb/core/dataset.h"

// Should be in the cc file
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/arch/timers.h"

namespace Datacratic {
namespace MLDB {

struct ColumnScope: public SqlExpressionMldbScope {
    ColumnScope(MldbServer * server, std::shared_ptr<Dataset> dataset)
        : SqlExpressionMldbScope(server), dataset(dataset)
    {
    }

    std::shared_ptr<Dataset> dataset;

    std::map<ColumnName, size_t> requiredColumnIndexes;
    std::vector<ColumnName> requiredColumns;

    struct RowScope: public SqlRowScope {
        RowScope(size_t rowIndex,
                 const std::vector<std::vector<CellValue> > & inputs)
            : rowIndex(rowIndex), inputs(inputs)
        {
        }

        size_t rowIndex;
        const std::vector<std::vector<CellValue> > & inputs;
    };

    virtual ColumnGetter
    doGetColumn(const Utf8String & tableName,
                const ColumnName & columnName)
    {
        if (!requiredColumnIndexes.count(columnName)) {
            size_t index = requiredColumns.size();
            requiredColumnIndexes[columnName] = index;
            requiredColumns.push_back(columnName);
        }

        size_t index = requiredColumnIndexes[columnName];
        
        return {[=] (const SqlRowScope & scope,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    auto & row = scope.as<RowScope>();
                    return storage
                        = ExpressionValue(row.inputs.at(index).at(row.rowIndex),
                                          Date::notADate());
                },
                std::make_shared<AtomValueInfo>()};
    }

    virtual GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    std::function<ColumnName (const ColumnName &)> keep)
    {
        throw HttpReturnException(400, "Attempt to bind expression with wildcard in column scope");
    }

    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope)
    {
        return SqlBindingScope::doGetFunction(tableName, functionName, args, argScope);
    }

    std::vector<std::vector<CellValue> >
    run(const std::vector<BoundSqlExpression> & exprs) const
    {
        size_t numRows = dataset->getMatrixView()->getRowCount();

        std::vector<std::vector<CellValue> > results(exprs.size());
        for (auto & r: results)
            r.resize(numRows);

        auto onVal = [&] (size_t rowNum, CellValue * vals)
            {
                for (size_t i = 0;  i < exprs.size();  ++i) {
                    results[i][rowNum] = std::move(vals[i]);
                }
                return true;
            };
        
        return std::move(results);
    }

    bool
    runIncremental(const std::vector<BoundSqlExpression> & exprs,
                   std::function<bool (size_t rowNum,
                                       CellValue * vals)> onVal) const
    {
        size_t numRows = dataset->getMatrixView()->getRowCount();

        std::vector<std::vector<CellValue> > inputs(requiredColumns.size());
        auto doColumn = [&] (size_t index)
            {
                inputs[index] = dataset->getColumnIndex()
                     ->getColumnDense(requiredColumns[index]);
            };

        parallelMap(0, requiredColumns.size(), doColumn);

        std::atomic<bool> stop(false);

        // Apply the expression to everything
        auto doRow = [&] (size_t first, size_t last)
            {
                CellValue results[exprs.size()];
                for (size_t i = first;  i < last && !stop.load(std::memory_order_relaxed);  ++i) {
                    RowScope scope(i, inputs);
                    for (unsigned j = 0;  j < exprs.size();  ++j) {
                        ExpressionValue storage;
                        const ExpressionValue & result
                            = exprs[j](scope, storage, GET_LATEST);

                        // Currently, only atoms are supported as results
                        if (&storage == &result)
                            results[j] = storage.stealAtom();
                        else 
                            results[j] = result.getAtom();
                    }

                    if (!onVal(i, results)) {
                        stop = true;
                        return false;
                    }
                }

                return true;
            };
        
        parallelMapChunked(0, numRows, 1024 /* rows at once */,
                           doRow);
        
        return !stop.load();
    }

    bool
    runIncrementalDouble(const std::vector<BoundSqlExpression> & exprs,
                         std::function<bool (size_t rowNum,
                                             double * vals)> onVal) const
    {
        size_t numRows = dataset->getMatrixView()->getRowCount();

        using namespace std;
        ML::Timer timer;
        cerr << "doing rows" << endl;
        auto rowGen = dataset->generateRowsWhere(*this, "" /* alias */,
                                                 *SqlExpression::TRUE,
                                                 0 /* offset */,
                                                 -1 /* limit */);

        cerr << "rowGen.rowStream = " << !!rowGen.rowStream << endl;

        if (rowGen.rowStream) {
            // No need to ever allocate a huge amount of memory
            std::vector<size_t> chunkOffsets;
            auto chunks = rowGen.rowStream->parallelize(numRows,
                                                        RowStream::AUTO,
                                                        &chunkOffsets);
            
            static constexpr size_t ROWS_AT_ONCE = 1000;

            std::unique_ptr<double[]> ptr(new double[requiredColumns.size()
                                                     * ROWS_AT_ONCE]);
            double * values = ptr.get();

            auto onChunk = [&] (size_t chunkNum)
                {
                    RowStream & stream = *chunks[chunkNum];

                    ssize_t startOffset = chunkOffsets[chunkNum];
                    ssize_t endOffset = chunkOffsets[chunkNum + 1];
                    
                    size_t numRows = endOffset - startOffset;

                    for (size_t i = 0;  i < numRows;  i += ROWS_AT_ONCE) {
                        size_t startRow = i + startOffset;
                        size_t endRow
                            = std::min<size_t>(startRow + ROWS_AT_ONCE,
                                               endOffset);
                        size_t blockRows = endRow - startRow;

                        stream.extractNumbers(blockRows, requiredColumns,
                                              values);
                        
                        for (size_t j = 0;  j < blockRows;  ++j) {
                            if (!onVal(startRow + j,
                                       values + requiredColumns.size() * j))
                                return false;
                        }
                        
                        //stream.advanceBy(blockRows);
                    }
                    
                    return true;
                };

            return parallelMapHaltable(0, chunks.size(), onChunk);
        }

        std::vector<std::vector<CellValue> > inputs(requiredColumns.size());

        auto rows = rowGen(-1, nullptr);

        cerr << "got " << rows.first.size() << " rows in " << timer.elapsed()
             << " seconds" << endl;

        auto doColumn = [&] (size_t index)
            {
                inputs[index] = dataset->getColumnIndex()
                     ->getColumnDense(requiredColumns[index]);
            };

        parallelMap(0, requiredColumns.size(), doColumn);

        // For each expression, if it simply extracts the column then we put
        // the index of the column.  Otherwise, we put -1 as it's a more
        // complex expression.
        std::vector<int> columnNumbers;

        // Those that are simply reading a column get a fast path
        for (auto & e: exprs) {
            auto asColumn = dynamic_cast<const ReadColumnExpression *>
                (e.expr.get());

            if (asColumn) {
                // Find the index of the column name
                int index = -1;
                for (int i = 0;  i < requiredColumns.size() && index == -1;  ++i) {
                    if (requiredColumns[i] == asColumn->columnName)
                        index = i;
                }
                ExcAssert(index != -1);
                columnNumbers.emplace_back(index);
            }
            else {
                columnNumbers.emplace_back(-1);
            }
        }

        using namespace std;
        cerr << "done columns" << endl;

        std::atomic<bool> stop(false);

        // Apply the expression to everything
        auto doRow = [&] (size_t first, size_t last)
            {
                double results[exprs.size()];
                for (size_t i = first;  i < last && !stop.load(std::memory_order_relaxed);  ++i) {
                    RowScope scope(i, inputs);
                    for (unsigned j = 0;  j < exprs.size();  ++j) {
                        if (columnNumbers[j] != -1) {
                            results[j] = inputs[j][i].toDouble();
                        }
                        else {
                            ExpressionValue storage;
                            const ExpressionValue & result
                                = exprs[j](scope, storage, GET_LATEST);
                            results[j] = result.toDouble();
                        }
                    }

                    if (!onVal(i, results)) {
                        stop = true;
                        return false;
                    }
                }

                return true;
            };
        
        parallelMapChunked(0, numRows, 1024 /* rows at once */,
                           doRow);
        
        return !stop.load();
    }
};

}
}
