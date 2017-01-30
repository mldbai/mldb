/** column_scope.cc
    Jeremy Barnes, 30 June 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/

#include "column_scope.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/arch/timers.h"
#include "mldb/base/parallel.h"
#include "mldb/base/thread_pool.h"
#include "mldb/core/dataset.h"
#include "mldb/http/http_exception.h"
#include "mldb/base/optimized_path.h"
#include "mldb/utils/possibly_dynamic_buffer.h"


using namespace std;



namespace MLDB {


/*****************************************************************************/
/* COLUMN SCOPE                                                              */
/*****************************************************************************/

ColumnScope::
ColumnScope(MldbServer * server, std::shared_ptr<Dataset> dataset)
    : SqlExpressionMldbScope(server), dataset(dataset)
{
}

ColumnGetter
ColumnScope::
doGetColumn(const Utf8String & tableName,
            const ColumnPath & columnName)
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
                if (row.cellInputs) {
                    return storage
                        = ExpressionValue(row.cellInputs[index],
                                          Date::notADate());
                }
                else if (row.numericInputs) {
                    return storage
                        = ExpressionValue(row.numericInputs[index],
                                          Date::notADate());
                }
                else {
                    return storage
                        = ExpressionValue(row.inputs->at(index).at(row.rowIndex),
                                          Date::notADate());
                }
            },
            std::make_shared<AtomValueInfo>()};
}

GetAllColumnsOutput
ColumnScope::
doGetAllColumns(const Utf8String & tableName,
                const ColumnFilter& keep)
{
    throw HttpReturnException
        (400, "Attempt to bind expression with wildcard in column scope");
}

BoundFunction
ColumnScope::
doGetFunction(const Utf8String & tableName,
              const Utf8String & functionName,
              const std::vector<BoundSqlExpression> & args,
              SqlBindingScope & argScope)
{
    return SqlBindingScope::doGetFunction(tableName, functionName, args, argScope);
}

std::vector<std::vector<CellValue> >
ColumnScope::
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

    runIncremental(exprs, onVal);
    
    return results;
}

/// Allow control over whether the given optimization path is run
/// so that we can test both with and without optimization.
static const OptimizedPath optimizeRunIncremental
("mldb.ColumnScope.runIncremental");

static void extractVals(size_t blockRows,
                        const std::vector<ColumnPath> & columnNames,
                        double * values, RowStream & stream)
{
    stream.extractNumbers(blockRows, columnNames,
                          values);
}

static double extractVal(const CellValue & val, double *)
{
    return val.toDouble();
}

static void extractVals(size_t blockRows,
                        const std::vector<ColumnPath> & columnNames,
                        CellValue * values, RowStream & stream)
{
    stream.extractColumns(blockRows, columnNames,
                          values);
}

static CellValue extractVal(CellValue && val, CellValue *)
{
    return std::move(val);
}

static CellValue extractVal(const CellValue & val, CellValue *)
{
    return val;
}

template<typename Val>
bool
ColumnScope::
runIncrementalT(const std::vector<BoundSqlExpression> & exprs,
                std::function<bool (size_t rowNum,
                                    Val * vals)> onVal) const
{
    size_t numRows = dataset->getMatrixView()->getRowCount();

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

    //Timer timer;
    //cerr << "doing rows" << endl;
    auto rowGen = dataset->generateRowsWhere(*this, "" /* alias */,
                                             *SqlExpression::TRUE,
                                             0 /* offset */,
                                             -1 /* limit */);

    bool canTakeOptimizedPath
        = rowGen.rowStream
        && rowGen.rowStream->supportsExtendedInterface();

    // Number of columns; for offset calculations
    size_t nc = requiredColumns.size();

    if (optimizeRunIncremental(canTakeOptimizedPath)) {
        // No need to ever allocate a huge amount of memory
        std::vector<size_t> chunkOffsets;
        auto chunks = rowGen.rowStream->parallelize(numRows,
                                                    RowStream::AUTO,
                                                    &chunkOffsets);
            
        static constexpr size_t ROWS_AT_ONCE = 1000;

        auto onChunk = [&] (size_t chunkNum)
            {
                PossiblyDynamicBuffer<Val> valuesHolder(
                    requiredColumns.size() * ROWS_AT_ONCE);
                Val * values = valuesHolder.data();

                RowStream & stream = *chunks[chunkNum];

                ssize_t startOffset = chunkOffsets[chunkNum];
                ssize_t endOffset = chunkOffsets[chunkNum + 1];
                    
                size_t numRows = endOffset - startOffset;

                PossiblyDynamicBuffer<Val> resultsHolder(exprs.size());
                Val * results = resultsHolder.data();

                for (size_t i = 0;  i < numRows;  i += ROWS_AT_ONCE) {
                    size_t startRow = i + startOffset;
                    size_t endRow
                        = std::min<size_t>(startRow + ROWS_AT_ONCE,
                                           endOffset);
                    size_t blockRows = endRow - startRow;

                    extractVals(blockRows, requiredColumns, values, stream);

                    for (size_t r = 0;  r < blockRows;  ++r) {

                        RowScope scope(values + nc * r);

                        for (unsigned j = 0;  j < exprs.size();  ++j) {
                            if (columnNumbers[j] != -1) {
                                Val & val = values[nc * r + columnNumbers[j]];
                                results[j]
                                    = extractVal(std::move(val), (Val *)0);
                            }
                            else {
                                ExpressionValue storage;
                                const ExpressionValue & result
                                    = exprs[j](scope, storage, GET_LATEST);
                                if (&result == &storage) {
                                    results[j] = extractVal(storage.stealAtom(),
                                                            (Val *)0);
                                }
                                else {
                                    results[j] = extractVal(result.getAtom(),
                                                            (Val *)0);
                                }
                            }
                        }
                        
                        if (!onVal(startRow + r, results))
                            return false;
                    }
                }
                    
                return true;
            };

        return parallelMapHaltable(0, chunks.size(), onChunk);
    }

    // Fall back to version without a row stream
    std::vector<std::vector<CellValue> > inputs(requiredColumns.size());

    auto rows = rowGen(-1, nullptr);

    //cerr << "got " << rows.first.size() << " rows in " << timer.elapsed()
    //     << " seconds" << endl;

    auto doColumn = [&] (size_t index)
        {
            inputs[index] = dataset->getColumnIndex()
                ->getColumnDense(requiredColumns[index]);
        };

    parallelMap(0, requiredColumns.size(), doColumn);

    using namespace std;
    cerr << "done columns" << endl;

    std::atomic<bool> stop(false);

    // Apply the expression to everything
    auto doRow = [&] (size_t first, size_t last)
        {
            PossiblyDynamicBuffer<Val> resultsHolder(exprs.size());
            Val * results = resultsHolder.data();
            for (size_t i = first;  i < last
                     && !stop.load(std::memory_order_relaxed);  ++i) {
                RowScope scope(i, inputs);
                for (unsigned j = 0;  j < exprs.size();  ++j) {
                    if (columnNumbers[j] != -1) {
                        results[j] = extractVal(std::move(inputs[columnNumbers[j]][i]), (Val *)0);
                    }
                    else {
                        ExpressionValue storage;
                        const ExpressionValue & result
                            = exprs[j](scope, storage, GET_LATEST);
                        if (&result == &storage) {
                            results[j] = extractVal(storage.stealAtom(), (Val *)0);
                        }
                        else {
                            results[j] = extractVal(result.getAtom(), (Val *)0);
                        }
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

bool
ColumnScope::
runIncremental(const std::vector<BoundSqlExpression> & exprs,
               std::function<bool (size_t rowNum,
                                   CellValue * vals)> onVal) const
{
    return runIncrementalT<CellValue>(exprs, onVal);
}

bool
ColumnScope::
runIncrementalDouble(const std::vector<BoundSqlExpression> & exprs,
                     std::function<bool (size_t rowNum,
                                         double * vals)> onVal) const
{
    return runIncrementalT<double>(exprs, onVal);
}

} // namespace MLDB

