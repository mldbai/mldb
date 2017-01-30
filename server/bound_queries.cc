/** bound_queries.cc
    Jeremy Barnes, 12 August 2015

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Bound version of SQL queries.
*/

#include "mldb/server/bound_queries.h"
#include "mldb/core/dataset.h"
#include "mldb/server/dataset_context.h"
#include "mldb/base/parallel.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/server/parallel_merge_sort.h"
#include "mldb/arch/timers.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/sql/sql_utils.h"
#include "mldb/http/http_exception.h"
#include "mldb/utils/log.h"
#include "mldb/arch/demangle.h"

#include <boost/algorithm/string.hpp>

#include "mldb/jml/utils/profile.h"


using namespace std;




// Defined in thread_pool.h, and different from hardware_concurrency() as
// it can be overridden.
int numCpus();

namespace MLDB {

const int MIN_ROW_PER_TASK = 32;
const int TASK_PER_THREAD = 8;

__thread int QueryThreadTracker::depth = 0;


/*****************************************************************************/
/* BOUND SELECT QUERY                                                        */
/*****************************************************************************/

struct BoundSelectQuery::Executor {
    virtual bool execute(std::function<bool (NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd,
                                             int bucketNum)> processor,
                         bool processInParallel,
                         ssize_t offset,
                         ssize_t limit,
                         const ProgressFunc & onProgress)
    {
        auto newProcessor = [&] (Path & rowName,
                                 ExpressionValue & val,
                                 std::vector<ExpressionValue> & calcd,
                                 int bucketNum)
            {
                NamedRowValue output;
                output.rowName = std::move(rowName);
                output.rowHash = output.rowName;
                val.appendToRowDestructive(Path(), output.columns);
                return processor(output, calcd, bucketNum);
            };

        return executeExpr(newProcessor, processInParallel, offset, limit, onProgress);
    }

    virtual bool executeExpr(std::function<bool (Path & rowName,
                                                 ExpressionValue & val,
                                                 std::vector<ExpressionValue> & calcd,
                                                 int bucketNum)> processor,
                             bool processInParallel,
                             ssize_t offset,
                             ssize_t limit,
                             const ProgressFunc & onProgress)
    {
        auto newProcessor = [&] (NamedRowValue & output,
                                 std::vector<ExpressionValue> & calcd,
                                 int bucketNum)
            {
                ExpressionValue val(std::move(output.columns));
                return processor(output.rowName, val, calcd, bucketNum);
            };

        return execute(newProcessor, processInParallel, offset, limit, onProgress);
    }

    virtual std::shared_ptr<ExpressionValueInfo> getOutputInfo() const = 0;
};

struct UnorderedExecutor: public BoundSelectQuery::Executor {
    const Dataset & dataset;
    GenerateRowsWhereFunction whereGenerator;
    SqlExpressionDatasetScope & context;
    BoundSqlExpression whereBound;
    BoundWhenExpression whenBound;
    BoundSqlExpression boundSelect;
    std::vector<BoundSqlExpression> boundCalc;
    int numBuckets;
    std::shared_ptr<spdlog::logger> logger;


    typedef std::function<bool (NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd,
                                             int rowNum)> ExecutorAggregator;

    UnorderedExecutor(const Dataset & dataset,
                      GenerateRowsWhereFunction whereGenerator,
                      SqlExpressionDatasetScope & context,
                      BoundWhenExpression whenBound,
                      BoundSqlExpression boundSelect,
                      std::vector<BoundSqlExpression> boundCalc,
                      OrderByExpression newOrderBy,
                      int numBuckets,
                      std::shared_ptr<spdlog::logger> logger)
        : dataset(dataset),
          whereGenerator(std::move(whereGenerator)),
          context(context),
          whenBound(std::move(whenBound)),
          boundSelect(std::move(boundSelect)),
          boundCalc(std::move(boundCalc)),
          numBuckets(numBuckets),
          logger(logger)
    {
    }

    virtual bool executeExpr(std::function<bool (Path & rowName,
                                                 ExpressionValue & output,
                                                 std::vector<ExpressionValue> & calcd,
                                                 int rowNum)> processor,
                             bool processInParallel,
                             ssize_t offset,
                             ssize_t limit,
                             const ProgressFunc & onProgress)
    {
        //There are two variations on how to generate the rows, 
        //but most of the output code is the same
        if (numBuckets > 1 && whereGenerator.rowStream)
            return execute_iterative(processor, processInParallel, offset, limit, onProgress);
        else
            return execute_bloc(processor, processInParallel, offset, limit, onProgress);
    }
    
    /* execute_bloc will query all the relevant rowNames in advance
       using the whereGenerator()                                  */
    bool execute_bloc(std::function<bool (Path & rowName,
                                          ExpressionValue & output,
                                          std::vector<ExpressionValue> & calcd,
                                          int rowNum)> processor,
                      bool processInParallel,
                      ssize_t offset,
                      ssize_t limit,
                      const ProgressFunc & onProgress)
    {
        //STACK_PROFILE(UnorderedExecutor);
        DEBUG_MSG(logger) << "bound query unordered num buckets: " << numBuckets
                          << (processInParallel ? " multi-threaded"  : " single-threaded");
        QueryThreadTracker parentTracker;

        // Get a list of rows that we run over
        // Ordering is arbitrary but deterministic
        auto rows = whereGenerator(-1, Any(), BoundParameters(), onProgress).first;

        //cerr << "ROWS MEMORY SIZE " << rows.size() * sizeof(RowName) << endl;

        // Simple case... no order by and no limit

        ExcAssert(numBuckets != 0);

        // Do we select *?  In that case we can avoid a lot of copying
        bool selectStar = boundSelect.expr->isIdentitySelect(context);

        size_t numRows = rows.size();
        size_t numPerBucket = std::max((size_t)std::floor((float)numRows / numBuckets), (size_t)1);
        size_t effectiveNumBucket = std::min((size_t)numBuckets, numRows);
        std::atomic_ulong rowCount(0);
        ProgressState progress(numRows);
        auto doRow = [&] (size_t rowNum) -> bool
            {
                ++rowCount;

                if (rowCount % PROGRESS_RATE == 0) {
                    if (onProgress) {
                        progress = rowCount;
                        if (!onProgress(progress)) {
                            DEBUG_MSG(logger) << "dataset iteration was cancelled";
                            return false;
                        }
                    }
                }

                ExpressionValue row = dataset.getRowExpr(rows[rowNum]);
                auto output = processRow(rows[rowNum], row, rowNum, numPerBucket,
                                         selectStar);

                int bucketNumber
                    = numBuckets > 0
                    ? std::min((size_t)(rowNum/numPerBucket), (size_t)(numBuckets-1))
                    : -1;

                /* Finally, pass to the terminator to continue. */
                return processor(std::get<0>(output), std::get<1>(output),
                                 std::get<2>(output), bucketNumber);
            };

        if (numBuckets > 0) {
            ExcAssert(processInParallel);
            ExcAssertEqual(limit, -1);
            ExcAssertEqual(offset, 0);

            auto doBucket = [&] (int bucketNumber) -> bool
                {
                    size_t it = bucketNumber * numPerBucket;
                    int stopIt = bucketNumber == numBuckets - 1 ? numRows : it + numPerBucket;
                    for (; it < stopIt; ++it)
                    {
                        if (!doRow(it))
                            return false;
                    }

                    return true;
                };

            DEBUG_MSG(logger) << "iterating rows in parallel using buckets";
            return parallelMapHaltable(0, effectiveNumBucket, doBucket);
        }
        else {
            size_t upper =  rows.size();
            if (limit != -1)
                upper = std::min((size_t)(offset+limit), upper);

            if (offset <= upper) {
                if (processInParallel) {
                    DEBUG_MSG(logger) << "iterating rows in parallel";
                    return parallelMapHaltable(offset, upper, doRow);
                }
                else {
                    // TODO: to reduce memory usage, we should fill blocks of
                    // output on worker threads
                    // in order as much as possible
                    // while calling the aggregator on the caller thread.
                    ExcAssert(offset >= 0 && offset <= upper);
                    std::vector<std::tuple<Path, ExpressionValue, std::vector<ExpressionValue> > >
                        output(upper-offset);
                
                    ProgressState progress(upper-offset);
                    auto copyRow = [&] (int rowNum) -> bool
                        {
                            if (rowNum % PROGRESS_RATE == 0) {
                                if (onProgress) {
                                    progress = rowNum;
                                    if (!onProgress(progress)) {
                                        DEBUG_MSG(logger) << "dataset iteration was cancelled";
                                        return false;
                                    }
                                }
                            }
                            auto row = dataset.getRowExpr(rows[rowNum]);
                            auto outputRow = processRow(rows[rowNum], row, rowNum,
                                                        numPerBucket, selectStar);
                            output[rowNum-offset] = std::move(outputRow);
                            return true;
                        };

                    DEBUG_MSG(logger) << "iterating rows sequentially";
                    if (!parallelMapHaltable(offset, upper, copyRow))
                        return false;

                    for (size_t i = offset; i < upper; ++i) {
                        auto& outputRow = output[i-offset];
                        if (!processor(std::get<0>(outputRow), std::get<1>(outputRow),
                                       std::get<2>(outputRow), -1))
                            return false;
                    }
                }
            }
            return true;
        }
    }

    /* execute_iterative will use the whereGenerator rowStream to get the rowNames one by one
       in order to avoid having a big array of all the relevant rowNames                    */
    bool execute_iterative(std::function<bool (Path & rowName,
                                               ExpressionValue & output,
                                               std::vector<ExpressionValue> & calcd,
                                               int rowNum)> processor,
                           bool processInParallel,
                           ssize_t offset,
                           ssize_t limit,
                           const ProgressFunc & onProgress)
    {
        //STACK_PROFILE(UnorderedExecutor_optimized);
        DEBUG_MSG(logger) << "UnorderedIterExecutor num buckets: " << numBuckets 
                          << (processInParallel ? " multi-threaded " : " single-threaded");

        QueryThreadTracker parentTracker;

        // Simple case... no order by and no limit

        ExcAssertEqual(limit, -1);
        ExcAssertEqual(offset, 0);
        ExcAssert(numBuckets > 0);

        // Do we select *?  In that case we can avoid a lot of copying
        bool selectStar = boundSelect.expr->isIdentitySelect(context);

        int64_t numRows = whereGenerator.rowStreamTotalRows;
        
        size_t numPerBucket = std::max((size_t)std::floor((float)numRows / numBuckets), (size_t)1);
        size_t effectiveNumBucket = std::min((size_t)numBuckets, (size_t)numRows);

        //cerr << "Number of buckets :" << effectiveNumBucket << endl;
        //cerr << "Number of row per bucket: " << numPerBucket << endl;
        //cerr << "Number of rows: " << numRows << endl;

        ExcAssert(processInParallel);

        std::atomic_ulong bucketCount(0);
        ProgressState progress(effectiveNumBucket);
        auto doBucket = [&] (int bucketNumber) -> bool
            {
                size_t it = bucketNumber * numPerBucket;
                int stopIt = bucketNumber == numBuckets - 1 ? numRows : it + numPerBucket;
                auto stream = whereGenerator.rowStream->clone();
                stream->initAt(it);
                for (;  it < stopIt; ++it) {
                    RowPath rowName = stream->next();
                    auto row = dataset.getRowExpr(rowName);

                    auto output = processRow(rowName, row, it, numPerBucket, selectStar);
                    int bucketNumber
                        = numBuckets > 0 ? std::min((size_t)(it/numPerBucket),
                                                    (size_t)(numBuckets-1)) : -1;

                    /* Finally, pass to the terminator to continue. */
                    if (!processor(std::get<0>(output), std::get<1>(output),
                                   std::get<2>(output), bucketNumber))
                        return false;
                }
                if (onProgress) {
                    progress = ++bucketCount;
                    if (!onProgress(progress))
                        return false;
                }
                return true;
            };

        return parallelMapHaltable(0, effectiveNumBucket, doBucket);
    }

    std::tuple<RowPath, ExpressionValue, std::vector<ExpressionValue> >
    processRow(const RowPath & rowName,
               ExpressionValue & row,
               int rowNum,
               int numPerBucket,
               bool selectStar)
    {
        auto rowContext = context.getRowScope(rowName, row);

        whenBound.filterInPlace(row, rowContext);

        std::tuple<RowPath, ExpressionValue, std::vector<ExpressionValue> > output;

        std::get<0>(output) = rowName;

        auto selectRowScope = context.getRowScope(rowName, row);

        vector<ExpressionValue>& calcd = std::get<2>(output);
        calcd.resize(boundCalc.size());

        // Run the extra calculations before the select, as the select may
        // destroy the input if it's a select star.
        for (unsigned i = 0;  i < boundCalc.size();  ++i) {
            calcd[i] = boundCalc[i](selectRowScope, GET_LATEST);
        }

        if (selectStar) {
            // Move into place, since we know we're selecting *
            std::get<1>(output) = std::move(row);
        }
        else {
            // Run the select expression
            std::get<1>(output) = boundSelect(selectRowScope, GET_ALL);
        }

        return output;
    }

    virtual std::shared_ptr<ExpressionValueInfo> getOutputInfo() const
    {
        return boundSelect.info;
    }
};

struct OrderedExecutor: public BoundSelectQuery::Executor {

    const Dataset & dataset;
    GenerateRowsWhereFunction whereGenerator;
    SqlExpressionDatasetScope & context;
    BoundWhenExpression whenBound;
    BoundSqlExpression boundSelect;
    std::vector<BoundSqlExpression> boundCalc;
    OrderByExpression newOrderBy;
    size_t numDistinctOnClauses_;

    OrderedExecutor(const Dataset & dataset,
                    GenerateRowsWhereFunction whereGenerator,
                    SqlExpressionDatasetScope & context,
                    BoundWhenExpression whenBound,
                    BoundSqlExpression boundSelect,
                    std::vector<BoundSqlExpression> boundCalc,
                    OrderByExpression newOrderBy,
                    size_t numDistinctOnClauses)
        : dataset(dataset),
          whereGenerator(std::move(whereGenerator)),
          context(context),
          whenBound(std::move(whenBound)),
          boundSelect(std::move(boundSelect)),
          boundCalc(std::move(boundCalc)),
          newOrderBy(std::move(newOrderBy)),
          numDistinctOnClauses_(numDistinctOnClauses)
    {
    }

    virtual bool execute(std::function<bool (NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd, 
                                             int rowNum)> processor,
        bool processInParallel,
        ssize_t offset,
        ssize_t limit,
        const ProgressFunc & onProgress)
    {
        //STACK_PROFILE(OrderedExecutor);

        QueryThreadTracker parentTracker;

        // Get a list of rows that we run over
        // Ordering is arbitrary but deterministic
        auto rows = whereGenerator(-1, Any()).first;

        // cerr << "doing " << rows.size() << " rows with order by" << endl;
        // We have a defined order, so we need to sort here

        SqlExpressionOrderByScope orderByContext(context);

        auto boundOrderBy = newOrderBy.bindAll(orderByContext);

        // Two phases:
        // 1.  Generate rows that match the where expression, in the correct order
        // 2.  Select over those rows to get our result

   
        // For each one, generate the order by key

        typedef std::tuple<std::vector<ExpressionValue>, NamedRowValue, std::vector<ExpressionValue> > SortedRow;
        typedef std::vector<SortedRow> SortedRows;
        
        PerThreadAccumulator<SortedRows> accum;

        std::atomic<int64_t> rowsAdded(0);
        ProgressState progress(rows.size());

        auto doWhere = [&] (int rowNum) -> bool
            {
                QueryThreadTracker childTracker = parentTracker.child();

                auto row = dataset.getRowExpr(rows[rowNum]);

                if (onProgress && rowsAdded % PROGRESS_RATE == 0) {
                    progress = rowsAdded;
                    if (!onProgress(progress))
                        return false;
                }

                // Check it matches the where expression.  If not, we don't process
                // it.
                auto rowContext = context.getRowScope(rows[rowNum], row);

                //where already checked in whereGenerator

                whenBound.filterInPlace(row, rowContext);

                NamedRowValue outputRow;
                outputRow.rowName = rows[rowNum];
                outputRow.rowHash = rows[rowNum];
            
                // A new row scope for the select, as the row may have changed
                // due to being processed by the when.
                auto selectRowScope = context.getRowScope(rows[rowNum], row);
             
                // Run the bound select expressions
                ExpressionValue selectOutput
                = boundSelect(selectRowScope, GET_ALL);
                selectOutput.mergeToRowDestructive(outputRow.columns);

                vector<ExpressionValue> calcd(boundCalc.size());
                for (unsigned i = 0;  i < boundCalc.size();  ++i) {
                    calcd[i] = boundCalc[i](selectRowScope, GET_LATEST);
                }

                // Get the order by context, which can read from both the result
                // of the select and the underlying row.
                auto orderByRowScope
                    = orderByContext.getRowScope(rowContext, outputRow);

                std::vector<ExpressionValue> sortFields
                    = boundOrderBy.apply(orderByRowScope);

                SortedRows * sortedRows = &accum.get();
                sortedRows->emplace_back(std::move(sortFields),
                                         std::move(outputRow),
                                         std::move(calcd));

                ++rowsAdded;
                return true;
            };

        Timer timer;

        if (!parallelMapHaltable(0, rows.size(), doWhere)) {
            return false;  // the processing has been cancelled
        }

        //cerr << "map took " << timer.elapsed() << endl;
        timer.restart();
        
        // Compare two rows according to the sort criteria
        auto compareRows = [&] (const SortedRow & row1,
                                const SortedRow & row2) -> bool
            {
                return boundOrderBy.less(std::get<0>(row1), std::get<0>(row2));
            };
            
        auto rowsSorted = parallelMergeSort(accum.threads, compareRows);

        //cerr << "shuffle took " << timer.elapsed() << endl;
        timer.restart(); 

        // Now select only the required subset of sorted rows
        if (limit == -1)
            limit = rowsSorted.size();

        ExcAssertGreaterEqual(offset, 0);

        if (numDistinctOnClauses_ > 0) {

            std::vector<ExpressionValue> reference;
            reference.resize(numDistinctOnClauses_);
            ssize_t count = 0;

            for (unsigned i = 0;  i < rowsSorted.size();  ++i) {

                std::vector<ExpressionValue> & mark = std::get<0>(rowsSorted[i]);

                if (i == 0) {
                    std::copy_n(mark.begin(), numDistinctOnClauses_, reference.begin());
                }
                else {

                    bool same = true;
                    for (int i = 0; i < numDistinctOnClauses_; ++i){
                        if (reference[i] != mark[i]) {
                            same = false;
                            break;
                        }
                    }

                    if (!same)
                        std::copy_n(mark.begin(), numDistinctOnClauses_, reference.begin());
                    else
                        continue; //skip duplicates
                }

                ++count;

                if (count <= offset)
                    continue;

                auto & row = std::get<1>(rowsSorted[i]);
                auto & calcd = std::get<2>(rowsSorted[i]);

                /* Finally, pass to the terminator to continue. */
                if (!processor(row, calcd, i))
                    return false;

                if (count - offset == limit)
                    break;
            }

        }
        else {
            ssize_t begin = std::min<ssize_t>(offset, rowsSorted.size());
            ssize_t end = std::min<ssize_t>(offset + limit, rowsSorted.size());
            for (unsigned i = begin;  i < end;  ++i) {

                auto & row = std::get<1>(rowsSorted[i]);
                auto & calcd = std::get<2>(rowsSorted[i]);

                /* Finally, pass to the terminator to continue. */
                if (!processor(row, calcd, i))
                    return false;
            }
        }

     

        cerr << "reduce took " << timer.elapsed() << endl;

        return true;
    }

    virtual std::shared_ptr<ExpressionValueInfo> getOutputInfo() const
    {
        return boundSelect.info;
    }
};

namespace {

struct SortByRowHash {
    bool operator () (const RowPath & row1, const RowPath & row2)
    {
        RowHash h1(row1), h2(row2);

        return h1 < h2 || (h1 == h2 && row1 < row2);
    }
};

} // file scope

struct RowHashOrderedExecutor: public BoundSelectQuery::Executor {
    const Dataset & dataset;
    GenerateRowsWhereFunction whereGenerator;
    SqlExpressionDatasetScope & context;
    BoundWhenExpression whenBound;
    BoundSqlExpression boundSelect;
    std::vector<BoundSqlExpression> boundCalc;
    OrderByExpression newOrderBy;
    bool allowParallelOutput;

    RowHashOrderedExecutor(const Dataset & dataset,
                           GenerateRowsWhereFunction whereGenerator,
                           SqlExpressionDatasetScope & context,
                           BoundWhenExpression whenBound,
                           BoundSqlExpression boundSelect,
                           std::vector<BoundSqlExpression> boundCalc,
                           OrderByExpression newOrderBy,
                           bool allowParallelOutput)
        : dataset(dataset),
          whereGenerator(std::move(whereGenerator)),
          context(context),
          whenBound(std::move(whenBound)),
          boundSelect(std::move(boundSelect)),
          boundCalc(std::move(boundCalc)),
          newOrderBy(std::move(newOrderBy)),
          allowParallelOutput(allowParallelOutput)
    {
    }

     virtual bool execute(std::function<bool (NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd,
                                             int rowNum)> processor,
                         bool processInParallel,
                         ssize_t offset,
                         ssize_t limit,
                         const ProgressFunc & onProgress)
    {
        if (limit < 0 || !(whereGenerator.rowStream))
            return execute_bloc(processor, offset, limit, onProgress);
        else
            return execute_iter(processor, offset, limit, onProgress);
    }

     /* execute_bloc will query all the relevant rowNames in advance
        using the whereGenerator()                                           */
     virtual bool execute_bloc(std::function<bool (NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd,
                                             int rowNum)> processor,
                         ssize_t offset,
                         ssize_t limit,
                         const ProgressFunc & onProgress)
    {
        //STACK_PROFILE(RowHashOrderedExecutor_execute_bloc);

        QueryThreadTracker parentTracker;

        Timer rowsTimer;

        // Get a list of rows that we run over
        // Ordering is arbitrary but deterministic
        auto rows = whereGenerator(-1, Any()).first;

        if (!std::is_sorted(rows.begin(), rows.end(), SortByRowHash()))
            std::sort(rows.begin(), rows.end(), SortByRowHash());

        //cerr << "ROWS MEMORY SIZE " << rows.size() * sizeof(RowName) << endl;

        cerr << "Generated " << rows.size() << " rows in "
             << rowsTimer.elapsed()
             << " on " << (rowsTimer.elapsed_cpu() / rowsTimer.elapsed_wall()) << " cpus "
             << " at " << rows.size() / rowsTimer.elapsed_wall() << "/second and "
             << rows.size() / rowsTimer.elapsed_cpu() << " /cpu-second" << endl;
        
        Timer scanTimer;

        // Special but exceedingly common case: we sort by row hash.

        Spinlock mutex;
        std::vector<std::tuple<RowHash, NamedRowValue, std::vector<ExpressionValue> > >
            sorted;

        // We will get them in a random order.  But once we have enough, we know
        // that we don't need to go past the point.

        std::atomic<size_t> maxRowNumNeeded(-1);
        std::atomic<ssize_t> minRowNum(-1);
        std::atomic<size_t> maxRowNum(0);

        // Do we select *?  In that case we can avoid a lot of copying
        bool selectStar = boundSelect.expr->isIdentitySelect(context);

        auto doRow = [&] (ssize_t rowNum) -> bool
            {
                //if (rowNum % 1000 == 0)
                //    cerr << "doing row " << rowNum << " with minRowNum "
                //         << minRowNum << " maxRowNumNeeded " << maxRowNumNeeded
                //         << " maxRowNum " << maxRowNum << endl;

                QueryThreadTracker childTracker = parentTracker.child();

                ExpressionValue row;
                try {
                    // If we've gotten all past the maxRowNumNeeded, then we can stop
                    // otherwise push it up
                    if (rowNum == minRowNum + 1) {
                        if (rowNum == maxRowNumNeeded)
                            return false;
                        minRowNum += 1;
                    }

                    if (rowNum > maxRowNumNeeded)
                        return true;

                    // Update maxRowNum to the maximum value
                    {
                        size_t knownMaxRowNum = maxRowNum;
                        while (rowNum > knownMaxRowNum) {
                            if (maxRowNum.compare_exchange_strong(knownMaxRowNum, rowNum))
                                break;
                        }
                    }

                    //if (rowNum % 1000 == 0)
                    //    cerr << "applying row " << rowNum << " of " << rows.size() << endl;

                    //RowPath rowName = rows[rowNum];

                    row = dataset.getRowExpr(rows[rowNum]);

                    // Check it matches the where expression.  If not, we don't process
                    // it.
                    auto rowContext = context.getRowScope(rows[rowNum], row);

                    //where was already filtered by the where generator

                    whenBound.filterInPlace(row, rowContext);

                    NamedRowValue outputRow;
                    outputRow.rowName = rows[rowNum];
                    outputRow.rowHash = rows[rowNum];

                    vector<ExpressionValue> calcd(boundCalc.size());
                    for (unsigned i = 0;  i < boundCalc.size();  ++i) {
                        calcd[i] = boundCalc[i](rowContext, GET_LATEST);
                    }

                    if (selectStar) {
                        // Move into place, since we know we're selecting *
                        // This is more complicated than it looks, because the input is
                        // flattened but the output is structured, so we have to go
                        // through the ExpressionValue to add the structure in first.
                        row.mergeToRowDestructive(outputRow.columns);
                    }
                    else {
                        // Run the select expression
                        ExpressionValue selectOutput = boundSelect(rowContext, GET_ALL);
                        selectOutput.mergeToRowDestructive(outputRow.columns);
                    }

                    std::unique_lock<Spinlock> guard(mutex);
                    sorted.emplace_back(outputRow.rowHash,
                                        std::move(outputRow),
                                        std::move(calcd));

                    if (limit != -1 && sorted.size() >= offset + limit) {
                        maxRowNumNeeded = maxRowNum.load();
                        return true;
                    }

                    return true;
                } catch (...) {
                    rethrowHttpException(KEEP_HTTP_CODE,
                                         "Executing non-grouped query bound to row: " + getExceptionString(),
                                         "row", row,
                                         "rowHash", rows[rowNum],
                                         "rowNum", rowNum);
                }
            };

        // Do the first 100 in a single thread, to see what our hit rate is
        static constexpr size_t NUM_TO_SAMPLE = 100;

        for (unsigned i = 0;  i < NUM_TO_SAMPLE && i < rows.size()
                 && (limit == -1 || sorted.size() < offset + limit);  ++i)
            doRow(i);

        //cerr << "Done first " << NUM_TO_SAMPLE << " rows with "
        //     << sorted.size() << " total" << endl;

        size_t numProcessed = std::min(NUM_TO_SAMPLE, rows.size());

        while (numProcessed < rows.size()
               && (limit == -1 || (sorted.size() < offset + limit))) {
            double hitRate = 1.0 * sorted.size() / numProcessed;
            size_t numRequired = rows.size();
            if (limit != -1) {
                // Project how many we need to get to the limit
                // Make sure we always make progress
                numRequired
                    = std::min<size_t>(rows.size(),
                                       std::max<size_t>
                                       (numProcessed + 100,
                                        (offset + limit) / hitRate));
            }

            cerr << "hit rate after " << numProcessed << " is "
                 << 100.0 * hitRate << "%; estimate that we need to scan "
                 << numRequired << " rows in total" << endl;

            // Do another block
            if (numRequired - numProcessed <= 100) {
                for (size_t n = numProcessed;  n < numRequired
                         && (limit == -1 || sorted.size() < offset + limit);
                     ++n) {
                    doRow(n);
                }
            }
            else {
                parallelMap(numProcessed, numRequired, doRow);
            }
            
            numProcessed = numRequired;
        }

        //cerr << "got " << maxRowNumNeeded << " rows with min row num " << minRowNum << endl;
        //cerr << "sorted.size() = " << sorted.size() << endl;
        cerr << "Scanned " << sorted.size() << " in " << scanTimer.elapsed()
             << " on " << (scanTimer.elapsed_cpu() / scanTimer.elapsed_wall()) << " cpus "
             << " at " << sorted.size() / scanTimer.elapsed_wall() << "/second and "
             << sorted.size() / scanTimer.elapsed_cpu() << " /cpu-second" << endl;

        scanTimer.restart();

        // Now select only the required subset of sorted rows
        if (limit == -1)
            limit = sorted.size();
        if (limit > sorted.size())
            limit = sorted.size();

        //cerr << "limit = " << limit << endl;

        std::partial_sort(sorted.begin(), sorted.begin() + limit, sorted.end(),
                          [] (const std::tuple<RowHash, NamedRowValue, std::vector<ExpressionValue> > & t1,
                              const std::tuple<RowHash, NamedRowValue, std::vector<ExpressionValue> > & t2)
                          {
                              return std::get<0>(t1) < std::get<0>(t2);
                          });

        //cerr << "done partial sort" << endl;

        ExcAssertGreaterEqual(offset, 0);

        cerr << "Sorted " << sorted.size() << " in " << scanTimer.elapsed()
             << " on " << (scanTimer.elapsed_cpu() / scanTimer.elapsed_wall()) << " cpus "
             << " at " << sorted.size() / scanTimer.elapsed_wall() << "/second and "
             << sorted.size() / scanTimer.elapsed_cpu() << " /cpu-second" << endl;

        scanTimer.restart();

        ssize_t begin = std::min<ssize_t>(offset, sorted.size());
        ssize_t end = std::min<ssize_t>(offset + limit, sorted.size());

        //cerr << "begin = " << begin << " end = " << end << endl;

        if (!allowParallelOutput) {

            for (unsigned i = begin;  i < end;  ++i) {
                if (!processor(std::get<1>(sorted[i]), std::get<2>(sorted[i]), i - begin))
                    return false;
            }
        }
        else {
            std::atomic<bool> stop(false);
            auto onOutput = [&] (size_t i)
                {
                    if (stop)
                        return false;

                    if (!processor(std::get<1>(sorted[i]), std::get<2>(sorted[i]), i - begin)) {
                        stop = true;
                        return false;
                    }

                    return true;
                };

            if (!parallelMapHaltable(begin, end, onOutput))
                return false;
        }

        cerr << "Output " << sorted.size() << " in " << scanTimer.elapsed()
             << " on " << (scanTimer.elapsed_cpu() / scanTimer.elapsed_wall()) << " cpus "
             << " at " << sorted.size() / scanTimer.elapsed_wall() << "/second and "
             << sorted.size() / scanTimer.elapsed_cpu() << " /cpu-second" << endl;

        return true;
    }

   /* execute_iterative will use the whereGenerator rowStream to get the rowNames one by one
       in order to avoid having a big array of all the relevant rowNames                    */
    virtual bool execute_iter(std::function<bool (NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd,
                                             int rowNum)> processor,
                         ssize_t offset,
                         ssize_t limit,
                         const ProgressFunc & onProgress)
    {
        //STACK_PROFILE(RowHashOrderedExecutor_execute_iter);

        QueryThreadTracker parentTracker;

        if (limit == 0)
          throw HttpReturnException(400, "limit must be non-zero");

        Timer rowsTimer;

        typedef std::vector<RowPath> AccumRows;
        
        PerThreadAccumulator<AccumRows> accum;

        int numNeeded = offset + limit;

        int64_t upperBound = whereGenerator.rowStreamTotalRows;
        int maxNumTask = numCpus() * TASK_PER_THREAD;
        //try to have at least MIN_ROW_PER_TASK element per task
        int numChunk = upperBound < maxNumTask*MIN_ROW_PER_TASK ? (upperBound / maxNumTask) : maxNumTask;
        numChunk = std::max(numChunk, (int)1U);
        int chunkSize = (int)std::floor((float)upperBound / numChunk);

        auto doChunk = [&] (int bucketIndex)
        {
          int index = bucketIndex*chunkSize;
          int stopIndex = bucketIndex == numChunk - 1 ? upperBound : index + chunkSize;
          AccumRows& rows = accum.get();

          auto stream = whereGenerator.rowStream->clone();
          stream->initAt(index);

          while (index < stopIndex)
          {
              RowPath rowName = stream->next();

              if (rowName == RowPath())
                  break;

              RowHash r1 = RowHash(rowName);
              bool spaceLeft = rows.size() < numNeeded;

              if (spaceLeft || RowHash(rows[numNeeded-1]) > r1)
              {
                  //evaluate the BoundWhere here?

                  if (!spaceLeft)
                    rows.pop_back();

                  //sorted insert
                  auto iter = rows.begin();
                  for (; iter != rows.end(); ++iter) {
                      if (r1 < RowHash(*iter)) {
                          rows.insert(iter, rowName);
                          break;
                      }
                  }   

                  if (iter == rows.end())
                    rows.insert(iter, rowName);  

              }

              ++index;
          }
        };      

        parallelMap(0, numChunk, doChunk);        
       
        // Compare two rows according to the sort criteria
        auto compareRows = [&] (const RowPath & row1,
                                const RowPath & row2) -> bool
            {
                return RowHash(row1) < RowHash(row2);
            };
            
        auto rowsMerged = parallelMergeSort(accum.threads, compareRows);
        
        if (rowsMerged.size() < offset )
            return true;

        rowsMerged.erase(rowsMerged.begin(), rowsMerged.begin() + offset);
        
        if (rowsMerged.size() > limit)
            rowsMerged.erase(rowsMerged.begin() + limit, rowsMerged.end());

        //Assuming the limit is small enough we can output the rows in order

         // Do we select *?  In that case we can avoid a lot of copying
        bool selectStar = boundSelect.expr->isIdentitySelect(context);

        // TODO: parallel...
        int count = 0;
        for (auto & r : rowsMerged) {

            ExpressionValue row = dataset.getRowExpr(r);
            auto rowContext = context.getRowScope(r, row);

            whenBound.filterInPlace(row, rowContext);
            NamedRowValue outputRow;
            outputRow.rowName = r;
            outputRow.rowHash = r;

            vector<ExpressionValue> calcd(boundCalc.size());
            for (unsigned i = 0;  i < boundCalc.size();  ++i) {
                calcd[i] = boundCalc[i](rowContext, GET_LATEST);
            }

            if (selectStar) {
                // Move into place, since we know we're selecting *
                // This is more complicated than it looks, because the input is
                // flattened but the output is structured, so we have to go
                // through the ExpressionValue to add the structure in first.
                row.mergeToRowDestructive(outputRow.columns);
            }
            else {
                // Run the select expression
                ExpressionValue selectOutput = boundSelect(rowContext, GET_ALL);
                selectOutput.mergeToRowDestructive(outputRow.columns);
            }
            if (!processor(outputRow, calcd, count))
                return false;

            ++count;
        }      

        return true;
    }

    virtual std::shared_ptr<ExpressionValueInfo> getOutputInfo() const
    {
        return boundSelect.info;
    }
};

BoundSelectQuery::
BoundSelectQuery(const SelectExpression & select,
                 const Dataset & from,
                 const Utf8String& alias,
                 const WhenExpression & when,
                 const SqlExpression & where,
                 const OrderByExpression & orderBy,
                 std::vector<std::shared_ptr<SqlExpression> > calc,
                 int numBuckets)
    : select(select), from(from), when(when), where(where), calc(calc),
      orderBy(orderBy), context(new SqlExpressionDatasetScope(from, std::move(alias))),
      logger(getMldbLog<BoundSelectQuery>())
{
    try {
        SqlExpressionWhenScope whenScope(*context);
        auto whenBound = when.bind(whenScope);

        // Get a generator for the rows that match 
        auto whereGenerator = context->doCreateRowsWhereGenerator(where, 0, -1);

        auto boundSelect = select.bind(*context);

        selectInfo = boundSelect.info;

        for (auto & c: calc) {
            ExcAssert(c);
            boundCalc.emplace_back(c->bind(*context));
        }

        // Get a generator rows from the for the ordered, limited where expression
        // Remove any constants from the order by clauses

        OrderByExpression newOrderBy;
        for (auto & x: orderBy.clauses) {

            // TODO: Better constant detection
            if (x.first->getType() == "constant") {
                continue;  
            }

            newOrderBy.clauses.push_back(x);
        }

        bool orderByRowHash = false;
        bool outputInParallel = false /*newOrderBy.clauses.empty()*/;
        if (newOrderBy.clauses.size() == 1
            && newOrderBy.clauses[0].second == ASC
            && newOrderBy.clauses[0].first->getType() == "function"
            && newOrderBy.clauses[0].first->getOperation() == "rowHash") {

            Utf8String datasetName = static_pointer_cast<FunctionCallExpression>(newOrderBy.clauses[0].first)->tableName;

            if (datasetName.empty() || datasetName.empty() == alias)
                orderByRowHash = true;
        }

        if (!orderByRowHash && newOrderBy.clauses.size() > 0) {
            //if we have an order by, always add a rowHash() to make sure we have a fully deterministic sorting order
            newOrderBy.clauses.emplace_back(SqlExpression::parse("rowHash()"), ASC);
        }
 
        if (orderByRowHash) {
            ExcAssert(numBuckets < 0);
            DEBUG_MSG(logger) << "executing with " << demangle(typeid(RowHashOrderedExecutor));
            executor.reset(new RowHashOrderedExecutor(from,
                                                      std::move(whereGenerator),
                                                      *context,
                                                      std::move(whenBound),
                                                      std::move(boundSelect),
                                                      boundCalc,
                                                      std::move(newOrderBy),
                                                      outputInParallel));          
           
        }
        else if (!newOrderBy.clauses.empty()) {
            ExcAssert(numBuckets < 0);
            DEBUG_MSG(logger) << "executing with " << demangle(typeid(OrderedExecutor));
            executor.reset(new OrderedExecutor(from,
                                               std::move(whereGenerator),
                                               *context,
                                               std::move(whenBound),
                                               std::move(boundSelect),
                                               boundCalc,
                                               std::move(newOrderBy),
                                               select.distinctExpr.size()));
        } else {
            DEBUG_MSG(logger) << "executing with " << demangle(typeid(UnorderedExecutor));
            executor.reset(new UnorderedExecutor(from,
                                                 std::move(whereGenerator),
                                                *context,
                                                 std::move(whenBound),
                                                 std::move(boundSelect),
                                                 boundCalc,
                                                 std::move(newOrderBy),
                                                 numBuckets,
                                                 logger));
        }

    } MLDB_CATCH_ALL {
        rethrowHttpException(KEEP_HTTP_CODE, "Binding error: "
                             + getExceptionString(),
                             "select", select.surface,
                             "from", from.getStatus(),
                             "where", where.shallowCopy(),
                             "calc", calc,
                             "orderBy", orderBy);
    }
}

bool
BoundSelectQuery::
execute(RowProcessorEx processor,
        ssize_t offset,
        ssize_t limit,
        const ProgressFunc & onProgress)
{
    //STACK_PROFILE(BoundSelectQuery);

    auto subProcessor = [&] (NamedRowValue & row,
                             std::vector<ExpressionValue> & calc,
                             int groupNum) {
        return processor(row, calc);
    };

    return execute(subProcessor, processor.processInParallel, offset, limit, onProgress);

}

bool
BoundSelectQuery::
execute(std::function<bool (NamedRowValue & output,
                            std::vector<ExpressionValue> & calcd,
                            int groupNum)> processor,
        bool processInParallel,
        ssize_t offset,
        ssize_t limit,
        const ProgressFunc & onProgress)
{
    //STACK_PROFILE(BoundSelectQuery);

    ExcAssert(processor);

    try {
        return executor->execute(processor, processInParallel, offset, limit, onProgress);
    } MLDB_CATCH_ALL {
        rethrowHttpException(KEEP_HTTP_CODE, "Execution error: "
                             + getExceptionString(),
                             "select", select.surface,
                             "from", from.getStatus(),
                             "where", where.shallowCopy(),
                             "calc", calc,
                             "orderBy", orderBy,
                             "offset", offset,
                             "limit", limit);
    }
}

bool
BoundSelectQuery::
executeExpr(RowProcessorExpr processor,
            ssize_t offset,
            ssize_t limit,
            const ProgressFunc & onProgress)
{
    auto subProcessor = [&] (Path & rowName,
                             ExpressionValue & row,
                             std::vector<ExpressionValue> & calc,
                             int groupNum)
        {
            return processor(rowName, row, calc);
        };
    
    return executeExpr(subProcessor, processor.processInParallel,
                       offset, limit, onProgress);
}

bool
BoundSelectQuery::
executeExpr(std::function<bool (Path & rowName,
                                ExpressionValue & output,
                                std::vector<ExpressionValue> & calcd,
                                int groupNum)> processor,
            bool processInParallel,
            ssize_t offset,
            ssize_t limit,
            const ProgressFunc & onProgress)
{
    //STACK_PROFILE(BoundSelectQuery);

    ExcAssert(processor);

    try {
        return executor->executeExpr(processor, processInParallel,
                                     offset, limit, onProgress);
    } MLDB_CATCH_ALL {
        rethrowHttpException(KEEP_HTTP_CODE, "Execution error: "
                             + getExceptionString(),
                             "select", select.surface,
                             "from", from.getStatus(),
                             "where", where.shallowCopy(),
                             "calc", calc,
                             "orderBy", orderBy,
                             "offset", offset,
                             "limit", limit);
    }
}

std::shared_ptr<ExpressionValueInfo>
BoundSelectQuery::
getSelectOutputInfo() const
{
    return executor->getOutputInfo();
}

/*****************************************************************************/
/* GROUP CONTEXT                                                             */
/*****************************************************************************/

/** Context in which we run our expressions that operate inside a group
    by.  This one has access to information about the current group, not
    the current row.
*/

typedef std::vector<std::shared_ptr<void> > GroupMapValue;

struct GroupContext: public SqlExpressionDatasetScope {

    GroupContext(const Dataset& dataset, const Utf8String& alias, 
            const TupleExpression & groupByExpression,
            std::vector<std::shared_ptr<ExpressionValueInfo> >& groupInfo) : 
        SqlExpressionDatasetScope(dataset, alias),
        groupByExpression(groupByExpression),
        groupInfo(groupInfo),
        argCounter(0), argOffset(groupInfo.size()),
        evaluateEmptyGroups(false)
    {
    }

    const TupleExpression & groupByExpression;

    struct RowScope: public SqlRowScope {
        RowScope(NamedRowValue & output,
                 const std::vector<ExpressionValue> & currentGroupKey)
            : output(output), currentGroupKey(currentGroupKey)
        {
        }

        NamedRowValue & output;
        const std::vector<ExpressionValue> & currentGroupKey;
    };

    virtual BoundFunction doGetFunction(const Utf8String & tableName,
                                        const Utf8String & functionName,
                                        const std::vector<BoundSqlExpression> & args,
                                        SqlBindingScope & argScope)
    {

        auto getGroupRowName = [] (const SqlRowScope & context) -> RowPath
            {
            auto & row = context.as<RowScope>();

            //Todo: now we end up with extra quotes, not super pretty
            static VectorDescription<ExpressionValue>
                desc(getExpressionValueDescriptionNoTimestamp());

            std::string result;
            result.reserve(116);  /// try to force a 128 byte allocation
            StringJsonPrintingContext scontext(result);
            scontext.writeUtf8 = true;
            desc.printJsonTyped(&row.currentGroupKey, scontext);

            return PathElement(result);
        };

        if (functionName == "rowName") {
            return {[getGroupRowName] (const std::vector<ExpressionValue> & args,
                        const SqlRowScope & context)
                    {                        
                        auto result = getGroupRowName(context);
                        return ExpressionValue(result.toUtf8String(),
                                               Date::negativeInfinity());
                    },
                    std::make_shared<StringValueInfo>()};
        }
        if (functionName == "rowPath") {
            return {[getGroupRowName] (const std::vector<ExpressionValue> & args,
                        const SqlRowScope & context)
                    {                        
                        auto result = getGroupRowName(context);
                        return ExpressionValue(CellValue(result),
                                               Date::negativeInfinity());
                    },
                    std::make_shared<PathValueInfo>()};
        }
        else if (functionName == "rowHash") {
                return {[getGroupRowName] (const std::vector<ExpressionValue> & args,
                        const SqlRowScope & context)
                    {                        
                        auto rowName = getGroupRowName(context);
                        return ExpressionValue(RowHash(rowName),
                                               Date::negativeInfinity());
                        
                    },
                    std::make_shared<Uint64ValueInfo>()};
        }
        else if (functionName == "groupKeyElement"
                 || functionName == "group_key_element") {
            return {[] (const std::vector<ExpressionValue> & args,
                        const SqlRowScope & context)
                    {
                        auto & row = context.as<RowScope>();

                        int position = args[0].toInt(); //(context, GET_LATEST).toInt();

                        return row.currentGroupKey.at(position);
                    },
                    // TODO: get it from the value info for the group keys...
                    std::make_shared<AnyValueInfo>()};
        }

        //check aggregators
        auto aggFn = SqlBindingScope::doGetAggregator(functionName, args);
        if (aggFn) {
            if (functionName == "count")
                {
                    //count is *special*
                    evaluateEmptyGroups = true;
                }

            int aggIndex = outputAgg.size();
            OutputAggregator boundagg(argCounter,
                                      args.size(),
                                      aggFn);
            outputAgg.emplace_back(boundagg);              

            argCounter += args.size();

            return {[&,aggIndex] (const std::vector<ExpressionValue> & args,
                                  const SqlRowScope & context)
                    {
                        return outputAgg[aggIndex]
                            .aggregate.extract(aggData[aggIndex].get());
                    },
                    // TODO: get it from the value info for the group keys...
                    std::make_shared<AnyValueInfo>()};
        }
        return SqlExpressionDatasetScope::doGetFunction(tableName,
                                                        functionName,
                                                        args, argScope);
    }

    // Within a group by context, we can get either:
    // 1.  The value of the variable in the row
    // 2.  The value of the variable within the group by expression
    virtual ColumnGetter doGetColumn(const Utf8String & tableName,
                                     const ColumnPath & columnName)
    {
        // First, search for something that matches the surface (ugh)
        // of a group by clause.  We can use that directly.

        // This whole block is only necessary right now because of this use-case:
        // Select dataset.column From dataset group by column
        // When this case if properly managed by replaceGroupByKey
        // we can eliminate this block
        for (unsigned i = 0;  i < groupByExpression.clauses.size();  ++i) {
            const std::shared_ptr<SqlExpression> & g
                = groupByExpression.clauses[i];

            ColumnPath simplifiedSurface;
            if (columnName[0] == alias) {
                simplifiedSurface = columnName.removePrefix();
            }
            else {
                if (!alias.empty())
                    simplifiedSurface = PathElement(alias) + columnName;
                else simplifiedSurface = columnName;
            }

            auto variable = std::dynamic_pointer_cast<ReadColumnExpression>(g);

            if (variable) {
                if (variable->columnName == columnName ||
                    (!simplifiedSurface.empty() && simplifiedSurface == variable->columnName)) {

                    return {[=] (const SqlRowScope & context,
                             ExpressionValue & storage,
                             const VariableFilter & filter)
                        -> const ExpressionValue &
                        {
                            auto & row = context.as<RowScope>();
                            return storage = row.currentGroupKey.at(i);
                        },
                        // TODO: return real type
                        std::make_shared<AnyValueInfo>()};
                }
            }

            // cerr << "columnName = " << columnName << endl;
            // cerr << "simplified columnName = " << simplifiedSurface << endl;
            // cerr << "g->print() = " << g->print() << endl;
            // cerr << "alias = " << alias << endl;
            // cerr << "surface = " << g->surface << endl;
            // if (variable)
                //cerr << "expression variable = " << variable->columnName << endl;
        }

        // Otherwise, it must be a variable in the output row.
        return {[=] (const SqlRowScope & context,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    auto & row = context.as<RowScope>();
             
                    const ExpressionValue * result
                        = searchRow(row.output.columns, columnName,
                                    filter, storage);

                    if (result)
                        return *result;
                    
                    throw HttpReturnException
                        (400, "variable '" + columnName.toUtf8String() 
                         + "' must appear in the GROUP BY clause or "
                         "be used in an aggregate function");
                },
                std::make_shared<AtomValueInfo>()};
    }

    ColumnGetter
    doGetGroupByKey(size_t index)
    {
        return {[=] (const SqlRowScope & context,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    auto & row = context.as<RowScope>();
                    return row.currentGroupKey[index];
                },
                groupInfo[index]};
    }

    RowScope
    getRowScope(NamedRowValue & output,
                  const std::vector<ExpressionValue> & currentGroupKey) const
    {
        return RowScope(output, currentGroupKey);
    }

    // Represents a clause that is output by the program TODO: Rename this
    struct OutputAggregator {
        /// Initialize from an aggregator function
        OutputAggregator(int inputIndex, int numInputs,
                         BoundAggregator aggregate)
            :  inputIndex(inputIndex), numInputs(numInputs),
               aggregate(std::move(aggregate))
        {
        }

        /// Initialize from a variable reference
        int inputIndex;               ///< First column number we get the input from
        int numInputs;                ///< Number of inputs we get
        BoundAggregator aggregate;    ///< Aggregate function, for aggregates
    };

    void initializePerThreadAggregators(GroupMapValue& mapInstance)
    {
        mapInstance.resize(outputAgg.size());
        for (unsigned i = 0;  i < outputAgg.size();  ++i) {
           mapInstance[i] = outputAgg[i].aggregate.init();
        }
    }

    void aggregateRow(GroupMapValue& mapInstance,
                      const std::vector<ExpressionValue>& row)
    {
        for (size_t i = 0;  i < outputAgg.size();  ++i) {
            ExcAssert(argOffset + outputAgg[i].inputIndex + outputAgg[i].numInputs <= row.size());
            outputAgg[i].aggregate
                .process(&row[argOffset + outputAgg[i].inputIndex],
                         outputAgg[i].numInputs,
                         mapInstance[i].get());
        }
    }

    void mergeThreadMap(GroupMapValue& outMapInstance,
                        const GroupMapValue& inMapInstance)
    {
        for (size_t i = 0;  i < outputAgg.size();  ++i) {
           outputAgg[i].aggregate
               .mergeInto(outMapInstance[i].get(), inMapInstance[i].get());
        }
    }
             
    std::vector<std::shared_ptr<ExpressionValueInfo> > groupInfo;
    std::vector<OutputAggregator> outputAgg;    
    std::vector<std::shared_ptr<void> > aggData;  //working buffers for the above
    int argCounter;
    int argOffset;
    bool evaluateEmptyGroups;
};


/*****************************************************************************/
/* BOUND GROUP BY QUERY                                                      */
/*****************************************************************************/

//Replace all expressions that appears as a key in the group by
//with an expression that reads the key
std::shared_ptr<SqlExpression>
replaceGroupByKey(const std::shared_ptr<SqlExpression> p, const std::vector<Utf8String>& printGroupbyClauses)
{
    std::function<std::shared_ptr<SqlExpression> (std::shared_ptr<SqlExpression> a)>
        doArg;

    auto onChild = [&] (std::vector<std::shared_ptr<SqlExpression> > args)
        {
            for (auto & a: args) {
                a = doArg(a);
            }

            return args;
        };

    doArg = [&] (std::shared_ptr<SqlExpression> a)
        -> std::shared_ptr<SqlExpression>
        {
            auto function = dynamic_pointer_cast<FunctionCallExpression>(a);
            if (!function || (function->functionName != "rowName" && function->functionName != "rowHash")) {
                auto printref = a->print();

                auto iter = std::find(printGroupbyClauses.begin(), printGroupbyClauses.end(), printref);
                if (iter != printGroupbyClauses.end()) {
                    size_t index = iter - printGroupbyClauses.begin();
                    return  make_shared<GroupByKeyExpression>(index);
                }
            }

            return a->transform(onChild);
        };

    // Walk the tree.
    auto result = doArg(p);
    result->surface = result->print();
    return result;
}

BoundGroupByQuery::
BoundGroupByQuery(const SelectExpression & select,
                  const Dataset & from,
                  const Utf8String& alias,
                  const WhenExpression & when,
                  const SqlExpression & where,
                  const TupleExpression & groupBy,
                  const std::vector< std::shared_ptr<SqlExpression> >& aggregatorsExpr,
                  const SqlExpression & having,
                  const SqlExpression & rowName,
                  const OrderByExpression & orderBy)
    : from(from),
      when(when),
      where(where),
      rowContext(new SqlExpressionDatasetScope(from, alias)),
      groupBy(groupBy),
      select(select),
      having(having.shallowCopy()),
      orderBy(orderBy),
      numBuckets(1),
      logger(getMldbLog<BoundGroupByQuery>())
{
    for (auto & g: groupBy.clauses) {
        calc.push_back(g);
    }

    // Convert the select clauses to a list
    for (auto & expr : aggregatorsExpr)
    {
        auto fn = dynamic_cast<const FunctionCallExpression *>(expr.get());
        //Important: This assumes they are in the same order as in the group context
        for (auto & a: fn->args) {
           calc.emplace_back(a);
        }
    }

    size_t maxNumRow = from.getMatrixView()->getRowCount();
    int maxNumTask = numCpus() * TASK_PER_THREAD;
    //try to have at least MIN_ROW_PER_TASK rows per task
    numBuckets = maxNumRow <= maxNumTask*MIN_ROW_PER_TASK? maxNumRow / maxNumTask : maxNumTask;
    numBuckets = std::max(numBuckets, (size_t)1U);

    // bind the subselect
    //false means no implicit sort by rowhash, we want unsorted
    subSelect.reset(new BoundSelectQuery(subSelectExpr, from, alias, when, where, subOrderBy, calc, numBuckets));

    std::vector<std::shared_ptr<ExpressionValueInfo> > groupInfo;
    for (size_t c = 0; c < groupBy.clauses.size(); ++c) {
        groupInfo.push_back(subSelect->boundCalc[c].info);
    }

    groupContext = make_shared<GroupContext>(from, alias, groupBy, groupInfo);

    // replace any sub-expression thats a group key with a GroupByKeyExpression
    std::vector<Utf8String> printGroupbyClauses;
    for (auto& c : groupBy.clauses) {
        printGroupbyClauses.push_back(c->print());
    }

    for (auto& p : this->select.clauses)
        p = dynamic_pointer_cast<SqlRowExpression>(replaceGroupByKey(p, printGroupbyClauses));

    for (auto& p : this->orderBy.clauses) {
        p.first =replaceGroupByKey(p.first, printGroupbyClauses);
    }

    if (!this->having->isConstantTrue() && !this->having->isConstantFalse())
        this->having = replaceGroupByKey(this->having, printGroupbyClauses);

    auto pRowName = replaceGroupByKey(rowName.shallowCopy(), printGroupbyClauses);

     // Bind the row name expression
    boundRowName = pRowName->bind(*groupContext);

}

std::pair<bool, std::shared_ptr<ExpressionValueInfo> >
BoundGroupByQuery::
execute(RowProcessor processor,
        ssize_t offset,
        ssize_t limit,
        const ProgressFunc & onProgress)
{
    //STACK_PROFILE(BoundGroupByQuery);

    typedef std::tuple<std::vector<ExpressionValue>,
                       NamedRowValue,
                       std::vector<ExpressionValue> >
        SortedRow;

    std::vector<SortedRow> rowsSorted;
    std::atomic<ssize_t> groupsDone(0);

    typedef std::vector<ExpressionValue> RowKey;
    typedef std::map<RowKey, GroupMapValue> GroupByMapType;
    std::vector<GroupByMapType> accum(numBuckets);

    for (const auto & c: select.clauses) {
        if (c->isWildcard()) {
            throw HttpReturnException(
                400, "Wildcard cannot be used with GROUP BY");
        }
    }

    //bind the selectexpression, this will create the bound aggregators (which we wont use, ah!)
    auto boundSelect = select.bind(*groupContext);
    auto selectInfo = boundSelect.info;

    //bind the having expression. Must be bound after the select because
    //we placed the having aggregators after the select aggregator in the list
    BoundSqlExpression boundHaving = having->bind(*groupContext);

    //The bound having must resolve to a boolean expression
    if (!having->isConstantTrue() && !having->isConstantFalse() && dynamic_cast<BooleanValueInfo*>(boundHaving.info.get()) == nullptr)
        throw HttpReturnException(400, "HAVING must be a boolean expression");

    // Bind in the order by expression. Must be bound after the having because
    //we placed the orderby aggregators after the having aggregator in the list
    boundOrderBy = orderBy.bindAll(*groupContext);

    // When we get a row, we record it under the group key
    auto onRow = [&] (NamedRowValue & row,
                      const std::vector<ExpressionValue> & calc,
                      int groupNum)
    {
       GroupByMapType & map = accum[groupNum];
       RowKey rowKey(calc.begin(), calc.begin() + groupBy.clauses.size());

       auto pair = map.insert({rowKey, GroupMapValue()});
       auto & iter = pair.first;
       if (pair.second)
       {
          //initialize aggregator data
          groupContext->initializePerThreadAggregators(iter->second);
       }

       groupContext->aggregateRow(iter->second, calc);

       return true;
    };  
            
    subSelect->execute(onRow, true /*processInParallel*/, 0, -1, onProgress);
  
    //merge the maps in fixed order
    GroupByMapType destMap;
    std::vector<GroupByMapType>& threads = accum;
    if (threads.size() > 0)
    {
//        STACK_PROFILE(MergingBuckets);
        for (auto & srcMap : threads)
        {
            for (auto it = srcMap.begin(); it != srcMap.end(); ++it)
            {
                auto pair = destMap.insert({it->first, GroupMapValue()});
                auto destiter = pair.first;
                if (pair.second)
                {
                    //initialize aggregator data
                    groupContext->initializePerThreadAggregators(destiter->second);
                }

                groupContext->mergeThreadMap(destiter->second, it->second);
            }
        }
    }

    if (destMap.empty() && groupContext->evaluateEmptyGroups
        && groupBy.clauses.empty())
    {
        auto pair = destMap.emplace(RowKey(), GroupMapValue());
        groupContext->initializePerThreadAggregators(pair.first->second);
    }

    //output rows
    //each entry in the final map should be an output row for us   
    for (auto it = destMap.begin(); it != destMap.end(); ++it)
    {
        RowKey rowKey = it->first;
        groupContext->aggData = it->second;

         // Create the context to evaluate the row name and order by
        NamedRowValue outputRow;

        auto rowContext = groupContext->getRowScope(outputRow, rowKey);

        //Evaluate the HAVING expression
        ExpressionValue havingResult = boundHaving(rowContext, GET_LATEST);

        if (!havingResult.isTrue())
            continue;

        outputRow.rowName = boundRowName(rowContext, GET_LATEST).coerceToPath();
        outputRow.rowHash = outputRow.rowName;        

        //Evaluating the whole bound select expression
        ExpressionValue result = boundSelect(rowContext, GET_ALL);
        result.mergeToRowDestructive(outputRow.columns);

        //In case of no output ordering, we can early exit
        if (boundOrderBy.empty()) {
            ssize_t n = groupsDone.fetch_add(1);
            if (limit != -1 && n >= limit)
               break;

            processor(outputRow);
        }
        else
        {
             //Else we add the result to the output rows
            std::vector<ExpressionValue> sortFields
            = boundOrderBy.apply(rowContext);

            std::vector<ExpressionValue> calcd;
                
            rowsSorted.emplace_back(std::move(sortFields),
                                    std::move(outputRow),
                                    std::move(calcd));
        }           
    }

    if (boundOrderBy.empty())
        return {true, selectInfo};

    // Compare two rows according to the sort criteria
    auto compareRows = [&] (const SortedRow & row1,
                            const SortedRow & row2)
        {
            return boundOrderBy.less(std::get<0>(row1),
                                     std::get<0>(row2));
        };

    // Sort our output rows
    std::sort(rowsSorted.begin(), rowsSorted.end(), compareRows);

    // Now select only the required subset of sorted rows
    if (limit == -1)
        limit = rowsSorted.size();

    ExcAssertGreaterEqual(offset, 0);

    if (select.distinctExpr.size() > 0) {

        std::vector<ExpressionValue> reference;
        size_t numDistinctOnClauses = select.distinctExpr.size();
        reference.resize(numDistinctOnClauses);
        ssize_t count = 0;

        for (unsigned i = 0;  i < rowsSorted.size();  ++i) {

            std::vector<ExpressionValue> & mark = std::get<0>(rowsSorted[i]);

            if (i == 0) {
                std::copy_n(mark.begin(), numDistinctOnClauses, reference.begin());
            }
            else {

                bool same = true;
                for (int i = 0; i < numDistinctOnClauses; ++i){
                    if (reference[i] != mark[i]) {
                        same = false;
                        break;
                    }
                }

                if (!same)
                    std::copy_n(mark.begin(), numDistinctOnClauses, reference.begin());
                else
                    continue; //skip duplicates
            }
            ++count;

            if (count <= offset)
                continue;

            auto & row = std::get<1>(rowsSorted[i]);

            /* Finally, pass to the terminator to continue. */
            if (!processor(row))
                return {false, selectInfo}; //early exis on processor error

            if (count - offset == limit)
                break;
        }

    }
    else {

        ssize_t begin = std::min<ssize_t>(offset, rowsSorted.size());
        ssize_t end = std::min<ssize_t>(offset + limit, rowsSorted.size());

        for (unsigned i = begin;  i < end;  ++i) {
            auto & row = std::get<1>(rowsSorted[i]);

            /* Finally, pass to the terminator to continue. */
            if (!processor(row))
                return {false, selectInfo}; //early exis on processor error
        } 
    }  

    return {true, selectInfo};
}

} // namespace MLDB

