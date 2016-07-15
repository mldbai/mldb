/** bound_queries.cc
    Jeremy Barnes, 12 August 2015

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

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
#include <boost/algorithm/string.hpp>

#include "mldb/jml/utils/profile.h"


using namespace std;


namespace Datacratic {

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
    virtual void execute(std::function<bool (NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd,
                                             int bucketNum)> processor,
                         bool processInParallel,
                         ssize_t offset,
                         ssize_t limit,
                         std::function<bool (const Json::Value &)> onProgress) = 0;

    virtual std::shared_ptr<ExpressionValueInfo> getOutputInfo() const = 0;
};

struct UnorderedExecutor: public BoundSelectQuery::Executor {
    std::shared_ptr<MatrixView> matrix;
    GenerateRowsWhereFunction whereGenerator;
    SqlExpressionDatasetScope & context;
    BoundSqlExpression whereBound;
    BoundWhenExpression whenBound;
    BoundSqlExpression boundSelect;
    std::vector<BoundSqlExpression> boundCalc;
    int numBuckets;
    typedef std::function<bool (NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd,
                                             int rowNum)> ExecutorAggregator;

    UnorderedExecutor(std::shared_ptr<MatrixView> matrix,
                      GenerateRowsWhereFunction whereGenerator,
                      SqlExpressionDatasetScope & context,
                      BoundWhenExpression whenBound,
                      BoundSqlExpression boundSelect,
                      std::vector<BoundSqlExpression> boundCalc,
                      OrderByExpression newOrderBy,
                      int numBuckets)
        : matrix(std::move(matrix)),
          whereGenerator(std::move(whereGenerator)),
          context(context),
          whenBound(std::move(whenBound)),
          boundSelect(std::move(boundSelect)),
          boundCalc(std::move(boundCalc)),
          numBuckets(numBuckets)
    {
    }

     virtual void execute(std::function<bool (NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd,
                                             int rowNum)> processor,
                         bool processInParallel,
                         ssize_t offset,
                         ssize_t limit,
                         std::function<bool (const Json::Value &)> onProgress)
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
    void execute_bloc(std::function<bool (NamedRowValue & output,
                                          std::vector<ExpressionValue> & calcd,
                                          int rowNum)> processor,
                      bool processInParallel,
                      ssize_t offset,
                      ssize_t limit,
                      std::function<bool (const Json::Value &)> onProgress)
    {
        //STACK_PROFILE(UnorderedExecutor);
        //cerr << "bound query unordered num buckets: " << numBuckets << endl;
        QueryThreadTracker parentTracker;

        // Get a list of rows that we run over
        // Ordering is arbitrary but deterministic
        auto rows = whereGenerator(-1, Any()).first;

        //cerr << "ROWS MEMORY SIZE " << rows.size() * sizeof(RowName) << endl;

        // Simple case... no order by and no limit

        ExcAssert(numBuckets != 0);

        // Do we select *?  In that case we can avoid a lot of copying
        bool selectStar = boundSelect.expr->isIdentitySelect(context);

        size_t numRows = rows.size();
        size_t numPerBucket = std::max((size_t)std::floor((float)numRows / numBuckets), (size_t)1);
        size_t effectiveNumBucket = std::min((size_t)numBuckets, numRows);

        auto doRow = [&] (int rowNum) -> bool
            {
                //if (rowNum % 1000 == 0)
                //    cerr << "applying row " << rowNum << " of " << rows.size() << endl;

                //RowName rowName = rows[rowNum];

                auto row = matrix->getRow(rows[rowNum]);
                auto output = processRow(row, rowNum, numPerBucket, selectStar);

                int bucketNumber = numBuckets > 0 ? std::min((size_t)(rowNum/numPerBucket), (size_t)(numBuckets-1)) : -1;

                /* Finally, pass to the terminator to continue. */
                return processor(std::get<0>(output), std::get<1>(output), bucketNumber);
            };

        if (numBuckets > 0) {
            ExcAssert(processInParallel);
            ExcAssertEqual(limit, -1);
            ExcAssertEqual(offset, 0);
            std::atomic_ulong bucketCount(0);
            auto doBucket = [&] (int bucketNumber) -> bool
                {
                    size_t it = bucketNumber * numPerBucket;
                    int stopIt = bucketNumber == numBuckets - 1 ? numRows : it + numPerBucket;
                    for (; it < stopIt; ++it)
                    {
                        if (!doRow(it))
                            return false;
                    }

                    if (onProgress) {
                        Json::Value progress;
                        progress["percent"] = (float) ++bucketCount / effectiveNumBucket;
                        onProgress(progress);
                    }
                    return true;
                };

            parallelMap(0, effectiveNumBucket, doBucket);
        }
        else {
            size_t upper =  rows.size();
            if (limit != -1)
                upper = std::min((size_t)(offset+limit), upper);

            if (offset <= upper) {
                if (processInParallel) {
                    parallelMap(offset, upper, doRow);
                }
                else {
                    // TODO: to reduce memory usage, we should fill blocks of
                    // output on worker threads
                    // in order as much as possible
                    // while calling the aggregator on the caller thread.
                    ExcAssert(offset >= 0 && offset <= upper);
                    std::vector<std::tuple<NamedRowValue, std::vector<ExpressionValue> >>
                        output(upper-offset);

                    auto copyRow = [&] (int rowNum)
                    {
                        auto row = matrix->getRow(rows[rowNum]);

                        auto outputRow =
                            processRow(row, rowNum, numPerBucket, selectStar);
                        output[rowNum-offset] = std::move(outputRow);
                    };

                    parallelMap(offset, upper, copyRow);

                    for (size_t i = offset; i < upper; ++i) {
                        auto& outputRow = output[i-offset];
                        if (!processor(std::get<0>(outputRow),
                                       std::get<1>(outputRow), -1)) {
                            break;
                        }
                    }
                }
            }
        }
    }

    /* execute_iterative will use the whereGenerator rowStream to get the rowNames one by one
       in order to avoid having a big array of all the relevant rowNames                    */
     void execute_iterative(std::function<bool (NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd,
                                             int rowNum)> processor,
                         bool processInParallel,
                         ssize_t offset,
                         ssize_t limit,
                         std::function<bool (const Json::Value &)> onProgress)
    {   
        //STACK_PROFILE(UnorderedExecutor_optimized);
        //cerr << "UnorderedIterExecutor num buckets: " << numBuckets << " allowMT " << allowMT << endl;
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
        auto doBucket = [&] (int bucketNumber) -> bool
            {
                size_t it = bucketNumber * numPerBucket;
                int stopIt = bucketNumber == numBuckets - 1 ? numRows : it + numPerBucket;
                auto stream = whereGenerator.rowStream->clone();
                stream->initAt(it);
                for (;  it < stopIt; ++it)
                {
                    RowName rowName = stream->next();
                    auto row = matrix->getRow(rowName);

                    auto output = processRow(row, it, numPerBucket, selectStar);
                    int bucketNumber = numBuckets > 0 ? std::min((size_t)(it/numPerBucket), (size_t)(numBuckets-1)) : -1;

                    /* Finally, pass to the terminator to continue. */
                    if (!processor(std::get<0>(output), std::get<1>(output), bucketNumber))
                        return false;
                }
                if (onProgress) {
                    Json::Value progress;
                    progress["percent"] = (float) ++bucketCount / effectiveNumBucket;
                    onProgress(progress);
                }
                return true;
            };

            parallelMap(0, effectiveNumBucket, doBucket);
    }

    std::tuple<NamedRowValue, std::vector<ExpressionValue> >
    processRow(MatrixNamedRow& row,
               int rowNum,
               int numPerBucket,
               bool selectStar)
    {
        auto rowContext = context.getRowScope(row);

        whenBound.filterInPlace(row, rowContext);

        std::tuple<NamedRowValue, std::vector<ExpressionValue> > output;

        NamedRowValue& outputRow = std::get<0>(output);
        outputRow.rowName = row.rowName;
        outputRow.rowHash = row.rowName;
    
        auto selectRowScope = context.getRowScope(row);
        vector<ExpressionValue>& calcd = std::get<1>(output);
        calcd.resize(boundCalc.size());

        // Run the extra calculations
        for (unsigned i = 0;  i < boundCalc.size();  ++i) {
            calcd[i] = std::move(boundCalc[i](selectRowScope, GET_LATEST));
        }
        
        if (selectStar) {
            // Move into place, since we know we're selecting *
            // This is more complicated than it looks, because the input is
            // flattened but the output is structured, so we have to go
            // through the ExpressionValue to add the structure in first.
            ExpressionValue structured(std::move(row.columns));
            structured.mergeToRowDestructive(outputRow.columns);
        }
        else {
            // Run the select expression
            ExpressionValue selectOutput = boundSelect(selectRowScope, GET_ALL);
            selectOutput.mergeToRowDestructive(outputRow.columns);
        }

        return output;
    }

    virtual std::shared_ptr<ExpressionValueInfo> getOutputInfo() const
    {
        return boundSelect.info;
    }
};

struct OrderedExecutor: public BoundSelectQuery::Executor {

    std::shared_ptr<MatrixView> matrix;
    GenerateRowsWhereFunction whereGenerator;
    SqlExpressionDatasetScope & context;
    BoundWhenExpression whenBound;
    BoundSqlExpression boundSelect;
    std::vector<BoundSqlExpression> boundCalc;
    OrderByExpression newOrderBy;

    OrderedExecutor(std::shared_ptr<MatrixView> matrix,
                    GenerateRowsWhereFunction whereGenerator,
                    SqlExpressionDatasetScope & context,
                    BoundWhenExpression whenBound,
                    BoundSqlExpression boundSelect,
                    std::vector<BoundSqlExpression> boundCalc,
                    OrderByExpression newOrderBy)
        : matrix(std::move(matrix)),
          whereGenerator(std::move(whereGenerator)),
          context(context),
          whenBound(std::move(whenBound)),
          boundSelect(std::move(boundSelect)),
          boundCalc(std::move(boundCalc)),
          newOrderBy(std::move(newOrderBy))
    {
    }

    virtual void execute(std::function<bool (NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd, 
                                             int rowNum)> processor,
        bool processInParallel,
        ssize_t offset,
        ssize_t limit,
        std::function<bool (const Json::Value &)> onProgress)
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

        auto doWhere = [&] (int rowNum) -> bool
            {
                QueryThreadTracker childTracker = parentTracker.child();

                auto row = matrix->getRow(rows[rowNum]);

                if (onProgress && rowsAdded % 1000 == 0) {
                    Json::Value progress;
                    progress["percent"] = (float) rowsAdded / rows.size();
                    onProgress(progress);
                }

                // Check it matches the where expression.  If not, we don't process
                // it.
                auto rowContext = context.getRowScope(row);

                //where already checked in whereGenerator

                whenBound.filterInPlace(row, rowContext);

                NamedRowValue outputRow;
                outputRow.rowName = row.rowName;
                outputRow.rowHash = row.rowName;
            
                auto selectRowScope = context.getRowScope(row);
             
                // Run the bound select expressions
                ExpressionValue selectOutput
                = boundSelect(selectRowScope, GET_ALL);
                selectOutput.mergeToRowDestructive(outputRow.columns);

                vector<ExpressionValue> calcd(boundCalc.size());
                for (unsigned i = 0;  i < boundCalc.size();  ++i) {
                    calcd[i] = std::move(boundCalc[i](selectRowScope, GET_LATEST));
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

        ML::Timer timer;

        parallelMap(0, rows.size(), doWhere);

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

        auto doSelect = [&] (int rowNum) -> bool
            {
                auto & row = std::get<1>(rowsSorted[rowNum]);
                auto & calcd = std::get<2>(rowsSorted[rowNum]);

                /* Finally, pass to the terminator to continue. */
                return processor(row, calcd, rowNum);
            };

        // Now select only the required subset of sorted rows
        if (limit == -1)
            limit = rowsSorted.size();

        ExcAssertGreaterEqual(offset, 0);

        ssize_t begin = std::min<ssize_t>(offset, rowsSorted.size());
        ssize_t end = std::min<ssize_t>(offset + limit, rowsSorted.size());

        for (unsigned i = begin;  i < end;  ++i) {
            doSelect(i);
        }

        cerr << "reduce took " << timer.elapsed() << endl;
    }

    virtual std::shared_ptr<ExpressionValueInfo> getOutputInfo() const
    {
        return boundSelect.info;
    }
};

namespace {

struct SortByRowHash {
    bool operator () (const RowName & row1, const RowName & row2)
    {
        RowHash h1(row1), h2(row2);

        return h1 < h2 || (h1 == h2 && row1 < row2);
    }
};

} // file scope

struct RowHashOrderedExecutor: public BoundSelectQuery::Executor {
    std::shared_ptr<MatrixView> matrix;
    GenerateRowsWhereFunction whereGenerator;
    SqlExpressionDatasetScope & context;
    BoundWhenExpression whenBound;
    BoundSqlExpression boundSelect;
    std::vector<BoundSqlExpression> boundCalc;
    OrderByExpression newOrderBy;
    bool allowParallelOutput;

    RowHashOrderedExecutor(std::shared_ptr<MatrixView> matrix,
                           GenerateRowsWhereFunction whereGenerator,
                           SqlExpressionDatasetScope & context,
                           BoundWhenExpression whenBound,
                           BoundSqlExpression boundSelect,
                           std::vector<BoundSqlExpression> boundCalc,
                           OrderByExpression newOrderBy,
                           bool allowParallelOutput)
        : matrix(std::move(matrix)),
          whereGenerator(std::move(whereGenerator)),
          context(context),
          whenBound(std::move(whenBound)),
          boundSelect(std::move(boundSelect)),
          boundCalc(std::move(boundCalc)),
          newOrderBy(std::move(newOrderBy)),
          allowParallelOutput(allowParallelOutput)
    {
    }

     virtual void execute(std::function<bool (NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd,
                                             int rowNum)> processor,
                         bool processInParallel,
                         ssize_t offset,
                         ssize_t limit,
                         std::function<bool (const Json::Value &)> onProgress)
    {
        if (limit < 0 || !(whereGenerator.rowStream))
          return execute_bloc(processor, offset, limit, onProgress);
        else
          return execute_iter(processor, offset, limit, onProgress);
    }

     /* execute_bloc will query all the relevant rowNames in advance
       using the whereGenerator()                                           */
     virtual void execute_bloc(std::function<bool (NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd,
                                             int rowNum)> processor,
                         ssize_t offset,
                         ssize_t limit,
                         std::function<bool (const Json::Value &)> onProgress)
    {
//        STACK_PROFILE(RowHashOrderedExecutor.execute_bloc);

        QueryThreadTracker parentTracker;

        ML::Timer rowsTimer;

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
        
        ML::Timer scanTimer;

        // Special but exceedingly common case: we sort by row hash.

        ML::Spinlock mutex;
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

                QueryThreadTracker childTracker
                    = std::move(parentTracker.child());

                MatrixNamedRow row;
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

                    //RowName rowName = rows[rowNum];

                    row = std::move(matrix->getRow(rows[rowNum]));

                    // Check it matches the where expression.  If not, we don't process
                    // it.
                    auto rowContext = context.getRowScope(row);

                    //where was already filtered by the where generator

                    whenBound.filterInPlace(row, rowContext);
                    NamedRowValue outputRow;
                    outputRow.rowName = row.rowName;
                    outputRow.rowHash = row.rowName;

                    vector<ExpressionValue> calcd(boundCalc.size());
                    for (unsigned i = 0;  i < boundCalc.size();  ++i) {
                        calcd[i] = std::move(boundCalc[i](rowContext, GET_LATEST));
                    }

                    if (selectStar) {
                        // Move into place, since we know we're selecting *
                        // This is more complicated than it looks, because the input is
                        // flattened but the output is structured, so we have to go
                        // through the ExpressionValue to add the structure in first.
                        ExpressionValue structured(std::move(row.columns));
                        structured.mergeToRowDestructive(outputRow.columns);
                    }
                    else {
                        // Run the select expression
                        ExpressionValue selectOutput = boundSelect(rowContext, GET_ALL);
                        selectOutput.mergeToRowDestructive(outputRow.columns);
                    }

                    std::unique_lock<ML::Spinlock> guard(mutex);
                    sorted.emplace_back(row.rowHash,
                                        std::move(outputRow),
                                        std::move(calcd));

                    if (limit != -1 && sorted.size() >= offset + limit) {
                        maxRowNumNeeded = maxRowNum.load();
                        return true;
                    }

                    return true;
                } catch (...) {
                    rethrowHttpException(-1, "Executing non-grouped query bound to row: " + ML::getExceptionString(),
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
                    return;
            }
        }
        else {
            std::atomic<bool> stop(false);
            auto onOutput = [&] (size_t i)
                {
                    if (stop)
                        return;

                    if (!processor(std::get<1>(sorted[i]), std::get<2>(sorted[i]), i - begin)) {
                        stop = true;
                        return;
                    }
                };

            parallelMap(begin, end, onOutput);
        }

        cerr << "Output " << sorted.size() << " in " << scanTimer.elapsed()
             << " on " << (scanTimer.elapsed_cpu() / scanTimer.elapsed_wall()) << " cpus "
             << " at " << sorted.size() / scanTimer.elapsed_wall() << "/second and "
             << sorted.size() / scanTimer.elapsed_cpu() << " /cpu-second" << endl;

        return;
    }

   /* execute_iterative will use the whereGenerator rowStream to get the rowNames one by one
       in order to avoid having a big array of all the relevant rowNames                    */
    virtual void execute_iter(std::function<bool (NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd,
                                             int rowNum)> processor,
                         ssize_t offset,
                         ssize_t limit,
                         std::function<bool (const Json::Value &)> onProgress)
    {
        //STACK_PROFILE(RowHashOrderedExecutor_execute_iter);

        QueryThreadTracker parentTracker;

        if (limit == 0)
          throw HttpReturnException(400, "limit must be non-zero");

        ML::Timer rowsTimer;

        typedef std::vector<RowName> AccumRows;
        
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
              RowName rowName = stream->next();

              if (rowName == RowName())
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
        auto compareRows = [&] (const RowName & row1,
                                const RowName & row2) -> bool
            {
                return RowHash(row1) < RowHash(row2);
            };
            
        auto rowsMerged = parallelMergeSort(accum.threads, compareRows);

        if (rowsMerged.size() < offset )
          return;

        rowsMerged.erase(rowsMerged.begin(), rowsMerged.begin() + offset);

        if (rowsMerged.size() > limit)
          rowsMerged.erase(rowsMerged.begin() + limit, rowsMerged.end());

        //Assuming the limit is small enough we can output the rows in order

         // Do we select *?  In that case we can avoid a lot of copying
        bool selectStar = boundSelect.expr->isIdentitySelect(context);

        int count = 0;
        for (auto & r : rowsMerged) {

            MatrixNamedRow row = std::move(matrix->getRow(r));
            auto rowContext = context.getRowScope(row);

            whenBound.filterInPlace(row, rowContext);
            NamedRowValue outputRow;
            outputRow.rowName = row.rowName;
            outputRow.rowHash = row.rowName;

            vector<ExpressionValue> calcd(boundCalc.size());
            for (unsigned i = 0;  i < boundCalc.size();  ++i) {
                calcd[i] = std::move(boundCalc[i](rowContext, GET_LATEST));
            }

            if (selectStar) {
                // Move into place, since we know we're selecting *
                // This is more complicated than it looks, because the input is
                // flattened but the output is structured, so we have to go
                // through the ExpressionValue to add the structure in first.
                ExpressionValue structured(std::move(row.columns));
                structured.mergeToRowDestructive(outputRow.columns);
            }
            else {
                // Run the select expression
                ExpressionValue selectOutput = boundSelect(rowContext, GET_ALL);
                selectOutput.mergeToRowDestructive(outputRow.columns);
            }
            if (!processor(outputRow, calcd, count))
              break;

            ++count;
        }      
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
                 int  numBuckets)
    : select(select), from(from), when(when), where(where), calc(calc),
      orderBy(orderBy), context(new SqlExpressionDatasetScope(from, std::move(alias)))
{
    try {
        SqlExpressionWhenScope whenScope(*context);
        auto whenBound = when.bind(whenScope);

        // Get a generator for the rows that match 
        auto whereGenerator = context->doCreateRowsWhereGenerator(where, 0, -1);

        auto matrix = from.getMatrixView();

        auto boundSelect = select.bind(*context);

        std::vector<BoundSqlExpression> boundCalc;
        for (auto & c: calc) {
            ExcAssert(c);
            boundCalc.emplace_back(c->bind(*context));
        }

        // Get a generator rows from the for the ordered, limited where expression

        // Remove any constants from the order by clauses
        OrderByExpression newOrderBy;
        for (auto & x: orderBy.clauses) {

            // TODO: Better constant detection
            if (x.first->getType() == "constant")
                continue;  

            newOrderBy.clauses.push_back(x);
        }

        bool orderByRowHash = false;
        bool outputInParallel = false /*newOrderBy.clauses.empty()*/;
        if (newOrderBy.clauses.size() == 1
            && newOrderBy.clauses[0].second == ASC
            && newOrderBy.clauses[0].first->getType() == "function"
            && newOrderBy.clauses[0].first->getOperation() == "rowHash") {
            orderByRowHash = true;
        }
        else if (newOrderBy.clauses.size() > 0) {
            //if we have an order by, always add a rowHash() to make sure we have a fully deterministic sorting order
            newOrderBy.clauses.emplace_back(SqlExpression::parse("rowHash()"), ASC);
        }
 
        if (orderByRowHash) {
            ExcAssert(numBuckets < 0);
            executor.reset(new RowHashOrderedExecutor(std::move(matrix),
                                                      std::move(whereGenerator),
                                                      *context,
                                                      std::move(whenBound),
                                                      std::move(boundSelect),
                                                      std::move(boundCalc),
                                                      std::move(newOrderBy),
                                                      outputInParallel));          
           
        }
        else if (!newOrderBy.clauses.empty()) {
            ExcAssert(numBuckets < 0);
            executor.reset(new OrderedExecutor(std::move(matrix),
                                               std::move(whereGenerator),
                                               *context,
                                               std::move(whenBound),
                                               std::move(boundSelect),
                                               std::move(boundCalc),
                                               std::move(newOrderBy)));
        } else {
            executor.reset(new UnorderedExecutor(std::move(matrix),
                                                 std::move(whereGenerator),
                                                *context,
                                                 std::move(whenBound),
                                                 std::move(boundSelect),
                                                 std::move(boundCalc),
                                                 std::move(newOrderBy),
                                                 numBuckets));
        }

    } JML_CATCH_ALL {
        rethrowHttpException(-1, "Binding error: "
                             + ML::getExceptionString(),
                             "select", select.surface,
                             "from", from.getStatus(),
                             "where", where.shallowCopy(),
                             "calc", calc,
                             "orderBy", orderBy);
    }
}

void
BoundSelectQuery::
execute(RowProcessorEx processor,
        ssize_t offset,
        ssize_t limit,
        std::function<bool (const Json::Value &)> onProgress)
{
    //STACK_PROFILE(BoundSelectQuery);

    auto subProcessor = [&] (NamedRowValue & row,
                              std::vector<ExpressionValue> & calc,
                              int groupNum)
    {
       return processor(row, calc);
    };

    return execute(subProcessor, processor.processInParallel, offset, limit, onProgress);

}

void
BoundSelectQuery::
execute(std::function<bool (NamedRowValue & output,
                            std::vector<ExpressionValue> & calcd,
                            int groupNum)> processor,
        bool processInParallel,
        ssize_t offset,
        ssize_t limit,
        std::function<bool (const Json::Value &)> onProgress)
{
    //STACK_PROFILE(BoundSelectQuery);

    ExcAssert(processor);

    try {
        executor->execute(processor, processInParallel, offset, limit, onProgress);
    } JML_CATCH_ALL {
        rethrowHttpException(-1, "Execution error: "
                             + ML::getExceptionString(),
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
            const TupleExpression & groupByExpression) : 
        SqlExpressionDatasetScope(dataset, alias), 
        groupByExpression(groupByExpression),
        argCounter(0), argOffset(0),
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

        auto getGroupRowName = [] (const SqlRowScope & context) -> RowName
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
                                     const ColumnName & columnName)
    {
        // First, search for something that matches the surface (ugh)
        // of a group by clause.  We can use that directly.
        for (unsigned i = 0;  i < groupByExpression.clauses.size();  ++i) {
            const std::shared_ptr<SqlExpression> & g
                = groupByExpression.clauses[i];

            // This logic is not completely implemented.  We need to identify
            // any parts of the group by expression that are referred to by
            // the select clause and return their value, not just the variable
            // names.  For the moment, we're just hacking it so that it will
            // work with variable names.

            ColumnName simplifiedSurface;
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
             

    std::vector<OutputAggregator> outputAgg;    
    std::vector<std::shared_ptr<void> > aggData;  //working buffers for the above
    int argCounter;
    int argOffset;
    bool evaluateEmptyGroups;
};


/*****************************************************************************/
/* BOUND GROUP BY QUERY                                                      */
/*****************************************************************************/

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
      groupContext(new GroupContext(from, alias, groupBy)),
      groupBy(groupBy),
      select(select),
      having(having),
      orderBy(orderBy),
      numBuckets(1)
{
    for (auto & g: groupBy.clauses) {
        calc.push_back(g);
    }

    groupContext->argOffset = calc.size();

    // Convert the select clauses to a list
    for (auto & expr : aggregatorsExpr)
    {
        auto fn = dynamic_cast<const FunctionCallExpression *>(expr.get());
        //Important: This assumes they are in the same order as in the group context
        for (auto & a: fn->args) {
           calc.emplace_back(a);
        } 
    }

    // Bind the row name expression
    boundRowName = rowName.bind(*groupContext);

    size_t maxNumRow = from.getMatrixView()->getRowCount();
    int maxNumTask = numCpus() * TASK_PER_THREAD;
    //try to have at least MIN_ROW_PER_TASK rows per task
    numBuckets = maxNumRow <= maxNumTask*MIN_ROW_PER_TASK? maxNumRow / maxNumTask : maxNumTask;
    numBuckets = std::max(numBuckets, (size_t)1U);

    // bind the subselect
    //false means no implicit sort by rowhash, we want unsorted
    subSelect.reset(new BoundSelectQuery(subSelectExpr, from, alias, when, where, subOrderBy, calc, numBuckets));

}

void
BoundGroupByQuery::
execute(RowProcessor processor,
             ssize_t offset,
             ssize_t limit,
             std::function<bool (const Json::Value &)> onProgress)
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

    //bind the having expression. Must be bound after the select because
    //we placed the having aggregators after the select aggregator in the list
    BoundSqlExpression boundHaving = having.bind(*groupContext);

    //The bound having must resolve to a boolean expression
    if (!having.isConstantTrue() && !having.isConstantFalse() && dynamic_cast<BooleanValueInfo*>(boundHaving.info.get()) == nullptr)
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
        return;

    // Compare two rows according to the sort criteria
    auto compareRows = [&] (const SortedRow & row1,
                            const SortedRow & row2)
        {
            return boundOrderBy.less(std::get<0>(row1),
                                     std::get<0>(row2));
        };

    // Sort our output rows
    std::sort(rowsSorted.begin(), rowsSorted.end(), compareRows);

    auto doSelect = [&] (int rowNum) -> bool
        {
            auto & row = std::get<1>(rowsSorted[rowNum]);

            /* Finally, pass to the terminator to continue. */
            return processor(row);
        };

    // Now select only the required subset of sorted rows
    if (limit == -1)
        limit = rowsSorted.size();

    ExcAssertGreaterEqual(offset, 0);

    ssize_t begin = std::min<ssize_t>(offset, rowsSorted.size());
    ssize_t end = std::min<ssize_t>(offset + limit, rowsSorted.size());

    for (unsigned i = begin;  i < end;  ++i) {
        doSelect(i);
    } 
}

} // namespace MLDB
} // namespace Datacratic
