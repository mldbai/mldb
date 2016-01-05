// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** bound_queries.cc
    Jeremy Barnes, 12 August 2015

    Bound version of SQL queries.
*/

#include "mldb/server/bound_queries.h"
#include "mldb/core/dataset.h"
#include "mldb/server/dataset_context.h"
#include "mldb/jml/utils/worker_task.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/server/parallel_merge_sort.h"
#include "mldb/arch/timers.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/http/http_exception.h"
#include <boost/algorithm/string.hpp>

#include "mldb/jml/utils/profile.h"


using namespace std;


namespace Datacratic {
namespace MLDB {

__thread int QueryThreadTracker::depth = 0;


/*****************************************************************************/
/* BOUND SELECT QUERY                                                        */
/*****************************************************************************/

struct BoundSelectQuery::Executor {
    virtual void execute(std::function<bool (const NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd,
                                             int bucketNum)> aggregator,
                         ssize_t offset,
                         ssize_t limit,
                         std::function<bool (const Json::Value &)> onProgress, bool allowMT) = 0;

    virtual std::shared_ptr<ExpressionValueInfo> getOutputInfo() const = 0;
};

struct UnorderedExecutor: public BoundSelectQuery::Executor {
    std::shared_ptr<MatrixView> matrix;
    GenerateRowsWhereFunction whereGenerator;
    SqlExpressionDatasetContext & context;
    BoundSqlExpression whereBound;
    BoundWhenExpression whenBound;
    BoundSqlExpression boundSelect;
    std::vector<BoundSqlExpression> boundCalc;
    int numBuckets;

    UnorderedExecutor(std::shared_ptr<MatrixView> matrix,
                      GenerateRowsWhereFunction whereGenerator,
                      SqlExpressionDatasetContext & context,
                      BoundSqlExpression whereBound,
                      BoundWhenExpression whenBound,
                      BoundSqlExpression boundSelect,
                      std::vector<BoundSqlExpression> boundCalc,
                      OrderByExpression newOrderBy,
                      int numBuckets)
        : matrix(std::move(matrix)),
          whereGenerator(std::move(whereGenerator)),
          context(context),
          whereBound(std::move(whereBound)),
          whenBound(std::move(whenBound)),
          boundSelect(std::move(boundSelect)),
          boundCalc(std::move(boundCalc)),
          numBuckets(numBuckets)
    {
    }

    virtual void execute(std::function<bool (const NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd,
                                             int rowNum)> aggregator,
                         ssize_t offset,
                         ssize_t limit,
                         std::function<bool (const Json::Value &)> onProgress,
                         bool allowMT)
    {   
        //cerr << "bound query num buckets: " << numBuckets << endl;
        QueryThreadTracker parentTracker;

        // Get a list of rows that we run over
        auto rows = whereGenerator(-1, Any()).first;

        // Simple case... no order by and no limit

        ExcAssertEqual(limit, -1);
        ExcAssertEqual(offset, 0);
        ExcAssert(numBuckets != 0);

        // Do we select *?  In that case we can avoid a lot of copying
        bool selectStar = boundSelect.expr->isIdentitySelect(context);

        // Do we have where TRUE?  In that case we can avoid evaluating 
        bool whereTrue = whereBound.expr->isConstantTrue();

        size_t numPerBucket = (size_t)std::ceil((float)rows.size() / numBuckets);

        auto doRow = [&] (int rowNum) -> bool
            {
                QueryThreadTracker childTracker = parentTracker.child();

                //if (rowNum % 1000 == 0)
                //    cerr << "applying row " << rowNum << " of " << rows.size() << endl;

                //RowName rowName = rows[rowNum];

                auto row = matrix->getRow(rows[rowNum]);

                // Check it matches the where expression.  If not, we don't process
                // it.
                auto rowContext = context.getRowContext(row);

                if (!whereTrue && !whereBound(rowContext).isTrue())
                    return true;

                whenBound.filterInPlace(row, rowContext);

                NamedRowValue outputRow;
                outputRow.rowName = row.rowName;
                outputRow.rowHash = row.rowName;
            
                auto selectRowContext = context.getRowContext(row);
                vector<ExpressionValue> calcd(boundCalc.size());
                // Run the extra calculations
                for (unsigned i = 0;  i < boundCalc.size();  ++i) {
                    calcd[i] = std::move(boundCalc[i](selectRowContext));
                }
                
                if (selectStar) {
                    // Move into place, since we know we're selecting *
                    outputRow.columns.reserve(row.columns.size());
                    for (auto & c: row.columns) {
                        outputRow.columns.emplace_back
                            (std::move(std::get<0>(c)),
                             ExpressionValue(std::move(std::get<1>(c)),
                                             std::get<2>(c)));
                    }
                }
                else {
                    // Run the select expression
                    ExpressionValue selectOutput = boundSelect(selectRowContext);
                    selectOutput.mergeToRowDestructive(outputRow.columns);
                }

                int bucketNumber = numBuckets > 0 ? rowNum/numPerBucket : -1;
                
                /* Finally, pass to the terminator to continue. */
                return aggregator(outputRow, calcd, bucketNumber);
            };

        if (numBuckets > 0) {
            int numRows = rows.size();
            auto doBucket = [&] (int bucketNumber) -> bool
                {
                    size_t it = bucketNumber * numPerBucket;
                    for (size_t i=0;  i<numPerBucket && it<numRows;  ++i, ++it)
                    {
                        if (!doRow(it))
                            return false;
                    }
                    return true;
                };

            if (allowMT) {
                ML::run_in_parallel(0, numBuckets, doBucket);
            }
            else {
                for (int i = 0; i < numBuckets; ++i)
                    doBucket(i);
            }
        }
        else {
            if (allowMT) {
                ML::run_in_parallel_blocked(0, rows.size(), doRow);
            }
            else {
                for (int i = 0; i < rows.size(); ++i)
                    doRow(i);
            }
        }
    }

    virtual std::shared_ptr<ExpressionValueInfo> getOutputInfo() const
    {
        return boundSelect.info;
    }
};

struct OrderedExecutor: public BoundSelectQuery::Executor {

    std::shared_ptr<MatrixView> matrix;
    GenerateRowsWhereFunction whereGenerator;
    SqlExpressionDatasetContext & context;
    BoundSqlExpression whereBound;
    BoundWhenExpression whenBound;
    BoundSqlExpression boundSelect;
    std::vector<BoundSqlExpression> boundCalc;
    OrderByExpression newOrderBy;

    OrderedExecutor(std::shared_ptr<MatrixView> matrix,
                    GenerateRowsWhereFunction whereGenerator,
                    SqlExpressionDatasetContext & context,
                    BoundSqlExpression whereBound,
                    BoundWhenExpression whenBound,
                    BoundSqlExpression boundSelect,
                    std::vector<BoundSqlExpression> boundCalc,
                    OrderByExpression newOrderBy)
        : matrix(std::move(matrix)),
          whereGenerator(std::move(whereGenerator)),
          context(context),
          whereBound(std::move(whereBound)),
          whenBound(std::move(whenBound)),
          boundSelect(std::move(boundSelect)),
          boundCalc(std::move(boundCalc)),
          newOrderBy(std::move(newOrderBy))
    {
    }

    virtual void execute(std::function<bool (const NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd, 
                                             int rowNum)> aggregator,
        ssize_t offset,
        ssize_t limit,
        std::function<bool (const Json::Value &)> onProgress,
        bool allowMT)
    {
        QueryThreadTracker parentTracker;

        // Get a list of rows that we run over
        auto rows = whereGenerator(-1, Any()).first;

        //cerr << "doing " << rows.size() << " rows with order by" << endl;
        // We have a defined order, so we need to sort here

        SqlExpressionOrderByContext orderByContext(context);

        auto boundOrderBy = newOrderBy.bindAll(orderByContext);

        // Two phases:
        // 1.  Generate rows that match the where expression, in the correct order
        // 2.  Select over those rows to get our result

   
        // For each one, generate the order by key

        typedef std::tuple<std::vector<ExpressionValue>, NamedRowValue, std::vector<ExpressionValue> > SortedRow;
        typedef std::vector<SortedRow> SortedRows;
        
        PerThreadAccumulator<SortedRows> accum;

        std::atomic<int64_t> rowsAdded(0);

        // Do we have where TRUE?  In that case we can avoid evaluating 
        bool whereTrue = whereBound.expr->isConstantTrue();

        auto doWhere = [&] (int rowNum) -> bool
            {
                QueryThreadTracker childTracker = parentTracker.child();

                auto row = matrix->getRow(rows[rowNum]);

                //if (rowNum % 1000 == 0)
                //    cerr << "applying row " << rowNum << " of " << rows.size() << endl;

                // Check it matches the where expression.  If not, we don't process
                // it.
                auto rowContext = context.getRowContext(row);

                if (!whereTrue && !whereBound(rowContext).isTrue())
                    return true;

                whenBound.filterInPlace(row, rowContext);

                NamedRowValue outputRow;
                outputRow.rowName = row.rowName;
                outputRow.rowHash = row.rowName;
            
                auto selectRowContext = context.getRowContext(row);
             
                // Run the bound select expressions
                ExpressionValue selectOutput
                    = boundSelect(selectRowContext);
                selectOutput.mergeToRowDestructive(outputRow.columns);

                vector<ExpressionValue> calcd(boundCalc.size());
                for (unsigned i = 0;  i < boundCalc.size();  ++i) {
                    calcd[i] = std::move(boundCalc[i](selectRowContext));
                }

                // Get the order by context, which can read from both the result
                // of the select and the underlying row.
                auto orderByRowContext
                    = orderByContext.getRowContext(rowContext, outputRow);

                std::vector<ExpressionValue> sortFields
                    = boundOrderBy.apply(orderByRowContext);

                SortedRows * sortedRows = &accum.get();
                sortedRows->emplace_back(std::move(sortFields),
                                         std::move(outputRow),
                                         std::move(calcd));

                ++rowsAdded;
                return true;
            };

        ML::Timer timer;

        ML::run_in_parallel_blocked(0, rows.size(), doWhere);

        cerr << "map took " << timer.elapsed() << endl;
        timer.restart();
        
        // Compare two rows according to the sort criteria
        auto compareRows = [&] (const SortedRow & row1,
                                const SortedRow & row2) -> bool
            {
                return boundOrderBy.less(std::get<0>(row1), std::get<0>(row2));
            };
            
        auto rowsSorted = parallelMergeSort(accum.threads, compareRows);

        cerr << "shuffle took " << timer.elapsed() << endl;
        timer.restart();

        auto doSelect = [&] (int rowNum) -> bool
            {
                auto & row = std::get<1>(rowsSorted[rowNum]);
                auto & calcd = std::get<2>(rowsSorted[rowNum]);

                /* Finally, pass to the terminator to continue. */
                return aggregator(row, calcd, rowNum);
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
    SqlExpressionDatasetContext & context;
    BoundSqlExpression whereBound;
    BoundWhenExpression whenBound;
    BoundSqlExpression boundSelect;
    std::vector<BoundSqlExpression> boundCalc;
    OrderByExpression newOrderBy;
    bool allowParallelOutput;

    RowHashOrderedExecutor(std::shared_ptr<MatrixView> matrix,
                           GenerateRowsWhereFunction whereGenerator,
                           SqlExpressionDatasetContext & context,
                           BoundSqlExpression whereBound,
                           BoundWhenExpression whenBound,
                           BoundSqlExpression boundSelect,
                           std::vector<BoundSqlExpression> boundCalc,
                           OrderByExpression newOrderBy,
                           bool allowParallelOutput)
        : matrix(std::move(matrix)),
          whereGenerator(std::move(whereGenerator)),
          context(context),
          whereBound(std::move(whereBound)),
          whenBound(std::move(whenBound)),
          boundSelect(std::move(boundSelect)),
          boundCalc(std::move(boundCalc)),
          newOrderBy(std::move(newOrderBy)),
          allowParallelOutput(allowParallelOutput)
    {
    }

    virtual void execute(std::function<bool (const NamedRowValue & output,
                                             std::vector<ExpressionValue> & calcd,
                                             int rowNum)> aggregator,
                         ssize_t offset,
                         ssize_t limit,
                         std::function<bool (const Json::Value &)> onProgress,
                         bool allowMT)
    {
        QueryThreadTracker parentTracker;

        ML::Timer rowsTimer;

        // Get a list of rows that we run over
        auto rows = whereGenerator(-1, Any()).first;

        if (!std::is_sorted(rows.begin(), rows.end(), SortByRowHash()))
            std::sort(rows.begin(), rows.end(), SortByRowHash());

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

        // Do we have where TRUE?  In that case we can avoid evaluating 
        bool whereTrue = whereBound.expr->isConstantTrue();

        auto doRow = [&] (ssize_t rowNum) -> bool
            {
                QueryThreadTracker childTracker = std::move(parentTracker.child());

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
                    auto rowContext = context.getRowContext(row);

                    if (whereTrue && !whereBound(rowContext).isTrue())
                        return true;


                    whenBound.filterInPlace(row, rowContext);
                    NamedRowValue outputRow;
                    outputRow.rowName = row.rowName;
                    outputRow.rowHash = row.rowName;

                    vector<ExpressionValue> calcd(boundCalc.size());
                    for (unsigned i = 0;  i < boundCalc.size();  ++i) {
                        calcd[i] = std::move(boundCalc[i](rowContext));
                    }

                    if (selectStar) {
                        // Move into place, since we know we're selecting *
                        outputRow.columns.reserve(row.columns.size());
                        for (auto & c: row.columns) {
                            outputRow.columns.emplace_back
                                (std::move(std::get<0>(c)),
                                 ExpressionValue(std::move(std::get<1>(c)),
                                                 std::get<2>(c)));
                        }
                    }
                    else {
                        // Run the select expression
                        ExpressionValue selectOutput = boundSelect(rowContext);
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


        if ((limit == -1 || sorted.size() < offset + limit)
            && rows.size() > NUM_TO_SAMPLE) {
            cerr << "hit rate is " << sorted.size() << "%" << endl;
            // TODO: knowing the hit rate, we can start off with a
            // smaller number of rows to test
            ML::run_in_parallel_blocked(NUM_TO_SAMPLE, rows.size(), doRow);
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
                if (!aggregator(std::get<1>(sorted[i]), std::get<2>(sorted[i]), i - begin))
                    return;
            }
        }
        else {
            std::atomic<bool> stop(false);
            auto onOutput = [&] (size_t i)
                {
                    if (stop)
                        return;

                    if (!aggregator(std::get<1>(sorted[i]), std::get<2>(sorted[i]), i - begin)) {
                        stop = true;
                        return;
                    }
                };

            ML::run_in_parallel_blocked(begin, end, onOutput);
        }

        cerr << "Output " << sorted.size() << " in " << scanTimer.elapsed()
             << " on " << (scanTimer.elapsed_cpu() / scanTimer.elapsed_wall()) << " cpus "
             << " at " << sorted.size() / scanTimer.elapsed_wall() << "/second and "
             << sorted.size() / scanTimer.elapsed_cpu() << " /cpu-second" << endl;

        return;
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
                 std::shared_ptr<SqlExpression> where,
                 const OrderByExpression & orderBy,
                 std::vector<std::shared_ptr<SqlExpression> > calc,
                 bool implicitOrderByRowHash, 
                 int  numBuckets)
    : select(select), from(from), when(when), where(where), calc(calc),
      orderBy(orderBy), context(new SqlExpressionDatasetContext(from, std::move(alias)))
{
    try {
        ExcAssert(where);
 
        SqlExpressionWhenScope whenScope(*context);
        auto whenBound = when.bind(whenScope);

        // Bind our where statement
        auto whereBound = where->bind(*context);

        // Get a generator for the rows that match 
        auto whereGenerator = context->doCreateRowsWhereGenerator(*where, 0, -1);

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
            && newOrderBy.clauses[0].first->getOperation() == "rowHash")
            orderByRowHash = true;

        if (newOrderBy.clauses.empty() && implicitOrderByRowHash
            /* && (limit != -1 || offset != 0) */)
            orderByRowHash = true;
 
        if (orderByRowHash) {
            ExcAssert(numBuckets < 0);
            executor.reset(new RowHashOrderedExecutor(std::move(matrix),
                                                      std::move(whereGenerator),
                                                      *context,
                                                      std::move(whereBound),
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
                                               std::move(whereBound),
                                               std::move(whenBound),
                                               std::move(boundSelect),
                                               std::move(boundCalc),
                                               std::move(newOrderBy)));
        } else {
            executor.reset(new UnorderedExecutor(std::move(matrix),
                                                 std::move(whereGenerator),
                                                *context,
                                                 std::move(whereBound),
                                                 std::move(whenBound),
                                                 std::move(boundSelect),
                                                 std::move(boundCalc),
                                                 std::move(newOrderBy),
                                                 numBuckets));
        }

    } JML_CATCH_ALL {
        rethrowHttpException(-1, "Error binding non-grouped query: "
                             + ML::getExceptionString(),
                             "select", select.surface,
                             "from", from.getStatus(),
                             "where", where,
                             "calc", calc,
                             "orderBy", orderBy);
    }
}

void
BoundSelectQuery::
execute(std::function<bool (const NamedRowValue & output,
                            std::vector<ExpressionValue> & calcd)> aggregator,
        ssize_t offset,
        ssize_t limit,
        std::function<bool (const Json::Value &)> onProgress,
        bool allowMT)
{
    auto subAggregator = [&] (const NamedRowValue & row,
                              std::vector<ExpressionValue> & calc,
                              int groupNum)
    {
       return aggregator(row, calc);
    };

    return execute(subAggregator, offset, limit, onProgress, allowMT);

}

void
BoundSelectQuery::
execute(std::function<bool (const NamedRowValue & output,
                            std::vector<ExpressionValue> & calcd,
                            int groupNum)> aggregator,
        ssize_t offset,
        ssize_t limit,
        std::function<bool (const Json::Value &)> onProgress,
        bool allowMT)
{
    ExcAssert(aggregator);

    try {
        executor->execute(aggregator, offset, limit, onProgress, allowMT);
    } JML_CATCH_ALL {
        rethrowHttpException(-1, "Error executing non-grouped query: "
                             + ML::getExceptionString(),
                             "select", select.surface,
                             "from", from.getStatus(),
                             "where", where,
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

struct GroupContext: public SqlExpressionDatasetContext {

    GroupContext(const Dataset& dataset, const Utf8String& alias, 
            const TupleExpression & groupByExpression) : 
        SqlExpressionDatasetContext(dataset, alias), 
        groupByExpression(groupByExpression),
        argCounter(0), argOffset(0),
        evaluateEmptyGroups(false)
    {
    }

    const TupleExpression & groupByExpression;

    struct RowContext: public SqlRowScope {
        RowContext(NamedRowValue & output,
                   const std::vector<ExpressionValue> & currentGroupKey)
            : output(output), currentGroupKey(currentGroupKey)
        {
        }

        NamedRowValue & output;
        const std::vector<ExpressionValue> & currentGroupKey;
    };

    virtual BoundFunction doGetFunction(const Utf8String & tableName,
                                        const Utf8String & functionName,
                                        const std::vector<BoundSqlExpression> & args)
    {
        if (functionName == "rowName") {
            return {[] (const std::vector<ExpressionValue> & args,
                        const SqlRowScope & context)
                    {
                        auto & row = static_cast<const RowContext &>(context);

                        static VectorDescription<ExpressionValue>
                            desc(getExpressionValueDescriptionNoTimestamp());

                        std::string result;
                        result.reserve(116);  /// try to force a 128 byte allocation
                        StringJsonPrintingContext scontext(result);
                        scontext.writeUtf8 = true;
                        desc.printJsonTyped(&row.currentGroupKey, scontext);

                        return ExpressionValue(std::move(Utf8String(std::move(result), false /* check */)),
                                               Date::negativeInfinity());
                    },
                    std::make_shared<StringValueInfo>()};
        }
        else if (functionName == "groupKeyElement" || functionName == "group_key_element") {
            return {[] (const std::vector<ExpressionValue> & args,
                        const SqlRowScope & context)
                    {
                        auto & row = static_cast<const RowContext &>(context);

                        int position = args[0].toInt();

                        return row.currentGroupKey.at(position);
                    },
                    // TODO: get it from the value info for the group keys...
                    std::make_shared<AnyValueInfo>()};
        }

        //check aggregators
        Utf8String functionNameLower(boost::algorithm::to_lower_copy(functionName.extractAscii()));
        auto aggFn = SqlBindingScope::doGetAggregator(functionNameLower, args);
        if (aggFn)
        {
            if (functionNameLower == "count")
            {
                //count is *special*
                evaluateEmptyGroups = true;
            }

            int aggIndex = argCounter;
            OutputAggregator boundagg(aggIndex,
                                    args.size(),
                                    aggFn);
              outputAgg.emplace_back(boundagg);              

               argCounter += args.size();

              return {[&,aggIndex] (const std::vector<ExpressionValue> & args,
                        const SqlRowScope & context)
                    {
                        return outputAgg[aggIndex].aggregate.extract(aggData[aggIndex].get());
                    },
                    // TODO: get it from the value info for the group keys...
                    std::make_shared<AnyValueInfo>()};
        }

        return SqlBindingScope::doGetFunction(tableName, functionName, args);
    }

    // Within a group by context, we can get either:
    // 1.  The value of the variable in the row
    // 2.  The value of the variable within the group by expression
    virtual VariableGetter doGetVariable(const Utf8String & tableName,
                                         const Utf8String & variableName)
    {
         Utf8String simplifiedVariableName = removeQuotes(removeTableName(variableName));

        for (unsigned i = 0;  i < groupByExpression.clauses.size();  ++i) {
            const std::shared_ptr<SqlExpression> & g
                = groupByExpression.clauses[i];

            Utf8String simplifiedSurface = removeQuotes(removeTableName(g->surface));

            if (simplifiedSurface == simplifiedVariableName) {
                return {[=] (const SqlRowScope & context,
                             ExpressionValue & storage,
                             const VariableFilter & filter)
                        -> const ExpressionValue &
                        {
                            auto & row = static_cast<const RowContext &>(context);
                            return storage = row.currentGroupKey.at(i);
                        },
                        // TODO: return real type
                        std::make_shared<AnyValueInfo>()};
            }
        }

        ColumnName columnName(simplifiedVariableName);

        return {[=] (const SqlRowScope & context,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    auto & row = static_cast<const RowContext &>(context);
             
                    const ExpressionValue * result
                        = searchRow(row.output.columns, columnName, filter, storage);

                    if (result)
                        return *result;     
                    
                    throw HttpReturnException(400, "variable " + variableName + " must appear in the GROUP BY clause or be used in an aggregate function");
                },
                std::make_shared<AtomValueInfo>()};
    }

    RowContext getRowContext(NamedRowValue & output,
                             const std::vector<ExpressionValue> & currentGroupKey) const
    {
        return RowContext(output, currentGroupKey);
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

    void aggregateRow(GroupMapValue& mapInstance, const std::vector<ExpressionValue>& row)
    {
       for (unsigned i = 0;  i < outputAgg.size();  ++i) {

                outputAgg[i].aggregate.process(&row[argOffset + outputAgg[i].inputIndex],
                                                       outputAgg[i].numInputs,
                                                       mapInstance[i].get());
            }
    }

    void mergeThreadMap(GroupMapValue& outMapInstance, const GroupMapValue& inMapInstance)
    {
        for (unsigned i = 0;  i < outputAgg.size();  ++i) {
           outputAgg[i].aggregate.mergeInto(outMapInstance[i].get(), inMapInstance[i].get());
       }
    }
             

    std::vector<OutputAggregator> outputAgg;    
    std::vector<std::shared_ptr<void> > aggData;  //working buffers for the above
    int argCounter;
    int argOffset;
    bool evaluateEmptyGroups;
};

/*****************************************************************************/
/* BOUND GROUP BY QUERY                                               */
/*****************************************************************************/

BoundGroupByQuery::
BoundGroupByQuery(const SelectExpression & select,
                  const Dataset & from,
                  const Utf8String& alias,
                  const WhenExpression & when,
                  std::shared_ptr<SqlExpression> where,
                  const TupleExpression & groupBy,
                  const std::vector< std::shared_ptr<SqlExpression> >& aggregatorsExpr,
                  const SqlExpression & having,
                  const SqlExpression & rowName,
                  const OrderByExpression & orderBy)
    : from(from),
      when(when),
      where(where),
      rowContext(new SqlExpressionDatasetContext(from, alias)),
      groupContext(new GroupContext(from, alias, groupBy)),
      groupBy(groupBy),
      select(select),
      numBuckets(1)
{
    for (auto & g: groupBy.clauses) {
        calc.push_back(g);
    }

    groupContext->argOffset = calc.size();

    // Convert the select clauses to a list
    for (auto & expr : aggregatorsExpr)
    {
        auto fn = dynamic_cast<const FunctionCallWrapper *>(expr.get());

        //Important: This assumes they are in the same order as in the group context
        for (auto & a: fn->args) {
           calc.emplace_back(a);
        } 
    }

    // Bind in the order by expression
    boundOrderBy = orderBy.bindAll(*groupContext);

    // Bind the row name expression
    boundRowName = rowName.bind(*groupContext);

    size_t maxNumRow = from.getMatrixView()->getRowCount();
    numBuckets = maxNumRow <= 32 ? 1 : (maxNumRow / 32);

    // And bind the subselect
    //false means no implicit sort by rowhash, we want unsorted
    subSelect.reset(new BoundSelectQuery(subSelectExpr, from, alias, when, where, subOrderBy, calc, false, numBuckets));

}

void
BoundGroupByQuery::
execute(std::function<bool (const NamedRowValue & output)> aggregator,
             ssize_t offset,
             ssize_t limit,
             std::function<bool (const Json::Value &)> onProgress,
             bool allowMT)
{
    typedef std::tuple<std::vector<ExpressionValue>,
                       NamedRowValue,
                       std::vector<ExpressionValue> >
        SortedRow;

    std::vector<SortedRow> rowsSorted;
    std::atomic<ssize_t> groupsDone(0);

    typedef std::vector<ExpressionValue> RowKey;
    typedef std::map<RowKey, GroupMapValue> GroupByMapType;
    std::vector<GroupByMapType> accum(numBuckets);

    //bind the selectexpression, this will create the bound aggregators (which we wont use, ah!)
    auto boundSelect = select.bind(*groupContext);

    // When we get a row, we record it under the group key
    auto onRow = [&] (const NamedRowValue & row,
                      const std::vector<ExpressionValue> & calc,
                      int groupNum)
    {
       GroupByMapType & map = accum[groupNum];
       RowKey rowKey(calc.begin(), calc.begin() + groupBy.clauses.size());

       auto pair = map.insert({rowKey, GroupMapValue()});
       auto iter = pair.first;
       if (pair.second)
       {
          //initialize aggregator data
          groupContext->initializePerThreadAggregators(iter->second);
       }

       groupContext->aggregateRow(iter->second, calc);

       return true;
    };  
            
    subSelect->execute(onRow, 0, -1, onProgress, allowMT);
  
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

    if (destMap.empty() && groupContext->evaluateEmptyGroups)
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

        auto rowContext = groupContext->getRowContext(outputRow, rowKey);

        outputRow.rowName = RowName(boundRowName(rowContext).toUtf8String());
        outputRow.rowHash = outputRow.rowName;        

        //Evaluating the whole bound select expression
        ExpressionValue result = boundSelect(rowContext);
        result.mergeToRowDestructive(outputRow.columns);

        //In case of no output ordering, we can early exit
        if (boundOrderBy.empty()) {
            ssize_t n = groupsDone.fetch_add(1);
            if (limit != -1 && n >= limit)
               break;

            aggregator(outputRow);
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
            return aggregator(row);
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
