/** analytics.cc
    Jeremy Barnes, 30 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "mldb/server/analytics.h"
#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/base/parallel.h"
#include "mldb/arch/timers.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/server/dataset_context.h"
#include "mldb/sql/execution_pipeline.h"
#include <boost/algorithm/string.hpp>
#include "mldb/server/bound_queries.h"
#include "mldb/server/parallel_merge_sort.h"
#include "mldb/jml/stats/distribution.h"
#include <mutex>
#include <functional>

using namespace std;
using namespace std::placeholders;


namespace MLDB {


/*****************************************************************************/
/* ITERATE DATASET                                                           */
/*****************************************************************************/

/** Equivalent to SELECT (select) FROM (dataset) WHEN (when) WHERE (where), and each matching
    row is passed to the aggregator.
*/
std::pair<bool, std::shared_ptr<ExpressionValueInfo> >
iterateDataset(const SelectExpression & select,
                    const Dataset & from,
                    const Utf8String & alias,
                    const WhenExpression & when,
                    const SqlExpression & where,
                    std::vector<std::shared_ptr<SqlExpression> > calc,
                    RowProcessorEx processor,
                    const OrderByExpression & orderBy,
                    ssize_t offset,
                    ssize_t limit,
                    const ProgressFunc & onProgress)
{
    BoundSelectQuery query(select, from, alias, when, where, orderBy, calc);

    bool success = query.execute(processor, offset, limit, onProgress);

    return {success, query.selectInfo};

}

std::pair<bool, std::shared_ptr<ExpressionValueInfo> >
iterateDatasetExpr(const SelectExpression & select,
                        const Dataset & from,
                        const Utf8String & alias,
                        const WhenExpression & when,
                        const SqlExpression & where,
                        std::vector<std::shared_ptr<SqlExpression> > calc,
                        RowProcessorExpr processor,
                        const OrderByExpression & orderBy,
                        ssize_t offset,
                        ssize_t limit,
                        const ProgressFunc & onProgress)
{
    BoundSelectQuery query(select, from, alias, when, where, orderBy, calc);

    bool success = query.executeExpr(processor, offset, limit, onProgress);

    return {success, query.selectInfo};
}

/** Full select function, with grouping. */
std::pair<bool, std::shared_ptr<ExpressionValueInfo> >
iterateDatasetGrouped(const SelectExpression & select,
                           const Dataset & from,
                           const Utf8String & alias,
                           const WhenExpression & when,
                           const SqlExpression & where,
                           const TupleExpression & groupBy,
                           const std::vector< std::shared_ptr<SqlExpression> >& aggregators,
                           const SqlExpression & having,
                           const SqlExpression & rowName,
                           RowProcessor processor,
                           const OrderByExpression & orderBy,
                           ssize_t offset,
                           ssize_t limit,
                           const ProgressFunc & onProgress)
{
    BoundGroupByQuery query(select, from, alias, when, where, groupBy,
                             aggregators, having, rowName, orderBy);

    return query.execute(processor, offset, limit, onProgress);

}

std::pair<bool, std::shared_ptr<ExpressionValueInfo> >
iterateDataset(const SelectExpression & select,
                    const Dataset & from,
                    const Utf8String & alias,
                    const WhenExpression & when,
                    const SqlExpression & where,
                    RowProcessor processor,
                    const OrderByExpression & orderBy,
                    ssize_t offset,
                    ssize_t limit,
                    const ProgressFunc & onProgress)
{
    std::function<bool (NamedRowValue & output,
                        const std::vector<ExpressionValue> & calcd)>
    processor2 = [&] (NamedRowValue & output,
                       const std::vector<ExpressionValue> & calcd)
        {
            return processor(output);
        };

    return iterateDataset(select, from, std::move(alias), when, where, {}, {processor2, processor.processInParallel}, orderBy, offset, limit, onProgress);
}

/** Iterates over the dataset, extracting a dense feature vector from each row. */
void iterateDense(const SelectExpression & select,
                  const Dataset & from,
                  const Utf8String& alias,
                  const WhenExpression & when,
                  const SqlExpression & where,
                  std::vector<std::shared_ptr<SqlExpression> > calc,
                  std::function<bool (const RowHash & rowHash,
                                      const RowPath & rowName,
                                      int64_t rowNumber,
                                      const std::vector<double> & features,
                                      const std::vector<ExpressionValue> & extra)> processor,
                  const ProgressFunc & onProgress)
{
    ExcAssert(processor);

    SqlExpressionDatasetScope context(from, alias);
    SqlExpressionWhenScope whenContext(context);
    auto whenBound = when.bind(whenContext);
    // Bind our where statement
    auto whereBound = where.bind(context);

    auto matrix = from.getMatrixView();

    auto boundSelect = select.bind(context);

    int numOutputVariables = boundSelect.info->getKnownColumns().size();

    //int numOutputVariables = context.variables.size();
    //cerr << "total of " << numOutputVariables << " variables set"
    //     << endl;

    if (numOutputVariables == 0)
        throw HttpReturnException(400, "Select expression '"
                                  + (select.surface.empty() ? select.surface : select.print())
                                  + "' matched no columns",
                                  "rowInfo",
                                  static_pointer_cast<ExpressionValueInfo>(from.getRowInfo()));
    
    std::vector<BoundSqlExpression> boundCalc;
    for (auto & c: calc) {
        boundCalc.emplace_back(c->bind(context));
    }
    
    auto columns = boundSelect.info->allColumnNames();

    // This function lets us efficiently extract the embedding from each row
    auto extractEmbedding = boundSelect.info->extractDoubleEmbedding(columns);

    // Get a list of rows that we run over
    // getRowPaths can return row names in an arbitrary order as long as it is deterministic.
    auto rows = matrix->getRowPaths();

    auto doRow = [&] (int rowNum) -> bool
        {
            //if (rowNum % 1000 == 0)
            //    cerr << "applying row " << rowNum << " of " << rows.size() << endl;

            const RowPath & rowName = rows[rowNum];

            auto row = from.getRowExpr(rowName);

            // Check it matches the where expression.  If not, we don't process
            // it.
            auto rowContext = context.getRowScope(rowName, row);

            if (!whereBound(rowContext, GET_LATEST).isTrue())
                return true;

            // Filter the tuples using the WHEN expression
            whenBound.filterInPlace(row, rowContext);

            // Run the with expressions
            ExpressionValue out = boundSelect(rowContext, GET_ALL);

            auto embedding = extractEmbedding(out);

            vector<ExpressionValue> calcd(boundCalc.size());
            for (unsigned i = 0;  i < boundCalc.size();  ++i) {
                calcd[i] = boundCalc[i](rowContext, GET_LATEST);
            }
            
            /* Finally, pass to the aggregator to continue. */
            return processor(rowName, rowName, rowNum, embedding, calcd);
        };

    parallelMap(0, rows.size(), doRow);
}

std::pair<std::vector<std::tuple<RowHash, RowPath, std::vector<double>, std::vector<ExpressionValue> > >,
          std::vector<KnownColumn> >
getEmbedding(const SelectExpression & select,
             const Dataset & dataset,
             const Utf8String& alias,
             const WhenExpression & when,
             const SqlExpression & where,
             std::vector<std::shared_ptr<SqlExpression> > calc,
             int maxDimensions,
             const OrderByExpression & orderBy,
             int offset,
             int limit,
             const ProgressFunc & onProgress)
{
    SqlExpressionDatasetScope context(dataset, alias);

    BoundSqlExpression boundSelect = select.bind(context);

    std::vector<KnownColumn> vars = boundSelect.info->getKnownColumns();

    if (maxDimensions == -1)
        maxDimensions = vars.size();

    if (vars.size() > maxDimensions)
        vars.resize(maxDimensions);

    ExcAssertGreaterEqual(maxDimensions, 0);

    std::vector<ColumnPath> varNames;
    varNames.reserve(vars.size());
    for (auto & v: vars)
        varNames.emplace_back(v.columnName);

    auto getEmbeddingDouble = boundSelect.info->extractDoubleEmbedding(varNames);

    // Now we know what values came out of it

    std::mutex rowsLock;
    std::vector<std::tuple<RowHash, RowPath, std::vector<double>,
                           std::vector<ExpressionValue> > > rows;

    auto processor = [&] (const RowHash & rowHash,
                          const RowPath & rowName,
                          int64_t rowIndex,
                          const std::vector<double> & features,
                          const std::vector<ExpressionValue> & extraVals)
        {
            std::unique_lock<std::mutex> guard(rowsLock);
                
            if (features.size() <= maxDimensions) {
                rows.emplace_back(rowHash, rowName, features, extraVals);
            }
            else {
                ExcAssertLessEqual(maxDimensions, features.size());
                rows.emplace_back(rowHash, rowName,
                                  vector<double>(features.begin(),
                                                 features.begin() + maxDimensions),
                                  extraVals);
            }

            return true;
        };

    auto sparseProcessor = [&] (NamedRowValue & output,
                                const std::vector<ExpressionValue> & calcd)
        {
            auto features = getEmbeddingDouble(std::move(output.columns));
            return processor(output.rowHash, output.rowName, -1 /* rowIndex */,
                             features, calcd);
        };
    
    if (limit != -1 || offset != 0) { //TODO - MLDB-1127 - orderBy condition here
        
        //getEmbedding is expected to have a consistent row order
        iterateDataset(select, dataset, std::move(alias), when, where, calc,
                       {sparseProcessor, false /*processInParallel*/},
                       orderBy, offset, limit, onProgress);
    }
    else { 
        // this has opportunity for more optimization since there are no
        // offset or limit
        
        iterateDense(select, dataset, alias, when, where, calc,
                     processor, nullptr);
        
        // because the results come in parallel
        // we still need to order by rowHash to ensure determinism in the results
        parallelQuickSortRecursive(rows);
    }

    return { std::move(rows), std::move(vars) };
}

std::pair<std::vector<std::tuple<RowHash, RowPath, std::vector<double>,
                                 std::vector<ExpressionValue> > >,
          std::vector<KnownColumn> >
getEmbedding(const SelectStatement & stm,
             SqlExpressionMldbScope & context,
             int maxDimensions,
             const ProgressFunc & onProgress)
{
    auto boundDataset = stm.from->bind(context, onProgress);
    if (!boundDataset.dataset)
        throw HttpReturnException
            (400, "You can't train this algorithm from a sub-select or "
             "table expression; it must be FROM <dataset name>");
    return getEmbedding(stm.select, 
                        *boundDataset.dataset, 
                        boundDataset.asName, 
                        stm.when, *stm.where, {},
                        maxDimensions, stm.orderBy, 
                        stm.offset, stm.limit, onProgress);
}

void
validateQueryWithoutDataset(const SelectStatement& stm, SqlBindingScope& scope)
{
    stm.when.bind(scope);
    stm.orderBy.bindAll(scope);
    if (!stm.groupBy.clauses.empty()) {
        throw HttpReturnException(
            400, "GROUP BY usage requires a FROM statement");
    }
    if (!stm.having->isConstantTrue()) {
        throw HttpReturnException(
            400, "HAVING usage requires a FROM statement");
    }
}

std::vector<MatrixNamedRow>
queryWithoutDataset(const SelectStatement& stm, SqlBindingScope& scope)
{
    std::vector<MatrixNamedRow> output;
    auto rows = queryWithoutDatasetExpr(stm, scope);
    for (auto& r : std::get<0>(rows)) {
        output.push_back(r.flattenDestructive());
    }
    return output;
}

std::tuple<std::vector<NamedRowValue>, std::shared_ptr<ExpressionValueInfo> >
queryWithoutDatasetExpr(const SelectStatement& stm, SqlBindingScope& scope)
{
    for (const auto & c: stm.select.clauses) {
        if (c->isWildcard()) {
            throw HttpReturnException(
                400, "Wildcard usage requires a FROM statement");
        }
    }
    auto boundSelect = stm.select.bind(scope);
    auto rowInfo = boundSelect.info;

    validateQueryWithoutDataset(stm, scope);

    auto boundWhere = stm.where->bind(scope);
    ExcAssert(boundWhere.info->isConst()); //If not const binding above should have failed
    bool whereValue = boundWhere.constantValue().isTrue();

    if (stm.offset < 1 && stm.limit != 0 && whereValue) {
        // Fast path when there is no possibility of result since
        // queryWithoutDataset produces at most single row results.

        NamedRowValue row;
        SqlRowScope context;
        ExpressionValue val = boundSelect(context, GET_ALL);
        auto boundRowName = stm.rowName->bind(scope);

        row.rowName = getValidatedRowName(boundRowName(context, GET_ALL));
        row.rowHash = row.rowName;
        val.mergeToRowDestructive(row.columns);
        std::vector<NamedRowValue> outputcolumns = {std::move(row)};
        return make_tuple<std::vector<NamedRowValue>, std::shared_ptr<ExpressionValueInfo> >
                    (std::move(outputcolumns), std::move(rowInfo));
    }

    return make_tuple<std::vector<NamedRowValue>, std::shared_ptr<ExpressionValueInfo> >
            (vector<NamedRowValue>(), make_shared<EmptyValueInfo>());
}

std::vector<MatrixNamedRow>
queryFromStatement(const SelectStatement & stm,
                   SqlBindingScope & scope,
                   BoundParameters params,
                   const ProgressFunc & onProgress)
{
    std::vector<MatrixNamedRow> output;
    auto rows = queryFromStatementExpr(stm, scope, params, onProgress);
    for (auto& r : std::get<0>(rows)) {
        output.push_back(r.flattenDestructive());
    }
    return output;
}

std::tuple<std::vector<NamedRowValue>, std::shared_ptr<ExpressionValueInfo> >
queryFromStatementExpr(const SelectStatement & stm,
                       SqlBindingScope & scope,
                       BoundParameters params,
                       const ProgressFunc & onProgress)
{
    /* The assumption is that both sides have the same number
       of items to process.  This is obviously not always the case
       so the progress may differ in speed when switching from the 
       left dataset to right dataset.
    */ 
    ProgressState joinState(100);
    auto joinedProgress = [&](uint side, const ProgressState & state) {
        joinState = (50 * state.count / *state.total) + (50 * side);
        //cerr << "joinState.count " << joinState.count << " joinState.total " << *joinState.total << endl;
        return onProgress(joinState);
    };

    auto & bindProgress = onProgress ? bind(joinedProgress, 0, _1) : onProgress;
    BoundTableExpression table = stm.from->bind(scope, bindProgress);
    
    auto & iterateProgress = onProgress ? bind(joinedProgress, 1, _1) : onProgress;
    if (table.dataset) {
        return table.dataset->queryStructuredExpr(stm.select, stm.when,
                                                  *stm.where,
                                                  stm.orderBy, stm.groupBy,
                                                  stm.having,
                                                  stm.rowName,
                                                  stm.offset, stm.limit, 
                                                  table.asName,
                                                  iterateProgress);
    }
    else if (table.table.runQuery && stm.from) {

        auto getParamInfo = [&] (const Utf8String & paramName)
            -> std::shared_ptr<ExpressionValueInfo>
            {
                throw HttpReturnException(500, "No query parameter " + paramName);
            };
        
        if (!params)
            params = [] (const Utf8String & param) -> ExpressionValue { 
                throw HttpReturnException(500, "No query parameter " + param); 
            };

        std::shared_ptr<PipelineElement> pipeline = PipelineElement::root(scope)->statement(stm, getParamInfo);

        auto boundPipeline = pipeline->bind();

        auto executor = boundPipeline->start(params);
        
        std::vector<NamedRowValue> rows;

        ssize_t limit = stm.limit;
        ssize_t offset = stm.offset;

        auto output = executor->take();

        for (size_t n = 0;
             output && (limit == -1 || n < limit + offset);
             output = executor->take(), ++n) {

            // MLDB-1329 band-aid fix.  This appears to break a circlar
            // reference chain that stops the elements from being
            // released.
            output->group.clear();

            if (n < offset) {
                continue;
            }

            NamedRowValue row;
            // Second last element is the row name
            row.rowName = output->values.at(output->values.size() - 2)
                .coerceToPath(); 
            row.rowHash = row.rowName;
            output->values.back().mergeToRowDestructive(row.columns);
            rows.emplace_back(std::move(row));
        }
            
        return std::make_tuple<std::vector<NamedRowValue>, 
                              std::shared_ptr<ExpressionValueInfo> >(std::move(rows), std::make_shared<UnknownRowValueInfo>());
    }
    else {
        // No from at all
        return queryWithoutDatasetExpr(stm, scope);
    }
}

/** Select from the given statement.  This will choose the most
    appropriate execution method based upon what is in the query.

    The scope should be a clean scope, not requiring any row scope.
    See the comment above if you have errors inside this function.

    Will return the results one by one, and will stop when the
    onRow function returns false.  Returns false if one of the
    onRow calls returned false, or true otherwise.
*/
bool
queryFromStatement(std::function<bool (Path &, ExpressionValue &)> & onRow,
                   const SelectStatement & stm,
                   SqlBindingScope & scope,
                   BoundParameters params,
                   const ProgressFunc & onProgress)
{
    BoundTableExpression table = stm.from->bind(scope, onProgress);
    
    if (table.dataset) {
        return table.dataset->queryStructuredIncremental
            (onRow, stm.select, stm.when,
             *stm.where,
             stm.orderBy, stm.groupBy,
             stm.having,
             stm.rowName,
             stm.offset, stm.limit, 
             table.asName);
    }
    else if (table.table.runQuery && stm.from) {

        auto getParamInfo = [&] (const Utf8String & paramName)
            -> std::shared_ptr<ExpressionValueInfo>
            {
                throw HttpReturnException(500, "No query parameter " + paramName);
            };
        
        if (!params)
            params = [] (const Utf8String & param) -> ExpressionValue
                {
                    throw HttpReturnException(500, "No query parameter " + param);
                };
        
        std::shared_ptr<PipelineElement> pipeline
            = PipelineElement::root(scope)->statement(stm, getParamInfo);

        auto boundPipeline = pipeline->bind();

        auto executor = boundPipeline->start(params);
        
        std::vector<MatrixNamedRow> rows;

        ssize_t limit = stm.limit;
        ssize_t offset = stm.offset;

        auto output = executor->take();

        for (size_t n = 0;
             output && (limit == -1 || n < limit + offset);
             output = executor->take(), ++n) {

            // MLDB-1329 band-aid fix.  This appears to break a circlar
            // reference chain that stops the elements from being
            // released.
            output->group.clear();

            if (n < offset) {
                continue;
            }

            Path path = output->values.at(output->values.size() - 2)
                .coerceToPath(); 
            ExpressionValue val(std::move(output->values.back()));
            if (!onRow(path, val))
                return false;
        }
            
        return true;
    }
    else {
        // No from at all
        auto rows = queryWithoutDataset(stm, scope);
        for (auto & r: rows) {
            ExpressionValue val(std::move(r.columns));
            if (!onRow(r.rowName, val))
                return false;
        }
        return true;
    }
}

RowPath getValidatedRowName(const ExpressionValue& rowNameEV)
{
    RowPath name;
    try {
        name = rowNameEV.coerceToPath();
    }
    MLDB_CATCH_ALL {
        rethrowHttpException
            (400, "Unable to create a row name from the passed expression. "
             "Row names must be either a simple atom, or a Path atom, or an "
             "array of atoms.",
             "value", rowNameEV);
    }

    if (name.empty()) {
        throw HttpReturnException(400, "Can't create a row with a null name.");
    }
    return name;
}

} // namespace MLDB

