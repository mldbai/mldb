/** analytics.cc
    Jeremy Barnes, 30 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
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


using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* ITERATE DATASET                                                           */
/*****************************************************************************/

/** Equivalent to SELECT (select) FROM (dataset) WHEN (when) WHERE (where), and each matching
    row is passed to the aggregator.
*/
bool iterateDataset(const SelectExpression & select,
                    const Dataset & from,
                    const Utf8String & alias,
                    const WhenExpression & when,
                    const SqlExpression & where,
                    std::vector<std::shared_ptr<SqlExpression> > calc,
                    RowProcessorEx processor,
                    const OrderByExpression & orderBy,
                    ssize_t offset,
                    ssize_t limit,
                    std::function<bool (const Json::Value &)> onProgress)
{
    return BoundSelectQuery(select, from, alias, when, where, orderBy, calc)
        .execute(processor, offset, limit, onProgress);
}


/** Full select function, with grouping. */
bool iterateDatasetGrouped(const SelectExpression & select,
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
                           std::function<bool (const Json::Value &)> onProgress)
{
    return BoundGroupByQuery(select, from, alias, when, where, groupBy,
                             aggregators, having, rowName, orderBy)
      .execute(processor, offset, limit, onProgress);
}

bool iterateDataset(const SelectExpression & select,
                    const Dataset & from,
                    const Utf8String & alias,
                    const WhenExpression & when,
                    const SqlExpression & where,
                    RowProcessor processor,
                    const OrderByExpression & orderBy,
                    ssize_t offset,
                    ssize_t limit,
                    std::function<bool (const Json::Value &)> onProgress)
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
                                      const RowName & rowName,
                                      int64_t rowNumber,
                                      const std::vector<double> & features,
                                      const std::vector<ExpressionValue> & extra)> processor,
                  std::function<bool (const Json::Value &)> onProgress)
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
    // getRowNames can return row names in an arbitrary order as long as it is deterministic.
    auto rows = matrix->getRowNames();

    auto doRow = [&] (int rowNum) -> bool
        {
            //if (rowNum % 1000 == 0)
            //    cerr << "applying row " << rowNum << " of " << rows.size() << endl;

            const RowName & rowName = rows[rowNum];

            auto row = matrix->getRow(rowName);

            // Check it matches the where expression.  If not, we don't process
            // it.
            auto rowContext = context.getRowScope(row);

            if (!whereBound(rowContext, GET_LATEST).isTrue())
                return true;

            // Filter the tuples using the WHEN expression
            whenBound.filterInPlace(row, rowContext);

            // Run the with expressions
            ExpressionValue out = boundSelect(rowContext, GET_ALL);

            auto embedding = extractEmbedding(out);

            vector<ExpressionValue> calcd(boundCalc.size());
            for (unsigned i = 0;  i < boundCalc.size();  ++i) {
                calcd[i] = std::move(boundCalc[i](rowContext, GET_LATEST));
            }
            
            /* Finally, pass to the aggregator to continue. */
            return processor(row.rowHash, rowName, rowNum, embedding, calcd);
        };

    parallelMap(0, rows.size(), doRow);
}

std::pair<std::vector<std::tuple<RowHash, RowName, std::vector<double>, std::vector<ExpressionValue> > >,
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
             const std::function<bool (const Json::Value &)> & onProgress)
{
    SqlExpressionDatasetScope context(dataset, alias);

    BoundSqlExpression boundSelect = select.bind(context);

    std::vector<KnownColumn> vars = boundSelect.info->getKnownColumns();

    if (maxDimensions == -1)
        maxDimensions = vars.size();

    if (vars.size() > maxDimensions)
        vars.resize(maxDimensions);

    ExcAssertGreaterEqual(maxDimensions, 0);

    std::vector<ColumnName> varNames;
    varNames.reserve(vars.size());
    for (auto & v: vars)
        varNames.emplace_back(v.columnName);

    auto getEmbeddingDouble = boundSelect.info->extractDoubleEmbedding(varNames);

    // Now we know what values came out of it

    std::mutex rowsLock;
    std::vector<std::tuple<RowHash, RowName, std::vector<double>,
                           std::vector<ExpressionValue> > > rows;

    auto processor = [&] (const RowHash & rowHash,
                          const RowName & rowName,
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

std::pair<std::vector<std::tuple<RowHash, RowName, std::vector<double>,
                                 std::vector<ExpressionValue> > >,
          std::vector<KnownColumn> >
getEmbedding(const SelectStatement & stm,
             SqlExpressionMldbScope & context,
             int maxDimensions,
             const std::function<bool (const Json::Value &)> & onProgress)
{
    auto boundDataset = stm.from->bind(context);
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

std::vector<MatrixNamedRow>
queryWithoutDataset(const SelectStatement& stm, SqlBindingScope& scope)
{
    auto boundSelect = stm.select.bind(scope);
    SqlRowScope context;
    ExpressionValue val = boundSelect(context, GET_ALL);
    MatrixNamedRow row;
    auto boundRowName = stm.rowName->bind(scope);

    row.rowName = getValidatedRowName(boundRowName(context, GET_ALL));
    row.rowHash = row.rowName;
    val.mergeToRowDestructive(row.columns);

    return { std::move(row) };
}

std::vector<MatrixNamedRow>
queryFromStatement(const SelectStatement & stm,
                   SqlBindingScope & scope,
                   BoundParameters params)
{
    BoundTableExpression table = stm.from->bind(scope);
    
    if (table.dataset) {
        return table.dataset->queryStructured(stm.select, stm.when,
                                              *stm.where,
                                              stm.orderBy, stm.groupBy,
                                              *stm.having,
                                              *stm.rowName,
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
            params = [] (const Utf8String & param) -> ExpressionValue { throw HttpReturnException(500, "No query parameter " + param); };

        std::shared_ptr<PipelineElement> pipeline = PipelineElement::root(scope)->statement(stm, getParamInfo);

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

            MatrixNamedRow row;
            // Second last element is the row name
            row.rowName = output->values.at(output->values.size() - 2)
                .coerceToPath(); 
            row.rowHash = row.rowName;
            output->values.back().mergeToRowDestructive(row.columns);
            rows.emplace_back(std::move(row));
        }
            
        return rows;
    }
    else {
        // No from at all
        return queryWithoutDataset(stm, scope);
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
                   BoundParameters params)
{
    BoundTableExpression table = stm.from->bind(scope);
    
    if (table.dataset) {
        return table.dataset->queryStructuredIncremental
            (onRow, stm.select, stm.when,
             *stm.where,
             stm.orderBy, stm.groupBy,
             *stm.having,
             *stm.rowName,
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
            params = [] (const Utf8String & param) -> ExpressionValue { throw HttpReturnException(500, "No query parameter " + param); };

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

RowName getValidatedRowName(const ExpressionValue& rowNameEV)
{
    if (rowNameEV.empty()) {
        throw HttpReturnException(400, "Can't create a row with a null or empty name.");
    }

    RowName name;
    try {
        name = rowNameEV.coerceToPath();
    }
    JML_CATCH_ALL {
        rethrowHttpException
            (400, "Unable to create a row name from the passed expression. "
             "Row names must be either a simple atom, or a Path atom, or an "
             "array of atoms.",
             "value", rowNameEV);
    }

    static const Path empty{""};

    if (name.empty() || name.compare(empty) == 0)
        throw HttpReturnException(400, "Can't create a row with a null or empty name.");
    return name;
}

} // namespace MLDB
} // namespace Datacratic
