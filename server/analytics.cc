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
#include "mldb/server/function_contexts.h"
#include "mldb/sql/execution_pipeline.h"
#include <boost/algorithm/string.hpp>
#include "mldb/server/bound_queries.h"
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
void iterateDataset(const SelectExpression & select,
                    const Dataset & from,
                    const Utf8String & alias,
                    const WhenExpression & when,
                    const SqlExpression & where,
                    std::vector<std::shared_ptr<SqlExpression> > calc,
                    std::function<bool (NamedRowValue & output,
                                        const std::vector<ExpressionValue> & calcd)> aggregator,
                    const OrderByExpression & orderBy,
                    ssize_t offset,
                    ssize_t limit,
                    std::function<bool (const Json::Value &)> onProgress)
{
    BoundSelectQuery(select, from, alias, when, where, orderBy, calc)
        .execute(aggregator, offset, limit, onProgress);
}


/** Full select function, with grouping. */
void iterateDatasetGrouped(const SelectExpression & select,
                           const Dataset & from,
                           const Utf8String & alias,
                           const WhenExpression & when,
                           const SqlExpression & where,
                           const TupleExpression & groupBy,
                           const std::vector< std::shared_ptr<SqlExpression> >& aggregators,
                           const SqlExpression & having,
                           const SqlExpression & rowName,
                           std::function<bool (NamedRowValue & output)> aggregator,
                           const OrderByExpression & orderBy,
                           ssize_t offset,
                           ssize_t limit,
                           std::function<bool (const Json::Value &)> onProgress,
                           bool allowMT)
{
    BoundGroupByQuery(select, from, alias, when, where, groupBy, aggregators, having, rowName, orderBy)
      .execute(aggregator, offset, limit, onProgress, allowMT);
}

void iterateDataset(const SelectExpression & select,
                    const Dataset & from,
                    const Utf8String & alias,
                    const WhenExpression & when,
                    const SqlExpression & where,
                    std::function<bool (NamedRowValue & output)> aggregator,
                    const OrderByExpression & orderBy,
                    ssize_t offset,
                    ssize_t limit,
                    std::function<bool (const Json::Value &)> onProgress)
{
    std::function<bool (NamedRowValue & output,
                        const std::vector<ExpressionValue> & calcd)>
    aggregator2 = [&] (NamedRowValue & output,
                       const std::vector<ExpressionValue> & calcd)
        {
            return aggregator(output);
        };
    
    iterateDataset(select, from, std::move(alias), when, where, {}, aggregator2, orderBy, offset, limit, onProgress);
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
                                      const std::vector<ExpressionValue> & extra)> aggregator,
                  std::function<bool (const Json::Value &)> onProgress)
{
    ExcAssert(aggregator);

    SqlExpressionDatasetContext context(from, alias);
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
                                + "' matched no columns");
    
    std::vector<BoundSqlExpression> boundCalc;
    for (auto & c: calc) {
        boundCalc.emplace_back(c->bind(context));
    }
    
    // Get a list of rows that we run over
    auto rows = matrix->getRowNames();

    auto doRow = [&] (int rowNum) -> bool
        {
            //if (rowNum % 1000 == 0)
            //    cerr << "applying row " << rowNum << " of " << rows.size() << endl;

            RowName rowName = rows[rowNum];

            auto row = matrix->getRow(rowName);

            // Check it matches the where expression.  If not, we don't process
            // it.
            auto rowContext = context.getRowContext(row);

            if (!whereBound(rowContext, GET_LATEST).isTrue())
                return true;

            // Filter the tuples using the WHEN expression
            whenBound.filterInPlace(row, rowContext);

            // Run the with expressions
            ExpressionValue out = boundSelect(rowContext, GET_ALL);
            auto embedding = out.getEmbeddingDouble(-1);

            vector<ExpressionValue> calcd(boundCalc.size());
            for (unsigned i = 0;  i < boundCalc.size();  ++i) {
                calcd[i] = std::move(boundCalc[i](rowContext, GET_LATEST));
            }
            
            /* Finally, pass to the aggregator to continue. */
            return aggregator(row.rowHash, rowName, rowNum, embedding, calcd);
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
    SqlExpressionDatasetContext context(dataset, alias);

    BoundSqlExpression boundSelect = select.bind(context);

    std::vector<KnownColumn> vars = boundSelect.info->getKnownColumns();

    if (maxDimensions == -1)
        maxDimensions = vars.size();

    if (vars.size() > maxDimensions)
        vars.resize(maxDimensions);

    ExcAssertGreaterEqual(maxDimensions, 0);

 
    // Now we know what values came out of it

    std::mutex rowsLock;
    std::vector<std::tuple<RowHash, RowName, std::vector<double>, std::vector<ExpressionValue> > > rows;

    if (limit != -1 || offset != 0) { //TODO - MLDB-1127 - orderBy condition here

        auto getEmbeddingDouble = [] (const StructValue & columns)
            {
                //cerr << "getEmbedding for " << jsonEncode(*this) << endl;

                // TODO: this is inefficient.  We should be able to have the
                // info function return us one that does it much more
                // efficiently.

                std::vector<std::pair<ColumnName, double> > features;
             
                auto onColumnValue = [&] (const std::tuple<Coord, ExpressionValue> & column)
                {
                    features.emplace_back(get<0>(column), get<1>(column).toDouble());
                    return true;
                };
                
                std::for_each(columns.begin(), columns.end(), onColumnValue);
                
                std::sort(features.begin(), features.end());

                ML::distribution<double> result;
                result.reserve(features.size());
                for (unsigned i = 0;  i < features.size();  ++i) {
                    result.push_back(features[i].second);
                }
    
                return result;
            };

        std::function<bool (NamedRowValue & output,
                            const std::vector<ExpressionValue> & calcd)>
            aggregator = [&] (NamedRowValue & output,
                              const std::vector<ExpressionValue> & calcd)
            {
                std::unique_lock<std::mutex> guard(rowsLock);
               
                auto features = getEmbeddingDouble(output.columns);
               
                if (features.size() <= maxDimensions) {
                    rows.emplace_back(output.rowHash, output.rowName, features, calcd);
                }
                else {
                    ExcAssertLessEqual(maxDimensions, features.size());
                    rows.emplace_back(output.rowHash, output.rowName,
                                      vector<double>(features.begin(), features.begin() + maxDimensions),
                                      calcd);
                }
               
                return true;
            };
       
        iterateDataset(select, dataset, std::move(alias), when, where, calc, aggregator, orderBy, offset, limit, onProgress);
    }
    else { // this has opportunity for more optimization since there are no offset or limit
        auto aggregator = [&] (const RowHash & rowHash,
                               const RowName & rowName,
                               int64_t rowIndex,
                               const std::vector<double> & features,
                               const std::vector<ExpressionValue> & extraVals)
            {
                std::unique_lock<std::mutex> guard(rowsLock);
                
                //cerr << "embedding got row " << rowName << " with index "
                //     << rowIndex << endl;
                
                if (features.size() <= maxDimensions) {
                    rows.emplace_back(rowHash, rowName, features, extraVals);
                }
                else {
                    ExcAssertLessEqual(maxDimensions, features.size());
                    rows.emplace_back(rowHash, rowName,
                                      vector<double>(features.begin(), features.begin() + maxDimensions),
                                      extraVals);
                }

                return true;
            };

        iterateDense(select, dataset, alias, when, where, calc, aggregator, nullptr);

        // we still need to order by rowHash to ensure determinism in the results
        std::sort(rows.begin(), rows.end());
    }
  

    return { std::move(rows), std::move(vars) };
}

std::pair<std::vector<std::tuple<RowHash, RowName, std::vector<double>, std::vector<ExpressionValue> > >,
          std::vector<KnownColumn> >
getEmbedding(const SelectStatement & stm,
             SqlExpressionMldbContext & context,
             int maxDimensions,
             const std::function<bool (const Json::Value &)> & onProgress)
{
    auto boundDataset = stm.from->bind(context);
    return getEmbedding(stm.select, 
                        *boundDataset.dataset, 
                        boundDataset.asName, 
                        stm.when, *stm.where, {},
                        maxDimensions, stm.orderBy, 
                        stm.offset, stm.limit, onProgress);
}

std::vector<MatrixNamedRow>
queryWithoutDataset(SelectStatement& stm, SqlBindingScope& scope)
{
    auto boundSelect = stm.select.bind(scope);
    SqlRowScope context;
    ExpressionValue val = boundSelect(context, GET_ALL);
    MatrixNamedRow row;
    //row.rowName = RowName(stm.select.surface); //TO BE REVIEWED AS PART OF MLDBFB-264
    row.rowHash = row.rowName;
    val.mergeToRowDestructive(row.columns);

    return { std::move(row) };
}

std::vector<MatrixNamedRow>
queryFromStatement(SelectStatement & stm,
                   SqlBindingScope & scope,
                   BoundParameters params)
{
    BoundTableExpression table = stm.from->bind(scope);
    
    bool allowParallel = !QueryThreadTracker::inChildThread();

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


            

        std::shared_ptr<PipelineElement> pipeline;

        if (!stm.groupBy.empty()) {
            // Create our pipeline

            pipeline
                = PipelineElement::root(scope)
                ->params(getParamInfo)
                ->from(stm.from, stm.when,
                       SelectExpression::STAR, stm.where)
                ->where(stm.where)
                ->select(stm.groupBy)
                ->sort(stm.groupBy)
                ->partition(stm.groupBy.clauses.size())
                ->where(stm.having)
                ->select(stm.orderBy)
                ->sort(stm.orderBy)
                ->select(stm.rowName)  // second last element is rowName
                ->select(stm.select);  // last element is select
        }
        else {
            pipeline
                = PipelineElement::root(scope)
                ->params(getParamInfo)
                ->from(stm.from, stm.when,
                       SelectExpression::STAR, stm.where)
                ->where(stm.where)
                ->select(stm.orderBy)
                ->sort(stm.orderBy)
                ->select(stm.rowName)  // second last element is rowname
                ->select(stm.select);  // last element is select
        }
        
        auto boundPipeline = pipeline->bind();

        auto executor = boundPipeline->start(params, allowParallel);
        
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
            row.rowName = RowName(output->values.at(output->values.size() - 2).toUtf8String());

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

} // namespace MLDB
} // namespace Datacratic
