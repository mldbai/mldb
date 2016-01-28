/** sql_functions.cc
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#include "sql_functions.h"
#include "mldb/server/mldb_server.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/server/dataset_context.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/jml/utils/worker_task.h"
#include "mldb/server/function_contexts.h"
#include "mldb/server/bound_queries.h"
#include "mldb/sql/table_expression_operations.h"
#include "mldb/sql/join_utils.h"
#include "mldb/sql/execution_pipeline.h"
#include "mldb/arch/backtrace.h"
#include "mldb/types/any_impl.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/rest/in_process_rest_connection.h"
#include <memory>

using namespace std;


namespace Datacratic {
namespace MLDB {

std::shared_ptr<PipelineElement>
getMldbRoot(MldbServer * server)
{
    return PipelineElement::root(std::make_shared<SqlExpressionMldbContext>(server));
}

/*****************************************************************************/
/* SQL QUERY FUNCTION                                                        */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(SqlQueryFunctionConfig);

SqlQueryFunctionConfigDescription::
SqlQueryFunctionConfigDescription()
{
    addField("query", &SqlQueryFunctionConfig::query,
             "SQL query to run.  The values in the dataset, as "
             "well as the input values, will be available for the expression "
             "calculation");
}
                      
SqlQueryFunction::
SqlQueryFunction(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<SqlQueryFunctionConfig>();
}

Any
SqlQueryFunction::
getStatus() const
{
    Json::Value result;
    result["expression"]["query"]["surface"] = functionConfig.query.stm->surface;
    result["expression"]["query"]["ast"] = functionConfig.query.stm->print();
    return result;
}

/** Structure that does all the work of the SQL expression function. */
struct SqlQueryFunctionApplier: public FunctionApplier {
    SqlQueryFunctionApplier(const SqlQueryFunction * function,
                            const SqlQueryFunctionConfig & config)
        : FunctionApplier(function),
          from(std::move(from))
    {
        // Called when we bind a parameter, to get its information
        auto getParamInfo = [&] (const Utf8String & paramName)
            {
                auto info = std::make_shared<AnyValueInfo>();
                    
                // Record that we need it into our input info
                this->info.input.addValue(paramName, info);
                return info;
            };

        if (!config.query.stm->groupBy.empty()) {
            // Create our pipeline

            pipeline
                = getMldbRoot(function->server)
                ->params(getParamInfo)
                ->from(config.query.stm->from, config.query.stm->when,
                       SelectExpression::STAR, config.query.stm->where)
                ->where(config.query.stm->where)
                ->select(config.query.stm->groupBy)
                ->sort(config.query.stm->groupBy)
                ->partition(config.query.stm->groupBy.clauses.size())
                ->where(config.query.stm->having)
                ->select(config.query.stm->orderBy)
                ->sort(config.query.stm->orderBy)
                ->select(config.query.stm->select);
        }
        else {
                
            // Create our pipeline
            pipeline
                = getMldbRoot(function->server)
                ->params(getParamInfo)
                ->from(config.query.stm->from, config.query.stm->when,
                       SelectExpression::STAR, config.query.stm->where)
                ->where(config.query.stm->where)
                ->select(config.query.stm->orderBy)
                ->sort(config.query.stm->orderBy)
                ->select(config.query.stm->select);
        }

        // Bind the pipeline
        boundPipeline = pipeline->bind();

        // What type does the pipeline return?
        this->info.output = *boundPipeline->outputScope()->outputInfo().back();
    }

    virtual ~SqlQueryFunctionApplier()
    {
    }

    FunctionOutput apply(const FunctionContext & context) const
    {
        // 1.  Run our generator, finding all rows
        BoundParameters params
            = [&] (const Utf8String & name) -> ExpressionValue
            {
                return context.get(name);
            };
        
        auto executor = boundPipeline->start(params,
                                             !QueryThreadTracker::inChildThread() /* allowParallel */);
        auto output = executor->take();

        //if (output)
        //    cerr << "got output " << jsonEncode(output) << endl;

        FunctionOutput result;
        if (output)
            result = std::move(output->values.back());
        return result;
    }

    std::shared_ptr<Dataset> from;
    std::shared_ptr<PipelineElement> pipeline;
    std::shared_ptr<BoundPipelineElement> boundPipeline;
};

std::unique_ptr<FunctionApplier>
SqlQueryFunction::
bind(SqlBindingScope & outerContext,
     const FunctionValues & input) const
{
    std::unique_ptr<SqlQueryFunctionApplier> result
        (new SqlQueryFunctionApplier(this, functionConfig));

    // Check that these input values can provide everything needed for the result
    input.checkCompatibleAsInputTo(result->info.input);

    return std::move(result);
}

FunctionOutput
SqlQueryFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
    return static_cast<const SqlQueryFunctionApplier &>(applier)
        .apply(context);
}

FunctionInfo
SqlQueryFunction::
getFunctionInfo() const
{
    SqlQueryFunctionApplier applier(this, functionConfig);
    return applier.info;
}

static RegisterFunctionType<SqlQueryFunction, SqlQueryFunctionConfig>
regSqlQueryFunction(builtinPackage(),
                    "sql.query",
                    "Run a single row SQL query against a dataset",
                    "functions/SqlQueryFunction.md.html");


/*****************************************************************************/
/* SQL EXPRESSION FUNCTION                                                   */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(SqlExpressionFunctionConfig);

SqlExpressionFunctionConfigDescription::
SqlExpressionFunctionConfigDescription()
{
    addField("expression", &SqlExpressionFunctionConfig::expression,
             "SQL expression function to run.  Takes the same syntax as a SELECT "
             "clause (but without the SELECT keyword); for example "
             "'x, y + 1 AS z'");
    addField("prepared", &SqlExpressionFunctionConfig::prepared,
             "Do we pre-prepare the expression to be run many times quickly?  "
             "If this is true, it will only be bound once, for generic "
             "inputs, and so will allow for quick individual queries, "
             "possibly at the expense of batch queries being slower.  In "
             "this case, the expression also cannot refer to variables "
             "outside of the arguments to the expression.  "
             "If this is false, the default, then for every query the "
             "expression will be specialized (rebound) for that query's "
             "data type.  "
             "This can lead to faster batch queries, at the expense of a "
             "possibly high per-query overhead for individual queries.",
             false);
}

SqlExpressionFunction::
SqlExpressionFunction(MldbServer * owner,
                      PolyConfig config,
                      const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner), outerScope(owner), innerScope(outerScope)
{
    functionConfig = config.params.convert<SqlExpressionFunctionConfig>();

    if (functionConfig.prepared) {
        // 1.  Bind the expression in.  That will tell us what it is expecting
        //     as an input.
        this->bound = functionConfig.expression.bind(innerScope);

        // 2.  Our output is known by the bound expression
        this->info.output = *this->bound.info;
    
        // 3.  Our required input is known by the binding context, as it records
        //     what was read.
        info.input = innerScope.input;
    }
}

Any
SqlExpressionFunction::
getStatus() const
{
    Json::Value result;
    result["expression"]["surface"] = functionConfig.expression.surface;
    result["expression"]["ast"] = functionConfig.expression.print();
    return result;
}

/** Structure that does all the work of the SQL expression function. */
struct SqlExpressionFunctionApplier: public FunctionApplier {
    SqlExpressionFunctionApplier(SqlBindingScope & outerScope,
                                 const SqlExpressionFunction * function,
                                 const FunctionValues & input)
        : FunctionApplier(function),
          function(function),
          innerScope(outerScope, input)
    {
        if (!function->functionConfig.prepared) {
            // Specialize to this input
            this->bound = function->functionConfig.expression.bind(innerScope);
            // That leads to a specialized output
            this->info.output = *bound.info;
        }
        else {
            this->info = function->info;
        }
    }
    
    virtual ~SqlExpressionFunctionApplier()
    {
    }

    FunctionOutput apply(const SqlRowScope& outerRowScope,
                         const FunctionContext & context) const
    {
        if (function->functionConfig.prepared) {
            // Use the pre-bound version.    Note that we ignore the outer
            // row scope, which wasn't available when we prepared the
            // expression.
            SqlRowScope emptyOuterRowScope;
            return function->bound(function->innerScope.getRowContext(emptyOuterRowScope, context));
        }
        else {
            // Use the specialized version.  The outer row scope is available
            // from the expression.
            return bound(this->innerScope.getRowContext(outerRowScope, context));
        }
    }

    const SqlExpressionFunction * function;
    FunctionExpressionContext innerScope;
    BoundSqlExpression bound;
};

std::unique_ptr<FunctionApplier>
SqlExpressionFunction::
bind(SqlBindingScope & outerContext,
     const FunctionValues & input) const
{
    std::unique_ptr<SqlExpressionFunctionApplier> result
        (new SqlExpressionFunctionApplier(outerContext, this, input));

    // Check that these input values can provide everything needed for the result
    input.checkCompatibleAsInputTo(result->info.input);

    return std::move(result);
}

FunctionOutput 
SqlExpressionFunction::
applyOuter(const SqlRowScope& outer, const FunctionApplier & applier,
                              const FunctionContext & context) const
{
  return static_cast<const SqlExpressionFunctionApplier &>(applier)
        .apply(outer, context);
}

FunctionOutput
SqlExpressionFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
    //applyOuter should get called instead 
    ExcAssert(false);
}

FunctionInfo
SqlExpressionFunction::
getFunctionInfo() const
{
    if (functionConfig.prepared) {
        return this->info;
    }

    FunctionInfo result;

    // 0.  We want the pure function information, so we assume there is
    //     no context for it apart from MLDB itself.
    SqlExpressionMldbContext outerContext(MldbEntity::getOwner(this->server));

    // 1.  Create a binding context to see what this function takes
    FunctionExpressionContext context(outerContext);

    // 2.  Bind the expression in.  That will tell us what it is expecting
    //     as an input.
    BoundSqlExpression bound = functionConfig.expression.bind(context);

    // 3.  Our output is known by the bound expression
    result.output = *bound.info;
    
    // 4.  Our required input is known by the binding context, as it records
    //     what was read.
    result.input = context.input;

    return result;
}

static RegisterFunctionType<SqlExpressionFunction, SqlExpressionFunctionConfig>
regSqlExpressionFunction(builtinPackage(),
                         "sql.expression",
                         "Run an SQL expression as a function",
                         "functions/SqlExpressionFunction.md.html");


/*****************************************************************************/
/* TRANSFORM DATASET                                                         */
/*****************************************************************************/

TransformDatasetConfig::
TransformDatasetConfig()
    : skipEmptyRows(false)
{
    outputDataset.withType("sparse.mutable");
}

DEFINE_STRUCTURE_DESCRIPTION(TransformDatasetConfig);


TransformDatasetConfigDescription::
TransformDatasetConfigDescription()
{
    addField("inputData", &TransformDatasetConfig::inputData,
             "A SQL statement to select the rows from a dataset to be transformed.  This supports "
             "all MLDB's SQL expressions including but not limited to where, when, order by and "
             "group by clauses.  These expressions can be used to refine the rows to transform.");
    addField("outputDataset", &TransformDatasetConfig::outputDataset,
             "Output dataset configuration.  This may refer either to an "
             "existing dataset, or a fully specified but non-existing dataset "
             "which will be created by the procedure.", PolyConfigT<Dataset>().withType("sparse.mutable"));
    addField("skipEmptyRows", &TransformDatasetConfig::skipEmptyRows,
             "Skip rows from the input dataset where no values are selected",
             false);
    addParent<ProcedureConfig>();
}

TransformDataset::
TransformDataset(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    procedureConfig = config.params.convert<TransformDatasetConfig>();
}

RunOutput
TransformDataset::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    // Get the input dataset
    SqlExpressionMldbContext context(server);

    auto boundDataset = procedureConfig.inputData.stm->from->bind(context);
    std::vector< std::shared_ptr<SqlExpression> > aggregators = 
        procedureConfig.inputData.stm->select.findAggregators();

    // Create the output 
    std::shared_ptr<Dataset> output;
    if (!procedureConfig.outputDataset.type.empty() || !procedureConfig.outputDataset.id.empty()) {
        output = createDataset(server, procedureConfig.outputDataset, nullptr, true /*overwrite*/);
    }

    bool skipEmptyRows = procedureConfig.skipEmptyRows;

    // Run it
    if (procedureConfig.inputData.stm->groupBy.clauses.empty()) {

        // We accumulate multiple rows per thread and insert with recordRows
        // to be more efficient.
        PerThreadAccumulator<std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > > accum;


        auto recordRowInOutputDataset
            = [&] (const MatrixNamedRow & row,
                   const std::vector<ExpressionValue> & calc)
            {
                // Nulls with non-finite timestamp are not recorded; they
                // come from an expression that matched nothing and can't
                // be represented (they will be read automatically as nulls).
                std::vector<std::tuple<ColumnName, CellValue, Date> > cols;
                cols.reserve(row.columns.size());
                for (auto & c: row.columns) {
                    if (std::get<1>(c).empty()
                        && !std::get<2>(c).isADate())
                        continue;
                    cols.push_back(c);
                }

                if (!skipEmptyRows || cols.size() > 0)
                {
                    auto & rows = accum.get();
                    rows.reserve(10000);
                    rows.emplace_back(RowName(calc.at(0).toString()), std::move(cols));

                    if (rows.size() >= 10000) {
                        output->recordRows(rows);
                        rows.clear();
                    }
                }

                return true;
            };

        // We only add an implicit order by (which defeats parallelization)
        // if we have a limit or offset parameter.
        bool implicitOrderByRowHash
            = (procedureConfig.inputData.stm->offset != 0 || 
               procedureConfig.inputData.stm->limit != -1);

        BoundSelectQuery(procedureConfig.inputData.stm->select,
                         *boundDataset.dataset,
                         boundDataset.asName,
                         procedureConfig.inputData.stm->when,
                         *procedureConfig.inputData.stm->where,
                         procedureConfig.inputData.stm->orderBy,
                         { procedureConfig.inputData.stm->rowName },
                         implicitOrderByRowHash)
            .execute(recordRowInOutputDataset,
                     procedureConfig.inputData.stm->offset,
                     procedureConfig.inputData.stm->limit,
                     onProgress);

        // Finish off the last bits of each thread
        accum.forEach([&] (std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > * rows)
                      {
                          output->recordRows(*rows);
                      });
    }
    else {
        auto recordRowInOutputDataset
            = [&] (const MatrixNamedRow & row)
            {
                output->recordRow(row.rowName, row.columns);
                return true;
            };

        BoundGroupByQuery(procedureConfig.inputData.stm->select,
                          *boundDataset.dataset,
                          boundDataset.asName,
                          procedureConfig.inputData.stm->when,
                          *procedureConfig.inputData.stm->where,
                          procedureConfig.inputData.stm->groupBy,
                          aggregators,
                          *procedureConfig.inputData.stm->having,
                          *procedureConfig.inputData.stm->rowName,
                          procedureConfig.inputData.stm->orderBy)
            .execute(recordRowInOutputDataset,
                     procedureConfig.inputData.stm->offset,
                     procedureConfig.inputData.stm->limit,
                     onProgress);
    }
    // Save the dataset we created

    output->commit();

    return output->getStatus();
}

Any
TransformDataset::
getStatus() const
{
    return Any();
}

static RegisterProcedureType<TransformDataset, TransformDatasetConfig>
regTransformDataset(builtinPackage(),
                    "transform",
                    "Apply an SQL expression over a dataset to transform into another dataset",
                    "procedures/TransformDataset.md.html");


} // namespace MLDB
} // namespace Datacratic
