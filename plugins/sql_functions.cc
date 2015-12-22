// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** sql_functions.cc
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

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

SqlQueryFunctionConfig::
SqlQueryFunctionConfig()
    : select("*"),
      when(WhenExpression::TRUE),
      where(SqlExpression::TRUE),
      having(SqlExpression::TRUE)
{
}

DEFINE_STRUCTURE_DESCRIPTION(SqlQueryFunctionConfig);

SqlQueryFunctionConfigDescription::
SqlQueryFunctionConfigDescription()
{
    addField("select", &SqlQueryFunctionConfig::select,
             "SQL select expression to run.  The values in the dataset, as "
             "well as the input values, will be available for the expression "
             "calculation",
             SelectExpression("*"));
    addField("from", &SqlQueryFunctionConfig::from,
             "Dataset to select from.  The dataset is fixed at initialization "
             "time and cannot be changed in the query.");
    addField("when", &SqlQueryFunctionConfig::when,
             "Boolean expression determining which tuples from the dataset "
             "to keep based on their timestamps",
             WhenExpression::TRUE);
    addField("where", &SqlQueryFunctionConfig::where,
             "Boolean expression to choose which row to select.  In almost all "
             "cases this should be set to restrict the query to the part of "
             "the dataset that is interesting in the context of the query.",
             SqlExpression::TRUE);
    addField("orderBy", &SqlQueryFunctionConfig::orderBy,
             "Expression to choose how to order multiple rows.  The function will "
             "only return the first row, so this effectively chooses which of "
             "multiple rows will be chosen.  If not defined, the selected row "
             "will be an abitrary one of those that match.");
    addField("groupBy", &SqlQueryFunctionConfig::groupBy,
             "Expression to choose how to group rows for an aggregate query. "
             "If this is specified, the having and order by clauses will "
             "choose which row is actually selected.  Leaving this unset "
             "will disable grouping.  Grouping can cause queries to run slowly "
             "and so should be avoided for real-time queries if possible.");
    addField("having", &SqlQueryFunctionConfig::having,
             "Boolean expression to choose which group to select.  Only the "
             "groups where this expression evaluates to true will be "
             "selected.",
             SqlExpression::TRUE);
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
    result["expression"]["select"]["surface"] = functionConfig.select.surface;
    result["expression"]["select"]["ast"] = functionConfig.select.print();
    result["expression"]["where"]["surface"] = functionConfig.where->surface;
    result["expression"]["where"]["ast"] = functionConfig.where->print();
    result["expression"]["orderBy"]["surface"] = functionConfig.orderBy.surface;
    result["expression"]["orderBy"]["ast"] = functionConfig.orderBy.print();
    result["expression"]["groupBy"]["surface"] = functionConfig.groupBy.surface;
    result["expression"]["groupBy"]["ast"] = functionConfig.groupBy.print();
    result["expression"]["having"]["surface"] = functionConfig.having->surface;
    result["expression"]["having"]["ast"] = functionConfig.having->print();
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

        if (!config.groupBy.empty()) {
            // Create our pipeline

            pipeline
                = getMldbRoot(function->server)
                ->params(getParamInfo)
                ->from(config.from, config.when)
                ->where(config.where)
                ->select(config.groupBy)
                ->sort(config.groupBy)
                ->partition(config.groupBy.clauses.size())
                ->where(config.having)
                ->select(config.orderBy)
                ->sort(config.orderBy)
                ->select(config.select);
        }
        else {
                
            // Create our pipeline
            pipeline
                = getMldbRoot(function->server)
                ->params(getParamInfo)
                ->from(config.from, config.when)
                ->where(config.where)
                ->select(config.orderBy)
                ->sort(config.orderBy)
                ->select(config.select);
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
}

SqlExpressionFunction::
SqlExpressionFunction(MldbServer * owner,
                      PolyConfig config,
                      const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<SqlExpressionFunctionConfig>();
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
    SqlExpressionFunctionApplier(SqlBindingScope & outerContext,
                                 const SqlExpressionFunction * function,
                                 const SelectExpression & expression,
                                 const FunctionValues & input)
        : FunctionApplier(function),
          context(outerContext, input),
          bound(expression.bind(context))
    {
        this->info.output = *bound.info;
    }

    virtual ~SqlExpressionFunctionApplier()
    {
    }

    FunctionOutput apply(const FunctionContext & context) const
    {
        return bound(this->context.getRowContext(context));
    }

    FunctionExpressionContext context;
    BoundSqlExpression bound;
};

std::unique_ptr<FunctionApplier>
SqlExpressionFunction::
bind(SqlBindingScope & outerContext,
     const FunctionValues & input) const
{
    std::unique_ptr<SqlExpressionFunctionApplier> result
        (new SqlExpressionFunctionApplier(outerContext, this,
                                          functionConfig.expression,
                                          input));

    // Check that these input values can provide everything needed for the result
    input.checkCompatibleAsInputTo(result->info.input);

    return std::move(result);
}

FunctionOutput
SqlExpressionFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
    return static_cast<const SqlExpressionFunctionApplier &>(applier)
        .apply(context);
}

FunctionInfo
SqlExpressionFunction::
getFunctionInfo() const
{
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
    : select(SelectExpression::parse("*")),
      when(WhenExpression::parse("true")),
      where(SqlExpression::parse("true")),
      having(SqlExpression::parse("true")),
      offset(0),
      limit(-1),
      rowName(SqlExpression::parse("rowName()")),
      skipEmptyRows(false)
{
    outputDataset.withType("sparse.mutable");
}

DEFINE_STRUCTURE_DESCRIPTION(TransformDatasetConfig);


TransformDatasetConfigDescription::
TransformDatasetConfigDescription()
{
    addFieldDesc("inputDataset", &TransformDatasetConfig::inputDataset,
                 "Dataset to be transformed.  This must be an existing dataset.",
                 makeInputDatasetDescription());
    addField("outputDataset", &TransformDatasetConfig::outputDataset,
             "Output dataset configuration.  This may refer either to an "
             "existing dataset, or a fully specified but non-existing dataset "
             "which will be created by the procedure.", PolyConfigT<Dataset>().withType("sparse.mutable"));
    addField("select", &TransformDatasetConfig::select,
             "Values to select.  These columns will be written as the output "
             "of the dataset.",
             SelectExpression::parse("*"));
    addField("when", &TransformDatasetConfig::when,
             "Boolean expression determining which tuples from the dataset "
             "to keep based on their timestamps",
             WhenExpression::parse("true"));
    addField("where", &TransformDatasetConfig::where,
             "Boolean expression determining which rows from the input "
             "dataset will be processed.",
             SqlExpression::parse("true"));
    addField("groupBy", &TransformDatasetConfig::groupBy,
             "Expression used to group values for aggregation queries.  "
             "Default is to run a row-by-row query, not an aggregation.");
    addField("having", &TransformDatasetConfig::having,
             "Boolean expression used to select which groups will write a "
             "value to the output for a grouped query.  Default is to "
             "write all groups",
             SqlExpression::parse("true"));
    addField("orderBy", &TransformDatasetConfig::orderBy,
             "Expression dictating how output rows will be ordered.  This is "
             "only meaningful when offset and/or limit is used, as it "
             "affects in which order those rows will be seen by the "
             "windowing code.");
    addField("offset", &TransformDatasetConfig::offset,
             "Number of rows of output to skip.  Default is to skip none. "
             "Note that selecting a subset of data is usally better done "
             "using the where clause (eg, `where rowHash() % 10 = 0`) as "
             "it is more efficient and repeatable.");
    addField("limit", &TransformDatasetConfig::limit,
             "Number of rows of output to produce.  Default is to produce all. "
             "This can be used to produce a cut-down dataset, but again it's "
             "normally better to use where as that doesn't require that "
             "results be sorted for repeatability.");
    addField("rowName", &TransformDatasetConfig::rowName,
             "Expression to set the row name for the output dataset.  Default "
             "depends on whether it's a grouping query or not: for a grouped "
             "query, it's the groupBy expression.  For a non-grouped query, "
             "it's the rowName() of the input dataset.  Beware of a rowName "
             "expression that gives non-unique row names; this will lead to "
             "errors in some dataset implementations.",
             SqlExpression::parse("rowName()"));
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

    auto boundDataset = procedureConfig.inputDataset->bind(context);
    std::vector< std::shared_ptr<SqlExpression> > aggregators = procedureConfig.select.findAggregators();

    // Create the output 
    std::shared_ptr<Dataset> output;
    if (!procedureConfig.outputDataset.type.empty() || !procedureConfig.outputDataset.id.empty()) {
        output = createDataset(server, procedureConfig.outputDataset, nullptr, true /*overwrite*/);
    }

    bool skipEmptyRows = procedureConfig.skipEmptyRows;

    // Run it
    if (procedureConfig.groupBy.clauses.empty()) {

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
            = (procedureConfig.offset != 0 || procedureConfig.limit != -1);

        BoundSelectQuery(procedureConfig.select,
                         *boundDataset.dataset,
                         boundDataset.asName,
                         procedureConfig.when,
                         procedureConfig.where,
                         procedureConfig.orderBy,
                         { procedureConfig.rowName },
                         implicitOrderByRowHash)
            .execute(recordRowInOutputDataset,
                     procedureConfig.offset,
                     procedureConfig.limit,
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

        BoundGroupByQuery(procedureConfig.select,
                          *boundDataset.dataset,
                          boundDataset.asName,
                          procedureConfig.when,
                          procedureConfig.where,
                          procedureConfig.groupBy,
                          aggregators,
                          *procedureConfig.having,
                          *procedureConfig.rowName,
                          procedureConfig.orderBy)
            .execute(recordRowInOutputDataset,
                     procedureConfig.offset,
                     procedureConfig.limit,
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
