/** sql_functions.cc
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "sql_functions.h"
#include "mldb/server/mldb_server.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/server/dataset_context.h"
#include "mldb/server/dataset_context.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/base/parallel.h"
#include "mldb/server/bound_queries.h"
#include "mldb/sql/table_expression_operations.h"
#include "mldb/sql/join_utils.h"
#include "mldb/sql/execution_pipeline.h"
#include "mldb/arch/backtrace.h"
#include "mldb/types/any_impl.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "mldb/plugins/sql_config_validator.h"
#include "mldb/server/analytics.h"
#include "mldb/utils/log.h"
#include "mldb/rest/cancellation_exception.h"
#include <memory>

using namespace std;



namespace MLDB {

namespace {
inline std::vector<std::tuple<ColumnPath, CellValue, Date> >
filterEmptyColumns(MatrixNamedRow & row) {
    // Nulls with non-finite timestamp are not recorded; they
    // come from an expression that matched nothing and can't
    // be represented (they will be read automatically as nulls).
    std::vector<std::tuple<ColumnPath, CellValue, Date> > cols;
    cols.reserve(row.columns.size());
    for (auto & c: row.columns) {
        if (std::get<1>(c).empty()
            && !std::get<2>(c).isADate())
            continue;
        cols.emplace_back(std::move(c));
    }
    return cols;
}
}

std::shared_ptr<PipelineElement>
getMldbRoot(MldbServer * server)
{
    return PipelineElement::root(std::make_shared<SqlExpressionMldbScope>(server));
}

/*****************************************************************************/
/* SQL QUERY FUNCTION                                                        */
/*****************************************************************************/

DEFINE_ENUM_DESCRIPTION(SqlQueryOutput);

SqlQueryOutputDescription::
SqlQueryOutputDescription()
{
    addValue("FIRST_ROW", FIRST_ROW, "Return only the first row of the query");
    addValue("NAMED_COLUMNS", NAMED_COLUMNS,
             "Output is a table with a 'value' and optional 'column' "
             "column.  Output row will be constructed from all of the "
             "returned columns, assembled into a single row, with column "
             "names provided by the 'column' column, or if null, the "
             "row name.");
}

DEFINE_STRUCTURE_DESCRIPTION(SqlQueryFunctionConfig);

SqlQueryFunctionConfigDescription::
SqlQueryFunctionConfigDescription()
{
    addField("query", &SqlQueryFunctionConfig::query,
             "SQL query to run.  The values in the dataset, as "
             "well as the input values, will be available for the expression "
             "calculation");
    addField("output", &SqlQueryFunctionConfig::output,
             "Controls how the query output is converted into a row. "
             "`FIRST_ROW` (the default) will return only the first row produced "
             "by the query.  `NAMED_COLUMNS` will construct a row from the "
             "whole returned table, which must have a 'value' column "
             "containing the value.  If there is a 'column' column, it will "
             "be used as a column name, otherwise the row name will be used.",
             FIRST_ROW);
}

SqlQueryFunction::
SqlQueryFunction(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner, config)
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
    result["info"] = jsonEncode(getFunctionInfo());
    return result;
}

/** Structure that does all the work of the SQL expression function. */
struct SqlQueryFunctionApplier: public FunctionApplier {
    SqlQueryFunctionApplier(const SqlQueryFunction * function,
                            const SqlQueryFunctionConfig & config)
        : FunctionApplier(function), function(function)
    {
        std::set<Utf8String> inputParams;

        // Called when we bind a parameter, to get its information
        auto getParamInfo = [&] (const Utf8String & paramName)
            {
                inputParams.insert(paramName);
                return std::make_shared<AnyValueInfo>();
            };

        pipeline = getMldbRoot(function->server)->statement(*config.query.stm, getParamInfo);

        // Bind the pipeline; this populates the input parameters
        boundPipeline = pipeline->bind();

        std::vector<KnownColumn> inputColumns;
        inputColumns.reserve(inputParams.size());
        for (auto & p: inputParams) {
            inputColumns.emplace_back(PathElement(p), std::make_shared<AnyValueInfo>(),
                                      COLUMN_IS_SPARSE);
        }

        if (!inputColumns.empty())
            this->info.input.emplace_back(new RowValueInfo(std::move(inputColumns),
                                                           SCHEMA_CLOSED));
        

        switch (function->functionConfig.output) {
        case FIRST_ROW:
            // What type does the pipeline return?
            this->info.output = ExpressionValueInfo::toRow
                (boundPipeline->outputScope()->outputInfo().back());
            break;
        case NAMED_COLUMNS:
            std::vector<KnownColumn> outputColumns;
            outputColumns.emplace_back(PathElement("output"),
                                       std::make_shared<UnknownRowValueInfo>(),
                                       COLUMN_IS_DENSE,
                                       0);
            this->info.output.reset(new RowValueInfo(std::move(outputColumns),
                                                     SCHEMA_CLOSED));
            break;
        }
    }

    virtual ~SqlQueryFunctionApplier()
    {
    }

    ExpressionValue apply(const ExpressionValue & context) const
    {
        // 1.  Run our generator, finding all rows
        BoundParameters params
            = [&] (const Utf8String & name) -> ExpressionValue
            {
                return context.getColumn(name);
            };

        auto executor = boundPipeline->start(params);

        switch (function->functionConfig.output) {
        case FIRST_ROW: {
            ExpressionValue result;

            auto output = executor->take();

            if (output) {
                // MLDB-1329 band-aid fix.  This appears to break a circlar
                // reference chain that stops the elements from being
                // released.
                output->group.clear();
                result = std::move(output->values.back());
            }

            return result;
        }
        case NAMED_COLUMNS:
            std::vector<std::tuple<PathElement, ExpressionValue> > row;

            ssize_t limit = function->functionConfig.query.stm->limit;
            ssize_t offset = function->functionConfig.query.stm->offset;

            auto output = executor->take();
            for (size_t n = 0;
                 output && (limit == -1 || n < limit + offset);
                 output = executor->take(), ++n) {

                if (output) {
                    // MLDB-1329 band-aid fix.  This appears to break a circlar
                    // reference chain that stops the elements from being
                    // released.
                    output->group.clear();
                }

                if (n < offset) {
                    continue;
                }

                PathElement foundCol;
                ExpressionValue foundVal;
                int numFoundCol = 0;
                int numFoundVal = 0;

                auto onVal = [&] (PathElement & col,
                                  ExpressionValue & val)
                    {
                        if (col == PathElement("column")) {
                            if (val.empty()) {
                                throw HttpReturnException
                                (400, "Column names in NAMED_COLUMNS SQL can't be "
                                 "null");
                            }
                            foundCol = PathElement(val.getAtom().toUtf8String());
                            ++numFoundCol;
                        }
                        else if (col == PathElement("value")) {
                            foundVal = std::move(val);
                            ++numFoundVal;
                        }
                        else {
                            throw HttpReturnException
                                (400, "Rows returned from NAMED_COLUMNS SQL "
                                 "query can only contain 'column' and 'value' "
                                 "columns",
                                 "unknownColumn", col,
                                 "unknownColumnValue", val);
                        }

                        return true;
                    };

                output->values.back().forEachColumnDestructive(onVal);

                if (numFoundCol != 1 || numFoundVal != 1) {
                    throw HttpReturnException
                        (400, "Rows returned from NAMED_COLUMNS SQL query "
                         "must contain exactly one 'column' and one "
                         "'value' column",
                         "numTimesFoundColumn", numFoundCol,
                         "numTimesFoundValue", numFoundVal);
                }
                if (foundCol.null()) {
                    throw HttpReturnException
                        (400, "Empty or null column names cannot be "
                         "returned from NAMED_COLUMNS sql query");
                }

                row.emplace_back(std::move(foundCol), std::move(foundVal));
            }

            StructValue result;
            result.emplace_back("output", std::move(row));

            return std::move(result);
        }

        ExcAssert(false);
    }

    const SqlQueryFunction * function;
    std::shared_ptr<Dataset> from;
    std::shared_ptr<PipelineElement> pipeline;
    std::shared_ptr<BoundPipelineElement> boundPipeline;
};

std::unique_ptr<FunctionApplier>
SqlQueryFunction::
bind(SqlBindingScope & outerContext,
     const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const
{
    std::unique_ptr<SqlQueryFunctionApplier> result
        (new SqlQueryFunctionApplier(this, functionConfig));

    result->info.checkInputCompatibility(input);

    return std::move(result);
}

ExpressionValue
SqlQueryFunction::
apply(const FunctionApplier & applier,
      const ExpressionValue & context) const
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
    addField("raw", &SqlExpressionFunctionConfig::raw,
             "If true, then the output will be raw (just the result "
             "of the expression will be returned; it will not be turned "
             "into a row and the name of the output will be ignored).  If "
             "false (default), then the output will be structured into "
             "a row.  For example, the expression `1 AS z` will return "
             "`1` if raw is true, but `{z: 1}` if raw is "
             "false.", false);
    addField("autoInput", &SqlExpressionFunctionConfig::autoInput,
             "If true, then a function that takes a single parameter "
             "will automatically pass that parameter without needing "
             "to put it within an object.  For example, if `expression` "
             "is `x + 1`, then with `autoInput` as `false` the function "
             "must be called with `{x: 2}` but with `autoInput` as `true` "
             "the function can be called with `2` and the `x` will be "
             "added automatically.", false);
}

SqlExpressionFunction::
SqlExpressionFunction(MldbServer * owner,
                      PolyConfig config,
                      const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner, config),
      outerScope(new SqlExpressionMldbScope(owner)),
      innerScope(new SqlExpressionExtractScope(*outerScope))
{
    functionConfig = config.params.convert<SqlExpressionFunctionConfig>();

    if (functionConfig.prepared) {
        // 1.  Bind the expression in.  That will tell us what it is expecting
        //     as an input.
        this->bound = doBind(*innerScope);

        // 2.  Our output is known by the bound expression
        this->info.output = this->bound.info;
        
        if (functionConfig.autoInput) {
            std::tie(this->preparedAutoInputName, info.input)
                = getAutoInputName(*innerScope);
        }
        else {
            // 3.  Infer the input, now the binding is all done
            innerScope->inferInput();
            
            // 4.  Our required input is known by the binding context, as it
            // records what was read.
            info.input.emplace_back(innerScope->inputInfo);
        }

    }
}

SqlExpressionFunction::
~SqlExpressionFunction()
{
}

std::tuple<PathElement, std::vector<std::shared_ptr<ExpressionValueInfo> > >
SqlExpressionFunction::
getAutoInputName(SqlExpressionExtractScope & innerScope) const
{
    if (innerScope.inferredInputs.size() != 1) {
        Utf8String knownVars;
        for (auto & v: innerScope.inferredInputs) {
            if (!knownVars.empty())
                knownVars += ", ";
            knownVars += v.toUtf8String();
        }
        throw HttpReturnException
            (400, "An sql.expression function with autoInput=true "
             "must have a single variable in the expression; "
             "the passed expression " + functionConfig.expression.surface
             + " had " + to_string(innerScope.inferredInputs.size())
             + " variables (" + knownVars + ").");
    }
    // Can take any input
    std::vector<std::shared_ptr<ExpressionValueInfo> > inputs
        = {std::make_shared<AnyValueInfo>()};
    return make_tuple((*innerScope.inferredInputs.begin())[0],
                      std::move(inputs));
}

BoundSqlExpression
SqlExpressionFunction::
doBind(SqlExpressionExtractScope & innerScope) const
{
    if (functionConfig.raw) {
        // 1.  Grab the single SqlExpression that we need from the select
        //     expression
        if (functionConfig.expression.clauses.size() != 1) {
            throw HttpReturnException
                (400, "An sql.expression function with raw=true "
                 "must have a single clause in the select; there were "
                 + to_string(functionConfig.expression.clauses.size())
                 + " in the passed expression "
                 + functionConfig.expression.surface);
        }
        auto singleClause = functionConfig.expression.clauses[0];
        auto named
            = std::dynamic_pointer_cast<NamedColumnExpression>(singleClause);
        if (!named) {
            throw HttpReturnException
                (400, "An sql.expression function with raw=true "
                 "must have a single statement in the select; passed "
                 "expression was " + functionConfig.expression.surface
                 + " which parsed as " + functionConfig.expression.print()
                 + ".");
        }
        return named->expression->bind(innerScope);
    }
    else {
        // 1.  Bind the expression in.  That will tell us what it is expecting
        //     as an input.
        return functionConfig.expression.bind(innerScope);
    }
}

Any
SqlExpressionFunction::
getStatus() const
{
    Json::Value result;
    result["expression"]["surface"] = functionConfig.expression.surface;
    result["expression"]["ast"] = functionConfig.expression.print();
    result["info"] = jsonEncode(getFunctionInfo());
    return result;
}

/** Structure that does all the work of the SQL expression function. */
struct SqlExpressionFunctionApplier: public FunctionApplier {
    SqlExpressionFunctionApplier
        (SqlBindingScope & outerScope,
         const SqlExpressionFunction * function,
         const std::vector<std::shared_ptr<ExpressionValueInfo> > & input)
        : FunctionApplier(function),
          function(function),
          autoInputName(nullptr),
          innerScope(outerScope, input.at(0))
    {
        if (!function->functionConfig.prepared) {
            // Specialize to this input
            this->bound = function->doBind(innerScope);

            if (function->functionConfig.autoInput) {
                std::tie(this->autoInputNameStorage,
                         this->info.input)
                    = function->getAutoInputName(innerScope);
                this->autoInputName = &this->autoInputNameStorage;
            }
            else {
                innerScope.inferInput();
                this->info.input = { std::move(innerScope.inputInfo) };
            }
            // That leads to a specialized output
            this->info.output = std::move(bound.info);
        }
        else {
            this->info = function->info;
            this->autoInputName = &function->preparedAutoInputName;
        }
    }

    virtual ~SqlExpressionFunctionApplier()
    {
    }

    ExpressionValue apply(const ExpressionValue & input) const
    {
        ExpressionValue autoInputNameStorage;
        if (function->functionConfig.autoInput) {
            ExcAssert(this->autoInputName);
            StructValue val;
            val.emplace_back(*autoInputName, input);
            autoInputNameStorage = std::move(val);
        }

        const ExpressionValue & realInput 
            = function->functionConfig.autoInput
            ? autoInputNameStorage
            : input;

        // We know that we won't go outside of the current row, so we can
        // pass in a dummy object here.
        SqlRowScope outerRow;

        if (function->functionConfig.prepared) {
            // Use the pre-bound version.   
            return function->bound(function->innerScope->getRowScope(realInput),
                                   GET_LATEST);
        }
        else {
            // Use the specialized version. 
            return bound(this->innerScope.getRowScope(realInput),
                         GET_LATEST);
        }
    }

    const SqlExpressionFunction * function;
    const PathElement * autoInputName;
    PathElement autoInputNameStorage;
    SqlExpressionExtractScope innerScope;
    BoundSqlExpression bound;
};

std::unique_ptr<FunctionApplier>
SqlExpressionFunction::
bind(SqlBindingScope & outerContext,
     const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const
{
    std::unique_ptr<SqlExpressionFunctionApplier> result
        (new SqlExpressionFunctionApplier(outerContext, this, input));

    result->info.checkInputCompatibility(input);

    return std::move(result);
}

ExpressionValue
SqlExpressionFunction::
apply(const FunctionApplier & applier,
      const ExpressionValue & context) const
{
    return static_cast<const SqlExpressionFunctionApplier &>(applier)
           .apply(context);
}

FunctionInfo
SqlExpressionFunction::
getFunctionInfo() const
{
    if (functionConfig.prepared) {
        return this->info;
    }

    FunctionInfo result;

    // 1.  Create a binding context to see what this function takes
    //     We want the pure function information, so we assume there is
    //     no context for it apart from MLDB itself.
    SqlExpressionMldbScope ultimateScope(MldbEntity::getOwner(this->server));
    SqlExpressionExtractScope outerScope(ultimateScope);

    // 2.  Bind the expression in.  That will tell us what it is expecting
    //     as an input.
    BoundSqlExpression bound = functionConfig.expression.bind(outerScope);

    // 3.  Our output is known by the bound expression
    result.output = ExpressionValueInfo::toRow(bound.info);
    
    // 4.  Infer our input
    outerScope.inferInput();

    // 4.  Our required input is known by the binding context, as it records
    //     what was read.
    if (functionConfig.autoInput) {
        result.input = std::get<1>(getAutoInputName(outerScope));
    }
    else {
        result.input.emplace_back(outerScope.inputInfo);
    }
    
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
    auto runProcConf = applyRunConfOverProcConf(procedureConfig, run);

    if (runProcConf.inputData.stm == nullptr) {
        throw HttpReturnException(400, "You need to define inputData");
    }

    // Get the input dataset
    SqlExpressionMldbScope context(server);

    bool emptyGroupBy = runProcConf.inputData.stm->groupBy.clauses.empty();

    std::vector< std::shared_ptr<SqlExpression> > aggregators = 
        runProcConf.inputData.stm->select
        .findAggregators(!emptyGroupBy);
    std::vector< std::shared_ptr<SqlExpression> > havingaggregators
        = findAggregators(runProcConf.inputData.stm->having, !emptyGroupBy);
    std::vector< std::shared_ptr<SqlExpression> > orderbyaggregators
        = runProcConf.inputData.stm->orderBy.findAggregators(!emptyGroupBy);
    std::vector< std::shared_ptr<SqlExpression> > namedaggregators
        = findAggregators(runProcConf.inputData.stm->rowName, !emptyGroupBy);

    // Create the output
    std::shared_ptr<Dataset> output =
        createDataset(server, runProcConf.outputDataset, nullptr, true /*overwrite*/);
    bool skipEmptyRows = runProcConf.skipEmptyRows;

    auto recordRowInOutputDataset = [&output, &skipEmptyRows] (MatrixNamedRow & row) {
        std::vector<std::tuple<ColumnPath, CellValue, Date> > cols
            = filterEmptyColumns(row);

        if (!skipEmptyRows || cols.size() > 0)
            output->recordRow(row.rowName, cols);

        return true;
        };

    if (!runProcConf.inputData.stm->from) {
        DEBUG_MSG(logger) << "performing transform without FROM statement";
        // query without dataset
        std::vector<MatrixNamedRow> rows = queryWithoutDataset(*runProcConf.inputData.stm, context);
        std::for_each(rows.begin(), rows.end(), recordRowInOutputDataset);
        output->commit();
        return output->getStatus();
    }

    Progress transformProgress;
    std::shared_ptr<Step> bindingStep = transformProgress.steps({
            make_pair("binding", "percentile"),
            make_pair("transforming", "percentile")
    });

    DEBUG_MSG(logger) << "binding FROM statement";
    
    std::mutex progressMutex;
    auto onBindingProgress = [&](const ProgressState & percent) {
        lock_guard<mutex> lock(progressMutex);
        if (bindingStep->value < (float) percent.count / *percent.total) {       
            bindingStep->value = (float) percent.count / *percent.total;     
        }
        return onProgress(jsonEncode(transformProgress));
    };

    auto boundDataset = runProcConf.inputData.stm->from->bind(context, onBindingProgress);

    auto transformingStep = bindingStep->nextStep(1);
    auto onTransformingProgress = [&](const ProgressState & percent) {
        lock_guard<mutex> lock(progressMutex);    
        if (transformingStep->value < (float) percent.count / *percent.total) {       
            transformingStep->value = (float) percent.count / *percent.total;     
        }
        return onProgress(jsonEncode(transformProgress));
    };

    if (!boundDataset.dataset) {
        ExcAssert(boundDataset.table);

        std::function<bool (Path &, ExpressionValue &)> rowAccumulator = 
            [=] (Path & rowName, ExpressionValue &rowValue) -> bool { 

             if (!skipEmptyRows || rowValue.rowLength() > 0)
                output->recordRowExpr(rowName, rowValue);

             return true;
        };

        DEBUG_MSG(logger) << "performing transform without a dataset";

        queryFromStatement(rowAccumulator,
                           *runProcConf.inputData.stm,
                           context,
                           nullptr, /*params*/
                           onTransformingProgress);
    }
    else if (runProcConf.inputData.stm->groupBy.clauses.empty() && aggregators.empty()) {
        Dataset::MultiChunkRecorder recorder
            = output->getChunkRecorder();

        struct ThreadAccum {
            /// Recorder object for this thread that the dataset gives us
            /// to record into the dataset.
            std::unique_ptr<Recorder> threadRecorder;

            /// Special function to allow rapid insertion of fixed set of
            /// atom valued columns.  Only for isIdentitySelect.
            //std::function<void (RowPath rowName,
            //                    Date timestamp,
            //                    CellValue * vals,
            //                    size_t numVals,
            //                    std::vector<std::pair<ColumnPath, CellValue> > extra)>
            //specializedRecorder;

        };

        PerThreadAccumulator<ThreadAccum> accum;

        std::atomic<size_t> chunkNumber(0);
        auto recordRowInOutputDataset
            = [&] (RowPath & rowPath,
                   ExpressionValue & row,
                   std::vector<ExpressionValue> & calc)
            {
                auto & threadAccum = accum.get();
                if (!threadAccum.threadRecorder) {
                    threadAccum.threadRecorder = recorder.newChunk(chunkNumber.fetch_add(1));
                }
                if (skipEmptyRows) {
                    if (row.empty())
                        return true;

                    bool hasNonNull = false;
                    // Also look to see if we have only null elements
                    auto onAtom = [&] (const Path & columnName,
                                       const Path & prefix,
                                       const CellValue & val,
                                       Date ts)
                    {
                        if (!val.empty()) {
                            hasNonNull = true;
                            return false;
                        }
                        return true;
                    };
                    row.forEachAtom(onAtom);
                    if (!hasNonNull)
                        return true;
                }
                // TODO: could optimize slightly by finding rowName == rowName()
                // and copying the existing rowPath in that case
                threadAccum.threadRecorder->recordRowExprDestructive
                    (calc[0].coerceToPath(), std::move(row));
                return true;
            };

        DEBUG_MSG(logger) << "performing dataset transform";
   
        ConvertProgressToJson convertProgressToJson(onProgress);
        if (!BoundSelectQuery(runProcConf.inputData.stm->select,
                              *boundDataset.dataset,
                              boundDataset.asName,
                              runProcConf.inputData.stm->when,
                              *runProcConf.inputData.stm->where,
                              runProcConf.inputData.stm->orderBy,
                              { runProcConf.inputData.stm->rowName })
            .executeExpr({recordRowInOutputDataset, true /*processInParallel*/},
                         runProcConf.inputData.stm->offset,
                         runProcConf.inputData.stm->limit,
                         onTransformingProgress) )
            {
                DEBUG_MSG(logger) << TransformDatasetConfig::name << " procedure was cancelled";
                throw CancellationException(std::string(TransformDatasetConfig::name) +
                                                " procedure was cancelled");
                
            }

        // Finish off the last bits of each thread
        parallelMap(0, accum.threads.size(),
                    [&] (size_t n)
                    {
                        auto & threadAccum = *accum.threads[n];
                        ExcAssert(threadAccum.threadRecorder.get());
                        threadAccum.threadRecorder->finishedChunk();
                    });
    }
    else {
        auto recordRowInOutputDataset
            = [&] (NamedRowValue & row_)
            {
                MatrixNamedRow row = row_.flattenDestructive();
                std::vector<std::tuple<ColumnPath, CellValue, Date> > cols
                    = filterEmptyColumns(row);
                if (!skipEmptyRows || cols.size() > 0)
                    output->recordRow(row.rowName, cols);

                return true;
            };

        aggregators.insert(aggregators.end(), havingaggregators.begin(), havingaggregators.end());
        aggregators.insert(aggregators.end(), orderbyaggregators.begin(), orderbyaggregators.end());
        aggregators.insert(aggregators.end(), namedaggregators.begin(), namedaggregators.end());

        DEBUG_MSG(logger) << "performing dataset transform with group by";

        ConvertProgressToJson convertProgressToJson(onProgress);
        if(!BoundGroupByQuery(runProcConf.inputData.stm->select,
                          *boundDataset.dataset,
                          boundDataset.asName,
                          runProcConf.inputData.stm->when,
                          *runProcConf.inputData.stm->where,
                          runProcConf.inputData.stm->groupBy,
                          aggregators,
                          *runProcConf.inputData.stm->having,
                          *runProcConf.inputData.stm->rowName,
                          runProcConf.inputData.stm->orderBy)
            .execute({recordRowInOutputDataset, false /*processInParallel*/},
                     runProcConf.inputData.stm->offset,
                     runProcConf.inputData.stm->limit,
                     onTransformingProgress).first ) {
            DEBUG_MSG(logger) << TransformDatasetConfig::name << " procedure was cancelled";
            throw CancellationException(std::string(TransformDatasetConfig::name) +
                                            " procedure was cancelled");
            }
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
                    "Apply an SQL expression over a dataset to transform into another dataset",
                    "procedures/TransformDataset.md.html");


} // namespace MLDB

