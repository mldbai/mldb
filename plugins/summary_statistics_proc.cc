/**
 * summary_statistics_proc.cc
 * Mich, 2016-06-30
 * This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
 **/
#include "summary_statistics_proc.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/server/mldb_server.h"
#include "mldb/sql/sql_expression.h"
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
#include "mldb/types/date.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/plugins/sql_config_validator.h"
#include "mldb/utils/log.h"
#include "progress.h"
#include <memory>


using namespace std;


namespace Datacratic {
namespace MLDB {

SummaryStatisticsProcedureConfig::
SummaryStatisticsProcedureConfig()
{
    outputDataset.withType("sparse.mutable");
}

DEFINE_STRUCTURE_DESCRIPTION(SummaryStatisticsProcedureConfig);

SummaryStatisticsProcedureConfigDescription::
SummaryStatisticsProcedureConfigDescription()
{
    addField("inputData", &SummaryStatisticsProcedureConfig::inputData,
             "An SQL statement to select the input data.");
    addField("outputDataset", &SummaryStatisticsProcedureConfig::outputDataset,
             "Output dataset configuration. This may refer either to an "
             "existing dataset, or a fully specified but non-existing dataset "
             "which will be created by the procedure.",
             PolyConfigT<Dataset>().withType("sparse.mutable"));
    addParent<ProcedureConfig>();

    onPostValidate = [&] (SummaryStatisticsProcedureConfig * cfg,
                          JsonParsingContext & context)
    {
        MustContainFrom()(cfg->inputData, SummaryStatisticsProcedureConfig::name);
    };
}

SummaryStatisticsProcedure::
SummaryStatisticsProcedure(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    procedureConfig = config.params.convert<SummaryStatisticsProcedureConfig>();
}

struct NumericalStats {
    uint64_t numUnique;
    uint64_t numNull;
    double min;
    double max;
    double mean;
    double std;
    double q1;
    double median;
    double q3;
};

RunOutput
SummaryStatisticsProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(procedureConfig, run);
    Progress bucketizeProgress;
    std::shared_ptr<Step> iterationStep = bucketizeProgress.steps({
        make_pair("iterating", "percentile"),
    });

    SqlExpressionMldbScope context(server);

    auto boundDataset = runProcConf.inputData.stm->from->bind(context);

    vector<shared_ptr<SqlExpression> > calc;

    for (auto & c: runProcConf.inputData.stm->orderBy.clauses) {
        auto whenClause = std::make_shared<FunctionCallExpression>
            ("" /* tableName */, "latest_timestamp",
             vector<shared_ptr<SqlExpression> >(1, c.first));
        calc.emplace_back(whenClause);
    }

    BoundSelectQuery bsq(runProcConf.inputData.stm->select,
                         *boundDataset.dataset,
                         boundDataset.asName,
                         runProcConf.inputData.stm->when,
                         *runProcConf.inputData.stm->where,
                         runProcConf.inputData.stm->orderBy,
                         calc);
    mutex progressMutex;
    auto onProgress2 = [&](const Json::Value & progress) {
        auto itProgress = jsonDecode<IterationProgress>(progress);
        lock_guard<mutex> lock(progressMutex);
        if (iterationStep->value > itProgress.percent) {
            iterationStep->value = itProgress.percent;
        }
        return onProgress(jsonEncode(bucketizeProgress));
    };

    map<Utf8String, NumericalStats> statsMap;
    if (runProcConf.inputData.stm->select.clauses.size() == 1
        && runProcConf.inputData.stm->select.clauses[0]->isWildcard())
    {
        vector<Utf8String> numericalColumns;
        vector<Utf8String> categoricalColumns;
        bool first = true;
        const Utf8String * rowName;
        const int AVG_IDX = 0;
        const int MAX_IDX = 1;
        const int MIN_IDX = 2;
        const int NUM_NULL_IDX = 3;
        const int NUM_UNIQUE_IDX = 4;

        auto onRow = [&] (NamedRowValue & row)
        {
            auto & stats = statsMap[*rowName];
            const auto & cols = row.columns;
            if (first) {
                ExcAssert(std::get<0>(cols[AVG_IDX]).toUtf8String() == "avg");
                ExcAssert(std::get<0>(cols[MAX_IDX]).toUtf8String() == "max");
                ExcAssert(std::get<0>(cols[MIN_IDX]).toUtf8String() == "min");
                ExcAssert(std::get<0>(cols[NUM_NULL_IDX]).toUtf8String() == "num_null");
                ExcAssert(std::get<0>(cols[NUM_UNIQUE_IDX]).toUtf8String() == "num_unique");
                first = false;
            }
            stats.mean      = std::get<1>(cols[AVG_IDX]).toDouble();
            stats.max       = std::get<1>(cols[MAX_IDX]).toDouble();
            stats.min       = std::get<1>(cols[MIN_IDX]).toDouble();
            stats.numNull   = std::get<1>(cols[NUM_NULL_IDX]).toInt();
            stats.numUnique = std::get<1>(cols[NUM_UNIQUE_IDX]).toInt();
            return true;
        };

        SqlExpressionDatasetScope datasetContext(boundDataset);
        for (const auto & c: bsq.getSelectOutputInfo()->allColumnNames()) {
            const auto & name = c.toUtf8String();
            rowName = &name;
            auto select = SelectExpression::parse(
                "count_distinct(\"" + name + "\") AS num_unique, "
                "min(\"" + name + "\") AS min, "
                "max(\"" + name + "\") AS max, "
                "avg(\"" + name + "\") AS avg, "
                "sum(\"" + name + "\" IS NULL) AS num_null"
            );
            try {
                std::vector<std::shared_ptr<SqlExpression>> aggregators =
                    select.findAggregators(!runProcConf.inputData.stm->groupBy.clauses.empty());
                BoundGroupByQuery(select,
                                *boundDataset.dataset,
                                boundDataset.asName,
                                runProcConf.inputData.stm->when,
                                *runProcConf.inputData.stm->where,
                                runProcConf.inputData.stm->groupBy,
                                aggregators,
                                *runProcConf.inputData.stm->having,
                                *runProcConf.inputData.stm->rowName,
                                runProcConf.inputData.stm->orderBy)
                    .execute({onRow, false /*processInParallel*/},
                            0, // offset
                            -1, // limit
                            onProgress2);
                numericalColumns.emplace_back(c.toUtf8String());
            }
            catch (const ML::Exception & exc) {
                // catch ML::Exception is painful, not specific enough
                categoricalColumns.emplace_back(c.toUtf8String());
            }
        }
    }
    else {
        throw ML::Exception("Unimplemented for non wildcard select");
    }

    auto output = createDataset(server, runProcConf.outputDataset,
                                nullptr, true /*overwrite*/);

    typedef tuple<ColumnName, CellValue, Date> Cell;
    Date now = Date::now();
    for (const auto & it: statsMap) {
        vector<Cell> row;
        const auto & numStats = it.second;
        row.emplace_back(ColumnName("data_type"), "numerical", now);
        row.emplace_back(ColumnName("num_unique"), numStats.numUnique, now);
        row.emplace_back(ColumnName("min"), numStats.min, now);
        row.emplace_back(ColumnName("max"), numStats.max, now);
        row.emplace_back(ColumnName("mean"), numStats.min, now);
        row.emplace_back(ColumnName("num_null"), numStats.numNull, now);

        row.emplace_back(ColumnName("std"), numStats.std, now);
        row.emplace_back(ColumnName("quartile1"), numStats.q1, now);
        row.emplace_back(ColumnName("median"), numStats.median, now);
        row.emplace_back(ColumnName("quartile3"), numStats.q3, now);
        output->recordRow(RowName(it.first), row);
    }
    output->commit();
    return output->getStatus();
}

Any
SummaryStatisticsProcedure::
getStatus() const
{
    return Any();
}

static RegisterProcedureType<SummaryStatisticsProcedure, SummaryStatisticsProcedureConfig>
regSummaryStatisticsProcedure(
    builtinPackage(),
    "Creates a dataset with summary statistics for each columns of an input dataset",
    "procedures/SummaryStatisticsProcedure.md.html",
    nullptr /* static route */,
    { MldbEntity::INTERNAL_ENTITY });


} // namespace MLDB
} // namespace Datacratic
