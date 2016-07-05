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

typedef tuple<ColumnName, CellValue, Date> Cell;

// Simple handlers to reduce the size of the code in the run function.
struct NumericRowHandler {
    NumericRowHandler() = delete;
    NumericRowHandler(BoundTableExpression & boundDataset,
                      SummaryStatisticsProcedureConfig & config,
                      shared_ptr<Dataset> output,
                      Date now,
                      const std::function<bool (const Json::Value &)> & onProgress)
        : first(true), boundDataset(boundDataset), config(config),
          output(output), now(now), onProgress(onProgress) {}

    const int AVG_IDX = 0;
    const int MAX_IDX = 1;
    const int MIN_IDX = 2;
    const int NUM_NULL_IDX = 3;
    const int NUM_UNIQUE_IDX = 4;

    bool first;
    BoundTableExpression & boundDataset;
    SummaryStatisticsProcedureConfig & config;
    shared_ptr<Dataset> output;
    Date now;
    const std::function<bool (const Json::Value &)> & onProgress;

    // Throws if the column is not numeric
    void recordStatsForColumn(const Utf8String & name) {
        auto onRow = [&] (NamedRowValue & row) {
            const auto & cols = row.columns;
            if (JML_UNLIKELY(first)) {
                ExcAssert(std::get<0>(cols[AVG_IDX]).toUtf8String() == "avg");
                ExcAssert(std::get<0>(cols[MAX_IDX]).toUtf8String() == "max");
                ExcAssert(std::get<0>(cols[MIN_IDX]).toUtf8String() == "min");
                ExcAssert(std::get<0>(cols[NUM_NULL_IDX]).toUtf8String() == "num_null");
                ExcAssert(std::get<0>(cols[NUM_UNIQUE_IDX]).toUtf8String() == "num_unique");
                first = false;
            }
            vector<Cell> toRecord;
            toRecord.emplace_back(ColumnName("data_type"), "number", now);
            toRecord.emplace_back(ColumnName("mean"), std::get<1>(cols[AVG_IDX]).toDouble(), now);
            toRecord.emplace_back(ColumnName("max"), std::get<1>(cols[MAX_IDX]).toDouble(), now);
            toRecord.emplace_back(ColumnName("min"), std::get<1>(cols[MIN_IDX]).toDouble(), now);
            toRecord.emplace_back(ColumnName("num_null"), std::get<1>(cols[NUM_NULL_IDX]).toDouble(), now);
            toRecord.emplace_back(ColumnName("num_unique"), std::get<1>(cols[NUM_UNIQUE_IDX]).toDouble(), now);
            output->recordRow(RowName(name), toRecord);
            return true;
        };

        auto select = SelectExpression::parse(
            "count_distinct(\"" + name + "\") AS num_unique, "
            "min(\"" + name + "\") AS min, "
            "max(\"" + name + "\") AS max, "
            "avg(\"" + name + "\") AS avg, "
            "sum(\"" + name + "\" IS NULL) AS num_null"
        );
        vector<shared_ptr<SqlExpression>> aggregators =
            select.findAggregators(!config.inputData.stm->groupBy.clauses.empty());

        BoundGroupByQuery(select,
                        *boundDataset.dataset,
                        boundDataset.asName,
                        config.inputData.stm->when,
                        *config.inputData.stm->where,
                        config.inputData.stm->groupBy,
                        aggregators,
                        *config.inputData.stm->having,
                        *config.inputData.stm->rowName,
                        config.inputData.stm->orderBy)
            .execute({onRow, false /*processInParallel*/},
                    0, // offset
                    -1, // limit
                    onProgress);
    }

};

struct CategoricalRowHandler {
    CategoricalRowHandler() = delete;
    CategoricalRowHandler(BoundTableExpression & boundDataset,
                          SummaryStatisticsProcedureConfig & config,
                          shared_ptr<Dataset> output,
                          Date now,
                          const std::function<bool (const Json::Value &)> & onProgress)
        : first(true), boundDataset(boundDataset), config(config),
          output(output), now(now), onProgress(onProgress) {}

    const int NUM_NULL_IDX = 0;
    const int NUM_UNIQUE_IDX = 1;

    bool first;
    BoundTableExpression & boundDataset;
    SummaryStatisticsProcedureConfig & config;
    shared_ptr<Dataset> output;
    Date now;
    const std::function<bool (const Json::Value &)> & onProgress;

    void recordStatsForColumn(const Utf8String & name) {
        auto onRow = [&] (NamedRowValue & row) {
            const auto & cols = row.columns;
            if (JML_UNLIKELY(first)) {
                ExcAssert(std::get<0>(cols[NUM_NULL_IDX]).toUtf8String() == "num_null");
                ExcAssert(std::get<0>(cols[NUM_UNIQUE_IDX]).toUtf8String() == "num_unique");
                first = false;
            }
            vector<Cell> toRecord;
            toRecord.emplace_back(ColumnName("data_type"), "categorical", now);
            toRecord.emplace_back(ColumnName("num_null"), std::get<1>(cols[NUM_NULL_IDX]).toDouble(), now);
            toRecord.emplace_back(ColumnName("num_unique"), std::get<1>(cols[NUM_UNIQUE_IDX]).toDouble(), now);
            output->recordRow(RowName(name), toRecord);
            return true;
        };

        auto select = SelectExpression::parse(
            "count_distinct(\"" + name + "\") AS num_unique, "
            "sum(\"" + name + "\" IS NULL) AS num_null"
        );
        vector<shared_ptr<SqlExpression>> aggregators =
            select.findAggregators(!config.inputData.stm->groupBy.clauses.empty());

        BoundGroupByQuery(select,
                        *boundDataset.dataset,
                        boundDataset.asName,
                        config.inputData.stm->when,
                        *config.inputData.stm->where,
                        config.inputData.stm->groupBy,
                        aggregators,
                        *config.inputData.stm->having,
                        *config.inputData.stm->rowName,
                        config.inputData.stm->orderBy)
            .execute({onRow, false /*processInParallel*/},
                    0, // offset
                    -1, // limit
                    onProgress);
    }
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

    Date now = Date::now();
    auto output = createDataset(server, runProcConf.outputDataset,
                                nullptr, true /*overwrite*/);

    if (runProcConf.inputData.stm->select.clauses.size() == 1
        && runProcConf.inputData.stm->select.clauses[0]->isWildcard())
    {
        vector<Utf8String> numericalColumns;
        vector<Utf8String> categoricalColumns;
        NumericRowHandler nrh(boundDataset, runProcConf, output, now,
                              onProgress); // TODO onProgress2
        CategoricalRowHandler crh(boundDataset, runProcConf, output, now,
                                  onProgress); // TODO onProgress2
        using std::placeholders::_1;

        SqlExpressionDatasetScope datasetContext(boundDataset);
        for (const auto & c: bsq.getSelectOutputInfo()->allColumnNames()) {
            const auto & name = c.toUtf8String();
            try {
                nrh.recordStatsForColumn(name);
                numericalColumns.emplace_back(c.toUtf8String());
            }
            catch (const ML::Exception & exc) {
                // TODO ? catch ML::Exception is painful, not specific enough
                crh.recordStatsForColumn(name);
                categoricalColumns.emplace_back(c.toUtf8String());
            }
        }
    }
    else {
        throw ML::Exception("Unimplemented for non wildcard select");
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
