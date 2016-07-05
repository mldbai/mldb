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

// Simple handlers to enhance the lisibility of the run function.
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
    const int NUM_NOT_NULL_IDX = 3;
    const int NUM_NULL_IDX = 4;
    const int NUM_UNIQUE_IDX = 5;

    bool first;
    BoundTableExpression & boundDataset;
    SummaryStatisticsProcedureConfig & config;
    shared_ptr<Dataset> output;
    Date now;
    std::function<bool (const Json::Value &)> onProgress;

    // Throws if the column is not numeric
    void recordStatsForColumn(const Utf8String & name) {
        int64_t numNotNull = 0;
        auto onRow = [&] (NamedRowValue & row) {
            const auto & cols = row.columns;
            if (JML_UNLIKELY(first)) {
                // Checks on the first row if the expected order is correct
                ExcAssert(std::get<0>(cols[AVG_IDX]).toUtf8String() == "avg");
                ExcAssert(std::get<0>(cols[MAX_IDX]).toUtf8String() == "max");
                ExcAssert(std::get<0>(cols[MIN_IDX]).toUtf8String() == "min");
                ExcAssert(std::get<0>(cols[NUM_NOT_NULL_IDX]).toUtf8String() == "num_not_null");
                ExcAssert(std::get<0>(cols[NUM_NULL_IDX]).toUtf8String() == "num_null");
                ExcAssert(std::get<0>(cols[NUM_UNIQUE_IDX]).toUtf8String() == "num_unique");
                first = false;
            }
            vector<Cell> toRecord;
            toRecord.emplace_back(ColumnName("value_data_type"), "number", now);
            toRecord.emplace_back(ColumnName("value_mean"), std::get<1>(cols[AVG_IDX]).toDouble(), now);
            toRecord.emplace_back(ColumnName("value_max"), std::get<1>(cols[MAX_IDX]).toDouble(), now);
            toRecord.emplace_back(ColumnName("value_min"), std::get<1>(cols[MIN_IDX]).toDouble(), now);
            toRecord.emplace_back(ColumnName("value_num_null"), std::get<1>(cols[NUM_NULL_IDX]).toInt(), now);
            toRecord.emplace_back(ColumnName("value_num_unique"), std::get<1>(cols[NUM_UNIQUE_IDX]).toInt(), now);
            output->recordRow(RowName(name), toRecord);
            numNotNull = std::get<1>(cols[NUM_NOT_NULL_IDX]).toInt();
            return true;
        };

        auto select = SelectExpression::parse(
            "count_distinct(\"" + name + "\") AS num_unique, "
            "min(\"" + name + "\") AS min, "
            "max(\"" + name + "\") AS max, "
            "avg(\"" + name + "\") AS avg, "
            "sum(\"" + name + "\" IS NULL) AS num_null, "
            "sum(\"" + name + "\" IS NOT NULL) AS num_not_null"
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

        select = SelectExpression::parse("count(\"" + name + "\") AS _0, "
                                         "\"" + name + "\" AS _1");
        auto groupBy = TupleExpression::parse("\"" + name + "\"");
        auto orderBy = OrderByExpression::parse("\"" + name + "\"");
        auto where = SqlExpression::parse("\"" + name + "\" IS NOT NULL");
        const int NUM_QUARTILES = 3;
        double quartiles[NUM_QUARTILES];
        double quartilesThreshold[NUM_QUARTILES] = {numNotNull * 0.25,
                                                    numNotNull * 0.5,
                                                    numNotNull * 0.75};
        int idx = 0;
        int64_t count = 0;
        auto onRow2 = [&] (NamedRowValue & row) {
            // TODO check order
            const auto & cols = row.columns;
            count += std::get<1>(cols[0]).toInt();
            while (idx < NUM_QUARTILES && quartilesThreshold[idx] < count) {
                auto val = std::get<1>(cols[1]);
                quartiles[idx] = val.toDouble();
                ++idx;
            }
            return true;
        };
        BoundGroupByQuery(select,
                        *boundDataset.dataset,
                        boundDataset.asName,
                        config.inputData.stm->when,
                        *where,
                        groupBy,
                        aggregators,
                        *config.inputData.stm->having,
                        *config.inputData.stm->rowName,
                        orderBy)
            .execute({onRow2, false /*processInParallel*/},
                    0, // offset
                    -1, // limit
                    onProgress);
        ExcAssert(count == numNotNull);
        ExcAssert(idx == NUM_QUARTILES);
        vector<Cell> toRecord;
        toRecord.emplace_back(ColumnName("value_1st_quartile"), quartiles[0], now);
        toRecord.emplace_back(ColumnName("value_median"), quartiles[1], now);
        toRecord.emplace_back(ColumnName("value_3rd_quartile"), quartiles[2], now);
        output->recordRow(RowName(name), toRecord);
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
    std::function<bool (const Json::Value &)> onProgress;

    void recordStatsForColumn(const Utf8String & name) {
        auto onRow = [&] (NamedRowValue & row) {
            const auto & cols = row.columns;
            if (JML_UNLIKELY(first)) {
                // Checks on the first row if the expected order is correct
                ExcAssert(std::get<0>(cols[NUM_NULL_IDX]).toUtf8String() == "num_null");
                ExcAssert(std::get<0>(cols[NUM_UNIQUE_IDX]).toUtf8String() == "num_unique");
                first = false;
            }
            vector<Cell> toRecord;
            toRecord.emplace_back(ColumnName("value_data_type"), "categorical", now);
            toRecord.emplace_back(ColumnName("value_num_null"), std::get<1>(cols[NUM_NULL_IDX]).toInt(), now);
            toRecord.emplace_back(ColumnName("value_num_unique"), std::get<1>(cols[NUM_UNIQUE_IDX]).toInt(), now);
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

        select = SelectExpression::parse("count(\"" + name + "\") AS c");
        auto groupBy = TupleExpression::parse("\"" + name + "\"");
        auto orderBy = OrderByExpression::parse("c DESC");

        auto onRow2 = [&] (NamedRowValue & row) {
            const auto & cols = row.columns;
            vector<Cell> toRecord;
            auto rowName = row.rowName.toSimpleName();
            toRecord.emplace_back(
                ColumnName("value_most_frequent_items." + rowName),
                std::get<1>(cols[0]).toInt(),
                now);
            // TODO confirm we really want it? Can create a ton of columns
            //output->recordRow(RowName(name), toRecord);
            return true;
        };
        BoundGroupByQuery(select,
                        *boundDataset.dataset,
                        boundDataset.asName,
                        config.inputData.stm->when,
                        *config.inputData.stm->where,
                        groupBy,
                        aggregators,
                        *config.inputData.stm->having,
                        *config.inputData.stm->rowName,
                        orderBy)
            .execute({onRow2, false /*processInParallel*/},
                    0, // offset
                    1, // limit
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
    int num = 0;
    float denum = 0;
    auto onProgress2 = [&](const Json::Value & progress) {
        Json::Value v;
        v["percent"] = num / denum;
        return onProgress(progress);
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
                              onProgress2); // TODO onProgress2
        CategoricalRowHandler crh(boundDataset, runProcConf, output, now,
                                  onProgress); // TODO onProgress2
        using std::placeholders::_1;

        SqlExpressionDatasetScope datasetContext(boundDataset);
        denum = bsq.getSelectOutputInfo()->allColumnNames().size() / 100.0;
        for (const auto & c: bsq.getSelectOutputInfo()->allColumnNames()) {
            ++ num;
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
