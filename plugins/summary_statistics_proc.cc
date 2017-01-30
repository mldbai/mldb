/**
 * summary_statistics_proc.cc
 * Mich, 2016-06-30
 * This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
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
#include "mldb/utils/progress.h"
#include <memory>


using namespace std;

namespace MLDB {

SummaryStatisticsProcedureConfig::
SummaryStatisticsProcedureConfig() : gotWildcard(false)
{
    outputDataset.withType("sparse.mutable");
}

DEFINE_STRUCTURE_DESCRIPTION(SummaryStatisticsProcedureConfig);

SummaryStatisticsProcedureConfigDescription::
SummaryStatisticsProcedureConfigDescription()
{
    addField("inputData", &SummaryStatisticsProcedureConfig::inputData,
             "An SQL statement to select the input data. The query must not "
             "contain GROUP BY or HAVING clauses and, unlike most select "
             "expressions, this one can only select whole columns, not "
             "expressions involving columns. So X will work, but not X + 1. "
             "If you need derived values in the query, create a dataset with "
             "the derived columns as a previous step and use a query on that "
             "dataset instead.");
    addField("outputDataset", &SummaryStatisticsProcedureConfig::outputDataset,
             GENERIC_OUTPUT_DS_DESC,
             PolyConfigT<Dataset>().withType("sparse.mutable"));
    addParent<ProcedureConfig>();

    onPostValidate = [&] (SummaryStatisticsProcedureConfig * cfg,
                          JsonParsingContext & context)
    {
        auto logger = MLDB::getMldbLog<SummaryStatisticsProcedure>();
        MustContainFrom()(cfg->inputData, SummaryStatisticsProcedureConfig::name);
        NoGroupByHaving()(cfg->inputData, SummaryStatisticsProcedureConfig::name);

        for (const auto & clause: cfg->inputData.stm->select.clauses) {
            if (clause->isWildcard()) {
                cfg->gotWildcard = true;
                continue;
            }
            auto expr = dynamic_cast<NamedColumnExpression *>(clause.get());
            if (expr == nullptr) {
                DEBUG_MSG(logger) << "Failed to cast " << clause->surface;
                throw MLDB::Exception("%s is not a supported SELECT value "
                                    "expression for summary.statistics",
                                    clause->surface.rawData());
            }
            if (expr->alias.empty()) {
                DEBUG_MSG(logger) << "Empty alias " << clause->surface;
                throw MLDB::Exception("%s is not a supported SELECT value "
                                    "expression for summary.statistics",
                                    clause->surface.rawData());
            }
            try {
                SelectExpression::parse(
                    "sum(" + expr->getChildren()[0]->surface + ") AS "
                    + expr->alias.toSimpleName());
            }
            catch (const MLDB::Exception & exc) {
                DEBUG_MSG(logger) << "Failed to parse within sum "
                                << clause->surface;
                throw MLDB::Exception("%s is not a supported SELECT value "
                                    "expression for summary.statistics",
                                    clause->surface.rawData());
            }
        }
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

typedef tuple<ColumnPath, CellValue, Date> Cell;

template <typename T, int size>
struct MostFrequents {
    static_assert(size > 0, "MostFrequents size must be greather than 0");
    std::pair<int64_t, T> top[size];
    int currSize;
    int lowerIdx;

    MostFrequents() : currSize(0), lowerIdx(0) {}

    void addItem(std::pair<int64_t, T> item) {
        // For the size first items
        if (currSize < size) {
            top[currSize] = item;
            if (currSize > 0 && item < top[lowerIdx]) {
                lowerIdx = currSize;
            }
            ++ currSize;
            return;
        }

        // For the rest
        if (item > top[lowerIdx]) {
            top[lowerIdx] = item;

            // find new lower
            lowerIdx = 0;
            for (int i = 1; i < size; ++ i) {
                if (top[i] < top[lowerIdx]) {
                    lowerIdx = i;
                }
            }
        }
    }
};

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
    const int STDDEV_IDX = 6;

    bool first;
    BoundTableExpression & boundDataset;
    SummaryStatisticsProcedureConfig & config;
    shared_ptr<Dataset> output;
    Date now;
    std::function<bool (const Json::Value &)> onProgress;

    // Returns false if the column failed to be treated as numeric
    bool recordStatsForColumn(const Utf8String & name, const Path & rowName) {

        int64_t numNotNull = 0;
        bool isNumeric = false;
        ColumnPath value("value");
        auto onRow = [&] (NamedRowValue & row) {

            // If the data is categorical, we don't even reach this point

            const auto & cols = row.columns;
            if (MLDB_UNLIKELY(first)) {
                // Checks on the first row if the expected order is correct
                ExcAssert(std::get<0>(cols[AVG_IDX]).toUtf8String() == "avg");
                ExcAssert(std::get<0>(cols[MAX_IDX]).toUtf8String() == "max");
                ExcAssert(std::get<0>(cols[MIN_IDX]).toUtf8String() == "min");
                ExcAssert(std::get<0>(cols[NUM_NOT_NULL_IDX]).toUtf8String() == "num_not_null");
                ExcAssert(std::get<0>(cols[NUM_NULL_IDX]).toUtf8String() == "num_null");
                ExcAssert(std::get<0>(cols[NUM_UNIQUE_IDX]).toUtf8String() == "num_unique");
                ExcAssert(std::get<0>(cols[STDDEV_IDX]).toUtf8String() == "stddev");

            }

            vector<Cell> toRecord;
            if (!std::get<1>(cols[MAX_IDX]).isNumber()) {
                // If the column is empty, we reach this point (max is null)
                auto theThing = std::get<1>(cols[MAX_IDX]);
                return false;
            }
            isNumeric = true;
            toRecord.emplace_back(value + "avg", std::get<1>(cols[AVG_IDX]).toDouble(), now);
            toRecord.emplace_back(value + "max", std::get<1>(cols[MAX_IDX]).toDouble(), now);
            toRecord.emplace_back(value + "min", std::get<1>(cols[MIN_IDX]).toDouble(), now);
            toRecord.emplace_back(value + "num_null", std::get<1>(cols[NUM_NULL_IDX]).toInt(), now);
            toRecord.emplace_back(value + "num_unique", std::get<1>(cols[NUM_UNIQUE_IDX]).toInt(), now);
            toRecord.emplace_back(value + "stddev", std::get<1>(cols[STDDEV_IDX]).toDouble(), now);
            toRecord.emplace_back(value + "data_type", "number", now);
            output->recordRow(rowName, toRecord);
            numNotNull = std::get<1>(cols[NUM_NOT_NULL_IDX]).toInt();

            return true;
        };

        auto select = SelectExpression::parse(
            "count_distinct(" + name + ") AS num_unique, "
            "min(" + name + ") AS min, "
            "max(" + name + ") AS max, "
            "avg(" + name + ") AS avg, "
            "sum(" + name + " IS NULL) AS num_null, "
            "sum(" + name + " IS NOT NULL) AS num_not_null, "
            "stddev(" + name + ") AS stddev"
        );
        vector<shared_ptr<SqlExpression>> aggregators =
            select.findAggregators(!config.inputData.stm->groupBy.clauses.empty());

        try {
            // The first query is used to determine whether it's numeric or
            // not, hence the special try/catch/failedProcessingQuery handling.
            ConvertProgressToJson convertProgressToJson(onProgress);
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
                         convertProgressToJson);
        }
        catch (const MLDB::Exception & exc) {
            if (!isNumeric) {
                // Categorical, the query doesn't work
                return false;
            }
            throw;
        }
        if (!isNumeric) {
            // empty col
            return false;
        }

        select = SelectExpression::parse("count(" + name + ") AS _0, "
                                         + name + " AS _1");
        auto groupBy = TupleExpression::parse(name);
        auto orderBy = OrderByExpression::parse(name);
        auto where = SqlExpression::parse(name + " IS NOT NULL");
        const int NUM_QUARTILES = 3;
        double quartiles[NUM_QUARTILES];
        double quartilesThreshold[NUM_QUARTILES] = {numNotNull * 0.25,
                                                    numNotNull * 0.5,
                                                    numNotNull * 0.75};
        int idx = 0;
        int64_t count = 0;
        MostFrequents<double, 10> mostFrequents; // Keep top 10
        auto onRow2 = [&] (NamedRowValue & row) {
            const auto & cols = row.columns;
            if (MLDB_UNLIKELY(first)) {
                ExcAssert(std::get<0>(cols[0]).toUtf8String() == "_0");
                ExcAssert(std::get<0>(cols[1]).toUtf8String() == "_1");
                first = false;
            }
            int64_t currentCount = std::get<1>(cols[0]).toInt();
            mostFrequents.addItem(make_pair(currentCount,
                                            std::get<1>(cols[1]).toDouble()));
            count += currentCount;
            while (idx < NUM_QUARTILES && quartilesThreshold[idx] < count) {
                quartiles[idx] = std::get<1>(cols[1]).toDouble();
                ++idx;
            }
            return true;
        };
        ConvertProgressToJson convertProgressToJson(onProgress);
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
                     convertProgressToJson);
        ExcAssert(count == numNotNull);
        ExcAssert(idx == NUM_QUARTILES);
        vector<Cell> toRecord;
        toRecord.emplace_back(value + "1st_quartile", quartiles[0], now);
        toRecord.emplace_back(value + "median", quartiles[1], now);
        toRecord.emplace_back(value + "3rd_quartile", quartiles[2], now);
        for (int i = 0; i < mostFrequents.currSize; ++ i) {
            toRecord.emplace_back(
                // CellValue::to_string returns "1" instead of "1.00000"
                value + "most_frequent_items" + to_string(CellValue(mostFrequents.top[i].second)),
                mostFrequents.top[i].first, now);
        }
        output->recordRow(rowName, toRecord);
        return true;
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

    void recordStatsForColumn(const Utf8String & name, const Path & rowName) {
        ColumnPath value("value");
        auto onRow = [&] (NamedRowValue & row) {
            const auto & cols = row.columns;
            if (MLDB_UNLIKELY(first)) {
                // Checks on the first row if the expected order is correct
                ExcAssert(std::get<0>(cols[NUM_NULL_IDX]).toUtf8String() == "num_null");
                ExcAssert(std::get<0>(cols[NUM_UNIQUE_IDX]).toUtf8String() == "num_unique");
            }
            vector<Cell> toRecord;
            toRecord.emplace_back(value + "data_type", "categorical", now);
            toRecord.emplace_back(value + "num_null", std::get<1>(cols[NUM_NULL_IDX]).toInt(), now);
            toRecord.emplace_back(value + "num_unique", std::get<1>(cols[NUM_UNIQUE_IDX]).toInt(), now);
            output->recordRow(rowName, toRecord);
            return true;
        };

        auto select = SelectExpression::parse(
            "count_distinct(" + name + ") AS num_unique, "
            "sum(" + name + " IS NULL) AS num_null"
        );
        vector<shared_ptr<SqlExpression>> aggregators =
            select.findAggregators(!config.inputData.stm->groupBy.clauses.empty());

        ConvertProgressToJson convertProgressToJson(onProgress);
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
                    convertProgressToJson);

        select = SelectExpression::parse("count(" + name + ") AS _0, "
                                         + name + " AS _1");
        auto groupBy = TupleExpression::parse(name);
        auto orderBy = OrderByExpression::parse("_0 DESC");

        MostFrequents<Utf8String, 10> mostFrequents; // Keep top 10
        auto onRow2 = [&] (NamedRowValue & row) {
            const auto & cols = row.columns;
            if (MLDB_UNLIKELY(first)) {
                ExcAssert(std::get<0>(cols[0]).toUtf8String() == "_0");
                ExcAssert(std::get<0>(cols[1]).toUtf8String() == "_1");
            }
            if (MLDB_UNLIKELY(std::get<1>(cols[1]).empty())) {
                // skipp null
                return true;
            }
            mostFrequents.addItem(make_pair(std::get<1>(cols[0]).toInt(),
                                            std::get<1>(cols[1]).toString()));
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
                    -1, // limit
                    convertProgressToJson);
        vector<Cell> toRecord;
        for (int i = 0; i < mostFrequents.currSize; ++ i) {
            toRecord.emplace_back(
                value + "most_frequent_items" + mostFrequents.top[i].second,
                mostFrequents.top[i].first,
                now);
        }
        output->recordRow(rowName, toRecord);
        first = false;
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

    ConvertProgressToJson convertProgressToJson(onProgress);
    auto boundDataset = runProcConf.inputData.stm->from->bind(context, convertProgressToJson);

    vector<shared_ptr<SqlExpression> > calc;

    for (auto & c: runProcConf.inputData.stm->orderBy.clauses) {
        auto whenClause = std::make_shared<FunctionCallExpression>
            ("" /* tableName */, "latest_timestamp",
             vector<shared_ptr<SqlExpression> >(1, c.first));
        calc.emplace_back(whenClause);
    }

    int num = 0;
    float denum = 0;
    {
        BoundSelectQuery bsq(runProcConf.inputData.stm->select,
                             *boundDataset.dataset,
                             boundDataset.asName,
                             runProcConf.inputData.stm->when,
                             *runProcConf.inputData.stm->where,
                             runProcConf.inputData.stm->orderBy,
                             calc);
        denum = bsq.getSelectOutputInfo()->allColumnNames().size();
    }
    auto onProgress2 = [&](const Json::Value & progress) {
        Json::Value v;
        v["percent"] = num / denum;
        return onProgress(progress);
    };

    Date now = Date::now();
    auto output = createDataset(server, runProcConf.outputDataset,
                                nullptr, true /*overwrite*/);


    NumericRowHandler nrh(boundDataset, runProcConf, output, now, onProgress2);
    CategoricalRowHandler crh(boundDataset, runProcConf, output, now,
                                onProgress);
    auto record = [&] (const Utf8String & expr, const Path & alias) {
        const Utf8String name = "\"" + expr + "\"";
        if (!nrh.recordStatsForColumn(name, alias)) {
            crh.recordStatsForColumn(name, alias);
        }
    };

    for (const auto & clause: procedureConfig.inputData.stm->select.clauses) {
        Utf8String surface;
        Utf8String alias;
        if (clause->isWildcard()) {
            BoundSelectQuery bsq(SelectExpression::parse("*"),
                                *boundDataset.dataset,
                                boundDataset.asName,
                                runProcConf.inputData.stm->when,
                                *runProcConf.inputData.stm->where,
                                runProcConf.inputData.stm->orderBy,
                                calc);
            for (const auto & colName: bsq.getSelectOutputInfo()->allColumnNames()) {
                ++ num;
                record(colName.toSimpleName(), colName);
            }
            continue;
        }
        ++ num;
        // static_cast -> validated already from onPostValidate
        auto expr = static_cast<NamedColumnExpression *>(clause.get());
        record(expr->getChildren()[0]->surface, expr->alias);
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
    "procedures/SummaryStatisticsProcedure.md.html");


} // namespace MLDB

