/**
 * dataset_stats_procedure.cc
 * Mich, 2016-05-18
 * This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
 **/

#include <memory>

#include "dataset_stats_procedure.h"

#include "mldb/compiler/compiler.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/date.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/bound_queries.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/server/dataset_context.h"
#include "mldb/pro/pro_plugin.h"

using namespace std;



namespace MLDB {

DatasetStatsProcedureConfig::
DatasetStatsProcedureConfig()
{
    outputDataset.withType("tabular");
}

DEFINE_STRUCTURE_DESCRIPTION(DatasetStatsProcedureConfig);

DatasetStatsProcedureConfigDescription::
DatasetStatsProcedureConfigDescription()
{
    addField("inputData", &DatasetStatsProcedureConfig::inputData,
             "An SQL statement to select the input data.");
    addField("outputDataset", &DatasetStatsProcedureConfig::outputDataset,
             "Output dataset configuration. This may refer either to an "
             "existing dataset, or a fully specified but non-existing dataset "
             "which will be created by the procedure.",
             PolyConfigT<Dataset>().withType("tabular"));
    addParent<ProcedureConfig>();
}

DatasetStatsProcedure::
DatasetStatsProcedure(
    MldbServer * owner,
    PolyConfig config,
    const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    procedureConfig = config.params.convert<DatasetStatsProcedureConfig>();
}

RunOutput
DatasetStatsProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(procedureConfig, run);

    SqlExpressionMldbScope context(server);
    ConvertProgressToJson convertProgressToJson(onProgress);
    auto boundDataset = runProcConf.inputData.stm->from->bind(context, convertProgressToJson);

    vector<shared_ptr<SqlExpression> > calc;
    // We calculate an expression with the timestamp of the order by
    // clause.  First, we need to calculate each of the order by clauses
    for (auto & c: runProcConf.inputData.stm->orderBy.clauses) {
        auto whenClause = std::make_shared<FunctionCallExpression>
            ("" /* tableName */, "latestTimestamp",
             vector<shared_ptr<SqlExpression> >(1, c.first));
        calc.emplace_back(whenClause);
    }

    auto output = createDataset(server, runProcConf.outputDataset,
                                nullptr, true /*overwrite*/);

    atomic<ssize_t> rowsCount(0);
    atomic<ssize_t> datapointsCount(0);
    atomic<ssize_t> cellsCount(0);
    PerThreadAccumulator<unordered_set<Date>> timestampsAcc;

    auto earliestCtor = [] ()
    {
        auto earl = new Date();
        *earl = Date::positiveInfinity();
        return earl;
    };
    auto latestCtor = [] ()
    {
        auto earl = new Date();
        *earl = Date::negativeInfinity();
        return earl;
    };
    PerThreadAccumulator<Date> earliestAcc(earliestCtor);
    PerThreadAccumulator<Date> latestAcc(latestCtor);

    auto analyseRow = [&] (NamedRowValue & row,
                           const vector<ExpressionValue> & calc)
    {
        ++rowsCount;
        auto & earliest = earliestAcc.get();
        auto & latest = latestAcc.get();
        auto & timestamps = timestampsAcc.get();
        ssize_t localDatapointsCount(0);
        ssize_t localCellsCount(0);

        auto onAtom = [&] (const Path & suffix,
                           const Path & prefix,
                           const CellValue & value,
                           Date ts)
        {
            ++ localDatapointsCount;
            timestamps.insert(ts);
            if (MLDB_UNLIKELY(ts > latest)) {
                latest = ts;
            }
            if (MLDB_UNLIKELY(ts < earliest)) {
                earliest = ts;
            }
            timestampsAcc.get().insert(ts);
            return true;
        };

        for (const auto & col: row.columns) {
            ++ localCellsCount;
            std::get<1>(col).forEachAtom(onAtom);
        }
        cellsCount += localCellsCount;
        datapointsCount += localDatapointsCount;
        return true;
    };

    OrderByExpression orderBy;
    BoundSelectQuery bsq(runProcConf.inputData.stm->select,
                         *boundDataset.dataset,
                         boundDataset.asName,
                         runProcConf.inputData.stm->when,
                         *runProcConf.inputData.stm->where,
                         orderBy,
                         calc);

    bsq.execute({analyseRow, true/*processInParallel*/},
                runProcConf.inputData.stm->offset,
                runProcConf.inputData.stm->limit,
                convertProgressToJson);

    set<Date> timestamps;
    timestampsAcc.forEach([&] (unordered_set<Date> * dates)
    {
        timestamps.insert(dates->begin(), dates->end());
    });
    Date earliest = Date::positiveInfinity();
    earliestAcc.forEach([&] (Date * threadEarliest) {
        if (*threadEarliest < earliest) {
            earliest = *threadEarliest;
        }
    });
    Date latest = Date::negativeInfinity();
    latestAcc.forEach([&] (Date * threadLatest) {
        if (*threadLatest > latest) {
            latest = *threadLatest;
        }
    });

    typedef tuple<ColumnPath, CellValue, Date> Cell;
    vector<std::pair<RowPath, vector<Cell>>> rows;
    vector<Cell> cells;
    cells.reserve(7);
    Date now = Date::now();
    cells.emplace_back(ColumnPath("rowsCount"), rowsCount.load(), now);
    cells.emplace_back(
        ColumnPath("columnsCount"),
        bsq.getSelectOutputInfo()->allColumnNames().size(), now);
    cells.emplace_back(ColumnPath("distinctTimestampsCount"), timestamps.size(), now);
    cells.emplace_back(ColumnPath("cellsCount"),
                       cellsCount.load(), now);
    cells.emplace_back(ColumnPath("dataPointsCount"),
                       datapointsCount.load(), now);
    cells.emplace_back(ColumnPath("earliestTimestamp"), earliest, now);
    cells.emplace_back(ColumnPath("latestTimestamp"), latest, now);
    rows.push_back(make_pair(RowPath("result"), std::move(cells)));
    output->recordRows(rows);
    output->commit();
    return output->getStatus();
}

Any
DatasetStatsProcedure::
getStatus() const
{
    return Any();
}

static RegisterProcedureType<DatasetStatsProcedure, DatasetStatsProcedureConfig>
regDatasetStatsProcedure(
    proPackage(),
    "Provides general statistics about a dataset.",
    "DatasetStatsProcedure.md.html",
    nullptr /* static route */,
    { MldbEntity::INTERNAL_ENTITY });


} // namespace MLDB


