// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** dist_table_procedure.cc                                        -*- C++ -*-
    Simon Lemieux, June 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    distTable procedure
*/

#include "dist_table_procedure.h"
#include "types/basic_value_descriptions.h"
#include "types/distribution_description.h"
#include "types/map_description.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "server/dataset_context.h"
#include "plugins/matrix.h"
#include "server/analytics.h"
#include "ml/value_descriptions.h"
#include "types/any_impl.h"
#include "jml/utils/string_functions.h"
#include "plugins/sql_functions.h"
#include "sql/execution_pipeline.h"
#include "server/bound_queries.h"
#include "mldb/http/http_exception.h"
#include "mldb/jml/db/persistent.h"
#include "mldb/types/jml_serialization.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/plugins/sql_config_validator.h"
#include "mldb/base/parallel.h"
#include "mldb/types/optional_description.h"


using namespace std;

namespace Datacratic {
namespace MLDB {

inline ML::DB::Store_Writer &
operator << (ML::DB::Store_Writer & store, const PathElement & coord)
{
    return store << Id(coord.toUtf8String());
}

inline ML::DB::Store_Reader &
operator >> (ML::DB::Store_Reader & store, PathElement & coord)
{
    Id id;
    store >> id;
    coord = id.toUtf8String();
    return store;
}

inline ML::DB::Store_Writer &
operator << (ML::DB::Store_Writer & store, const Path & coords)
{
    return store << coords.toUtf8String();
}

inline ML::DB::Store_Reader &
operator >> (ML::DB::Store_Reader & store, Path & coords)
{
    Utf8String str;
    store >> str;
    coords = Path::parse(str);
    return store;
}

/*****************************************************************************/
/* DIST TABLE                                                               */
/*****************************************************************************/

inline ML::DB::Store_Writer &
operator << (ML::DB::Store_Writer & store, const DistTableStats & s)
{
    return store << s.count << s.avg << s.var << s.M2 << s.min << s.max;
}

inline ML::DB::Store_Reader &
operator >> (ML::DB::Store_Reader & store, DistTableStats & s)
{
    store >> s.count >> s.avg >> s.var >> s.M2 >> s.min >> s.max;
    return store;
}

void
DistTableStats::increment(double value)
{
    // special case if first value
    if (JML_UNLIKELY(count == 0)) {
        count = 1;
        avg = min = max = value;
        M2 = 0.;
    } else {
        count++;
        double delta = value - avg;
        avg += delta / count;
        // For the unbiased std, I'm using Welford's algorithm described here
        // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
        M2 += delta * (value - avg);

        if (count >= 2) // otherwise unbiased variance doesn't make sense
            var = M2 / (count - 1);
        
        min = std::min(min, value);
        max = std::max(max, value);
    }
}

DistTable::
DistTable(const std::string & filename):
    unknownStats(std::vector<DistTableStats>(outcome_names.size()))
{
    filter_istream stream(filename);
    ML::DB::Store_Reader store(stream);
    reconstitute(store);
}

void DistTable::
increment(const Utf8String & featureValue, const vector<double> & targets)
{
    const auto & it = stats.find(featureValue);
    vector<DistTableStats> & targetStats =
        it == stats.end()
        ? stats[featureValue]
        : it->second;
    // if it's the first time we see that `featureValue`, let's prepare the
    // stats vector
    if (it == stats.end()) {
        targetStats.reserve(targets.size());
        for (int i=0; i < targets.size(); ++i)
            targetStats.emplace_back();
    }

    for (int i=0; i < targets.size(); ++i) {
        targetStats[i].increment(targets[i]);
    }
}

const vector<DistTableStats> &
DistTable::
getStats(const Utf8String & featureValue) const
{
    const auto & it = stats.find(featureValue);
    if (it == stats.end())
        return unknownStats;
    return it->second;
}

void DistTable::
save(const std::string & filename) const
{
    filter_ostream stream(filename);
    ML::DB::Store_Writer store(stream);
    serialize(store);
}

void DistTable::
serialize(ML::DB::Store_Writer & store) const
{
    int version = 1;
    store << string("MLDB Dist Table Binary")
          << version << colName << outcome_names << stats << unknownStats;
}

void DistTable::
reconstitute(ML::DB::Store_Reader & store)
{
    int version;
    int REQUIRED_V = 1;
    std::string name;
    store >> name >> version;
    if (name != "MLDB Dist Table Binary") {
        throw HttpReturnException(400, "File does not appear to be a dist "
                                  "table model");
    }
    if(version!=REQUIRED_V) {
        throw HttpReturnException(400, ML::format(
                    "invalid DistTable version! exptected %d, got %d",
                    REQUIRED_V, version));
    }

    store >> colName >> outcome_names >> stats >> unknownStats;
}


/*****************************************************************************/
/* DIST TABLE PROCEDURE CONFIG                                              */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(DistTableProcedureConfig);

DistTableProcedureConfigDescription::
DistTableProcedureConfigDescription()
{
    addField("trainingData", &DistTableProcedureConfig::trainingData,
             "SQL query to select the data on which the rolling operations will "
             "be performed.");
    addField("outputDataset", &DistTableProcedureConfig::output,
             "Output dataset",
             PolyConfigT<Dataset>().withType("tabular"));
    addField("outcomes", &DistTableProcedureConfig::outcomes,
             "List of expressions to generate the outcomes. Each can be any expression "
             "involving the columns in the dataset. The type of the outcomes "
             "must be of type `INTEGER` or `NUMBER`");
    addField("distTableFileUrl", &DistTableProcedureConfig::modelFileUrl,
             "URL where the model file (with extension '.dt') should be saved. "
             "This file can be loaded by the ![](%%doclink experimental.distTable.getStats function). "
             "This parameter is optional unless the `functionName` parameter is used.");
    addField("functionName", &DistTableProcedureConfig::functionName,
             "If specified, an instance of the ![](%%doclink experimental.distTable.getStats function) "
             "of this name will be created using the trained dist tables. Note that to use "
             "this parameter, the `distTableFileUrl` must also be provided.");
    addParent<ProcedureConfig>();

    onPostValidate = chain(validateQuery(&DistTableProcedureConfig::trainingData,
                                         MustContainFrom()),
                           validateFunction<DistTableProcedureConfig>());
}



/*****************************************************************************/
/* DIST TABLE PROCEDURE                                                     */
/*****************************************************************************/

DistTableProcedure::
DistTableProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    procConfig = config.params.convert<DistTableProcedureConfig>();
}

Any
DistTableProcedure::
getStatus() const
{
    return Any();
}

RunOutput
DistTableProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{
    DistTableProcedureConfig runProcConf =
        applyRunConfOverProcConf(procConfig, run);

    SqlExpressionMldbScope context(server);
    auto boundDataset = runProcConf.trainingData.stm->from->bind(context);

    vector<Utf8String> outcome_names;
    for(const pair<string, std::shared_ptr<SqlExpression>> & lbl : runProcConf.outcomes)
        outcome_names.push_back(lbl.first);

    DistTablesMap distTablesMap;

    {
        // Find only those variables used
        SqlExpressionDatasetScope context(boundDataset);

        auto selectBound = runProcConf.trainingData.stm->select.bind(context);

        for (const auto & c: selectBound.info->getKnownColumns()) {
            distTablesMap.insert(make_pair(c.columnName, DistTable(c.columnName, outcome_names)));
        }
    }

    auto onProgress2 = [&] (const Json::Value & progress)
        {
            Json::Value value;
            value["dataset"] = progress;
            return onProgress(value);
        };

    auto output = createDataset(server, runProcConf.output, onProgress2, true /*overwrite*/);

    int num_req = 0;
    Date start = Date::now();

    const int nbOutcomes = runProcConf.outcomes.size();

    auto processor = [&] (NamedRowValue & row_,
                           const std::vector<ExpressionValue> & extraVals)
        {
            MatrixNamedRow row = row_.flattenDestructive();

            if (num_req++ % 5000 == 0) {
                double secs = Date::now().secondsSinceEpoch() - start.secondsSinceEpoch();
                string progress = ML::format("done %d. %0.4f/sec", num_req, num_req / secs);
                onProgress(progress);
                cerr << progress << endl;
            }

            // we parse in advance the value for each outcome
            vector<double> targets(nbOutcomes);
            for (int i=0; i < nbOutcomes; i++) {
                CellValue outcome = extraVals.at(i).getAtom();
                targets[i] = outcome.toDouble();
            }

            map<ColumnName, size_t> column_idx;
            for (int i=0; i<row.columns.size(); i++) {
                column_idx.insert(make_pair(get<0>(row.columns[i]), i));
            }

            std::vector<std::tuple<ColumnName, CellValue, Date> > output_cols;

            // for each of our feature column (or distribution table)
            for (auto it = distTablesMap.begin(); it != distTablesMap.end(); it++) {
                const ColumnName & featureColumnName = it->first;
                DistTable & distTable = it->second;

                // It seems that all the columns from the select will always
                // be here, even if NULL. If that is not the case, we will need
                // to treat this case separately and return (0,nan, nan, nan,
                // nan, nan) as statistics.
                auto col_ptr = column_idx.find(featureColumnName);
                if (col_ptr == column_idx.end()) {
                    ExcAssert(false);
                }

                const tuple<ColumnName, CellValue, Date> & col =
                    row.columns[col_ptr->second];

                const Utf8String & featureValue = get<1>(col).toString();
                const Date & ts = get<2>(col); 

                // note current dist tables for output dataset
                // TODO we compute the column names everytime, maybe we should
                // cache them
                const auto & stats = distTable.getStats(featureValue);

                for (int i=0; i < nbOutcomes; ++i) {
                    output_cols.emplace_back(
                        PathElement(outcome_names[i]) + featureColumnName
                            + "count",
                        CellValue(stats[i].count),
                        ts);
                    output_cols.emplace_back(
                        PathElement(outcome_names[i]) + featureColumnName
                            + "avg",
                        CellValue(stats[i].avg),
                        ts);
                    output_cols.emplace_back(
                        PathElement(outcome_names[i]) + featureColumnName
                            + "std",
                        CellValue(stats[i].getStd()),
                        ts);
                    output_cols.emplace_back(
                        PathElement(outcome_names[i]) + featureColumnName
                            + "min",
                        CellValue(stats[i].min),
                        ts);
                    output_cols.emplace_back(
                        PathElement(outcome_names[i]) + featureColumnName
                            + "max",
                        CellValue(stats[i].max),
                        ts);
                }

                // increment stats tables with current row
                distTable.increment(featureValue, targets);
            }

            output->recordRow(ColumnName(row.rowName), output_cols);

            return true;
        };

    // We want to calculate the outcome and weight of each row as well
    // as the select expression
    std::vector<std::shared_ptr<SqlExpression> > extra;
    extra.reserve(runProcConf.outcomes.size());
    for(const pair<string, std::shared_ptr<SqlExpression>> & lbl : runProcConf.outcomes)
        extra.push_back(lbl.second);

    iterateDataset(runProcConf.trainingData.stm->select,
                   *boundDataset.dataset, boundDataset.asName,
                   runProcConf.trainingData.stm->when,
                   *runProcConf.trainingData.stm->where,
                   extra,
                   {processor,false/*processInParallel*/},
                   runProcConf.trainingData.stm->orderBy,
                   runProcConf.trainingData.stm->offset,
                   runProcConf.trainingData.stm->limit);

    output->commit();

    // save if required
    if(!runProcConf.modelFileUrl.empty()) {
        filter_ostream stream(runProcConf.modelFileUrl.toString());
        ML::DB::Store_Writer store(stream);
        store << distTablesMap;
    }

    if(!runProcConf.modelFileUrl.empty() && !runProcConf.functionName.empty()) {
        PolyConfig clsFuncPC;
        clsFuncPC.type = "experimental.distTable.getStats";
        clsFuncPC.id = runProcConf.functionName;
        clsFuncPC.params = DistTableFunctionConfig(runProcConf.modelFileUrl);

        createFunction(server, clsFuncPC, onProgress, true);
    }

    return RunOutput();
}





/*****************************************************************************/
/* DIST TABLE FUNCTION                                                      */
/*****************************************************************************/
DEFINE_STRUCTURE_DESCRIPTION(DistTableFunctionConfig);

DistTableFunctionConfigDescription::
DistTableFunctionConfigDescription()
{
    addField("distTableFileUrl", &DistTableFunctionConfig::modelFileUrl,
             "URL of the model file (with extension '.dt') to load. "
             "This file is created by the ![](%%doclink experimental.distTable.train procedure).");
}

DistTableFunction::
DistTableFunction(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<DistTableFunctionConfig>();

    // Load saved stats tables
    filter_istream stream(functionConfig.modelFileUrl.toString());
    ML::DB::Store_Reader store(stream);
    store >> distTablesMap;
}

DistTableFunction::
~DistTableFunction()
{
}

Any
DistTableFunction::
getStatus() const
{
    Json::Value result;
    return result;
}

Any
DistTableFunction::
getDetails() const
{
    return Any();
}

ExpressionValue
DistTableFunction::
apply(const FunctionApplier & applier,
      const ExpressionValue & context) const
{
    StructValue result;

    ExpressionValue arg = context.getColumn("features");

    if(!arg.isRow())
        throw HttpReturnException(400, "wrong input type to dist table",
                                  "input", arg);


    RowValue rtnRow;
    // TODO should we cache column names
    auto onAtom = [&] (const ColumnName & columnName,
                        const ColumnName & prefix,
                        const CellValue & val,
                        Date ts)
    {
        auto st = distTablesMap.find(columnName);
        if (st == distTablesMap.end())
            return true;

        if (val.empty())
            return true;

        const DistTable & distTable = st->second;

        const auto & stats = distTable.getStats(val.toString());
        for (int i=0; i < distTable.outcome_names.size(); ++i) {

            rtnRow.emplace_back(
                PathElement(distTable.outcome_names[i]) + columnName
                            + "count", stats[i].count, ts);
            rtnRow.emplace_back(
                PathElement(distTable.outcome_names[i]) + columnName
                            + "avg", stats[i].avg, ts);
            rtnRow.emplace_back(
                PathElement(distTable.outcome_names[i]) + columnName
                            + "std", stats[i].getStd(), ts);
            rtnRow.emplace_back(
                PathElement(distTable.outcome_names[i]) + columnName
                            + "min", stats[i].min, ts);
            rtnRow.emplace_back(
                PathElement(distTable.outcome_names[i]) + columnName
                            + "max", stats[i].max, ts);
        }

        return true;
    };

    arg.forEachAtom(onAtom);
    result.emplace_back("stats", ExpressionValue(std::move(rtnRow)));

    return std::move(result);
}

FunctionInfo
DistTableFunction::
getFunctionInfo() const
{
    FunctionInfo result;

    std::vector<KnownColumn> inputColumns, outputColumns;
    inputColumns.emplace_back(PathElement("features"), std::make_shared<UnknownRowValueInfo>(),
                              COLUMN_IS_DENSE, 0);
    outputColumns.emplace_back(PathElement("stats"), std::make_shared<UnknownRowValueInfo>(),
                               COLUMN_IS_DENSE, 0);
    
    result.input.reset(new RowValueInfo(inputColumns, SCHEMA_CLOSED));
    result.output.reset(new RowValueInfo(outputColumns, SCHEMA_CLOSED));
    
    return result;
}


/*****************************************************************************/
/*****************************************************************************/
/*****************************************************************************/

namespace {

RegisterProcedureType<DistTableProcedure, DistTableProcedureConfig>
regSTTrain(builtinPackage(),
           "Create a statistical table on a given set of outcomes, with"
           " respect to certain feature columns",
           "procedures/DistTableProcedure.md.html",
           nullptr /* static route */,
           { MldbEntity::INTERNAL_ENTITY });

RegisterFunctionType<DistTableFunction, DistTableFunctionConfig>
regClassifyFunction(builtinPackage(),
                    "experimental.distTable.getStats",
                    "Get statistics from the previously created statistical table for a row of features",
                    "functions/DistTableGetStats.md.html",
                    nullptr /* static route */,
                    { MldbEntity::INTERNAL_ENTITY });

} // file scope

} // namespace MLDB
} // namespace Datacratic
