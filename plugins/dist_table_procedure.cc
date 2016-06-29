// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** dist_table_procedure.cc                                        -*- C++ -*-
    Simon Lemieux, June 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    distTable procedure

    Note that this was mainly copied from stats_table_procedure.cc in a hurry.
    This will probably be revisited.
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

DistTable::
DistTable(const std::string & filename)
{
    filter_istream stream(filename);
    ML::DB::Store_Reader store(stream);
    reconstitute(store);
}

void DistTable::
increment(const Utf8String & featureValue, uint outcome, double targetValue) {
    auto it = counts[featureValue].find(outcome);
    if (it == counts[featureValue].end()) {
        counts[featureValue][outcome] = 1;
        avgs[featureValue][outcome] = targetValue;
        mins[featureValue][outcome] = targetValue;
        maxs[featureValue][outcome] = targetValue;
        M2s[featureValue][outcome] = 0.;
    } else {
        uint64_t n = ++counts[featureValue][outcome];

        double delta = targetValue - avgs[featureValue][outcome];
        avgs[featureValue][outcome] += delta / n;
        // For the unbiased std, I'm using Welford's algorithm described here
        // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
        double M2 = M2s[featureValue][outcome] =
            M2s[featureValue][outcome]
            + delta * (targetValue - avgs[featureValue][outcome]);
        if (n >= 2) // otherwise unbiased variance doesn't make sense
            vars[featureValue][outcome] = M2 / (n - 1);

        mins[featureValue][outcome] = std::min(mins[featureValue][outcome],
                                               targetValue);
        maxs[featureValue][outcome] = std::max(maxs[featureValue][outcome],
                                               targetValue);
    }
}

tuple<uint64_t, double, double, double, double>
DistTable::
getStats(const Utf8String & featureValue, int outcome) const
{
    // if we don't have anything for that feature and outcome, return
    // something dummy
    if (!(counts.count(featureValue)
            && counts.at(featureValue).count(outcome)))
        return make_tuple(0, NAN, NAN, NAN, NAN);

    int count = counts.at(featureValue).at(outcome);
    ExcAssert(count > 0);
    return make_tuple(
        counts.at(featureValue).at(outcome),
        avgs.at(featureValue).at(outcome),
        // unbiased std needs n >= 2
        count >= 2 ? sqrt(vars.at(featureValue).at(outcome)) : NAN,
        mins.at(featureValue).at(outcome),
        maxs.at(featureValue).at(outcome));
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
          << version << colName << outcome_names << counts << avgs << vars
          << M2s << mins << maxs;
}

void DistTable::
reconstitute(ML::DB::Store_Reader & store)
{
    int version;
    int REQUIRED_V = 1;
    std::string name;
    store >> name >> version;
    if (name != "MLDB Dist Table Binary") {
        throw HttpReturnException(400, "File does not appear to be a stats "
                                  "table model");
    }
    if(version!=REQUIRED_V) {
        throw HttpReturnException(400, ML::format(
                    "invalid DistTable version! exptected %d, got %d",
                    REQUIRED_V, version));
    }

    store >> colName >> outcome_names >> counts >> avgs >> vars
          >> M2s >> mins >> maxs;
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
             PolyConfigT<Dataset>().withType("sparse.mutable"));
    addField("outcomes", &DistTableProcedureConfig::outcomes,
             "List of expressions to generate the outcomes. Each can be any expression "
             "involving the columns in the dataset. The type of the outcomes "
             "must be of type `INTEGER` or `NUMBER`");
    addField("distTableFileUrl", &DistTableProcedureConfig::modelFileUrl,
             "URL where the model file (with extension '.dt') should be saved. "
             "This file can be loaded by the ![](%%doclink distTable.getStats function). "
             "This parameter is optional unless the `functionName` parameter is used.");
    addField("functionName", &DistTableProcedureConfig::functionName,
             "If specified, an instance of the ![](%%doclink distTable.getStats function) "
             "of this name will be created using the trained stats tables. Note that to use "
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

        for (auto & c: selectBound.info->getKnownColumns()) {
            distTablesMap.insert(make_pair(c.columnName, DistTable(c.columnName, outcome_names)));
            cerr << "initializing a stats table for column " 
                 << c.columnName << endl;
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

    // columns cache
    // key: feature column name
    // values: in order, column name for 
    map<ColumnName, vector<ColumnName>> colCache;

    const int nbOutcomes = runProcConf.outcomes.size();

    auto processor = [&] (NamedRowValue & row_,
                           const std::vector<ExpressionValue> & extraVals)
        {
            MatrixNamedRow row = row_.flattenDestructive();

            if(num_req++ % 5000 == 0) {
                double secs = Date::now().secondsSinceEpoch() - start.secondsSinceEpoch();
                string progress = ML::format("done %d. %0.4f/sec", num_req, num_req / secs);
                onProgress(progress);
                cerr << progress << endl;
            }

            // we parse in advance the value for each outcome
            vector<bool> haveTarget(nbOutcomes);
            vector<double> targets(nbOutcomes);
            for(int i=0; i < nbOutcomes; i++) {
                CellValue outcome = extraVals.at(i).getAtom();
                haveTarget[i] = !outcome.empty();
                if (haveTarget[i])
                    targets[i] = outcome.toDouble();
            }

            map<ColumnName, size_t> column_idx;
            for(int i=0; i<row.columns.size(); i++) {
                column_idx.insert(make_pair(get<0>(row.columns[i]), i));
            }

            std::vector<std::tuple<ColumnName, CellValue, Date> > output_cols;

            // for each of our feature column (or distribution table)
            for(auto it = distTablesMap.begin(); it != distTablesMap.end(); it++) {

                const ColumnName & featureColumnName = it->first;
                DistTable & distTable = it->second;

                // make sure the column is there (not NULL) because if the
                // column is NULL, it won't make sense in the output dataset
                // anyway
                auto col_ptr = column_idx.find(featureColumnName);
                if(col_ptr == column_idx.end())
                    continue;

                const tuple<ColumnName, CellValue, Date> & col =
                    row.columns[col_ptr->second];

                Utf8String featureValue = get<1>(col).toString();


                // note current dist tables for output dataset
                for (int i=0; i < nbOutcomes; ++i) {
                    // TODO the column names could be cached if we think it's
                    // going to speed up things

                    auto stats = distTable.getStats(featureValue, i);
                    uint64_t count = get<0>(stats);
                    double avg = get<1>(stats);
                    double std = get<2>(stats);
                    double min = get<3>(stats);
                    double max = get<4>(stats);

                    output_cols.emplace_back(
                        PathElement(outcome_names[i]) + featureColumnName
                            + "count",
                        CellValue(count),
                        Date::negativeInfinity());
                    if (count >= 1) {
                        output_cols.emplace_back(
                            PathElement(outcome_names[i]) + featureColumnName
                                + "avg",
                            CellValue(avg),
                            Date::negativeInfinity());
                        output_cols.emplace_back(
                            PathElement(outcome_names[i]) + featureColumnName
                                + "min",
                            CellValue(min),
                            Date::negativeInfinity());
                        output_cols.emplace_back(
                            PathElement(outcome_names[i]) + featureColumnName
                                + "max",
                            CellValue(max),
                            Date::negativeInfinity());
                    }
                    if (count >= 2) {
                        output_cols.emplace_back(
                            PathElement(outcome_names[i]) + featureColumnName
                                + "std",
                            CellValue(std),
                            Date::negativeInfinity());
                    }
                } 

                // increment stats tables with current row
                // for each target we increment the aggregators
                for (int i=0; i < nbOutcomes; ++i) {
                    if (haveTarget[i]) {
                        distTable.increment(featureValue, i, targets[i]);
                    }
                }
            }

            output->recordRow(ColumnName(row.rowName), output_cols);

            return true;
        };


    // We want to calculate the outcome and weight of each row as well
    // as the select expression
    std::vector<std::shared_ptr<SqlExpression> > extra;
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
        cerr << "Saving dist tables to " << runProcConf.modelFileUrl.toString() << endl;
        PolyConfig clsFuncPC;
        clsFuncPC.type = "distTable.getStats";
        clsFuncPC.id = runProcConf.functionName;
        clsFuncPC.params = DistTableFunctionConfig(runProcConf.modelFileUrl);

        cerr << "creating function" << endl;
        createFunction(server, clsFuncPC, onProgress, true);
        cerr << "done creating function" << endl;
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
             "This file is created by the ![](%%doclink distTable.train procedure).");
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

    if(arg.isRow()) {
        RowValue rtnRow;

        // TODO should we cache column names as we did in the procedure?
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

                for (int i=0; i < distTable.getNbOutcomes(); ++i) {
                    auto stats = distTable.getStats(val.toString(), i);
                    uint64_t count = get<0>(stats);

                    rtnRow.emplace_back(
                        PathElement(distTable.outcome_names[i]) + columnName
                                    + "count", count, ts);
                    if (count >= 1) {
                        rtnRow.emplace_back(
                            PathElement(distTable.outcome_names[i]) + columnName
                                        + "avg", get<1>(stats), ts);
                        rtnRow.emplace_back(
                            PathElement(distTable.outcome_names[i]) + columnName
                                        + "min", get<3>(stats), ts);
                        rtnRow.emplace_back(
                            PathElement(distTable.outcome_names[i]) + columnName
                                        + "max", get<4>(stats), ts);
                    }
                    // the std only makes sense with n >= 2
                    if (count >= 2) {
                        rtnRow.emplace_back(
                            PathElement(distTable.outcome_names[i]) + columnName
                                        + "std", get<2>(stats), ts);
                    }
                }

                return true;
            };

        arg.forEachAtom(onAtom);
        result.emplace_back("stats", ExpressionValue(std::move(rtnRow)));
    }
    else {
        throw HttpReturnException(400, "wrong input type to stats table",
                                  "input", arg);
    }

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
    outputColumns.emplace_back(PathElement("counts"), std::make_shared<UnknownRowValueInfo>(),
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
           "Create statistical tables a given set of feature columns against numeric outcome columns",
           "procedures/DistTableProcedure.md.html");

RegisterFunctionType<DistTableFunction, DistTableFunctionConfig>
regClassifyFunction(builtinPackage(),
                    "distTable.getStats",
                    "Get statistics from the previously created statistical tables for a row of features",
                    "functions/DistTableGetStats.md.html");

} // file scope

} // namespace MLDB
} // namespace Datacratic
