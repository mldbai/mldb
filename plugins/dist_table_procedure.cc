/** dist_table_procedure.cc                                        -*- C++ -*-
    Simon Lemieux, June 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    distTable procedure
*/

#include "dist_table_procedure.h"
#include "types/basic_value_descriptions.h"
#include "ml/value_descriptions.h"
#include "mldb/types/optional_description.h"
#include "mldb/types/distribution_description.h"
#include "mldb/types/pair_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/map_description.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "server/dataset_context.h"
#include "plugins/matrix.h"
#include "server/analytics.h"
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
#include "mldb/utils/log.h"


using namespace std;


namespace MLDB {

inline ML::DB::Store_Writer &
operator << (ML::DB::Store_Writer & store, const PathElement & coord)
{
    return store << coord.toUtf8String();
}

inline ML::DB::Store_Reader &
operator >> (ML::DB::Store_Reader & store, PathElement & coord)
{
    Utf8String id;
    store >> id;
    coord = std::move(id);
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
/* DIST TABLE STATS                                                          */
/*****************************************************************************/

inline ML::DB::Store_Writer &
operator << (ML::DB::Store_Writer & store, const DistTableStats & s)
{
    return store << s.count << s.avg << s.var << s.M2 << s.min << s.max
                 << s.last << s.sum;
}

inline ML::DB::Store_Reader &
operator >> (ML::DB::Store_Reader & store, DistTableStats & s)
{
    store >> s.count >> s.avg >> s.var >> s.M2 >> s.min >> s.max >> s.last
          >> s.sum;
    return store;
}

void
DistTableStats::
increment(double value)
{
    // special case if first value
    if (MLDB_UNLIKELY(count == 0)) {
        count = 1;
        avg = min = max = sum = value;
        M2 = 0.;
    } else {
        count++;
        sum += value;
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

    last = value;
}

double
DistTableStats::
getStat(DISTTABLE_STATISTICS stat) const
{
    switch(stat) {
        case DT_COUNT:  return count;
        case DT_AVG:    return avg;
        case DT_STD:    return getStd();
        case DT_MIN:    return min;
        case DT_MAX:    return max;
        case DT_LAST:   return last;
        case DT_SUM:    return sum;
        default:
            throw MLDB::Exception("Unknown DistTable_Stat");
    }
}


/*****************************************************************************/
/* DIST TABLE                                                                */
/*****************************************************************************/

DistTable::
DistTable(const std::string & filename):
    unknownStats(std::vector<DistTableStats>(outcome_names.size()))
{
    filter_istream stream(filename);
    ML::DB::Store_Reader store(stream);
    reconstitute(store);
}

void
DistTable::
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
        throw HttpReturnException(400, MLDB::format(
                    "invalid DistTable version! exptected %d, got %d",
                    REQUIRED_V, version));
    }

    store >> colName >> outcome_names >> stats >> unknownStats;
}


/*****************************************************************************/
/* DIST TABLE PROCEDURE CONFIG                                              */
/*****************************************************************************/

DEFINE_ENUM_DESCRIPTION(DistTableMode);

DistTableModeDescription::
DistTableModeDescription()
{
    addValue("bagOfWords",   DT_MODE_BAG_OF_WORDS, "This mode will use the name of "
        "the columns as the keys to the distribution tables.");
    addValue("fixedColumns", DT_MODE_FIXED_COLUMNS, "This mode will use the value "
        "of the cells as the keys to the distribution tables.");
}

DEFINE_STRUCTURE_DESCRIPTION(DistTableProcedureConfig);

DistTableProcedureConfigDescription::
DistTableProcedureConfigDescription()
{
    Optional<PolyConfigT<Dataset> > optionalOutputDataset;
    optionalOutputDataset.emplace(PolyConfigT<Dataset>().
                                  withType(DistTableProcedureConfig::defaultOutputDatasetType));

    addField("trainingData", &DistTableProcedureConfig::trainingData,
             "SQL query to select the data on which the rolling operations will "
             "be performed.");
    addField("outputDataset", &DistTableProcedureConfig::output,
             "Output dataset", optionalOutputDataset);
    addField("outcomes", &DistTableProcedureConfig::outcomes,
             "List of expressions to generate the outcomes. Each can be any expression "
             "involving the columns in the dataset. The type of the outcomes "
             "must be of type `INTEGER` or `NUMBER`");

    std::vector<Utf8String> defaultStats = { "count", "avg", "std", "min", "max" };
    addField("statistics", &DistTableProcedureConfig::statistics,
             "List of statistics to track for each outcome.", defaultStats);

    addField("mode", &DistTableProcedureConfig::mode,
            "Distribution table mode. This must match the type of data to use: fixed "
            "columns or bag of words.", DT_MODE_FIXED_COLUMNS);

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


    // build a cache of the names for quick access
    std::string dtStatsNames[DT_NUM_STATISTICS];
    for(int i=0; i<DT_NUM_STATISTICS; i++)
        dtStatsNames[(DISTTABLE_STATISTICS)i] = print((DISTTABLE_STATISTICS)i);

    // configure active statistics
    if(runProcConf.statistics.size() == 0)
        runProcConf.statistics = { "count", "avg", "std", "min", "max" };

    vector<DISTTABLE_STATISTICS> activeStats;
    for(auto & stat : runProcConf.statistics) {
        activeStats.push_back(parseDistTableStatistic(stat));
    }


    SqlExpressionMldbScope context(server);
    ConvertProgressToJson convertProgressToJson(onProgress);
    auto boundDataset = runProcConf.trainingData.stm->from->bind(context, convertProgressToJson);

    vector<Utf8String> outcome_names;
    for(const pair<string, std::shared_ptr<SqlExpression>> & lbl : runProcConf.outcomes)
        outcome_names.push_back(lbl.first);

    DistTablesMap distTablesMap;

    if(runProcConf.mode == DT_MODE_FIXED_COLUMNS) {
        // Find only those variables used
        SqlExpressionDatasetScope context(boundDataset);

        auto selectBound = runProcConf.trainingData.stm->select.bind(context);

        for (const auto & c: selectBound.info->getKnownColumns()) {
            distTablesMap.insert(
                    make_pair(c.columnName,
                              DistTable(c.columnName, outcome_names)));
        }
    }
    else if(runProcConf.mode == DT_MODE_BAG_OF_WORDS) {
        distTablesMap.insert(
                make_pair(ColumnPath("words"),
                          DistTable(ColumnPath("words"), outcome_names)));
    }
    else {
        throw MLDB::Exception("unsupported dist table mode");
    }

    auto onProgress2 = [&] (const Json::Value & progress)
        {
            Json::Value value;
            value["dataset"] = progress;
            return onProgress(value);
        };

    std::shared_ptr<Dataset> output;
    if(runProcConf.output) {
        if(runProcConf.mode == DT_MODE_BAG_OF_WORDS)
            throw MLDB::Exception("Cannot use outputDataset when mode=bagOfWords");

        PolyConfigT<Dataset> outputDataset = *runProcConf.output;
        if (outputDataset.type.empty())
            outputDataset.type = DistTableProcedureConfig::defaultOutputDatasetType;

        output = createDataset(server, outputDataset, onProgress2, true /*overwrite*/);
    }

    int num_req = 0;
    Date start = Date::now();

    const int nbOutcomes = runProcConf.outcomes.size();

    auto processor = [&] (NamedRowValue & row_,
                           const std::vector<ExpressionValue> & extraVals)
        {
            MatrixNamedRow row = row_.flattenDestructive();

            if (num_req++ % 5000 == 0) {
                double secs = Date::now().secondsSinceEpoch() - start.secondsSinceEpoch();
                string message = MLDB::format("done %d. %0.4f/sec", num_req, num_req / secs);
                Json::Value progress;
                progress["message"] = message; 
                onProgress(progress);
                INFO_MSG(logger) << message;
            }

            // we parse in advance the value for each outcome
            vector<double> targets(nbOutcomes);
            for (int i=0; i < nbOutcomes; i++) {
                const CellValue & outcome = extraVals.at(i).getAtom();
                targets[i] = outcome.toDouble();
            }


            if(runProcConf.mode == DT_MODE_BAG_OF_WORDS) {
                // there is only a single table
                DistTable & distTable = distTablesMap.begin()->second;
                for(const std::tuple<ColumnPath, CellValue, Date> & col : row.columns) {
                    distTable.increment(get<0>(col).toUtf8String(), targets);
                }
            }
            else if(runProcConf.mode == DT_MODE_FIXED_COLUMNS) {

                std::vector<std::tuple<ColumnPath, CellValue, Date> > output_cols;

                map<ColumnPath, size_t> column_idx;
                for (int i=0; i<row.columns.size(); i++) {
                    column_idx.insert(make_pair(get<0>(row.columns[i]), i));
                }

                // for each of our feature column (or distribution table)
                for (auto it = distTablesMap.begin(); it != distTablesMap.end(); it++) {
                    const ColumnPath & featureColumnName = it->first;
                    DistTable & distTable = it->second;

                    // It seems that all the columns from the select will always
                    // be here, even if NULL. If that is not the case, we will need
                    // to treat this case separately and return (0,nan, nan, nan,
                    // nan, nan) as statistics.
                    auto col_ptr = column_idx.find(featureColumnName);
                    if (col_ptr == column_idx.end()) {
                        ExcAssert(false);
                    }

                    const tuple<ColumnPath, CellValue, Date> & col =
                        row.columns[col_ptr->second];

                    const Utf8String & featureValue = get<1>(col).toUtf8String();
                    const Date & ts = get<2>(col);

                    if(output) {
                        // note current dist tables for output dataset
                        // TODO we compute the column names everytime, maybe we should
                        // cache them
                        const auto & stats = distTable.getStats(featureValue);

                        for (int i=0; i < nbOutcomes; ++i) {
                            for(DISTTABLE_STATISTICS sid : activeStats) {
                                output_cols.emplace_back(
                                    PathElement(outcome_names[i]) + featureColumnName
                                        + dtStatsNames[sid],
                                    CellValue(stats[i].getStat(sid)),
                                    ts);
                            }
                        }
                    }

                    // increment stats tables with current row
                    distTable.increment(featureValue, targets);
                }

                if(output) {
                    output->recordRow(ColumnPath(row.rowName), std::move(output_cols));
                }
            }
            else {
                throw MLDB::Exception("Unknown distTable mode");
            }

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

    if(output) {
        output->commit();
    }

    // save if required
    if(!runProcConf.modelFileUrl.empty()) {
        DistTableProcedure::persist(runProcConf.modelFileUrl,
                runProcConf.mode, distTablesMap);
    }

    if(!runProcConf.modelFileUrl.empty() && !runProcConf.functionName.empty()) {
        PolyConfig clsFuncPC;
        clsFuncPC.type = "experimental.distTable.getStats";
        clsFuncPC.id = runProcConf.functionName;
        clsFuncPC.params = DistTableFunctionConfig(runProcConf.modelFileUrl,
                runProcConf.statistics);

        createFunction(server, clsFuncPC, onProgress, true);
    }

    return RunOutput();
}

void DistTableProcedure::
persist(const Url & modelFileUrl, DistTableMode mode, 
        const DistTablesMap & distTablesMap)
{
    filter_ostream stream(modelFileUrl);
    ML::DB::Store_Writer store(stream);
    store << DistTableProcedure::DIST_TABLE_PERSIST_VERSION << mode << distTablesMap;
}


/*****************************************************************************/
/* DIST TABLE FUNCTION FUNCTIONS ENDPOINT PAYLOAD                            */
/*****************************************************************************/

struct IncrementPayload {
    std::vector<std::pair<Utf8String, Utf8String>> keys;
    std::vector<double> outcomes;
};

DECLARE_STRUCTURE_DESCRIPTION(IncrementPayload);
DEFINE_STRUCTURE_DESCRIPTION(IncrementPayload);

IncrementPayloadDescription::
IncrementPayloadDescription()
{
    addField("keys",   &IncrementPayload::keys, "");
    addField("outcomes", &IncrementPayload::outcomes, "");
}


struct PersistPayload {
    Url modelFileUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(PersistPayload);
DEFINE_STRUCTURE_DESCRIPTION(PersistPayload);

PersistPayloadDescription::
PersistPayloadDescription()
{
    addField("modelFileUrl", &PersistPayload::modelFileUrl, "");
}



/*****************************************************************************/
/* DIST TABLE FUNCTION                                                       */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(DistTableFunctionConfig);

DistTableFunctionConfigDescription::
DistTableFunctionConfigDescription()
{
    std::vector<Utf8String> defaultStats = { "count", "avg", "std", "min", "max" };
    addField("statistics", &DistTableFunctionConfig::statistics,
             "List of statistics to track for each outcome.", defaultStats);

    addField("distTableFileUrl", &DistTableFunctionConfig::modelFileUrl,
             "URL of the model file (with extension '.dt') to load. "
             "This file is created by the ![](%%doclink experimental.distTable.train procedure).");
}

DistTableFunction::
DistTableFunction(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner, config)
{
    functionConfig = config.params.convert<DistTableFunctionConfig>();

    int version;
    int REQUIRED_VERSION = 2;
    int i_mode;

    // Load saved stats tables
    filter_istream stream(functionConfig.modelFileUrl);
    ML::DB::Store_Reader store(stream);
    store >> version;
    if(version != REQUIRED_VERSION)
        throw MLDB::Exception("Wrong DistTable map version");

    store >> i_mode;
    mode = (DistTableMode)i_mode;

    if(mode != DT_MODE_BAG_OF_WORDS && mode != DT_MODE_FIXED_COLUMNS)
        throw MLDB::Exception("Unsupported DistTable mode");

    store >> distTablesMap;

    // build a cache of the names for quick access
    for(int i=0; i<DT_NUM_STATISTICS; i++)
        dtStatsNames[(DISTTABLE_STATISTICS)i] = print((DISTTABLE_STATISTICS)i);

    // configure active statistics
    if(functionConfig.statistics.size() == 0)
        functionConfig.statistics = { "count", "avg", "std", "min", "max" };

    for(auto & stat : functionConfig.statistics) {
        activeStats.push_back(parseDistTableStatistic(stat));
    }
}

RestRequestMatchResult
DistTableFunction::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    if (context.remaining == "/increment") {

        StreamingJsonParsingContext jsonContext("input",
                request.payload.c_str(), request.payload.size());

        auto descPtr = getDefaultDescriptionSharedT<IncrementPayload>();
        auto & desc = *descPtr;
        IncrementPayload result;
        desc.parseJsonTyped(&result, jsonContext);

        increment(result.keys, result.outcomes);

        connection.sendResponse(200, Json::Value("OK"), "application/json");
        return RestRequestRouter::MR_YES;
    }
    else if (context.remaining == "/persist") {
        StreamingJsonParsingContext jsonContext("input",
                request.payload.c_str(), request.payload.size());

        auto descPtr = getDefaultDescriptionSharedT<PersistPayload>();
        auto & desc = *descPtr;
        PersistPayload result;
        desc.parseJsonTyped(&result, jsonContext);

        persist(result.modelFileUrl);

        connection.sendResponse(200, Json::Value("OK"), "application/json");
        return RestRequestRouter::MR_YES;

    }

    return Function::handleRequest(connection, request, context);
}

void
DistTableFunction::
increment(const vector<pair<Utf8String, Utf8String>> & keys,
          const vector<double> & outcomes) const
{
    // get exclusive access
    boost::unique_lock<boost::shared_mutex> uniqueLock(_access);

    for(const auto & key : keys) {
        Path pKey(key.first);
        auto table_it = distTablesMap.find(pKey);
        if(table_it == distTablesMap.end())
            throw MLDB::Exception("Unknown dist table '"+
                        key.first.utf8String()+"'");

        //for(double outcome : outcomes)
        (*table_it).second.increment(key.second, outcomes);
    }
}

void
DistTableFunction::
persist(const Url & modelFileUrl) const
{
    // get exclusive access
    DistTablesMap copiedMap;
    {
        boost::shared_lock<boost::shared_mutex> uniqueLock(_access);
        copiedMap = DistTablesMap(distTablesMap);
    }

    DistTableProcedure::persist(modelFileUrl,
            mode, copiedMap);
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
    auto onAtomFixedColumns =
        [&] (const ColumnPath & columnName,
             const ColumnPath & prefix,
             const CellValue & val,
             Date ts)
    {
        auto st = distTablesMap.find(columnName);
        if (st == distTablesMap.end())
            return true;

        const DistTable & distTable = st->second;
        const auto & stats = distTable.getStats(val.toUtf8String());
        for (int i=0; i < distTable.outcome_names.size(); ++i) {
            for(DISTTABLE_STATISTICS sid : activeStats) {
                rtnRow.emplace_back(
                    PathElement(distTable.outcome_names[i]) + columnName
                                + dtStatsNames[sid],
                    stats[i].getStat(sid),
                    ts);
            }
        }

        return true;
    };

    auto onAtomBow =
        [&] (const ColumnPath & columnName,
             const ColumnPath & prefix,
             const CellValue & val,
             Date ts)
    {
        if (val.empty())
            return true;

        const DistTable & distTable = distTablesMap.begin()->second;
        const auto & stats = distTable.getStats(columnName.toUtf8String());
        for (int i=0; i < distTable.outcome_names.size(); ++i) {
            for(DISTTABLE_STATISTICS sid : activeStats) {
                rtnRow.emplace_back(
                    PathElement(distTable.outcome_names[i]) + columnName
                                + dtStatsNames[sid],
                    stats[i].getStat(sid),
                    ts);
            }
        }

        return true;
    };

    // get reader lock
    {
        boost::shared_lock<boost::shared_mutex> lock(_access);
        switch(mode) {
            case DT_MODE_FIXED_COLUMNS:
                arg.forEachAtom(onAtomFixedColumns);
                break;
            case DT_MODE_BAG_OF_WORDS:
                arg.forEachAtom(onAtomBow);
                break;
        }
    }

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

    result.input.emplace_back(std::make_shared<RowValueInfo>(inputColumns, SCHEMA_CLOSED));
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

