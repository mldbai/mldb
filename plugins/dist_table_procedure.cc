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
    } else {
        uint64_t n = ++counts[featureValue][outcome];
        avgs[featureValue][outcome] =
             (avgs[featureValue][outcome] * (n - 1) + targetValue) / n;
    }
}

void
DistTable::
getStats(const Utf8String & featureValue, int outcome, bool & out_notNull, tuple<uint64_t, double> & out_stats) const
{
    out_notNull = counts.count(featureValue)
              && counts.at(featureValue).count(outcome);
    if (out_notNull) {
        auto count = counts.at(featureValue).at(outcome);
        auto avg = avgs.at(featureValue).at(outcome);
        out_stats = tie(count, avg);
    }
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
          << version << colName << outcome_names << counts << avgs; }

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

    store >> colName >> outcome_names >> counts >> avgs;
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
             "must be a boolean (0 or 1)");
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
    cerr << "** IN RUN **"<< endl;

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
            cerr << "** IN processor **"<< endl;
            MatrixNamedRow row = row_.flattenDestructive();

            cerr << row.rowName << " : ";
            for (const auto & col : row.columns) {
                cerr << get<0>(col) << "=" << get<1>(col) << ", ";
            }

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
                    bool notNull =
                        distTable.counts.count(featureValue)
                        && distTable.counts[featureValue].count(i);
                    // count
                    output_cols.emplace_back(
                        PathElement(outcome_names[i]) + featureColumnName + "count",
                        notNull ? CellValue(distTable.counts[featureValue][i]) : CellValue(),
                        Date::negativeInfinity());

                    // avg
                    output_cols.emplace_back(
                        PathElement(outcome_names[i]) + featureColumnName + "avg",
                        notNull ? CellValue(distTable.avgs[featureValue][i]) : CellValue(),
                        Date::negativeInfinity());
                            
                } 

                // increment stats tables with current row
                // for each target we increment the aggregators
                for (int i=0; i < nbOutcomes; ++i) {
                    if (haveTarget[i]) {
                        distTable.increment(featureValue, i, targets[i]);
                    }
                }

                    // *******
                    // column name caching
                    // TODO for speed
            //         auto colIt = colCache.find(get<0>(col));
            //         vector<ColumnName> * colNames;
            //         if(colIt != colCache.end()) {
            //             colNames = &(colIt->second);
            //         }
            //         else {
                        // if we didn't compute the column names yet, do that now
            //             const Path & key = std::get<0>(col);

            //             vector<ColumnName> names;
            //             names.emplace_back(PathElement("trial") + key);
            //             for(int lbl_idx=0; lbl_idx<encodedLabels.size(); lbl_idx++) {
            //                 names.emplace_back(PathElement(outcome_names[lbl_idx]) + key);
            //             }

            //             auto inserted = colCache.emplace(get<0>(col), names);
            //             colNames = &((*(inserted.first)).second);
            //         }


            //         output_cols.emplace_back(colNames->at(0),
            //                                  counts.first - 1,
            //                                  get<2>(col));

                    // add all outcomes
            //         for(int lbl_idx=0; lbl_idx<encodedLabels.size(); lbl_idx++)
            //             output_cols.emplace_back(colNames->at(lbl_idx+1),
            //                                      counts.second[lbl_idx] - encodedLabels[lbl_idx],
            //                                      get<2>(col));

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

    ExpressionValue arg = context.getColumn("keys");

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
                    bool notNull;
                    tuple<uint64_t, double> stats;
                    distTable.getStats(val.toString(), i, notNull, stats);

                    rtnRow.emplace_back(
                        PathElement(distTable.outcome_names[i]) + columnName
                                    + "count", get<0>(stats), ts);
                    rtnRow.emplace_back(
                        PathElement(distTable.outcome_names[i]) + columnName
                                    + "avg", get<1>(stats), ts);
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
    inputColumns.emplace_back(PathElement("keys"), std::make_shared<UnknownRowValueInfo>(),
                              COLUMN_IS_DENSE, 0);
    outputColumns.emplace_back(PathElement("counts"), std::make_shared<UnknownRowValueInfo>(),
                               COLUMN_IS_DENSE, 0);
    
    result.input.reset(new RowValueInfo(inputColumns, SCHEMA_CLOSED));
    result.output.reset(new RowValueInfo(outputColumns, SCHEMA_CLOSED));
    
    return result;
}


#if 0
/*****************************************************************************/
/* DIST TABLE FUNCTION                                                      */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(DistTableDerivedColumnsGeneratorProcedureConfig);

DistTableDerivedColumnsGeneratorProcedureConfigDescription::
DistTableDerivedColumnsGeneratorProcedureConfigDescription()
{
    addParent<ProcedureConfig>();
    addField("functionId", &DistTableDerivedColumnsGeneratorProcedureConfig::functionId,
            "ID to use for the instance of the ![](%%doclink sql.expression function) that will be created");
    addField("distTableFileUrl", &DistTableDerivedColumnsGeneratorProcedureConfig::modelFileUrl,
             "URL of the model file (with extension '.st') to load. "
             "This file is created by the ![](%%doclink distTable.train procedure).");
    addField("expression", &DistTableDerivedColumnsGeneratorProcedureConfig::expression,
             "Expression to be expanded");
}


DistTableDerivedColumnsGeneratorProcedure::
DistTableDerivedColumnsGeneratorProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->procConfig = config.params.convert<DistTableDerivedColumnsGeneratorProcedureConfig>();
}

Any
DistTableDerivedColumnsGeneratorProcedure::
getStatus() const
{
    return Any();
}

RunOutput
DistTableDerivedColumnsGeneratorProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{
    DistTableDerivedColumnsGeneratorProcedureConfig runProcConf =
        applyRunConfOverProcConf(procConfig, run);

    // Load saved stats tables
    filter_istream stream(runProcConf.modelFileUrl.toString());
    ML::DB::Store_Reader store(stream);

    DistTablesMap distTables;
    store >> distTables;

    /*******/
    // Expand the expression. this is painfully naive and slow for now
    vector<std::string> stNames; // UTF-8 encoded raw strings
    vector<std::string> tempExpressions;  // UTF-8 encoded raw strings


    // mettre toutes les expression dans une liste
    for(auto it=distTables.begin(); it != distTables.end(); it++) {
        stNames.emplace_back(it->first.toUtf8String().rawString());
        tempExpressions.emplace_back(runProcConf.expression);
    }

    // looper sur toutes les replace. trial, outcome1, outcome2. si un replace retourne 0, skip
    auto do_replace = [&] (const std::string & outcome)
        {
            for(int i=0; i<tempExpressions.size(); i++) {
                if(!ML::replace_all(tempExpressions[i], outcome,
                                    outcome+"."+stNames[i]))
                    return;
            }
        };

    do_replace("trial");
    for(const string & outcomeName: distTables.begin()->second.outcome_names) {
        do_replace(outcomeName);
    }

    // replace explicit $tbl
    for(int i=0; i<tempExpressions.size(); i++) {
        if(!ML::replace_all(tempExpressions[i], "$tbl", stNames[i]))
            break;
    }


    // assemble the final select expression
    std::string assembled;
    for(auto & ts : tempExpressions)
        assembled += ts + ",";
    assembled.pop_back();


    ////////////////
    // Now create an sql.expression function using the expression to execute the
    // expanded expression
    PolyConfig funcPC;
    funcPC.id = runProcConf.functionId;
    funcPC.type = "sql.expression";

    Json::Value expression;
    expression["expression"] = assembled;
    funcPC.params = jsonEncode(expression);

    createFunction(server, funcPC, onProgress, true /* overwrite */);

    return RunOutput(funcPC);
}


/*****************************************************************************/
/* DIST TABLE POS NEG FUNCTION                                              */
/*****************************************************************************/
DEFINE_STRUCTURE_DESCRIPTION(DistTablePosNegFunctionConfig);

DistTablePosNegFunctionConfigDescription::
DistTablePosNegFunctionConfigDescription()
{
    addField("numPos", &DistTablePosNegFunctionConfig::numPos,
            "Number of top positive words to use", ssize_t(50));
    addField("numNeg", &DistTablePosNegFunctionConfig::numNeg,
            "Number of top negative words to use", ssize_t(50));
    addField("minTrials", &DistTablePosNegFunctionConfig::minTrials,
            "Minimum number of trials a word needs to have in order "
            "to be considered", ssize_t(50));
    addField("outcomeToUse", &DistTablePosNegFunctionConfig::outcomeToUse,
            "This must be one of the outcomes the stats "
            "table was trained with.");
    addField("distTableFileUrl", &DistTablePosNegFunctionConfig::modelFileUrl,
             "URL of the model file (with extension '.st') to load. "
             "This file is created by the ![](%%doclink distTable.bagOfWords.train procedure).");
}

DistTablePosNegFunction::
DistTablePosNegFunction(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<DistTablePosNegFunctionConfig>();

    DistTable distTable;

    // Load saved stats tables
    filter_istream stream(functionConfig.modelFileUrl.toString());
    ML::DB::Store_Reader store(stream);
    store >> distTable;

    // find the index of the outcome we're requesting in the config
    int outcomeToUseIdx = -1;
    for(int i=0; i<distTable.outcome_names.size(); i++) {
        if(distTable.outcome_names[i] == functionConfig.outcomeToUse) {
            outcomeToUseIdx = i;
        }
    }
    if(outcomeToUseIdx == -1) {
        throw HttpReturnException(400, "Outcome '"+functionConfig.outcomeToUse+
                                  "' not found in stats table!");
    }

    // sort all the keys by their p(outcome)
    vector<pair<Utf8String, float>> accum;
    for(auto it = distTable.counts.begin(); it!=distTable.counts.end(); it++) {
        const pair<int64_t, std::vector<int64_t>> & counts = it->second;

        if(counts.first < functionConfig.minTrials)
            continue;

        float poutcome = counts.second[outcomeToUseIdx] / float(counts.first);
        accum.push_back(make_pair(it->first, poutcome));
    }

    if(accum.size() < functionConfig.numPos + functionConfig.numNeg) {
        for(const auto & col : accum)
            p_outcomes.insert(col);
    }
    else {
        auto compareFunc = [](const pair<Utf8String, float> & a,
                           const pair<Utf8String, float> & b)
            {
                return a.second > b.second;
            };
        std::sort(accum.begin(), accum.end(), compareFunc);

        for(int i=0; i<functionConfig.numPos; i++) {
            auto & a = accum[i];
            p_outcomes.insert(make_pair(a.first, a.second));
        }

        for(int i=0; i<functionConfig.numNeg; i++) {
            auto & a = accum[accum.size() - 1 - i];
            p_outcomes.insert(make_pair(a.first, a.second));
        }
    }
}

DistTablePosNegFunction::
~DistTablePosNegFunction()
{
}

Any
DistTablePosNegFunction::
getStatus() const
{
    return Any();
}

Any
DistTablePosNegFunction::
getDetails() const
{
    return Any();
}

ExpressionValue
DistTablePosNegFunction::
apply(const FunctionApplier & applier,
      const ExpressionValue & context) const
{
    StructValue result;

    ExpressionValue arg = context.getColumn("words");

    if(arg.isRow()) {
        RowValue rtnRow;

        // TODO should we cache column names as we did in the procedure?
        auto onAtom = [&] (const ColumnName & columnName,
                           const ColumnName & prefix,
                           const CellValue & val,
                           Date ts)
            {

                auto it = p_outcomes.find(columnName.toUtf8String());
                if(it == p_outcomes.end()) {
                    return true;
                }

                rtnRow.emplace_back(columnName
                                    + PathElement(functionConfig.outcomeToUse),
                                    it->second,
                                    ts);

                return true;
            };

        arg.forEachAtom(onAtom);
        result.emplace_back("probs", ExpressionValue(std::move(rtnRow)));
    }
    else {
        cerr << jsonEncode(arg) << endl;
        throw HttpReturnException(400, "distTable.bagOfWords.posneg : expect 'keys' as a row");
    }
    
    return std::move(result);
}

FunctionInfo
DistTablePosNegFunction::
getFunctionInfo() const
{
    FunctionInfo result;
    
    std::vector<KnownColumn> inputColumns, outputColumns;
    inputColumns.emplace_back(PathElement("words"), std::make_shared<UnknownRowValueInfo>(),
                              COLUMN_IS_DENSE, 0);
    outputColumns.emplace_back(PathElement("probs"), std::make_shared<UnknownRowValueInfo>(),
                               COLUMN_IS_DENSE, 0);
    
    result.input.reset(new RowValueInfo(inputColumns, SCHEMA_CLOSED));
    result.output.reset(new RowValueInfo(outputColumns, SCHEMA_CLOSED));
    
    return result;
}


#endif
/*****************************************************************************/
/*****************************************************************************/
/*****************************************************************************/

namespace {

RegisterProcedureType<DistTableProcedure, DistTableProcedureConfig>
regSTTrain(builtinPackage(),
           "Create statistical tables of trials against outcomes",
           "procedures/DistTableProcedure.md.html");

RegisterFunctionType<DistTableFunction, DistTableFunctionConfig>
regClassifyFunction(builtinPackage(),
                    "distTable.getStats",
                    "Get stats table counts for a row of keys",
                    "functions/DistTableGetCounts.md.html");

#if 0
RegisterProcedureType<DistTableDerivedColumnsGeneratorProcedure,
                      DistTableDerivedColumnsGeneratorProcedureConfig>
regSTDerColGenProc(builtinPackage(),
                   "Generate an sql.expression function to be used to compute "
                   "derived statistics from a stats table",
                   "procedures/DistTableDerivedColumnsGeneratorProcedure.md.html",
                   nullptr,
                   { MldbEntity::INTERNAL_ENTITY });


RegisterProcedureType<BagOfWordsDistTableProcedure,
                      BagOfWordsDistTableProcedureConfig>
regPosNegTrain(builtinPackage(),
           "Create statistical tables of trials against outcomes for bag of words",
           "procedures/BagOfWordsDistTableProcedure.md.html");

RegisterFunctionType<DistTablePosNegFunction, DistTablePosNegFunctionConfig>
regPosNegFunction(builtinPackage(),
                    "distTable.bagOfWords.posneg",
                    "Get the pos/neg p(outcome)",
                    "functions/BagOfWordsDistTablePosNeg.md.html");
#endif

} // file scope

} // namespace MLDB
} // namespace Datacratic
