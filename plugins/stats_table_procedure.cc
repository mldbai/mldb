// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** stats_table_procedure.cc
    Francois Maillet, 2 septembre 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Stats table procedure
*/

#include "stats_table_procedure.h"
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
#include "server/function_contexts.h"
#include "server/bound_queries.h"
#include "mldb/http/http_exception.h"
#include "mldb/jml/db/persistent.h"
#include "mldb/types/jml_serialization.h"
#include "mldb/vfs/filter_streams.h"


using namespace std;

namespace Datacratic {
namespace MLDB {




/*****************************************************************************/
/* STATS TABLE                                                               */
/*****************************************************************************/

StatsTable::
StatsTable(const std::string & filename)
{
    ML::filter_istream stream(filename);
    ML::DB::Store_Reader store(stream);
    reconstitute(store);
}

const StatsTable::BucketCounts &
StatsTable::
increment(const CellValue & val, const vector<uint> & outcomes) {
    Utf8String key = val.toUtf8String();
    auto it = counts.find(key);
    if(it == counts.end()) {
        auto rtn = counts.emplace(key,
                std::move(make_pair(1, vector<int64_t>(outcomes.begin(),
                                                    outcomes.end()))));

        // return inserted value
        return (*(rtn.first)).second;
    }

    it->second.first ++;
    for(int i=0; i<outcomes.size(); i++)
        it->second.second[i] += outcomes[i];

    return it->second;
}

const StatsTable::BucketCounts & 
StatsTable::
getCounts(const CellValue & val) const
{
    Utf8String key = val.toUtf8String();
    auto it = counts.find(key);
    if(it == counts.end()) {
        return zeroCounts;
    }

    return it->second;
}

void StatsTable::
save(const std::string & filename) const
{
    ML::filter_ostream stream(filename);
    ML::DB::Store_Writer store(stream);
    serialize(store);
}

void StatsTable::
serialize(ML::DB::Store_Writer & store) const
{
    int version = 1;
    store << version << colName << outcome_names << counts << zeroCounts;
}

void StatsTable::
reconstitute(ML::DB::Store_Reader & store)
{
    int version;
    int REQUIRED_V = 1;
    store >> version;
    if(version!=REQUIRED_V) {
        throw ML::Exception(ML::format(
                    "invalid StatsTable version! exptected %d, got %d",
                    REQUIRED_V, version));
    }

    store >> colName >> outcome_names >> counts >> zeroCounts;
}


/*****************************************************************************/
/* STATS TABLE PROCEDURE CONFIG                                              */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(StatsTableProcedureConfig);

StatsTableProcedureConfigDescription::
StatsTableProcedureConfigDescription()
{
    addFieldDesc("trainingDataset", &StatsTableProcedureConfig::dataset,
                 "Dataset on which the rolling operations will be performed.",
                 makeInputDatasetDescription());
    addField("outputDataset", &StatsTableProcedureConfig::output,
             "Output dataset",
             PolyConfigT<Dataset>().withType("sparse.mutable"));
    addField("select", &StatsTableProcedureConfig::select,
             "Select these columns (default all columns).  Only plain "
             "column names may be "
             "used; it is not possible to select on an expression (like x + 1)",
             SelectExpression::STAR);
    addField("where", &StatsTableProcedureConfig::where,
             "Only use rows matching this clause (default all rows)",
             SqlExpression::TRUE);
    addField("when", &StatsTableProcedureConfig::when,
             "Only use values matching this timestamp expression (default all values)",
             WhenExpression::TRUE);
    addField("orderBy", &StatsTableProcedureConfig::orderBy,
             "Process rows in this order. This is extermely important because "
             "each row's counts will be influenced by rows that came before it.",
             OrderByExpression::ROWHASH);
    addField("outcomes", &StatsTableProcedureConfig::outcomes,
             "List of expressions to generate the outcomes. Each can be any expression "
             "involving the columns in the dataset. The type of the outcomes "
             "must be a boolean (0 or 1)");
    addField("statsTableFileUrl", &StatsTableProcedureConfig::statsTableFileUrl,
             "URL where the stats table file (with extension '.st') should be saved. "
             "This file can be loaded by a function of type 'statsTable.getCounts'.");
    addField("functionName", &StatsTableProcedureConfig::functionName,
             "If specified, a 'statsTable.getCounts' function of this name will be "
             "created using the trained stats tables.");
}



/*****************************************************************************/
/* STATS TABLE PROCEDURE                                                     */
/*****************************************************************************/

StatsTableProcedure::
StatsTableProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    procConfig = config.params.convert<StatsTableProcedureConfig>();
}

Any
StatsTableProcedure::
getStatus() const
{
    return Any();
}

RunOutput
StatsTableProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{

    StatsTableProcedureConfig runProcConf =
        applyRunConfOverProcConf(procConfig, run);

    SqlExpressionMldbContext context(server);
    auto boundDataset = runProcConf.dataset->bind(context);
    
    vector<string> outcome_names;
    for(const pair<string, std::shared_ptr<SqlExpression>> & lbl : runProcConf.outcomes)
        outcome_names.push_back(lbl.first);

    StatsTablesMap statsTables;

    {
        // Find only those variables used
        SqlExpressionDatasetContext context(boundDataset);

        auto selectBound = runProcConf.select.bind(context);

        for (auto & c: selectBound.info->getKnownColumns()) {
            statsTables.insert(make_pair(c.columnName, StatsTable(c.columnName, outcome_names)));
            cout << c.columnName << endl;
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
    map<ColumnName, vector<ColumnName>> colCache;

    auto aggregator = [&] (const MatrixNamedRow & row,
                           const std::vector<ExpressionValue> & extraVals)
        {
            if(num_req++ % 5000 == 0) {
                double secs = Date::now().secondsSinceEpoch() - start.secondsSinceEpoch();
                string progress = ML::format("done %d. %0.4f/sec", num_req, num_req / secs);
                onProgress(progress);
                cerr << progress << endl;
            }

            vector<uint> encodedLabels;
            for(int lbl_idx=0; lbl_idx<runProcConf.outcomes.size(); lbl_idx++) {
                CellValue outcome = extraVals.at(lbl_idx).getAtom();
                encodedLabels.push_back( !outcome.empty() && outcome.isTrue() );
            }
  
            map<ColumnName, size_t> column_idx;
            for(int i=0; i<row.columns.size(); i++) {
                column_idx.insert(make_pair(get<0>(row.columns[i]), i)); 
            }

            std::vector<std::tuple<ColumnName, CellValue, Date> > output_cols;
            for(auto it = statsTables.begin(); it != statsTables.end(); it++) {
                // is this col present for row?
                auto col_ptr = column_idx.find(it->first);
                if(col_ptr == column_idx.end()) {
                    // TODO handle unknowns
                    //output_cols.emplace_back(get<0>(col), count, get<2>(col));
                }
                else {
                    const tuple<ColumnName, CellValue, Date> & col = row.columns[col_ptr->second];
                    const StatsTable::BucketCounts & counts = it->second.increment(get<1>(col), encodedLabels);

                    // *******
                    // column name caching
                    auto colIt = colCache.find(get<0>(col));
                    vector<ColumnName> * colNames;
                    if(colIt != colCache.end()) {
                        colNames = &(colIt->second);
                    }
                    else {
                        // if we didn't compute the column names yet, do that now
                        Utf8String keySuffix = get<0>(col).toUtf8String();

                        vector<ColumnName> names;
                        names.emplace_back(Id("trial_"+keySuffix));
                        for(int lbl_idx=0; lbl_idx<encodedLabels.size(); lbl_idx++) {
                            names.emplace_back(Id(outcome_names[lbl_idx]+"_"+keySuffix));
                        }

                        auto inserted = colCache.emplace(get<0>(col), names);
                        colNames = &((*(inserted.first)).second);
                    }


                    output_cols.emplace_back(colNames->at(0),
                                             counts.first - 1,
                                             get<2>(col));

                    // add all outcomes
                    for(int lbl_idx=0; lbl_idx<encodedLabels.size(); lbl_idx++) {
                        output_cols.emplace_back(colNames->at(lbl_idx+1),
                                                 counts.second[lbl_idx] - encodedLabels[lbl_idx],
                                                 get<2>(col));
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

    iterateDataset(runProcConf.select,
                   *boundDataset.dataset, boundDataset.asName, 
                   runProcConf.when, runProcConf.where,
                   extra,
                   aggregator, runProcConf.orderBy);

    output->commit();

    // save if required
    if(!runProcConf.statsTableFileUrl.empty()) {
        ML::filter_ostream stream(runProcConf.statsTableFileUrl.toString());
        ML::DB::Store_Writer store(stream);
        store << statsTables;
    }
    
    if(!runProcConf.statsTableFileUrl.empty() && !runProcConf.functionName.empty()) {
        cerr << "Saving stats tables to " << runProcConf.statsTableFileUrl.toString() << endl;
        PolyConfig clsFuncPC;
        clsFuncPC.type = "statsTable.getCounts";
        clsFuncPC.id = runProcConf.functionName;
        clsFuncPC.params = StatsTableFunctionConfig(runProcConf.statsTableFileUrl);

        obtainFunction(server, clsFuncPC, onProgress);
    }

    return RunOutput();
}





/*****************************************************************************/
/* STATS TABLE FUNCTION                                                      */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(StatsTableFunctionConfig);

StatsTableFunctionConfigDescription::
StatsTableFunctionConfigDescription()
{
    addField("statsTableFileUrl", &StatsTableFunctionConfig::statsTableFileUrl,
             "URL of the stats tables file (with extension '.st') to load. "
             "This file is created by a procedure of type 'statsTable.train'.");
}

StatsTableFunction::
StatsTableFunction(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<StatsTableFunctionConfig>();

    // Load saved stats tables
    ML::filter_istream stream(functionConfig.statsTableFileUrl.toString());
    ML::DB::Store_Reader store(stream);
    store >> statsTables;
}

StatsTableFunction::
~StatsTableFunction()
{
}

Any
StatsTableFunction::
getStatus() const
{
    Json::Value result;
    return result;
}

Any
StatsTableFunction::
getDetails() const
{
    return Any();
}

FunctionOutput
StatsTableFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
    FunctionOutput result;

    ExpressionValue args = context.get<ExpressionValue>("keys");

    if(args.isObject()) {
        RowValue rtnRow;

        // TODO should we cache column names as we did in the procedure?
        auto onAtom = [&] (const ColumnName & columnName,
                           const ColumnName & prefix,
                           const CellValue & val,
                           Date ts)
            {
                auto st = statsTables.find(columnName);
                if(st == statsTables.end())
                    return true;

                auto counts = st->second.getCounts(val);

                auto strCol = columnName.toUtf8String(); 
                rtnRow.push_back(make_tuple(Id("trial_"+strCol), counts.first, ts));

                for(int lbl_idx=0; lbl_idx<st->second.outcome_names.size(); lbl_idx++) {
                    rtnRow.push_back(make_tuple(Id(st->second.outcome_names[lbl_idx]+"_"+strCol),
                                                counts.second[lbl_idx],
                                                ts));
                }

                return true;
            };

        args.forEachAtom(onAtom);
        result.set("counts", ExpressionValue(std::move(rtnRow)));
    }
    else {
        throw ML::Exception("wrong input type");
    }

    return result;
}

FunctionInfo
StatsTableFunction::
getFunctionInfo() const
{
    FunctionInfo result;
    result.input.addRowValue("keys");
    result.output.addRowValue("counts");
    return result;
}


/*****************************************************************************/
/* STATS TABLE FUNCTION                                                      */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(StatsTableDerivedColumnsGeneratorProcedureConfig);

StatsTableDerivedColumnsGeneratorProcedureConfigDescription::
StatsTableDerivedColumnsGeneratorProcedureConfigDescription()
{
    addField("functionId", &StatsTableDerivedColumnsGeneratorProcedureConfig::functionId,
            "ID to use for the sql.expression function that will be created");
    addField("statsTableFileUrl", &StatsTableDerivedColumnsGeneratorProcedureConfig::statsTableFileUrl,
             "URL of the stats tables file (with extension '.st') to load. "
             "This file is created by a procedure of type 'statsTable.train'.");
    addField("expression", &StatsTableDerivedColumnsGeneratorProcedureConfig::expression,
             "Expression to be expanded");
}


StatsTableDerivedColumnsGeneratorProcedure::
StatsTableDerivedColumnsGeneratorProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->procConfig = config.params.convert<StatsTableDerivedColumnsGeneratorProcedureConfig>();
}

Any
StatsTableDerivedColumnsGeneratorProcedure::
getStatus() const
{
    return Any();
}

RunOutput
StatsTableDerivedColumnsGeneratorProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{
    StatsTableDerivedColumnsGeneratorProcedureConfig runProcConf =
        applyRunConfOverProcConf(procConfig, run);

    // Load saved stats tables
    ML::filter_istream stream(runProcConf.statsTableFileUrl.toString());
    ML::DB::Store_Reader store(stream);
    
    StatsTablesMap statsTables;
    store >> statsTables;

    /*******/
    // Expand the expression. this is painfully naive and slow for now
    vector<string> stNames;
    vector<string> tempExpressions;


    // mettre toutes les expression dans une liste
    for(auto it=statsTables.begin(); it != statsTables.end(); it++) {
        stNames.emplace_back(it->first.toString());
        tempExpressions.emplace_back(runProcConf.expression);
    }

    // looper sur toutes les replace. trial, outcome1, outcome2. si un replace retourne 0, skip
    auto do_replace = [&] (const string & outcome)
        {
            for(int i=0; i<tempExpressions.size(); i++) {
                if(!ML::replace_all(tempExpressions[i], outcome, outcome+"_"+stNames[i]))
                    return;
            }
        };

    do_replace("trial");
    for(const string & outcomeName: statsTables.begin()->second.outcome_names) {
        do_replace(outcomeName);
    }

    // replace explicit $tbl
    for(int i=0; i<tempExpressions.size(); i++) {
        if(!ML::replace_all(tempExpressions[i], "$tbl", stNames[i]))
            break;
    }


    // assemble the final select expression
    string assembled = "";
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
/* BAG OF WORDS STATS TABLE PROCEDURE CONFIG                                 */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(BagOfWordsStatsTableProcedureConfig);

BagOfWordsStatsTableProcedureConfigDescription::
BagOfWordsStatsTableProcedureConfigDescription()
{
    addFieldDesc("trainingDataset", &BagOfWordsStatsTableProcedureConfig::dataset,
                 "Dataset on which the rolling operations will be performed.",
                 makeInputDatasetDescription());
    addField("select", &BagOfWordsStatsTableProcedureConfig::select,
             "Select these columns (default all columns).  Only plain "
             "column names may be "
             "used; it is not possible to select on an expression (like x + 1)",
             SelectExpression::STAR);
    addField("where", &BagOfWordsStatsTableProcedureConfig::where,
             "Only use rows matching this clause (default all rows)",
             SqlExpression::TRUE);
    addField("when", &BagOfWordsStatsTableProcedureConfig::when,
             "Only use values matching this timestamp expression (default all values)",
             WhenExpression::TRUE);
    addField("orderBy", &BagOfWordsStatsTableProcedureConfig::orderBy,
             "Process rows in this order. This is extermely important because "
             "each row's counts will be influenced by rows that came before it.",
             OrderByExpression::ROWHASH);
    addField("outcomes", &BagOfWordsStatsTableProcedureConfig::outcomes,
             "List of expressions to generate the outcomes. Each can be any expression "
             "involving the columns in the dataset. The type of the outcomes "
             "must be a boolean (0 or 1)");
    addField("statsTableFileUrl", &BagOfWordsStatsTableProcedureConfig::statsTableFileUrl,
             "URL where the stats table file (with extension '.st') should be saved. "
             "This file can be loaded by a function of type 'statsTable.bagOfWords.posneg'.");
    addParent<ProcedureConfig>();
}

/*****************************************************************************/
/* BOW STATS TABLE PROCEDURE                                                 */
/*****************************************************************************/

BagOfWordsStatsTableProcedure::
BagOfWordsStatsTableProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    procConfig = config.params.convert<StatsTableProcedureConfig>();
}

Any
BagOfWordsStatsTableProcedure::
getStatus() const
{
    return Any();
}

RunOutput
BagOfWordsStatsTableProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{

    StatsTableProcedureConfig runProcConf =
        applyRunConfOverProcConf(procConfig, run);

    SqlExpressionMldbContext context(server);
    auto boundDataset = runProcConf.dataset->bind(context);
    
    vector<string> outcome_names;
    for(const pair<string, std::shared_ptr<SqlExpression>> & lbl : runProcConf.outcomes)
        outcome_names.push_back(lbl.first);

    StatsTable statsTable(ColumnName("words"), outcome_names);


    auto onProgress2 = [&] (const Json::Value & progress)
        {
            Json::Value value;
            value["dataset"] = progress;
            return onProgress(value);
        };

    int num_req = 0;
    Date start = Date::now();

    auto aggregator = [&] (const MatrixNamedRow & row,
                           const std::vector<ExpressionValue> & extraVals)
        {
            if(num_req++ % 10000 == 0) {
                double secs = Date::now().secondsSinceEpoch() - start.secondsSinceEpoch();
                string progress = ML::format("done %d. %0.4f/sec", num_req, num_req / secs);
                onProgress(progress);
                cerr << progress << endl;
            }

            vector<uint> encodedLabels;
            for(int lbl_idx=0; lbl_idx<runProcConf.outcomes.size(); lbl_idx++) {
                CellValue outcome = extraVals.at(lbl_idx).getAtom();
                encodedLabels.push_back( !outcome.empty() && outcome.isTrue() );
            }
  
            for(const std::tuple<ColumnName, CellValue, Date> & col : row.columns) {
                statsTable.increment(CellValue(get<0>(col).toUtf8String()), encodedLabels);
            }

            return true;
        };


    // We want to calculate the outcome and weight of each row as well
    // as the select expression
    std::vector<std::shared_ptr<SqlExpression> > extra;
    for(const pair<string, std::shared_ptr<SqlExpression>> & lbl : runProcConf.outcomes)
        extra.push_back(lbl.second);

    iterateDataset(runProcConf.select,
                   *boundDataset.dataset, boundDataset.asName, 
                   runProcConf.when, runProcConf.where,
                   extra,
                   aggregator, runProcConf.orderBy);


    // save if required
    if(!runProcConf.statsTableFileUrl.empty()) {
        ML::filter_ostream stream(runProcConf.statsTableFileUrl.toString());
        ML::DB::Store_Writer store(stream);
        store << statsTable;
    }
    
    return RunOutput();
}


/*****************************************************************************/
/* STATS TABLE POS NEG FUNCTION                                              */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(StatsTablePosNegFunctionConfig);

StatsTablePosNegFunctionConfigDescription::
StatsTablePosNegFunctionConfigDescription()
{
    addField("numPos", &StatsTablePosNegFunctionConfig::numPos,
            "Number of top positive words to use", ssize_t(50));
    addField("numNeg", &StatsTablePosNegFunctionConfig::numNeg,
            "Number of top negative words to use", ssize_t(50));
    addField("minTrials", &StatsTablePosNegFunctionConfig::minTrials,
            "Minimum number of trials a words needs to have "
            "to be considered", ssize_t(50));
    addField("outcomeToUse", &StatsTablePosNegFunctionConfig::outcomeToUse,
            "Outcome to use. This must be one of the outcomes the stats "
            "table was trained with.");
    addField("statsTableFileUrl", &StatsTablePosNegFunctionConfig::statsTableFileUrl,
             "URL of the stats tables file (with extension '.st') to load. "
             "This file is created by a procedure of type 'statsTable.bagOfWords.train'.");
}

StatsTablePosNegFunction::
StatsTablePosNegFunction(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<StatsTablePosNegFunctionConfig>();

    StatsTable statsTable;

    // Load saved stats tables
    ML::filter_istream stream(functionConfig.statsTableFileUrl.toString());
    ML::DB::Store_Reader store(stream);
    store >> statsTable;

    // find the index of the outcome we're requesting in the config
    int outcomeToUseIdx = -1;
    for(int i=0; i<statsTable.outcome_names.size(); i++) {
        if(statsTable.outcome_names[i] == functionConfig.outcomeToUse) {
            outcomeToUseIdx = i;
        }
    }
    if(outcomeToUseIdx == -1) {
        throw ML::Exception("Outcome '"+functionConfig.outcomeToUse+
                "' not found in stats table!");
    }
   
    // sort all the keys by their p(outcome)
    vector<pair<Utf8String, float>> accum;
    for(auto it = statsTable.counts.begin(); it!=statsTable.counts.end(); it++) {
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

StatsTablePosNegFunction::
~StatsTablePosNegFunction()
{
}

Any
StatsTablePosNegFunction::
getStatus() const
{
    return Any();
}

Any
StatsTablePosNegFunction::
getDetails() const
{
    return Any();
}

FunctionOutput
StatsTablePosNegFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
    FunctionOutput result;

    ExpressionValue args = context.get<ExpressionValue>("words");

    if(args.isObject()) {
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

                rtnRow.push_back(make_tuple(Id(columnName.toUtf8String() + "_" + functionConfig.outcomeToUse),
                                            it->second,
                                            ts));

                return true;
            };

        args.forEachAtom(onAtom);
        result.set("probs", ExpressionValue(std::move(rtnRow)));
    }
    else {
        throw ML::Exception("wrong input type");
    }

    return result;
}

FunctionInfo
StatsTablePosNegFunction::
getFunctionInfo() const
{
    FunctionInfo result;
    result.input.addRowValue("words");
    result.output.addRowValue("probs");
    return result;
}



/*****************************************************************************/
/*****************************************************************************/
/*****************************************************************************/

namespace {

RegisterFunctionType<StatsTableFunction, StatsTableFunctionConfig>
regClassifyFunction(builtinPackage(),
                    "statsTable.getCounts",
                    "Get stats table counts for a row of keys",
                    "functions/StatsTableGetCounts.md.html");

RegisterProcedureType<StatsTableDerivedColumnsGeneratorProcedure,
                      StatsTableDerivedColumnsGeneratorProcedureConfig>
regSTDerColGenProc(builtinPackage(),
                   "experimental.statsTable.derivedColumnsGenerator",
                   "Generate an sql.expression function to be used to compute "
                   "derived statistics from a stats table",
                   "procedures/StatsTableDerivedColumnsGeneratorProcedure.md.html",
                   nullptr,
                   { MldbEntity::INTERNAL_ENTITY });

RegisterProcedureType<StatsTableProcedure, StatsTableProcedureConfig>
regSTTrain(builtinPackage(),
           "statsTable.train",
           "Create statistical tables of trials against outcomes",
           "procedures/StatsTableProcedure.md.html");


RegisterProcedureType<BagOfWordsStatsTableProcedure,
                      BagOfWordsStatsTableProcedureConfig>
regPosNegTrain(builtinPackage(),
           "statsTable.bagOfWords.train",
           "Create statistical tables of trials against outcomes for bag of words",
           "procedures/BagOfWordsStatsTableProcedure.md.html");

RegisterFunctionType<StatsTablePosNegFunction, StatsTablePosNegFunctionConfig>
regPosNegFunction(builtinPackage(),
                    "statsTable.bagOfWords.posneg",
                    "Get the pos/neg p(outcome)",
                    "functions/BagOfWordsStatsTablePosNeg.md.html");

} // file scope

} // namespace MLDB
} // namespace Datacratic
