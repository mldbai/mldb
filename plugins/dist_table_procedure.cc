// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** dist_table_procedure.cc                                        -*- C++ -*-
    Simon Lemieux, June 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    distTable procedure

    Note that this was mainly copied from stats_table_procedure.cc in a hurry.
    This will probably be revisited.
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
/* STATS TABLE                                                               */
/*****************************************************************************/

DistTable::
DistTable(const std::string & filename)
{
    filter_istream stream(filename);
    ML::DB::Store_Reader store(stream);
    reconstitute(store);
}

const DistTable::BucketCounts &
DistTable::
increment(const CellValue & val, const vector<uint> & outcomes) {
    Utf8String key = val.toUtf8String();
    auto it = counts.find(key);
    if(it == counts.end()) {
        auto rtn = counts.emplace(
            key, std::move(make_pair(1, vector<int64_t>(outcomes.begin(),
                                                        outcomes.end()))));
        // return inserted value
        return (*(rtn.first)).second;
    }

    it->second.first ++;
    for(int i=0; i<outcomes.size(); i++)
        it->second.second[i] += outcomes[i];

    return it->second;
}

const DistTable::BucketCounts &
DistTable::
getCounts(const CellValue & val) const
{
    Utf8String key = val.toUtf8String();
    auto it = counts.find(key);
    if(it == counts.end()) {
        return zeroCounts;
    }

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
    int version = 2;
    store << string("MLDB Stats Table Binary")
          << version << colName << outcome_names << counts << zeroCounts;
}

void DistTable::
reconstitute(ML::DB::Store_Reader & store)
{
    int version;
    int REQUIRED_V = 2;
    std::string name;
    store >> name >> version;
    if (name != "MLDB Stats Table Binary") {
        throw HttpReturnException(400, "File does not appear to be a stats "
                                  "table model");
    }
    if(version!=REQUIRED_V) {
        throw HttpReturnException(400, ML::format(
                    "invalid DistTable version! exptected %d, got %d",
                    REQUIRED_V, version));
    }

    store >> colName >> outcome_names >> counts >> zeroCounts;
}


/*****************************************************************************/
/* STATS TABLE PROCEDURE CONFIG                                              */
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
             "URL where the model file (with extension '.st') should be saved. "
             "This file can be loaded by the ![](%%doclink distTable.getCounts function). "
             "This parameter is optional unless the `functionName` parameter is used.");
    addField("functionName", &DistTableProcedureConfig::functionName,
             "If specified, an instance of the ![](%%doclink distTable.getCounts function) "
             "of this name will be created using the trained stats tables. Note that to use "
             "this parameter, the `distTableFileUrl` must also be provided.");
    addParent<ProcedureConfig>();

    onPostValidate = chain(validateQuery(&DistTableProcedureConfig::trainingData,
                                         MustContainFrom()),
                           validateFunction<DistTableProcedureConfig>());
}



/*****************************************************************************/
/* STATS TABLE PROCEDURE                                                     */
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

    vector<string> outcome_names;
    for(const pair<string, std::shared_ptr<SqlExpression>> & lbl : runProcConf.outcomes)
        outcome_names.push_back(lbl.first);

    DistTablesMap distTables;

    {
        // Find only those variables used
        SqlExpressionDatasetScope context(boundDataset);

        auto selectBound = runProcConf.trainingData.stm->select.bind(context);

        for (auto & c: selectBound.info->getKnownColumns()) {
            distTables.insert(make_pair(c.columnName, DistTable(c.columnName, outcome_names)));
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
            for(auto it = distTables.begin(); it != distTables.end(); it++) {
                // is this col present for row?
                auto col_ptr = column_idx.find(it->first);
                if(col_ptr == column_idx.end()) {
                    // TODO handle unknowns
                    //output_cols.emplace_back(get<0>(col), count, get<2>(col));
                }
                else {
                    const tuple<ColumnName, CellValue, Date> & col = row.columns[col_ptr->second];
                    const DistTable::BucketCounts & counts = it->second.increment(get<1>(col), encodedLabels);

                    // *******
                    // column name caching
                    auto colIt = colCache.find(get<0>(col));
                    vector<ColumnName> * colNames;
                    if(colIt != colCache.end()) {
                        colNames = &(colIt->second);
                    }
                    else {
                        // if we didn't compute the column names yet, do that now
                        const Path & key = std::get<0>(col);

                        vector<ColumnName> names;
                        names.emplace_back(PathElement("trial") + key);
                        for(int lbl_idx=0; lbl_idx<encodedLabels.size(); lbl_idx++) {
                            names.emplace_back(PathElement(outcome_names[lbl_idx]) + key);
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
        store << distTables;
    }

    if(!runProcConf.modelFileUrl.empty() && !runProcConf.functionName.empty()) {
        cerr << "Saving stats tables to " << runProcConf.modelFileUrl.toString() << endl;
        PolyConfig clsFuncPC;
        clsFuncPC.type = "distTable.getCounts";
        clsFuncPC.id = runProcConf.functionName;
        clsFuncPC.params = DistTableFunctionConfig(runProcConf.modelFileUrl);

        createFunction(server, clsFuncPC, onProgress, true);
    }

    return RunOutput();
}





/*****************************************************************************/
/* STATS TABLE FUNCTION                                                      */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(DistTableFunctionConfig);

DistTableFunctionConfigDescription::
DistTableFunctionConfigDescription()
{
    addField("distTableFileUrl", &DistTableFunctionConfig::modelFileUrl,
             "URL of the model file (with extension '.st') to load. "
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
    store >> distTables;
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
                auto st = distTables.find(columnName);
                if(st == distTables.end())
                    return true;

                auto counts = st->second.getCounts(val);

                rtnRow.emplace_back(PathElement("trial") + columnName, counts.first, ts);

                for(int lbl_idx=0; lbl_idx<st->second.outcome_names.size(); lbl_idx++) {
                    rtnRow.emplace_back(PathElement(st->second.outcome_names[lbl_idx])
                                        +columnName,
                                        counts.second[lbl_idx],
                                        ts);
                }
                
                return true;
            };

        arg.forEachAtom(onAtom);
        result.emplace_back("counts", ExpressionValue(std::move(rtnRow)));
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


/*****************************************************************************/
/* STATS TABLE FUNCTION                                                      */
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
/* BAG OF WORDS STATS TABLE PROCEDURE CONFIG                                 */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(BagOfWordsDistTableProcedureConfig);

BagOfWordsDistTableProcedureConfigDescription::
BagOfWordsDistTableProcedureConfigDescription()
{
    Optional<PolyConfigT<Dataset> > optionalOutputDataset;
    optionalOutputDataset.emplace(PolyConfigT<Dataset>().
                                  withType(BagOfWordsDistTableProcedureConfig::defaultOutputDatasetType));

    addField("trainingData", &BagOfWordsDistTableProcedureConfig::trainingData,
             "SQL query to select the data on which the rolling operations will be performed.");
    addField("outcomes", &BagOfWordsDistTableProcedureConfig::outcomes,
             "List of expressions to generate the outcomes. Each can be any expression "
             "involving the columns in the dataset. The type of the outcomes "
             "must be a boolean (0 or 1)");
    addField("distTableFileUrl", &BagOfWordsDistTableProcedureConfig::modelFileUrl,
             "URL where the model file (with extension '.st') should be saved. "
             "This file can be loaded by the ![](%%doclink distTable.bagOfWords.posneg function). "
             "This parameter is optional unless the `functionName` parameter is used.");
    addField("outputDataset", &BagOfWordsDistTableProcedureConfig::outputDataset,
             "Output dataset with the total counts for each word along with"
             " the cooccurrence count with each outcome.", optionalOutputDataset);
    addField("functionName", &BagOfWordsDistTableProcedureConfig::functionName,
             "If specified, an instance of the ![](%%doclink distTable.bagOfWords.posneg function) "
             "of this name will be created using the trained stats tables and that function type's "
             "default parameters. Note that to use this parameter, the `distTableFileUrl` must also "
             "be provided.");
    addField("functionOutcomeToUse", &BagOfWordsDistTableProcedureConfig::functionOutcomeToUse,
            "When `functionName` is provided, an instance of the "
            "![](%%doclink distTable.bagOfWords.posneg function) with the outcome of this name will "
            "be created. This parameter represents the `outcomeToUse` field of the ![](%%doclink distTable.bagOfWords.posneg function).");
    addParent<ProcedureConfig>();

    onPostValidate = validateQuery(&BagOfWordsDistTableProcedureConfig::trainingData,
                                   MustContainFrom());
}

/*****************************************************************************/
/* BOW STATS TABLE PROCEDURE                                                 */
/*****************************************************************************/

BagOfWordsDistTableProcedure::
BagOfWordsDistTableProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    procConfig = config.params.convert<BagOfWordsDistTableProcedureConfig>();
}

Any
BagOfWordsDistTableProcedure::
getStatus() const
{
    return Any();
}

RunOutput
BagOfWordsDistTableProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{

    BagOfWordsDistTableProcedureConfig runProcConf =
        applyRunConfOverProcConf(procConfig, run);
    
    if(!runProcConf.functionName.empty() && runProcConf.functionOutcomeToUse.empty()) {
        throw ML::Exception("The 'functionOutcomeToUse' parameter must be set when the "
                "'functionName' parameter is set.");
    }

    SqlExpressionMldbScope context(server);
    auto boundDataset = runProcConf.trainingData.stm->from->bind(context);

    vector<string> outcome_names;
    for(const pair<string, std::shared_ptr<SqlExpression>> & lbl : runProcConf.outcomes)
        outcome_names.push_back(lbl.first);

    DistTable distTable(ColumnName("words"), outcome_names);

    auto onProgress2 = [&] (const Json::Value & progress)
        {
            Json::Value value;
            value["dataset"] = progress;
            return onProgress(value);
        };

    int num_req = 0;
    Date start = Date::now();

    auto processor = [&] (NamedRowValue & row_,
                           const std::vector<ExpressionValue> & extraVals)
        {
            MatrixNamedRow row = row_.flattenDestructive();
            if(num_req++ % 10000 == 0) {
                double secs = Date::now().secondsSinceEpoch() - start.secondsSinceEpoch();
                string progress = ML::format("done %d. %0.4f/sec", num_req, num_req / secs);
                onProgress2(progress);
                cerr << progress << endl;
            }

            vector<uint> encodedLabels;
            for(int lbl_idx=0; lbl_idx < runProcConf.outcomes.size(); lbl_idx++) {
                CellValue outcome = extraVals.at(lbl_idx).getAtom();
                encodedLabels.push_back( !outcome.empty() && outcome.isTrue() );
            }

            for(const std::tuple<ColumnName, CellValue, Date> & col : row.columns) {
                distTable.increment(CellValue(get<0>(col).toUtf8String()), encodedLabels);
            }

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

    // Optionally save counts to a dataset
    if (runProcConf.outputDataset) {
        Date date0;
        PolyConfigT<Dataset> outputDatasetConf = *runProcConf.outputDataset;
        if (outputDatasetConf.type.empty())
            outputDatasetConf.type = BagOfWordsDistTableProcedureConfig
                                     ::defaultOutputDatasetType;
        auto output = createDataset(server, outputDatasetConf, onProgress2,
                                    true /*overwrite*/);

        uint64_t load_factor = std::ceil(distTable.counts.load_factor());

        vector<ColumnName> outcome_col_names;
        outcome_col_names.reserve(distTable.outcome_names.size());
        for (int i=0; i < distTable.outcome_names.size(); ++i)
            outcome_col_names
                .emplace_back(PathElement("outcome") + distTable.outcome_names[i]);

        typedef std::vector<std::tuple<ColumnName, CellValue, Date>> Columns;

        auto onBucketChunk = [&] (size_t i0, size_t i1)
        {
            std::vector<std::pair<RowName, Columns>> rows;
            rows.reserve((i1 - i0)*load_factor);
            // for each bucket of the unordered_map in our chunk
            for (size_t i = i0; i < i1; ++i) {
                // for each item in the bucket
                for (auto it = distTable.counts.begin(i);
                        it != distTable.counts.end(i); ++it) {
                    Columns columns;
                    // number of trials
                    columns.emplace_back(PathElement("trials"), it->second.first, date0);
                    // coocurence with outcome for each outcome
                    for (int i=0; i < distTable.outcome_names.size(); ++i) {
                        columns.emplace_back(
                            outcome_col_names[i],
                            it->second.second[i],
                            date0);
                    }
                    rows.emplace_back(PathElement(it->first), columns);
                }
            }
            output->recordRows(rows);
            return true;
        };

        parallelMapChunked(0, distTable.counts.bucket_count(),
                           256 /* chunksize */, onBucketChunk);
        output->commit();
    }


    // save if required
    if(!runProcConf.modelFileUrl.empty()) {
        filter_ostream stream(runProcConf.modelFileUrl.toString());
        ML::DB::Store_Writer store(stream);
        store << distTable;
    }
    
    if(!runProcConf.modelFileUrl.empty() && !runProcConf.functionName.empty() &&
            !runProcConf.functionOutcomeToUse.empty()) {
        cerr << "Saving stats tables to " << runProcConf.modelFileUrl.toString() << endl;
        PolyConfig clsFuncPC;
        clsFuncPC.type = "distTable.bagOfWords.posneg";
        clsFuncPC.id = runProcConf.functionName;
        clsFuncPC.params = DistTablePosNegFunctionConfig(runProcConf.modelFileUrl,
                                                          runProcConf.functionOutcomeToUse);

        createFunction(server, clsFuncPC, onProgress, true);
    }
    
    return RunOutput();
}


/*****************************************************************************/
/* STATS TABLE POS NEG FUNCTION                                              */
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



/*****************************************************************************/
/*****************************************************************************/
/*****************************************************************************/

namespace {

RegisterFunctionType<DistTableFunction, DistTableFunctionConfig>
regClassifyFunction(builtinPackage(),
                    "distTable.getCounts",
                    "Get stats table counts for a row of keys",
                    "functions/DistTableGetCounts.md.html");

RegisterProcedureType<DistTableDerivedColumnsGeneratorProcedure,
                      DistTableDerivedColumnsGeneratorProcedureConfig>
regSTDerColGenProc(builtinPackage(),
                   "Generate an sql.expression function to be used to compute "
                   "derived statistics from a stats table",
                   "procedures/DistTableDerivedColumnsGeneratorProcedure.md.html",
                   nullptr,
                   { MldbEntity::INTERNAL_ENTITY });

RegisterProcedureType<DistTableProcedure, DistTableProcedureConfig>
regSTTrain(builtinPackage(),
           "Create statistical tables of trials against outcomes",
           "procedures/DistTableProcedure.md.html");


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

} // file scope

} // namespace MLDB
} // namespace Datacratic
