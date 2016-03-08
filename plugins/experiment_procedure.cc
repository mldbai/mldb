/** experiment_procedure.cc
    Francois Maillet, 8 septembre 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Experiment procedure
*/

#include "experiment_procedure.h"
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
#include "arch/timers.h"
#include "types/optional_description.h"
#include "mldb/plugins/sql_config_validator.h"
#include "mldb/plugins/sql_expression_extractors.h"
#include "mldb/plugins/sparse_matrix_dataset.h"

using namespace std;

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* EXPERIMENT PROCEDURE CONFIG                                               */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(DatasetFoldConfig);

DatasetFoldConfigDescription::
DatasetFoldConfigDescription()
{
    addField("training_where", &DatasetFoldConfig::training_where,
             "The WHERE clause for which rows to include from the training dataset. "
             "This can be any expression involving the columns in the dataset. ",
             SqlExpression::parse("true"));
    addField("testing_where", &DatasetFoldConfig::testing_where,
             "The WHERE clause for which rows to include from the testing dataset. "
             "This can be any expression involving the columns in the dataset. ",
             SqlExpression::parse("true"));
    addField("training_offset", &DatasetFoldConfig::training_offset,
             "How many rows to skip before using data.",
             ssize_t(0));
    addField("training_limit", &DatasetFoldConfig::training_limit,
             "How many rows of data to use.  -1 (the default) means use all "
             "of the rest of the rows in the dataset after skipping OFFSET rows.",
             ssize_t(-1));
    addField("testing_offset", &DatasetFoldConfig::testing_offset,
             "How many rows to skip before using data.",
             ssize_t(0));
    addField("testing_limit", &DatasetFoldConfig::testing_limit,
             "How many rows of data to use.  -1 (the default) means use all "
             "of the rest of the rows in the dataset after skipping OFFSET rows.",
             ssize_t(-1));
    addField("orderBy", &DatasetFoldConfig::orderBy,
             "How to order the rows.  This only has an effect when OFFSET "
             "and LIMIT are used.  Default is to order by rowHash(). ",
             OrderByExpression::parse("rowHash()"));
    setTypeName("DatasetFoldConfig");
    documentationUri = "/doc/builtin/procedures/ExperimentProcedure.md#DatasetFoldConfig";
}

DEFINE_STRUCTURE_DESCRIPTION(ExperimentProcedureConfig);

ExperimentProcedureConfigDescription::
ExperimentProcedureConfigDescription()
{
    addField("trainingData", &ExperimentProcedureConfig::trainingData,
             "Specification of the data for input to the classifier procedure. "
             "The select expression must contain these two sub-expressions: \n"
             "\n"
             "1.  one row expression to identify the features on which to \n"
             "    train, and \n"
             "2.  one scalar expression to identify the label.\n"
             "\n"
             "The type of the label expression must match "
             "that of the classifier mode: a boolean (0 or 1) for `boolean` mode; "
             "a real for regression mode, and any combination of numbers and strings "
             "for `categorical` mode.  Labels with a null value will have their row skipped. "
             "The select expression can contain an optional weigth expression.  The weight "
             "allows the relative importance of examples to be set.  It must "
             "be a real number.  If the expression is not specified each example will have "
             "a weight of one.  Rows with a null weight will cause a training error. "
             "The select statement does not support groupby and having clauses. "
             "Also, unlike most select expressions, this one can only select whole columns, "
             "not expressions involving columns. So X will work, but not X + 1. "
             "If you need derived values in the select expression, create a dataset with "
             "the derived columns as a previous step and run the classifier over that dataset instead.");
    addField("testingData", &ExperimentProcedureConfig::testingData,
             "Specification of the data for input to the accuracy procedure. "
             "The select expression must contain these sub-expressions: one scalar expression "
             "to identify the label and one scalar expression to identify the score. "
             "The type of the label expression must match "
             "that of the classifier mode from which the model was trained. "
             "Labels with a null value will have their row skipped. "
             "The expression to generate the score represents the output "
             "of whatever is having its accuracy tested.  This needs to be "
             "a number, and normally should be a floating point number that "
             "represents the degree of confidence in the prediction, not "
             "just the class. This is typically, the training function returned "  
             "by a classifier.train procedure. "
             "The select expression can also contain an optional weight sub-expression. "
             "This expression generates the relative weight for each example.  In some "
             "circumstances it is necessary to calculate accuracy statistics "
             "with uneven weighting, for example to counteract the effect of "
             "non-uniform sampling in dataset.  By default, each class will "
             "get the same weight.  This value is relative to the other "
             "examples, in other words having all examples weighted 1 or all "
             "examples weighted 10 will have the same effect.  That being "
             "said, it is a good idea to keep the weights centered around 1 "
             "to avoid numeric errors in the calculations."
             "The select statement does not support groupby and having clauses.");
    addField("experimentName", &ExperimentProcedureConfig::experimentName,
             "Name of the experiment that will be used to name the artifacts.");
    addField("keepArtifacts", &ExperimentProcedureConfig::keepArtifacts,
             "If true, all procedures and intermediary datasets are kept.", false);
    addField("datasetFolds", &ExperimentProcedureConfig::datasetFolds,
             "Dataset folds to use. This parameter can be used if the dataset folds "
             "required are more complex than a simple k-fold cross-validation. "
             "It cannot be specified as the same time as the kfold parameter.");
    addField("kfold", &ExperimentProcedureConfig::kfold,
             "Do a k-fold cross-validation. This is a helper parameter that "
             "generates a datasetFolds configuration splitting the dataset in k subsamples, "
             "each fold testing on one subsample and training on the rest. 0 means it is not used and "
             "1 is an invalid number. It cannot be specified at the "
             "same time as the datasetFolds parameter.", ssize_t(0));
    addField("modelFileUrlPattern", &ExperimentProcedureConfig::modelFileUrlPattern,
             "URL where the model file (with extension '.cls') should be saved. It "
             "should include the string $runid that will be replaced by an identifier "
             "for each run, if using multiple dataset folds.");
    addField("algorithm", &ExperimentProcedureConfig::algorithm,
             "Algorithm to use to train classifier with.  This must point to "
             "an entry in the configuration or configurationFile parameters");
    addField("configuration", &ExperimentProcedureConfig::configuration,
             "Configuration object to use for the classifier.  Each one has "
             "its own parameters.  If none is passed, then the configuration "
             "will be loaded from the ConfigurationFile parameter",
             Json::Value());
    addField("configurationFile", &ExperimentProcedureConfig::configurationFile,
             "File to load configuration from.  This is a JSON file containing "
             "only objects, strings and numbers.  If the configuration object is "
             "non-empty, then that will be used preferentially.",
             string("/opt/bin/classifiers.json"));
    addField("equalizationFactor", &ExperimentProcedureConfig::equalizationFactor,
              "Amount to adjust weights so that all classes have an equal "
              "total weight.  A value of 0 (default) will not equalize weights "
              "at all.  A value of 1 will ensure that the total weight for "
              "both positive and negative examples is exactly identical. "
              "A number between will choose a balanced tradeoff.  Typically 0.5 "
              "is a good number to use for unbalanced probabilities", 0.5);
     addField("mode", &ExperimentProcedureConfig::mode,
              "Mode of classifier.  Controls how the label is interpreted and "
              "what is the output of the classifier.", CM_BOOLEAN);
     addField("outputAccuracyDataset", &ExperimentProcedureConfig::outputAccuracyDataset,
              "If true, an output dataset for scored examples will created for each fold.",
              true);
    addParent<ProcedureConfig>();

    onPostValidate = chain(validate<ExperimentProcedureConfig, 
                           InputQuery,
                           NoGroupByHaving, 
                           MustContainFrom,
                           PlainColumnSelect>(&ExperimentProcedureConfig::trainingData, "classifier.experiment"),
                           validate<ExperimentProcedureConfig, 
                           Optional<InputQuery>,
                           NoGroupByHaving, 
                           MustContainFrom,
                           PlainColumnSelect,
                           FeaturesLabelSelect>(&ExperimentProcedureConfig::testingData, "classifier.experiment"));
    
}

/*****************************************************************************/
/* JSON STATS STATISTICS GENERATOR
 *****************************************************************************/
void
JsStatsStatsGenerator::
accumStats(const Json::Value & js, const string & path) {
    if (!js.isObject()) return;
    if(path == "") lastObj = js;

    for (const auto & key : js.getMemberNames()) {
        string currPath = path + "." + key;
        if (js[key].isObject()) {
            accumStats(js[key], currPath);
        }
        else if(js[key].isNumeric()) {
            auto it = outputStatsAccum.find(currPath);
            if(it != outputStatsAccum.end()) {
                it->second.push_back(js[key].asDouble());
            }
            else {
                outputStatsAccum.insert(make_pair(currPath,
                            ML::distribution<double>{js[key].asDouble()}));
            }
        }
    }
}

Json::Value
JsStatsStatsGenerator::
generateStatistics() const {
    Json::Value results;
    generateStatistics(lastObj, results, "");
    return results;
}

void
JsStatsStatsGenerator::
generateStatistics(const Json::Value & js,
                   Json::Value & result,
                   const string & path) const {

    for (const auto & key : js.getMemberNames()) {
        string currPath = path + "." + key;
        if (js[key].isObject()) {
            result[key] = Json::Value();
            generateStatistics(js[key], result[key], currPath);
        }
        else if(js[key].isNumeric()) {
            auto it = outputStatsAccum.find(currPath);
            if(it == outputStatsAccum.end()) {
                throw ML::Exception("Unable to find statistics '"+currPath+"' in results map");
            }

            Json::Value stats;
            stats["max"] = it->second.max();
            stats["min"] = it->second.min();
            stats["std"] = it->second.std();
            stats["mean"] = it->second.mean();
            result[key] = stats;
        }
    }
}



/*****************************************************************************/
/* EXPERIMENT PROCEDURE                                                      */
/*****************************************************************************/

ExperimentProcedure::
ExperimentProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->procConfig = config.params.convert<ExperimentProcedureConfig>();
}

Any
ExperimentProcedure::
getStatus() const
{
    return Any();
}

RunOutput
ExperimentProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    JsStatsStatsGenerator statsGen;
    JsStatsStatsGenerator durationStatsGen;

    auto runProcConf = applyRunConfOverProcConf(procConfig, run);

    std::atomic<int> progress(0);


    auto onProgress2 = [&] (const Json::Value & details)
        {
            Json::Value value;
            value["foldNumber"] = (int)progress;
            value["details"] = details;
            return onProgress(value);
        };

    vector<string> resourcesToDelete;

    std::shared_ptr<Procedure> clsProcedure;
    std::shared_ptr<Procedure> accuracyProc;

    
    Json::Value rtn_results(Json::ValueType::arrayValue);
    Json::Value rtn_details(Json::ValueType::arrayValue);

    if(!runProcConf.trainingData.stm) {
        throw ML::Exception("Training data must be specified.");
    }

    // validation
    if(runProcConf.datasetFolds.size() > 0 && runProcConf.kfold != 0) {
        throw ML::Exception("The datasetFolds and kfold parameters "
                "cannot be specified at the same time.");
    }
    else if(runProcConf.kfold == 1) {
        throw ML::Exception("When using the kfold parameter, it must be >= 2.");
    }
    
    // default behaviour if nothing is defined
    if(runProcConf.datasetFolds.size() == 0 && runProcConf.kfold == 0) {
        // if we're not using a testing dataset
        if(!runProcConf.testingData) {
            runProcConf.datasetFolds.push_back(
                DatasetFoldConfig(
                    SqlExpression::parse("rowHash() % 2 != 1"),
                    SqlExpression::parse("rowHash() % 2 = 1")));
        }
        // if we're using different train and test, use each one of them completely
        // for test and train
        else {
            runProcConf.datasetFolds.push_back(DatasetFoldConfig(SqlExpression::parse("true"), 
                                                                 SqlExpression::parse("true")));
        }
    }

    // generate dataset_splits if using the kfold helper parameter
    if(runProcConf.kfold >= 2) {
        if(runProcConf.testingData) {
            throw ML::Exception("Should not use a k-fold cross-validation if testing "
                "dataset is specified.");
        }

        for(int k=0; k<runProcConf.kfold; k++) {
            //train and test
            runProcConf.datasetFolds.push_back(
                DatasetFoldConfig(
                    SqlExpression::parse(ML::format("rowHash() %% %d != %d", runProcConf.kfold, k)),
                    SqlExpression::parse(ML::format("rowHash() %% %d = %d",  runProcConf.kfold, k))));
        }
    }

    ExcAssertGreater(runProcConf.datasetFolds.size(), 0);


    for(auto & datasetFold : runProcConf.datasetFolds) {
        /***
         * TRAIN
         * **/
        ClassifierConfig clsProcConf;
        clsProcConf.trainingData = runProcConf.trainingData;
        clsProcConf.trainingData.stm->where = datasetFold.training_where;

        string baseUrl = runProcConf.modelFileUrlPattern.toString();
        ML::replace_all(baseUrl, "$runid",
                        ML::format("%s-%d", runProcConf.experimentName, (int)progress));
        clsProcConf.modelFileUrl = Url(baseUrl);
        clsProcConf.configuration = runProcConf.configuration;
        clsProcConf.configurationFile = runProcConf.configurationFile;
        clsProcConf.algorithm = runProcConf.algorithm;
        clsProcConf.equalizationFactor = runProcConf.equalizationFactor;
        clsProcConf.mode = runProcConf.mode;
      
     
        clsProcConf.functionName = ML::format("%s_scorer_%d", runProcConf.experimentName, (int)progress);

        if(progress == 0) {
            PolyConfig clsProcPC;
            clsProcPC.id = runProcConf.experimentName + "_trainer";
            clsProcPC.type = "classifier.train";
            clsProcPC.params = jsonEncode(clsProcConf);

            cerr << " >>>>> Creating training procedure" << endl;
            clsProcedure = obtainProcedure(server, clsProcPC, onProgress2);
            resourcesToDelete.push_back("/v1/procedures/"+clsProcPC.id.utf8String());
        }

        if(!clsProcedure) {
            throw ML::Exception("Was unable to obtain classifier.train procedure");
        }

        // create run configuration
        ProcedureRunConfig clsProcRunConf;
        clsProcRunConf.id = "run_"+to_string(progress);
        clsProcRunConf.params = jsonEncode(clsProcConf);
        Date trainStart = Date::now();
        RunOutput output = clsProcedure->run(clsProcRunConf, onProgress2);
        Date trainFinish = Date::now();

//          cout << jsonEncode(output.results).toStyledString() << endl;
//          cout << jsonEncode(output.details).toStyledString() << endl;


        /***
         * scoring function
         * created during the training so only add it to the cleanup list
         * **/

        resourcesToDelete.push_back("/v1/functions/" + clsProcConf.functionName.utf8String());

        /***
         * accuracy
         * **/
        AccuracyConfig accuracyConf;
        accuracyConf.mode = runProcConf.mode;

        if(runProcConf.outputAccuracyDataset) {
            PolyConfigT<Dataset> outputPC;
            outputPC.id = ML::format("%s_results_%d", runProcConf.experimentName, (int)progress);
            outputPC.type = "tabular";

            {
                InProcessRestConnection connection;
                RestRequest request("DELETE", "/v1/datasets/"+outputPC.id.utf8String(), RestParams(), "{}");
                server->handleRequest(connection, request);
            }
            accuracyConf.outputDataset.emplace(outputPC);
        }
        
        accuracyConf.testingData = runProcConf.testingData ? *runProcConf.testingData : runProcConf.trainingData;
        accuracyConf.testingData.stm->where = datasetFold.testing_where;

        auto features = extractNamedSubSelect("features", accuracyConf.testingData.stm->select);
        auto label = extractNamedSubSelect("label", accuracyConf.testingData.stm->select);
        shared_ptr<SqlRowExpression> weight = extractNamedSubSelect("weight", accuracyConf.testingData.stm->select);
        if (!weight)
            weight = SqlRowExpression::parse("1.0 as weight");

        string scoreExpr;
        if     (runProcConf.mode == CM_BOOLEAN || 
                runProcConf.mode == CM_REGRESSION)  scoreExpr = "\"%s\"({%s})[score] as score";
        else if(runProcConf.mode == CM_CATEGORICAL) scoreExpr = "\"%s\"({%s})[scores] as score";
        else throw ML::Exception("Classifier mode %d not implemented", runProcConf.mode);

        auto score = SqlRowExpression::parse(ML::format(scoreExpr.c_str(),
                                                        clsProcConf.functionName.utf8String(),
                                                        features->surface.utf8String()));
        accuracyConf.testingData.stm->select = SelectExpression({features, label, weight, score});

        ML::Timer timer;

        if(progress == 0) {
            PolyConfig accuracyProcPC;
            accuracyProcPC.id = runProcConf.experimentName + "_scorer";
            accuracyProcPC.type = "classifier.test";
            accuracyProcPC.params = accuracyConf;

            cerr << " >>>>> Creating testing procedure" << endl;
            accuracyProc = obtainProcedure(server, accuracyProcPC, onProgress2);

            resourcesToDelete.push_back("/v1/procedures/"+accuracyProcPC.id.utf8String());
        }

        if(!accuracyProc) {
            throw ML::Exception("Was unable to obtain accuracy procedure");
        }

        ProcedureRunConfig accuracyProcRunConf;
        accuracyProcRunConf.id = "run_"+to_string(progress);
        accuracyProcRunConf.params = jsonEncode(accuracyConf);
        Date testStart = Date::now();
        RunOutput accuracyOutput = accuracyProc->run(accuracyProcRunConf, onProgress2);
        Date testFinish = Date::now();

        cerr << "accuracy took " << timer.elapsed() << endl;

//          cout << jsonEncode(accuracyOutput.results).toStyledString() << endl;
//          cout << jsonEncode(accuracyOutput.details).toStyledString() << endl;

        Json::Value duration;
        duration["train"] = trainFinish.secondsSinceEpoch() - trainStart.secondsSinceEpoch();
        duration["test"]  = testFinish.secondsSinceEpoch() - testStart.secondsSinceEpoch();
        durationStatsGen.accumStats(duration, "");

        // Add results
        Json::Value foldRez;
        foldRez["fold"] = jsonEncode(datasetFold);
        foldRez["modelFileUrl"] = clsProcConf.modelFileUrl.toUtf8String();
        foldRez["results"] = jsonEncode(accuracyOutput.results);
        foldRez["durationSecs"] = duration;
        statsGen.accumStats(foldRez["results"], "");
        rtn_results.append(foldRez);


        progress ++;
    }

    /***
     * cleanups
     * **/
    if(!runProcConf.keepArtifacts) {
        InProcessRestConnection connection;
        for(const string & resource : resourcesToDelete) {
            RestRequest request("DELETE", resource, RestParams(), "{}");
            server->handleRequest(connection, request);
            if(connection.responseCode != 204) {
                throw ML::Exception(ML::format("Unable to delete resource '%s'. "
                            "Response code %d", resource, connection.responseCode));
            }
        }
    }

    Json::Value final_res;
    final_res["folds"] = rtn_results;
    final_res["aggregated"] = statsGen.generateStatistics();
    final_res["avgDuration"] = durationStatsGen.generateStatistics();

    return RunOutput(final_res);
}


namespace {

RegisterProcedureType<ExperimentProcedure, ExperimentProcedureConfig>
regExpProc(builtinPackage(),
          "classifier.experiment",
          "Train and test a classifier",
          "procedures/ExperimentProcedure.md.html");

} // file scope

} // namespace MLDB
} // namespace Datacratic
