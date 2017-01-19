/** experiment_procedure.cc
    Francois Maillet, 8 septembre 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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
#include "mldb/utils/log.h"

using namespace std;


namespace MLDB {


/*****************************************************************************/
/* EXPERIMENT PROCEDURE CONFIG                                               */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(DatasetFoldConfig);

DatasetFoldConfigDescription::
DatasetFoldConfigDescription()
{
    addField("trainingWhere", &DatasetFoldConfig::trainingWhere,
             "The WHERE clause for which rows to include from the training dataset. "
             "This can be any expression involving the columns in the dataset. ",
             SqlExpression::parse("true"));
    addField("testingWhere", &DatasetFoldConfig::testingWhere,
             "The WHERE clause for which rows to include from the testing dataset. "
             "This can be any expression involving the columns in the dataset. ",
             SqlExpression::parse("true"));
    addField("trainingOffset", &DatasetFoldConfig::trainingOffset,
             "How many rows to skip before using data.",
             ssize_t(0));
    addField("trainingLimit", &DatasetFoldConfig::trainingLimit,
             "How many rows of data to use.  -1 (the default) means use all "
             "of the rest of the rows in the dataset after skipping OFFSET rows.",
             ssize_t(-1));
    addField("testingOffset", &DatasetFoldConfig::testingOffset,
             "How many rows to skip before using data.",
             ssize_t(0));
    addField("testingLimit", &DatasetFoldConfig::testingLimit,
             "How many rows of data to use.  -1 (the default) means use all "
             "of the rest of the rows in the dataset after skipping OFFSET rows.",
             ssize_t(-1));
    addField("trainingOrderBy", &DatasetFoldConfig::trainingOrderBy,
             "How to order the rows.  This only has an effect when `trainingOffset` "
             "or `trainingLimit` are used.",
             OrderByExpression::parse("rowHash()"));
    addField("testingOrderBy", &DatasetFoldConfig::testingOrderBy,
             "How to order the rows.  This only has an effect when `testingOffset` "
             "or `testingLimit` are used.",
             OrderByExpression::parse("rowHash()"));
    setTypeName("DatasetFoldConfig");
    documentationUri = "/doc/builtin/procedures/ExperimentProcedure.md#DatasetFoldConfig";
}

DEFINE_STRUCTURE_DESCRIPTION(ExperimentProcedureConfig);

ExperimentProcedureConfigDescription::
ExperimentProcedureConfigDescription()
{
    addField("experimentName", &ExperimentProcedureConfig::experimentName,
             "A string without spaces which will be used to name the various datasets, "
             "procedures and functions created this procedure runs.");
     addField("mode", &ExperimentProcedureConfig::mode,
             "Model mode: `boolean`, `regression` or `categorical`. "
             "Controls how the label is interpreted and what is the output of the classifier. "
             , CM_BOOLEAN);
    addField("inputData", &ExperimentProcedureConfig::inputData,
             "SQL query which specifies the features, labels and optional weights for the training and testing procedures. "
             "This query is used to create a training and testing set according to the [steps laid "
             "out below](#TrainTest).\n\n"
             "The query should be of the form `select {f1, f2} as features, x as label from ds`.\n\n"
             "The select expression must contain these two columns: \n\n"
             "  * `features`: a row expression to identify the features on which to \n"
             "    train, and \n"
             "  * `label`: one scalar expression to identify the row's label, and whose type "
             "must match that of the classifier mode. Rows with null labels will be ignored. \n"
             "     * `boolean` mode: a boolean (0 or 1)\n"
             "     * `regression` mode: a real number\n"
             "     * `categorical` mode: any combination of numbers and strings for\n\n"
             "The select expression can contain an optional `weight` column. The weight "
             "allows the relative importance of examples to be set. It must "
             "be a real number. If the `weight` is not specified each row will have "
             "a weight of 1. Rows with a null weight will cause a training error. \n\n"
             "The query must not contain `WHERE`, `LIMIT`, `OFFSET`, `GROUP BY` or `HAVING` clauses "
             "(they can be defined in datasetFolds) and, "
             "unlike most select expressions, this one can only select whole columns, "
             "not expressions involving columns. So `X` will work, but not `X + 1`. "
             "If you need derived values in the query, create a dataset with "
             "the derived columns as a previous step and use a query on that dataset instead.");
    addField("kfold", &ExperimentProcedureConfig::kfold,
             "Do a k-fold cross-validation. This is a helper parameter that "
             "generates a [DatasetFoldConfig](#DatasetFoldConfig) splitting the dataset into k subsamples, "
             "each fold testing on one `k`th of the input data and training on the rest.\n\n"
             "0 means it is not used and 1 is an invalid number. It cannot be specified at the "
             "same time as the `datasetFolds` parameter "
             "and in general should not be used at the same time as the `testingDataOverride` "
             "parameter.", ssize_t(0));
    addField("datasetFolds", &ExperimentProcedureConfig::datasetFolds,
             "[DatasetFoldConfig](#DatasetFoldConfig) to use. This parameter can be used if the dataset folds "
             "required are more complex than a simple k-fold cross-validation. "
             "It cannot be specified as the same time as the `kfold` parameter "
             "and in general should not be used at the same time as the `testingDataOverride` "
             "parameter.");
    addField("testingDataOverride", &ExperimentProcedureConfig::testingDataOverride,
             "SQL query which overrides the input data for the testing procedure. "
             "This optional parameter must be of the same form as the `inputData` "
             "parameter above, and by default takes the same value as `inputData`.\n\n"
             "This query is used to create a test set according to the [steps laid "
             "out below](#TrainTest).\n\n"
             "This parameter is useful when it is necessary to test on data contained in "
             "a different dataset from the training data, or to calculate accuracy statistics "
             "with uneven weighting, for example to counteract the effect of "
             "non-uniform sampling in the `inputData`.\n\n"
             "The query must not contain `WHERE`, `LIMIT`, `OFFSET`, `GROUP BY` or `HAVING` clauses "
             "(they can be defined in datasetFolds) and, "
             "unlike most select expressions, this one can only select whole columns, "
             "not expressions involving columns. So `X` will work, but not `X + 1`. "
             "If you need derived values in the query, create a dataset with "
             "the derived columns as a previous step and use a query on that dataset instead.");
    addField("algorithm", &ExperimentProcedureConfig::algorithm,
             "Algorithm to use to train classifier with.  This must point to "
             "an entry in the configuration or configurationFile parameters. "
             "See the [classifier configuration documentation](../ClassifierConf.md.html) for details.");
    addField("configuration", &ExperimentProcedureConfig::configuration,
             "Configuration object to use for the classifier.  Each one has "
             "its own parameters.  If none is passed, then the configuration "
             "will be loaded from the ConfigurationFile parameter. "
             "See the [classifier configuration documentation](../ClassifierConf.md.html) for details.",
             Json::Value());
    addField("configurationFile", &ExperimentProcedureConfig::configurationFile,
             "File to load configuration from.  This is a JSON file containing "
             "only objects, strings and numbers.  If the configuration object is "
             "non-empty, then that will be used preferentially. "
             "See the [classifier configuration documentation](../ClassifierConf.md.html) for details.",
             string("/opt/bin/classifiers.json"));
    addField("equalizationFactor", &ExperimentProcedureConfig::equalizationFactor,
              "Amount to adjust weights so that all classes have an equal "
              "total weight.  A value of 0 (default) will not equalize weights "
              "at all.  A value of 1 will ensure that the total weight for "
              "both positive and negative examples is exactly identical. "
              "A number between will choose a balanced tradeoff.  Typically 0.5 "
              "is a good number to use for unbalanced probabilities. "
              "See the [classifier configuration documentation](../ClassifierConf.md.html) for details.", 0.5);
    addField("modelFileUrlPattern", &ExperimentProcedureConfig::modelFileUrlPattern,
             "URL where the model file (with extension '.cls') should be saved. It "
             "should include the string $runid that will be replaced by an identifier "
             "for each run, if using multiple dataset folds.");
    addField("keepArtifacts", &ExperimentProcedureConfig::keepArtifacts,
             "If true, all procedures and intermediary datasets are kept.", false);
     addField("evalTrain", &ExperimentProcedureConfig::evalTrain,
              "Run the evaluation on the training set. If true, the same performance "
              "statistics that are returned for the testing set will also be "
              "returned for the training set.", false);
     addField("outputAccuracyDataset", &ExperimentProcedureConfig::outputAccuracyDataset,
              "If true, an output dataset for scored examples will created for each fold.",
              true);
    addField("uniqueScoresOnly", &ExperimentProcedureConfig::uniqueScoresOnly,
              "If `outputAccuracyDataset` is set and `mode` is set to `boolean`, setting this parameter "
              "to `true` will output a single row per unique score. This is useful if the "
              "test set is very large and aggregate statistics for each unique score is "
              "sufficient, for instance to generate a ROC curve. This has no effect "
              "for other values of `mode`.", false);
    addParent<ProcedureConfig>();

    onPostValidate = chain(validateQuery(&ExperimentProcedureConfig::inputData,
                                         NoGroupByHaving(),
                                         NoWhere(),
                                         NoLimit(),
                                         NoOffset(),
                                         MustContainFrom(),
                                         PlainColumnSelect(),
                                         FeaturesLabelSelect()),
                           validateQuery(&ExperimentProcedureConfig::testingDataOverride,
                                         NoGroupByHaving(),
                                         NoWhere(),
                                         NoLimit(),
                                         NoOffset(),
                                         MustContainFrom(),
                                         PlainColumnSelect(),
                                         FeaturesLabelSelect()));

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
                            distribution<double>{js[key].asDouble()}));
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
                throw MLDB::Exception("Unable to find statistics '"+currPath+"' in results map");
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
    JsStatsStatsGenerator statsGenTrain;
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


    Json::Value test_eval_results(Json::ValueType::arrayValue);

    if(!runProcConf.inputData.stm) {
        throw MLDB::Exception("Training data must be specified.");
    }

    // validation
    if(runProcConf.datasetFolds.size() > 0 && runProcConf.kfold != 0) {
        throw MLDB::Exception("The datasetFolds and kfold parameters "
                "cannot be specified at the same time.");
    }
    else if(runProcConf.kfold == 1) {
        throw MLDB::Exception("When using the kfold parameter, it must be >= 2.");
    }

    // default behaviour if nothing is defined
    if(runProcConf.datasetFolds.size() == 0 && runProcConf.kfold == 0) {
        // if we're not using a testing dataset
        if(!runProcConf.testingDataOverride) {
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
        if(runProcConf.testingDataOverride) {
            throw MLDB::Exception("Should not use a k-fold cross-validation if testing "
                "dataset is specified.");
        }

        for(int k=0; k<runProcConf.kfold; k++) {
            //train and test
            runProcConf.datasetFolds.push_back(
                DatasetFoldConfig(
                    SqlExpression::parse(MLDB::format("rowHash() %% %d != %d", runProcConf.kfold, k)),
                    SqlExpression::parse(MLDB::format("rowHash() %% %d = %d",  runProcConf.kfold, k))));
        }
    }

    ExcAssertGreater(runProcConf.datasetFolds.size(), 0);

    for(auto & datasetFold : runProcConf.datasetFolds) {
        /***
         * TRAIN
         * **/
        ClassifierConfig clsProcConf;
        clsProcConf.trainingData = runProcConf.inputData;
        clsProcConf.trainingData.stm->where = datasetFold.trainingWhere;
        clsProcConf.trainingData.stm->limit = datasetFold.trainingLimit;
        clsProcConf.trainingData.stm->offset = datasetFold.trainingOffset;
        clsProcConf.trainingData.stm->orderBy = datasetFold.trainingOrderBy;

        string baseUrl = runProcConf.modelFileUrlPattern.toString();
        ML::replace_all(baseUrl, "$runid",
                        MLDB::format("%s-%d", runProcConf.experimentName, (int)progress));
        clsProcConf.modelFileUrl = Url(baseUrl);
        clsProcConf.configuration = runProcConf.configuration;
        clsProcConf.configurationFile = runProcConf.configurationFile;
        clsProcConf.algorithm = runProcConf.algorithm;
        clsProcConf.equalizationFactor = runProcConf.equalizationFactor;
        clsProcConf.mode = runProcConf.mode;


        clsProcConf.functionName = MLDB::format("%s_scorer_%d", runProcConf.experimentName, (int)progress);

        if(progress == 0) {
            PolyConfig clsProcPC;
            clsProcPC.id = runProcConf.experimentName + "_trainer";
            clsProcPC.type = "classifier.train";
            clsProcPC.params = jsonEncode(clsProcConf);

            INFO_MSG(logger) << " >>>>> Creating training procedure";
            clsProcedure = createProcedure(server, clsProcPC, onProgress2, true);
            resourcesToDelete.push_back("/v1/procedures/"+clsProcPC.id.utf8String());
        }

        if(!clsProcedure) {
            throw MLDB::Exception("Was unable to create classifier.train procedure");
        }

        // create run configuration
        ProcedureRunConfig clsProcRunConf;
        clsProcRunConf.id = "run_"+to_string(progress);
        clsProcRunConf.params = jsonEncode(clsProcConf);
        Date trainStart = Date::now();
        RunOutput output = clsProcedure->run(clsProcRunConf, onProgress2);
        Date trainFinish = Date::now();


        /***
         * scoring function
         * created during the training so only add it to the cleanup list
         * **/

        resourcesToDelete.push_back("/v1/functions/" + clsProcConf.functionName.utf8String());

        /***
         * accuracy
         * **/
        auto createAccuracyProcedure = [&] (AccuracyConfig & accuracyConf)
        {
            PolyConfig accuracyProcPC;
            accuracyProcPC.id = runProcConf.experimentName + "_scorer";
            accuracyProcPC.type = "classifier.test";
            accuracyProcPC.params = accuracyConf;

            INFO_MSG(logger) << " >>>>> Creating testing procedure";
            accuracyProc = createProcedure(server, accuracyProcPC, onProgress2, true);

            resourcesToDelete.push_back("/v1/procedures/"+accuracyProcPC.id.utf8String());
        };


        // setup score expression
        string scoreExpr;
        if     (runProcConf.mode == CM_BOOLEAN ||
                runProcConf.mode == CM_REGRESSION)  scoreExpr = "\"%s\"({%s})[score] as score";
        else if(runProcConf.mode == CM_CATEGORICAL) scoreExpr = "\"%s\"({%s})[scores] as score";
        else throw MLDB::Exception("Classifier mode %d not implemented", runProcConf.mode);


        // this lambda actually runs the accuracy procedure for the given config
        auto runAccuracyFor = [&] (AccuracyConfig & accuracyConf)
        {
            auto features = extractNamedSubSelect("features", accuracyConf.testingData.stm->select);
            auto label = extractNamedSubSelect("label", accuracyConf.testingData.stm->select);
            shared_ptr<SqlRowExpression> weight = extractNamedSubSelect("weight", accuracyConf.testingData.stm->select);
            if (!weight)
                weight = SqlRowExpression::parse("1.0 as weight");

            auto score = SqlRowExpression::parse(MLDB::format(scoreExpr.c_str(),
                                                            clsProcConf.functionName.utf8String(),
                                                            features->surface.utf8String()));

            accuracyConf.testingData.stm->select = SelectExpression({features, label, weight, score});

            Timer timer;

            if(!accuracyProc)
                throw MLDB::Exception("Was unable to create accuracy procedure");

            ProcedureRunConfig accuracyProcRunConf;
            accuracyProcRunConf.id = "run_"+to_string(progress);
            accuracyProcRunConf.params = jsonEncode(accuracyConf);
            Date testStart = Date::now();
            RunOutput accuracyOutput = accuracyProc->run(accuracyProcRunConf, onProgress2);
            Date testFinish = Date::now();

            INFO_MSG(logger) << "accuracy took " << timer.elapsed();

            return make_tuple(accuracyOutput,
                              testFinish.secondsSinceEpoch() - testStart.secondsSinceEpoch());
        };


        auto getAccuracyConfig = [&] (bool onTestSet)
            {
                // create config for the accuracy procedure
                AccuracyConfig accuracyConfig;
                accuracyConfig.mode = runProcConf.mode;
                accuracyConfig.uniqueScoresOnly = runProcConf.uniqueScoresOnly;

                if(runProcConf.outputAccuracyDataset && onTestSet) {
                    PolyConfigT<Dataset> outputPC;
                    outputPC.id = MLDB::format("%s_results_%d", runProcConf.experimentName, 
                                                              (int)progress);
                    outputPC.type = "tabular";

                    {
                        InProcessRestConnection connection;
                        RestRequest request("DELETE", "/v1/datasets/"+outputPC.id.utf8String(),
                                            RestParams(), "{}");
                        server->handleRequest(connection, request);

                        if(connection.responseCode != 204) {
                            throw MLDB::Exception("HTTP error "+std::to_string(connection.responseCode)+
                                " when trying to DELETE dataset '"+outputPC.id.utf8String()+"'");
                        }
                    }
                    accuracyConfig.outputDataset.emplace(outputPC);
                }

                if(onTestSet) {
                    accuracyConfig.testingData =
                        runProcConf.testingDataOverride ? *runProcConf.testingDataOverride
                                                        : runProcConf.inputData;
                    accuracyConfig.testingData.stm->where = datasetFold.testingWhere;
                    accuracyConfig.testingData.stm->limit = datasetFold.testingLimit;
                    accuracyConfig.testingData.stm->offset = datasetFold.testingOffset;
                    accuracyConfig.testingData.stm->orderBy = datasetFold.testingOrderBy;
                }
                else {
                    accuracyConfig.testingData = runProcConf.inputData;
                    accuracyConfig.testingData.stm->where = datasetFold.trainingWhere;
                    accuracyConfig.testingData.stm->limit = datasetFold.trainingLimit;
                    accuracyConfig.testingData.stm->offset = datasetFold.trainingOffset;
                    accuracyConfig.testingData.stm->orderBy = datasetFold.trainingOrderBy;
                }

                return accuracyConfig;
            };

        auto accuracyConfig = getAccuracyConfig(true);

        // create empty testing procedure
        if(progress == 0)
            createAccuracyProcedure(accuracyConfig);

        // run evaluation on testing
        auto accuracyOutput = runAccuracyFor(accuracyConfig);

        // run evaluation on training
        std::tuple<RunOutput, double> accuracyOutputTrain;
        if(runProcConf.evalTrain) {
            auto accuracyTrainingConf = getAccuracyConfig(false);
            createAccuracyProcedure(accuracyTrainingConf);
            accuracyOutputTrain = runAccuracyFor(accuracyTrainingConf);
        }

        Json::Value duration;
        duration["train"] = trainFinish.secondsSinceEpoch() - trainStart.secondsSinceEpoch();
        duration["test"] = get<1>(accuracyOutput) + (runProcConf.evalTrain ? get<1>(accuracyOutputTrain)
                                                                           : 0);
        durationStatsGen.accumStats(duration, "");

        // Add results
        Json::Value foldRez;
        foldRez["fold"] = jsonEncode(datasetFold);
        foldRez["modelFileUrl"] = clsProcConf.modelFileUrl.toUtf8String();
        foldRez["functionName"] = clsProcConf.functionName;

        if(runProcConf.outputAccuracyDataset)
            foldRez["accuracyDataset"] = accuracyConfig.outputDataset->id;

        foldRez["resultsTest"] = jsonEncode(get<0>(accuracyOutput).results);
        foldRez["durationSecs"] = duration;
        statsGen.accumStats(foldRez["resultsTest"], "");

        if(runProcConf.evalTrain) {
            foldRez["resultsTrain"] = jsonEncode(get<0>(accuracyOutputTrain).results);
            statsGenTrain.accumStats(foldRez["resultsTrain"], "");
        }

        test_eval_results.append(foldRez);

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
                throw MLDB::Exception(MLDB::format("Unable to delete resource '%s'. "
                            "Response code %d", resource, connection.responseCode));
            }
        }
    }

    Json::Value final_res;
    final_res["folds"] = test_eval_results;
    final_res["aggregatedTest"] = statsGen.generateStatistics();
    final_res["avgDuration"] = durationStatsGen.generateStatistics();
    if(runProcConf.evalTrain) {
        final_res["aggregatedTrain"] = statsGenTrain.generateStatistics();
    }

    return RunOutput(final_res);
}


namespace {

RegisterProcedureType<ExperimentProcedure, ExperimentProcedureConfig>
regExpProc(builtinPackage(),
          "Train and test a classifier",
          "procedures/ExperimentProcedure.md.html");

} // file scope

} // namespace MLDB

