/** randomforest_procedure.cc                                             -*- C++ -*-
    Mathieu Marquis Bolduc, 11 Mars 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Procedure to train a random forest binary classifier.
*/

#include "randomforest_procedure.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/arch/bitops.h"
#include "mldb/arch/timers.h"
#include "mldb/jml/utils/profile.h"
#include "mldb/ml/randomforest.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/plugins/sql_expression_extractors.h"
#include "mldb/plugins/classifier.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "mldb/server/bound_queries.h"
#include "mldb/server/mldb_server.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/set_description.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/plugins/sql_config_validator.h"

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/uniform_01.hpp>

using namespace std;
using namespace ML;


namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(RandomForestProcedureConfig);

RandomForestProcedureConfigDescription::
RandomForestProcedureConfigDescription()
{
    addField("trainingData", &RandomForestProcedureConfig::trainingData,
             "Specification of the data for input to the classifier procedure. "
             "The select expression must contain these two sub-expressions: one row expression "
             "to identify the features on which to train and one scalar expression "
             "to identify the label.  The type of the label expression must be a boolean (0 or 1)"
             "Labels with a null value will have their row skipped. "
             "The select statement does not support groupby and having clauses. "
             "Also, unlike most select expressions, this one can only select whole columns, "
             "not expressions involving columns. So X will work, but not X + 1. "
             "If you need derived values in the select expression, create a dataset with "
             "the derived columns as a previous step and run the classifier over that dataset instead.");
    addField("modelFileUrl", &RandomForestProcedureConfig::modelFileUrl,
             "URL where the model file (with extension '.cls') should be saved. "
             "This file can be loaded by the ![](%%doclink classifier function). ");
    addField("featureVectorSamplings", &RandomForestProcedureConfig::featureVectorSamplings,
             "Number of samplings of feature vectors. "
             "The total number of bags will be featureVectorSamplings*featureSamplings.", 5);
    addField("featureVectorSamplingProp", &RandomForestProcedureConfig::featureVectorSamplingProp,
             "Proportion of feature vectors to select in each sample. ", 0.3f);
    addField("featureSamplings", &RandomForestProcedureConfig::featureSamplings,
             "Number of samplings of features. "
             "The total number of bags will be ```featureVectorSamplings```*```featureSamplings```.", 20);
    addField("featureSamplingProp", &RandomForestProcedureConfig::featureSamplingProp,
             "Proportion of features to select in each sample. ", 0.3f);
    addField("maxDepth", &RandomForestProcedureConfig::maxDepth,
             "Maximum depth of the trees ", 20);
    addField("functionName", &RandomForestProcedureConfig::functionName,
             "If specified, an instance of the ![](%%doclink classifier function) of this name will be created using "
             "the trained model. Note that to use this parameter, the `modelFileUrl` must "
             "also be provided.");
    addField("verbosity", &RandomForestProcedureConfig::verbosity,
             "Should the procedure be verbose for debugging and tuning purposes", false);
    addParent<ProcedureConfig>();

    onPostValidate = chain(validateQuery(&RandomForestProcedureConfig::trainingData,
                                         NoGroupByHaving(),
                                         PlainColumnSelect(),
                                         MustContainFrom(),
                                         FeaturesLabelSelect()),
                           validateFunction<RandomForestProcedureConfig>());
}

/*****************************************************************************/
/* CLASSIFIER PROCEDURE                                                       */
/*****************************************************************************/

RandomForestProcedure::
RandomForestProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->procedureConfig = config.params.convert<RandomForestProcedureConfig>();
}

Any
RandomForestProcedure::
getStatus() const
{
    return Any();
}

struct RandomForestRNG {

    RandomForestRNG()
    {
    }

    boost::mt19937 rng;

    template<class T>
    T operator () (T val)
    {
        return rng() % val;
    }
};

RunOutput
RandomForestProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{
    //Todo: we will need heuristics for those. (MLDB-1449)
    int maxBagsAtOnce = 1;
    int maxTreesAtOnce = 20;

    RandomForestProcedureConfig runProcConf =
        applyRunConfOverProcConf(procedureConfig, run);

    Timer timer;

    // this includes being empty
    if(!runProcConf.modelFileUrl.valid()) {
         throw MLDB::Exception("modelFileUrl is not valid");
    }

    checkWritability(runProcConf.modelFileUrl.toDecodedString(),
                     "modelFileUrl");

    // 1.  Get the input dataset
    SqlExpressionMldbScope context(server);

    auto boundDataset = runProcConf.trainingData.stm->from->bind(context);

    ML::Mutable_Feature_Info labelInfo = ML::Mutable_Feature_Info(ML::BOOLEAN);
    labelInfo.set_biased(true);

    auto extractWithinExpression = [](std::shared_ptr<SqlExpression> expr)
        -> std::shared_ptr<SqlRowExpression>
        {
            auto withinExpression = std::dynamic_pointer_cast<const SelectWithinExpression>(expr);
            if (withinExpression)
                return withinExpression->select;

            return nullptr;
        };

    auto label = extractNamedSubSelect("label", runProcConf.trainingData.stm->select)->expression;
    auto features = extractNamedSubSelect("features", runProcConf.trainingData.stm->select)->expression;
    shared_ptr<SqlRowExpression> subSelect = extractWithinExpression(features);

    if (!label || !subSelect)
        throw HttpReturnException(400, "trainingData must return a 'features' row and a 'label'");

    ColumnScope colScope(server, boundDataset.dataset);
    auto boundLabel = label->bind(colScope);

    cerr << "label uses columns " << jsonEncode(colScope.requiredColumns)
         << endl;

    Timer labelsTimer;

    std::vector<CellValue> labels(std::move(colScope.run({boundLabel})[0]));

    cerr << "got " << labels.size() << " labels in " << labelsTimer.elapsed()
         << endl;

    SelectExpression select({subSelect});

    auto getColumnsInExpression = [&] (const SqlExpression & expr)
        -> std::set<ColumnPath>
        {
            std::set<ColumnPath> knownInputColumns;

            // Find only those variables used
            SqlExpressionDatasetScope scope(boundDataset);
            
            auto selectBound = select.bind(scope);

            for (auto & c : selectBound.info->getKnownColumns()) {
                knownInputColumns.insert(c.columnName);
            }

            return knownInputColumns;
        };

    std::set<ColumnPath> knownInputColumns
        = getColumnsInExpression(select);

    auto featureSpace = std::make_shared<DatasetFeatureSpace>
        (boundDataset.dataset, labelInfo, knownInputColumns, true /* bucketize */);

    cerr << "feature space construction took " << timer.elapsed() << endl;
    timer.restart();

    for (auto& c : knownInputColumns) {
        cerr << c << " feature " << featureSpace->getFeature(c)
             << " had " << featureSpace->columnInfo[c].buckets.numBuckets
             << " buckets"
             << " type is " << featureSpace->columnInfo[c].info << endl;
    }

    // Get the feature buckets per row

    //TODO: Need to pack this into 1 memory buffer

    //optimize when we want every row
    size_t numRows = boundDataset.dataset->getMatrixView()->getRowCount();

    int numFeatures = knownInputColumns.size();
    cerr << "NUM FEATURES : " << numFeatures << endl;

    PartitionData allData(featureSpace);

    allData.reserve(numRows);
    for (size_t i = 0;  i < numRows;  ++i) {
        allData.addRow(labels[i].isTrue(), 1.0 /* weight */, i);
    }

    const float trainprop = 1.0f;
    RandomForestRNG myrng;
    int totalResultCount = runProcConf.featureVectorSamplings*runProcConf.featureSamplings;
    vector<shared_ptr<Decision_Tree>> results(totalResultCount);

    //We will serialize the classifier with the unbucketized feature space
    //todo: we should get it directly from the bucketized one and potentially PartitionData.
    auto contFeatureSpace = std::make_shared<DatasetFeatureSpace>
        (boundDataset.dataset, labelInfo, knownInputColumns, false /* bucketize */);

    auto doFeatureVectorSampling = [&] (int bag)
        {
            Timer bagTimer;

            boost::mt19937 rng(bag + 245);
            distribution<float> in_training(numRows);
            vector<int> tr_ex_nums(numRows);
            std::iota(tr_ex_nums.begin(), tr_ex_nums.end(), 0);
            std::random_shuffle(tr_ex_nums.begin(), tr_ex_nums.end(), myrng);
            for (unsigned i = 0;  i < numRows * trainprop;  ++i)
                in_training[tr_ex_nums[i]] = 1.0;

            distribution<float> example_weights(numRows);

            // Generate our example weights.
            for (unsigned i = 0;  i < numRows;  ++i)
                example_weights[myrng(numRows)] += 1.0;

            distribution<float> training_weights
                = in_training * example_weights;

            training_weights.normalize();

            size_t numNonZero = (training_weights != 0).count();
            cerr << "numNonZero = " << numNonZero << endl;

            auto data = allData.reweightAndCompact(training_weights);

            cerr << "bag " << bag << " setup took " << bagTimer.elapsed() << endl;

            auto trainFeaturePartition = [&] (int partitionNum)
            {
                boost::mt19937 rng(bag + 245 + partitionNum);

                PartitionData mydata(data);
                for (unsigned i = 0;  i < data.features.size();  ++i) {
                    if (mydata.features[i].active
                        && rng() % 3 != 0)
                        mydata.features[i].active = false;
                }

                Timer timer;
                ML::Tree tree;
                tree.root = mydata.train(0 /* depth */, runProcConf.maxDepth, tree);
                cerr << "bag " << bag << " partition " << partitionNum << " took "
                     << timer.elapsed() << endl;

                int resultIndex = bag*runProcConf.featureSamplings + partitionNum;

                results[resultIndex] = make_shared<Decision_Tree>(contFeatureSpace, labelFeature);

                results[resultIndex]->tree = std::move(tree);

                if (runProcConf.verbosity)
                    cerr << results[resultIndex]->print() << endl;
                //cerr << dtree.print() << endl;
            };

            parallelMap(0, runProcConf.featureSamplings, trainFeaturePartition,
                        maxTreesAtOnce);

            cerr << "bag " << bag << " took " << bagTimer.elapsed() << endl;
        };

    parallelMap(0, runProcConf.featureVectorSamplings, doFeatureVectorSampling, maxBagsAtOnce);

    shared_ptr<Committee> result = make_shared<Committee>(contFeatureSpace, labelFeature);

    for (unsigned i = 0;  i < totalResultCount;  ++i)
        result->add(results[i], 1.0 / totalResultCount);

    ML::Classifier classifier(result);

    //Save the model, create the function

    bool saved = true;
    try {
        makeUriDirectory(
            runProcConf.modelFileUrl.toDecodedString());
        classifier.save(runProcConf.modelFileUrl.toString());
    }
    catch (const std::exception & exc) {
        saved = false;
        cerr << "Error saving classifier: " << exc.what() << endl;
    }

    if(saved && !runProcConf.functionName.empty()) {
        PolyConfig clsFuncPC;
        clsFuncPC.type = "classifier";
        clsFuncPC.id = runProcConf.functionName;
        clsFuncPC.params = ClassifyFunctionConfig(runProcConf.modelFileUrl);

        obtainFunction(server, clsFuncPC, onProgress);
    }

    return RunOutput();
}

namespace{
	static RegisterProcedureType<RandomForestProcedure, RandomForestProcedureConfig>
	regPrototypeClassifier(builtinPackage(),
	              "Train a supervised binary random forest",
	              "procedures/RandomForest.md.html");

}

} // namespace MLDB
