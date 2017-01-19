/** randomforest_procedure.cc                                             -*- C++ -*-
    Mathieu Marquis Bolduc, 11 Mars 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

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
#include "mldb/arch/simd_vector.h"
#include "mldb/utils/log.h"

#include <random>

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

    RandomForestRNG(int seed)
        : rng(seed + (seed == 0))
    {
    }

    mt19937 rng;

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

    ConvertProgressToJson convertProgressToJson(onProgress);
    auto boundDataset = runProcConf.trainingData.stm->from->bind(context, convertProgressToJson);

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

    auto labelVal = extractNamedSubSelect("label", runProcConf.trainingData.stm->select);
    auto featuresVal = extractNamedSubSelect("features", runProcConf.trainingData.stm->select);
    if (!labelVal || !featuresVal) {
        throw HttpReturnException(400, "trainingData must return a 'features' row and a 'label'");
    }

    auto weightVal = extractNamedSubSelect("weight", runProcConf.trainingData.stm->select);
    auto weight = weightVal ? weightVal->expression : SqlExpression::ONE;

    shared_ptr<SqlRowExpression> subSelect = extractWithinExpression(featuresVal->expression);

    if (!subSelect) {
        throw HttpReturnException(400, "trainingData must return a 'features' row");
    }

    auto label = labelVal->expression;
    
    ColumnScope colScope(server, boundDataset.dataset);
    auto boundLabel = label->bind(colScope);
    auto boundWhere = runProcConf.trainingData.stm->where->bind(colScope);
    auto boundWeight = weight->bind(colScope);

    INFO_MSG(logger) << "label uses columns " << jsonEncode(colScope.requiredColumns);

    Timer labelsTimer;

    std::vector<std::vector<CellValue> > labelsWhereWeight
        = colScope.run({boundLabel, boundWhere, boundWeight});

    const std::vector<CellValue> & labels = labelsWhereWeight[0];
    const std::vector<CellValue> & wheres = labelsWhereWeight[1];
    const std::vector<CellValue> & weights = labelsWhereWeight[2];

    INFO_MSG(logger) << "got " << labels.size() << " labels in " << labelsTimer.elapsed();

    size_t numRowsKept = 0;
    for (size_t i = 0;  i < labels.size();  ++i) {
        if (!weights[i].empty()
            && weights[i].toDouble() > 0.0
            && !labels[i].empty()
            && wheres[i].isTrue())
            ++numRowsKept;
    }

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

    INFO_MSG(logger) << "feature space construction took " << timer.elapsed();
    timer.restart();

    auto outputFeature = [&]() {
        stringstream str;
        for (auto& c : knownInputColumns) {
            str << c << " feature " << featureSpace->getFeature(c)
                << " had " << featureSpace->columnInfo[c].buckets.numBuckets
                << " buckets"
                << " type is " << featureSpace->columnInfo[c].info << endl;
        }
        return str.str();
    };
    
    TRACE_MSG(logger) << outputFeature();

    // Get the feature buckets per row

    //TODO: Need to pack this into 1 memory buffer

    int numFeatures = knownInputColumns.size();
    INFO_MSG(logger) << "NUM FEATURES : " << numFeatures;

    PartitionData allData(featureSpace);

    allData.reserve(numRowsKept);
    size_t numRows = 0;
    for (size_t i = 0;  i < labels.size();  ++i) {
        if (!wheres[i].isTrue() || labels[i].empty()
            || weights[i].empty() || weights[i].toDouble() == 0)
            continue;
        allData.addRow(labels[i].isTrue(), weights[i].toDouble(), numRows++);
    }
    ExcAssertEqual(numRows, numRowsKept);

    const float trainprop = 1.0f;
    int totalResultCount = runProcConf.featureVectorSamplings*runProcConf.featureSamplings;
    vector<shared_ptr<Decision_Tree>> results(totalResultCount);

    auto contFeatureSpace = featureSpace;

    auto doFeatureVectorSampling = [&] (int bag)
        {
            Timer bagTimer;

            size_t numPartitions = std::min<size_t>(numRows, 32);

            distribution<float> trainingWeights(numRows);
            std::atomic<size_t> numNonZero(0);
            
            // Parallelize the setup, since the slow part is random number
            // generation and we set up each bag independently
            auto doPartition = [&] (size_t i)
            {
                RandomForestRNG myrng(38341 + i);

                size_t first = (numRows / numPartitions) * i;
                size_t last = (numRows / numPartitions) * (i + 1);
                if (last > numRows || i == numPartitions)
                    last = numRows;

                mt19937 rng(245 + (bag * numPartitions) + i);

                size_t numRowsInPartition = last - first;

                distribution<float> in_training(numRowsInPartition);
                vector<int> tr_ex_nums(numRowsInPartition);
                std::iota(tr_ex_nums.begin(), tr_ex_nums.end(), 0);
                std::random_shuffle(tr_ex_nums.begin(), tr_ex_nums.end(), myrng);
                
                for (unsigned i = 0;  i < numRowsInPartition * trainprop;  ++i)
                    in_training[tr_ex_nums[i]] = 1.0;

                distribution<float> example_weights(numRowsInPartition);

                // Generate our example weights.
                for (unsigned i = 0;  i < numRowsInPartition;  ++i)
                    example_weights[myrng(numRowsInPartition)] += 1.0;

                size_t partitionNumNonZero = 0;
                double totalTrainingWeights = 0;
                for (unsigned i = 0;  i < numRowsInPartition;  ++i) {
                    float wt = in_training[i] * example_weights[i];
                    trainingWeights[first + i] = wt;
                    partitionNumNonZero += (wt != 0);
                    totalTrainingWeights += wt;
                }

                SIMD::vec_scale(trainingWeights.data() + first,
                                1.0 / (totalTrainingWeights * numPartitions),
                                trainingWeights.data() + first,
                                numRowsInPartition);
                numNonZero += partitionNumNonZero;
            };

            parallelMap(0, numPartitions, doPartition);

            INFO_MSG(logger) << "bag " << bag << " weight generation took "
                 << bagTimer.elapsed();

            INFO_MSG(logger) << "numNonZero = " << numNonZero;
            
            auto data = allData.reweightAndCompact(trainingWeights, numNonZero);

            INFO_MSG(logger) << "bag " << bag << " setup took " << bagTimer.elapsed();

            auto trainFeaturePartition = [&] (int partitionNum)
            {
                mt19937 rng(bag + 371 + partitionNum);
                uniform_real_distribution<> uniform01(0, 1);

                PartitionData mydata(data);

                // Cull the features according to the sampling proportion
                for (unsigned i = 0;  i < data.features.size();  ++i) {
                    if (mydata.features[i].active
                        && uniform01(rng) > procedureConfig.featureVectorSamplingProp) {
                        mydata.features[i].active = false;
                    }
                }

                Timer timer;
                ML::Tree tree;
                tree.root = mydata.train(0 /* depth */, runProcConf.maxDepth, tree);
                INFO_MSG(logger) << "bag " << bag << " partition " << partitionNum << " took "
                     << timer.elapsed();

                int resultIndex = bag*runProcConf.featureSamplings + partitionNum;

                results[resultIndex] = make_shared<Decision_Tree>(contFeatureSpace, labelFeature);

                results[resultIndex]->tree = std::move(tree);

                if (runProcConf.verbosity)
                    INFO_MSG(logger) << results[resultIndex]->print();
            };

            parallelMap(0, runProcConf.featureSamplings, trainFeaturePartition,
                        maxTreesAtOnce);

            INFO_MSG(logger) << "bag " << bag << " took " << bagTimer.elapsed();
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
        INFO_MSG(logger) << "Error saving classifier: " << exc.what();
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
