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
#include "mldb/utils/profile.h"
#include "mldb/plugins/jml/randomforest.h"
#include "mldb/plugins/jml/value_descriptions.h"
#include "mldb/builtin/sql_expression_extractors.h"
#include "mldb/plugins/jml/classifier.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "mldb/core/bound_queries.h"
#include "mldb/core/mldb_engine.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/set_description.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/builtin/sql_config_validator.h"
#include "mldb/arch/simd_vector.h"
#include "mldb/utils/log.h"

#include <random>

using namespace std;
using namespace ML;


namespace MLDB {

using namespace RF;


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
    addField("sampleFeatureVectors", &RandomForestProcedureConfig::sampleFeatureVectors,
             "Should we sample feature vectors (default yes)?  Should only be set "
             "to false for testing or with a very small number of featureVectorSamplings.",
             true);
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
RandomForestProcedure(MldbEngine * owner,
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
    int maxBagsAtOnce = 5;
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
    SqlExpressionMldbScope context(engine);

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
        throw AnnotatedException(400, "trainingData must return a 'features' row and a 'label'");
    }

    auto weightVal = extractNamedSubSelect("weight", runProcConf.trainingData.stm->select);
    auto weight = weightVal ? weightVal->expression : SqlExpression::ONE;

    shared_ptr<SqlRowExpression> subSelect = extractWithinExpression(featuresVal->expression);

    if (!subSelect) {
        throw AnnotatedException(400, "trainingData must return a 'features' row");
    }

    auto label = labelVal->expression;
    
    ColumnScope colScope(engine, boundDataset.dataset);
    auto boundLabel = label->bind(colScope);
    auto boundWhere = runProcConf.trainingData.stm->where->bind(colScope);
    auto boundWeight = weight->bind(colScope);

    INFO_MSG(logger) << "label uses columns " << jsonEncode(colScope.requiredColumns);

    Timer labelsTimer;

    std::vector<std::vector<float> > labelsWhereWeight
        = colScope.runFloat({boundLabel, boundWhere, boundWeight});

    std::vector<float> & labels = labelsWhereWeight[0];
    std::vector<float> & wheres = labelsWhereWeight[1];
    std::vector<float> & weights = labelsWhereWeight[2];

    INFO_MSG(logger) << "got " << labels.size() << " labels in " << labelsTimer.elapsed();

    auto keepExample = [&] (size_t i) -> bool
        {
            return
                !isnan(weights[i])
                && weights[i] > 0.0
                && !isnan(labels[i])
                && !isnan(wheres[i])
                && wheres[i] != 0;
        };
    
    size_t numRowsKept = 0;
    size_t numTrue = 0;
    size_t numFalse = 0;
    for (size_t i = 0;  i < labels.size();  ++i) {
        if (keepExample(i)) {
            (!!labels[i] ? numTrue: numFalse) += 1;
            ++numRowsKept;
        }
    }

    cerr << "labels: false " << numFalse << " true " << numTrue << endl;
    
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

    MemorySerializer serializer;

    Timer beforeTrees;
    
    PartitionData allData(featureSpace);

    auto writer = allData.rows.getRowWriter(numRowsKept, numRowsKept,
                                            serializer,
                                            false /* sequenial example nums */);

    size_t numRows = 0;
    for (size_t i = 0;  i < labels.size();  ++i) {
        if (!keepExample(i))
            continue;
        writer.addRow(!!labels[i], weights[i], numRows++);
    }
    ExcAssertEqual(numRows, numRowsKept);

    allData.rows = writer.freeze(serializer);

    // Save memory by removing these now they are no longer needed
    labels = std::vector<float>();
    weights = std::vector<float>();
    wheres = std::vector<float>();
    
    const float trainprop = 1.0f;
    int totalResultCount = runProcConf.featureVectorSamplings*runProcConf.featureSamplings;
    vector<shared_ptr<Decision_Tree> > results(totalResultCount);

    auto contFeatureSpace = featureSpace;

    std::atomic<int> bagsDone(0);
    
    auto doFeatureVectorSampling = [&] (int bag)
        {
            Timer bagTimer;

            size_t numPartitions = std::min<size_t>(numRows, 32);

            distribution<uint8_t> trainingWeights(numRows);
            std::atomic<size_t> numNonZero(0);

            std::mutex totalTrainingWeightMutex;
            double totalTrainingWeight = 0;
            
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

                distribution<uint8_t> in_training(numRowsInPartition);
                distribution<uint8_t> example_weights(numRowsInPartition);
                

                if (runProcConf.sampleFeatureVectors) {
                    vector<int> tr_ex_nums(numRowsInPartition);
                    std::iota(tr_ex_nums.begin(), tr_ex_nums.end(), 0);
                    std::shuffle(tr_ex_nums.begin(), tr_ex_nums.end(), rng);
                
                    for (unsigned i = 0;  i < numRowsInPartition * trainprop;  ++i)
                        in_training[tr_ex_nums[i]] = 1;

                    // Generate our example weights.
                    for (unsigned i = 0;  i < numRowsInPartition;  ++i)
                        example_weights[myrng(numRowsInPartition)] += 1;
                }
                else {
                    std::fill(in_training.begin(), in_training.end(), 1);
                    std::fill(example_weights.begin(), example_weights.end(), 1);
                }

                size_t partitionNumNonZero = 0;
                double totalTrainingWeights = 0;
                for (unsigned i = 0;  i < numRowsInPartition;  ++i) {
                    uint8_t wt = in_training[i] * example_weights[i];
                    trainingWeights[first + i] = wt;
                    partitionNumNonZero += (wt != 0);
                    totalTrainingWeights += wt;
                }

                //SIMD::vec_scale(trainingWeights.data() + first,
                //                1.0 / (totalTrainingWeights * numPartitions),
                //                trainingWeights.data() + first,
                //                numRowsInPartition);
                numNonZero += partitionNumNonZero;

                std::unique_lock<std::mutex> guard(totalTrainingWeightMutex);
                totalTrainingWeight += totalTrainingWeights;
            };

            parallelMap(0, numPartitions, doPartition);

            INFO_MSG(logger) << "bag " << bag << " weight generation took "
                 << bagTimer.elapsed();

            INFO_MSG(logger) << "numNonZero = " << numNonZero;
            
            MemorySerializer serializer;

            auto data = allData.reweightAndCompact(trainingWeights, numNonZero,
                                                   1.0 / totalTrainingWeight,
                                                   serializer);
            
            INFO_MSG(logger) << "bag " << bag << " setup took " << bagTimer.elapsed();

            // The last one clears the original data to save space
            if (bagsDone.fetch_add(1) == runProcConf.featureVectorSamplings - 1) {
                allData.clear();
            }

            auto trainFeaturePartition = [&] (int partitionNum)
            {
                mt19937 rng(bag + 371 + partitionNum);
                uniform_real_distribution<> uniform01(0, 1);

                PartitionData mydata(data);

                // Cull the features according to the sampling proportion
                for (int attempt = 0;  true /* break in loop */; ++attempt) {
                    int numActiveFeatures = 0;

                    // Cull the features according to the sampling proportion
                    for (unsigned i = 0;  i < data.features.size();  ++i) {
                        if (data.features[i].active
                            && uniform01(rng) > procedureConfig.featureSamplingProp) {
                            mydata.features[i].active = false;
                        }
                        else {
                            mydata.features[i].active = true;
                            ++numActiveFeatures;
                        }
                    }
                    
                    if (numActiveFeatures == 0) {
                        if (attempt == 9) {
                            throw AnnotatedException
                                (400, "Feature partition had no features; "
                                 "consider increasing featureVectorSamplingProp");
                        }
                    }
                    else {
                        break;
                      }
                }

                Timer timer;
                ML::Tree tree;
                tree.root = mydata.train(0 /* depth */, runProcConf.maxDepth, tree, serializer);
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

    cerr << "tree construction took " << beforeTrees.elapsed() << endl;
    
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

        obtainFunction(engine, clsFuncPC, onProgress);
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
