/** classifier.cc
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Integration of JML machine learning library to train classifiers.
*/

#include "classifier.h"
#include "mldb/ml/jml/classifier.h"
#include "dataset_feature_space.h"
#include "mldb/server/mldb_server.h"
#include "mldb/core/dataset.h"
#include "mldb/server/bound_queries.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/jml/utils/guard.h"
#include "mldb/base/parallel.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/arch/timers.h"
#include "mldb/arch/simd_vector.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/plugins/sql_config_validator.h"
#include "mldb/plugins/sql_expression_extractors.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/ml/jml/training_data.h"
#include "mldb/ml/jml/training_index.h"
#include "mldb/ml/jml/classifier_generator.h"
#include "mldb/ml/jml/registry.h"
#include "mldb/jml/utils/map_reduce.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/server/analytics.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/server/parallel_merge_sort.h"
#include "mldb/ml/jml/feature_info.h"
#include "ml/value_descriptions.h"
#include "mldb/types/any_impl.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "mldb/server/static_content_macro.h"


using namespace std;
using namespace ML;

namespace Datacratic {
namespace MLDB {

DEFINE_ENUM_DESCRIPTION(ClassifierMode);

ClassifierModeDescription::
ClassifierModeDescription()
{
    addValue("regression",  CM_REGRESSION, "Regression mode (predicting values)");
    addValue("boolean",     CM_BOOLEAN, "Boolean mode (predicting P(true))");
    addValue("categorical", CM_CATEGORICAL, "Categorical mode (predicting P(category))");
}


DEFINE_STRUCTURE_DESCRIPTION(ClassifierConfig);

ClassifierConfigDescription::
ClassifierConfigDescription()
{
    addField("trainingData", &ClassifierConfig::trainingData,
             "Specification of the data for input to the classifier procedure. "
             "The select expression must contain these two sub-expressions: one row expression "
             "to identify the features on which to train and one scalar expression "
             "to identify the label.  The type of the label expression must match "
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
    addField("modelFileUrl", &ClassifierConfig::modelFileUrl,
             "URL where the model file (with extension '.cls') should be saved. "
             "This file can be loaded by a function of type 'classifier'.");
    addField("algorithm", &ClassifierConfig::algorithm,
             "Algorithm to use to train classifier with.  This must point to "
             "an entry in the configuration or configurationFile parameters");
    addField("configuration", &ClassifierConfig::configuration,
             "Configuration object to use for the classifier.  Each one has "
             "its own parameters.  If none is passed, then the configuration "
             "will be loaded from the ConfigurationFile parameter",
             Json::Value());
    addField("configurationFile", &ClassifierConfig::configurationFile,
             "File to load configuration from.  This is a JSON file containing "
             "only objects, strings and numbers.  If the configuration object is "
             "non-empty, then that will be used preferentially.",
             string("/opt/bin/classifiers.json"));
    addField("equalizationFactor", &ClassifierConfig::equalizationFactor,
             "Amount to adjust weights so that all classes have an equal "
             "total weight.  A value of 0 will not equalize weights "
             "at all.  A value of 1 will ensure that the total weight for "
             "both positive and negative examples is exactly identical. "
             "A number between will choose a balanced tradeoff.  Typically 0.5 (default) "
             "is a good number to use for unbalanced probabilities",
             0.5);
    addField("mode", &ClassifierConfig::mode,
             "Mode of classifier.  Controls how the label is interpreted and "
             "what is the output of the classifier.", CM_BOOLEAN);
    addField("functionName", &ClassifierConfig::functionName,
             "If specified, a classifier function of this name will be created using "
             "the trained classifier.");
    addParent<ProcedureConfig>();

    onPostValidate = validate<ClassifierConfig, 
                              InputQuery,
                              NoGroupByHaving,
                              PlainColumnSelect,
                              MustContainFrom,
                              FeaturesLabelSelect>(&ClassifierConfig::trainingData, "classifier");
}

/*****************************************************************************/
/* CLASSIFIER PROCEDURE                                                       */
/*****************************************************************************/

ClassifierProcedure::
ClassifierProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->procedureConfig = config.params.convert<ClassifierConfig>();
}

Any
ClassifierProcedure::
getStatus() const
{
    return Any();
}

RunOutput
ClassifierProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{
    // 1.  Construct an applyFunctionToProcedure object
    
    // 2.  Extend with our training function

    // 3.  Apply everything to construct the dataset

    // 4.  Apply the dataset


    ClassifierConfig runProcConf =
        applyRunConfOverProcConf(procedureConfig, run);

    // this includes being empty
    if(!runProcConf.modelFileUrl.valid()) {
        throw ML::Exception("modelFileUrl is not valid");
    }

    // 1.  Get the input dataset
    SqlExpressionMldbContext context(server);

    auto boundDataset = runProcConf.trainingData.stm->from->bind(context);

    std::shared_ptr<ML::Mutable_Categorical_Info> categorical;

    ML::Mutable_Feature_Info labelInfo;

    switch (runProcConf.mode) {
    case CM_REGRESSION:
        labelInfo = ML::Mutable_Feature_Info(ML::REAL);
        break;
    case CM_BOOLEAN:
        labelInfo = ML::Mutable_Feature_Info(ML::BOOLEAN);
        break;
    case CM_CATEGORICAL:
        categorical = std::make_shared<ML::Mutable_Categorical_Info>();
        labelInfo = ML::Feature_Info(categorical);
        break;
    default:
        throw HttpReturnException(400, "Unknown classifier mode");
    }

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
    auto weightSubSelect = extractNamedSubSelect("weight", runProcConf.trainingData.stm->select);
    shared_ptr<SqlExpression> weight = weightSubSelect ? weightSubSelect->expression : SqlExpression::ONE;
    shared_ptr<SqlRowExpression> subSelect = extractWithinExpression(features);

    if (!label || !subSelect)
        throw HttpReturnException(400, "trainingData must return a 'features' row and a 'label'");

    SelectExpression select({subSelect});

    std::set<ColumnName> knownInputColumns;
    {
        // Find only those variables used
        SqlExpressionDatasetContext context(boundDataset);
        
        auto selectBound = select.bind(context);

        for (auto & c : selectBound.info->getKnownColumns()) {
            knownInputColumns.insert(c.columnName);
        }
    }

    // cerr << "knownInputColumns are " << jsonEncode(knownInputColumns);

    ML::Timer timer;

    // TODO: it's not the feature space itself, but indeed the output of
    // the select expression that's important...
    auto featureSpace = std::make_shared<DatasetFeatureSpace>
        (boundDataset.dataset, labelInfo, knownInputColumns);
    
    cerr << "initialized feature space in " << timer.elapsed() << endl;
    
    // We want to calculate the label and weight of each row as well
    // as the select expression
    std::vector<std::shared_ptr<SqlExpression> > extra
        = { label, weight };

    struct Fv {
        Fv()
        {
        }

        Fv(RowName rowName,
           ML::Mutable_Feature_Set featureSet)
            : rowName(std::move(rowName)),
              featureSet(std::move(featureSet))
        {
        }

        RowName rowName;
        ML::Mutable_Feature_Set featureSet;
        
        float label() const
        {
            ExcAssertEqual(featureSet.at(0).first, labelFeature);
            return featureSet.at(0).second;
        }

        float weight() const
        {
            ExcAssertEqual(featureSet.at(1).first, weightFeature);
            return featureSet.at(1).second;
        }

        void setLabel(float label)
        {
            ExcAssertEqual(featureSet.at(0).first, labelFeature);
            featureSet.at(0).second = label;
        }
        
        bool operator < (const Fv & other) const
        {
            return rowName < other.rowName
               || (rowName == other.rowName
                   && std::lexicographical_compare(featureSet.begin(),
                                                   featureSet.end(),
                                                   other.featureSet.begin(),
                                                   other.featureSet.end()));
        }
    };

    // Build it
    struct ThreadAccum {
        std::vector<Fv> fvs;

        // These are for categorical variables only.  Since we need to create a
        // stable label ordering to enable determinism in model training,
        // but we don't know the label alphabet ahead of time, we accumulate the
        // labels here as well as an alphabet, and then when merging together
        // we re-map them onto their final values.
        std::map<std::string, int> categoricalLabels;
        std::vector<std::string> categoricalLabelList;
        std::map<int, int> labelMapping;

        void sort()
        {
            if (!labelMapping.empty()) {
                for (auto & fv: fvs) {
                    // Modify the explicit label field
                    float label = fv.label();
                    ExcAssert(labelMapping.count(label));
                    fv.setLabel(labelMapping[label]);
                }
                labelMapping.clear();
            }

            std::sort(fvs.begin(), fvs.end());
        }

        static void merge(ThreadAccum & t1, ThreadAccum & t2)
        {
            size_t split = t1.fvs.size();

            t1.fvs.insert(t1.fvs.end(), 
                          std::make_move_iterator(t2.fvs.begin()),
                          std::make_move_iterator(t2.fvs.end()));
            t2.fvs.clear();

            std::inplace_merge(t1.fvs.begin(),
                               t1.fvs.begin() + split,
                               t1.fvs.end());
        }
    };

    std::atomic<int> numRows(0);

    PerThreadAccumulator<ThreadAccum> accum;


    auto aggregator = [&] (NamedRowValue & row_,
                           const std::vector<ExpressionValue> & extraVals)
        {
            MatrixNamedRow row = row_.flattenDestructive();
            CellValue label = extraVals.at(0).getAtom();
            if (label.empty())
                return true;

            ThreadAccum & thr = accum.get();

            float encodedLabel;
            switch (runProcConf.mode) {
            case CM_REGRESSION:
                encodedLabel = label.toDouble();
                break;
            case CM_BOOLEAN:
                encodedLabel = label.isTrue();
                break;
            case CM_CATEGORICAL: {
                // Get a list of categorical labels, for this thread.  Later
                // we map them to the overall list of labels.
                std::string labelStr = jsonEncodeStr(label);
                auto it = thr.categoricalLabels.find(labelStr);
                if (it == thr.categoricalLabels.end()) {
                    encodedLabel = thr.categoricalLabelList.size();
                    thr.categoricalLabelList.push_back(labelStr);
                    thr.categoricalLabels.emplace(labelStr, encodedLabel);
                }
                else {
                    encodedLabel = it->second;
                }

                break;
            }
            default:
                throw HttpReturnException(400, "Unknown classifier mode");
            }

            float weight = extraVals.at(1).toDouble();

            //cerr << "label = " << label << " weight = " << weight << endl;
            //cerr << "row.columns.size() = " << row.columns.size() << endl;

            //cerr << "got row " << jsonEncode(row) << endl;
            ++numRows;

            std::vector<std::pair<ML::Feature, float> > features
            = { { labelFeature, encodedLabel }, { weightFeature, weight } };
                
            for (auto & c: row.columns) {
                featureSpace->encodeFeature(std::get<0>(c), std::get<1>(c), features);
            }

            thr.fvs.emplace_back(row.rowName, std::move(features));
            return true;
        };

    // If no order by or limit, the order doesn't matter
    if (runProcConf.trainingData.stm->limit == -1 && runProcConf.trainingData.stm->offset == 0)
        runProcConf.trainingData.stm->orderBy.clauses.clear();

    timer.restart();

    BoundSelectQuery(select, *boundDataset.dataset,
                     boundDataset.asName, runProcConf.trainingData.stm->when,
                     *runProcConf.trainingData.stm->where,
                     runProcConf.trainingData.stm->orderBy, extra,
                     false /* implicit order by row hash */)
        .execute(aggregator, 
                 runProcConf.trainingData.stm->offset, 
                 runProcConf.trainingData.stm->limit, 
                 nullptr /* progress */);

    cerr << "extracted feature vectors in " << timer.elapsed() << endl;
    
    // If we're categorical, we need to sort out the labels over all
    // of the threads.

    std::map<std::string, int> labelMapping;
    
    if (runProcConf.mode == CM_CATEGORICAL) {
    
        std::set<std::string> allLabels;

        auto onThread = [&] (ThreadAccum * acc)
            {
                allLabels.insert(acc->categoricalLabelList.begin(),
                                 acc->categoricalLabelList.end());
            };
        
        accum.forEach(onThread);

        // Now, initialize a mapping for each thread
        for (auto & labelStr: allLabels) {
            int encodedLabel = categorical->parse_or_add(labelStr);
            labelMapping[labelStr] = encodedLabel;
        }
        
        auto onThread2 = [&] (ThreadAccum * acc)
            {
                for (auto & labelStr: acc->categoricalLabelList) {
                    acc->labelMapping[acc->categoricalLabels[labelStr] ]
                        = labelMapping[labelStr];
                }
            };
        
        accum.forEach(onThread2);
    }

    // Now merge them together in parallel

    std::vector<Fv> fvs;

    timer.restart();
   
    parallelMergeSortRecursive(accum.threads, 0, accum.threads.size(),
                               [] (const std::shared_ptr<ThreadAccum> & t)
                               {
                                   t->sort();
                               },
                               [] (const std::shared_ptr<ThreadAccum> & t1,
                                   const std::shared_ptr<ThreadAccum> & t2)
                               {
                                   ThreadAccum::merge(*t1, *t2);
                               },
                               [] (const std::shared_ptr<ThreadAccum> & t)
                               {
                                   return t->fvs.size();
                               },
                               10000 /* thread threshold */);
    
    cerr << "merged feature vectors in " << timer.elapsed() << endl;

    if (!accum.threads.empty()) {
        fvs = std::move(accum.threads[0]->fvs);
    }

    ExcAssertEqual(fvs.size(), numRows);

    int nx = numRows;

    if (nx == 0 && boundDataset.dataset->getMatrixView()->getRowHashes(0, 1).empty()) {
        throw HttpReturnException(400, "Error training classifier: "
                                  "No feature vectors were produced as dataset was empty",
                                  "datasetConfig", boundDataset.dataset->config_,
                                  "datasetName", boundDataset.dataset->config_->id,
                                  "datasetStatus", boundDataset.dataset->getStatus());
    }

    if (nx == 0) {
        throw HttpReturnException(400, "Error training classifier: "
                                  "No feature vectors were produced as all rows were filtered by "
                                    "WHEN, WHERE, OFFSET or LIMIT, or all labels were NULL (or "
                                    "label column doesn't exist)",
                                  "datasetConfig", boundDataset.dataset->config_,
                                  "datasetName", boundDataset.dataset->config_->id,
                                  "datasetStatus", boundDataset.dataset->getStatus(),
                                  "whenClause", runProcConf.trainingData.stm->when,
                                  "whereClause", runProcConf.trainingData.stm->where,
                                  "offsetClause", runProcConf.trainingData.stm->offset,
                                  "limitClause", runProcConf.trainingData.stm->limit);
    }
    
    timer.restart();

    ML::Training_Data trainingSet(featureSpace);

    ML::distribution<float> labelWeights[2];
    labelWeights[0].resize(nx);
    labelWeights[1].resize(nx);

    ML::distribution<float> exampleWeights(nx);

    for (unsigned i = 0;  i < nx;  ++i) {
        float label  = fvs[i].label();
        float weight = fvs[i].weight();

        if (weight < 0)
            throw HttpReturnException(400, "classifier example weight cannot be negative");
        if (!isfinite(weight))
            throw HttpReturnException(400, "classifier example weights must be finite");

        trainingSet.add_example(std::make_shared<ML::Mutable_Feature_Set>(std::move(fvs[i].featureSet)));

        labelWeights[0][i] = weight * !label;
        labelWeights[1][i] = weight * label;
        exampleWeights[i]  = weight;
    }

    ExcAssertEqual(nx, trainingSet.example_count());

    cerr << "added feature vectors in " << timer.elapsed() << endl;


    {
        timer.restart();
        trainingSet.preindex(labelFeature);

        //cerr << "indexed " << nx
        //     << " feature vectors in " << after.secondsSince(before)
        //     << " at " << nx / after.secondsSince(before)
        //     << " per second" << endl;
    }

    cerr << "indexed training data in " << timer.elapsed() << endl;

    // ...
    //trainingSet.dump("training_set.txt.gz");

    // Find all features
    std::vector<ML::Feature> allFeatures = trainingSet.index().all_features();

    cerr << "Training with " << allFeatures.size() << " features" << endl;

    std::vector<ML::Feature> trainingFeatures;

    for (unsigned i = 0;  i < allFeatures.size();  ++i) {
        //cerr << "allFeatures[i] = " << allFeatures[i] << endl;

        string featureName = featureSpace->print(allFeatures[i]);
        //cerr << "featureName = " << featureName << endl;

        if (allFeatures[i] == labelFeature)
            continue;
        if (allFeatures[i] == weightFeature)
            continue;

#if 0
        if (boost::regex_match(featureName, excludeFeatures)
            || featureName == "LABEL") {
            cerr << "excluding feature " << featureName << " from training"
                 << endl;
            continue;
        }
#endif
        trainingFeatures.push_back(allFeatures[i]);
    }

    ML::Configuration classifierConfig;

    if (!runProcConf.configuration.isNull()) {
        classifierConfig = jsonDecode<ML::Configuration>(runProcConf.configuration);
    }
    else {
        ML::filter_istream stream(runProcConf.configurationFile.size() > 0 ?
                                  runProcConf.configurationFile :
                                  "/opt/bin/classifiers.json");
        classifierConfig = jsonDecodeStream<ML::Configuration>(stream);
    }

    timer.restart();

    std::shared_ptr<ML::Classifier_Generator> trainer
        = ML::get_trainer(runProcConf.algorithm,
                          classifierConfig);

    trainer->init(featureSpace, labelFeature);

    int randomSeed = 1;

    double equalizationFactor = runProcConf.equalizationFactor;

    ML::Thread_Context threadContext;
    threadContext.seed(randomSeed);

    ML::distribution<float> weights;
    if(runProcConf.mode == CM_REGRESSION) {
        weights = exampleWeights;
    }
    else {
        double factorTrue  = pow(labelWeights[1].total(), -equalizationFactor);
        double factorFalse = pow(labelWeights[0].total(), -equalizationFactor);

        cerr << "factorTrue = " << factorTrue << endl;
        cerr << "factorFalse = " << factorFalse << endl;

        weights = exampleWeights
            * (factorTrue  * labelWeights[true]
            + factorFalse * labelWeights[false]);

        weights.normalize();
    }

    //cerr << "training classifier" << endl;
    ML::Classifier classifier(trainer->generate(threadContext, trainingSet, weights,
                                                trainingFeatures));
    //cerr << "done training classifier" << endl;

    cerr << "trained classifier in " << timer.elapsed() << endl;

    bool saved = true;
    try {
        Datacratic::makeUriDirectory(runProcConf.modelFileUrl.toString());
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

        InProcessRestConnection connection;
        RestRequest request("PUT", "/v1/functions/" + runProcConf.functionName.rawString(),
                RestParams(), jsonEncode(clsFuncPC).toString());
        server->handleRequest(connection, request);
    }

    //cerr << "done saving classifier" << endl;

    //trainingSet.dump("training_set.txt.gz");
 
    return RunOutput();
}


/*****************************************************************************/
/* CLASSIFIER FUNCTION                                                          */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(ClassifyFunctionConfig);

ClassifyFunctionConfigDescription::
ClassifyFunctionConfigDescription()
{
    addField("modelFileUrl", &ClassifyFunctionConfig::modelFileUrl,
             "URL of the model file (with extension '.cls') to load. "
             "This file is created by a procedure of type 'classifier.train'.");
}

struct ClassifyFunction::Itl {
    ML::Classifier classifier;
    std::shared_ptr<const DatasetFeatureSpace> featureSpace;
    ML::Feature_Info labelInfo;
    ClassifierMode mode;
};

ClassifyFunction::
ClassifyFunction(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<ClassifyFunctionConfig>();

    itl.reset(new Itl());

    itl->classifier.load(functionConfig.modelFileUrl.toString());

    itl->featureSpace = itl->classifier.feature_space<DatasetFeatureSpace>();

    ML::Feature_Info labelInfo = itl->featureSpace->info(labelFeature);

    itl->labelInfo = labelInfo;

    //cerr << "labelInfo = " << labelInfo << endl;
}

ClassifyFunction::
ClassifyFunction(MldbServer * owner,
                 std::shared_ptr<ML::Classifier_Impl> classifier,
                 const std::string & labelFeatureName)
    : Function(owner)
{
    itl.reset(new Itl());

    itl->classifier = classifier;

    //itl->featureSpace = itl->classifier.feature_space<DatasetFeatureSpace>();

    //int index = itl->featureSpace->feature_index(labelFeatureName);
    //
    //ML::Feature_Info labelInfo = itl->featureSpace->info(ML::Feature(index, 0, 0));

    //itl->labelInfo = labelInfo;
}

ClassifyFunction::
~ClassifyFunction()
{
}

Any
ClassifyFunction::
getStatus() const
{
    Json::Value result;
    result["summary"] = itl->classifier.impl->summary();
    result["mode"] = jsonEncode(itl->mode);
    return result;
}

Any
ClassifyFunction::
getDetails() const
{
    Json::Value result;
    result["model"] = jsonEncode(itl->classifier.impl);
    return result;
}

std::tuple<std::vector<float>, std::shared_ptr<ML::Mutable_Feature_Set>, Date>
ClassifyFunction::
getFeatureSet(const FunctionContext & context, bool attemptDense) const
{
    auto row = context.get<RowValue>("features");

    Date ts = Date::negativeInfinity();

    bool multiValue = false;
    if (attemptDense) {
        std::vector<float> denseFeatures(itl->featureSpace->columnInfo.size(),
                                         std::numeric_limits<float>::quiet_NaN());
        for (auto & r: row) {
            ColumnName columnName(std::get<0>(r));
            ColumnHash columnHash(columnName);

            auto it = itl->featureSpace->columnInfo.find(columnHash);
            if (it == itl->featureSpace->columnInfo.end())
                continue;

            CellValue value = std::get<1>(r);
            ts.setMax(std::get<2>(r));

            // TODO: if more than one value, we need to fall back
            if (!isnanf(denseFeatures[it->second.index])) {
                multiValue = true;
                break;
            }
        
            denseFeatures[it->second.index]
                = itl->featureSpace->encodeFeatureValue(columnHash, value);
        }
        if (!multiValue)
            return std::make_tuple( std::move(denseFeatures), nullptr, ts );
    }


    std::vector<std::pair<ML::Feature, float> > features;

    //cerr << "row = " << jsonEncode(row) << endl;

    for (auto & r: row) {
        ColumnName columnName(std::get<0>(r));
        ColumnHash columnHash(columnName);

        auto it = itl->featureSpace->columnInfo.find(columnHash);
        if (it == itl->featureSpace->columnInfo.end())
            continue;

        CellValue value = std::get<1>(r);
        ts.setMax(std::get<2>(r));

        itl->featureSpace->encodeFeature(columnHash, value, features);
    }

    std::sort(features.begin(), features.end());
    
    auto fset = std::make_shared<ML::Mutable_Feature_Set>(features.begin(), features.end());
    fset->locked = true;

    return std::make_tuple( vector<float>(), std::move(fset), ts );
}

struct ClassifyFunctionApplier: public FunctionApplier {
    ClassifyFunctionApplier(const Function * owner)
        : FunctionApplier(owner)
    {
        info = owner->getFunctionInfo();
    }

    ML::Optimization_Info optInfo;
};

std::unique_ptr<FunctionApplier>
ClassifyFunction::
bind(SqlBindingScope & outerContext,
     const FunctionValues & input) const
{
    // Assume there is one of each features
    vector<ML::Feature> features(itl->featureSpace->columnInfo.size());

    for (auto & col: itl->featureSpace->columnInfo)
        features[col.second.index] = itl->featureSpace->getFeature(col.first);

    std::unique_ptr<ClassifyFunctionApplier> result
        (new ClassifyFunctionApplier(this));
    result->optInfo = itl->classifier.impl->optimize(features);
 
    return std::move(result);
}

FunctionOutput
ClassifyFunction::
apply(const FunctionApplier & applier_,
      const FunctionContext & context) const
{
    auto & applier = (ClassifyFunctionApplier &)applier_;

    FunctionOutput result;

    int labelCount = itl->classifier.label_count();

    std::vector<float> dense;
    std::shared_ptr<ML::Mutable_Feature_Set> fset;
    Date ts;

    std::tie(dense, fset, ts) = getFeatureSet(context, true /* try to optimize */);

    auto cat = itl->labelInfo.categorical();
    if (!dense.empty()) {
        if (cat) {
            auto scores = itl->classifier.impl->predict(dense, applier.optInfo);
            ExcAssertEqual(scores.size(), labelCount);

            vector<tuple<Coord, ExpressionValue> > row;
            for (unsigned i = 0;  i < labelCount;  ++i) {
                row.emplace_back(RowName(cat->print(i)),
                                 ExpressionValue(scores[i], ts));
            }

            result.set("scores", row);
        }
        else if (itl->labelInfo.type() == ML::REAL) {
            ExcAssertEqual(labelCount, 1);
            float score = itl->classifier.impl->predict(0, dense, applier.optInfo);
            result.set("score", ExpressionValue(score, ts));
        }
        else {
            ExcAssertEqual(labelCount, 2);
            float score = itl->classifier.impl->predict(1, dense, applier.optInfo);
            result.set("score", ExpressionValue(score, ts));
        }
    }
    else {
        if (cat) {
            auto scores = itl->classifier.predict(*fset);
            ExcAssertEqual(scores.size(), labelCount);

            vector<tuple<Coord, ExpressionValue> > row;

            for (unsigned i = 0;  i < labelCount;  ++i) {
                row.emplace_back(RowName(cat->print(i)),
                                 ExpressionValue(scores[i], ts));
            }
        
            result.set("scores", row);
        }
        else if (itl->labelInfo.type() == ML::REAL) {
            ExcAssertEqual(labelCount, 1);
            float score = itl->classifier.predict(0, *fset);
            result.set("score", ExpressionValue(score, ts));
        }
        else {
            ExcAssertEqual(labelCount, 2);
            float score = itl->classifier.predict(1, *fset);
            result.set("score", ExpressionValue(score, ts));
        }
    }

    return result;
}

FunctionInfo
ClassifyFunction::
getFunctionInfo() const
{
    FunctionInfo result;

    std::vector<KnownColumn> inputColumns;

    // Input is cell values
    for (auto & col: itl->featureSpace->columnInfo) {
        
        ColumnSparsity sparsity = col.second.info.optional()
            ? COLUMN_IS_SPARSE : COLUMN_IS_DENSE;

        //cerr << "column " << col.second.columnName << " info " << col.second.info
        //     << endl;

        // Be specific about what type we're looking for.  This will allow
        // us to be more leniant when encoding for input.
        switch (col.second.info.type()) {
        case ML::BOOLEAN:
            inputColumns.emplace_back(col.second.columnName,
                                      std::make_shared<BooleanValueInfo>(),
                                      sparsity);
            break;

        case ML::REAL:
            inputColumns.emplace_back(col.second.columnName,
                                      std::make_shared<Float32ValueInfo>(),
                                      sparsity);
            break;

        case ML::CATEGORICAL:
        case ML::STRING:
            inputColumns.emplace_back(col.second.columnName,
                                      std::make_shared<StringValueInfo>(),
                                      sparsity);
            break;

        default:
            throw HttpReturnException(400, "unknown value info");
        }
    }

    std::sort(inputColumns.begin(), inputColumns.end(),
              [] (const KnownColumn & c1, const KnownColumn & c2)
              {
                  return c1.columnName < c2.columnName;
              });

    result.input.addRowValue("features", inputColumns, SCHEMA_CLOSED);

    auto cat = itl->labelInfo.categorical();

    if (cat) {
        int labelCount = itl->classifier.label_count();

        std::vector<KnownColumn> scoreColumns;

        for (unsigned i = 0;  i < labelCount;  ++i) {
            scoreColumns.emplace_back(ColumnName(cat->print(i)),
                                      std::make_shared<Float32ValueInfo>(),
                                      COLUMN_IS_DENSE);
        }

#if 0 // disabled because we want them in the same order produced by the output       
        std::sort(scoreColumns.begin(), scoreColumns.end(),
              [] (const KnownColumn & c1, const KnownColumn & c2)
              {
                  return c1.columnName < c2.columnName;
              });
#endif

        result.output.addRowValue("scores", scoreColumns, SCHEMA_CLOSED);
    }
    else {
        result.output.addNumericValue("score");
    }
    return result;
}


/*****************************************************************************/
/* EXPLAIN FUNCTION                                                             */
/*****************************************************************************/

ExplainFunction::
ExplainFunction(MldbServer * owner,
             PolyConfig config,
             const std::function<bool (const Json::Value &)> & onProgress)
    : ClassifyFunction(owner, config, onProgress)
{
}

ExplainFunction::
~ExplainFunction()
{
}

FunctionOutput
ExplainFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
    FunctionOutput result;

    std::vector<float> dense;
    std::shared_ptr<ML::Mutable_Feature_Set> fset;
    Date ts;

    std::tie(dense, fset, ts) = getFeatureSet(context, false /* attempt to optimize */);

    ML::Explanation expl
        = itl->classifier.impl
        ->explain(*fset, itl->featureSpace->encodeLabel(context.get<CellValue>("label")));

    result.set("bias", ExpressionValue(expl.bias, ts));

    RowValue output;

    Date effectiveDate = ts;

    for(auto iter=expl.feature_weights.begin(); iter!=expl.feature_weights.end(); iter++) {
        output.emplace_back(ColumnName(itl->featureSpace->print(iter->first)),
                            iter->second,
                            effectiveDate);
    }

    result.set("explanation", output);

    return result;
}

FunctionInfo
ExplainFunction::
getFunctionInfo() const
{
    FunctionInfo result;

    result.input.addAtomValue("label");
    result.input.addRowValue("features");
    result.output.addRowValue("explanation");
    result.output.addNumericValue("bias");

    return result;
}

/** Documentation macro for JML classifiers. */
void jmlclassifierMacro(MacroContext & context,
                        const std::string & macroName,
                        const Utf8String & args)
{
    string classifierType = args.rawString();

    try {
        std::shared_ptr<ML::Classifier_Generator> generator
            = ML::Registry<ML::Classifier_Generator>::singleton().create(classifierType);
        

        context.writeHtml("<table><tr><th>Parameter</th><th>Range</th>"
                          "<th>Default</th><th>Description</th></tr>");
        for (auto & o: generator->options())
            context.writeHtml(ML::format(
                                         "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>",
                                         o.name.c_str(), o.range.c_str(), o.value.c_str(), o.doc.c_str()
                                         ));

        context.writeHtml("</table>");

    } catch (const std::exception & exc) {
        context.writeHtml("unregistered JML type '" + classifierType + "' :" + exc.what());
    }

    return;
}

namespace {

auto regJmlClassifier = RegisterMacro("jmlclassifier", jmlclassifierMacro);

static RegisterProcedureType<ClassifierProcedure, ClassifierConfig>
regClassifier(builtinPackage(),
              "classifier.train",
              "Train a supervised classifier",
              "procedures/Classifier.md.html");

static RegisterFunctionType<ClassifyFunction, ClassifyFunctionConfig>
regClassifyFunction(builtinPackage(),
                    "classifier",
                    "Apply a trained classifier to new data",
                    "functions/ClassifierApply.md.html");

static RegisterFunctionType<ExplainFunction, ClassifyFunctionConfig>
regExplainFunction(builtinPackage(),
                   "classifier.explain",
                   "Explain the output of a classifier",
                   "functions/ClassifierExplain.md.html");

} // file scope

} // namespace MLDB
} // namespace Datacratic
