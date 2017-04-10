/** classifier.cc
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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
#include "mldb/types/set_description.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/plugins/sql_config_validator.h"
#include "mldb/plugins/sql_expression_extractors.h"
#include "mldb/types/tuple_description.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/ml/jml/training_data.h"
#include "mldb/ml/jml/training_index.h"
#include "mldb/ml/jml/classifier_generator.h"
#include "mldb/ml/jml/registry.h"
#include "mldb/ml/jml/onevsall_generator.h"
#include "mldb/jml/utils/map_reduce.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/server/analytics.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/server/parallel_merge_sort.h"
#include "mldb/ml/jml/feature_info.h"
#include "mldb/types/any_impl.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "mldb/server/static_content_macro.h"
#include "mldb/utils/log.h"


using namespace std;
using namespace ML;


namespace MLDB {

DEFINE_ENUM_DESCRIPTION(ClassifierMode);

ClassifierModeDescription::
ClassifierModeDescription()
{
    addValue("regression",  CM_REGRESSION, "Regression mode (predicting values)");
    addValue("boolean",     CM_BOOLEAN, "Boolean mode (predicting P(true))");
    addValue("categorical", CM_CATEGORICAL, "Categorical mode (predicting P(category)), examples have a single label");
    addValue("multilabel", CM_MULTILABEL, "Categorical mode (predicting P(category)), examples can have multiple labels");
}

DEFINE_ENUM_DESCRIPTION(MultilabelStrategy);

MultilabelStrategyDescription::
MultilabelStrategyDescription()
{
    addValue("random",  MULTILABEL_RANDOM, "Label is selected at random");
    addValue("decompose", MULTILABEL_DECOMPOSE, "Examples are decomposed in single-label examples");
    addValue("one-vs-all", MULTILABEL_ONEVSALL, "A probabilized binary classifier is trained for each label");
}

DEFINE_STRUCTURE_DESCRIPTION(ClassifierConfig);

ClassifierConfigDescription::
ClassifierConfigDescription()
{
    addField("mode", &ClassifierConfig::mode,
             "Model mode: `boolean`, `regression` or `categorical`. "
             "Controls how the label is interpreted and what is the output of the classifier. "
             , CM_BOOLEAN);
    addField("multilabelStrategy", &ClassifierConfig::multilabelStrategy,
             "Multilabel strategy: `random` or `decompose`. "
             "Controls how examples are prepared to handle multilabel classification. "
             , MULTILABEL_ONEVSALL);
    addField("trainingData", &ClassifierConfig::trainingData,
             "SQL query which specifies the features, labels and optional weights for training. "
             "The query should be of the form `select {f1, f2} as features, x as label from ds`.\n\n"
             "The select expression must contain these two columns: \n\n"
             "  * `features`: a row expression to identify the features on which to \n"
             "    train, and \n"
             "  * `label`: one expression to identify the row's label(s), and whose type "
             "must match that of the classifier mode. Rows with null labels will be ignored. \n"
             "     * `boolean` mode: a boolean (0 or 1)\n"
             "     * `regression` mode: a real number\n"
             "     * `categorical` mode: any combination of numbers and strings\n"
             "     * `multilabel` mode: a row, in which each non-null column is a separate label\n\n"
             "The select expression can contain an optional `weight` column. The weight "
             "allows the relative importance of examples to be set. It must "
             "be a real number. If the `weight` is not specified each row will have "
             "a weight of 1. Rows with a null weight will cause a training error. \n\n"
             "The query must not contain `GROUP BY` or `HAVING` clauses and, "
             "unlike most select expressions, this one can only select whole columns, "
             "not expressions involving columns. So `X` will work, but not `X + 1`. "
             "If you need derived values in the query, create a dataset with "
             "the derived columns as a previous step and use a query on that dataset instead.");
    addField("algorithm", &ClassifierConfig::algorithm,
             "Algorithm to use to train classifier with.  This must point to "
             "an entry in the configuration or configurationFile parameters. "
             "See the [classifier configuration documentation](../ClassifierConf.md.html) for details.");
    addField("configuration", &ClassifierConfig::configuration,
             "Configuration object to use for the classifier.  Each one has "
             "its own parameters.  If none is passed, then the configuration "
             "will be loaded from the ConfigurationFile parameter. "
             "See the [classifier configuration documentation](../ClassifierConf.md.html) for details.",
             Json::Value());
    addField("configurationFile", &ClassifierConfig::configurationFile,
             "File to load configuration from.  This is a JSON file containing "
             "only objects, strings and numbers.  If the configuration object is "
             "non-empty, then that will be used preferentially. "
             "See the [classifier configuration documentation](../ClassifierConf.md.html) for details.",
             string("/opt/bin/classifiers.json"));
    addField("equalizationFactor", &ClassifierConfig::equalizationFactor,
             "Amount to adjust weights so that all classes have an equal "
             "total weight.  A value of 0 will not equalize weights "
             "at all.  A value of 1 will ensure that the total weight for "
             "both positive and negative examples is exactly identical. "
             "A number between will choose a balanced tradeoff.  Typically 0.5 (default) "
             "is a good number to use for unbalanced probabilities. "
             "See the [classifier configuration documentation](../ClassifierConf.md.html) for details.",
             0.5);
    addField("modelFileUrl", &ClassifierConfig::modelFileUrl,
             "URL where the model file (with extension '.cls') should be saved. "
             "This file can be loaded by the ![](%%doclink classifier function). "
             "This parameter is optional unless the `functionName` parameter is used.");
    addField("functionName", &ClassifierConfig::functionName,
             "If specified, an instance of the ![](%%doclink classifier function) of this name will be created using "
             "the trained model. Note that to use this parameter, the `modelFileUrl` must "
             "also be provided.");
    addParent<ProcedureConfig>();

    onPostValidate = chain(validateQuery(&ClassifierConfig::trainingData,
                                         NoGroupByHaving(),
                                         PlainColumnSelect(),
                                         MustContainFrom(),
                                         FeaturesLabelSelect()),
                           validateFunction<ClassifierConfig>());
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
        throw HttpReturnException
            (400, "The 'modelFileUrl' parameter '"
             + runProcConf.modelFileUrl.toString()
             + " is not valid.");
    }

    if (!runProcConf.functionName.empty()
        && runProcConf.modelFileUrl.empty()) {
        throw HttpReturnException
            (400, "The 'modelFileUrl' parameter must be set if the "
             "functionName parameter is set so that the function can "
             "load the classifier");
    }

    // try to create output folder and write open a writer to make sure 
    // we have permissions before we do the actual training
    checkWritability(runProcConf.modelFileUrl.toDecodedString(),
                     "modelFileUrl");

    // 1.  Get the input dataset
    SqlExpressionMldbScope context(server);

    ConvertProgressToJson convertProgressToJson(onProgress);
    auto boundDataset = runProcConf.trainingData.stm->from->bind(context, convertProgressToJson);

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
    case CM_MULTILABEL:
        categorical = std::make_shared<ML::Mutable_Categorical_Info>();
        labelInfo = ML::Feature_Info(categorical);
        break;
    default:
        throw HttpReturnException(400, "Unknown classifier mode");
    }

    ML::Configuration classifierConfig;

    if (!runProcConf.configuration.isNull()) {
        classifierConfig =
            jsonDecode<ML::Configuration>(runProcConf.configuration);
    }
    else {
        filter_istream stream(runProcConf.configurationFile.size() > 0 ?
                                  runProcConf.configurationFile :
                                  "/opt/bin/classifiers.json");
        classifierConfig = jsonDecodeStream<ML::Configuration>(stream);
    }

    auto algorithm = runProcConf.algorithm;

    std::shared_ptr<ML::Classifier_Generator> trainer = ML::get_trainer(runProcConf.algorithm,
                          classifierConfig);

    std::shared_ptr<ML::OneVsAll_Classifier_Generator> multilabelGenerator;
    if (runProcConf.mode == CM_MULTILABEL && runProcConf.multilabelStrategy == MULTILABEL_ONEVSALL) {
        multilabelGenerator = make_shared<OneVsAll_Classifier_Generator>(trainer);
        trainer = multilabelGenerator;
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

    std::set<ColumnPath> knownInputColumns;
    {
        // Find only those variables used
        SqlExpressionDatasetScope context(boundDataset);
        
        auto selectBound = select.bind(context);

        for (auto & c : selectBound.info->getKnownColumns()) {
            knownInputColumns.insert(c.columnName);
        }
    }

    DEBUG_MSG(logger) << "knownInputColumns are " << jsonEncode(knownInputColumns);

    Timer timer;

    // TODO: it's not the feature space itself, but indeed the output of
    // the select expression that's important...
    auto featureSpace = std::make_shared<DatasetFeatureSpace>
        (boundDataset.dataset, labelInfo, knownInputColumns);

    INFO_MSG(logger) << "initialized feature space in " << timer.elapsed();

    // We want to calculate the label and weight of each row as well
    // as the select expression
    std::vector<std::shared_ptr<SqlExpression> > extra
        = { label, weight };

    struct Fv {
        Fv()
        {
        }

        Fv(RowPath rowName,
           ML::Mutable_Feature_Set featureSet)
            : rowName(std::move(rowName)),
              featureSet(std::move(featureSet))
        {
        }

        RowPath rowName;
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

        std::map<std::vector<int>, int> multiLabelMap; //index in multiLabelList
        std::vector<std::vector<int>> multiLabelList; //list of combinaison found for this thread, index in categoricalLabelList.

        std::map<int, int> labelMapping;
        std::map<int, int> multilabelMapping;

        void sort()
        {
            if (!multilabelMapping.empty()) {
                for (auto & fv: fvs) {
                    // Modify the explicit label field
                    float label = fv.label();
                    ExcAssert(multilabelMapping.count(label));
                    fv.setLabel(multilabelMapping[label]);
                }
            }
            else if (!labelMapping.empty()) {
                for (auto & fv: fvs) {
                    // Modify the explicit label field
                    float label = fv.label();
                    ExcAssert(labelMapping.count(label));
                    fv.setLabel(labelMapping[label]);
                }
            }

            multilabelMapping.clear();
            labelMapping.clear();

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

    auto accumRow = [&] (float weight, const MatrixNamedRow& row, float encodedLabel) {

        ThreadAccum & thr = accum.get();

        //DEBUG_MSG(logger) << "label = " << label << " weight = " << weight;
        DEBUG_MSG(logger) << "row.columns.size() = " << row.columns.size();

        DEBUG_MSG(logger) << "got row " << jsonEncode(row);
        ++numRows;

        std::vector<std::pair<ML::Feature, float> > features
        = { { labelFeature, encodedLabel }, { weightFeature, weight } };

        unordered_set<Path> unique_known_features;
        for (auto & c: row.columns) {

            //cerr << "column " << std::get<0>(c).toUtf8String() << endl;

            try {
                featureSpace->encodeFeature(std::get<0>(c), std::get<1>(c), features);
            } MLDB_CATCH_ALL {
                rethrowHttpException
                    (KEEP_HTTP_CODE,
                     "Error processing row '" + row.rowName.toUtf8String()
                     + "' column '" + std::get<0>(c).toUtf8String()
                     + "': " + getExceptionString(),
                     "rowName", row.rowName,
                     "columns", row.columns);
            }

            if (unique_known_features.count(std::get<0>(c)) != 0) {
                throw HttpReturnException
                    (400, "Training dataset cannot have duplicated column '" + 
                     std::get<0>(c).toUtf8String()
                     + "' for row '"
                     +row.rowName.toUtf8String()+"'");
            }
            unique_known_features.insert(std::get<0>(c));
        }

        thr.fvs.emplace_back(row.rowName, std::move(features));
    };


    auto processor = [&] (NamedRowValue & row_,
                           const std::vector<ExpressionValue> & extraVals)
        {
            MatrixNamedRow row = row_.flattenDestructive();
            ExpressionValue label = extraVals.at(0);
            if (label.empty())
                return true;

            ThreadAccum & thr = accum.get();

            switch (runProcConf.mode) {
            case CM_REGRESSION:
                accumRow(extraVals.at(1).toDouble(), row, label.getAtom().toDouble());
                break;
            case CM_BOOLEAN:
                accumRow(extraVals.at(1).toDouble(), row, label.getAtom().isTrue());
                break;
            case CM_MULTILABEL: {
                if (!label.isRow())
                    throw HttpReturnException(400, "Multilabel classification labels requires a row");

                if (runProcConf.multilabelStrategy == MULTILABEL_RANDOM) {

                    std::vector<float> labels;

                    std::function<bool (const PathElement & columnName,
                                        const ExpressionValue & val)> randomStrategy = 
                                        [&] (const PathElement & columnName,
                                             const ExpressionValue & val) ->bool
                        {
                            if(!val.empty()) {
                                std::string labelStr = jsonEncodeStr(columnName);
                                auto it = thr.categoricalLabels.find(labelStr);
                                if (it == thr.categoricalLabels.end()) {
                                    size_t labelid = thr.categoricalLabelList.size();
                                    labels.push_back(labelid);
                                    thr.categoricalLabelList.push_back(labelStr);
                                    thr.categoricalLabels.emplace(labelStr, labelid);
                                }
                                else {
                                    labels.push_back(it->second);
                                }
                            }
                            
                            return true;
                        };

                    label.forEachColumn(randomStrategy);

                    if (labels.size() == 0)
                        return true;

                    accumRow(extraVals.at(1).toDouble(), row, labels[std::rand() % labels.size()]);
                }
                else if (runProcConf.multilabelStrategy == MULTILABEL_DECOMPOSE) {

                    std::function<bool (const PathElement & columnName,
                                        const ExpressionValue & val)> decomposeStrategy = 
                                        [&] (const PathElement & columnName,
                                             const ExpressionValue & val) ->bool
                        {
                            if(!val.empty()) {
                                size_t labelid = 0;
                                std::string labelStr = jsonEncodeStr(columnName);
                                auto it = thr.categoricalLabels.find(labelStr);
                                if (it == thr.categoricalLabels.end()) {
                                    labelid = thr.categoricalLabelList.size();
                                    thr.categoricalLabelList.push_back(labelStr);
                                    thr.categoricalLabels.emplace(labelStr, labelid);
                                }
                                else {
                                    labelid = it->second;
                                }

                                accumRow(extraVals.at(1).toDouble(), row, labelid);
                            }
                            
                            return true;
                        };

                    label.forEachColumn(decomposeStrategy);

                    return true;
                }
                else if (runProcConf.multilabelStrategy == MULTILABEL_ONEVSALL) {

                    std::vector<int> labels;

                    std::function<bool (const PathElement & columnName,
                                        const ExpressionValue & val)> onevsallStrategy = 
                                        [&] (const PathElement & columnName,
                                             const ExpressionValue & val) ->bool
                        {
                            if(!val.empty()) {
                                size_t labelid = 0;
                                std::string labelStr = jsonEncodeStr(columnName);
                                auto it = thr.categoricalLabels.find(labelStr);
                                if (it == thr.categoricalLabels.end()) {
                                    labelid = thr.categoricalLabelList.size();
                                    thr.categoricalLabelList.push_back(labelStr);
                                    thr.categoricalLabels.emplace(labelStr, labelid);
                                }
                                else {
                                    labelid = it->second;
                                }

                                labels.push_back(labelid);
                            }

                            return true;
                        };

                    label.forEachColumn(onevsallStrategy);
                    std::sort(labels.begin(), labels.end());
                    int combinaisonId = 0;
                    auto it = thr.multiLabelMap.find(labels);
                    if (it == thr.multiLabelMap.end()) {
                        combinaisonId = thr.multiLabelList.size();
                        thr.multiLabelList.push_back(labels);
                        thr.multiLabelMap.emplace(std::move(labels), combinaisonId);

                    }
                    else {
                        combinaisonId = it->second;
                    }

                    accumRow(extraVals.at(1).toDouble(), row, combinaisonId);

                    return true;
                }
                 else {
                    throw HttpReturnException(400, "Unknown multilabel strategy in classifier training");
                }

                break;
            }
            case CM_CATEGORICAL: {
                // Get a list of categorical labels, for this thread.  Later
                // we map them to the overall list of labels.
                std::string labelStr = jsonEncodeStr(label.getAtom());
                float encodedLabel = 0;
                auto it = thr.categoricalLabels.find(labelStr);
                if (it == thr.categoricalLabels.end()) {
                    encodedLabel = thr.categoricalLabelList.size();
                    thr.categoricalLabelList.push_back(labelStr);
                    thr.categoricalLabels.emplace(labelStr, encodedLabel);
                }
                else {
                    encodedLabel = it->second;
                }

                accumRow(extraVals.at(1).toDouble(), row, encodedLabel);

                break;
            }
            default:
                throw HttpReturnException(400, "Unknown classifier mode");
            }

            return true;
        };

    // If no order by or limit, the order doesn't matter
    if (runProcConf.trainingData.stm->limit == -1 && runProcConf.trainingData.stm->offset == 0)
        runProcConf.trainingData.stm->orderBy.clauses.clear();

    timer.restart();

    BoundSelectQuery(select, *boundDataset.dataset,
                     boundDataset.asName, runProcConf.trainingData.stm->when,
                     *runProcConf.trainingData.stm->where,
                     runProcConf.trainingData.stm->orderBy, extra)
        .execute({processor,true/*processInParallel*/},
                 runProcConf.trainingData.stm->offset,
                 runProcConf.trainingData.stm->limit,
                 nullptr /* progress */);

    INFO_MSG(logger) << "extracted feature vectors in " << timer.elapsed();

    // If we're categorical, we need to sort out the labels over all
    // of the threads.

    std::map<std::string, int> labelMapping;

    std::map<std::vector<int>, int> multiLabelMap;
    std::vector<std::vector<int>> uniqueMultiLabelList;

    if (runProcConf.mode == CM_CATEGORICAL || 
        runProcConf.mode == CM_MULTILABEL) {

        std::set<std::string> allLabels;
        std::vector<std::vector<int>> multiLabelList;

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

        // Now multilabels 

        auto onThread3 = [&] (ThreadAccum * acc)
            {
                size_t i = 0;
                for (auto&tuple : acc->multiLabelList) {
                    for (auto & label : tuple) {
                        //from local category mapping to global category mapping
                        label = acc->labelMapping[label]; 
                    }
                    //The above broke the sorting
                    std::sort(tuple.begin(), tuple.end());
                    int globalCombinaisonId = 0;
                    auto it = multiLabelMap.find(tuple);
                    if (it == multiLabelMap.end()) {
                        globalCombinaisonId = uniqueMultiLabelList.size();
                        uniqueMultiLabelList.push_back(tuple);
                        multiLabelMap.emplace(std::move(tuple), globalCombinaisonId);
                    }
                    else {
                        globalCombinaisonId = it->second;
                    }

                    //mapping from local combinaison array to global combinaison array
                    acc->multilabelMapping[i] = globalCombinaisonId;
                    ++i;
                }
            };

        accum.forEach(onThread3);

        if (multilabelGenerator) {
           multilabelGenerator->setMultilabelMapping(uniqueMultiLabelList, allLabels.size());
        }
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

    INFO_MSG(logger) << "merged feature vectors in " << timer.elapsed();

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


    unsigned num_weight_labels;
    switch (runProcConf.mode) {
    case CM_REGRESSION:
        num_weight_labels = 1;
        break;
    case CM_BOOLEAN:
        num_weight_labels = 2;
        break;
    case CM_CATEGORICAL:
    case CM_MULTILABEL:
        num_weight_labels = labelMapping.size();
        break;
    default:
        throw HttpReturnException(400, "Unknown classifier mode");
    }

    std::vector<distribution<float>> labelWeights(num_weight_labels);
    for(int i=0; i<num_weight_labels; i++) {
        labelWeights[i].resize(nx);
    }

    distribution<float> exampleWeights(nx);

    for (unsigned i = 0;  i < nx;  ++i) {

        float label  = fvs[i].label(); //IF MULTILABEL THIS LABEL IS A GLOBAL COMBINAISON LABEL
        float weight = fvs[i].weight();

        if (weight < 0)
            throw HttpReturnException(400, "classifier example weight cannot be negative");
        if (!isfinite(weight))
            throw HttpReturnException(400, "classifier example weights must be finite");

        if (runProcConf.mode == CM_REGRESSION
            && !isfinite(label)) {
            throw HttpReturnException
                (400,
                 "Regression labels must not be infinite or NaN.  Should you "
                 "add a condition like `WHERE isfinite(label)` to your data, "
                 "or preprocess your labels with `replace_not_finite(label, 0)`?");
        }

        trainingSet.add_example(std::make_shared<ML::Mutable_Feature_Set>(std::move(fvs[i].featureSet)));

        if(runProcConf.mode != CM_REGRESSION) {

            if (uniqueMultiLabelList.size() > 0) {

                auto labelCombinaison = uniqueMultiLabelList[label];

                for(int lbl=0; lbl<num_weight_labels; lbl++) {
                    labelWeights[lbl][i] = std::find(labelCombinaison.begin(), 
                                                     labelCombinaison.end(), lbl) 
                                           != labelCombinaison.end() ?  weight : 0;
                }
            }
            else {
                for(int lbl=0; lbl<num_weight_labels; lbl++) {
                    labelWeights[lbl][i] = weight * (label == lbl);
                }
            }
        }

        exampleWeights[i]  = weight;
    }

    ExcAssertEqual(nx, trainingSet.example_count());

    INFO_MSG(logger) << "added feature vectors in " << timer.elapsed();

    timer.restart();
    trainingSet.preindex(labelFeature);

    INFO_MSG(logger) << "indexed training data in " << timer.elapsed();

    // ...
    //trainingSet.dump("training_set.txt.gz");

    // Find all features
    std::vector<ML::Feature> allFeatures = trainingSet.index().all_features();

    INFO_MSG(logger) << "Training with " << allFeatures.size() << " features";

    std::vector<ML::Feature> trainingFeatures;

    for (unsigned i = 0;  i < allFeatures.size();  ++i) {
        DEBUG_MSG(logger) << "allFeatures[i] = " << allFeatures[i];

        string featureName = featureSpace->print(allFeatures[i]);
        DEBUG_MSG(logger) << "featureName = " << featureName;

        if (allFeatures[i] == labelFeature)
            continue;
        if (allFeatures[i] == weightFeature)
            continue;

#if 0
        if (boost::regex_match(featureName, excludeFeatures)
            || featureName == "LABEL") {
            INFO_MSG(logger) << "excluding feature " << featureName << " from training";
            continue;
        }
#endif
        trainingFeatures.push_back(allFeatures[i]);
    }

    timer.restart();


    trainer->init(featureSpace, labelFeature);

    int randomSeed = 1;

    double equalizationFactor = runProcConf.equalizationFactor;

    ML::Thread_Context threadContext;
    threadContext.seed(randomSeed);

    distribution<float> weights;
    if(runProcConf.mode == CM_REGRESSION) {
        weights = exampleWeights;
    }
    else {
        distribution<float> factor_accum(exampleWeights.size(), 0);
        for(int lbl=0; lbl<num_weight_labels; lbl++) {
            double factor = pow(labelWeights[lbl].total(), -equalizationFactor);

            INFO_MSG(logger) << "factor for class " << lbl << " = " << factor;
            INFO_MSG(logger) << "weight for class " << lbl << " = " << labelWeights[lbl].total();

            factor_accum += factor * labelWeights[lbl];
        }

        weights = exampleWeights * factor_accum;
        weights.normalize();
    }

    DEBUG_MSG(logger) << "training classifier";
    ML::Classifier classifier(trainer->generate(threadContext, trainingSet, weights,
                                                trainingFeatures));
    DEBUG_MSG(logger) << "done training classifier";

    INFO_MSG(logger) << "trained classifier in " << timer.elapsed();

    ExcAssert(classifier.feature_space());

    if (!runProcConf.modelFileUrl.empty()) {
        try {
            classifier.save(runProcConf.modelFileUrl.toDecodedString());
        }
        MLDB_CATCH_ALL {
            rethrowHttpException(400, "Error saving classifier to '"
                                 + runProcConf.modelFileUrl.toString() + "': "
                                 + getExceptionString(),
                                 "url", runProcConf.modelFileUrl);
        }
        INFO_MSG(logger) << "Saved classifier to " << runProcConf.modelFileUrl;
    }


    if(!runProcConf.functionName.empty()) {
        PolyConfig clsFuncPC;
        clsFuncPC.type = "classifier";
        clsFuncPC.id = runProcConf.functionName;
        clsFuncPC.params = ClassifyFunctionConfig(runProcConf.modelFileUrl);

        createFunction(server, clsFuncPC, onProgress, true);
    }

    DEBUG_MSG(logger) << "done saving classifier";

    //trainingSet.dump("training_set.txt.gz");

    return RunOutput();
}


/*****************************************************************************/
/* CLASSIFIER FUNCTION                                                       */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(ClassifyFunctionConfig);

ClassifyFunctionConfigDescription::
ClassifyFunctionConfigDescription()
{
    addField("modelFileUrl", &ClassifyFunctionConfig::modelFileUrl,
             "URL of the model file (with extension '.cls') to load. "
             "This file is created by the ![](%%doclink classifier.train procedure).");
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
    : Function(owner, config)
{
    functionConfig = config.params.convert<ClassifyFunctionConfig>();

    itl.reset(new Itl());

    itl->classifier.load(functionConfig.modelFileUrl.toDecodedString());

    itl->featureSpace = itl->classifier.feature_space<DatasetFeatureSpace>();

    ML::Feature_Info labelInfo = itl->featureSpace->info(labelFeature);

    itl->labelInfo = labelInfo;

    isRegression = itl->classifier.label_count() == 1;
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
getFeatureSet(const ExpressionValue & context, bool attemptDense) const
{
    auto row = context.getColumn(PathElement("features"));

    Date ts = Date::negativeInfinity();

    bool multiValue = false;
    if (attemptDense) {
        std::vector<float> denseFeatures(itl->featureSpace->columnInfo.size(),
                                         std::numeric_limits<float>::quiet_NaN());

        auto onAtom = [&] (const Path & suffix,
                           const Path & prefix,
                           const CellValue & value,
                           Date tsIn)
            {
                ColumnPath columnName(prefix + suffix);
                ColumnHash columnHash(columnName);
                
                auto it = itl->featureSpace->columnInfo.find(columnHash);
                if (it == itl->featureSpace->columnInfo.end())
                    return true;

                ts.setMax(tsIn);

                if (!isnanf(denseFeatures[it->second.index])) {
                    multiValue = true;
                    return false;
                }
                
                denseFeatures[it->second.index]
                    = itl->featureSpace->encodeFeatureValue(columnHash, value);

                return true;
            };

        row.forEachAtom(onAtom);

        if (!multiValue)
            return std::make_tuple( std::move(denseFeatures), nullptr, ts );
    }


    std::vector<std::pair<ML::Feature, float> > features;

    auto onAtom = [&] (const Path & suffix,
                       const Path & prefix,
                       const CellValue & value,
                       Date tsIn)
        {
            ColumnPath columnName(prefix + suffix);
            ColumnHash columnHash(columnName);

            auto it = itl->featureSpace->columnInfo.find(columnHash);
            if (it == itl->featureSpace->columnInfo.end())
                return true;

            ts.setMax(tsIn);

            itl->featureSpace->encodeFeature(columnHash, value, features);

            return true;
        };

    row.forEachAtom(onAtom);

    std::sort(features.begin(), features.end());

    auto fset = std::make_shared<ML::Mutable_Feature_Set>
        (features.begin(), features.end());
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
     const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const
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

ExpressionValue
ClassifyFunction::
apply(const FunctionApplier & applier_,
      const ExpressionValue & context) const
{
    auto & applier = (ClassifyFunctionApplier &)applier_;

    int labelCount = itl->classifier.label_count();

    std::vector<float> dense;
    std::shared_ptr<ML::Mutable_Feature_Set> fset;
    Date ts;

    std::tie(dense, fset, ts)
        = getFeatureSet(context, applier.optInfo /* try to optimize */);

    StructValue result;
    result.reserve(1);

    auto cat = itl->labelInfo.categorical();
    if (!dense.empty() && applier.optInfo) {
        if (cat) {

            ML::Label_Dist scores
                = itl->classifier.impl->predict(dense, applier.optInfo);
            ExcAssertEqual(scores.size(), labelCount);

            vector<tuple<PathElement, ExpressionValue> > row;
            for (unsigned i = 0;  i < labelCount;  ++i) {
                row.emplace_back(PathElement(cat->print(i)),
                                 ExpressionValue(scores[i], ts));
            }

            result.emplace_back("scores", std::move(row));
        }
        else if (itl->labelInfo.type() == ML::REAL) {
            ExcAssertEqual(labelCount, 1);
            float score
                = itl->classifier.impl->predict(0, dense, applier.optInfo);
            result.emplace_back("score", ExpressionValue(score, ts));
        }
        else {
            ExcAssertEqual(labelCount, 2);
            float score
                = itl->classifier.impl->predict(1, dense, applier.optInfo);
            result.emplace_back("score", ExpressionValue(score, ts));
        }
    }
    else {
        if(!fset) {
            throw MLDB::Exception("Feature_Set is null! Are you giving "
                                "only null features to the classifier function?");
        }
        
        if (cat) {
            auto scores = itl->classifier.predict(*fset);
            ExcAssertEqual(scores.size(), labelCount);

            vector<tuple<PathElement, ExpressionValue> > row;

            for (unsigned i = 0;  i < labelCount;  ++i) {
                row.emplace_back(PathElement(cat->print(i)),
                                 ExpressionValue(scores[i], ts));
            }
            result.emplace_back("scores", std::move(row));
        }
        else if (itl->labelInfo.type() == ML::REAL) {
            ExcAssertEqual(labelCount, 1);
            float score = itl->classifier.predict(0, *fset);
            result.emplace_back("score", ExpressionValue(score, ts));
        }
        else {
            ExcAssertEqual(labelCount, 2);
            float score = itl->classifier.predict(1, *fset);
            result.emplace_back("score", ExpressionValue(score, ts));
        }
    }

    return std::move(result);
}

FunctionInfo
ClassifyFunction::
getFunctionInfo() const
{
    FunctionInfo result;

    std::vector<KnownColumn> featureColumns;

    // Input is cell values
    for (auto & col: itl->featureSpace->columnInfo) {

        ColumnSparsity sparsity = col.second.info.optional()
            ? COLUMN_IS_SPARSE : COLUMN_IS_DENSE;

        DEBUG_MSG(logger) << "column " << col.second.columnName << " info " << col.second.info;

        // Be specific about what type we're looking for.  This will allow
        // us to be more leniant when encoding for input.
        switch (col.second.info.type()) {
        case ML::BOOLEAN:
            featureColumns.emplace_back(col.second.columnName,
                                      std::make_shared<BooleanValueInfo>(),
                                      sparsity);
            break;

        case ML::REAL:
            featureColumns.emplace_back(col.second.columnName,
                                      std::make_shared<Float32ValueInfo>(),
                                      sparsity);
            break;

        case ML::CATEGORICAL:
        case ML::STRING:
            featureColumns.emplace_back(col.second.columnName,
                                      std::make_shared<StringValueInfo>(),
                                      sparsity);
            break;

        case ML::INUTILE:
            // inutile, ignore it
            break;

        default:
            throw HttpReturnException(400, "unknown value info");
        }
    }

    std::sort(featureColumns.begin(), featureColumns.end(),
              [] (const KnownColumn & c1, const KnownColumn & c2)
              {
                  return c1.columnName < c2.columnName;
              });


    std::vector<KnownColumn> inputColumns;
    inputColumns.emplace_back(PathElement("features"),
                              std::make_shared<RowValueInfo>(featureColumns,
                                                             SCHEMA_CLOSED),
                              COLUMN_IS_DENSE);
    result.input.emplace_back
        (std::make_shared<RowValueInfo>(std::move(inputColumns),
                                        SCHEMA_CLOSED));
    
    std::vector<KnownColumn> outputColumns;

    auto cat = itl->labelInfo.categorical();

    if (cat) {
        int labelCount = itl->classifier.label_count();

        std::vector<KnownColumn> scoreColumns;

        for (unsigned i = 0;  i < labelCount;  ++i) {
            scoreColumns.emplace_back(ColumnPath::parse(cat->print(i)),
                                      std::make_shared<Float32ValueInfo>(),
                                      COLUMN_IS_DENSE, i);
        }

#if 0 // disabled because we want them in the same order produced by the output
        std::sort(scoreColumns.begin(), scoreColumns.end(),
              [] (const KnownColumn & c1, const KnownColumn & c2)
              {
                  return c1.columnName < c2.columnName;
              });
#endif

        outputColumns.emplace_back(PathElement("scores"),
                                   std::make_shared<RowValueInfo>(scoreColumns,
                                                                  SCHEMA_CLOSED),
                                   COLUMN_IS_DENSE, 0);
    }
    else {
        outputColumns.emplace_back(PathElement("score"),
                                   std::make_shared<NumericValueInfo>(),
                                   COLUMN_IS_DENSE, 0);
    }

    result.output = std::make_shared<RowValueInfo>(std::move(outputColumns),
                                                   SCHEMA_CLOSED);

    return result;
}


/*****************************************************************************/
/* EXPLAIN FUNCTION                                                          */
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

ExpressionValue
ExplainFunction::
apply(const FunctionApplier & applier,
      const ExpressionValue & context) const
{
    std::vector<float> dense;
    std::shared_ptr<ML::Mutable_Feature_Set> fset;
    Date ts;

    std::tie(dense, fset, ts) = getFeatureSet(context, false /* attempt to optimize */);

    if (fset->features.empty()) {
        throw MLDB::Exception("The specified features couldn't be found in the "
                            "classifier. At least one non-null feature column "
                            "must be provided.");
    }

    CellValue label = context.getColumn("label").getAtom();

    ML::Explanation expl
        = itl->classifier.impl
        ->explain(*fset, itl->featureSpace->encodeLabel(label, isRegression));

    StructValue output;
    output.reserve(2);
    output.emplace_back("bias", ExpressionValue(expl.bias, ts));
    
    RowValue features;

    Date effectiveDate = ts;

    for(auto iter=expl.feature_weights.begin(); iter!=expl.feature_weights.end(); iter++) {
        features.emplace_back(ColumnPath::parse(itl->featureSpace->print(iter->first)),
                              iter->second,
                              effectiveDate);
    }

    output.emplace_back("explanation", std::move(features));

    return std::move(output);
}

FunctionInfo
ExplainFunction::
getFunctionInfo() const
{
    FunctionInfo result;

    std::vector<KnownColumn> inputCols, outputCols;

    inputCols.emplace_back(PathElement("label"),
                           std::make_shared<AtomValueInfo>(),
                           COLUMN_IS_DENSE, 0);
    inputCols.emplace_back(PathElement("features"),
                           std::make_shared<UnknownRowValueInfo>(),
                           COLUMN_IS_DENSE, 1);

    outputCols.emplace_back(PathElement("explanation"), std::make_shared<UnknownRowValueInfo>(),
                            COLUMN_IS_DENSE, 0);
    outputCols.emplace_back(PathElement("bias"), std::make_shared<NumericValueInfo>(),
                            COLUMN_IS_DENSE, 1);

    result.input.emplace_back
        (std::make_shared<RowValueInfo>(std::move(inputCols),
                                        SCHEMA_CLOSED));
    result.output = std::make_shared<RowValueInfo>(std::move(outputCols),
                                                   SCHEMA_CLOSED);
    
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
            context.writeHtml(MLDB::format(
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

