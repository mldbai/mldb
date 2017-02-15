/** accuracy.cc
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Implementation of an ACCURACY algorithm for embedding of a dataset.
*/

#include "accuracy.h"
#include "matrix.h"
#include "mldb/server/mldb_server.h"
#include "mldb/core/dataset.h"
#include "mldb/server/bound_queries.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/jml/stats/distribution.h"
#include <boost/multi_array.hpp>
#include "mldb/jml/utils/guard.h"
#include "mldb/base/parallel.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/arch/timers.h"
#include "mldb/arch/simd_vector.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/ml/separation_stats.h"
#include "mldb/server/analytics.h"
#include "mldb/types/any_impl.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/types/optional_description.h"
#include "mldb/http/http_exception.h"
#include "mldb/plugins/sql_config_validator.h"
#include "mldb/plugins/sql_expression_extractors.h"
#include "mldb/server/parallel_merge_sort.h"
#include "mldb/utils/log.h"

#define NO_DATA_ERR_MSG "Cannot run classifier.test procedure on empty test set"

using namespace std;



namespace MLDB {

typedef std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > Rows;

DEFINE_STRUCTURE_DESCRIPTION(AccuracyConfig);


AccuracyConfigDescription::
AccuracyConfigDescription()
{
    Optional<PolyConfigT<Dataset> > optionalOutputDataset;
    optionalOutputDataset.emplace(PolyConfigT<Dataset>().
                                  withType(AccuracyConfig::defaultOutputDatasetType));

    addField("mode", &AccuracyConfig::mode,
             "Model mode: `boolean`, `regression` or `categorical`. "
             "Controls how the label is interpreted and what is the output of the classifier. "
             "This must match what was used during training.", CM_BOOLEAN);
    addField("testingData", &AccuracyConfig::testingData,
             "SQL query which specifies the scores, labels and optional weights for evaluation. "
             "The query is usually of the form: "
             "`select classifier_function({features: {f1, f2}})[score] as score, x as label from ds`.\n\n"
             "The select expression must contain these two columns: \n\n"
             "  * `score`: one scalar expression which evaluates to the score a classifier "
             "has assigned to the given row, and \n"
             "  * `label`: one scalar expression to identify the row's label, and whose type "
             "must match that of the classifier mode. Rows with null labels will be ignored. \n"
             "     * `boolean` mode: a boolean (0 or 1)\n"
             "     * `regression` mode: a real number\n"
             "     * `categorical` mode: any combination of numbers and strings for\n\n"
             "The select expression can contain an optional `weight` column. The weight "
             "allows the relative importance of examples to be set. It must "
             "be a real number. If the `weight` is not specified each row will have "
             "a weight of 1. Rows with a null weight will cause a training error. \n\n"
             "The query must not contain `GROUP BY` or `HAVING` clauses. ");
    addField("outputDataset", &AccuracyConfig::outputDataset,
             "Output dataset for scored examples. The score for the test "
             "example will be written to this dataset. Examples get grouped when "
              "they have the same score when `mode` is `boolean`. Specifying a "
             "dataset is optional.", optionalOutputDataset);
    addField("uniqueScoresOnly", &AccuracyConfig::uniqueScoresOnly,
              "If `outputDataset` is set and `mode` is set to `boolean`, setting this parameter "
              "to `true` will output a single row per unique score. This is useful if the "
              "test set is very large and aggregate statistics for each unique score is "
              "sufficient, for instance to generate a ROC curve. This has no effect "
              "for other values of `mode`.", false);
    addParent<ProcedureConfig>();

    onPostValidate = validateQuery(&AccuracyConfig::testingData,
                                   NoGroupByHaving(),
                                   PlainColumnSelect(),
                                   ScoreLabelSelect(),
                                   MustContainFrom());

}


/*****************************************************************************/
/* ACCURACY PROCEDURE                                                         */
/*****************************************************************************/

AccuracyProcedure::
AccuracyProcedure(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->accuracyConfig = config.params.convert<AccuracyConfig>();
    if (!accuracyConfig.testingData.stm)
        throw HttpReturnException(400, "Classifier testing procedure requires 'testingData' to be set",
                                  "config", this->accuracyConfig);
}

Any
AccuracyProcedure::
getStatus() const
{
    return Any();
}

RunOutput
runBoolean(AccuracyConfig & runAccuracyConf,
           BoundSelectQuery & selectQuery,
           std::shared_ptr<Dataset> output)
{

    PerThreadAccumulator<ScoredStats> accum;
    auto logger = MLDB::getMldbLog<AccuracyProcedure>();

    auto processor = [&] (NamedRowValue & row,
                          const std::vector<ExpressionValue> & scoreLabelWeight)
        {
            double score = scoreLabelWeight[0].toDouble();
            bool label = scoreLabelWeight[1].asBool();
            double weight = scoreLabelWeight[2].toDouble();

            TRACE_MSG(logger) << "score=" << score << "; label=" << label << "; weight=" << weight;

            accum.get().update(label, score, weight, row.rowName);

            return true;
        };

    selectQuery.execute({processor,true/*processInParallel*/}, runAccuracyConf.testingData.stm->offset,
             runAccuracyConf.testingData.stm->limit,
             nullptr /* progress */);

    // Now merge out stats together
    ScoredStats stats;
    bool gotStuff = false;

    accum.forEach([&] (ScoredStats * thrStats)
                  {
                      gotStuff = true;
                      thrStats->sort();
                      stats.add(*thrStats);
                  });

    if (!gotStuff) {
        throw MLDB::Exception(NO_DATA_ERR_MSG);
    }

    //stats.sort();
    stats.calculate();
    if(output) {
        const Date recordDate = Date::now();

        int prevIncludedPop = 0;

        std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > rows;

        auto recordRow = [&] (unsigned i, const BinaryStats & bstats, ScoredStats::ScoredEntry & entry)
        {
            std::vector<std::tuple<RowPath, CellValue, Date> > row;

            row.emplace_back(ColumnPath("index"), i, recordDate);
            row.emplace_back(ColumnPath("label"), entry.label, recordDate);
            row.emplace_back(ColumnPath("score"), entry.score, recordDate);
            row.emplace_back(ColumnPath("weight"), entry.weight, recordDate);
            row.emplace_back(ColumnPath("truePositives"), bstats.truePositives(), recordDate);
            row.emplace_back(ColumnPath("falsePositives"), bstats.falsePositives(), recordDate);
            row.emplace_back(ColumnPath("trueNegatives"), bstats.trueNegatives(), recordDate);
            row.emplace_back(ColumnPath("falseNegatives"), bstats.falseNegatives(), recordDate);
            row.emplace_back(ColumnPath("accuracy"), bstats.accuracy(), recordDate);
            row.emplace_back(ColumnPath("precision"), bstats.precision(), recordDate);
            row.emplace_back(ColumnPath("recall"), bstats.recall(), recordDate);
            row.emplace_back(ColumnPath("truePositiveRate"), bstats.truePositiveRate(), recordDate);
            row.emplace_back(ColumnPath("falsePositiveRate"), bstats.falsePositiveRate(), recordDate);

            rows.emplace_back(boost::any_cast<RowPath>(entry.key), std::move(row));
            if (rows.size() > 10000) {
                output->recordRows(rows);
                rows.clear();
            }
        };

        for (unsigned i = 1, j = 0;  i < stats.stats.size();  ++i) {
            auto & bstats = stats.stats[i];
            auto & entry = stats.entries[j];

            // the difference between included population of the current versus
            // last stats.stats represents the number of exemples included in the stats.
            // examples get grouped when they have the same score. use the unweighted
            // scores because we care about the actual number of examples, whatever
            // what their training weight was
            unsigned next_j = j + (bstats.includedPopulation(false) - prevIncludedPop);
            prevIncludedPop = bstats.includedPopulation(false);

            if(runAccuracyConf.uniqueScoresOnly) {
                j = next_j;
                ExcAssertEqual(bstats.threshold, entry.score);
                recordRow(i, bstats, entry);
            }
            else {
                for(; j<next_j; j++) {
                    entry = stats.entries[j];
                    ExcAssertEqual(bstats.threshold, entry.score);
                    recordRow(i, bstats, entry);
                }
            }
        }

        output->recordRows(rows);

        output->commit();
    }

    DEBUG_MSG(logger) << "stats are ";

    DEBUG_MSG(logger) << stats.toJson();

    DEBUG_MSG(logger) << stats.atPercentile(0.50).toJson();
    DEBUG_MSG(logger) << stats.atPercentile(0.20).toJson();
    DEBUG_MSG(logger) << stats.atPercentile(0.10).toJson();
    DEBUG_MSG(logger) << stats.atPercentile(0.05).toJson();
    DEBUG_MSG(logger) << stats.atPercentile(0.01).toJson();

    return Any(stats.toJson());
}

RunOutput
runCategorical(AccuracyConfig & runAccuracyConf,
               BoundSelectQuery & selectQuery,
               std::shared_ptr<Dataset> output)
{
    typedef vector<std::tuple<CellValue, CellValue, double, double, RowPath>> AccumBucket;
    PerThreadAccumulator<AccumBucket> accum;

    PerThreadAccumulator<Rows> rowsAccum;
    Date recordDate = Date::now();


    auto processor = [&] (NamedRowValue & row,
                           const std::vector<ExpressionValue> & scoreLabelWeight)
        {
            CellValue maxLabel;
            double maxLabelScore = -INFINITY;

            std::vector<std::tuple<RowPath, CellValue, Date> > outputRow;

            static const ColumnPath score("score");
            
            auto onAtom = [&] (const Path & columnName,
                               const Path & prefix,
                               const CellValue & val,
                               Date ts)
                {
                    auto v = val.toDouble();
                    if(v > maxLabelScore) {
                        maxLabelScore = v;
                        maxLabel = jsonDecodeStr<CellValue>(columnName.toSimpleName());
                    }

                    if(output) {
                        outputRow.emplace_back(score + columnName, v, recordDate);
                    }

                    return true;
                };
            scoreLabelWeight[0].forEachAtom(onAtom);

            auto label = scoreLabelWeight[1].getAtom();
            double weight = scoreLabelWeight[2].toDouble();

            accum.get().emplace_back(label, maxLabel, maxLabelScore, weight, row.rowName);

            if(output) {
                outputRow.emplace_back(ColumnPath("maxLabel"), maxLabel, recordDate);
                outputRow.emplace_back(ColumnPath("label"), label, recordDate);
                outputRow.emplace_back(ColumnPath("weight"), weight, recordDate);

                rowsAccum.get().emplace_back(row.rowName, std::move(outputRow));
                if(rowsAccum.get().size() > 1000) {
                    output->recordRows(rowsAccum.get());
                    rowsAccum.get().clear();
                }
            }

            return true;
        };

    selectQuery.execute({processor,true/*processInParallel*/},
            runAccuracyConf.testingData.stm->offset,
            runAccuracyConf.testingData.stm->limit,
            nullptr /* progress */);


    if(output) {
        rowsAccum.forEach([&] (Rows * thrRow)
            {
                output->recordRows(*thrRow);
            });
        output->commit();
    }


    // Create confusion matrix (actual / predicted)
    map<CellValue, map<CellValue, double>> confusion_matrix;
    map<CellValue, double> predicted_sums;
    map<CellValue, double> real_sums;
    double conf_mat_sum = 0;
    bool gotStuff = false;
    accum.forEach([&] (AccumBucket * thrBucket)
            {
                gotStuff = true;
                for(auto & elem : *thrBucket) {
                    const CellValue & label = std::get<0>(elem);
                    const CellValue & predicted = std::get<1>(elem);
                    const double & weight = std::get<3>(elem);
                    confusion_matrix[label][predicted] += weight;
                    real_sums[label] += weight;
                    predicted_sums[predicted] += weight;
                    conf_mat_sum += weight;
                }
            });

    if (!gotStuff) {
        throw MLDB::Exception(NO_DATA_ERR_MSG);
    }
    // Create per-class statistics
    Json::Value results;
    results["labelStatistics"] = Json::Value();

    double total_accuracy = 0;
    double total_precision = 0;
    double total_recall = 0; // i'll be back!
    double total_f1 = 0;
    double total_support = 0;

    int nb_predicted_no_label = 0;

    auto logger = MLDB::getMldbLog<AccuracyProcedure>();
    results["confusionMatrix"] = Json::Value(Json::arrayValue);
    for(const auto & actual_it : confusion_matrix) {

        for(const auto & predicted_it : actual_it.second) {
            Json::Value conf_mat_elem;
            conf_mat_elem["predicted"] = jsonEncode(predicted_it.first);
            conf_mat_elem["actual"] = jsonEncode(actual_it.first);
            conf_mat_elem["count"] = predicted_it.second;
            results["confusionMatrix"].append(conf_mat_elem);
        }

        double tp = confusion_matrix[actual_it.first][actual_it.first];
        double fp = predicted_sums[actual_it.first] - tp;
        double fn = real_sums[actual_it.first] - tp;
        double tn = conf_mat_sum - fn - fp - tp;

        DEBUG_MSG(logger) << "label: " << actual_it.first;
        DEBUG_MSG(logger) << "TP: " << tp;
        DEBUG_MSG(logger) << "FP: " << fp;
        DEBUG_MSG(logger) << "TN: " << tn;
        DEBUG_MSG(logger) << "FN: " << fn;

        // if our class was (wrongfully) predicted (fp) but was never actually
        // there (tp + fn == 0), then this is strange
        if (tp + fn == 0 && fp > 0) {
            nb_predicted_no_label++;
            DEBUG_MSG(logger)
                << "WARNING!! Label '" << actual_it.first
                << "' was predicted but not in known labels!";
        }

        Json::Value class_stats;

        double accuracy = ML::xdiv(tp + tn, conf_mat_sum);
        double precision = ML::xdiv(tp, tp + fp);
        double support = tp + fn;
        double recall = ML::xdiv(tp, support);
        class_stats["accuracy"] = accuracy;
        class_stats["precision"] = precision;
        class_stats["recall"] = recall;
        class_stats["f1Score"] = 2 * ML::xdiv(precision * recall,
                                        precision + recall);
        class_stats["support"] = support;
        results["labelStatistics"][actual_it.first.toUtf8String()] = class_stats;

        total_accuracy += accuracy * support;
        total_precision += precision * support;
        total_recall += recall * support;
        total_f1 += class_stats["f1Score"].asDouble() * support;
        total_support += support;
    }

    // Create weighted statistics
    Json::Value weighted_stats;
    weighted_stats["accuracy"] = total_accuracy / total_support;
    weighted_stats["precision"] = total_precision / total_support;
    weighted_stats["recall"] = total_recall / total_support;
    weighted_stats["f1Score"] = total_f1 / total_support;
    weighted_stats["support"] = total_support;
    results["weightedStatistics"] = weighted_stats;


    // TODO maybe this should always return an error? The problem is it is not impossible that because
    // of the way the dataset is split, it is a normal situation. But it can also point to
    // misalignment in the way columns are named
    
    // throw if precision is zero and at least one predicted value wasn't even
    // in the labels
    if (weighted_stats["precision"].asDouble() == 0
        && nb_predicted_no_label > 0) {
        throw MLDB::Exception(MLDB::format("Weighted precision is 0 and %i"
                "labels were predicted but not in true labels! "
                "Are the columns of the predicted labels named properly?",
                nb_predicted_no_label));
    }
    
    // cout << results.toStyledString() << endl;

    return Any(results);
}

RunOutput
runRegression(AccuracyConfig & runAccuracyConf,
               BoundSelectQuery & selectQuery,
               std::shared_ptr<Dataset> output)
{
    /* Calculate the r-squared. */
    struct ThreadStats {
        ThreadStats() :
            mse_sum(0), totalWeight(0)
        {}

        void increment(double v, double l, double w) {
            if (!finite(v)) return;

            mse_sum += pow(v-l, 2) * w;
            absolute_percentage.push_back(abs( (v-l)/l ));

            labelsWeights.emplace_back(l,w);
            totalWeight += w;
        }

        static void merge(ThreadStats & t1, ThreadStats & t2)
        {
            size_t split = t1.absolute_percentage.size();

            t1.absolute_percentage.insert(t1.absolute_percentage.end(),
                          std::make_move_iterator(t2.absolute_percentage.begin()),
                          std::make_move_iterator(t2.absolute_percentage.end()));
            t2.absolute_percentage.clear();

            std::inplace_merge(t1.absolute_percentage.begin(),
                               t1.absolute_percentage.begin() + split,
                               t1.absolute_percentage.end());
        }

        double mse_sum;
        double totalWeight;
        distribution<double> absolute_percentage;
        vector<pair<double, double>> labelsWeights;
    };

    PerThreadAccumulator<ThreadStats> accum;

    PerThreadAccumulator<Rows> rowsAccum;
    Date recordDate = Date::now();

    auto processor = [&] (NamedRowValue & row,
                          const std::vector<ExpressionValue> & scoreLabelWeight)
        {
            double score = scoreLabelWeight[0].toDouble();
            double label = scoreLabelWeight[1].toDouble();
            double weight = scoreLabelWeight[2].toDouble();

            accum.get().increment(score, label, weight);

            if(output) {
                std::vector<std::tuple<RowPath, CellValue, Date> > outputRow;

                outputRow.emplace_back(ColumnPath("score"), score, recordDate);
                outputRow.emplace_back(ColumnPath("label"), label, recordDate);
                outputRow.emplace_back(ColumnPath("weight"), weight, recordDate);

                rowsAccum.get().emplace_back(row.rowName, outputRow);
                if(rowsAccum.get().size() > 1000) {
                    output->recordRows(rowsAccum.get());
                    rowsAccum.get().clear();
                }
            }

            return true;
        };

    selectQuery.execute({processor,true/*processInParallel*/},
             runAccuracyConf.testingData.stm->offset,
             runAccuracyConf.testingData.stm->limit,
             nullptr /* progress */);


    if(output) {
        rowsAccum.forEach([&] (Rows * thrRow)
            {
                output->recordRows(*thrRow);
            });
        output->commit();
    }


    double totalWeight = 0, mse_sum = 0;
    vector<vector<pair<double,double>> > allThreadLabelsWeights;
    accum.forEach([&] (ThreadStats * thrStats)
                  {
                        totalWeight += thrStats->totalWeight;
                        mse_sum += thrStats->mse_sum;
                        allThreadLabelsWeights.emplace_back(std::move(thrStats->labelsWeights));
                  });

    if(totalWeight == 0) {
        throw MLDB::Exception(NO_DATA_ERR_MSG);
    }

    std::mutex mergeAccumsLock;

    double meanOfLabel = 0;
    auto doThreadMeanLbl = [&] (int threadNum) -> bool
    {
        double averageAccum = 0;
        for(auto & labelWeight : allThreadLabelsWeights[threadNum])
            averageAccum += labelWeight.first * labelWeight.second / totalWeight;

        std::unique_lock<std::mutex> guard(mergeAccumsLock);
        meanOfLabel += averageAccum;
        return true;
    };
    parallelMap(0, allThreadLabelsWeights.size(), doThreadMeanLbl);

    double totalSumSquares = 0;
    auto doThreadSS = [&] (int threadNum) -> bool
    {
        double ssAccum = 0;
        for(auto & labelWeight : allThreadLabelsWeights[threadNum])
            ssAccum += pow(labelWeight.first - meanOfLabel, 2) * labelWeight.second;

        std::unique_lock<std::mutex> guard(mergeAccumsLock);
        totalSumSquares += ssAccum;
        return true;
    };
    parallelMap(0, allThreadLabelsWeights.size(), doThreadSS);


    // calculate the r2 while catching the edge cases
    double r_squared;
    if      (mse_sum == 0)          r_squared = 1;
    else if (totalSumSquares == 0)  r_squared = 0;
    else                            r_squared = 1 - (mse_sum / totalSumSquares);

    // prepare absolute_percentage distribution
    distribution<double> absolute_percentage;

    parallelMergeSortRecursive(accum.threads, 0, accum.threads.size(),
                               [] (const std::shared_ptr<ThreadStats> & t)
                               {
                                   std::sort(t->absolute_percentage.begin(),
                                             t->absolute_percentage.end());
                               },
                               [] (const std::shared_ptr<ThreadStats> & t1,
                                   const std::shared_ptr<ThreadStats> & t2)
                               {
                                   ThreadStats::merge(*t1, *t2);
                               },
                               [] (const std::shared_ptr<ThreadStats> & t)
                               {
                                   return t->absolute_percentage.size();
                               },
                               10000 /* thread threshold */);
    if (!accum.threads.empty()) {
        absolute_percentage = std::move(accum.threads[0]->absolute_percentage);
    }

    // create return object
    Json::Value results;
    results["r2"] = r_squared;
//     results["b"] = b;
//     results["bd"] = bd;
    results["mse"] = mse_sum / totalWeight;

    Json::Value quantile_errors;
    if(absolute_percentage.size() > 0) {
        size_t size = absolute_percentage.size() - 1;
        quantile_errors["0.25"] = absolute_percentage[(int)(size*0.25)];
        quantile_errors["0.5"] = absolute_percentage[(int)(size*0.5)];
        quantile_errors["0.75"] = absolute_percentage[(int)(size*0.75)];
        quantile_errors["0.9"] = absolute_percentage[(int)(size*0.9)];
    }
    results["quantileErrors"] = quantile_errors;

    return Any(results);
}

RunOutput
AccuracyProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runAccuracyConf = applyRunConfOverProcConf(accuracyConfig, run);

    // 1.  Get the input dataset
    SqlExpressionMldbScope context(server);

    ConvertProgressToJson convertProgressToJson(onProgress);
    auto dataset = runAccuracyConf.testingData.stm->from->bind(context, convertProgressToJson).dataset;

    // prepare output dataset
    std::shared_ptr<Dataset> output;
    if(runAccuracyConf.outputDataset) {
        PolyConfigT<Dataset> outputDataset = *runAccuracyConf.outputDataset;
        if (outputDataset.type.empty())
            outputDataset.type = AccuracyConfig::defaultOutputDatasetType;

        output = createDataset(server, outputDataset, nullptr, true /*overwrite*/);
    }

    // 5.  Run it
    auto score = extractNamedSubSelect("score", runAccuracyConf.testingData.stm->select)->expression;
    auto label = extractNamedSubSelect("label", runAccuracyConf.testingData.stm->select)->expression;
    auto weightSubSelect = extractNamedSubSelect("weight", runAccuracyConf.testingData.stm->select);
    shared_ptr<SqlExpression> weight = weightSubSelect ? weightSubSelect->expression : SqlExpression::ONE;

    std::vector<std::shared_ptr<SqlExpression> > calc = {
        score,
        label,
        weight
    };

    auto boundQuery =
        BoundSelectQuery({} /* select */, *dataset, "" /* table alias */,
                     runAccuracyConf.testingData.stm->when,
                     *runAccuracyConf.testingData.stm->where,
                     runAccuracyConf.testingData.stm->orderBy,
                     calc);

    if(runAccuracyConf.mode == CM_BOOLEAN)
        return runBoolean(runAccuracyConf, boundQuery, output);
    if(runAccuracyConf.mode == CM_CATEGORICAL)
        return runCategorical(runAccuracyConf, boundQuery, output);
    if(runAccuracyConf.mode == CM_REGRESSION)
        return runRegression(runAccuracyConf, boundQuery, output);

    throw MLDB::Exception("Classification mode '%d' not implemented", runAccuracyConf.mode);
}

namespace {

RegisterProcedureType<AccuracyProcedure, AccuracyConfig>
regAccuracy(builtinPackage(),
            "Calculate the accuracy of a classifier on held-out data",
            "procedures/Accuracy.md.html");

} // file scope

} // namespace MLDB

