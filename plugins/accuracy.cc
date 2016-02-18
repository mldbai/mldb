/** accuracy.cc
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

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
#include "mldb/jml/utils/worker_task.h"
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

using namespace std;


namespace Datacratic {
namespace MLDB {

typedef std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > Rows;

DEFINE_STRUCTURE_DESCRIPTION(AccuracyConfig);

AccuracyConfigDescription::
AccuracyConfigDescription()
{
    Optional<PolyConfigT<Dataset> > optionalOutputDataset;
    optionalOutputDataset.emplace(PolyConfigT<Dataset>().
                                  withType(AccuracyConfig::defaultOutputDatasetType));

    addField("testingData", &AccuracyConfig::testingData,
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
    addField("mode", &AccuracyConfig::mode,
             "Mode of evaluated classifier.  Controls how the label is interpreted and "
             "what is the expected output of the classifier is. This must match "
             "what was used during training.");
    addField("outputDataset", &AccuracyConfig::outputDataset,
             "Output dataset for scored examples. The score for the test "
             "example will be written to this dataset. Examples get grouped when "
              "they have the same score when mode=boolean. Specifying a "
             "dataset is optional.", optionalOutputDataset);
    addParent<ProcedureConfig>();
            
    onPostValidate = validate<AccuracyConfig, 
                              InputQuery,
                              NoGroupByHaving,
                              PlainColumnSelect,
                              ScoreLabelSelect,
                              MustContainFrom>(&AccuracyConfig::testingData, "accuracy");

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
run_boolean(AccuracyConfig & runAccuracyConf,
            BoundSelectQuery & selectQuery,
            std::shared_ptr<Dataset> output)
{

    PerThreadAccumulator<ScoredStats> accum;

    auto aggregator = [&] (NamedRowValue & row,
                           const std::vector<ExpressionValue> & scoreLabelWeight)
        {
            //cerr << "got vals " << labelWeight << " " << score << endl;

            double score = scoreLabelWeight[0].toDouble();
            bool label = scoreLabelWeight[1].asBool();
            double weight = scoreLabelWeight[2].toDouble();
            
            accum.get().update(label, score, weight, row.rowName);
            
            return true;
        };

    selectQuery.execute(aggregator, runAccuracyConf.testingData.stm->offset,
             runAccuracyConf.testingData.stm->limit,
             nullptr /* progress */);
    
    // Now merge out stats together
    ScoredStats stats;

    accum.forEach([&] (ScoredStats * thrStats)
                  {
                      thrStats->sort();
                      stats.add(*thrStats);
                  });
    

    //stats.sort();
    stats.calculate();
    if(output) {
        Date recordDate = Date::now();

        int prevIncludedPop = 0;

        std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > rows;

        for (unsigned i = 1, j = 0;  i < stats.stats.size();  ++i) {
            auto & bstats = stats.stats[i];
            auto & entry = stats.entries[j];

            // the difference between included population of the current versus
            // last stats.stats represents the number of exemples included in the stats.
            // examples get grouped when they have the same score
            j += (bstats.includedPopulation() - prevIncludedPop);
            prevIncludedPop = bstats.includedPopulation();

            ExcAssertEqual(bstats.threshold, entry.score);

            std::vector<std::tuple<RowName, CellValue, Date> > row;

            row.emplace_back(ColumnName("index"), i, recordDate);
            row.emplace_back(ColumnName("label"), entry.label, recordDate);
            row.emplace_back(ColumnName("score"), entry.score, recordDate);
            row.emplace_back(ColumnName("weight"), entry.weight, recordDate);
            row.emplace_back(ColumnName("truePositives"), bstats.truePositives(), recordDate);
            row.emplace_back(ColumnName("falsePositives"), bstats.falsePositives(), recordDate);
            row.emplace_back(ColumnName("trueNegatives"), bstats.trueNegatives(), recordDate);
            row.emplace_back(ColumnName("falseNegatives"), bstats.falseNegatives(), recordDate);
            row.emplace_back(ColumnName("precision"), bstats.precision(), recordDate);
            row.emplace_back(ColumnName("recall"), bstats.recall(), recordDate);
            row.emplace_back(ColumnName("truePositiveRate"), bstats.truePositiveRate(), recordDate);
            row.emplace_back(ColumnName("falsePositiveRate"), bstats.falsePositiveRate(), recordDate);

            rows.emplace_back(boost::any_cast<RowName>(entry.key), std::move(row));
            if (rows.size() > 1000) {
                output->recordRows(rows);
                rows.clear();
            }
        }

        output->recordRows(rows);

        output->commit();
    }

    cerr << "stats are " << endl;

    cerr << stats.toJson();

    cerr << stats.atPercentile(0.50).toJson();
    cerr << stats.atPercentile(0.20).toJson();
    cerr << stats.atPercentile(0.10).toJson();
    cerr << stats.atPercentile(0.05).toJson();
    cerr << stats.atPercentile(0.01).toJson();

    return Any(stats.toJson());

}

RunOutput
run_categorical(AccuracyConfig & runAccuracyConf,
                BoundSelectQuery & selectQuery,
                std::shared_ptr<Dataset> output)
{
    typedef vector<std::tuple<CellValue, CellValue, double, double, RowName>> AccumBucket;
    PerThreadAccumulator<AccumBucket> accum;

    PerThreadAccumulator<Rows> rowsAccum;
    Date recordDate = Date::now();


    auto aggregator = [&] (NamedRowValue & row,
                           const std::vector<ExpressionValue> & scoreLabelWeight)
        {
            CellValue maxLabel;
            double maxLabelScore = -std::numeric_limits<double>::infinity();

            std::vector<std::tuple<RowName, CellValue, Date> > outputRow;

            auto onAtom = [&] (const Coord & columnName,
                               const Coord & prefix,
                               const CellValue & val,
                               Date ts) 
                {
                    auto v = val.toDouble();
                    if(v > maxLabelScore) {
                        maxLabelScore = v;
                        maxLabel = jsonDecodeStr<CellValue>(columnName.toUtf8String());
                    }

                    if(output) {
                        outputRow.emplace_back(ColumnName("score." + columnName.toString()), v, recordDate);
                    }

                    return true;
                };
            scoreLabelWeight[0].forEachAtom(onAtom);

            auto label = scoreLabelWeight[1].getAtom();
            double weight = scoreLabelWeight[2].toDouble();

            accum.get().emplace_back(label, maxLabel, maxLabelScore, weight, row.rowName);

            if(output) {
                outputRow.emplace_back(ColumnName("maxLabel"), maxLabel, recordDate);
                outputRow.emplace_back(ColumnName("label"), label, recordDate);
                outputRow.emplace_back(ColumnName("weight"), weight, recordDate);

                rowsAccum.get().emplace_back(row.rowName, outputRow);
                if(rowsAccum.get().size() > 1000) {
                    output->recordRows(rowsAccum.get());
                    rowsAccum.get().clear();
                }
            }

            return true;
        };

    selectQuery.execute(aggregator,
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


    // Create confusion matrix
    map<CellValue, map<CellValue, unsigned>> confusion_matrix;
    map<CellValue, unsigned> predicted_sums;
    map<CellValue, unsigned> real_sums;
    accum.forEach([&] (AccumBucket * thrBucket) 
            {
                for(auto & elem : *thrBucket) {
                    auto label_it = confusion_matrix.find(get<0>(elem));
                    // label is a new true label
                    if(label_it == confusion_matrix.end()) {
                        confusion_matrix.emplace(get<0>(elem), map<CellValue, uint>{{get<1>(elem), 1}});
                    }
                    // we already know about this true label
                    else {
                        label_it->second[get<1>(elem)] += 1;
                    }

                    real_sums[get<0>(elem)] += 1;
                    predicted_sums[get<1>(elem)] += 1;
                }
            });


    // Create per-class statistics
    Json::Value results;
    results["label_statistics"] = Json::Value();

    double total_precision = 0;
    double total_recall = 0; // i'll be back!
    double total_f1 = 0;
    unsigned total_support = 0;

    results["confusion_matrix"] = Json::Value(Json::arrayValue);
    for(auto it = confusion_matrix.begin(); it != confusion_matrix.end(); it++) {
        unsigned fn = 0;
        unsigned tp = 0;

        for(auto predicted_it = it->second.begin(); 
                predicted_it != it->second.end(); predicted_it++) {

            if(predicted_it->first == it->first)  {
                tp += predicted_it->second;
            } else{
                fn += predicted_it->second;
            }
            
            Json::Value conf_mat_elem;
            conf_mat_elem["predicted"] = jsonEncode(predicted_it->first);
            conf_mat_elem["actual"] = jsonEncode(it->first);
            conf_mat_elem["count"] = predicted_it->second;
            results["confusion_matrix"].append(conf_mat_elem);
        }

        Json::Value class_stats;

        double precision = ML::xdiv(tp, float(predicted_sums[it->first]));
        double recall = ML::xdiv(tp, float(tp + fn));
        unsigned support = real_sums[it->first];
        class_stats["precision"] = precision;
        class_stats["recall"] = recall;
        class_stats["f1_score"] = 2 * ML::xdiv((precision * recall), (precision + recall));
        class_stats["support"] = support;
        results["label_statistics"][it->first.toString()] = class_stats;

        total_precision += precision * support;
        total_recall += recall * support;
        total_f1 += class_stats["f1_score"].asDouble() * support;
        total_support += support;
    }

    // Create weighted statistics
    Json::Value weighted_stats;
    weighted_stats["precision"] = total_precision / total_support;
    weighted_stats["recall"] = total_recall / total_support;
    weighted_stats["f1_score"] = total_f1 / total_support;
    weighted_stats["support"] = total_support;
    results["weighted_statistics"] = weighted_stats;


    // TODO maybe this should always return an error? The problem is it is not impossible that because
    // of the way the dataset is split, it is a normal situation. But it can also point to
    // misalignment in the way columns are named

    // for all predicted labels
    for(auto predicted_it = predicted_sums.begin(); predicted_it != predicted_sums.end(); predicted_it++) {
        // if it is not a true label
        if(real_sums.find(predicted_it->first) == real_sums.end()) {
            if(weighted_stats["precision"].asDouble() == 0) {
                throw ML::Exception(ML::format("Weighted precision is 0 and label '%s' " 
                        "was predicted but not in true labels! Are the columns of the predicted "
                        "labels named properly?", predicted_it->first.toString()));
            }
            cerr << "WARNING!! Label '" << predicted_it->first << "' was predicted but not in known labels!" << endl;
        }
    }


    cout << results.toStyledString() << endl;

    return Any(results);
}

RunOutput
run_regression(AccuracyConfig & runAccuracyConf,
               BoundSelectQuery & selectQuery,
               std::shared_ptr<Dataset> output)
{

    /* Calculate the r-squared. */
    struct ThreadStats {
        ThreadStats() :
            sum_vsq(0), sum_v(0), sum_lsq(0), sum_l(0),
            sum_vl(0), mse_sum(0), n(0)
        {}

        void increment(double v, double l) {
            sum_vsq += v*v;  sum_v += v;
            sum_lsq += l*l;  sum_l += l;
            sum_vl  += v*l;
            mse_sum += pow(v-l, 2);
            absolute_percentage.push_back(abs( (v-l)/l ));
            n++;
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

        double sum_vsq, sum_v, sum_lsq, sum_l, sum_vl, mse_sum;
        int n;
        ML::distribution<double> absolute_percentage;
    };

    PerThreadAccumulator<ThreadStats> accum;

    PerThreadAccumulator<Rows> rowsAccum;
    Date recordDate = Date::now();

    auto aggregator = [&] (NamedRowValue & row,
                           const std::vector<ExpressionValue> & scoreLabelWeight)
        {
            double score = scoreLabelWeight[0].toDouble();
            double label = scoreLabelWeight[1].toDouble();
            double weight = scoreLabelWeight[2].toDouble();

            accum.get().increment(score, label);

            if(output) {
                std::vector<std::tuple<RowName, CellValue, Date> > outputRow;

                outputRow.emplace_back(ColumnName("score"), score, recordDate);
                outputRow.emplace_back(ColumnName("label"), label, recordDate);
                outputRow.emplace_back(ColumnName("weight"), weight, recordDate);

                rowsAccum.get().emplace_back(row.rowName, outputRow);
                if(rowsAccum.get().size() > 1000) {
                    output->recordRows(rowsAccum.get());
                    rowsAccum.get().clear();
                }
            }

            return true;
        };

    selectQuery.execute(aggregator, runAccuracyConf.testingData.stm->offset,
             runAccuracyConf.testingData.stm->limit,
             nullptr /* progress */);


    if(output) {
        rowsAccum.forEach([&] (Rows * thrRow)
            {
                output->recordRows(*thrRow);
            });
        output->commit();
    }



    double sum_vsq = 0.0, sum_v = 0.0, sum_lsq = 0.0, sum_l = 0.0;
    double sum_vl = 0.0, n = 0, mse_sum = 0;

    accum.forEach([&] (ThreadStats * thrStats)
                  {
                        sum_vsq += thrStats->sum_vsq;
                        sum_v += thrStats-> sum_v;
                        sum_lsq += thrStats->sum_lsq;
                        sum_l += thrStats->sum_l;
                        sum_vl += thrStats->sum_vl;
                        n += thrStats->n;
                        mse_sum += thrStats->mse_sum;
                  });


    double svl = n * sum_vl - sum_v * sum_l;
    double svv = n * sum_vsq - sum_v * sum_v;
    double sll = n * sum_lsq - sum_l * sum_l;

    double r_squared = svl*svl / (svv * sll);
//     double b = svl / svv;
//     double bd = svl / sll;


    // prepare absolute_percentage distribution 
    ML::distribution<double> absolute_percentage;

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

    ExcAssertEqual(absolute_percentage.size(), n);


    // create return object
    Json::Value results;
    results["r2_score"] = r_squared;
//     results["b"] = b;
//     results["bd"] = bd;
    results["mse"] = mse_sum / n;

    Json::Value quantile_errors;
    if(absolute_percentage.size() > 0) {
        size_t size = absolute_percentage.size() - 1;
        quantile_errors["0.25"] = absolute_percentage[(int)(size*0.25)];
        quantile_errors["0.5"] = absolute_percentage[(int)(size*0.5)];
        quantile_errors["0.75"] = absolute_percentage[(int)(size*0.75)];
        quantile_errors["0.9"] = absolute_percentage[(int)(size*0.9)];
    }
    results["quantile_errors"] = quantile_errors;

    return Any(results);
}

RunOutput
AccuracyProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runAccuracyConf = applyRunConfOverProcConf(accuracyConfig, run);

    // 1.  Get the input dataset
    SqlExpressionMldbContext context(server);

    auto dataset = runAccuracyConf.testingData.stm->from->bind(context).dataset;
   
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
                     calc,
                     false /* implicit order by row hash */);

    if(runAccuracyConf.mode == CM_BOOLEAN)
        return run_boolean(runAccuracyConf, boundQuery, output);
    if(runAccuracyConf.mode == CM_CATEGORICAL)
        return run_categorical(runAccuracyConf, boundQuery, output);
    if(runAccuracyConf.mode == CM_REGRESSION)
        return run_regression(runAccuracyConf, boundQuery, output);

    throw ML::Exception("Classification mode '%d' not implemented", runAccuracyConf.mode);
}

namespace {

RegisterProcedureType<AccuracyProcedure, AccuracyConfig>
regAccuracy(builtinPackage(),
            "classifier.test",
            "Calculate the accuracy of a classifier on held-out data",
            "procedures/Accuracy.md.html");

} // file scope

} // namespace MLDB
} // namespace Datacratic
