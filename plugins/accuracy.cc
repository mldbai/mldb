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

using namespace std;


namespace Datacratic {
namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(AccuracyConfig);

AccuracyConfigDescription::
AccuracyConfigDescription()
{
    Optional<PolyConfigT<Dataset> > optionalOutputDataset;
    optionalOutputDataset.emplace(PolyConfigT<Dataset>().
                                  withType(AccuracyConfig::defaultOutputDatasetType));

    addField("testingData", &AccuracyConfig::testingData,
             "Specification of the data for input to the classifier procedure. "
             "The select expression must contain these expressions: one scalar expression "
             "to identify the label and one scalar expression to identify the score. "
             "The type of the label expression must match "
             "that of the classifier mode from which the model was trained. "
             "Labels with a null value will have their row skipped. "
             "The expression to generate the score represents the output "
             "of whatever is having its accuracy tested.  This needs to be "
             "a number, and normally should be a floating point number that "
             "represents the degree of confidence in the prediction, not "
             "just the class. "  
             "The select expression can also contain an optional weight sub-expression. "
             "This expression generates the relative weight for each example (e.g. " 
             "something like \"2.34*x+y\" if x and y are columns, or "
             "\"x\" or simply \"1.0\").  In some "
             "circumstances it is necessary to calculate accuracy statistics "
             "with uneven weighting, for example to counteract the effect of "
             "non-uniform sampling in dataset.  By default, each class will "
             "get the same weight.  This value is relative to the other "
             "examples, in other words having all examples weighted 1 or all "
             "examples weighted 10 will have the same effect.  That being "
             "said, it is a good idea to keep the weights centered around 1 "
             "to avoid numeric errors in the calculations."
             "The select statement does not support groupby and having clauses.");
    addField("outputDataset", &AccuracyConfig::outputDataset,
             "Output dataset for scored examples. The score for each "
             "example will be written to this dataset. Specifying a "
             "dataset is optional", optionalOutputDataset);
    addParent<ProcedureConfig>();

    onPostValidate = validate<AccuracyConfig, 
                              InputQuery,
                              NoGroupByHaving,
                              PlainColumnSelect,
                              ScoreLabelSelect>(&AccuracyConfig::testingData, "accuracy");

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
AccuracyProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runAccuracyConf = applyRunConfOverProcConf(accuracyConfig, run);

    // 1.  Get the input dataset
    SqlExpressionMldbContext context(server);

    auto dataset = runAccuracyConf.testingData.stm->from->bind(context).dataset;

    // 3.  Create our aggregator function, which records the output of
    //     this row.
    
    PerThreadAccumulator<ScoredStats> accum;

    auto aggregator = [&] (const MatrixNamedRow & row,
                           const std::vector<ExpressionValue> & scoreLabelWeight)
        {
            //cerr << "got vals " << labelWeight << " " << score << endl;

            double score = scoreLabelWeight[0].toDouble();
            bool label = scoreLabelWeight[1].asBool();
            double weight = scoreLabelWeight[2].toDouble();
            
            accum.get().update(label, score, weight, row.rowName);
            
            return true;
        };

    // 5.  Run it
    auto score = extractNamedSubSelect("score", runAccuracyConf.testingData.stm->select)->expression; 
    auto label = extractNamedSubSelect("label", runAccuracyConf.testingData.stm->select)->expression;
    auto weightSubSelect = extractNamedSubSelect("weight", runAccuracyConf.testingData.stm->select);
    shared_ptr<SqlExpression> weight = SqlExpression::ONE;
    if (weightSubSelect)
        weight = weightSubSelect->expression;

    std::vector<std::shared_ptr<SqlExpression> > calc = {
        score,
        label,
        weight
    };

    BoundSelectQuery({} /* select */, *dataset, "" /* table alias */,
                     runAccuracyConf.testingData.stm->when, runAccuracyConf.testingData.stm->where,
                     runAccuracyConf.testingData.stm->orderBy,
                     calc,
                     false /* implicit order by row hash */)
        .execute(aggregator, runAccuracyConf.testingData.stm->offset, runAccuracyConf.testingData.stm->limit,
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

    if(runAccuracyConf.outputDataset) {
        std::shared_ptr<Dataset> output;

        PolyConfigT<Dataset> outputDataset = *runAccuracyConf.outputDataset;
        if (outputDataset.type.empty())
            outputDataset.type = AccuracyConfig::defaultOutputDatasetType;

        output = createDataset(server, outputDataset, nullptr, true /*overwrite*/);

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

namespace {

RegisterProcedureType<AccuracyProcedure, AccuracyConfig>
regAccuracy(builtinPackage(),
            "classifier.test",
            "Calculate the accuracy of a classifier on held-out data",
            "procedures/Accuracy.md.html");

} // file scope

} // namespace MLDB
} // namespace Datacratic
