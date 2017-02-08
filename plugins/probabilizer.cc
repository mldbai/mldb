/** probabilizer.cc
    Jeremy Barnes, 16 December 2014
    This file is part of MLDB. Copyright 2014 mldb.ai inc. All rights reserved.

    Implementation of an algorithm to transform an arbitrary score into a
    calibrated probability.
*/

#include "probabilizer.h"
#include "mldb/ml/jml/probabilizer.h"
#include "mldb/server/mldb_server.h"
#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/plugins/sql_config_validator.h"
#include "mldb/plugins/sql_expression_extractors.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/jml/utils/guard.h"
#include "mldb/base/parallel.h"
#include "mldb/ml/jml/thread_context.h"
#include "mldb/arch/simd_vector.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/distribution_description.h"
#include "mldb/http/http_exception.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/utils/log.h"

#include "mldb/server/analytics.h"


using namespace std;
using namespace ML;


//#if (__GNUC__ < 4) || (__GNUC__ == 4 && __GNUC_MINOR__ <= 6)
#define override
//#endif


namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(ProbabilizerConfig);

ProbabilizerConfigDescription::
ProbabilizerConfigDescription()
{
    addField("trainingData", &ProbabilizerConfig::trainingData,
             "SQL query that specifies the scores, labels and optional weights for "
             "the probabilizer training procedure. "
             "The query should be of the form `select x as score, y as label from ds`.\n\n"
             "The select expression must contain these two columns: \n\n"
             "  * `score`: output of a classifier function applied to that row\n"
             "  * `label`: one boolean (0 or 1), so training a probabilizer only works "
             "for a classifier trained with mode `boolean`. Rows with null labels will be ignored.\n"
             " * `weight`: relative importance of examples. It must be a real number. "
             "A weight of 2.0 is equivalent "
             "to including the identical row twice in the training dataset.  "
             "If the `weight` is not specified each row will have "
             "a weight of 1. Rows with a null weight will cause a training error. "
             "This can be used to counteract the effect of sampling or weighting "
             "over the dataset that the probabilizer is trained on. The "
             "default will weight each example the same.\n\n"
             "The query must not contain `GROUP BY` or `HAVING` clauses and, "
             "unlike most select expressions, this one can only select whole columns, "
             "not expressions involving columns. So `X` will work, but not `X + 1`. "
             "If you need derived values in the query, create a dataset with "
             "the derived columns as a previous step and use a query on that dataset instead.");
    addField("link", &ProbabilizerConfig::link,
             "Link function to use.",
             ML::LOGIT);
    addField("modelFileUrl", &ProbabilizerConfig::modelFileUrl,
             "URL where the model file (with extension '.prb') should be saved. "
             "This file can be loaded by the ![](%%doclink probabilizer function). "
             "This parameter is optional unless the `functionName` parameter is used.");
    addField("functionName", &ProbabilizerConfig::functionName,
             "If specified, an instance of the ![](%%doclink probabilizer function) of "
             "this name will be created using "
             "the trained model. Note that to use this parameter, the `modelFileUrl` must "
             "also be provided.");
    addParent<ProcedureConfig>();

    onPostValidate = chain(validateQuery(&ProbabilizerConfig::trainingData,
                                         MustContainFrom(),
                                         ScoreLabelSelect(),
                                         NoGroupByHaving()),
                           validateFunction<ProbabilizerConfig>());
}

struct ProbabilizerRepr {
    std::string style;
    ML::Link_Function link;
    distribution<double> params;
};

DEFINE_STRUCTURE_DESCRIPTION(ProbabilizerRepr);

ProbabilizerReprDescription::
ProbabilizerReprDescription()
{
    addField("style", &ProbabilizerRepr::style,
             "Style of probabilizer to load");
    addField("link", &ProbabilizerRepr::link,
             "Link function for GLZ probabilizer");
    addField("params", &ProbabilizerRepr::params,
             "Parameterization of probabilizer");
}


/*****************************************************************************/
/* PROBABILIZER PROCEDURE                                                     */
/*****************************************************************************/

ProbabilizerProcedure::
ProbabilizerProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->probabilizerConfig = config.params.convert<ProbabilizerConfig>();
}

Any
ProbabilizerProcedure::
getStatus() const
{
    return Any();
}

RunOutput
ProbabilizerProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{
    // 1.  Construct an applyFunctionToProcedure object

    // 2.  Extend with our training function

    // 3.  Apply everything to construct the dataset

    // 4.  Apply the dataset

    auto runProcConf = applyRunConfOverProcConf(probabilizerConfig, run);


    auto onProgress2 = [&] (const Json::Value & progress)
        {
            Json::Value value;
            value["dataset"] = progress;
            return onProgress(value);
        };

    SqlExpressionMldbScope context(server);

    ConvertProgressToJson convertProgressToJson(onProgress);
    auto boundDataset = runProcConf.trainingData.stm->from->bind(context, convertProgressToJson);
    auto score = extractNamedSubSelect("score", runProcConf.trainingData.stm->select)->expression;
    auto label = extractNamedSubSelect("label", runProcConf.trainingData.stm->select)->expression;
    auto weightSubSelect = extractNamedSubSelect("weight", runProcConf.trainingData.stm->select);
    shared_ptr<SqlExpression> weight = weightSubSelect ? weightSubSelect->expression : SqlExpression::ONE;

    // Here is what we need to calculate for each row in the dataset
    std::vector<std::shared_ptr<SqlExpression> > extra
        = { score, label, weight };

    // Build it

    // ...

    std::mutex fvsLock;
    std::vector<std::tuple<RowPath, float, float, float> > fvs;

    std::atomic<int> numRows(0);

    auto processor = [&] (NamedRowValue & row,
                           const std::vector<ExpressionValue> & extraVals)
        {
            float score = extraVals.at(0).toDouble();
            float label = extraVals.at(1).toDouble();
            float weight = extraVals.at(2).toDouble();
            ++numRows;

            std::unique_lock<std::mutex> guard(fvsLock);

            fvs.emplace_back(row.rowName, score, label, weight);
            return true;
        };

    iterateDataset(SelectExpression(), *boundDataset.dataset, boundDataset.asName,
                   runProcConf.trainingData.stm->when,
                   *runProcConf.trainingData.stm->where,
                   extra, {processor,true/*processInParallel*/},
                   runProcConf.trainingData.stm->orderBy,
                   runProcConf.trainingData.stm->offset,
                   runProcConf.trainingData.stm->limit);


    int nx = numRows;

    /* Convert to the correct data structures. */

    boost::multi_array<double, 2> outputs(boost::extents[2][nx]);  // value, bias
    distribution<double> correct(nx, 0.0);
    distribution<double> weights(nx, 1.0);

    size_t numTrue = 0;

    for (unsigned x = 0;  x < nx;  ++x) {
        float score, label, weight;
        std::tie(std::ignore, score, label, weight) = fvs[x];

        DEBUG_MSG(logger) 
            << "x = " << x << " score = " << score << " label = " << label
            << " weight = " << weight;

        outputs[0][x] = score;
        outputs[1][x] = 1.0;
        correct[x]    = label;
        weights[x]    = weight;
        numTrue      += label == 1;
    }

#if 0
    filter_ostream out("prob-in.txt");

    for (unsigned i = 0;  i < nx;  ++i) {
        out << MLDB::format("%.15f %.16f %d\n",
                          outputs[0][i],
                          outputs[1][i],
                          correct[i]);

    }
#endif

    double trueOneRate = correct.dotprod(weights) / weights.total();
    double trueZeroRate = 1.0 - trueOneRate;

    double numExamples = nx;
    double sampleOneRate = numTrue / numExamples;
    double sampleZeroRate = 1.0 - sampleOneRate;

    INFO_MSG(logger) << "trueOneRate = " << trueOneRate
                     << " trueZeroRate = " << trueZeroRate
                     << " sampleOneRate = " << sampleOneRate
                     << " sampleZeroRate = " << sampleZeroRate;

    auto link = runProcConf.link;

    ML::Ridge_Regressor regressor;
    distribution<double> probParams
        = ML::run_irls(correct, outputs, weights, link, regressor);

    INFO_MSG(logger) << "probParams = " << probParams;

    // http://gking.harvard.edu/files/0s.pdf, section 4.2
    // Logistic Regression in Rare Events Data (Gary King and Langche Zeng)

    double correction = -log((1 - trueOneRate) / trueOneRate
                             * sampleOneRate / (1 - sampleOneRate));

    DEBUG_MSG(logger) << "paramBefore = " << probParams[1];
    DEBUG_MSG(logger) << "correction = " << correction;

    probParams[1] += correction;

    INFO_MSG(logger) << "paramAfter = " << probParams[1];

    ProbabilizerRepr repr;
    repr.style = "glz";
    repr.link = link;
    repr.params = probParams;

    if (!runProcConf.modelFileUrl.toString().empty()) {
        filter_ostream stream(runProcConf.modelFileUrl);
        stream << jsonEncode(repr);
        stream.close();

        if(!runProcConf.functionName.empty()) {
            PolyConfig probaFuncPC;
            probaFuncPC.type = "probabilizer";
            probaFuncPC.id = runProcConf.functionName;
            probaFuncPC.params = ProbabilizeFunctionConfig(runProcConf.modelFileUrl);

            obtainFunction(server, probaFuncPC, onProgress2);
        }
    }

#if 0
    itl->probabilizer.link = link;
    itl->probabilizer.params.resize(1);
    itl->probabilizer.params[0] = probParams;
#endif

    return RunOutput(repr);
}


/*****************************************************************************/
/* PROBABILIZER FUNCTION COLUMN                                                 */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(ProbabilizeFunctionConfig);

ProbabilizeFunctionConfigDescription::
ProbabilizeFunctionConfigDescription()
{
    addField("modelFileUrl", &ProbabilizeFunctionConfig::modelFileUrl,
             "URL of the model file (with extension '.prb') to load. "
             "This file is created by the ![](%%doclink probabilizer.train procedure)."
             );
}

struct ProbabilizeFunction::Itl {
    ML::GLZ_Probabilizer probabilizer;
};

ProbabilizeFunction::
ProbabilizeFunction(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner, config)
{
    functionConfig = config.params.convert<ProbabilizeFunctionConfig>();
    itl.reset(new Itl());
    filter_istream stream(functionConfig.modelFileUrl);
    auto repr = jsonDecodeStream<ProbabilizerRepr>(stream);
    itl->probabilizer.link = repr.link;
    itl->probabilizer.params.resize(1);
    itl->probabilizer.params[0] = repr.params;
}

ProbabilizeFunction::
ProbabilizeFunction(MldbServer * owner,
                   const ML::GLZ_Probabilizer & in)
    : Function(owner, PolyConfig())
{
    itl.reset(new Itl());
    itl->probabilizer = in;
}

ProbabilizeFunction::
~ProbabilizeFunction()
{
}

Any
ProbabilizeFunction::
getStatus() const
{
    return Any();
}

ExpressionValue
ProbabilizeFunction::
apply(const FunctionApplier & applier,
      const ExpressionValue & context) const
{
    ExpressionValue score = context.getColumn(PathElement("score"));
    float prob  = itl->probabilizer.apply(ML::Label_Dist(1, score.toDouble()))[0];

    StructValue result;
    result.emplace_back(PathElement("prob"),
                        ExpressionValue(prob, score.getEffectiveTimestamp()));
    return result;
}

FunctionInfo
ProbabilizeFunction::
getFunctionInfo() const
{
    std::vector<KnownColumn> knownInputColumns;
    knownInputColumns.emplace_back(ColumnPath("score"),
                                   std::make_shared<NumericValueInfo>(),
                                   COLUMN_IS_DENSE,
                                   0 /* position */);

    std::vector<KnownColumn> knownOutputColumns;
    knownOutputColumns.emplace_back(ColumnPath("prob"),
                                    std::make_shared<NumericValueInfo>(),
                                    COLUMN_IS_DENSE,
                                    0 /* position */);

    FunctionInfo result;
    result.input.emplace_back(new RowValueInfo(knownInputColumns, SCHEMA_CLOSED));
    result.output.reset(new RowValueInfo(knownOutputColumns, SCHEMA_CLOSED));
    return result;
}



namespace {

static RegisterProcedureType<ProbabilizerProcedure, ProbabilizerConfig>
regProbabilizer(builtinPackage(),
                "Trains a model to calibrate a score into a probability",
                "procedures/Probabilizer.md.html");

static RegisterFunctionType<ProbabilizeFunction, ProbabilizeFunctionConfig>
regProbabilizeFunction(builtinPackage(),
                       "probabilizer",
                       "Apply a probability calibration model",
                       "functions/ApplyProbabilizer.md.html");

} // file scope

} // namespace MLDB

