/** probabilizer.cc
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
    
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

#include "mldb/server/analytics.h"


using namespace std;
using namespace ML;


//#if (__GNUC__ < 4) || (__GNUC__ == 4 && __GNUC_MINOR__ <= 6)
#define override 
//#endif

namespace Datacratic {
namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(ProbabilizerConfig);

ProbabilizerConfigDescription::
ProbabilizerConfigDescription()
{
    addField("trainingData", &ProbabilizerConfig::trainingData,
             "Specification of the data for input to the probabilizer procedure. "
             "The select expression is used to calculate the score for a given dataset row. "
             "Normally this will be the output of a classifier function applied to that row. "
             "The select expression is also used to generate the label.  The label is used to "
             "know the correctness of the probabilizer. "
             "The select expression can also contain an optional weight sub-expression. "
             "A weight of 2.0 is equivalent "
             "to including the identical row twice in the training dataset.  "
             "This can be used to counteract the effect of sampling or weighting "
             "over the dataset that the probabilizer is trained on.  The "
             "default will weight each example the same."
             "The select statement does not support groupby and having clauses. ");
    addField("modelFileUrl", &ProbabilizerConfig::modelFileUrl,
             "URL where the model file (with extension '.prb') should be saved. "
             "This file can be loaded by a function of type 'probabilizer'.");
    addField("link", &ProbabilizerConfig::link,
             "Link function to use.  See documentation.  Generally the "
             "default, PROBIT, is a good place to start for binary "
             "classifification",
             ML::LOGIT);
    addField("functionName", &ProbabilizerConfig::functionName,
             "If specified, a probabilizer function of this name will be created using "
             "the trained probabilizer.");
    addParent<ProcedureConfig>();

    onPostValidate = validate<ProbabilizerConfig, 
                              InputQuery, 
                              MustContainFrom,
                              NoGroupByHaving>(&ProbabilizerConfig::trainingData, "probabilizer");
}

struct ProbabilizerRepr {
    std::string style;
    ML::Link_Function link;
    ML::distribution<double> params;
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

    SqlExpressionMldbContext context(server);

    auto boundDataset = runProcConf.trainingData.stm->from->bind(context);
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
    std::vector<std::tuple<RowName, float, float, float> > fvs;
    
    std::atomic<int> numRows(0);

    auto aggregator = [&] (NamedRowValue & row,
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
                   extra, aggregator,
                   runProcConf.trainingData.stm->orderBy,
                   runProcConf.trainingData.stm->offset,
                   runProcConf.trainingData.stm->limit);
    

    int nx = numRows;

    /* Convert to the correct data structures. */

    boost::multi_array<double, 2> outputs(boost::extents[2][nx]);  // value, bias
    ML::distribution<double> correct(nx, 0.0);
    ML::distribution<double> weights(nx, 1.0);

    size_t numTrue = 0;

    for (unsigned x = 0;  x < nx;  ++x) {
        float score, label, weight;
        std::tie(std::ignore, score, label, weight) = fvs[x];

        //cerr << "x = " << x << " score = " << score << " label = " << label
        //     << " weight = " << weight << endl;

        outputs[0][x] = score;
        outputs[1][x] = 1.0;
        correct[x]    = label;
        weights[x]    = weight;
        numTrue      += label == 1;
    }

#if 0
    filter_ostream out("prob-in.txt");

    for (unsigned i = 0;  i < nx;  ++i) {
        out << ML::format("%.15f %.16f %d\n",
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

    cerr << "trueOneRate = " << trueOneRate
         << " trueZeroRate = " << trueZeroRate
         << " sampleOneRate = " << sampleOneRate
         << " sampleZeroRate = " << sampleZeroRate
         << endl;

    auto link = runProcConf.link;

    ML::Ridge_Regressor regressor;
    ML::distribution<double> probParams
        = ML::run_irls(correct, outputs, weights, link, regressor);
    
    cerr << "probParams = " << probParams << endl;

    // http://gking.harvard.edu/files/0s.pdf, section 4.2
    // Logistic Regression in Rare Events Data (Gary King and Langche Zeng)

    double correction = -log((1 - trueOneRate) / trueOneRate
                             * sampleOneRate / (1 - sampleOneRate));

    //cerr << "paramBefore = " << probParams[1] << endl;
    //cerr << "correction = " << correction
    //<< endl;

    probParams[1] += correction;

    cerr << "paramAfter = " << probParams[1] << endl;

    ProbabilizerRepr repr;
    repr.style = "glz";
    repr.link = link;
    repr.params = probParams;

    if (!runProcConf.modelFileUrl.toString().empty()) {
        filter_ostream stream(runProcConf.modelFileUrl.toString());
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
             "This file is created by a procedure of type 'probabilizer.train'.");
}

struct ProbabilizeFunction::Itl {
    ML::GLZ_Probabilizer probabilizer;
};

ProbabilizeFunction::
ProbabilizeFunction(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<ProbabilizeFunctionConfig>();
    itl.reset(new Itl());
    filter_istream stream(functionConfig.modelFileUrl.toString());
    auto repr = jsonDecodeStream<ProbabilizerRepr>(stream);
    itl->probabilizer.link = repr.link;
    itl->probabilizer.params.resize(1);
    itl->probabilizer.params[0] = repr.params;
}

ProbabilizeFunction::
ProbabilizeFunction(MldbServer * owner,
                 const ML::GLZ_Probabilizer & in)
    : Function(owner)
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

FunctionOutput
ProbabilizeFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
    ExpressionValue score = context.get<ExpressionValue>("score");
    float prob  = itl->probabilizer.apply(ML::Label_Dist(1, score.toDouble()))[0];
    FunctionOutput result;
    result.set("prob", ExpressionValue(prob, score.getEffectiveTimestamp()));
    return result;
}

FunctionInfo
ProbabilizeFunction::
getFunctionInfo() const
{
    FunctionInfo result;

    result.input.addNumericValue("score");
    result.output.addNumericValue("prob");
    
    return result;
}



namespace {

static RegisterProcedureType<ProbabilizerProcedure, ProbabilizerConfig>
regProbabilizer(builtinPackage(),
                "probabilizer.train",
                "Trains a model to calibrate a score into a probability",
                "procedures/Probabilizer.md.html");

static RegisterFunctionType<ProbabilizeFunction, ProbabilizeFunctionConfig>
regProbabilizeFunction(builtinPackage(),
                       "probabilizer",
                       "Apply a probability calibration model",
                       "functions/ApplyProbabilizer.md.html");

} // file scope

} // namespace MLDB
} // namespace Datacratic
