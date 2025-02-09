/** probabilizer.cc
    Jeremy Barnes, 16 December 2014
    This file is part of MLDB. Copyright 2014 mldb.ai inc. All rights reserved.

    Implementation of an algorithm to transform an arbitrary score into a
    calibrated probability.
*/

#include "probabilizer.h"
#include "mldb/plugins/jml/jml/probabilizer.h"
#include "mldb/core/mldb_engine.h"
#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/builtin/sql_config_validator.h"
#include "mldb/builtin/sql_expression_extractors.h"
#include "mldb/utils/distribution.h"
#include "mldb/base/scope.h"
#include "mldb/base/parallel.h"
#include "mldb/plugins/jml/jml/thread_context.h"
#include "mldb/arch/simd_vector.h"
#include "mldb/utils/vector_utils.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/plugins/jml/value_descriptions.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/distribution_description.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/utils/log.h"

#include "mldb/core/analytics.h"


using namespace std;
using namespace MLDB;


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
             MLDB::LOGIT);
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

/*****************************************************************************/
/* PROBABILIZER PROCEDURE                                                    */
/*****************************************************************************/

ProbabilizerProcedure::
ProbabilizerProcedure(MldbEngine * owner,
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

    SqlExpressionMldbScope context(engine);

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
    std::vector<std::tuple<float, float, float> > fvs;

    auto processor = [&] (NamedRowValue & row,
                           const std::vector<ExpressionValue> & extraVals)
        {
            float score = extraVals.at(0).toDouble();
            float label = extraVals.at(1).toDouble();
            float weight = extraVals.at(2).toDouble();

            std::unique_lock<std::mutex> guard(fvsLock);

            fvs.emplace_back(score, label, weight);
            return true;
        };

    iterateDataset(SelectExpression(), *boundDataset.dataset, boundDataset.asName,
                   runProcConf.trainingData.stm->when,
                   *runProcConf.trainingData.stm->where,
                   extra, {processor,true/*processInParallel*/},
                   runProcConf.trainingData.stm->orderBy,
                   runProcConf.trainingData.stm->offset,
                   runProcConf.trainingData.stm->limit);

    ProbabilizerModel probabilizer;
    probabilizer.train(fvs, runProcConf.link);

    if (!runProcConf.modelFileUrl.toString().empty()) {
        filter_ostream stream(runProcConf.modelFileUrl);
        stream << jsonEncode(probabilizer);
        stream.close();

        if(!runProcConf.functionName.empty()) {
            PolyConfig probaFuncPC;
            probaFuncPC.type = "probabilizer";
            probaFuncPC.id = runProcConf.functionName;
            probaFuncPC.params = ProbabilizeFunctionConfig(runProcConf.modelFileUrl);

            obtainFunction(engine, probaFuncPC, onProgress2);
        }
    }

    return RunOutput(probabilizer);
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
    MLDB::GLZ_Probabilizer probabilizer;
};

ProbabilizeFunction::
ProbabilizeFunction(MldbEngine * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner, config)
{
    functionConfig = config.params.convert<ProbabilizeFunctionConfig>();
    itl.reset(new Itl());
    filter_istream stream(functionConfig.modelFileUrl);
    auto repr = jsonDecodeStream<ProbabilizerModel>(stream);
    itl->probabilizer.link = repr.link;
    itl->probabilizer.params.resize(1);
    itl->probabilizer.params[0] = repr.params;
}

ProbabilizeFunction::
ProbabilizeFunction(MldbEngine * owner,
                   const MLDB::GLZ_Probabilizer & in)
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
    float prob  = itl->probabilizer.apply(MLDB::Label_Dist(1, score.toDouble()))[0];

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

