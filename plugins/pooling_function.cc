/** pooling_function.cc
    Francois Maillet, 30 novembre 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "pooling_function.h"
#include "mldb/server/mldb_server.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/server/function_contexts.h"
#include "mldb/types/any_impl.h"
#include "server/dataset_context.h"
#include "mldb/server/analytics.h"
#include "mldb/core/dataset.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/jml/utils/profile.h"
#include "mldb/jml/stats/distribution.h"

using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* STEMMER FUNCTION CONFIG                                                   */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(PoolingFunctionConfig);

PoolingFunctionConfigDescription::
PoolingFunctionConfigDescription()
{
    std::vector<Utf8String> defaultAgg = { "avg" };
    addField("aggregators", &PoolingFunctionConfig::aggregators,
             "Aggregator functions. Valid values are: avg, min, max, sum",
             defaultAgg);
    addField("embeddingDataset", &PoolingFunctionConfig::embeddingDataset,
             "Dataset containing the word embedding");
}

/*****************************************************************************/
/* POOLING FUNCTION                                                          */
/*****************************************************************************/

PoolingFunction::
PoolingFunction(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<PoolingFunctionConfig>();

    SqlQueryFunctionConfig fnConfig;
    fnConfig.query.stm.reset(new SelectStatement());

    set<Utf8String> validAggs = {"avg", "min", "max", "sum"};
    if(functionConfig.aggregators.size() == 0) {
        functionConfig.aggregators.push_back("avg");
    }

    // Build SELECT statement and parse it
    string select_expr;
    for(auto agg : functionConfig.aggregators) {
        if(validAggs.find(agg) == validAggs.end())
            throw ML::Exception("Unknown aggregator: " + agg.rawString());

        if(select_expr.size() > 0)
            select_expr += ",";

        select_expr += ML::format("%s({*}) as %s", agg.rawData(), agg.rawData());
    }
    fnConfig.query.stm->select = SelectExpression::parseList(select_expr);
    fnConfig.query.stm->from = functionConfig.embeddingDataset;
    fnConfig.query.stm->where = SqlExpression::parse("rowName() IN (KEYS OF $words)");
    // Force group by, since that query executor doesn't know how to determine
    // aggregators
    fnConfig.query.stm->groupBy.clauses.emplace_back(SqlExpression::parse("1"));

    PolyConfig fnPConfig;
    fnPConfig.params = fnConfig;

    queryFunction = std::make_shared<SqlQueryFunction>(owner, fnPConfig,
                                                       onProgress);

    SqlExpressionMldbContext context(owner);
    boundEmbeddingDataset = functionConfig.embeddingDataset->bind(context);
    
    num_embed_cols
        = boundEmbeddingDataset.dataset->getRowInfo()->columns.size()
        * functionConfig.aggregators.size();
}

Any
PoolingFunction::
getStatus() const
{
    return Any();
}

struct PoolingFunctionApplier: public FunctionApplier {
    PoolingFunctionApplier(const PoolingFunction * owner,
                           SqlBindingScope & outerContext,
                           const FunctionValues & input)
        : FunctionApplier(owner)
    {
        queryApplier = owner->queryFunction->bind(outerContext, input);
    }

    std::unique_ptr<FunctionApplier> queryApplier;
};

std::unique_ptr<FunctionApplier>
PoolingFunction::
bind(SqlBindingScope & outerContext,
     const FunctionValues & input) const
{
    std::unique_ptr<PoolingFunctionApplier> result(new PoolingFunctionApplier(this, outerContext, input));
    result->info = getFunctionInfo();

    // Check that all values on the passed input are compatible with the required
    // inputs.
    for (auto & p: result->info.input.values) {
        input.checkValueCompatibleAsInputTo(p.first.toUtf8String(), p.second);
    }

    return std::move(result);
}

FunctionOutput
PoolingFunction::
apply(const FunctionApplier & applier_,
      const FunctionContext & context) const
{
 //   STACK_PROFILE(PoolingFunction_apply)

    auto & applier = static_cast<const PoolingFunctionApplier &>(applier_);

    auto queryOutput = queryFunction->apply(*applier.queryApplier, context);

    //cerr << "queryOutput = " << jsonEncode(queryOutput) << endl;

    std::vector<double> outputEmbedding;
    outputEmbedding.reserve(num_embed_cols);

    Date outputTs = context.get<ExpressionValue>("words").getEffectiveTimestamp();

    if (queryOutput.values.empty()) {
        outputEmbedding.resize(num_embed_cols, 0.0);  // TODO: should be NaN?
    }
    else {
        for (auto & agg: functionConfig.aggregators) {
            auto it = queryOutput.values.find(agg);
            if (it == queryOutput.values.end()) {
                throw HttpReturnException(500, "Didn't find output of aggregator",
                                          "queryOutput", queryOutput,
                                          "aggregator", agg);
            }

            const ExpressionValue & val = it->second;

            ML::distribution<double> dist
                = val.getEmbeddingDouble(num_embed_cols / functionConfig.aggregators.size());
            outputEmbedding.insert(outputEmbedding.end(),
                                   dist.begin(), dist.end());

            outputTs.setMax(val.getEffectiveTimestamp());
        }

        ExcAssertEqual(outputEmbedding.size(), num_embed_cols);
    }

    FunctionOutput foResult;
    foResult.set("embedding", ExpressionValue(std::move(outputEmbedding), outputTs));
    return foResult;
}

FunctionInfo
PoolingFunction::
getFunctionInfo() const
{
    FunctionInfo result;
    result.input.addRowValue("words");

    result.output.addEmbeddingValue("embedding", num_embed_cols);

    return result;
}

static RegisterFunctionType<PoolingFunction, PoolingFunctionConfig>
regPoolingFunction(builtinPackage(),
                   "pooling",
                   "Apply a pooling function",
                   "functions/Pooling.md.html");


} // namespace MLDB
} // namespace Datacratic
