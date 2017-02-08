/** pooling_function.cc
    Francois Maillet, 30 novembre 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "pooling_function.h"
#include "mldb/server/mldb_server.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/any_impl.h"
#include "server/dataset_context.h"
#include "mldb/server/analytics.h"
#include "mldb/core/dataset.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/jml/utils/profile.h"
#include "mldb/jml/stats/distribution.h"

using namespace std;



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

DEFINE_STRUCTURE_DESCRIPTION(PoolingInput);

PoolingInputDescription::PoolingInputDescription()
{
    addField("words", &PoolingInput::words,
             "Row with the words to be pooled.");
}

DEFINE_STRUCTURE_DESCRIPTION(PoolingOutput);

PoolingOutputDescription::PoolingOutputDescription()
{
    addField("embedding", &PoolingOutput::embedding,
             "Embedding corresponding to the input words.");
}

PoolingFunction::
PoolingFunction(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : BaseT(owner, config)
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
            throw MLDB::Exception("Unknown aggregator: " + agg.rawString());

        if(select_expr.size() > 0)
            select_expr += ",";

        select_expr += MLDB::format("%s({*}) as %s", agg.rawData(), agg.rawData());
    }
    fnConfig.query.stm->select = SelectExpression::parseList(select_expr);
    fnConfig.query.stm->from = functionConfig.embeddingDataset;
    fnConfig.query.stm->where = SqlExpression::parse("rowName() IN (KEYS OF $words)");

    PolyConfig fnPConfig;
    fnPConfig.params = fnConfig;

    queryFunction = std::make_shared<SqlQueryFunction>(owner, fnPConfig,
                                                       onProgress);

    SqlExpressionMldbScope context(owner);
    ConvertProgressToJson convertProgressToJson(onProgress);
    boundEmbeddingDataset = functionConfig.embeddingDataset->bind(context, convertProgressToJson);
    
    columnNames = boundEmbeddingDataset.dataset->getRowInfo()->allColumnNames();
}

struct PoolingFunctionApplier: public FunctionApplierT<PoolingInput, PoolingOutput> {
    PoolingFunctionApplier(const PoolingFunction * owner,
                           SqlBindingScope & outerContext,
                           const std::vector<std::shared_ptr<ExpressionValueInfo> > & input)
        : FunctionApplierT<PoolingInput, PoolingOutput>(owner)
    {
        queryApplier = owner->queryFunction->bind(outerContext, input);
    }

    std::unique_ptr<FunctionApplier> queryApplier;
};

PoolingOutput 
PoolingFunction::
applyT(const ApplierT & applier_, PoolingInput input) const
{
    //   STACK_PROFILE(PoolingFunction_apply)

    auto & applier = static_cast<const PoolingFunctionApplier &>(applier_);

    size_t num_embed_cols = columnNames.size() * functionConfig.aggregators.size();

    Date outputTs = input.words.getEffectiveTimestamp();
    StructValue inputRow;
    inputRow.emplace_back("words", std::move(input.words));

    ExpressionValue queryOutput
        = queryFunction->apply(*applier.queryApplier, std::move(inputRow));

    std::vector<double> outputEmbedding;
    outputEmbedding.reserve(num_embed_cols);

    if (queryOutput.empty()) {
        outputEmbedding.resize(num_embed_cols, 0.0);  // TODO: should be NaN?
    }
    else {
        for (auto & agg: functionConfig.aggregators) {

            ExpressionValue val = queryOutput.getColumn(agg);

            if (val.empty()) {
                throw HttpReturnException(500, "Didn't find output of aggregator",
                                          "queryOutput", queryOutput,
                                          "aggregator", agg);
            }

            distribution<double> dist
                = val.getEmbedding(columnNames.data(), columnNames.size());
            outputEmbedding.insert(outputEmbedding.end(),
                                   dist.begin(), dist.end());

            outputTs.setMax(val.getEffectiveTimestamp());
        }
        
        ExcAssertEqual(outputEmbedding.size(), num_embed_cols);
    }   

    return {ExpressionValue(std::move(outputEmbedding), outputTs)};
}
    
std::unique_ptr<FunctionApplierT<PoolingInput, PoolingOutput> >
PoolingFunction::
bindT(SqlBindingScope & outerContext,
      const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const
{
    std::unique_ptr<PoolingFunctionApplier> result
        (new PoolingFunctionApplier(this, outerContext, input));
    result->info = getFunctionInfo();

    // Check that all values on the passed input are compatible with the required
    // inputs.
    result->info.checkInputCompatibility(input);

    return std::move(result);
}

static RegisterFunctionType<PoolingFunction, PoolingFunctionConfig>
regPoolingFunction(builtinPackage(),
                   "pooling",
                   "Apply a pooling function",
                   "functions/Pooling.md.html");


} // namespace MLDB

