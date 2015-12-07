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
#include "server/dataset.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/jml/utils/profile.h"

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
    std::vector<std::string> defaultAgg = { "avg" };
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

    set<string> validAggs = {"avg", "min", "max", "sum"};
    if(functionConfig.aggregators.size() == 0) {
        functionConfig.aggregators.push_back("avg");
    }

    // Build SELECT statement and parse it
    string select_expr;
    for(auto agg : functionConfig.aggregators) {
        if(validAggs.find(agg) == validAggs.end())
            throw ML::Exception("Unknown aggregator: " + agg);

        if(select_expr.size() > 0)
            select_expr += ",";

        select_expr += ML::format("%s({*}) as %s", agg, agg);
    }
    select = SelectExpression::parseList(select_expr);


    SqlExpressionMldbContext context(owner);
    boundEmbeddingDataset = functionConfig.embeddingDataset->bind(context);
    
    num_embed_cols = boundEmbeddingDataset.dataset->getRowInfo()->columns.size() * 
                            functionConfig.aggregators.size();
}

Any
PoolingFunction::
getStatus() const
{
    return Any();
}


FunctionOutput
PoolingFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
 //   STACK_PROFILE(PoolingFunction_apply)

    /**
     * For each word in the bag of words
     * **/
    Utf8String whereAccum(" rowName() IN (");
    Date tss;
    std::atomic<int> num_added(0);
    auto onAtom = [&] (const Id & columnName,
                       const Id & prefix,
                       const CellValue & val,
                       Date ts)
        {
            tss = ts;
            
            // escape '
            auto col = columnName.toUtf8String();
            //ML::replace_all(col, "'", "''");

            if(col.find("'") != col.end())
                return true;

            whereAccum += " '" + col + "',";
            // TODO. consider weighting by frequency
            // float weighting = val.floatValue();
            
            num_added ++ ;
            return true;
        };

    ExpressionValue args = context.get<ExpressionValue>("words");
    args.forEachAtom(onAtom);


    vector<double> rtn;
    std::vector<MatrixNamedRow> rows;

    if(num_added > 0) { 
        // remove last ,
        // all this monkey-patching is because we don't have a substr for utf8 strings
        auto it = whereAccum.rfind("'");
        whereAccum.erase(it, whereAccum.end());
        whereAccum += "')";

        OrderByExpression orderby(ORDER_BY_NOTHING);
        std::shared_ptr<SqlExpression> having;
        TupleExpression groupBy;
        auto rowNameExpr = SqlExpression::parse(string("rowName()"));
        auto where = SqlExpression::parse(whereAccum);

        rows = boundEmbeddingDataset.dataset->queryStructured(select,
                    WhenExpression::TRUE,
                    where,
                    orderby,
                    groupBy,
                    having,
                    rowNameExpr,
                    0,
                    -1,
                    "",
                    false);

    }


    if(rows.size() == 0) {
        //cerr << "no result" << endl;
        for(int i=0; i<num_embed_cols; i++)
            rtn.emplace_back(0);
    }
    else
    {
        ExcAssert(rows.size() == 1);
        auto columns = rows[0].columns;
        ExcAssert(columns.size() == num_embed_cols);
        for(auto& val : columns)
           rtn.emplace_back(std::get<1>(val).toDouble());
    }

    FunctionOutput foResult;
    foResult.set("embedding", ExpressionValue(rtn, tss));
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
