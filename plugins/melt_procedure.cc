/** melt_procedure.cc
    Francois Maillet, 21 janvier 2016
    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
*/

#include "melt_procedure.h"
#include "types/basic_value_descriptions.h"
#include "types/distribution_description.h"
#include "types/map_description.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "server/dataset_context.h"
#include "plugins/matrix.h"
#include "server/analytics.h"
#include "ml/value_descriptions.h"
#include "types/any_impl.h"
#include "jml/utils/string_functions.h"
#include "arch/timers.h"
#include "types/optional_description.h"
#include "mldb/plugins/sql_config_validator.h"
#include "mldb/plugins/sql_expression_extractors.h"
#include "mldb/plugins/sparse_matrix_dataset.h"
#include "mldb/server/bound_queries.h"

using namespace std;

namespace Datacratic {
namespace MLDB {

/**
 *  Ensure the select contains a rows named "to_melt" and "to_fix".
 *  FieldType must contain a SelectStatement named stm.
 */
template<typename FieldType> struct MeltFixSelect
{
    void operator()(const FieldType & query, const char * name)
    {
        if (!containsNamedSubSelect(query, "to_melt") ||
            !containsNamedSubSelect(query, "to_fix") )
            throw ML::Exception("%s procedure expect a rows named 'to_melt' and 'to_fixed'", name);
    }
};



/*****************************************************************************/
/* EXPERIMENT PROCEDURE CONFIG                                               */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(MeltProcedureConfig);

MeltProcedureConfigDescription::
MeltProcedureConfigDescription()
{
    addField("inputData", &MeltProcedureConfig::inputData,
             "Specification of the data for input to the melt procedure. "
             "The select expression must contain these two sub-expressions: one row expression "
             "to identify the columns to keep fixed and another row expression "
             " to identify the columns to melt.");
    addField("outputDataset", &MeltProcedureConfig::outputDataset,
             "Configuration for output dataset",
             PolyConfigT<Dataset>().withType("sparse.mutable"));
    addField("keyColumnName", &MeltProcedureConfig::valueColumnName,
            "Column name for the key column", string("key"));
    addField("valueColumnName", &MeltProcedureConfig::valueColumnName,
            "Column name for the value column", string("value"));
    addParent<ProcedureConfig>();
    
    onPostValidate = validate<MeltProcedureConfig,
                              InputQuery,
                              NoGroupByHaving,
                              MeltFixSelect,
                              MustContainFrom>(&MeltProcedureConfig::inputData, "melt");

}


/*****************************************************************************/
/* MELT PROCEDURE                                                            */
/*****************************************************************************/

MeltProcedure::
MeltProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->procConfig = config.params.convert<MeltProcedureConfig>();
}

Any
MeltProcedure::
getStatus() const
{
    return Any();
}

RunOutput
MeltProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(procConfig, run);

    auto extractWithinExpression = [](std::shared_ptr<SqlExpression> expr) 
        -> std::shared_ptr<SqlRowExpression>
        {
            auto withinExpression = std::dynamic_pointer_cast<const SelectWithinExpression>(expr);
            if (withinExpression)
                return withinExpression->select;

            return nullptr;
        };


    SqlExpressionMldbContext context(server);
    auto boundDataset = runProcConf.inputData.stm->from->bind(context);

    auto toFix = extractNamedSubSelect("to_fix", runProcConf.inputData.stm->select)->expression;
    auto toMelt = extractNamedSubSelect("to_melt", runProcConf.inputData.stm->select)->expression;
     
    if (!toFix || !toMelt)
        throw HttpReturnException(400, "inputData must return a 'to_fix' row and a 'to_melt' row");

    shared_ptr<SqlRowExpression> subSelect = extractWithinExpression(toMelt);
    SelectExpression select({subSelect});

    // Create the output dataset
    std::shared_ptr<Dataset> outputDataset;
    if (!runProcConf.outputDataset.type.empty() || !runProcConf.outputDataset.id.empty()) {
        outputDataset = createDataset(server, runProcConf.outputDataset, nullptr, true /** overwrite **/);
    }

    if(!outputDataset) {
        throw ML::Exception("Unable to obtain output dataset");
    }

    
    ColumnName keyColumnName(runProcConf.keyColumnName);
    ColumnName valueColumnName(runProcConf.valueColumnName);

    std::mutex recordMutex;
    auto aggregator = [&] (NamedRowValue & row_,
                           const std::vector<ExpressionValue> & extraVals)
        {
            MatrixNamedRow row = row_.flattenDestructive();
            Date rowTs = Date::negativeInfinity();
            for(auto & col : row.columns) {
                if(get<2>(col) > rowTs)
                    rowTs = get<2>(col);
            }

            // Fixed columns
            RowValue fixedOutputRows;
            for(auto & expr : extraVals) {
                for(auto & col : expr.getRow()) {
                    fixedOutputRows.emplace_back(get<0>(col), get<1>(col).getAtom(), rowTs);
                }
            }

            // Melted
            for(auto & col : row.columns) {
                // TODO. should I call recordRow twice instead of doing a vector copy here?
                RowValue currOutputRow(fixedOutputRows);

                currOutputRow.emplace_back(keyColumnName, get<0>(col).toString(), rowTs);
                currOutputRow.emplace_back(valueColumnName, get<1>(col), rowTs);

                RowName rowName(ML::format("%s_%s", row.rowName.toString(), get<0>(col).toString()));

                std::unique_lock<std::mutex> guard(recordMutex);
                outputDataset->recordRow(rowName, currOutputRow);
            }
            return true;
        };

    std::vector<std::shared_ptr<SqlExpression> > extra = {extractWithinExpression(toFix)};

    BoundSelectQuery(select,
                     *boundDataset.dataset,
                     boundDataset.asName, runProcConf.inputData.stm->when,
                     *runProcConf.inputData.stm->where,
                     runProcConf.inputData.stm->orderBy, extra,
                     false /* implicit order by row hash */)
        .execute(aggregator, 
                 runProcConf.inputData.stm->offset, 
                 runProcConf.inputData.stm->limit, 
                 nullptr /* progress */);

    outputDataset->commit();

    return RunOutput();
}


namespace {

RegisterProcedureType<MeltProcedure, MeltProcedureConfig>
regMelt(builtinPackage(),
          "melt",
          "Performs a melt operation on a dataset",
          "procedures/MeltProcedure.md.html");

} // file scope

} // namespace MLDB
} // namespace Datacratic
