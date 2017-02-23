/** melt_procedure.cc
    Francois Maillet, 21 janvier 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
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


namespace MLDB {

/**
 *  Ensure the select contains a rows named "to_melt" and "to_fix".
 *  FieldType must contain a SelectStatement named stm.
 */
struct MeltFixSelect
{
    void operator()(const InputQuery & query, const std::string & name) const
    {
        if (!containsNamedSubSelect(query, "to_melt") ||
            !containsNamedSubSelect(query, "to_fix") )
            throw MLDB::Exception("%s procedure expect a rows named 'to_melt' and 'to_fix'", name.c_str());
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
             "called `to_fix` to identify the columns to keep fixed and another row expression "
             "called `to_melt` to identify the columns to melt.");
    addField("outputDataset", &MeltProcedureConfig::outputDataset,
             "Configuration for output dataset",
             PolyConfigT<Dataset>().withType("tabular"));
    addField("keyColumnName", &MeltProcedureConfig::keyColumnName,
            "Column name for the key column", string("key"));
    addField("valueColumnName", &MeltProcedureConfig::valueColumnName,
            "Column name for the value column", string("value"));
    addParent<ProcedureConfig>();

    onPostValidate = validateQuery(&MeltProcedureConfig::inputData,
                                   NoGroupByHaving(),
                                   MeltFixSelect(),
                                   MustContainFrom());

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


    SqlExpressionMldbScope context(server);
    ConvertProgressToJson convertProgressToJson(onProgress);
    auto boundDataset = runProcConf.inputData.stm->from->bind(context, convertProgressToJson);

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
        throw MLDB::Exception("Unable to obtain output dataset");
    }


    ColumnPath keyColumnName(runProcConf.keyColumnName);
    ColumnPath valueColumnName(runProcConf.valueColumnName);

    std::mutex recordMutex;
    auto processor = [&] (NamedRowValue & row_,
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

                auto onAtom = [&] (const Path & columnName,
                                   const Path & prefix,
                                   const CellValue & val,
                                   Date ts)
                    {
                        fixedOutputRows.emplace_back(prefix + columnName,
                                                     val, ts);
                        return true;
                    };

                expr.forEachAtom(onAtom, ColumnPath());
            }

            // Melted
            for(auto & col : row.columns) {
                // TODO. should I call recordRow twice instead of doing a vector copy here?
                RowValue currOutputRow(fixedOutputRows);

                currOutputRow.emplace_back(keyColumnName, get<0>(col).toUtf8String(), rowTs);
                currOutputRow.emplace_back(valueColumnName, get<1>(col), rowTs);

                RowPath rowName = row.rowName + std::get<0>(col);

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
                     runProcConf.inputData.stm->orderBy, extra)
        .execute({processor,true/*processInParallel*/},
                 runProcConf.inputData.stm->offset,
                 runProcConf.inputData.stm->limit,
                 nullptr /* progress */);

    outputDataset->commit();

    return RunOutput();
}


namespace {

RegisterProcedureType<MeltProcedure, MeltProcedureConfig>
regMelt(builtinPackage(),
          "Performs a melt operation on a dataset",
          "procedures/MeltProcedure.md.html");

} // file scope

} // namespace MLDB

