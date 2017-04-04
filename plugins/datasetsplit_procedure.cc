/** datasetsplit_procedure.cc                                                   -*- C++ -*-
    Mathieu Marquis Bolduc, April 3rd 2017
    This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
*/

#include "datasetsplit_procedure.h"
#include "types/basic_value_descriptions.h"
#include "types/distribution_description.h"
#include "types/map_description.h"
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

/*****************************************************************************/
/* DATASET SPLIT PROCEDURE CONFIG                                            */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(SplitProcedureConfig);

SplitProcedureConfigDescription::
SplitProcedureConfigDescription()
{
    InputQuery labels;
    std::vector<PolyConfigT<Dataset>> outputDatasets;
    std::vector<float> splits;

    addField("labels", &SplitProcedureConfig::labels,
             "Specification");
    addField("outputDatasets", &SplitProcedureConfig::outputDatasets,
             "Configuration for output dataset");
    addField("splits", &SplitProcedureConfig::splits,
             "splits");
    addParent<ProcedureConfig>();
}


/*****************************************************************************/
/* DATASET SPLIT PROCEDURE                                                   */
/*****************************************************************************/

SplitProcedure::
SplitProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->procConfig = config.params.convert<SplitProcedureConfig>();
}

Any
SplitProcedure::
getStatus() const
{
    return Any();
}

RunOutput
SplitProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(procConfig, run);
    SqlExpressionMldbScope context(server);
    ConvertProgressToJson convertProgressToJson(onProgress);

    auto boundDataset = runProcConf.labels.stm->from->bind(context, convertProgressToJson);

    std::vector<RowPath> rowPaths;

    //Get all the row names selected by the specified FROM/WHERE
     auto processor = [&] (NamedRowValue & row_,
                           const std::vector<ExpressionValue> & extraVals)
        {
            rowPaths.push_back(std::move(row_.rowName));
            return true;            
        };
    SelectExpression select("1");
    std::vector<std::shared_ptr<SqlExpression> > extra;
    BoundSelectQuery(select,
                     *boundDataset.dataset,
                     boundDataset.asName, runProcConf.labels.stm->when,
                     *runProcConf.labels.stm->where,
                     runProcConf.labels.stm->orderBy, extra)
        .execute({processor,false/*processInParallel*/},
                 runProcConf.labels.stm->offset,
                 runProcConf.labels.stm->limit,
                 nullptr /* progress */);

    std::minstd_rand rng;
    std::shuffle(rowPaths.begin(), rowPaths.end(), rng);

    size_t numFolds = runProcConf.splits.size();
    std::vector<std::vector<RowPath>> distributions(numFolds); //Rows per Fold
    std::map<PathElement, std::vector<size_t>> sums; //distribution per label

    SqlExpressionDatasetScope datasetScope(boundDataset.dataset, boundDataset.asName);
    BoundSqlExpression boundSelect = runProcConf.labels.stm->select.bind(datasetScope);

    ExpressionValue storage;
    for (const auto& rowPath : rowPaths) {
        MatrixNamedRow row = boundDataset.dataset->getMatrixView()->getRow(rowPath);
        auto rowScope = datasetScope.getRowScope(row);
        auto rowValue = boundSelect(rowScope, storage, GET_ALL);
        size_t bestFold = 0;
        float diff = 0.f;
        bool unknown = false; 

        auto onColumn = [&] (const PathElement & columnName,
                             const ExpressionValue & val) {
            auto it = sums.find(columnName);
            if (it == sums.end()) {
                //first time we see this label, put the row in fold 0
                std::vector<size_t> distribution(numFolds, 0);
                sums[columnName] = distribution;
                unknown = true;
                bestFold = 0;
            }
            else if (!unknown) {
                size_t labelSum = 0;
                const auto& labelDistribution = it->second;
                int i = 0;

                //size of the distribution
                for (const auto v : labelDistribution) {
                    if (v == 0) {
                        bestFold = i;
                        return false;
                    }
                    else {
                        labelSum += v;
                    }
                    ++i;
                }

                //find the biggest error for this label
                i = 0;
                float worstError = 0;
                float worstFold = 0;
                for (const auto v : labelDistribution) {
                    float ratio = v / (float)labelSum;
                    float target = runProcConf.splits[i];
                    float labeldiff = target - ratio;
                    if (labeldiff > worstError) {
                        worstError = labeldiff;
                        worstFold = i;
                    }

                    ++i;
                }

                if (worstError > diff) {
                    diff = worstError;
                    bestFold = worstFold;
                }
            }

            return true;
        };
        //find the best fold
        rowValue.forEachColumn(onColumn);

        //update distributions
        auto updateDistribution = [&] (const PathElement & columnName,
                             const ExpressionValue & val) {
            auto it = sums.find(columnName);
            ExcAssert(it != sums.end());
            it->second[bestFold]++;
            return true;
        };

        rowValue.forEachColumn(updateDistribution);
        distributions[bestFold].push_back(rowPath);
    }

    size_t i = 0;
    for (auto& datasetName : runProcConf.outputDatasets) {

        std::shared_ptr<Dataset> outputDataset;
        if (!datasetName.type.empty() || !datasetName.id.empty()) {
            outputDataset = createDataset(server, datasetName, nullptr, true /** overwrite **/);            
        }

        if(!outputDataset) {
            throw MLDB::Exception("Unable to obtain output dataset");
        }

        for (auto& rowPath : distributions[i]) {
            MatrixNamedRow row = boundDataset.dataset->getMatrixView()->getRow(rowPath);
            outputDataset->recordRow(rowPath, row.columns);
        }

        outputDataset->commit();

        ++i;   
    }

    return RunOutput();
}


namespace {

RegisterProcedureType<SplitProcedure, SplitProcedureConfig>
regMelt(builtinPackage(),
          "Split a dataset in parts with similar label distributions",
          "procedures/SplitProcedure.md.html");

} // file scope

} // namespace MLDB