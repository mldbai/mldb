/** em.cc                                                          -*- C++ -*-
    Mathieu Marquis Bolduc, October 28th, 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Guassian clustering procedure and functions.
*/

#include "em.h"
#include "mldb/ml/em.h"
#include "matrix.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/procedure_collection.h"
#include "mldb/server/function_collection.h"
#include "jml/stats/distribution.h"
#include <boost/multi_array.hpp>
#include "jml/utils/guard.h"
#include "base/parallel.h"
#include "jml/utils/pair_utils.h"
#include "mldb/arch/timers.h"
#include "mldb/types/optional_description.h"
#include "jml/utils/vector_utils.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/server/analytics.h"
#include "mldb/types/any_impl.h"
#include "jml/utils/smart_ptr_utils.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/plugins/sql_config_validator.h"
#include "mldb/utils/log.h"


using namespace std;



namespace MLDB {

std::vector<double> tovector(boost::multi_array<double, 2>& m)
{
    std::vector<double> embedding;
    for(int i = 0; i < m.shape()[0]; i++)
    {
        for(int j = 0; j < m.shape()[1]; j++) {
             embedding.push_back(m[i][j]); // multiply by elements on diagonal
        }
    }

    return embedding;
}

DEFINE_STRUCTURE_DESCRIPTION(EMConfig);

EMConfigDescription::
EMConfigDescription()
{
    Optional<PolyConfigT<Dataset> > optional;
    optional.emplace(PolyConfigT<Dataset>().
                     withType(EMConfig::defaultOutputDatasetType));

    addField("trainingData", &EMConfig::trainingData,
             "Specification of the data for input to the procedure.  This should be "
             "organized as an embedding, with each selected row containing the same "
             "set of columns with numeric values to be used as coordinates.  The select statement "
             "does not support groupby and having clauses.");
    addField("outputDataset", &EMConfig::output,
             "Dataset for cluster assignment.  This dataset will contain the same "
             "row names as the input dataset, but the coordinates will be replaced "
             "by a single column giving the cluster number that the row was assigned to.",
              optional);
    addField("centroidsDataset", &EMConfig::centroids,
             "Dataset in which the centroids will be recorded.  This dataset will "
             "have the same coordinates (columns) as those selected from the input "
             "dataset, but will have one row per cluster, providing the centroid of "
             "the cluster.",
             PolyConfigT<Dataset>().withType("embedding"));
    addField("modelFileUrl", &EMConfig::modelFileUrl,
             "URL where the model file (with extension '.gs') should be saved. "
             "This file can be loaded by a function of type 'gaussianclustering' to apply "
             "the trained model to new data. "
             "If someone is only interested in how the training input is clustered "
             "then the parameter can be omitted and the outputDataset param can "
             "be provided instead.");
    addField("numInputDimensions", &EMConfig::numInputDimensions,
             "Number of dimensions from the input to use (-1 = all).  This limits "
             "the number of columns used.  Columns will be ordered alphabetically "
             "and the lowest ones kept.",
             -1);
    addField("numClusters", &EMConfig::numClusters,
             "Number of clusters to create.  This will provide the total number of "
             "centroids created.  There must be at least as many rows selected as "
             "clusters.", 10);
    addField("maxIterations", &EMConfig::maxIterations,
             "Maximum number of iterations to perform.  If no convergance is "
             "reached within this number of iterations, the current clustering "
             "will be returned.", 100);
    addField("functionName", &EMConfig::functionName,
             "If specified, a function of this name will be created using "
             "the training result.");
    addParent<ProcedureConfig>();

    onPostValidate = validateFunction<EMConfig>();
}

/*****************************************************************************/
/* EM PROCEDURE                                                           */
/*****************************************************************************/

EMProcedure::
EMProcedure(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->emConfig = config.params.convert<EMConfig>();
}

Any
EMProcedure::
getStatus() const
{
    return Any();

}

RunOutput
EMProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(emConfig, run);

    auto onProgress2 = [&] (const Json::Value & progress)
        {
            Json::Value value;
            value["dataset"] = progress;
            return onProgress(value);
        };

    if (!runProcConf.modelFileUrl.empty()) {
        checkWritability(runProcConf.modelFileUrl.toDecodedString(),
                         "modelFileUrl");
    }

    SqlExpressionMldbScope context(server);

    ConvertProgressToJson convertProgressToJson(onProgress);
    auto embeddingOutput = getEmbedding(*runProcConf.trainingData.stm,
                                        context,
                                        runProcConf.numInputDimensions,
                                        convertProgressToJson);

    auto rows = embeddingOutput.first;
    std::vector<KnownColumn> & vars = embeddingOutput.second;

    std::vector<distribution<double> > vecs;

    for (unsigned i = 0;  i < rows.size();  ++i) {
        vecs.emplace_back(distribution<double>(std::get<2>(rows[i]).begin(),
                                                   std::get<2>(rows[i]).end()));
    }

    if (vecs.size() == 0)
        throw HttpReturnException(400, "Gaussian clustering training requires at least 1 datapoint. "
                                  "Make sure your dataset is not empty and that your WHERE expression "
                                  "does not filter all the rows");

    ML::EstimationMaximisation em;
    vector<int> inCluster;

    int numClusters = emConfig.numClusters;
    int numIterations = emConfig.maxIterations;

    DEBUG_MSG(logger) << "EM training start";
    em.train(vecs, inCluster, numClusters, numIterations, 0);
    DEBUG_MSG(logger) << "EM training end";

    // Let the model know about its column names
    std::vector<ColumnPath> columnNames;
    for (auto & v: vars) {
        columnNames.push_back(v.columnName);
        em.columnNames.push_back(v.columnName.toUtf8String());
    }

    // output

    bool saved = false;
    if (!runProcConf.modelFileUrl.empty()) {
        try {
            makeUriDirectory(
                runProcConf.modelFileUrl.toDecodedString());
            em.save(runProcConf.modelFileUrl.toDecodedString());
            saved = true;
        }
        catch (const std::exception & exc) {
            throw HttpReturnException(400, "Error saving gaussian clustering model at location'" +
                                      runProcConf.modelFileUrl.toString() + "': " +
                                      exc.what());
        }
    }

    if (runProcConf.output.get()) {

        PolyConfigT<Dataset> outputDataset = *runProcConf.output;
        if (outputDataset.type.empty())
            outputDataset.type = EMConfig::defaultOutputDatasetType;

        auto output = createDataset(server, outputDataset, onProgress2, true /*overwrite*/);

        Date applyDate = Date::now();

        for (unsigned i = 0;  i < rows.size();  ++i) {
            std::vector<std::tuple<ColumnPath, CellValue, Date> > cols;
            cols.emplace_back(ColumnPath("cluster"), inCluster[i], applyDate);
            output->recordRow(std::get<1>(rows[i]), cols);
        }

        output->commit();
    }

    if (runProcConf.centroids.type != "" || emConfig.centroids.id != "") {

        auto centroids = createDataset(server, runProcConf.centroids, onProgress2, true /*overwrite*/);

        Date applyDate = Date::now();

        for (unsigned i = 0;  i < em.clusters.size();  ++i) {
            auto & cluster = em.clusters[i];

            std::vector<std::tuple<ColumnPath, CellValue, Date> > cols;

            for (unsigned j = 0;  j < cluster.centroid.size();  ++j) {
                cols.emplace_back(columnNames[j], cluster.centroid[j], applyDate);
            }

            auto flatmatrix = tovector(cluster.covarianceMatrix);

            for (unsigned j = 0;  j < flatmatrix.size();  ++j) {
                cols.emplace_back(ColumnPath(MLDB::format("c%02d", j)), flatmatrix[j], applyDate);
            }

            centroids->recordRow(RowPath(MLDB::format("%i", i)), cols);
        }

        centroids->commit();
    }

    if (!runProcConf.functionName.empty()) {
        if (saved) {
            EMFunctionConfig funcConf;
            funcConf.modelFileUrl = runProcConf.modelFileUrl;
            
            PolyConfig emPC;
            emPC.type = "gaussianclustering";
            emPC.id = runProcConf.functionName;
            emPC.params = funcConf;
            obtainFunction(server, emPC, onProgress);
        } else {
            throw HttpReturnException(400, "Can't create gaussian clustering function '" +
                                      runProcConf.functionName.rawString() +
                                      "'. Have you provided a valid modelFileUrl?",
                                      "modelFileUrl", runProcConf.modelFileUrl.toString());
        }
    }

    return Any();
}

DEFINE_STRUCTURE_DESCRIPTION(EMFunctionConfig);

EMFunctionConfigDescription::
EMFunctionConfigDescription()
{
    addField("modelFileUrl", &EMFunctionConfig::modelFileUrl,
             "URL of the model file (with extension '.gs') to load. "
             "This file is created by a procedure of type 'gaussianclustering.train'.");

    onPostValidate = [] (EMFunctionConfig * cfg,
                         JsonParsingContext & context) {
        // this includes empty url
        if(!cfg->modelFileUrl.valid()) {
            throw MLDB::Exception("modelFileUrl \"" + cfg->modelFileUrl.toString()
                                + "\" is not valid");
        }
    };
}


/*****************************************************************************/
/* EM FUNCTION                                                               */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(EMInput);

EMInputDescription::EMInputDescription()
{
    addField("embedding", &EMInput::embedding,
             "Values to be assigned to a cluster.");
}

DEFINE_STRUCTURE_DESCRIPTION(EMOutput);

EMOutputDescription::EMOutputDescription()
{
    addField("cluster", &EMOutput::cluster,
             "Cluster corresponding to the input values.");
}


struct EMFunction::Impl {
    ML::EstimationMaximisation em;
    std::vector<ColumnPath> columnNames;

    Impl(const Url & modelFileUrl) {
        em.load(modelFileUrl.toDecodedString());
        for (auto & c: em.columnNames)
            this->columnNames.push_back(PathElement(c));
    }
};

EMFunction::
EMFunction(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : BaseT(owner, config)
{  

    functionConfig = config.params.convert<EMFunctionConfig>();

    impl.reset(new Impl(functionConfig.modelFileUrl));

    dimension = impl->em.clusters[0].centroid.size();

    auto logger = MLDB::getMldbLog<EMFunction>();
    DEBUG_MSG(logger) << "got " << impl->em.clusters.size()
         << " clusters with " << dimension
         << "values";
}

struct EMFunctionApplier: public FunctionApplierT<EMInput, EMOutput> {
    EMFunctionApplier(const EMFunction * owner,
                      const std::shared_ptr<ExpressionValueInfo> & input)
        : FunctionApplierT<EMInput, EMOutput>(owner)
    {
        info = owner->getFunctionInfo();
        extract = input->extractDoubleEmbedding(owner->impl->columnNames);
    }

    ExpressionValueInfo::ExtractDoubleEmbeddingFunction extract;
};

std::unique_ptr<FunctionApplierT<EMInput, EMOutput> >
EMFunction::
bindT(SqlBindingScope & outerContext,
      const std::vector<std::shared_ptr<ExpressionValueInfo> > & input) const
{
    if (input.size() != 1)
        throw HttpReturnException(400, "EM function requires a single input");
    return std::unique_ptr<EMFunctionApplier>
        (new EMFunctionApplier(this, input.at(0)));
}

EMOutput 
EMFunction::
applyT(const ApplierT & applier_, EMInput input_) const
{
    // Extract an embedding with the given column names
    ExpressionValue storage;

    const auto * downcast
            = dynamic_cast<const EMFunctionApplier *>(&applier_);

    distribution<double> input = downcast->extract(input_.embedding);
    Date ts = input_.embedding.getEffectiveTimestamp();

    int bestCluster = impl->em.assign(input);

    return {ExpressionValue(bestCluster, ts)};
}

namespace {

RegisterProcedureType<EMProcedure, EMConfig>
regEM(builtinPackage(),
      "Gaussian clustering algorithm using Estimation Maximization on Gaussian Mixture Models",
      "procedures/EMProcedure.md.html",
      nullptr /* static route */,
      { MldbEntity::INTERNAL_ENTITY });

RegisterFunctionType<EMFunction, EMFunctionConfig>
regEMFunction(builtinPackage(), "gaussianclustering",
              "Apply an gaussian clustering to new data",
              "functions/EM.md.html",
              nullptr /* static route */,
              { MldbEntity::INTERNAL_ENTITY });

} // file scope

} // namespace MLDB

