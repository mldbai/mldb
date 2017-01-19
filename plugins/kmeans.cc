/** kmeans.cc
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    Implementation of an KMEANS algorithm for embedding of a dataset.
*/

#include "mldb/jml/stats/distribution_simd.h"
#include "kmeans.h"
#include "matrix.h"
#include "mldb/server/mldb_server.h"
#include "mldb/core/dataset.h"
#include "mldb/jml/stats/distribution.h"
#include <boost/multi_array.hpp>
#include "mldb/jml/utils/guard.h"
#include "mldb/base/parallel.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/arch/timers.h"
#include "mldb/arch/simd_vector.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/ml/kmeans.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/plugins/sql_config_validator.h"
#include "mldb/server/analytics.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/optional_description.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/filter_streams.h"

using namespace std;



namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(KmeansConfig);


KmeansConfigDescription::
KmeansConfigDescription()
{
    Optional<PolyConfigT<Dataset> > optional;
    optional.emplace(PolyConfigT<Dataset>().
                     withType(KmeansConfig::defaultOutputDatasetType));

    addField("trainingData", &KmeansConfig::trainingData,
             "Specification of the data for input to the k-means procedure.  This should be "
             "organized as an embedding, with each selected row containing the same "
             "set of columns with numeric values to be used as coordinates.  The select statement "
             "does not support groupby and having clauses.");
    addField("outputDataset", &KmeansConfig::output,
             "Dataset for cluster assignment.  This dataset will contain the same "
             "row names as the input dataset, but the coordinates will be replaced "
             "by a single column giving the cluster number that the row was assigned to.",
              optional);
    addField("centroidsDataset", &KmeansConfig::centroids,
             "Dataset in which the centroids will be recorded.  This dataset will "
             "have the same coordinates (columns) as those selected from the input "
             "dataset, but will have one row per cluster, providing the centroid of "
             "the cluster.",
             optional);
    addField("numInputDimensions", &KmeansConfig::numInputDimensions,
             "Number of dimensions from the input to use (-1 = all).  This limits "
             "the number of columns used.  Columns will be ordered alphabetically "
             "and the lowest ones kept.",
             -1);
    addField("numClusters", &KmeansConfig::numClusters,
             "Number of clusters to create.  This will provide the total number of "
             "centroids created.  There must be at least as many rows selected as "
             "clusters.", 10);
    addField("maxIterations", &KmeansConfig::maxIterations,
             "Maximum number of iterations to perform.  If no convergeance is "
             "reached within this number of iterations, the current clustering "
             "will be returned.", 100);
    addField("metric", &KmeansConfig::metric,
             "Metric space in which the k-means distances will be calculated. "
             "Normally this will be Cosine for an orthonormal basis, and "
             "Euclidian for another basis",
             METRIC_COSINE);
    addField("modelFileUrl", &KmeansConfig::modelFileUrl,
             "URL where the model file (with extension '.kms') should be saved. "
             "This file can be loaded by the ![](%%doclink kmeans function). "
             "This parameter is optional unless the `functionName` parameter is used.");
    addField("functionName", &KmeansConfig::functionName,
             "If specified, an instance of the ![](%%doclink kmeans function) of this name will be created using "
             "the trained model. Note that to use this parameter, the `modelFileUrl` must "
             "also be provided.");
    addParent<ProcedureConfig>();

    onPostValidate = chain(validateQuery(&KmeansConfig::trainingData,
                                         MustContainFrom(),
                                         NoGroupByHaving()),
                           validateFunction<KmeansConfig>());
}

// TODO: see http://www.eecs.tufts.edu/~dsculley/papers/fastkmeans.pdf

namespace {

ML::KMeansMetric * makeMetric(MetricSpace metric)
{
    switch (metric) {
    case METRIC_EUCLIDEAN:
        return new ML::KMeansEuclideanMetric();
        break;
    case METRIC_COSINE:
        return new ML::KMeansCosineMetric();
        break;
    default:
        throw MLDB::Exception("Unknown kmeans metric type");
    }
}

} // file scope

/*****************************************************************************/
/* KMEANS PROCEDURE                                                           */
/*****************************************************************************/

KmeansProcedure::
KmeansProcedure(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    kmeansConfig = config.params.convert<KmeansConfig>();
}

Any
KmeansProcedure::
getStatus() const
{
    return Any();
}

RunOutput
KmeansProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(kmeansConfig, run);

    // an empty url is allowed but other invalid urls are not
    if(!runProcConf.modelFileUrl.empty() && !runProcConf.modelFileUrl.valid()) {
        throw HttpReturnException(400, "modelFileUrl \"" +
                                  runProcConf.modelFileUrl.toUtf8String()
                                  + "\" is not valid");
    }

    if (!runProcConf.modelFileUrl.empty()) {
        checkWritability(runProcConf.modelFileUrl.toDecodedString(), "modelFileUrl");
    }

    auto onProgress2 = [&] (const Json::Value & progress)
        {
            Json::Value value;
            value["dataset"] = progress;
            return onProgress(value);
        };

    SqlExpressionMldbScope context(server);

    ConvertProgressToJson convertProgressToJson(onProgress);
    auto embeddingOutput = getEmbedding(*runProcConf.trainingData.stm,
                                        context,
                                        runProcConf.numInputDimensions,
                                        convertProgressToJson);

    std::vector<std::tuple<RowHash, RowPath, std::vector<double>,
                           std::vector<ExpressionValue> > > & rows
        = embeddingOutput.first;
    std::vector<KnownColumn> & vars = embeddingOutput.second;

    std::vector<ColumnPath> columnNames;
    for (auto & v: vars) {
        columnNames.push_back(v.columnName);
    }

    std::vector<distribution<float> > vecs;

    for (unsigned i = 0;  i < rows.size();  ++i) {
        vecs.emplace_back(distribution<float>(std::get<2>(rows[i]).begin(),
                                                  std::get<2>(rows[i]).end()));
    }

    if (vecs.size() == 0)
        throw HttpReturnException(400, "Kmeans training requires at least 1 datapoint. "
                                  "Make sure your dataset is not empty and that your WHERE expression "
                                  "does not filter all the rows");

    ML::KMeans kmeans;
    kmeans.metric.reset(makeMetric(runProcConf.metric));

    vector<int> inCluster;

    int numClusters = runProcConf.numClusters;
    int numIterations = runProcConf.maxIterations;

    kmeans.train(vecs, inCluster, numClusters, numIterations);

    bool saved = false;
    if (!runProcConf.modelFileUrl.empty()) {
        try {
            makeUriDirectory(runProcConf.modelFileUrl.toDecodedString());
            Json::Value md;
            md["algorithm"] = "MLDB k-Means model";
            md["version"] = 1;
            md["columnNames"] = jsonEncode(columnNames);

            filter_ostream stream(runProcConf.modelFileUrl);
            stream << md.toString();
            ML::DB::Store_Writer writer(stream);
            kmeans.serialize(writer);
            saved = true;
        }
        catch (const std::exception & exc) {
            throw HttpReturnException(400, "Error saving kmeans centroids at location'" +
                                      runProcConf.modelFileUrl.toString() + "': " +
                                      exc.what());
        }
    }

    if (runProcConf.output.get()) {

        PolyConfigT<Dataset> outputDataset = *runProcConf.output;
        if (outputDataset.type.empty())
            outputDataset.type = KmeansConfig::defaultOutputDatasetType;

        auto output = createDataset(server, outputDataset, onProgress2, true /*overwrite*/);

        Date applyDate = Date::now();

        for (unsigned i = 0;  i < rows.size();  ++i) {
            std::vector<std::tuple<ColumnPath, CellValue, Date> > cols;
            cols.emplace_back(ColumnPath("cluster"), inCluster[i], applyDate);
            output->recordRow(std::get<1>(rows[i]), cols);
        }

        output->commit();
    }

    if (runProcConf.centroids.get()) {

        PolyConfigT<Dataset> centroidsDataset = *runProcConf.centroids;
        if (centroidsDataset.type.empty())
            centroidsDataset.type = KmeansConfig::defaultOutputDatasetType;

        auto centroids = createDataset(server, centroidsDataset, onProgress2, true /*overwrite*/);

        Date applyDate = Date::now();

        for (unsigned i = 0;  i < kmeans.clusters.size();  ++i) {
            auto & cluster = kmeans.clusters[i];

            std::vector<std::tuple<ColumnPath, CellValue, Date> > cols;

            for (unsigned j = 0;  j < cluster.centroid.size();  ++j) {
                cols.emplace_back(columnNames[j], cluster.centroid[j], applyDate);
            }

            centroids->recordRow(RowPath(MLDB::format("%i", i)), cols);
        }

        centroids->commit();
    }

    if(!runProcConf.functionName.empty()) {
        if (saved) {
            KmeansFunctionConfig funcConf;
            funcConf.modelFileUrl = runProcConf.modelFileUrl;

            PolyConfig kmeansFuncPC;
            kmeansFuncPC.type = "kmeans";
            kmeansFuncPC.id = runProcConf.functionName;
            kmeansFuncPC.params = funcConf;

            createFunction(server, kmeansFuncPC, onProgress, true);
        } else {
            throw HttpReturnException(400, "Can't create kmeans function '" +
                                      runProcConf.functionName.rawString() +
                                      "'. Have you provided a valid modelFileUrl?",
                                      "modelFileUrl", runProcConf.modelFileUrl.toString());
        }
    }

    return Any();
}

DEFINE_STRUCTURE_DESCRIPTION(KmeansFunctionConfig);

KmeansFunctionConfigDescription::
KmeansFunctionConfigDescription()
{
    addField("modelFileUrl", &KmeansFunctionConfig::modelFileUrl,
             "URL of the model file (with extension '.kms') to load. "
             "This file is created by the ![](%%doclink kmeans.train procedure).");

    onPostValidate = [] (KmeansFunctionConfig * cfg,
                         JsonParsingContext & context) {
        // this includes empty url
        if(!cfg->modelFileUrl.valid()) {
            throw MLDB::Exception("modelFileUrl \"" + cfg->modelFileUrl.toString()
                                + "\" is not valid");
        }
    };
}


/*****************************************************************************/
/* KMEANS FUNCTION                                                           */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(KmeansFunctionArgs);

KmeansFunctionArgsDescription::
KmeansFunctionArgsDescription()
{
    addField("embedding", &KmeansFunctionArgs::embedding,
             "Embedding values for the k-means function.  The column names in "
             "this embedding must match those used in the original kmeans "
             "dataset.");
}

DEFINE_STRUCTURE_DESCRIPTION(KmeansExpressionValue);

KmeansExpressionValueDescription::KmeansExpressionValueDescription()
{
    addField("cluster", &KmeansExpressionValue::cluster,
             "Index of the row in the `centroids` dataset whose columns describe "
             "the point which is closest to the input according to the `metric` "
             "specified in training.");
}


struct KmeansFunction::Impl {
    ML::KMeans kmeans;
    std::vector<ColumnPath> columnNames;

    Impl(const Url & modelFileUrl)
    {
        filter_istream stream(modelFileUrl);
        std::string firstLine;
        std::getline(stream, firstLine);
        Json::Value md = Json::parse(firstLine);
        if (md["algorithm"] != "MLDB k-Means model") {
            throw HttpReturnException(400, "Model file is not a k-means model");
        }
        if (md["version"].asInt() != 1) {
            throw HttpReturnException(400, "k-Means model version is wrong");
        }
        columnNames = jsonDecode<std::vector<ColumnPath> >(md["columnNames"]);
        
        ML::DB::Store_Reader store(stream);
        kmeans.reconstitute(store);
    }
};

KmeansFunction::
KmeansFunction(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : BaseT(owner, config)
{
    functionConfig = config.params.convert<KmeansFunctionConfig>();

    impl.reset(new Impl(functionConfig.modelFileUrl));

    dimension = impl->kmeans.clusters[0].centroid.size();
}

KmeansExpressionValue 
KmeansFunction::
call(KmeansFunctionArgs input) const
{
    Date ts = input.embedding.getEffectiveTimestamp();

    return {ExpressionValue
            (impl->kmeans.assign
             (input.embedding.getEmbedding(impl->columnNames.data(),
                                           impl->columnNames.size())
              .cast<float>()),
             ts)};
}

namespace {

RegisterProcedureType<KmeansProcedure, KmeansConfig>
regKmeans(builtinPackage(),
          "Simple clustering algorithm based on cluster centroids in embedding space",
          "procedures/KmeansProcedure.md.html");

RegisterFunctionType<KmeansFunction, KmeansFunctionConfig>
regKmeansFunction(builtinPackage(),
                  "kmeans",
                  "Apply a k-means clustering to new data",
                  "functions/Kmeans.md.html");

} // file scope

} // namespace MLDB

