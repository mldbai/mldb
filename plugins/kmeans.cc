// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** kmeans.cc
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

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
#include "mldb/jml/utils/worker_task.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/arch/timers.h"
#include "mldb/arch/simd_vector.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/ml/kmeans.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/server/analytics.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/optional_description.h"

using namespace std;


namespace Datacratic {
namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(KmeansConfig);

KmeansConfigDescription::
KmeansConfigDescription()
{
    Optional<PolyConfigT<Dataset> > optional;
    optional.emplace(PolyConfigT<Dataset>().
                     withType(KmeansConfig::defaultOutputDatasetType));
    
    addFieldDesc("trainingDataset", &KmeansConfig::dataset,
                 "Dataset provided for input to the k-means procedure.  This should be "
                 "organized as an embedding, with each selected row containing the same "
                 "set of columns with numeric values to be used as coordinates.",
                 makeInputDatasetDescription());
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
             PolyConfigT<Dataset>().withType("embedding"));
    addField("select", &KmeansConfig::select,
             "Columns to select from the input matrix for the coordinates to input "
             "into k-means training.  The selected columns must be finite numbers "
             "and must not have missing values.",
             SelectExpression("*"));
    addField("when", &KmeansConfig::when,
             "Boolean expression determining which tuples from the dataset "
             "to keep based on their timestamps",
             WhenExpression::parse("true"));
    addField("where", &KmeansConfig::where,
             "Rows to select for k-means training.  This expression allows a subset "
             "of the rows that were input to the training process to be selected.",
             SqlExpression::parse("true"));
    addField("orderBy", &KmeansConfig::orderBy,
             "How to order the rows.  This only has an effect when OFFSET "
             "or LIMIT are used.  Default is to order by rowHash.",
             OrderByExpression::ROWHASH);
    addField("offset", &KmeansConfig::offset,
             "How many rows to skip before using data",
             ssize_t(0));
    addField("limit", &KmeansConfig::limit,
             "How many rows of data to use.  -1 (the default) means use all "
             "of the rows in the dataset.",
             ssize_t(-1));
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
    addField("functionName", &KmeansConfig::functionName,
             "If specified, a kmeans function of this name will be created using "
             "the training result.");
    addParent<ProcedureConfig>();
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
        throw ML::Exception("Unknown kmeans metric type");
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

    auto onProgress2 = [&] (const Json::Value & progress)
        {
            Json::Value value;
            value["dataset"] = progress;
            return onProgress(value);
        };

    SqlExpressionMldbContext context(server);

    auto boundDataset = runProcConf.dataset->bind(context);

    auto embeddingOutput = getEmbedding(runProcConf.select,
                                        *boundDataset.dataset,
                                        boundDataset.asName, 
                                        runProcConf.when,
                                        runProcConf.where, {},
                                        runProcConf.numInputDimensions,
                                        runProcConf.orderBy,
                                        runProcConf.offset,
                                        runProcConf.limit,
                                        onProgress2);

    std::vector<std::tuple<RowHash, RowName, std::vector<double>,
                           std::vector<ExpressionValue> > > & rows
        = embeddingOutput.first;
    std::vector<KnownColumn> & vars = embeddingOutput.second;

    std::vector<ColumnName> columnNames;
    for (auto & v: vars) {
        columnNames.push_back(v.columnName);
    }

    std::vector<ML::distribution<float> > vecs;

    for (unsigned i = 0;  i < rows.size();  ++i) {
        vecs.emplace_back(ML::distribution<float>(std::get<2>(rows[i]).begin(),
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

    //doProgress("running k-means");

    kmeans.train(vecs, inCluster, numClusters, numIterations);

    //doProgress("finished k-means");
    if (runProcConf.output.get()) {

        PolyConfigT<Dataset> outputDataset = *runProcConf.output;
        if (outputDataset.type.empty())
            outputDataset.type = KmeansConfig::defaultOutputDatasetType;

        auto output = createDataset(server, outputDataset, onProgress2, true /*overwrite*/);

        Date applyDate = Date::now();
        
        for (unsigned i = 0;  i < rows.size();  ++i) {
            std::vector<std::tuple<ColumnName, CellValue, Date> > cols;
            cols.emplace_back(ColumnName("cluster"), inCluster[i], applyDate);
            output->recordRow(std::get<1>(rows[i]), cols);
        }
        
        output->commit();
    }

    if (runProcConf.centroids.type != "" || kmeansConfig.centroids.id != "") {

        auto centroids = createDataset(server, runProcConf.centroids, onProgress2, true /*overwrite*/);

        Date applyDate = Date::now();

        for (unsigned i = 0;  i < kmeans.clusters.size();  ++i) {
            auto & cluster = kmeans.clusters[i];

            std::vector<std::tuple<ColumnName, CellValue, Date> > cols;

            for (unsigned j = 0;  j < cluster.centroid.size();  ++j) {
                cols.emplace_back(columnNames[j], cluster.centroid[j], applyDate);
            }
            
            centroids->recordRow(RowName(ML::format("%i", i)), cols);
        }
        
        centroids->commit();
    }

    if(!runProcConf.functionName.empty()) {
        KmeansFunctionConfig funcConf;
        funcConf.metric = runProcConf.metric;
        funcConf.centroids = runProcConf.centroids;

        PolyConfig kmeansFuncPC;
        kmeansFuncPC.type = "kmeans";
        kmeansFuncPC.id = runProcConf.functionName;
        kmeansFuncPC.params = funcConf;

        obtainFunction(server, kmeansFuncPC, onProgress);
    }

    return Any();
}

DEFINE_STRUCTURE_DESCRIPTION(KmeansFunctionConfig);

KmeansFunctionConfigDescription::
KmeansFunctionConfigDescription()
{
    addField("centroids", &KmeansFunctionConfig::centroids,
             "Dataset containing centroids of each cluster: one row per cluster.");
    addField("metric", &KmeansFunctionConfig::metric,
             "Metric to use to calculate distances.  This should match the "
             "metric used in training.");
    addField("select", &KmeansFunctionConfig::select,
             "Fields to select to calculate k-means over.  Only those fields "
             "that are selected here need to be matched.  Default is to use "
             "all fields.",
             SelectExpression("*"));
    addField("when", &KmeansFunctionConfig::when,
             "Boolean expression determining which tuples from the dataset "
             "to keep based on their timestamps",
             WhenExpression::parse("true"));
    addField("where", &KmeansFunctionConfig::where,
             "Rows to select for k-means training.  This will effectively "
             "limit which clusters are active.  Default is to use all "
             "clusters.",
             SqlExpression::parse("true"));
}


/*****************************************************************************/
/* KMEANS FUNCTION                                                              */
/*****************************************************************************/

KmeansFunction::
KmeansFunction(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<KmeansFunctionConfig>();

    auto dataset = obtainDataset(server, functionConfig.centroids, onProgress);

    // Load up the embeddings
    auto embeddingOutput = getEmbedding(functionConfig.select,
                                        *dataset, "", 
                                        functionConfig.when,
                                        functionConfig.where,
                                        { },
                                        -1 /* max dimensions */,
                                        ORDER_BY_NOTHING, 0, -1,
                                        onProgress);
    
    // Each row is a cluster
    std::vector<std::tuple<RowHash, RowName, std::vector<double>,
                           std::vector<ExpressionValue> > > & rows
        = embeddingOutput.first;
    std::vector<KnownColumn> & vars = embeddingOutput.second;

    for (auto & v: vars) {
        columnNames.push_back(v.columnName);
    }
    
    for (auto & r: rows) {
        Cluster cluster;
        cluster.clusterName = jsonDecodeStr<CellValue>(std::get<1>(r).toString());
        cluster.centroid.insert(cluster.centroid.end(),
                                std::get<2>(r).begin(),
                                std::get<2>(r).end());
        clusters.emplace_back(std::move(cluster));
    }

    cerr << "got " << clusters.size()
         << " clusters with " << columnNames.size()
         << "values" << endl;
}

Any
KmeansFunction::
getStatus() const
{
    return Any();
}

FunctionOutput
KmeansFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
    FunctionOutput result;

    //cerr << "context = " << jsonEncode(context) << endl;

    // Extract an embedding with the given column names
    ExpressionValue storage;
    const ExpressionValue & inputVal = context.get("embedding", storage);
    //cerr << "getting embedding" << endl;
    ML::distribution<float> input = inputVal.getEmbedding(columnNames.size());
    Date ts = inputVal.getEffectiveTimestamp();

    std::unique_ptr<ML::KMeansMetric> metric(makeMetric(functionConfig.metric));

    double bestDist = INFINITY;
    CellValue bestCluster;

    for (unsigned i = 0;  i < clusters.size();  ++i) {
        double dist = metric->distance(input, clusters[i].centroid);
        if (dist < bestDist
            || (dist == bestDist && clusters[i].clusterName < bestCluster)) {
            bestDist = dist;
            bestCluster = clusters[i].clusterName;
        }
    }

    result.set("cluster", ExpressionValue(bestCluster, ts));
    
    return result;
}

FunctionInfo
KmeansFunction::
getFunctionInfo() const
{
    FunctionInfo result;

    result.input.addEmbeddingValue("embedding", columnNames.size());
    result.output.addAtomValue("cluster");

    return result;
}

namespace {

RegisterProcedureType<KmeansProcedure, KmeansConfig>
regKmeans(builtinPackage(),
          "kmeans.train",
          "Simple clustering algorithm based on cluster centroids in embedding space",
          "procedures/KmeansProcedure.md.html");

RegisterFunctionType<KmeansFunction, KmeansFunctionConfig>
regKmeansFunction(builtinPackage(),
                  "kmeans",
                  "Apply a k-means clustering to new data",
                  "functions/Kmeans.md.html");

} // file scope

} // namespace MLDB
} // namespace Datacratic

