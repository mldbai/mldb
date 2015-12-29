
#include "jml/stats/distribution_simd.h"
#include "em.h"
#include "matrix.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/procedure_collection.h"
#include "mldb/server/function_collection.h"
#include "jml/stats/distribution.h"
#include <boost/multi_array.hpp>
#include "jml/utils/guard.h"
#include "jml/utils/worker_task.h"
#include "jml/utils/pair_utils.h"
#include "mldb/arch/timers.h"
#include "mldb/arch/simd_vector.h"
#include "mldb/arch/atomic_ops.h"
#include "jml/utils/vector_utils.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/ml/kmeans.h"

#include "mldb/sql/sql_expression.h"
#include "mldb/server/analytics.h"
#include "mldb/types/any_impl.h"

#include <boost/random/mersenne_twister.hpp>
#include "jml/utils/smart_ptr_utils.h"
#include <boost/random/uniform_int.hpp>

#include "mldb/plugins/Eigen/Dense"
#include "mldb/plugins/Eigen/SVD"


using namespace std;


namespace Datacratic {
namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(EMConfig);

EMConfigDescription::
EMConfigDescription()
{
    addField("dataset", &EMConfig::dataset,
             "Dataset provided for input to the k-means procedure.  This should be "
             "organized as an embedding, with each selected row containing the same "
             "set of columns with numeric values to be used as coordinates.");
    addField("output", &EMConfig::output,
             "Dataset for cluster assignment.  This dataset will contain the same "
             "row names as the input dataset, but the coordinates will be replaced "
             "by a single column giving the cluster number that the row was assigned "
             "to.");
    addField("centroids", &EMConfig::centroids,
             "Dataset in which the centroids will be recorded.  This dataset will "
             "have the same coordinates (columns) as those selected from the input "
             "dataset, but will have one row per cluster, providing the centroid of "
             "the cluster.");
    addField("select", &EMConfig::select,
             "Columns to select from the input matrix for the coordinates to input "
             "into k-means training.  The selected columns must be finite numbers "
             "and must not have missing values.",
             SelectExpression("*"));
    addField("where", &EMConfig::where,
             "Rows to select for k-means training.  This expression allows a subset "
             "of the rows that were input to the training process to be selected.",
             SqlExpression::parse("true"));
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
             "Maximum number of iterations to perform.  If no convergeance is "
             "reached within this number of iterations, the current clustering "
             "will be returned.", 2);
}


Eigen::VectorXd toEigenVector(const ML::distribution<float> & distribution)
{
  Eigen::VectorXd result(distribution.size());
  for (int i = 0; i < distribution.size(); ++i)
  {
      result(i) = distribution[i];
  }

  return result;
}   

void contributeToAverage(ML::distribution<float> & average,
                         const ML::distribution<float> & point, double weight)
{
    // if [0,0,...,0], do not contribue to average
    if (point.any())
        average += point * weight; 
}

double distance(const ML::distribution<float> & x,
                const ML::distribution<float> & y)
{ return (x - y).two_norm(); }

ML::distribution<float>
average(const std::vector<ML::distribution<float>> & points)
{
    ML::distribution<float> avg(points[0].size(), 0.0);
    double weight = 1. / points.size();
    for (auto & x : points)
        contributeToAverage(avg, x, weight);
    return avg;
}    

double gaussianDistance(const ML::distribution<float> & pt, const ML::distribution<float> & origin, const Eigen::MatrixXd & covarianceMatrix, 
                        const Eigen::MatrixXd & invertCovarianceMatrix, float determinant)
{
    //cerr << "gaussian distance(" << pt[0] << "," << pt[1] << "vs centroid(" << origin[0] << "," << origin[1] << endl;

    Eigen::VectorXd mean = toEigenVector(origin);
    Eigen::VectorXd eigenPt = toEigenVector(pt);

    Eigen::VectorXd xToU = eigenPt - mean;
    Eigen::VectorXd variance = invertCovarianceMatrix * xToU ;

    double value_exponent = -0.5f*xToU.dot(variance);

    float determinantCovMatrix = determinant;

    //cerr << "cov matrix determinant " << determinantCovMatrix << endl;

    if (determinantCovMatrix < 0)
      determinantCovMatrix = fabs(determinantCovMatrix);

    double distance = (1.0f / (pow(2.0f * 3.14159, pt.size() / 2.0f) * sqrt(determinantCovMatrix))) * exp(value_exponent);

    //cerr << "Distance: " << distance << endl;

    return distance;

}


Eigen::MatrixXd EstimateCovariant(int i, const std::vector<ML::distribution<float>> & points, 
                                 const Eigen::MatrixXd& distanceMatrix, double totalWeight, ML::distribution<float> average)
{
    Eigen::MatrixXd variant;

    if (totalWeight < 0.000001f)
      return variant;

   // cerr << "EstimateCovariant num point " << count << " average " << average[0] << "," << average[1] << endl;

    variant.resize(average.size(), average.size());

   // cerr<< "EstimateCovariant num points " << distanceMatrix.rows() << " total weight " << totalWeight << endl;

    for (int n = 0; n < distanceMatrix.rows(); ++n)
    {
        ML::distribution<float> pt = points[n] - average;
        auto vec = toEigenVector(pt);

    //    cerr << "EstimateCovariant vec (" << pt[0] << "," << pt[1] << endl;

        Eigen::MatrixXd variantPt = vec * vec.transpose() * distanceMatrix(n, i);

        if (n == 0) {                                    
            variant = variantPt;
        }
        else {
            variant += variantPt;
        }
    }

   // cerr << variant(0,0) << endl;

    variant /= totalWeight;

    return variant;
}

std::vector<double> tovector(Eigen::MatrixXd& m)
{
    std::vector<double> embedding;
    for(int i = 0; i < m.rows(); i++)
    {
        for(int j = 0; j < m.cols(); j++) {
             embedding.push_back(m(i,j)); // multiply by elements on diagonal
        }
    }       

    return embedding;    
}

struct EstimationMaximisation
{
    struct Cluster {
        double totalWeight;
        ML::distribution<float> centroid;
        Eigen::MatrixXd covarianceMatrix;
        Eigen::MatrixXd invertCovarianceMatrix;
        float pseudoDeterminant;
    };

    std::vector<Cluster> clusters;

  void
  train(const std::vector<ML::distribution<float>> & points,
        std::vector<int> & in_cluster,
        int nbClusters,
        int maxIterations,
        int randomSeed
        ) 
  {
    using namespace std;

    if (nbClusters < 2)
        throw ML::Exception("EM with less than 2 clusters doesn't make any sense!");

    boost::mt19937 rng;
    rng.seed(randomSeed);

    int npoints = points.size();
    in_cluster.resize(npoints, -1);
    clusters.resize(nbClusters);

    Eigen::MatrixXd distanceMatrix(npoints, nbClusters);

    // Smart initialization of the centroids
    // Stolen from kmeans - why not
    //cerr << "EM initialization" << endl;
    clusters[0].centroid = points[rng() % points.size()];
    int n = min(100, (int) points.size()/2);
    for (int i=1; i < nbClusters; ++i) {
        // This is my version of the wiki algorithm :
        // Amongst 100 random points, I take the farthest from the closest
        // centroid as the next centroid
        float distMax = -INFINITY;
        int bestPoint = -1;
        // We try it for 100 points
        for (int j=0; j < n; ++j) {
            // For a random point
            int randomIdx = rng() % points.size();
            // Find the closest cluster
            float distMin = INFINITY;
            // For each cluster
            for (int k=0; k < i; ++k) {

                float dist = distance(points[randomIdx], clusters[k].centroid);

                distanceMatrix(j, i) = dist;

                if (dist < distMin) {
                    distMin = dist;
                }
                // cerr << "point " << j << " norm " << points[randomIdx].two_norm() << endl;
                // cerr << "cluster " << k << " norm " << clusters[k].centroid.two_norm() << endl;
                // cerr << "distance " << dist << endl;
            }
            if (distMin > distMax) {
                distMax = distMin;
                bestPoint = randomIdx;
            }
        }
        if (bestPoint == -1) {
            //cerr << "kmeans initialization failed for centroid [" << i << "]" << endl;
            bestPoint = rng() % points.size();
        }
        clusters[i].centroid = points[bestPoint];
        // cerr << "norm of best init centroid " << clusters[i].centroid.two_norm() << endl;
    }

    //cerr << "initialize cov matrices" << endl;
    int numdimensions = points[0].size();
    for (int i=0; i < nbClusters; ++i) {

      clusters[i].covarianceMatrix = Eigen::MatrixXd::Identity(numdimensions, numdimensions);
      clusters[i].invertCovarianceMatrix = clusters[i].covarianceMatrix;
      clusters[i].pseudoDeterminant = 1.0f; 
    }

    //cerr << "EM iterations" << endl;
    for (int iter = 0;  iter < maxIterations;  ++iter) {

        // How many have changed cluster?  Used to know when the cluster
        // contents are stable
        int changes = 0;

         //Step 1: assign each point to a distribution in the mixture

         auto findNewCluster = [&] (int i) {

            int best_cluster = this->assign(points[i], distanceMatrix, i);

            if (best_cluster != in_cluster[i]) {
                ML::atomic_inc(changes);
                in_cluster[i] = best_cluster;
            }
        };

        // cerr << "EM find clusters" << endl;        

        for (int i = 0; i < points.size(); ++i)
          findNewCluster(i);

        //cerr << "EM find end" << endl;

        //Step 2: maximizing distribution's parameters 
        for (auto & c : clusters)
        {
            // If no member, we want to leave it there
            std::fill(c.centroid.begin(), c.centroid.end(), 0.0);
		        c.totalWeight = 0.0f;
        }

        std::vector<std::mutex> locks(clusters.size());

        auto addToMeanForPoint = [&] (int i) {

            auto point = points[i];
             for (int cluster = 0; cluster < clusters.size(); ++cluster)    
            {
                double distance = distanceMatrix(i, cluster);
                // cerr << "(" << point[0] << "," << point[1] << ") : " << cluster << endl;
                clusters[cluster].centroid += point * distance;
		            clusters[cluster].totalWeight += distance;
            }
        };

        // calculate mean
        for (int i = 0; i < points.size(); ++i)
        {
          addToMeanForPoint(i);
        }

        //normalizeMean
        for (int cluster = 0; cluster < clusters.size(); ++cluster)
        {
          if (clusters[cluster].totalWeight > 0.000001f)
          {
            clusters[cluster].centroid = clusters[cluster].centroid / clusters[cluster].totalWeight;
          }
        }
         
        //calculate covariant matrix
        //  cerr << "EM find covariant" << endl;
        for (int i = 0; i < clusters.size(); ++i)
        {
          clusters[i].covarianceMatrix = EstimateCovariant(i, points, distanceMatrix, clusters[i].totalWeight, clusters[i].centroid);
          ExcAssert(clusters[i].covarianceMatrix.rows() == clusters[i].covarianceMatrix.cols());
          Eigen::JacobiSVD<Eigen::MatrixXd> svd(clusters[i].covarianceMatrix, Eigen::ComputeFullU | Eigen::ComputeFullV);

          //Remove small values and calculate pseudo determinant
          double pseudoDeterminant = 1.0f;
          Eigen::VectorXd singularValues = svd.singularValues();
          Eigen::VectorXd invertSingularValues = svd.singularValues();

          //cerr << "checking " << singularValues.size() << "singular values" << endl;

          for (int i = 0; i < singularValues.size(); ++i) {
              //cerr << singularValues(i) << endl;
              if (singularValues(i) < 0.0001f) {
                  //cerr << "DROPPING SINGULAR VALUE " << singularValues(i) << endl;
                  singularValues(i) = 0.0f;
                  invertSingularValues(i) = 0.0f;
              }
              else {
                  pseudoDeterminant *= singularValues(i);
                  invertSingularValues(i) = 1.0f / singularValues(i);
              }
          }

          //calculate pseudo matrix, pseudo inverse and pseudo determinant

          Eigen::MatrixXd pseudoCovariant = svd.matrixU() * singularValues.asDiagonal() * svd.matrixV();
          clusters[i].invertCovarianceMatrix = svd.matrixV() * invertSingularValues.asDiagonal() * svd.matrixU().transpose();
          clusters[i].pseudoDeterminant = pseudoDeterminant;
        }

        //cerr << "EM END ITER " << iter << endl << endl << endl;
      }
  }

  int
  assign(const ML::distribution<float> & point, Eigen::MatrixXd& distanceMatrix, int pIndex) const
  {
      using namespace std;
      if (clusters.size() == 0)
          throw ML::Exception("Did you train your em?");

      float distMin = 0;
      int best_cluster = -1;
      auto distances = clusterDistances(point);
      double totalWeight = 0.0f;

      for (int i=0; i < clusters.size(); ++i) {

          double distance = distances[i];

          totalWeight += distance;
          distanceMatrix(pIndex, i) = distance;

          if (distances[i] > distMin) {
              distMin = distances[i];
              best_cluster = i;
          }
      }

       if (totalWeight > 0) {

            for (int i=0; i < clusters.size(); ++i) {
                distanceMatrix(pIndex, i) /= totalWeight;
            }
       }

      //Most likely these are points with all distance at 0, often during the first iteration
      if (best_cluster == -1)
          best_cluster = 0;

      return best_cluster;
  }

  ML::distribution<float>
  clusterDistances(const ML::distribution<float> & point) const
  {
      ML::distribution<float> distances(clusters.size());
      for (int i=0; i < clusters.size(); ++i) {
          distances[i] = gaussianDistance(point, clusters[i].centroid, clusters[i].covarianceMatrix, clusters[i].invertCovarianceMatrix, clusters[i].pseudoDeterminant);
      }
      return distances;
  }

};


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
   EstimationMaximisation em;

    auto onProgress2 = [&] (const Json::Value & progress)
        {
            Json::Value value;
            value["dataset"] = progress;
            return onProgress(value);
        };

    SqlExpressionMldbContext context(server);

    //cerr << "binding dataset" << endl;
    auto boundDataset = emConfig.dataset->bind(context);

    auto embeddingOutput = getEmbedding(emConfig.select, *boundDataset.dataset, boundDataset.asName,
                                        
                                        WhenExpression::parse("true"),
                                        emConfig.where, {},
                                        emConfig.numInputDimensions, 
                                        ORDER_BY_NOTHING,
                                        0, -1,
                                        onProgress2);

    auto rows = embeddingOutput.first;
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

    vector<int> inCluster;

    int numClusters = emConfig.numClusters;
    int numIterations = emConfig.maxIterations;

    //cerr << "EM training start" << endl;
    em.train(vecs, inCluster, numClusters, numIterations, 0);
    //cerr << "EM training end" << endl;

    // output
    
    if (emConfig.output.type != "" || emConfig.output.id != "") {
        auto output = createDataset(server, emConfig.output, onProgress2, true /*overwrite*/);

        Date applyDate = Date::now();
        
        for (unsigned i = 0;  i < rows.size();  ++i) {
            std::vector<std::tuple<ColumnName, CellValue, Date> > cols;
            cols.emplace_back(ColumnName("cluster"), inCluster[i], applyDate);
            output->recordRow(std::get<1>(rows[i]), cols);
        }
        
        output->commit();
    }

    //cerr << "centroids type" << emConfig.centroids.type << endl;
    if (emConfig.centroids.type != "" || emConfig.centroids.id != "") {

        //cerr << "writing centroids" << emConfig.centroids.type << endl;
        auto centroids = createDataset(server, emConfig.centroids, onProgress2, true /*overwrite*/);

        Date applyDate = Date::now();

        for (unsigned i = 0;  i < em.clusters.size();  ++i) {
            auto & cluster = em.clusters[i];

            std::vector<std::tuple<ColumnName, CellValue, Date> > cols;

            for (unsigned j = 0;  j < cluster.centroid.size();  ++j) {
                cols.emplace_back(columnNames[j], cluster.centroid[j], applyDate);
            }

            auto flatmatrix = tovector(cluster.covarianceMatrix);

            for (unsigned j = 0;  j < flatmatrix.size();  ++j) {
                cols.emplace_back(ColumnName(ML::format("c%02d", j)), flatmatrix[j], applyDate);
            }
            
            centroids->recordRow(RowName(ML::format("%i", i)), cols);
        }
        
        centroids->commit();
    }   

    return Any();
}

DEFINE_STRUCTURE_DESCRIPTION(EMFunctionConfig);

EMFunctionConfigDescription::
EMFunctionConfigDescription()
{
    addField("centroids", &EMFunctionConfig::centroids,
             "Dataset containing centroids of each cluster: one row per cluster.");
    addField("select", &EMFunctionConfig::select,
             "Fields to select to calculate k-means over.  Only those fields "
             "that are selected here need to be matched.  Default is to use "
             "all fields.",
             SelectExpression("*"));
    addField("where", &EMFunctionConfig::where,
             "Rows to select for k-means training.  This will effectively "
             "limit which clusters are active.  Default is to use all "
             "clusters.",
             SqlExpression::parse("true"));
}


/*****************************************************************************/
/* EM FUNCTION                                                              */
/*****************************************************************************/

EMFunction::
EMFunction(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<EMFunctionConfig>();

    auto dataset = obtainDataset(server, functionConfig.centroids, onProgress);

    //cerr << "loading embedding" << endl;

    // Load up the embeddings
    auto embeddingOutput = getEmbedding(functionConfig.select, *dataset, "",                                        
                                        WhenExpression::parse("true"),
                                        functionConfig.where, {},
                                        -1, 
                                        ORDER_BY_NOTHING,
                                        0, -1,
                                        onProgress);
    
    // Each row is a cluster
    auto rows = embeddingOutput.first;
    std::vector<KnownColumn> & vars = embeddingOutput.second;

    for (auto & v: vars) {
        columnNames.push_back(v.columnName);
    }

    int numCol = columnNames.size();
    numDim = ((sqrt(1+4*numCol)) - 1) / 2;
    
    for (auto & r: rows) {
        Cluster cluster;
        cluster.clusterName = jsonDecodeStr<CellValue>(std::get<1>(r).toString());
       
        std::vector<double>& values = std::get<2>(r);
        for (int i = 0; i < numDim; ++i)
        {
            cluster.centroid.push_back(values[i]);
        }
        cluster.covarianceMatrix = Eigen::MatrixXd(numDim, numDim);
        for (int i = 0; i < numDim; ++i)
        {
            for (int j = 0; j < numDim; ++j)
            {
              cluster.covarianceMatrix(i,j) = values[numDim + i*numDim + j];
            }
        }

        clusters.emplace_back(std::move(cluster));
    }

    //cerr << "got " << clusters.size()
    //     << " clusters with " << columnNames.size()
    //     << "values" << endl;
}

Any
EMFunction::
getStatus() const
{
    return Any();
}

FunctionOutput
EMFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
    FunctionOutput result;

    ExpressionValue storage;
    const ExpressionValue & inputVal = context.get("embedding", storage);
    //cerr << "getting embedding" << endl;
    ML::distribution<float> input = inputVal.getEmbedding(numDim);
    Date ts = inputVal.getEffectiveTimestamp();

    double bestDist = INFINITY;
    CellValue bestCluster;

    for (unsigned i = 0;  i < clusters.size();  ++i) {
        double dist = 0.0f;
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
EMFunction::
getFunctionInfo() const
{
    FunctionInfo result;

    result.input.addEmbeddingValue("embedding", columnNames.size());
    result.output.addAtomValue("cluster");

    return result;
}


namespace {

RegisterProcedureType<EMProcedure, EMConfig>
regEM(builtinPackage(), "EM.train",
          "Estimation-Maximisation; Generic clustering algorithm based on making statistical models converge",
          "procedures/KmeansProcedure.md.html");

RegisterFunctionType<EMFunction, EMFunctionConfig>
regEMFunction(builtinPackage(), "EM",
               "Apply an Estimation-Maximization clustering to new data",
               "functions/Kmeans.md.html");

} // file scope

} // namespace MLDB
} // namespace Datacratic
