
/** em.cc
    Mathieu Marquis Bolduc, 30 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Implementation of the Estimation-Maximization algorithm.
*/

#include "em.h"

#include "mldb/arch/simd_vector.h"
#include "mldb/arch/atomic_ops.h"
#include "mldb/arch/math_builtins.h"

#include "mldb/base/exc_assert.h"

#include <boost/random/uniform_int.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <mutex>

#include "mldb/ml/Eigen/SVD"

using namespace std;

namespace ML {

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

void
EstimationMaximisation::
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
EstimationMaximisation::
assign(const ML::distribution<float> & point, Eigen::MatrixXd& distanceMatrix, int pIndex) const
{
  using namespace std;
  if (clusters.size() == 0)
      throw ML::Exception("Did you train your em?");

  ML::distribution<float> distances(clusters.size());
  for (int i=0; i < clusters.size(); ++i) {
      distances[i] = gaussianDistance(point, clusters[i].centroid, clusters[i].covarianceMatrix, clusters[i].invertCovarianceMatrix, clusters[i].pseudoDeterminant);
  }

  float distMin = 0;
  int best_cluster = -1;
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

} //ML
