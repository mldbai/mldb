/** em.cc
    Mathieu Marquis Bolduc, 30 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Implementation of the Estimation-Maximization algorithm.
*/

#include "em.h"

#include "mldb/arch/simd_vector.h"
#include "mldb/arch/math_builtins.h"

#include "mldb/base/exc_assert.h"

#include <random>
#include <mutex>

#include "mldb/ml/algebra/matrix_ops.h"
#include "mldb/ml/algebra/least_squares.h"
#include "mldb/types/jml_serialization.h"

using namespace std;
using namespace MLDB;

typedef boost::multi_array<double, 2> MatrixType;

namespace ML {

double distance(const distribution<double> & x,
                const distribution<double> & y)
{
    return (x - y).two_norm();
}

double gaussianDistance(const distribution<double> & pt,
                        const distribution<double> & origin,
                        const MatrixType& covarianceMatrix, 
                        const  MatrixType & invertCovarianceMatrix,
                        float determinant)
{
    auto xToU = pt - origin;
    auto variance = invertCovarianceMatrix * xToU ;

    double value_exponent = -0.5f*xToU.dotprod(variance);
    double determinantCovMatrix = determinant;

    if (determinantCovMatrix < 0)
      determinantCovMatrix = fabs(determinantCovMatrix);

    double distance
        = (1.0f / (pow(2.0f * 3.14159, pt.size() / 2.0f)
                   * sqrt(determinantCovMatrix))) 
        * exp(value_exponent);
    
    return distance;

}


boost::multi_array<double, 2>
EstimateCovariance(int i,
                   const std::vector<distribution<double>> & points, 
                   const  MatrixType& distanceMatrix,
                   double totalWeight,
                   distribution<double> average)
{
    boost::multi_array<double, 2> variant;

    if (totalWeight < 0.000001f)
      return variant;

    variant.resize(boost::extents[average.size()][average.size()]);
    for (int n = 0; n < distanceMatrix.shape()[0]; ++n)
    {
        distribution<double> pt = points[n] - average;
        double distance = distanceMatrix[n][i];
        int dim = average.size();
        MatrixType variantPt(boost::extents[dim][dim]);

	    for (unsigned i = 0;  i < dim;  ++i)
	    {
	        for (unsigned j = 0;  j < dim;  ++j)
	        {
	            variantPt[i][j] = pt[i]*pt[j] * distance;    
	        }
	    }

        if (n == 0) {                                    
            variant = std::move(variantPt);
        }
        else {
            variant = std::move(variant + variantPt);
        }
    }

    variant = variant / totalWeight;
    return variant;
}

void
EstimationMaximisation::
train(const std::vector<distribution<double>> & points,
      std::vector<int> & in_cluster,
      int nbClusters,
      int maxIterations,
      int randomSeed) 
{
    using namespace std;

    if (nbClusters < 2)
        throw MLDB::Exception("EM with less than 2 clusters doesn't make any sense!");

    mt19937 rng;
    rng.seed(randomSeed);

    int npoints = points.size();
    in_cluster.resize(npoints, -1);
    clusters.resize(nbClusters);

    boost::multi_array<double, 2> distanceMatrix
        (boost::extents[npoints][nbClusters]);

    // Smart initialization of the centroids
    // Same as Kmeans at the moment
    clusters[0].centroid = points[rng() % points.size()];
    int n = min(100, (int) points.size()/2);
    for (int i=1; i < nbClusters; ++i) {
        // This is our version of the wiki algorithm :
        // Amongst 100 random points, I take the farthest from the closest
        // centroid as the next centroid
        double distMax = -INFINITY;
        int bestPoint = -1;
        // We try it for 100 points
        for (int j=0; j < n; ++j) {
            // For a random point
            int randomIdx = rng() % points.size();
            // Find the closest cluster
            float distMin = INFINITY;
            // For each cluster
            for (int k=0; k < i; ++k) {

                double dist = distance(points[randomIdx], clusters[k].centroid);

                distanceMatrix[j][i] = dist;

                if (dist < distMin) {
                    distMin = dist;
                }
            }
            if (distMin > distMax) {
                distMax = distMin;
                bestPoint = randomIdx;
            }
        }
        if (bestPoint == -1) {
            bestPoint = rng() % points.size();
        }
        clusters[i].centroid = points[bestPoint];
    }

    int numdimensions = points[0].size();
    for (int i=0; i < nbClusters; ++i) {

        ML::setIdentity<double>(numdimensions, clusters[i].covarianceMatrix);
        clusters[i].invertCovarianceMatrix
            .resize(boost::extents[numdimensions][numdimensions]);
        clusters[i].invertCovarianceMatrix = clusters[i].covarianceMatrix;
        clusters[i].pseudoDeterminant = 1.0f; 
    }

    for (int iter = 0;  iter < maxIterations;  ++iter) {

        // How many have changed cluster?  Used to know when the cluster
        // contents are stable
        std::atomic<int> changes(0);

        //Step 1: assign each point to a distribution in the mixture

        auto findNewCluster = [&] (int i) {
            
            int best_cluster = this->assign(points[i], distanceMatrix, i);

            if (best_cluster != in_cluster[i]) {
                ++changes;
                in_cluster[i] = best_cluster;
            }
        };

        for (int i = 0; i < points.size(); ++i)
            findNewCluster(i);

        //Step 2: maximizing distribution's parameters 
        for (auto & c : clusters) {
            // If no member, we want to leave it there
            std::fill(c.centroid.begin(), c.centroid.end(), 0.0);
            c.totalWeight = 0.0f;
        }

        std::vector<std::mutex> locks(clusters.size());

        auto addToMeanForPoint = [&] (int i) {

            auto point = points[i];
            for (int cluster = 0; cluster < clusters.size(); ++cluster) {
                double distance = distanceMatrix[i][cluster];
                clusters[cluster].centroid += point * distance;
                clusters[cluster].totalWeight += distance;
            }
        };

        // calculate mean
        for (int i = 0; i < points.size(); ++i) {
            addToMeanForPoint(i);
        }

        //normalizeMean
        for (int cluster = 0; cluster < clusters.size(); ++cluster) {
            if (clusters[cluster].totalWeight > 0.000001f)
                {
                    clusters[cluster].centroid
                        = clusters[cluster].centroid
                        / clusters[cluster].totalWeight;
                }
        }
	     
        //calculate covariant matrix
        for (int i = 0; i < clusters.size(); ++i) {
            clusters[i].covarianceMatrix
                = EstimateCovariance(i, points, distanceMatrix,
                                     clusters[i].totalWeight,
                                     clusters[i].centroid);
            ExcAssertEqual(clusters[i].covarianceMatrix.shape()[0],
                           clusters[i].covarianceMatrix.shape()[1]);

            auto svdMatrix = clusters[i].covarianceMatrix;
            MatrixType VT,U;
            distribution<double> svalues;
            ML::svd_square(svdMatrix, VT, U, svalues);

            //Remove small values and calculate pseudo determinant
            double pseudoDeterminant = 1.0f;
            auto invertSingularValues = svalues;
            for (int i = 0; i < svalues.size(); ++i) {

                if (svalues[i] < 0.0001f) {
                    svalues[i] = 0.0f;
                    invertSingularValues[i] = 0.0f;
                }
                else {
                    pseudoDeterminant *= svalues[i];
                    invertSingularValues[i] = 1.0f / svalues[i];
                }
            }

            // calculate pseudo inverse and pseudo determinant

            // We dont actually need the pseudo covariant but it sould look
            // like this
            // MatrixType pseudoCovariant = U * diag(svalues) * VT;
            
            clusters[i].invertCovarianceMatrix
                = transpose(VT) * diag(invertSingularValues) * transpose(U);
            clusters[i].pseudoDeterminant = pseudoDeterminant;
        }
    }
}

int
EstimationMaximisation::
assign(const distribution<double> & point) const
{
    boost::multi_array<double, 2> dummySoftAssignMatrix;
    return assign(point, dummySoftAssignMatrix, -1);
}

int
EstimationMaximisation::
assign(const distribution<double> & point,
       boost::multi_array<double, 2>& distanceMatrix,
       int pIndex) const
{
    using namespace std;
    if (clusters.size() == 0)
        throw MLDB::Exception("Did you train your em?");

    distribution<double> distances(clusters.size());
    for (int i=0; i < clusters.size(); ++i) {
        distances[i]
            = gaussianDistance(point, clusters[i].centroid,
                               clusters[i].covarianceMatrix,
                               clusters[i].invertCovarianceMatrix,
                               clusters[i].pseudoDeterminant);
    }

    double distMin = 0;
    int best_cluster = -1;
    double totalWeight = 0.0f;

    for (int i=0; i < clusters.size(); ++i) {

        double distance = distances[i];

        if (pIndex >= 0) {
            totalWeight += distance;
            distanceMatrix[pIndex][i] = distance;
        }	    
        
        if (distances[i] > distMin) {
            distMin = distances[i];
            best_cluster = i;
        }
    }

    if (pIndex >= 0 && totalWeight > 0) {
        for (int i=0; i < clusters.size(); ++i) {
            distanceMatrix[pIndex][i] /= totalWeight;
        }
    }

    // Most likely these are points with all distance at 0, often during the
    // first iteration
    if (best_cluster == -1)
        best_cluster = 0;
    
    return best_cluster;
}

void
EstimationMaximisation::
serialize(ML::DB::Store_Writer & store) const
{
    std::string name = "em";
    int version = 1;
    store << name << version;
    store << (int) clusters.size();
    for (auto & c : clusters) {
        store << c.totalWeight;
        store << c.centroid;
        store << c.covarianceMatrix;
        store << c.invertCovarianceMatrix;
        store << c.pseudoDeterminant;
    }
    store << columnNames;
}


void
EstimationMaximisation::
reconstitute(ML::DB::Store_Reader & store)
{
    std::string name;
    store >> name;
    if (name != "em")
        throw MLDB::Exception("invalid name when loading a EM object");  
    int version;
    store >> version;
    if (version != 1)
        throw MLDB::Exception("invalid EM version");
    int nbClusters;
    store >> nbClusters;
    clusters.clear();
    clusters.resize(nbClusters);

    for (int i=0; i < nbClusters; ++i) {
        store >> clusters[i].totalWeight;
        store >> clusters[i].centroid;
        store >> clusters[i].covarianceMatrix;
        store >> clusters[i].invertCovarianceMatrix;
        store >> clusters[i].pseudoDeterminant;
    }    

    store >> columnNames;
}


void
EstimationMaximisation::
save(const std::string & filename) const
{
    filter_ostream stream(filename);
    DB::Store_Writer store(stream);
    serialize(store);
}

void
EstimationMaximisation::
load(const std::string & filename)
{
    filter_istream stream(filename);
    DB::Store_Reader store(stream);
    reconstitute(store);
}


} //ML
