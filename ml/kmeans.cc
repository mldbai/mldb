/** kmeans.cc
    Jeremy Barnes, 31 January 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Implementation of the k-means algorithm.
*/

#include "kmeans.h"

#include <random>
#include "mldb/jml/utils/smart_ptr_utils.h"

namespace ML {

void
KMeans::
train(const std::vector<distribution<float>> & points,
      std::vector<int> & in_cluster,
      int nbClusters,
      int maxIterations,
      int randomSeed
      )
{
    using namespace std;

    if (nbClusters < 2)
        throw MLDB::Exception("kmeans training requires at least 2 clusters");
    if (points.size() == 0)
        throw MLDB::Exception("kmeans training requires at least 1 datapoint");

    mt19937 rng;
    rng.seed(randomSeed);

    int npoints = points.size();
    in_cluster.resize(npoints, -1);
    clusters.resize(nbClusters);

    // Smart initialization of the centroids
    // FIXME http://en.wikipedia.org/wiki/K-means%2B%2B#Initialization_algorithm
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

                float dist = metric->distance(points[randomIdx], clusters[k].centroid);

                if (dist < distMin) {
                    distMin = dist;
                    // cerr << "NEW MIN" << endl;
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
            cerr << "kmeans initialization failed for centroid [" << i << "]" << endl;
            bestPoint = rng() % points.size();
        }
        clusters[i].centroid = points[bestPoint];
        // cerr << "norm of best init centroid " << clusters[i].centroid.two_norm() << endl;
    }


    for (int iter = 0;  iter < maxIterations;  ++iter) {

        // How many have changed cluster?  Used to know when the cluster
        // contents are stable
        std::atomic<int> changes(0);

        // Unfortunately, std::atomic can't be copied or moved, so we need
        // a wrapper to put it in a vector
        struct AI: public std::atomic<int> {
            AI(int n = 0)
                : std::atomic<int>(n)
            {
            }

            AI & operator = (const AI & other) noexcept
            {
                store(other.load());
                return *this;
            }
        };

        std::vector<AI> clusterNumMembers(nbClusters);

        auto findNewCluster = [&] (int i) {

            int best_cluster = this->assign(points[i]);

            if (best_cluster != in_cluster[i]) {
                ++changes;
                in_cluster[i] = best_cluster;
            }

            ++clusterNumMembers[best_cluster];
        };

        MLDB::parallelMap(0, points.size(), findNewCluster);

        for (unsigned i = 0;  i < nbClusters;  ++i)
            clusters[i].nbMembers = clusterNumMembers[i];

#if KMEANS_DEBUG
        auto printDebug = [&] (const string & step, int iter) {
            filter_ostream stream(MLDB::format("kmeans_debug_%i_%s.csv", iter, step));
            stream << "x,y,group,type\n";

            for (int i=0; i < clusters.size(); ++i) {
                auto & cluster = clusters[i];
                stream << cluster.centroid[0] << ","
                             << cluster.centroid[1] << ","
                             << i << ",centroid\n";
            }
            for (int i=0; i< npoints; ++i)
                stream << points[i][0] << ","
                              << points[i][1] << ","
                              << in_cluster[i] << ",point\n";
        };
        printDebug("assoc", iter);
#endif

        // std::cerr << "\niter " << iter << std::endl;

        // Calculate means
        for (auto & c : clusters)
            // If no member, we want to leave it there
            if (c.nbMembers > 0)
                std::fill(c.centroid.begin(), c.centroid.end(), 0.0);

        std::vector<std::mutex> locks(clusters.size());

        auto addToMeanForPoint = [&] (int i) {
            int cluster = in_cluster[i];
            auto point = points[i];

            {
                // cerr << "cluster du point i " << cluster << endl;
                std::unique_lock<std::mutex> guard(locks[cluster]);
                // cerr << "patate pour mich" << endl;
                metric->contributeToAverage(clusters[cluster].centroid, point, 1. / (double) clusters[cluster].nbMembers);
            }
        };

        MLDB::parallelMap(0, points.size(), addToMeanForPoint);

        // for (int i=0; i < clusters.size(); ++i) {
            // cerr << "cluster " << i << " had " << clusters[i].nbMembers
                 // << " members\n";
            // cerr << "centroid " << clusters[i].centroid << endl;
        // }

        cerr << "done clustering iter " << iter
             << ": " << changes << " changes" << endl;

        cerr << "nb of items per cluster" << endl << "[ ";
        for (auto & c : clusters)
            cerr << c.nbMembers << " ";
        cerr << "]" << endl;

        if (changes == 0)
            break;
        else
            for (auto & c : clusters)
                c.nbMembers = 0;


#if KMEANS_DEBUG
        printDebug("average", iter);
#endif

    }
}

distribution<float>
KMeans::
centroidDistances(const distribution<float> & point) const
{
    distribution<float> distances(clusters.size());
    for (int i=0; i < clusters.size(); ++i) {
        distances[i] = metric->distance(point, clusters[i].centroid);
    }
    return distances;
}

int
KMeans::
assign(const distribution<float> & point) const
{
    using namespace std;
    if (clusters.size() == 0)
        throw MLDB::Exception("Did you train your kmeans?");

    float distMin = INFINITY;
    int best_cluster = -1;
    auto distances = centroidDistances(point);
    for (int i=0; i < clusters.size(); ++i) {
        if (distances[i] < distMin) {
            distMin = distances[i];
            best_cluster = i;
        }
    }
    // Those are points with infinty distance or nan distance maybe
    // Let's put them in cluster 0
    if (best_cluster == -1) {
        cerr << MLDB::format("something went wrong when assigning this vector with norm [%f]",
               point.two_norm()) << endl;
        best_cluster = 0;
    }
    return best_cluster;

}

void
KMeans::
serialize(ML::DB::Store_Writer & store) const
{
    std::string name = "kmeans";
    int version = 0;
    store << name << metric->tag() << version;
    store << (int) clusters.size();
    for (auto & c : clusters) {
        // even though this is only useful while training...
        store << c.nbMembers; 
        store << c.centroid;
    }
}

void
KMeans::
reconstitute(ML::DB::Store_Reader & store)
{
    std::string name;
    store >> name;
    if (name != "kmeans")
        throw MLDB::Exception("invalid name when loading a KMeans object");
    std::string metricTag;
    store >> metricTag;
    if (metricTag != metric->tag()) {
        if (metricTag == "CosineMetric")
            metric.reset(new KMeansCosineMetric());
        else if (metricTag == "EuclideanMetric")
            metric.reset(new KMeansEuclideanMetric());
        else throw MLDB::Exception("unknown metric tag: tag = %s, current = %s",
                            metricTag.c_str(), metric->tag().c_str());
    }
    int version;
    store >> version;
    if (version != 0)
        throw MLDB::Exception("invalid KMeans version");
    int nbClusters;
    store >> nbClusters;
    clusters.clear();
    clusters.resize(nbClusters);
    for (int i=0; i < nbClusters; ++i) {
        store >> clusters[i].nbMembers;
        store >> clusters[i].centroid;
    }
}

void
KMeans::
save(const std::string & filename) const
{
    MLDB::filter_ostream stream(filename);
    DB::Store_Writer store(stream);
    serialize(store);
}

void
KMeans::
load(const std::string & filename)
{
    MLDB::filter_istream stream(filename);
    DB::Store_Reader store(stream);
    reconstitute(store);
}


} // namespace ML
