// -*- C++ -*-
// kmeans.h
// Simon Lemieux - 20 Jun 2013
// Copyright (c) 2013 mldb.ai inc. All rights reserved.
// 
// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#pragma once

#include <vector>
#include <mutex>
#include "mldb/jml/stats/distribution.h"
#include "mldb/jml/db/persistent.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/base/parallel.h"
#include <boost/math/special_functions/fpclassify.hpp>


namespace ML {

#define KMEANS_DEBUG 0


/*****************************************************************************/
/* METRICS FOR K-MEANS                                                       */
/*****************************************************************************/

class KMeansMetric {
public:
    // Distance between two points
    virtual double distance(const distribution<float> & x,
                            const distribution<float> & y) const = 0;

    // Computes the average of a set of points
    // This should correspond to the point that as the smallest
    // sum of `distance` between  all the points
    virtual distribution<float>
    average(const std::vector<distribution<float>> & points) const = 0;

    // Sometimes you just want to add the contribution of a point the some average
    virtual void contributeToAverage(distribution<float> & average,
                                     const distribution<float> & point, double weight) const = 0;

    // For serialization
    virtual std::string tag() const = 0;
};

class KMeansEuclideanMetric : public KMeansMetric {
public:
    double distance(const distribution<float> & x,
                    const distribution<float> & y) const
    { return (x - y).two_norm(); }

    distribution<float>
    average(const std::vector<distribution<float>> & points) const
    {
        distribution<float> avg(points[0].size(), 0.0);
        double weight = 1. / points.size();
        for (auto & x : points)
            contributeToAverage(avg, x, weight);
        return avg;
    }

    void contributeToAverage(distribution<float> & average,
                             const distribution<float> & point, double weight) const
    {
        // if [0,0,...,0], do not contribue to average
        if (point.any())
            average += point * weight; 
    }

    std::string tag() const { return "EuclideanMetric"; }
};

/*
 * Use the cosine distance (d(x,y) = cos(angle) = x/|x| dotprod y/|y|)
 * to compare two points.
 * For now the part where we do the average of the points in a
 * cluster is a bit hacky bit I think it does the trick
 *
 * If we have [0,0,...,0] kind of points, they should all end up in
 * one cluster.
 */

class KMeansCosineMetric : public KMeansMetric {
public:
    /* 
     * The distance has value in [-1, 1] U {2}.  It does not
     * define a metric but it has the property that if x1, x2 form an
     * angle smaller than y1, y2 then distance(x1, x2) < distance(y1, y2).
     */
    double distance(const distribution<float> & x,
                    const distribution<float> & y) const
    {
        bool x_zero = !x.any();
        bool y_zero = !y.any();
        if (x_zero && y_zero) {
            return -1.;
        } else if (x_zero || y_zero) {
            return 2.;
        } else
            return -x.dotprod(y) / y.two_norm() / x.two_norm();
    }

    // Not perfect but probably does the trick
    // Returns the (normalized) mean of the normalized points
    distribution<float>
    average(const std::vector<distribution<float>> & points) const
    {
        distribution<float> avg(points[0].size(), 0.0);
        double weight = 1. / points.size();
        for (auto & x : points)
            contributeToAverage(avg, x, weight);
        return avg/avg.two_norm();
    }

    void contributeToAverage(distribution<float> & average,
                             const distribution<float> & point,
                             double weight) const
    {
        if (point.any())
            average += point/point.two_norm() * weight; 
    }

    std::string tag() const { return "CosineMetric"; }
};


/*****************************************************************************/
/* KMEANS                                                                    */
/*****************************************************************************/

struct KMeans {

    KMeans(KMeansMetric * metric = new KMeansEuclideanMetric())
        : metric(metric)
    {
    }

    struct Cluster {
        int nbMembers;
        distribution<float> centroid;
    };

    std::vector<Cluster> clusters;
    std::shared_ptr<KMeansMetric> metric;
    
    void train(const std::vector<distribution<float> > & points,
               std::vector<int> & in_cluster,
               int nclusters=100,
               int maxIterations = 100,
               int randomSeed = 1);

    distribution<float> centroidDistances(const distribution<float> & point) const;

    // Find the closest cluster to `point` and returns its index
    int assign(const distribution<float> & point) const;

    void serialize(ML::DB::Store_Writer & store) const;
    void reconstitute(ML::DB::Store_Reader & store);
    void save(const std::string & filename) const;
    void load(const std::string & filename);
};

} // namespace ML
