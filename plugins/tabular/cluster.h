/* bit_compressed_bit_table.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once
#include "predictor.h"
#include "mldb/utils/min_max.h"
#include <compare>
#include <span>
#include <vector>
#include <functional>

namespace MLDB {

struct Point {
    uint32_t x;
    uint32_t y;

    auto operator <=> (const Point & other) const = default;
};

struct Cluster: public Predictor {
    // Label of the cluster, useful for debugging
    std::string label;

    // Parent label
    std::string parent;

    // In which iteration was this cluster created?
    int iter = -1;

    // These are updated by adding points and reset(), ie they measure properties of the points
    std::vector<Point> points = {};
    std::vector<int64_t> residuals;
    MinMaxIndex<int64_t> residualStats;  // stats about residuals
    MinMaxIndex<size_t> indexStats;      // stats about x
    MinMaxIndex<uint32_t> valueStats;    // stats about y

    // These are updated by shrinkToFit() or methods like that; they are cluster parameters
    // that describe the tightness of the cluster fit.
    uint64_t desiredResidualRange_ = MAX_LIMIT;  // residualRange() <= desiredResidualRange_; When we have no min/max residual, we use this one
    int64_t desiredMinResidual = MAX_LIMIT;
    int64_t desiredMaxResidual = MIN_LIMIT;

    void reset();

    void shrinkToFit();

    void broaden();

    float bitsRequiredForResidual() const;

    bool fitsInCluster(int64_t residual) const;

    bool couldFitInCluster(int64_t residual) const;

    //int64_t bitsAddedToContainResidual(int64_t residual);

    bool empty() const
    {
        return points.empty();
    }

    size_t size() const
    {
        return points.size();
    }

    void reserve(size_t size);

    void addPoint(Point point)
    {
        addPoint(point.x, point.y);
    }

    void addPoint(uint32_t x, uint32_t y);

    void addPoints(std::span<const Point> points);

    double density() const
    {
        return 1.0 * points.size() / (indexStats.range() + 1);
    }

    bool fit();

    bool quickFit();

    // Internal function used by fit().  Given a new set of parameters, this
    // commits them if they are viable and updates the statistics for the
    // new fit (and returns true).  If they are not viable, for example the
    // residual range increases or residuals are out of bounds, it does
    // nothing and returns false.
    bool commitParams(const std::array<float, BASIS_DIMS> & newParams);

    using ScoreFn = std::function<int64_t (const Cluster & oldCluster, const std::span<const Cluster> & newClusters)>;

    // Default scoring function that scores clusters by memory usage
    static int64_t scoreByMemoryUsage(const Cluster & oldCluster, const std::span<const Cluster> & newClusters);

    struct PiecewiseState;

    std::array<Cluster, 2> splitOnIndex(size_t index) const;

    std::vector<std::pair<int64_t, std::array<Cluster, 2>>>
    bestSplitPoints(const ScoreFn & scorer, const std::vector<uint32_t> & candidates,
                    size_t maxDesired) const;

    std::vector<Cluster> fitPiecewise(size_t desiredResidualRange,
                                      ScoreFn scorer = scoreByMemoryUsage,
                                      bool trace = false,
                                      PiecewiseState * state = nullptr,
                                      size_t maxRecursion = 8, size_t recursion = 0) const;

    std::vector<Cluster> optimize(ScoreFn scorer = scoreByMemoryUsage,
                                  bool trace = false,
                                  PiecewiseState * state = nullptr,
                                  size_t maxRecursion = 8, size_t recursion = 0) const;

    std::vector<Cluster> removeOutliers(double maxDeviations, bool trace = false) const;

    void commitSplit(std::vector<Cluster> splitClusters, std::vector<Cluster> & clusterList);

    // Current range of residuals
    uint64_t residualRange() const
    {
        return residualStats.range();
    }

    uint32_t desiredResidualRange() const
    {
        return desiredResidualRange_;
    }

    // Maximum range we can have without needing more storage (ie, for free)
    uint64_t maxResidualRange() const;

    int numBits() const;

    uint64_t storageBits() const;

    static uint64_t storageBits(const std::span<const Cluster> & clusters);

    double storageBitsPerPoint() const;

    static std::ostream & printHeaders(std::ostream & stream, const std::string & indent = "");

    std::string print(size_t index) const;

    static void printList(std::ostream & stream, const std::span<const Cluster> & clusters);
    static void printSplit(std::ostream & stream, const Cluster & original, const std::span<const Cluster> & split);
};

// Helper class used to score & apply splits of a cluster
struct ClusterSplitter {

    using ScoreFn = Cluster::ScoreFn;

    ClusterSplitter(const Cluster & parent, ScoreFn scoreFn = Cluster::scoreByMemoryUsage);

    // Function used to decide which split each point goes into
    using SplitFn = std::function<size_t (uint32_t x, uint32_t y, int64_t residual)>;

    // Try the split, returning the clusters and score
    std::pair<std::vector<Cluster>, int64_t> split(SplitFn splitFn, const char * splitType, int param, int iteration) const;
    
    // Try the split & keep the result if it's the best split so far
    int64_t testSplit(SplitFn split, const char * splitType, int param, int iteration);

    // Apply the best split
    void commit(Cluster & parent, std::vector<Cluster> & others);

    // Split into two on the x value
    static SplitFn xSplitter(uint32_t splitX);  // x < splitX
 
    // Split into two on the y value
    static SplitFn ySplitter(uint32_t splitY);  // y < splitY

    // Split into two on the residual value
    static SplitFn residualSplitter(int64_t minResidual);  // residual < minResidual; careful of boundary

    // Split into (below, within, above) on the residual value
    static SplitFn residualRangeSplitter(int64_t minResidual, int64_t maxResidual);  // residual < minresidual; residual > maxresidual
                    
    const Cluster * parent = nullptr;
    ScoreFn scoreFn;
    std::vector<Cluster> bestClusters;
    int64_t bestScore = 1;
    const char * bestSplitType = "none";
    int64_t bestParam = -1;
    bool trace = false;
};

} // namespace MLDB
