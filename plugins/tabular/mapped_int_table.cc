/* mapped_int_table.cc                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "mapped_int_table.h"
#include "cluster.h"
#include "mldb/vfs/compressibility.h"
#include "mldb/utils/ostream_array.h"
#include "mldb/arch/ansi.h"
#include "mldb/utils/min_max.h"
#include "mldb/utils/bits.h"
#include "mldb/base/exc_assert.h"
#include <set>
#include <fstream>

namespace MLDB {

using namespace std;
using namespace ansi;

MappingOption<int> INT_TABLE_TRACE_LEVEL("INT_TABLE_TRACE_LEVEL", 0);

// Levels:
// 1. Linear predictive (offset + slope)
// 2. (potentially a fitted model using other bases eg exponential and polynomial bases)
// 3. Lookup table
// 4. Run length encoding
// 5. Bit packing

// This stage will take an int table, and encode it in a form with:
// - A lookup table, with numLookups of the lowest values being pointers into that table
// - An offset and slope, to predict the values of the rest and create an residual table with a minimum number of bits
// - A set of residual values, one per item 

struct IntTableAnalyzer {
    using Int = uint32_t;
    static constexpr int IntBits = sizeof(Int) * 8;

    bool trace = false;

    size_t i = 0;
    Int minIntVal = MAX_LIMIT;
    Int maxIntVal = MIN_LIMIT;
    Int lastIntVal = MIN_LIMIT;
    bool monotonic = true;

    struct Slope {
        int numerator = 0;
        int denominator = 1;
        uint32_t element = -1;
        uint32_t value = 0;

        operator double() const { return 1.0 * numerator / denominator; }
    };

    // Previous values, used to sample the slope from
    std::vector<Int> values;

    // Sample the slopes to get the median slope.  This is an approximation of the Theil-Sen
    // estimator, which is robust to outliers.
    std::vector<Slope> slopes;

    // Look for repeated values, which could be encoded by a value table
    std::map<Int, int> valueCounts;
    int maxValueCount = 0;

    // Look for runs of values, which can be encoded with run length encoding
    int runLength = 0;
    int numRuns = 0;
    int maxRunLength = 0;

    float slope;

    int32_t minOffset = MAX_LIMIT;
    int32_t maxOffset = MIN_LIMIT;

    template<typename T>
    void analyzeOne(T el)
    {
        Int intVal = to_int(el);
        analyzeOne(intVal);
    }

    template<typename InputIterator>
    void analyze(InputIterator first, InputIterator last)
    {
        for (; first != last;  ++first) {
            analyzeOne(*first);
        }
    }

    void analyzeOne(Int intVal)
    {
        values.push_back(intVal);

        maxValueCount = std::max(maxValueCount, ++valueCounts[intVal]);
        
        minIntVal = std::min(minIntVal, intVal);
        maxIntVal = std::max(maxIntVal, intVal);
        monotonic = monotonic && intVal >= lastIntVal;
        if (i != 0) {
            if (lastIntVal == intVal) {
                // run continues
                ++runLength;
            }
            else {
                // run has finished
                maxRunLength = std::max(maxRunLength, runLength);
                runLength = 1;
                ++numRuns;
            }
        }

        lastIntVal = intVal;

        // Choose a random point to sample the slope from
        if (i > 0) {

            auto sample_point = i / 2;
            if (sample_point != i) {
                int denominator = i - sample_point;
                int numerator = intVal - to_int(values.at(sample_point));
                slopes.emplace_back(Slope{numerator, denominator, uint32_t(i), intVal});
            }
        }

        ++i;
    }

    // A range with a fixed number of points
    struct Range {
        uint32_t rangeStart;
        std::vector<uint32_t> predictorIndexes;
        std::vector<uint32_t> selectors;
        std::vector<uint32_t> residuals;
        size_t numPoints() const { return residuals.size(); }
    };

    // Cluster points
    auto clusterPoints(int numClusters, bool trace) -> std::tuple<std::vector<Range>, std::vector<Predictor>>
    {
        maxRunLength = std::max(maxRunLength, runLength);
        ++numRuns;

        std::sort(slopes.begin(), slopes.end());

        if (values.size() <= 1) {
            if (values.empty()) {
                return { {}, {} };
            }
            Predictor predictor;
            predictor.offset -= values[0];
            Range range{0, {0}, {0}, {0} };
            return { { range  }, { predictor } };
        }

        // 1.  Sample the slopes array, taking a sweep of the different values, to seed our clusters
        std::vector<Cluster> clusters;

        auto seedClusters = [&] ()
        {
            for (size_t i = 0;  i < slopes.size();  i += std::max<size_t>(1, slopes.size() / numClusters)) {
                auto [numerator, denominator, element, value] = slopes[i];
                //ExcAssert(element2 > denominator);
                //uint32_t element2 = element2 - denominator;
                Cluster cluster;
                cluster.label = "seed slope " + std::to_string(numerator/denominator);
                cluster.iter = -1;

                // Limit slope so we can't go outside the range representable in an int32_t
                constexpr float maxSlope = std::numeric_limits<float>::max() / (float)std::numeric_limits<uint32_t>::max();
                cluster.slope() = std::clamp(1.0f * numerator / denominator, -maxSlope, maxSlope);

                cluster.intercept() = value - (cluster.slope() * element);
                cluster.offset = cluster.predict(element) - value;

                //cerr << "  cluster seed " << i << ": slope " << cluster.slope << " intercept " << cluster.intercept << endl;
                if (cluster.predict(element) != value) {
                    cerr << "numerator " << numerator << endl;
                    cerr << "denominator " << denominator << endl;
                    cerr << "slope " << cluster.slope() << endl;
                    cerr << "intercept " << cluster.intercept() << endl;
                    cerr << "element " << element << endl;
                    cerr << "value " << value << endl;
                    cerr << "offset " << cluster.offset << endl;
                    cerr << "cluster.predict() " << cluster.predict(element) << endl;
                }
                ExcAssert(cluster.predict(element) == value);
                clusters.emplace_back(std::move(cluster));
            }

            // Create clusters with the oft-repeated values
            std::vector<std::pair<uint32_t, uint32_t> > valueCountsSorted;
            valueCountsSorted.reserve(valueCounts.size());
            for (auto [value, count]: valueCounts) {
                valueCountsSorted.emplace_back(count, value);
            }
            std::sort(valueCountsSorted.rbegin(), valueCountsSorted.rend());

            for (size_t i = 0;  i < valueCounts.size();  ++i) {
                auto [count, value] = valueCountsSorted[i];
                if (count <= 2) break;
                if (count <= values.size() / 128) break;
                
                Cluster cluster;
                cluster.label = "common value " + std::to_string(value) + " with count " + std::to_string(count);
                cluster.iter = -1;
                cluster.slope() = 0;
                cluster.intercept() = value;
                clusters.emplace_back(std::move(cluster));
            }
        };

        //std::vector<uint32_t> assignedClusters;
        //std::vector<int64_t> assignedResiduals;

        auto assignPoints = [&] (bool greedy, int iteration) -> std::vector<std::pair<size_t, uint32_t>>
        {
            //assignedClusters.resize(values.size(), -1);
            //assignedResiduals.resize(values.size(), -1);

            // Now remove points from clusters to re-assign them
            for (size_t i = 0;  i < clusters.size();  ++i) {
                clusters[i].reset();
            }

            // (residual, point number)
            std::vector<std::pair<size_t, uint32_t> > pointResiduals;

            // Which points don't fit?
            std::vector<uint32_t> unassignedPoints;

            // Which points are complete outliers?
            std::vector<uint32_t> outliers;

            // 2.  Assign each point to a cluster.  First, we do those that fit inside an existing
            //     residual profile.  We then go back and deal with those which don't fit in any.
            for (uint32_t i = 0;  i < values.size();  ++i) {
                int bestCluster = -1;
                int64_t bestResidual = MAX_LIMIT;

                //auto oldResidual = assignedResiduals[i];

                for (size_t j = 0;  j < clusters.size();  ++j) {
                    int64_t prediction = clusters[j].predict(i);
                    int64_t residual = prediction - values[i];

                    //cerr << "cluster " << j << " residual " << residual << " maxResidualRange "
                    //     << " min residual " << clusters[j].desiredMinResidual << " max residual " << clusters[j].desiredMaxResidual
                    //     << " residual range" << clusters[j].maxResidualRange() << endl;

                    if (clusters[j].fitsInCluster(residual) && abs(residual) < abs(bestResidual)) {
                        bestResidual = residual;
                        bestCluster = j;
                        // In greedy mode, we put it in the first cluster it will fit inside to try
                        // to limit the number of clusters.
                        if (greedy)
                            break;
                    }
                }

                if (bestCluster == -1) {
                    unassignedPoints.push_back(i);
                    continue;
                }                    

                auto statsBefore = clusters[bestCluster].residualStats;
                ExcAssert(clusters[bestCluster].residualRange() <= std::numeric_limits<uint32_t>::max());
                clusters[bestCluster].addPoint(i, values[i]);
                if (clusters[bestCluster].residualRange() > std::numeric_limits<uint32_t>::max()) {
                    cerr << "residual out of range" << endl;
                    cerr << "desiredMinResidual = " << clusters[bestCluster].desiredMinResidual << endl;
                    cerr << "desiredMaxResidual = " << clusters[bestCluster].desiredMaxResidual << endl;
                    cerr << "desiredResidualRange = " << clusters[bestCluster].desiredResidualRange() << endl;
                    cerr << "statsBefore = " << statsBefore << endl;
                    cerr << "statsAfter = " << clusters[bestCluster].residualStats << endl;
                }
                ExcAssert(clusters[bestCluster].residualRange() <= std::numeric_limits<uint32_t>::max());

                if (abs(bestResidual) > 0) {
                    pointResiduals.emplace_back(abs(bestResidual), i);
                }
            }

            if (trace)
                cerr << "doing " << unassignedPoints.size() << " of " << values.size() << " unassigned points" << endl;

            for (auto i: unassignedPoints) {
                // Assigning this point will cause an expansion in memory use of the containing
                // cluster, so we put it in the one which causes the least expansion.

                int bestCluster = -1;
                int64_t bestCost = MAX_LIMIT;
                int64_t bestResidual = 0;

                // See if we can fit it without expanding
                for (size_t j = 0;  j < clusters.size();  ++j) {
                    int64_t prediction = clusters[j].predict(i);
                    int64_t residual = prediction - values[i];
                    if (clusters[j].couldFitInCluster(residual)) {
                        bestCluster = j;
                        bestCost = 0;
                        bestResidual = residual;
                        break;
                    }
                }

#if 0
                // if it's not possible to fit without expanding, make them outlierschoose one to expand
                if (bestCluster == -1) {
                    for (size_t j = 0;  j < clusters.size();  ++j) {
                        int64_t prediction = clusters[j].predict(i);
                        int64_t residual = prediction - values[i];

                        int64_t cost = clusters[j].bitsAddedToContainResidual(residual);
                        //cerr << "  to reassign point " << i << " to cluster " << j << " with residual " << residual << " gives cost " << cost << endl;
                        if (cost < bestCost) {
                            bestCluster = j;
                            bestCost = cost;
                            bestResidual = residual;
                        }
                    }
                }
#endif

                if (bestCluster == -1) {
                    outliers.push_back(i);
                    continue;
                }

                if (bestCost != 0) {
                    cerr << "unassigned point " << i << ": best is to expand cluster " << bestCluster
                         << " from range " << clusters[bestCluster].maxResidualRange() << " to "
                         << (std::max<int64_t>(clusters[bestCluster].residualStats.maxValue, bestResidual)
                             - std::min<int64_t>(clusters[bestCluster].residualStats.minValue, bestResidual))
                         << " incurring a cost of " << bestCost << " over " << clusters[bestCluster].points.size() << endl;
                }

                ExcAssert(bestCluster != -1);
                ExcAssert(clusters[bestCluster].residualRange() <= std::numeric_limits<uint32_t>::max());
                clusters[bestCluster].addPoint(i, values[i]);
                ExcAssert(clusters[bestCluster].residualRange() <= std::numeric_limits<uint32_t>::max());
                pointResiduals.emplace_back(abs(bestResidual), i);
            }

            if (outliers.size() > 0) {
                if (trace)
                    cerr << "got " << outliers.size() << " outliers" << endl;
                // Put them in one cluster and see how that goes...

                Cluster outlierCluster;
                outlierCluster.label = std::to_string(outliers.size()) + " outliers";
                outlierCluster.iter = iteration;
                for (auto x: outliers) {
                    outlierCluster.addPoint(x, values[x]);
                }

                outlierCluster.fit();
                if (trace)
                    cerr << "outlier cluster: points " << outlierCluster.points.size()
                        << " params " << outlierCluster.params << " range " << outlierCluster.residualRange() << " bits "
                        << outlierCluster.storageBitsPerPoint() << endl;
                outlierCluster.shrinkToFit();

                clusters.emplace_back(std::move(outlierCluster));
            }

            for (auto & c: clusters) {
                std::sort(c.points.begin(), c.points.end());
            }

            return pointResiduals;
        };

#if 0
        auto splitClusters = [&] (int iteration)
        {
            // Split clusters that lead to high residual counts
            int highestResidualBits = 0;
            for (const Cluster & c: clusters) {
                highestResidualBits = std::max(highestResidualBits, c.numBits());
            }
            cerr << "highest residual bits is " << highestResidualBits << endl;

            std::vector<Cluster> newSplitClusters;

            for (Cluster & c: clusters) {
                //if (c.numBits() < highestResidualBits) continue;
                if (c.numBits() == 0)
                    continue;

                ClusterSplitter splitter(c);

                // Split the cluster in a few ways:
                // 1. Make two parallel predictions with an offset so that 1/2 of the residuals will fit
                //    into each one
                // 2. Try both a maximum and minimum sloped line

                // Are there outliers?
                std::map<int64_t, size_t> residualCounts;
                std::vector<int64_t> residuals;
                residuals.reserve(c.points.size());
                for (auto [x,y]: c.points) {
                    int64_t residual = c.predict(x) - y;
                    residuals.push_back(residual);
                    residualCounts[residual] += 1;
                }

                auto range = c.residualStats.maxValue - c.residualStats.minValue;

                FactorPackSolution thisSolution = get_factor_packing_solution(range);
                int64_t newRange = get_previous_factor(thisSolution.factor);

                //int64_t neededRange = range - prevRange;
                //ExcAssert(neededRange >= 0);

                int64_t neededMax = c.residualStats.minValue + newRange - 1;
                int64_t neededMin = c.residualStats.maxValue - newRange + 1;

                size_t maxCount = 0, minCount = 0;
                int64_t newMax = MIN_LIMIT;
                int64_t newMin = MAX_LIMIT;

                for (auto [residual, count]: residualCounts) {
                    if (residual > neededMax) {
                        maxCount += count;
                    }
                    else newMax = std::max<int64_t>(residual, newMax);

                    if (residual < neededMin) {
                        minCount += count;
                    }
                    else newMin = std::min<int64_t>(residual, newMin);
                }

                int64_t bitsBefore = c.storageBits();
                int64_t bitsMax = raw_mapped_indirect_bytes(c.points.size() - maxCount, newMax - c.residualStats.minValue)*8 + maxCount * 16;
                int64_t bitsMin = raw_mapped_indirect_bytes(c.points.size() - minCount, c.residualStats.maxValue - newMin)*8 + minCount * 16;

                if (false && (bitsMax < bitsBefore || bitsMin < bitsBefore)) {
                    cerr << " outlier analysis for " << c.points.size() << " points: neededRange " << newRange << " range " << range << ": removing " << maxCount << " from top or " << minCount
                         << " from bottom; bits before " << bitsBefore << " saved top " << bitsBefore - bitsMax
                         << " saved bottom " << bitsBefore - bitsMin << endl;
                    if (bitsMax < bitsMin) {
                        auto splitMax = [neededMax] (uint32_t, uint32_t, int64_t residual) -> uint32_t
                        {
                            return residual > neededMax;
                        };
                        splitter.testSplit(splitMax, "maxOutliers", neededMax, iteration);
                    }
                    else {
                        auto splitMax = [neededMin] (uint32_t, uint32_t, int64_t residual) -> uint32_t
                        {
                            return residual < neededMin;
                        };
                        splitter.testSplit(splitMax, "minOutliers", neededMin, iteration);
                    }
                }

                if (c.points.size() < 64) {
                    splitter.commit(c, newSplitClusters);
                    continue;
                }

                std::vector<int64_t> dresidual, d2residual;
                std::vector<std::pair<uint32_t, uint32_t> > d2residual_sorted;  // abs(residual), position

                if (c.points.size() > 16 && highestResidualBits > 1 && c.numBits() >= highestResidualBits - 2) {
#if 0                    
                    dresidual.reserve(c.points.size() - 1);
                    d2residual.reserve(c.points.size() - 3);
                    for (size_t i = 1;  i < c.points.size() - 1;  ++i) {
                        dresidual.push_back(residuals[i - 1] - residuals[i]);
                        if (i > 1) {
                            d2residual.push_back(dresidual[i - 1] - dresidual[i - 2]);
                            d2residual_sorted.emplace_back(abs(d2residual.back()), i);
                        }
                    }

                    std::sort(d2residual_sorted.rbegin(), d2residual_sorted.rend());

                    auto median_d2residual = d2residual_sorted[d2residual_sorted.size() / 2].first;

                    //cerr << "median d2residual " << median_d2residual << " min " << d2residual_sorted.back().first
                    //     << " max " << d2residual_sorted.front().first << endl;
#endif

                    // TODO: better determine important points...
                    for (unsigned i = 0;  i < c.points.size();  i += 4) {
                        auto x = c.points[i].x;
                        splitter.testSplit(ClusterSplitter::xSplitter(x), "x", x, iteration);
                    }

                    for (auto [r, count]: residualCounts) {
                        splitter.testSplit(ClusterSplitter::residualSplitter(r), "residual", r, iteration);
                    }
                }

                splitter.commit(c, newSplitClusters);
            }

            clusters.insert(clusters.end(), newSplitClusters.begin(), newSplitClusters.end());
        };
#endif

        auto reseedEmptyClusters  __attribute__((__unused__)) = [&] (auto & pointResiduals, int iteration)
        {
            // 3.  High residual points make new clusters
            std::sort(pointResiduals.rbegin(), pointResiduals.rend());

            // Keep top residuals only
            pointResiduals.resize(std::min(pointResiduals.size(), clusters.size() * 4));

            for (size_t i = 0;  i < clusters.size();  ++i) {
                if (clusters[i].points.size() < 2 && pointResiduals.size() >= 3 - clusters[i].points.size()) {

                    if (clusters.size() > (size_t)numClusters * 2) {
                        clusters.resize(numClusters * 2);
                        break;
                    }
                    // Take two random points to be the seed of a new cluster
                    uint32_t point1 = clusters[i].empty() ? std::rand() % pointResiduals.size() : clusters[i].points[0].x;
                    uint32_t point2 = point1;
                    while (point1 == point2) {
                        point2 = std::rand() % pointResiduals.size();
                    }

                    clusters[i] = Cluster();
                    clusters[i].iter = iteration;
                    clusters[i].label = "reseed " + std::to_string(i); 
                    clusters[i].addPoint(point1, values[point1]);
                    clusters[i].addPoint(point2, values[point2]);
                }
            }
        };

        auto sortClusters = [&] ()
        {
            // Order our clusters so points get assigned to larger clusters first
            std::sort(clusters.begin(), clusters.end(),
                      [] (const auto & c1, const auto & c2) { return c1.points.size() > c2.points.size(); });
        };

        auto printClusters = [&] (const std::string & when)
        {
            size_t numNonEmptyClusters = 0;
            for (auto & c: clusters) {
                if (!c.empty())
                    numNonEmptyClusters += 1;
            }

            cerr << endl;
            cerr << underline << bold << numNonEmptyClusters << "/" << clusters.size() << " active clusters " << when << reset << endl;
            Cluster::printList(cerr, clusters);
        };

        auto newPhase = [&] (const std::string & phaseName)
        {
            if (!trace)
                return;
            cerr << endl << endl << magenta << string(80, '=') << endl
                 << bold << underline << phaseName << reset << endl;
            printClusters("phase start");
        };

        newPhase("Phase 0: Seed");

        seedClusters();

        newPhase("Phase 1: Mixture Model");

        // First, do a few iterations of looking for clusters.  This is looking for a loose fit
        // to identify the basic clusters and get a sense for a possible mixture model.
        size_t i = 0;
        for (;  i < 5;  ++i) {
            if (trace)
                cerr << endl << endl << "round " << i << " clusters.size() = " << clusters.size() << endl;

            auto pointResiduals = assignPoints(false /* greedy */, i);

            sortClusters();

            if (trace)
                printClusters("after assignment on iteration " + std::to_string(i));

            if (pointResiduals.empty()) {
                // no residuals, we're done
                if (trace)
                    cerr << "*** perfect cluster fit" << endl;
                break;
            }

            reseedEmptyClusters(pointResiduals, i);

            for (size_t i = 0;  i < clusters.size();  ++i) {
                clusters[i].fit();
            }

            // Remove obvious outliers
            std::vector<Cluster> newClusters;
            for (size_t i = 0;  i < clusters.size();  ++i) {
                auto outlierClusters = clusters[i].removeOutliers(5.0);

                for (auto & ol: outlierClusters) {
                    ol.broaden();
                    ol.iter = i;
                }

                if (!outlierClusters.empty()) {
                    clusters[i] = std::move(outlierClusters[0]);
                    newClusters.insert(newClusters.end(),
                                       std::make_move_iterator(outlierClusters.begin() + 1),
                                       std::make_move_iterator(outlierClusters.end()));
                }
            }

            clusters.insert(clusters.end(),
                            std::make_move_iterator(newClusters.begin()),
                            std::make_move_iterator(newClusters.end()));

            sortClusters();

            if (trace)
                printClusters("after fit on iteration " + std::to_string(i));
        }

        // Shrink them so we're not too greedy
        for (size_t i = 0;  i < clusters.size();  ++i) {
            if (!clusters[i].empty())
                clusters[i].shrinkToFit();
        }

        assignPoints(false /* greedy */, i);

        newPhase("Phase 2: Cluster Tightening");

        for (;  i < 11;  ++i) {

            if (trace)
                cerr << endl << endl << "round " << i << " clusters.size() = " << clusters.size() << endl;
            
            auto pointResiduals = assignPoints(false /* greedy */, i);

            sortClusters();

            // 3.  Print out our clusters
            if (trace)
                printClusters("after assignment on iteration " + std::to_string(i));

            if (pointResiduals.empty()) {
                // no residuals, we're done
                if (trace)
                    cerr << "*** perfect cluster fit" << endl;
                break;
            }

            //reseedEmptyClusters(pointResiduals, i);

            size_t oldSize = clusters.size();
            for (size_t j = 0;  j < oldSize;  ++j) {
                auto newClusters = clusters[j].optimize();
                if (newClusters.empty())
                    continue;

                if (trace)
                    Cluster::printSplit(cerr, clusters[j], newClusters);
                clusters[j].commitSplit(std::move(newClusters), clusters);
            }

            break;

#if 0
            splitClusters(i);

            if (trace)
                printClusters("after split on iteration " + std::to_string(i));

            for (size_t i = 0;  i < clusters.size();  ++i) {
                clusters[i].fit();
            }

            sortClusters();
#endif
        }

        sortClusters();

        if (trace)
            printClusters("after iterations");

        auto pointResiduals = assignPoints(true /* greedy */, 98);
        sortClusters();

        if (trace)
            printClusters("after assign");


        int maxNumBits = -1;
        std::vector<int> clustersWithMaxNumBits;

        for (size_t i = 0;  i < clusters.size();  ++i) {
            if (clusters[i].empty()) {
                clusters.resize(i);
                break;
            }
            clusters[i].fit();
            int numBits = clusters[i].numBits();
            if (numBits > maxNumBits) {
                maxNumBits = numBits;
                clustersWithMaxNumBits.clear();
            }
            if (numBits == maxNumBits) {
                clustersWithMaxNumBits.push_back(i);
            }
        }

        // Last assignment with the final order and size
        assignPoints(true /* greedy */, 99);

        if (trace)
            printClusters("after final assignment");

        ExcAssert(!clusters.empty());

        auto postProcessClusters = [] (const std::vector<Cluster> & clusters,
                                       const std::vector<uint32_t> & values)
            -> std::tuple<std::vector<Predictor>, std::vector<uint32_t>, std::vector<uint32_t>>
        {
            //cerr << red << "POST PROCESS CLUSTERS" << reset << endl;
            // Fix up predictors so that residuals are all non-negative
            std::vector<Predictor> predictors;
            for (const Cluster & c: clusters) {
                Predictor p = c;
                p.offset += c.residualStats.minValue;
                predictors.emplace_back(p);
            }

            std::vector<uint32_t> assignedClusters(values.size(), (uint32_t)MAX_LIMIT);
            std::vector<uint32_t> residuals(values.size(), (uint32_t)MAX_LIMIT);

            Cluster emergency;

            // Truncate residuals down to 32 bits
            size_t pointsDone = 0;
            for (size_t i = 0;  i < clusters.size();  ++i) {
                for (auto [x,y]: clusters[i].points) {
                    auto residual = (int64_t)predictors[i].predict(x) - y;
                    //cerr << red << "<><><><><>< residual = " << residual << " y = " << y << " pred = " << predictors[i].predict(x) << reset << endl;
                    if (residual < 0) {
                        cerr << "residual " << residual << endl;
                        cerr << clusters[i].print(i) << endl;
                        cerr << "cluster params " << clusters[i].params << endl;
                        cerr << "predictor params " << predictors[i].params << endl;
                    }
                    if (residual > std::numeric_limits<uint32_t>::max()) {
                        Cluster::printHeaders(cerr);
                        cerr << clusters[i].print(i) << endl;
                        cerr << clusters[i].params << endl;
                        cerr << "current {";
                        for (auto [x,y]: clusters[i].points) {
                            cerr << "{" << x << "," << y << "},";
                        }
                        cerr << "}" << endl;
                        //cerr << "fit {";
                        //for (auto [x,y]: clusters[i].fitPoints) {
                        //    cerr << "{" << x << "," << y << "},";
                        //}
                        cerr << "}" << endl;
                        cerr << "cl params " << clusters[i].params << " pr params " << predictors[i].params << endl;
                        cerr << "x " << x << " y " << y << " predicted " << predictors[i].predict(x) << " residual " << residual << endl;
                        cerr << "cluster predicted " << clusters[i].predict(x) << endl;
                        cerr << "offset " << clusters[i].offset << " " << predictors[i].offset << endl;
                        cerr << red << "residual > INT_MAX " << residual << " > " << std::numeric_limits<uint32_t>::max() << reset << endl;
                        abort();
                    }
                    ExcAssert(residual >= 0);
                    ExcAssert(residual <= (int64_t)clusters[i].residualRange());
                    ExcAssert(assignedClusters[x] == MAX_LIMIT);
                    assignedClusters[x] = i;
                    residuals[x] = residual;
                }
                pointsDone += clusters[i].points.size();
            }

            ExcAssert(pointsDone == values.size());

            return { std::move(predictors), std::move(assignedClusters), std::move(residuals) };
        };

        auto [ predictors, assignedClusters, residuals] = postProcessClusters(clusters, values);

        auto analyzeRanges = [trace] (const std::vector<Cluster> & clusters,
                                 const std::vector<Predictor> & predictors,
                                 const std::vector<uint32_t> & assignedClusters,
                                 const std::vector<uint32_t> & residuals,
                                 const std::vector<uint32_t> & values) -> std::vector<Range>
        {
            struct PointOfInterest {
                size_t point;
                enum {
                    FINISH,  // comes first so it sorts with finishes before starts
                    START
                } type;
                uint32_t cluster;

                auto operator <=> (const PointOfInterest & other) const = default;
            };

            std::vector<PointOfInterest> pointsOfInterest;

            for (uint32_t i = 0;  i < clusters.size();  ++i) {
                PointOfInterest start{clusters[i].indexStats.minValue, PointOfInterest::START, i};
                PointOfInterest finish{clusters[i].indexStats.maxValue + 1, PointOfInterest::FINISH, i};
                pointsOfInterest.insert(pointsOfInterest.end(), { start, finish });
            }

            std::sort(pointsOfInterest.begin(), pointsOfInterest.end());

            std::set<uint32_t> activeClusters;

            uint64_t totalBits = 0;

            uint32_t rangeStart = 0;
            std::set<uint32_t> clustersStarted, clustersFinished;
            std::vector<Range> ranges;

            auto finishRange = [&] (uint32_t point)
            {
                ExcAssert(clustersStarted.empty() || clustersFinished.empty());
                ExcAssert(clustersStarted.size() + clustersFinished.size() > 0);

                // Previous range goes from rangeStart to newRangeActivePoint;
                uint32_t newRangeActivePoint = clustersStarted.empty() ? point : rangeStart;
                //clustersStarted.empty()
                //     ? *clustersFinished.rbegin() : *clustersStarted.begin();

                int64_t bitsRequiredForResidual = 0;

                for (uint32_t cluster: activeClusters) {
                    bitsRequiredForResidual = std::max<int64_t>(bitsRequiredForResidual, clusters[cluster].bitsRequiredForResidual());
                }

                auto bitsRequiredForClusterNumber = bits(activeClusters.size() - 1);
                auto bitsRequiredPerElement = bitsRequiredForResidual + bitsRequiredForClusterNumber;

                auto bitsRequiredForRange = (newRangeActivePoint - rangeStart) * bitsRequiredPerElement;
                totalBits += bitsRequiredForRange;

                if (newRangeActivePoint != rangeStart) {
                    if (trace) {
                        cerr << "  range " << rangeStart << "-" << newRangeActivePoint << ": " << activeClusters.size() << " active, "
                            << bitsRequiredForResidual
                            << "+" << bitsRequiredForClusterNumber << " = " << bitsRequiredPerElement << " bits required:";
                        for (uint32_t cluster: activeClusters)
                            cerr << " " << cluster;
                        cerr << " total " << totalBits << " bits" << endl;
                    }

                    Range range;
                    range.rangeStart = rangeStart;
                    for (size_t i = rangeStart;  i < newRangeActivePoint;  ++i) {
                        range.residuals.push_back(residuals[i]);
                        ExcAssert(activeClusters.contains(assignedClusters[i]));
                        range.selectors.push_back(assignedClusters[i]);
                        auto prediction = predictors[assignedClusters[i]].predict(i) - residuals[i];
                        //if (i < 10)
                        //    cerr << "point " << i << " cluster " << assignedClusters[i] << " range " << ranges.size()
                        //        << " residual " << residuals[i] << endl;
                        ExcAssertEqual(prediction, values[i]);
                    }

                    ranges.emplace_back(std::move(range));

                    rangeStart = newRangeActivePoint;
                }

                activeClusters.insert(clustersStarted.begin(), clustersStarted.end());
                for (uint32_t c: clustersFinished) 
                    activeClusters.erase(c);

                clustersStarted.clear();
                clustersFinished.clear();
            };

            uint32_t lastPoint = 0;

            // Multiple clusters that start or finish within the same range are rolled up
            for (auto [point, type, cluster]: pointsOfInterest) {
                //cerr << "point " << point << " " << (type == PointOfInterest::START ? "START" : "FINISH")
                //     << " cluster " << cluster << endl;
                switch (type) {
                    case PointOfInterest::START:
                        if (point - lastPoint > 32 || !clustersFinished.empty()) {
                            finishRange(lastPoint);
                        }
                        clustersStarted.insert(cluster);
                        break;

                    case PointOfInterest::FINISH:
                        if (point - lastPoint > 32 || !clustersStarted.empty()) {
                            finishRange(lastPoint);
                        }
                        clustersFinished.insert(cluster);
                        break;
                }

                lastPoint = point;
            }

            finishRange(lastPoint);

            if (trace)
                cerr << "total requirement: " << totalBits << " bits (" << (1.0 * totalBits / 8192) << "kb) at "
                    << 1.0 * totalBits / values.size() / 8.0 << " bytes/value" << endl;

            return ranges;
        };

        auto ranges = analyzeRanges(clusters, predictors, assignedClusters, residuals, values);

        return std::make_tuple(std::move(ranges), std::move(predictors));
    }
};

void freeze(MappingContext & context, MappedIntTableBase::Range & output, const IntTableAnalyzer::Range & range)
{
    // Two possibilities in how we serialize:
    // 1.  One residual table with the largest width across all clusters, but not needing our selector
    //     table to keep track of indexes.
    // 2.  A residual table for each of the clusters.

    uint32_t largestResidual = 0;
    for (auto r: range.residuals) {
        largestResidual = std::max(largestResidual, r);
    }

    size_t numPredictors = *std::max_element(range.selectors.begin(), range.selectors.end()) + 1;

    ExcAssert(range.selectors.size() == range.residuals.size());
    //size_t n = range.numPoints();

    // Remap selectors
    std::set<int> allSelectors(range.selectors.begin(), range.selectors.end());
    std::vector<uint32_t> selectorLookups(allSelectors.begin(), allSelectors.end());
    std::vector<int> newSelectors(allSelectors.begin(), allSelectors.end());

    if (newSelectors.size() > 256) {
        MLDB_THROW_LOGIC_ERROR("too many selectors to choose between");
    }

    std::vector<int> selectorCounts(*allSelectors.rbegin() + 1, 0);
    for (auto s: range.selectors) {
        selectorCounts.at(s) += 1;
    }

    ExcAssert(!allSelectors.empty());
    std::vector<int> selectorToNewSelector(numPredictors, -1);
    for (size_t i = 0;  i < newSelectors.size();  ++i) {
        selectorToNewSelector[newSelectors[i]] = i;
    }
    std::vector<uint8_t> selectors(range.selectors.size());
    //cerr << "selectorToNewSelector " << selectorToNewSelector << " selectorLookups " << selectorLookups << endl;
    std::transform(range.selectors.begin(), range.selectors.end(), selectors.begin(),
                    [&selectorToNewSelector] (uint32_t oldSelector) { return selectorToNewSelector.at(oldSelector); });

    std::vector<std::vector<uint32_t> > rangeResiduals(allSelectors.size());
    for (size_t i = 0;  i < range.residuals.size();  ++i) {
        rangeResiduals.at(selectorToNewSelector[range.selectors[i]]).push_back(range.residuals[i]);
    }

    size_t largestSelector = numPredictors ? numPredictors - 1 : 0;

    size_t n = range.residuals.size();

    size_t oneResidualTableBytes
        = raw_mapped_indirect_bytes(n, largestResidual)
        + raw_mapped_indirect_bytes(n, largestSelector);
    size_t multipleResidualTableBytes = mapped_selector_table_bytes_required(selectors);
    for (auto & r: rangeResiduals) {
        uint32_t maxRangeResidual = 0;
        if (!r.empty()) {
            maxRangeResidual = *std::max_element(r.begin(), r.end());
        }
        multipleResidualTableBytes += sizeof(RawMappedIntTable) + raw_mapped_indirect_bytes(r.size(), maxRangeResidual);
    }

    freeze_field(context, "activePredictors", output.activePredictors_, selectorLookups);

    if (oneResidualTableBytes <= multipleResidualTableBytes) {
        output.isIndexed_ = false;
        freeze_field(context, "raw.selectors", output.raw.selectors_, selectors);
        freeze_field(context, "raw.residuals", output.raw.residuals_, range.residuals);
    }
    else {
        output.isIndexed_ = true;
        freeze_field(context, "indexed.selectors", output.indexed.selectors_, selectors);
        freeze_field(context, "indexed.residuals", output.indexed.residuals_, rangeResiduals);
    }
}

uint32_t MappedIntTableBase::getRaw(uint32_t x) const
{
    // First, find which range it's in.  This is log(n) in the number of ranges
    uint32_t r = std::lower_bound(rangeStarts_.begin(), rangeStarts_.end(), x) - rangeStarts_.begin();
    if (rangeStarts_.at(r) > x)
        --r;

    // i is the x value but relative to the beginning of the range
    auto i = x - rangeStarts_.at(r);
    const Range & range = ranges_.at(r);

    // There are two ways to store the ranges... either indexed with a separate table per residual
    // or in a single table large enough to hold all the residuals.
    uint32_t selector;
    uint32_t residual;

    if (range.isIndexed_) {
        // Otherwise, each range is a mixture of different predictors.  The selectors tell us which
        // predictor and the index in this predictor's residual list of this entry.
        uint32_t index;
        std::tie(selector, index) = range.indexed.selectors_.at(i);

        // The residual tells us how to adjust the prediction to get the exact value back.
        residual = range.indexed.residuals_.at(selector).at(index);
    }
    else {
        // The active predictor number comes from the selector array...
        selector = range.raw.selectors_.at(i);

        // ... and the residual from the residual array
        residual = range.raw.residuals_.at(i);
    }

    // Predictors are in a global array, but they are indexed by a per-range set of active
    // predictors.  Transform the per-range index into a global index.
    uint32_t predictor = range.activePredictors_.at(selector);

    // Finally, we can make the prediction and adjust with the residual to get the final
    // prediction.
    return predictors_.at(predictor).predict(x) - residual;
}

void freeze(MappingContext & context, MappedIntTableBase & output, std::span<const uint32_t> input)
{
    size_t offsetBefore = context.getOffset();

    if (input.empty()) {
        new (&output) RawMappedIntTable();
        std::vector<uint32_t> rangeStarts(1,0);
        freeze_field(context, "rangeStarts", output.rangeStarts_, rangeStarts);
        ExcAssert(!output.rangeStarts_.empty());
        return;
    }

    int trace = context.getOption(INT_TABLE_TRACE_LEVEL);
    //trace = false;

    IntTableAnalyzer predictiveAnalyzer;
    predictiveAnalyzer.analyze(input.begin(), input.end());
    int numClusters = std::min<int>(32, 5 + std::log2(input.size())); // TODO better heuristic
    numClusters = 4;
    auto [ranges, predictors] = predictiveAnalyzer.clusterPoints(numClusters, trace);

    freeze_field(context, "predictors", output.predictors_, predictors);
    freeze_field(context, "ranges", output.ranges_, ranges);
    
    auto rangeStarts = mapTo<std::vector<uint32_t>>(ranges.begin(), ranges.end(),
                                                    [&] (auto & r) { return r.rangeStart; });
    rangeStarts.push_back(input.size());
    freeze_field(context, "rangeStarts" ,output.rangeStarts_, rangeStarts);

    //cerr << "ranges.size() = " << ranges.size() << endl;
    ExcAssert(output.ranges_.size() == ranges.size());
    ExcAssert(output.size() == input.size());

    bool validate = context.getOption(VALIDATE);
    for (size_t x = 0;  x < input.size() && validate;  ++x) {
        if (output.getRaw(x) != input[x]) {
            cerr << "residual at index " << x << ": expected " << input[x] << " got " << output.getRaw(x) << endl;
#if 0
            uint32_t range = std::lower_bound(output.rangeStarts_.begin(), output.rangeStarts_.end(), x) - output.rangeStarts_.begin();
            if (output.rangeStarts_.at(range) > x)
                --range;
            auto i = x - output.rangeStarts_.at(range);

            auto [selector, index] = output.ranges_.at(range).selectors_.at(i);
            uint32_t predictor = output.ranges_.at(range).activePredictors_.at(selector);
            uint32_t residual = output.ranges_.at(range).residuals_.at(selector).at(index);
            uint32_t result = predictors.at(predictor).predict(x) - residual;
            cerr << "predictor " << predictors.at(predictor).params << " offset " << predictors.at(predictor).offset << endl;
            cerr << "selector " << selector << " index " << index << " residual " << residual << " result " << result << endl;
#endif
        }
        ExcAssert(output.getRaw(x) == input[x]);
    }

    if (trace) {
        size_t offsetAfter = context.getOffset();
        const char * start = (const char *)context.getMemory(offsetBefore);

        auto [bytesUncompressed, bytesCompressed, compressionRatio]
            = calc_compressibility(start, offsetAfter - offsetBefore);

        if (compressionRatio < 0.9) {

            static int tableNum = 0;

            cerr << "int table number " << tableNum << " with " << output.size() << " integers"
                << " was compressible from " << bytesUncompressed
                << " bytes (" << 1.0 * bytesUncompressed / input.size() << "bytes/entry) to "
                << bytesCompressed << " bytes (" << 1.0 * bytesCompressed / input.size() << "bytes/entry) ("
                << 100.0 * compressionRatio << "% ratio) in table " << tableNum << endl;

            {
                std::ofstream writeTable("compressible-int-table-" + std::to_string(tableNum) + ".txt");
                for (auto v: input) {
                    char buf[128];
                    ::snprintf(buf, 128, "%d\n", to_int(v));
                    writeTable << buf;
                }
                writeTable << endl;
            }

            {
                std::ofstream writeBinary("compressible-int-table-" + std::to_string(tableNum) + ".bin");
                writeBinary.write(start, offsetAfter - offsetBefore);
            }

    #if 1
    #if 0
            {
                std::ofstream writeTable("compressible-int-table-" + std::to_string(tableNum) + "-residuals.txt");
                for (auto v: residuals) {
                    char buf[128];
                    ::snprintf(buf, 128, "%d\n", to_int(v));
                    writeTable << buf;
                }
                writeTable << endl;
            }
    #endif
    #if 0
            {
                std::ofstream writeTable("compressible-int-table-" + std::to_string(tableNum) + "-slopes.txt");
                for (auto v: predictiveAnalyzer.slopes) {
                    char buf[128];
                    ::snprintf(buf, 128, "%f %d %d %d %d\n", (double)v, v.element, v.numerator, v.denominator, v.value);
                    writeTable << buf;
                }
                writeTable << endl;
            }
    #endif

            {
                std::ofstream writeTable("compressible-int-table-" + std::to_string(tableNum) + "-clusters.txt");
                writeTable << "x,y,range,cluster,residual,predicted";
                for (unsigned i = 0;  i < predictors.size();  ++i) {
                    writeTable << ",cluster" + std::to_string(i);
                }
                writeTable << endl;
                for (unsigned i = 0;  i < input.size();  ++i) {
                    auto y = input[i];
                    auto x = i;

                    char buf[128];
                    uint32_t range = std::lower_bound(rangeStarts.begin(), rangeStarts.end(), x) - rangeStarts.begin();
                    if (rangeStarts.at(range) > x)
                        --range;
                    auto ri = x - rangeStarts.at(range);
                    uint32_t selector = ranges.at(range).selectors.at(ri);

                    uint32_t cluster = output.ranges_.at(range).activePredictors_.at(selector);
                    uint32_t residual = ranges.at(range).residuals.at(ri);
                    uint32_t predicted = predictors.at(cluster).predict(x);

                    ::snprintf(buf, 128, "%d,%d,%d,%d,%d,%d", x, y, range, cluster, residual, predicted);
                    writeTable << buf;

                    for (size_t j = 0;  j < predictors.size();  ++j) {
                        writeTable << ",";
                        if (cluster == j) {
                            ::snprintf(buf, 128, "%d", input[i]);
                            writeTable << buf;
                        }
                    }

                    writeTable << endl;
                }
                writeTable << endl;
            }
    #endif
            

            ++tableNum;
        } 
    }
}

} // namespace MLDB