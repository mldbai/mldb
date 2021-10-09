/** randomforest_kernels.cc                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#include "randomforest_kernels.h"
#include "randomforest_kernels_opencl.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/base/map_reduce.h"
#include "mldb/base/scope.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/tuple_description.h"
#include "mldb/types/pair_description.h"
#include "mldb/types/map_description.h"
#include "mldb/types/span_description.h"
#include "mldb/utils/environment.h"
#include "mldb/arch/vm.h"
#include "mldb/block/compute_kernel_host.h"
#include <condition_variable>
#include <sstream>

using namespace std;


namespace MLDB {
namespace RF {

// Core kernel of the decision tree search algorithm.  Transfer the
// example weight into the appropriate (bucket,label) accumulator.
// Returns whether or not the feature is still active
template<typename RowIterator, typename BucketList>
std::pair<bool, int>
testFeatureKernelCpuT(RowIterator rowIterator,
                     size_t numRows,
                     const BucketList & buckets,
                     W * w /* buckets.numBuckets entries */)
{
    // Minimum bucket number we've seen
    int minBucket = INT_MAX;
    
    // Maximum bucket number we've seen.  Can significantly reduce the
    // work required to search the buckets later on, as those without
    // an example have no possible split point.
    int maxBucket = INT_MIN;

    for (size_t j = 0;  j < numRows;  ++j) {
        DecodedRow r = decodeRow(rowIterator, j);//rowIterator.getDecodedRow();
        int bucket = buckets[r.exampleNum];
        //ExcAssertLess(bucket, buckets.numBuckets);

        w[bucket].add(r.label, r.weight);
        maxBucket = std::max(maxBucket, bucket);
        minBucket = std::min(minBucket, bucket);
    }

    return { minBucket < maxBucket, maxBucket };
}

std::pair<bool, int>
testFeatureKernelCpu(Rows::RowIterator rowIterator,
                  size_t numRows,
                  const BucketList & buckets,
                  W * w /* buckets.numBuckets entries */)
{
    return testFeatureKernelCpuT(rowIterator, numRows, buckets, w);
}

std::pair<bool, int>
testFeatureKernelCpu(const float * decodedRows,
                  size_t numRows,
                  const BucketList & buckets,
                  W * w /* buckets.numBuckets entries */)
{
    return testFeatureKernelCpuT(decodedRows, numRows, buckets, w);
}

std::pair<bool, int>
testFeatureKernel(Rows::RowIterator rowIterator,
                  size_t numRows,
                  const BucketList & buckets,
                  W * w /* buckets.numBuckets entries */)
{
    return testFeatureKernelCpu(rowIterator, numRows, buckets, w);
    //return testFeatureKernelOpencl(rowIterator, numRows, buckets, w);
}

std::pair<bool, int>
testFeatureKernel(const float * decodedRows,
                  size_t numRows,
                  const BucketList & buckets,
                  W * w /* buckets.numBuckets entries */)
{
    return testFeatureKernelCpu(decodedRows, numRows, buckets, w);
    //return testFeatureKernelOpencl(rowIterator, numRows, buckets, w);
}

std::pair<bool, int>
testFeatureKernel(Rows::RowIterator rowIterator,
                  size_t numRows,
                  const uint32_t * buckets,
                  W * w /* buckets.numBuckets entries */)
{
    return testFeatureKernelCpuT(rowIterator, numRows, buckets, w);
    //return testFeatureKernelOpencl(rowIterator, numRows, buckets, w);
}

std::pair<bool, int>
testFeatureKernel(Rows::RowIterator rowIterator,
                  size_t numRows,
                  const uint16_t * buckets,
                  W * w /* buckets.numBuckets entries */)
{
    return testFeatureKernelCpuT(rowIterator, numRows, buckets, w);
    //return testFeatureKernelOpencl(rowIterator, numRows, buckets, w);
}

void
testFeatureKernel(ComputeContext & context,
                  uint32_t f, uint32_t nf,
                  std::span<const float> decodedRows,
                  uint32_t numRows,

                  std::span<const uint32_t> allBucketData,
                  std::span<const uint32_t> bucketDataOffsets,
                  std::span<const uint32_t> bucketNumbers,
                  std::span<const uint32_t> bucketEntryBits,

                  std::span<const uint32_t> featureActive,

                  std::span<W> allWOut)
{
    if (!featureActive[f])
        return;

    ExcAssertLess(bucketNumbers[f + 1], allWOut.size());

    BucketList buckets;
    buckets.entryBits = bucketEntryBits[f];
    buckets.numBuckets = -1;  // unused but set to an invalid value to be sure
    buckets.numEntries = bucketNumbers[f + 1] - bucketNumbers[f];
    buckets.storagePtr = allBucketData.data() + bucketDataOffsets[f];

    testFeatureKernel(decodedRows.data(), numRows, buckets, allWOut.data() + bucketNumbers[f]);
}

// Chooses which is the best split for a given feature.
std::tuple<double /* bestScore */,
           int /* bestSplit */,
           W /* bestLeft */,
           W /* bestRight */>
chooseSplitKernel(const W * w /* at least maxBucket + 1 entries */,
                  int maxBucket,
                  bool ordinal,
                  const W & wAll,
                  bool debug)
{
    double bestScore = INFINITY;
    int bestSplit = -1;
        
    W bestLeft;
    W bestRight;

    debug = debug || (wAll.count() == 2315 && maxBucket == 256 && false);
    if (debug) {
        cerr << "chooseSplitKernel maxBucket " << maxBucket << " wAll "
             << jsonEncodeStr(wAll) << " ordinal " << ordinal << endl;
    }

    
    if (ordinal) {
        // Calculate best split point for ordered values
        W wFalse = wAll, wTrue;

        // Now test split points one by one
        for (unsigned j = 0;  j <= maxBucket;  ++j) {

            //if (w[j].empty())
            //    continue;                   

            if (wFalse.count() > 0 && wTrue.count() > 0) {
            
                double s = scoreSplit(wFalse, wTrue);

                if (debug) {
                    std::cerr << "  ord split " << j << " "
                        //                              << features.info->bucketDescriptions.getValue(j)
                              << " had score " << s << std::endl;
                    cerr << "    false: " << jsonEncodeStr(wFalse) << endl;
                    cerr << "    true:  " << jsonEncodeStr(wTrue) << endl;
                }
                
                if (s < bestScore) {
                    bestScore = s;
                    bestSplit = j;
                    bestRight = wFalse;
                    bestLeft = wTrue;
                }
            }
            
            if (j < maxBucket && w[j].count() != 0) {
                wFalse -= w[j];
                wTrue += w[j];
            }
        }
    }
    else {
        // Calculate best split point for non-ordered values
        // Now test split points one by one

        for (unsigned j = 0;  j <= maxBucket;  ++j) {
                    
            if (w[j].empty())
                continue;
            
            W wFalse = wAll;
            wFalse -= w[j];                    

            if (wFalse.count() == 0 || w[j].count() == 0) {
                continue;
            }
            
            double s = scoreSplit(wFalse, w[j]);

            if (debug) {
                std::cerr << "  non ord split " << j << " "
                    //    << features.info->bucketDescriptions.getValue(j)
                          << " had score " << s << std::endl;
                    cerr << "    false: " << jsonEncodeStr(wFalse) << endl;
                    cerr << "    true:  " << jsonEncodeStr(w[j]) << endl;
            }
                    
            if (s < bestScore) {
                bestScore = s;
                bestSplit = j;
                bestRight = wFalse;
                bestLeft = w[j];
            }
        }

    }

    return { bestScore, bestSplit, bestLeft, bestRight };
}

std::tuple<double /* bestScore */,
           int /* bestSplit */,
           W /* bestLeft */,
           W /* bestRight */,
           bool /* feature is still active */ >
testFeatureNumber(int featureNum,
                  const std::span<const Feature> & features,
                  Rows::RowIterator rowIterator,
                  size_t numRows,
                  const W & wAll,
                  bool debug)
{
    const Feature & feature = features[featureNum];
    const BucketList & buckets = feature.buckets;
    int nb = buckets.numBuckets;

    std::vector<W> w(nb);
    int maxBucket = -1;

    double bestScore = INFINITY;
    int bestSplit = -1;
    W bestLeft;
    W bestRight;

    if (!feature.active)
        return std::make_tuple(bestScore, bestSplit, bestLeft, bestRight, false);

    // Is s feature still active?
    bool isActive;

    std::tie(isActive, maxBucket)
        = testFeatureKernel(rowIterator, numRows,
                            buckets, w.data());

    if (isActive) {
        std::tie(bestScore, bestSplit, bestLeft, bestRight)
            = chooseSplitKernel(w.data(), maxBucket, feature.ordinal,
                                wAll, debug);
    }

    return { bestScore, bestSplit, bestLeft, bestRight, isActive };
}

std::tuple<double, int, int, W, W, std::vector<uint8_t> >
testAllCpu(int depth,
           const std::span<const Feature> & features,
           const Rows & rows,
           FrozenMemoryRegionT<uint32_t> bucketMemory,
           bool trace)
{
    std::vector<uint8_t> newActive;
    for (auto & f: features) {
        newActive.push_back(f.active);
    }

    // We have no impurity in our bucket.  Time to stop
    if (rows.wAll[0] == 0 || rows.wAll[1] == 0) {
        return std::make_tuple(1.0, -1, -1, rows.wAll, W(),
                               std::vector<uint8_t>(features.size(), false));
    }

    bool debug = trace;

    int nf = features.size();

    size_t totalNumBuckets = 0;
    size_t activeFeatures = 0;

    for (unsigned i = 0;  i < nf;  ++i) {
        if (!features[i].active)
            continue;
        ++activeFeatures;
        totalNumBuckets += features[i].buckets.numBuckets;
    }

    if (debug) {
        std::cerr << "total of " << totalNumBuckets << " buckets" << std::endl;
        std::cerr << activeFeatures << " of " << nf << " features active"
                  << std::endl;
    }

    double bestScore = INFINITY;
    int bestFeature = -1;
    int bestSplit = -1;
        
    W bestLeft;
    W bestRight;

    // Reduction over the best split that comes in feature by feature;
    // we find the best global split score and store it.  This is done
    // in order to be sure that we deterministically pick the right
    // one.
    auto findBest = [&] (int feature,
                         const std::tuple<double, int, W, W> & val)
        {
            double score = std::get<0>(val);

#if 0
            cerr << "CPU: rows "
                 << rows.rowCount() << " wAll " << jsonEncodeStr(rows.wAll)
                 << " feature " << feature << " score "
                 << score << " split " << std::get<1>(val)
                 << " left " << jsonEncodeStr(std::get<2>(val))
                 << " right " << jsonEncodeStr(std::get<3>(val))
                 << endl;
#endif
            
            if (score < bestScore) {
                //cerr << "  *** best" << endl;
                bestFeature = feature;
                std::tie(bestScore, bestSplit, bestLeft, bestRight) = val;
            }
        };

    // Parallel map over all features
    auto doFeature = [&] (int i)
        {
            if (debug)
                cerr << "doing feature " << i << features[i].info->columnName << endl;
            double score;
            int split = -1;
            W bestLeft, bestRight;

            Rows::RowIterator rowIterator = rows.getRowIterator();

            bool stillActive;
            std::tie(score, split, bestLeft, bestRight, stillActive)
                = testFeatureNumber(i, features, rowIterator,
                                    rows.rowCount(), rows.wAll, debug);
            if (debug)
                cerr << "  score " << score << " split " << split << endl;
            newActive[i] = stillActive;

            return std::make_tuple(score, split, bestLeft, bestRight);
        };

#if 0
    for (unsigned i = 0;  i < nf;  ++i) {
        if (depth < 3) {
            if (!features[i].active)
                continue;
            Date beforeCpu = Date::now();
            findBest(i, doFeature(i));
            Date afterCpu = Date::now();

            std::ostringstream str;
            str << "    feature " << i << " buckets "
                << features[i].buckets.numBuckets
                << " CPU took "
                << afterCpu.secondsSince(beforeCpu) * 1000.0
                << "ms" << endl;
            cerr << str.str();
        }
        else {
            findBest(i, doFeature(i));
        }
    }
#else
    if (!debug && (depth < 4 || (uint64_t)rows.rowCount() * (uint64_t)nf > 20000)) {
        parallelMapInOrderReduce(0, nf, doFeature, findBest);
    }
    else {
        for (unsigned i = 0;  i < nf;  ++i)
            findBest(i, doFeature(i));
    }
#endif
    
    int bucketsEmpty = 0;
    int bucketsOne = 0;
    int bucketsBoth = 0;

#if 0        
    for (auto & wt: w[i]) {
        //wAll += wt;
        bucketsEmpty += wt[0] == 0 && wt[1] == 0;
        bucketsBoth += wt[0] != 0 && wt[1] != 0;
        bucketsOne += (wt[0] == 0) ^ (wt[1] == 0);
    }
#endif
            
    if (debug) {
        std::cerr << "buckets: empty " << bucketsEmpty << " one " << bucketsOne
                  << " both " << bucketsBoth << std::endl;
        std::cerr << "bestScore " << bestScore << std::endl;
        std::cerr << "bestFeature " << bestFeature << " "
                  << features[bestFeature].info->columnName << std::endl;
        std::cerr << "bestSplit " << bestSplit << " "
                  << features[bestFeature].info->bucketDescriptions.getSplit(bestSplit)
                  << std::endl;
    }

    return std::make_tuple(bestScore, bestFeature, bestSplit,
                           bestLeft, bestRight, newActive);
}

std::tuple<double, int, int, W, W, std::vector<uint8_t> >
testAll(int depth,
        const std::span<const Feature> & features,
        const Rows & rows,
        FrozenMemoryRegionT<uint32_t> bucketMemory,
        bool trace)
{
    return testAllCpu(depth, features, rows, bucketMemory, trace);

    if (rows.rowCount() < 10000 && false) {
        if (depth < 3 && false) {
            //static std::mutex mutex;
            //std::unique_lock<std::mutex> guard(mutex);

            Date beforeCpu = Date::now();
            auto res = testAllCpu(depth, features, rows, bucketMemory, trace);
            Date afterCpu = Date::now();

            int activeFeatures = 0;
            int activeBuckets = 0;

            for (auto & f: features) {
                if (!f.active)
                    continue;
                ++activeFeatures;
                activeBuckets += f.buckets.numBuckets;
            }
            
            std::ostringstream str;
            str << "depth " << depth << " rows " << rows.rowCount()
                << " features " << activeFeatures
                << " buckets " << activeBuckets << " CPU took "
                << afterCpu.secondsSince(beforeCpu) * 1000.0
                << "ms" << endl;
            cerr << str.str();
            return res;
        }
        else {
            return testAllCpu(depth, features, rows, bucketMemory, trace);
        }
    }
    else {
#if OPENCL_ENABLED
        throw MLDB::Exception("OpenCL is disabled");
        static constexpr int MAX_GPU_JOBS = 4;
        static std::atomic<int> numGpuJobs = 0;
        

        if (numGpuJobs.fetch_add(1) >= MAX_GPU_JOBS) {
            --numGpuJobs;
            return testAllCpu(depth, features, rows, bucketMemory, false);
        }
        else {
            auto onExit = ScopeExit([&] () noexcept { --numGpuJobs; });
            try {
                return testAllOpenCL(depth, features, rows, bucketMemory);
            } MLDB_CATCH_ALL {
                return testAllCpu(depth, features, rows, bucketMemory, false);
            }
        }
        
        static std::mutex mutex;
        std::unique_lock<std::mutex> guard(mutex);

        Date beforeCpu = Date::now();
        auto cpuOutput = testAllCpu(depth, features, rows, bucketMemory, false);
        Date afterCpu = Date::now();
        auto gpuOutput = testAllOpenCL(depth, features, rows, bucketMemory);
        Date afterGpu = Date::now();

        ostringstream str;
        str << "CPU took " << afterCpu.secondsSince(beforeCpu) * 1000.0
            << "ms; GPU took " << afterGpu.secondsSince(afterCpu) * 1000.0
            << "ms" << endl;
        cerr << str.str();

        if (cpuOutput != gpuOutput) {
            cerr << "difference in outputs: " << endl;
            cerr << "rows.rowCount() = " << rows.rowCount() << endl;
            cerr << "depth = " << depth << endl;
            cerr << "cpu: " << jsonEncode(cpuOutput) << endl;
            cerr << "gpu: " << jsonEncode(gpuOutput) << endl;
            abort();
        }

        return cpuOutput;
#endif
    }
}


/*****************************************************************************/
/* RECURSIVE RANDOM FOREST KERNELS                                           */
/*****************************************************************************/

void updateBuckets(const std::span<const Feature> & features,
                   std::vector<RowPartitionInfo> & partitions,
                   std::span<W> buckets,
                   uint32_t numActiveBuckets,
                   std::vector<W> & wAll,
                   const std::span<const uint32_t> & bucketOffsets,
                   const std::span<const PartitionSplit> & partitionSplits,
                   const std::span<const std::pair<int32_t, int32_t> > & newPartitionNumbers,
                   int newNumPartitions,
                   const std::span<const float> & decodedRows,
                   const std::span<const int> & activeFeatures)
{
    int numPartitions = wAll.size();

    if (numPartitions == 0)
        return;
    
    ExcAssertEqual(numPartitions, wAll.size());
    ExcAssertEqual(numPartitions, partitionSplits.size());
    ExcAssertEqual(numPartitions, newPartitionNumbers.size());

    ExcAssertGreaterEqual(buckets.size(), newNumPartitions * numActiveBuckets);
    wAll.resize(std::max(buckets.size(), (size_t)newNumPartitions));

    auto getBuckets = [&] (uint32_t partition) -> std::span<W>
    {
        return buckets.subspan(partition * numActiveBuckets, numActiveBuckets);
    };

    auto getBucket = [&] (uint32_t partition, uint32_t bucket) -> W &
    {
        return getBuckets(partition)[bucket];
    };

    // Keep track of which place each of our partitions currently is
    std::vector<int> currentLocations(numPartitions, -1);

    //cerr << "newPartitionNumbers = " << jsonEncodeStr(newPartitionNumbers)
    //     << endl;

    {
        // We construct what we need and then copy it back in a block... inefficient,
        // could be optimized if ever we need to go back to this version
        std::vector<W> newBuckets(newNumPartitions * numActiveBuckets);
        std::vector<W> newWAll(newNumPartitions);
    
        // Distribute old ones to their new place
        for (size_t i = 0;  i < numPartitions;  ++i) {
            if (newPartitionNumbers[i].first == -1)
                continue;
            if (partitionSplits[i].left.count() == 0
                || partitionSplits[i].right.count() == 0)
                continue;

            int to = partitionSplits[i].transferDirection() == RL
                ? newPartitionNumbers[i].second
                : newPartitionNumbers[i].first;

            auto from = getBuckets(i);
            std::copy(from.begin(), from.end(), newBuckets.data() + (to * numActiveBuckets));
            newWAll[to] = wAll[i];
        }

        std::copy(newBuckets.begin(), newBuckets.end(), buckets.data());
        newWAll.swap(wAll);
    }
        
    constexpr bool checkPartitionCounts = false;

    // Make sure that the number in the buckets is actually what we
    // expected when we calculated the split.
    if (checkPartitionCounts) {
        bool different = false;

        for (int i = 0;  i < partitionSplits.size();  ++i) {
            int leftPartition = newPartitionNumbers[i].first;
            int rightPartition = newPartitionNumbers[i].second;

            if (leftPartition == -1 || rightPartition == -1) {
                ExcAssertEqual(leftPartition, -1);
                ExcAssertEqual(rightPartition, -1);
                ExcAssertEqual(partitionSplits[i].feature, -1);
                continue;
            }

            ExcAssertEqual(wAll[leftPartition].count()
                           + wAll[rightPartition].count(),
                           partitionSplits[i].left.count()
                           + partitionSplits[i].right.count());

#if 0            
            for (auto & f: activeFeatures) {
                W wLeft, wRight;
                int startBucket = bucketOffsets[f];
                int endBucket = bucketOffsets[f + 1];

                for (int b = startBucket; b < endBucket;  ++b) {
                    wLeft += buckets[leftPartition][b];
                    wRight += buckets[rightPartition][b];
                }

                if (wLeft != partitionSplits[i].left) {
                    cerr << "error on partition " << i << " left "
                         << leftPartition << " right " << rightPartition
                         << " feature " << f << ": left "
                         << jsonEncodeStr(wLeft) << " != "
                         << jsonEncodeStr(partitionSplits[i].left)
                         << endl;
                    different = true;
                }
                
                if (wRight != partitionSplits[i].right) {
                    cerr << "error on partition " << i << " left "
                         << leftPartition << " right " << rightPartition
                         << " feature " << f << ": right "
                         << jsonEncodeStr(wRight) << " != "
                         << jsonEncodeStr(partitionSplits[i].right)
                         << endl;
                    different = true;
                }
            }
#endif
        }

        ExcAssert(!different);
    }

    uint32_t rowCount = decodedRows.size();
        
    std::vector<uint32_t> numInPartition(buckets.size());
        
    for (size_t i = 0;  i < rowCount;  ++i) {
        auto partition = partitions[i].partition();

        // Example is not in a partition
        if (partition == RowPartitionInfo::max())
            continue;

        int splitFeature = partitionSplits[partition].feature;
                
        if (splitFeature == -1) {
            // reached a leaf here, nothing to split                    
            partitions[i] = RowPartitionInfo::max();
            continue;
        }

        float weight = fabs(decodedRows[i]);
        bool label = decodedRows[i] < 0;
        int exampleNum = i;
            
        int leftPartition = newPartitionNumbers[partition].first;
        int rightPartition = newPartitionNumbers[partition].second;

        if (leftPartition == -1)
            continue;
        
        int splitValue = partitionSplits[partition].value;
        bool ordinal = features[splitFeature].ordinal;
        int bucket = features[splitFeature].buckets[exampleNum];
        //int bucket = featureBuckets[splitFeature][exampleNum];
        int side = ordinal ? bucket >= splitValue : bucket != splitValue;

        // Set the new partition number
        partitions[i] = side ? rightPartition : leftPartition;
            
        //cerr << "row " << i << " partition " << partition << " --> "
        //     << partitions[i] << " wt " << weight << " lbl "
        //     << label << " side " << side << endl;
        
        // Verify partition counts?
        if (checkPartitionCounts)
            ++numInPartition[partitions[i].partition()];
            
        // 0 = left to right, 1 = right to left
        int direction = partitionSplits[partition].transferDirection() == RL;

        // We only need to update features on the wrong side, as we
        // transfer the weight rather than sum it from the
        // beginning.  This means less work for unbalanced splits
        // (which are typically most of them, especially for discrete
        // buckets)

        if (direction != side) {
            int fromPartition = direction ? rightPartition : leftPartition;
            int toPartition = direction ? leftPartition : rightPartition;

            //cerr << "row " << i << " side " << side << " direction "
            //     << direction << " from " << fromPartition
            //     << " to " << toPartition << " l " << leftPartition
            //     << " r " << rightPartition
            //     << " wAll " << jsonEncodeStr(wAll[fromPartition])
            //     << " " << jsonEncodeStr(wAll[toPartition])
            //     << endl;

            if (checkPartitionCounts) {
                ExcAssertGreater(wAll[fromPartition].count(), 0);
            }
            
            // Update the wAll, transfering weight
            //wAll[fromPartition].sub(label, weight);
            wAll[toPartition  ].add(label, weight);
                    
            // Transfer the weight from each of the features
            for (auto & f: activeFeatures) {
                int startBucket = bucketOffsets[f];
                int bucket = features[f].buckets[exampleNum];
                
                //cerr << "  feature " << f << " bucket " << bucket
                //     << " offset " << startBucket + bucket << " lbl "
                //     << label << " weight " << weight << endl;

                if (checkPartitionCounts) {
                    if (getBucket(fromPartition, startBucket + bucket).count() == 0) {
                        cerr << "  feature " << f << " from "
                             << jsonEncodeStr(buckets[fromPartition][startBucket + bucket])
                             << " to " << jsonEncodeStr(buckets[toPartition][startBucket + bucket])
                             << endl;
                    }
                    
                    ExcAssertGreater
                        (getBucket(fromPartition, startBucket + bucket).count(), 0);
                }
                
                getBucket(toPartition, startBucket + bucket).add(label, weight);
            }
        }               
    }

    // Fix up all of the "from" buckets by subtracting the accumulated
    // counts from the "to" buckets.  This saves lots of work since we
    // only need to transfer the aggregate weight, not update row by row
    for (int i = 0;  i < partitionSplits.size();  ++i) {
        int leftPartition = newPartitionNumbers[i].first;
        int rightPartition = newPartitionNumbers[i].second;
        
        if (leftPartition == -1 || rightPartition == -1) {
            continue;
        }

        int fromPartition
            = partitionSplits[i].transferDirection() == RL
            ? rightPartition : leftPartition;
        int toPartition
            = partitionSplits[i].transferDirection() == RL
            ? leftPartition : rightPartition;

        wAll[fromPartition] -= wAll[toPartition];

        for (auto & f: activeFeatures) {
            int startBucket = bucketOffsets[f];
            int endBucket = bucketOffsets[f + 1];

            for (int b = startBucket; b < endBucket;  ++b) {
                if (getBucket(toPartition, b).count() == 0)
                    continue;
                getBucket(fromPartition, b) -= getBucket(toPartition, b);
            }
        }
    }
    
    // Make sure that the number in the buckets is actually what we
    // expected when we calculated the split.
    if (checkPartitionCounts) {
        cerr << "numInPartition = " << jsonEncodeStr(numInPartition) << endl;
        
        bool different = false;

        for (int i = 0;  i < partitionSplits.size();  ++i) {
            int leftPartition = newPartitionNumbers[i].first;
            int rightPartition = newPartitionNumbers[i].second;

            if (leftPartition == -1 || rightPartition == -1) {
                ExcAssertEqual(leftPartition, -1);
                ExcAssertEqual(rightPartition, -1);
                ExcAssertEqual(partitionSplits[i].feature, -1);
                continue;
            }
            
            if (numInPartition[leftPartition]
                != partitionSplits[i].left.count()
                || numInPartition[rightPartition]
                != partitionSplits[i].right.count()) {
                using namespace std;
                    
                cerr << "PARTITION COUNT MISMATCH" << endl;
                cerr << "expected: left "
                     << partitionSplits[i].left.count()
                     << " right "
                     << partitionSplits[i].right.count() << endl;
                cerr << "got:      left " << numInPartition[leftPartition]
                     << " right " << numInPartition[rightPartition]
                     << endl;

                cerr << "feature " << partitionSplits[i].feature
                     << " " << features[partitionSplits[i].feature].info->columnName
                     << " bucket " << partitionSplits[i].value
                     << " " << features[partitionSplits[i].feature]
                    .info->bucketDescriptions.getSplit(partitionSplits[i].value)
                     << " ordinal " << features[partitionSplits[i].feature].ordinal
                     << endl;


            }
                
            ExcAssertEqual(numInPartition[leftPartition],
                           partitionSplits[i].left.count());
            ExcAssertEqual(numInPartition[rightPartition],
                           partitionSplits[i].right.count());

            for (auto & f: activeFeatures) {
                W wLeft, wRight;
                int startBucket = bucketOffsets[f];
                int endBucket = bucketOffsets[f + 1];

                for (int b = startBucket; b < endBucket;  ++b) {
                    wLeft += getBucket(leftPartition, b);
                    wRight += getBucket(rightPartition, b);
                }

                if (wLeft != partitionSplits[i].left) {
                    cerr << "error on partition " << i << " left "
                         << leftPartition << " right " << rightPartition
                         << " feature " << f << ": left "
                         << jsonEncodeStr(wLeft) << " != "
                         << jsonEncodeStr(partitionSplits[i].left)
                         << endl;
                    different = true;
                }
                
                if (wRight != partitionSplits[i].right) {
                    cerr << "error on partition " << i << " left "
                         << leftPartition << " right " << rightPartition
                         << " feature " << f << ": right "
                         << jsonEncodeStr(wRight) << " != "
                         << jsonEncodeStr(partitionSplits[i].right)
                         << endl;
                    different = true;
                }
            }
            
        }

        ExcAssert(!different);
    }
}

std::pair<std::vector<PartitionEntry>,
          FrozenMemoryRegionT<uint32_t> >
splitPartitions(const std::span<const Feature> features,
                const std::span<const int> & activeFeatures,
                const std::span<const float> & decodedRows,
                const std::span<const RowPartitionInfo> & partitions,
                const std::span<const W> & w,
                const std::span<const PartitionIndex> & indexes,
                MappedSerializer & serializer)
{
    using namespace std;

    int numPartitions = w.size();
    int numRows = decodedRows.size();

    std::vector<PartitionEntry> out(numPartitions);
        
    MutableMemoryRegionT<uint32_t> mutablePartitionMemory;
    std::vector<size_t> partitionMemoryOffsets = { 0 };
    size_t partitionMemoryOffset = 0;
    FrozenMemoryRegionT<uint32_t> partitionMemory;

#if 0
    std::vector<uint32_t> partitionRowCounts(numPartitions);

    for (auto & p: partitions)
        ++partitionRowCounts[p];

    size_t rows1 = 0, rows2 = 0;
        
    for (int i = 0;  i < numPartitions;  ++i) {
        //cerr << "part " << i << " count " << w[i].count() << " rows "
        //     << partitionRowCounts[i] << endl;

        ExcAssertEqual(partitionRowCounts[i], w[i].count());

        rows1 += w[i].count();
        rows2 += partitionRowCounts[i];
    }
#endif
        
    for (int i = 0;  i < numPartitions;  ++i) {
        int partitionRowCount = w[i].count();

        if (partitionRowCount == 0) {
            partitionMemoryOffsets.push_back(partitionMemoryOffset);
            continue;
        }

        //cerr << "part " << i << " count " << w[i].count() << " rows "
        //     << partitionRowCounts[i] << endl;

        //ExcAssertEqual(partitionRowCount, partitionRowCounts[i]);

        out[i].decodedRows.reserve(partitionRowCount);
        out[i].activeFeatures = activeFeatures;  // TODO: pass in real
        out[i].activeFeatureSet.insert
            (activeFeatures.begin(), activeFeatures.end());
        out[i].bucketMemoryOffsets = { 0 };
        out[i].index = indexes[i];
        
        size_t bucketMemoryRequired = 0;

        out[i].features.resize(features.size());
        for (int f = 0;  f < features.size();  ++f) {
            size_t bytesRequired = 0;
            if (out[i].activeFeatureSet.count(f)) {
                out[i].features[f].info = features[f].info;
                out[i].features[f].ordinal = features[f].ordinal;
                out[i].features[f].active = true;
                size_t wordsRequired
                    = WritableBucketList::wordsRequired
                    (partitionRowCount,
                     features[f].info->distinctValues);
                bytesRequired = wordsRequired * 4;
            }

            bucketMemoryRequired += bytesRequired;
            out[i].bucketMemoryOffsets
                .push_back(bucketMemoryRequired);
        }

        partitionMemoryOffset += bucketMemoryRequired;
        partitionMemoryOffsets.push_back(partitionMemoryOffset);
    }

    mutablePartitionMemory
        = serializer.allocateWritableT<uint32_t>
        (partitionMemoryOffset / 4, page_size);

    for (int i = 0;  i < numPartitions;  ++i) {
        out[i].mutableBucketMemory
            = mutablePartitionMemory
            .rangeBytes(partitionMemoryOffsets[i],
                        partitionMemoryOffsets[i + 1]);

        int partitionRowCount = w[i].count();
        if (partitionRowCount == 0) {
            partitionMemoryOffsets.push_back(partitionMemoryOffset);
            continue;
        }
    }
        
    for (size_t i = 0;  i < numRows;  ++i) {
        auto partition = partitions[i].partition();
        out[partition].decodedRows.push_back(decodedRows[i]);
        bool label = decodedRows[i] < 0;
        float weight = fabs(decodedRows[i]);
        out[partition].wAll.add(label, weight);
    }
    
    for (int f: activeFeatures) {
        std::vector<WritableBucketList> partitionFeatures(numPartitions);
        for (int i = 0;  i < numPartitions;  ++i) {
            int partitionRowCount = w[i].count();
            if (partitionRowCount == 0)
                continue;
#if 0
            cerr << "feature " << f << " partition " << i << " of "
                 << numPartitions << " rowCount "
                 << partitionRowCount
                 << " bytes " << out[i].bucketMemoryOffsets[f]
                 << " to " << out[i].bucketMemoryOffsets[f + 1]
                 << endl;
#endif
            partitionFeatures.at(i).init(partitionRowCount,
                                         features[f].buckets.numBuckets,
                                         out.at(i).mutableBucketMemory
                                         .rangeBytes(out.at(i).bucketMemoryOffsets.at(f),
                                                     out.at(i).bucketMemoryOffsets.at(f + 1)));
        }

        for (size_t j = 0;  j < numRows;  ++j) {
            auto partition = partitions[j].partition();
            partitionFeatures[partition].write(features[f].buckets[j]);
        }

        for (int i = 0;  i < numPartitions;  ++i) {
            int partitionRowCount = w[i].count();
            if (partitionRowCount == 0)
                continue;
            out[i].features[f].buckets
                = partitionFeatures[i].freeze(serializer);
        }
    }

    for (int i = 0;  i < numPartitions;  ++i) {
        int partitionRowCount = w[i].count();
        if (partitionRowCount == 0)
            continue;

        ExcAssertEqual(jsonEncodeStr(w[i]), jsonEncodeStr(out[i].wAll));

        out[i].bucketMemory
            = out[i].mutableBucketMemory.freeze();
    }

    partitionMemory = mutablePartitionMemory.freeze();

    return std::make_pair(std::move(out), std::move(partitionMemory));
}

/** This function takes the W values of each bucket of a number of
    partitions, and calculates the optimal split feature and value
    for each of the partitions.  It's the "search" part of the
    decision tree algorithm, but across a full set of rows split into
    multiple partitions.
*/
std::vector<PartitionSplit>
getPartitionSplits(const std::span<const W> & buckets,  // [np][nb] for each partition, feature buckets
                   uint32_t numActiveBuckets,
                   const std::span<const int> & activeFeatures,       // [naf] list of feature numbers of active features only (< nf)
                   const std::span<const uint32_t> & bucketOffsets,   // [nf+1] offset in flat bucket list of start of feature
                   const std::span<const Feature> & features,         // [nf] feature info
                   const std::span<const W> & wAll,                   // [np] sum of buckets[0..nb-1] for each partition
                   const std::span<const PartitionIndex> & indexes,   // [np] index of each partition
                   bool parallel)
{
    size_t numPartitions = wAll.size();
    ExcAssertGreater(bucketOffsets.size(), 0);
    size_t numBuckets = bucketOffsets.back();  // Total num buckets over ALL features
    ExcAssertEqual(numBuckets, numActiveBuckets);
    //cerr << "bucketOffsets = " << jsonEncodeStr(bucketOffsets) << endl;
    ExcAssertEqual(indexes.size(), numPartitions);
    ExcAssertEqual(wAll.size(), numPartitions);
    ExcAssertGreaterEqual(buckets.size(), numPartitions * numActiveBuckets);

    std::vector<PartitionSplit> partitionSplits(numPartitions);

    for (int partition = 0;  partition < numPartitions;  ++partition) {

        if (wAll[partition].empty())
            continue;

        //cerr << "partition " << partition << " wAll " << jsonEncodeStr(wAll[partition]) << endl;

        double bestScore = INFINITY;
        int bestFeature = -1;
        int bestSplit = -1;
                
        W bestLeft;
        W bestRight;
        
        // Reduction over the best split that comes in feature by
        // feature; we find the best global split score and store
        // it.  This is done in order to be sure that we
        // deterministically pick the right one.
        auto findBest = [&] (int af,
                             const std::tuple<int, double, int, W, W> & val)
            {
                bool debug = false; //partition == 3 && buckets.size() == 8 && activeFeatures[af] == 4;

                double score = std::get<1>(val);

                if (score == INFINITY) return;

                if (debug) {
                    cerr << "part " << partition << " af " << af
                         << " f " << std::get<0>(val)
                         << " score " << std::get<1>(val) << " split "
                         << std::get<2>(val)
                         << endl;
                    cerr << "    score " << std::get<1>(val) << " "
                         << features[std::get<0>(val)].info->columnName
                         << " "
                         << features[std::get<0>(val)]
                        .info->bucketDescriptions.getSplit(std::get<2>(val))
                         << " l " << jsonEncodeStr(std::get<3>(val)) << " r "
                         << jsonEncodeStr(std::get<4>(val)) << endl;
                }
                
                if (score < bestScore) {
                    //cerr << "*** best" << endl;
                    std::tie(bestFeature, bestScore, bestSplit, bestLeft,
                             bestRight) = val;
                }
            };
            
        // Finally, we re-split
        auto doFeature = [&] (int af)
            {
                int f = activeFeatures[af];
                bool isActive = true;
                double bestScore = INFINITY;
                int bestSplit = -1;
                W bestLeft;
                W bestRight;

                bool debug = false; // partition == 3 && buckets.size() == 8 && activeFeatures[af] == 4;

                if (isActive && !buckets[partition].empty()
                    && wAll[partition].v[0] != 0.0 && wAll[partition].v[1] != 0.0) {
                    int startBucket = bucketOffsets[f];
                    int endBucket MLDB_UNUSED = bucketOffsets[f + 1];
                    int maxBucket = endBucket - startBucket - 1;
                    const W * wFeature
                        = buckets.data() + (partition * numActiveBuckets) + startBucket;
                    std::tie(bestScore, bestSplit, bestLeft, bestRight)
                        = chooseSplitKernel(wFeature, maxBucket,
                                            features[f].ordinal,
                                            wAll[partition], debug);
                }

                //cerr << "CPU: partition " << partition << " feature " << f << " score " << bestScore << endl;

                //cerr << " score " << bestScore << " split "
                //     << bestSplit << endl;
                        
                return std::make_tuple(f, bestScore, bestSplit,
                                       bestLeft, bestRight);
            };
            

        if (parallel) {
            parallelMapInOrderReduce(0, activeFeatures.size(),
                                     doFeature, findBest);
        }
        else {
            for (size_t i = 0;  i < activeFeatures.size();  ++i) {
                findBest(i, doFeature(i));
            }
        }

        partitionSplits[partition].index = indexes[partition];
        partitionSplits[partition].score = bestScore;
        partitionSplits[partition].feature = bestFeature;
        partitionSplits[partition].value = bestSplit;
        partitionSplits[partition].left = bestLeft;
        partitionSplits[partition].right = bestRight;
#if 0
        cerr << "partition " << partition << " of " << buckets.size()
             << " with " << wAll[partition].count()
             << " rows: " << bestScore << " " << bestFeature
             << " wAll " << jsonEncodeStr(wAll[partition]);
        if (bestFeature != -1) {
            cerr << " " << features[bestFeature].info->columnName
                 << " " << bestSplit
                 << " " << features[bestFeature].info->bucketDescriptions.getSplit(bestSplit);
        }
        cerr << " " << jsonEncodeStr(bestLeft) << " "
             << jsonEncodeStr(bestRight)
             << " dir " << partitionSplits[partition].direction
             << endl;
#endif
    }

    return partitionSplits;
}

void
getPartitionSplitsKernel(ComputeContext & context,
                         uint32_t f, uint32_t nf,
                         uint32_t p, uint32_t np,
                         
                         uint32_t totalBuckets,
                         std::span<const uint32_t> bucketNumbers, // [nf]
                         
                         std::span<const uint32_t> featureActive, // [nf]
                         std::span<const uint32_t> featureIsOrdinal, // [nf]
                         
                         std::span<const W> buckets, // [np x totalBuckets]

                         std::span<const W> wAll,  // [np] one per partition
                         std::span<PartitionSplit> splitsOut) // [np x nf]
{
    PartitionSplit & result = splitsOut[p * nf + f];
    if (!featureActive[f] || wAll[p].empty() || wAll[p].uniform()) {
        result = PartitionSplit();
        return;
    }

    int startBucket = bucketNumbers[f];
    int endBucket = bucketNumbers[f + 1];
    int maxBucket = endBucket - startBucket - 1;
    const W * wFeature = buckets.data() + (p * totalBuckets) + startBucket;
    result.feature = f;
    std::tie(result.score, result.value, result.left, result.right)
        = chooseSplitKernel(wFeature, maxBucket, featureIsOrdinal[f], wAll[p], false /* debug */);
}

void
bestPartitionSplitKernel(ComputeContext & context,
                         uint32_t p, uint32_t np,
                         uint32_t nf,
                         std::span<const uint32_t> featureActive, // [nf]
                         std::span<const PartitionSplit> featurePartitionSplits, // [np x nf]
                         std::span<PartitionSplit> partitionSplitsOut,  // np
                         uint32_t partitionSplitsOffset)
{
    ExcAssertLess(partitionSplitsOffset + p, partitionSplitsOut.size());
    PartitionSplit & result = partitionSplitsOut[partitionSplitsOffset + p];
    result = PartitionSplit();

    for (size_t f = 0;  f < nf;  ++f) {
        if (!featureActive[f])
            continue;
        const PartitionSplit & fp = featurePartitionSplits[p * nf + f];
        if (fp.score == INFINITY)
            continue;
        //cerr << "kernel: partition " << p << " feature " << f << " score " << fp.score << endl;
        if (fp.score < result.score) {
            result = fp;
        }
    }

    result.index = p + partitionSplitsOffset;
}

void
clearBucketsKernel(ComputeContext & context,
                   uint32_t partition, uint32_t numPartitions,
                   ComputeKernelGridRange & bucketRange,
                   std::span<W> allPartitionBuckets,
                   std::span<W> wAll,
                   std::span<const PartitionSplit> partitionSplits,
                   uint32_t numActiveBuckets,
                   uint32_t partitionSplitsOffset)
{
    ExcAssertLess(partitionSplitsOffset + partition, partitionSplits.size());
    ExcAssertEqual(partitionSplitsOffset, numPartitions);

    uint32_t numBuckets = bucketRange.range();
    ExcAssertEqual(numBuckets, numActiveBuckets);

    for (uint32_t bucket: bucketRange) {
        ExcAssertLess(bucket, numActiveBuckets);

        //if (partitionSplits[partition].right.count() == 0) // unused partition
        //    return;
        
        allPartitionBuckets[(partitionSplitsOffset + partition) * numActiveBuckets + bucket] = W();
        
        if (bucket == 0) {
            wAll[partitionSplitsOffset + partition] = W();
        }
    }
}

void
updatePartitionNumbersKernel(ComputeContext & context,
                             ComputeKernelGridRange & rowRange,
          
                             uint32_t rightOffset,
                             
                             std::span<RowPartitionInfo> partitions,
                             std::span<uint8_t> directions,
          
                             std::span<const PartitionSplit> partitionSplits,
                             
                             // Feature data
                             std::span<const uint32_t> allBucketData,
                             std::span<const uint32_t> bucketDataOffsets,
                             std::span<const uint32_t> bucketNumbers,
                             std::span<const uint32_t> bucketEntryBits,
          
                             std::span<const uint32_t> featureIsOrdinal)
{
    // Skip to where we should be in our partition splits
    partitionSplits = partitionSplits.subspan(rightOffset);
    uint32_t nf = bucketEntryBits.size();

    BucketList featureBuckets[nf];

    for (uint32_t r: rowRange) {
        auto partition = partitions[r].partition();
        int splitFeature = partitionSplits[partition].feature;
                
        if (splitFeature == -1) {
            // reached a leaf here, nothing to split
            directions[r] = 0;
            continue;
        }

        uint32_t splitValue = partitionSplits[partition].value;
        bool ordinal = featureIsOrdinal[splitFeature];
        
        // Get buckets for the split feature
        BucketList & buckets = featureBuckets[splitFeature];
        if (buckets.numBuckets != -1) {
            // not initialized
            buckets.entryBits = bucketEntryBits[splitFeature];
            buckets.numBuckets = -1;  // unused but we use to track initialized or not
            buckets.numEntries = bucketNumbers[splitFeature + 1] - bucketNumbers[splitFeature];
            buckets.storagePtr = allBucketData.data() + bucketDataOffsets[splitFeature];
        }

        uint32_t bucket = buckets[r];

        uint32_t side = ordinal ? bucket >= splitValue : bucket != splitValue;

        // 0 = left to right, 1 = right to left
        uint32_t direction = partitionSplits[partition].transferDirection() == RL;
        directions[r] = direction;

        // Set the new partition number
        partitions[r] = partition + side * rightOffset;

        //cerr << "row " << r << " side " << side << " currently in " << partitionSplits[partition].index
        //     << " goes from partition " << partition << " (" << PartitionIndex(rightOffset + partition)
        //     << ") to partition " << partition + side * rightOffset << " ("
        //     << PartitionIndex(rightOffset + partition + side * rightOffset) << ")" << endl;
    }
}

void
updateBucketsKernel(ComputeContext & context,
                    uint32_t fp1, uint32_t nfp1,

                    uint32_t rightOffset,
                    uint32_t numActiveBuckets,
                    
                    std::span<const RowPartitionInfo> partitions,
                    std::span<const uint8_t> directions,
                    std::span<W> partitionBuckets,
                    std::span<W> wAll,

                    std::span<const PartitionSplit> partitionSplits,
                    
                    // Row data
                    std::span<const float> decodedRows,
                    uint32_t rowCount,
                    
                    // Feature data
                    std::span<const uint32_t> allBucketData,
                    std::span<const uint32_t> bucketDataOffsets,
                    std::span<const uint32_t> bucketNumbers,
                    std::span<const uint32_t> bucketEntryBits,

                    std::span<const uint32_t> featureActive,
                    std::span<const uint32_t> featureIsOrdinal)
{
    int f = fp1 - 1;  // -1 means wAll, otherwise it's the feature number

    if (f != -1 && !featureActive[f])
        return;

    // We have to set up to access to buckets for the feature we're updating for the split (f)
    BucketList buckets;
    if (f != -1) {
        buckets.entryBits = bucketEntryBits[f];
        buckets.numBuckets = -1;  // unused but set to an invalid value to be sure
        buckets.numEntries = bucketNumbers[f + 1] - bucketNumbers[f];
        buckets.storagePtr = allBucketData.data() + bucketDataOffsets[f];
    }

    // Skip to where we should be in our partition splits
    partitionSplits = partitionSplits.subspan(rightOffset);
    
    uint32_t startBucket;

    // Pointer to the global array we eventually want to update
    std::span<W> wGlobal;

    // We always transfer weights from the left to the right, and then
    // in a later kernel swap them
    if (f == -1) {
        wGlobal = wAll;
        startBucket = 0;
    }
    else {
        wGlobal = partitionBuckets;
        startBucket = bucketNumbers[f];
    }

    //size_t numSkipped = 0;

    for (uint32_t i = 0;  i < rowCount;  ++i) {

        // Partition was updated already, so here we mask out the offset to know where the
        // partition came from (ensure that this partition number is parent partition number)
        uint16_t partition = partitions[i] & (rightOffset - 1);
        bool side = partition != partitions[i];  // it is on the right if the partition number changed
        //uint32_t direction = partitionSplits[partition].transferDirection() == RL;
        //if (direction != directions[i]) {
        //    cerr << "direction = " << direction << endl;
        //    cerr << "directions[i] = " << directions[i] << endl;
        //    cerr << "rowNumber " << i << endl;
        //    cerr << "partition " << partition << endl;
        //    cerr << "split " << jsonEncodeStr(partitionSplits[partition]) << endl;
        //}
        //ExcAssertEqual(direction, directions[i]);
        bool direction = directions[i];

        //if (f == -1)
        //    cerr << "partition " << partition << " side = " << side << " direction " << direction << endl;

        // We only need to update features on the wrong side, as we
        // transfer the weight rather than sum it from the
        // beginning.  This means less work for unbalanced splits
        // (which are typically most of them, especially for discrete
        // buckets)

        if (direction == side) {
            //++numSkipped;
            continue;
        }

        uint32_t rightPartition = partition + rightOffset;
        uint32_t toBucket;
        
        if (f == -1) {
            // Since we don't touch the buckets on the left side of the
            // partition, we don't need to have local accumulators for them.
            // Hence, the partition number for local is the left partition.
            toBucket = rightPartition;
        }
        else {
            uint32_t bucket = buckets[i];
            toBucket = rightPartition * numActiveBuckets + startBucket + bucket;
        }

        float weight = fabs(decodedRows[i]);
        bool label = decodedRows[i] < 0;

        // TODO: needs to be an atomic add when multi-threaded...
        wGlobal[toBucket].add(label, weight);
    }

    //cerr << "numSkipped = " << numSkipped << " of " << rowCount << endl;
}

// For each partition and each bucket, we up to now accumulated just the
// weight that needs to be transferred in the right hand side of the
// partition splits.  We need to fix this up by:
// 1.  Subtracting this weight from the left hand side
// 2.  Flipping the buckets where the big amount of weight belongs on the
//     right not on the left.
//
// This is a 2 dimensional kernel:
// Dimension 0 = partition number (from 0 to the old number of partitions)
// Dimension 1 = bucket number (from 0 to the number of active buckets)
void
fixupBucketsKernel(ComputeContext & context,
                   uint32_t partition, uint32_t numPartitions,
                   ComputeKernelGridRange & bucketRange,
                   std::span<W> allPartitionBuckets,
                   std::span<W> wAll,
                   std::span<const PartitionSplit> partitionSplits)
{
    uint32_t numActiveBuckets = bucketRange.range();
    uint32_t partitionSplitsOffset = numPartitions;
    partitionSplits = partitionSplits.subspan(partitionSplitsOffset);

    //if (partitionSplits[partition].right.count() == 0)
    //    return;
            
    std::span<W> bucketsLeft
        = allPartitionBuckets.subspan(partition * numActiveBuckets);
    std::span<W> bucketsRight
        = allPartitionBuckets.subspan((partition + numPartitions) * numActiveBuckets);
    
    bool hasZero = false;
    for (uint32_t bucket: bucketRange) {
        hasZero = hasZero || bucket == 0;
        // We double the number of partitions.  The left all
        // go with the lower partition numbers, the right have the higher
        // partition numbers.
        bucketsLeft[bucket] -= bucketsRight[bucket];
        
        if (partitionSplits[partition].transferDirection() == RL) {
            // We need to swap the buckets
            std::swap(bucketsLeft[bucket], bucketsRight[bucket]);
        }
    }

    // Bucket zero also updates wAll
    if (hasZero) {
        //cerr << "bucket == 0: left " << jsonEncodeStr(wAll[partition])
        //     << " right " << jsonEncodeStr(wAll[partition + numPartitions]) << endl;
        wAll[partition] -= wAll[partition + numPartitions];

        if (partitionSplits[partition].transferDirection() == RL) {
            std::swap(wAll[partition], wAll[partition + numPartitions]);
        }
    }
}


// Check that the partition counts match the W counts.
void
verifyPartitionBuckets(const std::span<const RowPartitionInfo> & partitions,
                       const std::span<const W> & wAll)
{
    using namespace std;
        
    int numPartitions = wAll.size();
        
    // Check that our partition counts and W scores match
    std::vector<uint32_t> partitionRowCounts(numPartitions);

    for (auto & p: partitions) {
        if (p != RowPartitionInfo::max())
            ++partitionRowCounts[p];
    }

    //for (int i = 0;  i < numPartitions;  ++i) {
    //    cerr << "part " << i << " count " << wAll[i].count() << " rows "
    //         << partitionRowCounts[i] << endl;
    //}

    bool different = false;
    for (int i = 0;  i < numPartitions;  ++i) {
        if (partitionRowCounts[i] != wAll[i].count()) {
            different = true;
            cerr << "error on partition " << i << ": row count "
                 << partitionRowCounts[i] << " wAll count "
                 << wAll[i].count() << endl;
        }
    }
    ExcAssert(!different);
}

// Split our dataset into a separate dataset for each leaf, and
// recurse to create a leaf node for each.  This is mutually
// recursive with trainPartitionedRecursive.
std::map<PartitionIndex, ML::Tree::Ptr>
splitAndRecursePartitioned(int depth, int maxDepth,
                           ML::Tree & tree,
                           MappedSerializer & serializer,
                           const std::span<const W> & buckets,
                           uint32_t numActiveBuckets,
                           const std::span<const uint32_t> & bucketOffsets,
                           const std::span<const Feature> & features,
                           const std::span<const int> & activeFeatures,
                           const std::span<const float> & decodedRows,
                           const std::span<const RowPartitionInfo> & partitions,
                           const std::span<const W> & wAll,
                           const std::span<const PartitionIndex> & indexes,
                           const DatasetFeatureSpace & fs,
                           FrozenMemoryRegionT<uint32_t> bucketMemory)
{
    size_t numRows = decodedRows.size();
    ExcAssertEqual(partitions.size(), numRows);

    size_t numFeatures = features.size();
    ExcAssertEqual(bucketOffsets.size(), numFeatures + 1);

    size_t numPartitions = wAll.size();
    ExcAssertEqual(wAll.size(), numPartitions);
    ExcAssertEqual(indexes.size(), numPartitions);
    ExcAssertGreaterEqual(buckets.size(), numPartitions * numActiveBuckets);
    
    std::vector<std::pair<PartitionIndex, ML::Tree::Ptr> > leaves;

    if (depth == maxDepth)
        return { };
        
    leaves.resize(numPartitions); // TODO: we double copy leaves into result

    // New partitions, per row
    std::vector<PartitionEntry> newData;
    FrozenMemoryRegionT<uint32_t> partitionMem;

    std::tie(newData, partitionMem)
        = splitPartitions(features, activeFeatures, decodedRows,
                          partitions, wAll, indexes, serializer);

    cerr << "splitAndRecursePartitioned: got " << newData.size() << " partitions" << endl;

    auto doEntry = [&] (int i)
        {
            if (newData[i].index == PartitionIndex::none())
                return;

            //cerr << "training leaf for index " << newData[i].index << " with " << newData[i].decodedRows.size() << " rows" << endl;

            leaves[i].first = newData[i].index;
            leaves[i].second = trainPartitionedRecursive
                (depth, maxDepth,
                 tree, serializer,
                 bucketOffsets, newData[i].activeFeatures,
                 buckets.subspan(i * numActiveBuckets, numActiveBuckets),
                 newData[i].decodedRows,
                 newData[i].wAll,
                 newData[i].index,
                 fs,
                 newData[i].features,
                 bucketMemory);
        };

    if (depth <= 8) {
        parallelMap(0, numPartitions, doEntry);
    }
    else {
        for (size_t i = 0;  i < numPartitions;  ++i) {
            doEntry(i);
        }
    }

    std::map<PartitionIndex, ML::Tree::Ptr> result;
    for (auto index_branch: leaves) {
        PartitionIndex index = index_branch.first;
        ML::Tree::Ptr branch = index_branch.second;
        if (index == PartitionIndex::none())
            continue;
        result.emplace_hint(result.end(), index, branch);
    }

    return result;
}

struct PartitionExample {
    int ex;
    float row;
};

struct PartitionWorkEntry {
#if 0
    PartitionWorkEntry() = default;
    PartitionWorkEntry(PartitionIndex index,
                       PartitionExample * ex,
                       size_t numExamples,
                       W wAll,
                       std::vector<W> buckets)
        : index(index), ex(ex), numExamples(numExamples), wAll(wAll),
          buckets(std::move(buckets))
    {
    }
#endif
    
    PartitionIndex index;
    PartitionExample * ex;
    size_t numExamples;
    W wAll;
    std::vector<W> buckets;
};

// Go depth-first down a small partition until we reach the recursion limit,
// outputting as we go a set of splits and a set of work items to descend
// into.
//
// If buckets are passed in non-empty, they should represent the bucket
// weights of this partition.  Otherwise, they will be initialized at the
// beginning of the recursion.
//
// This function uses constant memory for the entire depth, and will
// create up to 2^d splits and O(d^2) extra work items where
// d = (index.depth() - maxDepth).  This means it is much more efficient in
// memory usage than a breath-first algorithm, which has temporary storage
// requirements in the order of O(2^d) not O(d^2).
//
// It is used as a counterpoint to the breadth-first algorithm for when
// the width is so high that the buckets are very sparse.

std::map<PartitionIndex, PartitionSplit>
descendSmallPartition(PartitionWorkEntry entry,
                      int maxDepth,
                      int numActiveBuckets,
                      const std::span<const Feature> & features,
                      const std::span<const int> & activeFeatures,
                      const std::span<const uint32_t> & bucketOffsets)
{
    PartitionIndex rootIndex = entry.index;
    std::map<PartitionIndex, PartitionSplit> splits;

    if (rootIndex.depth() >= maxDepth)
        return splits;
    
    // This is a stack of work we put off for later and need to come back to.
    // It should never be more than maxDepth - rootIndex.depth() entries, as
    // it traces a path back to the root of the tree.  Typically, work items
    // will get smaller as they are pushed on to this stack, so the last set
    // will be extremely small.
    
    std::vector<PartitionWorkEntry> work = { std::move(entry) };

    while (!work.empty()) {
        PartitionWorkEntry entry = std::move(work.back());
        work.pop_back();

        //cerr << "working on " << entry.index.path() << " with "
        //     << entry.numExamples << " examples and " << work.size()
        //     << " outstanding work" << endl;
        
        PartitionIndex index = entry.index;
        PartitionExample * ex = entry.ex;
        size_t numExamples = entry.numExamples;
        W & wAll = entry.wAll;
        std::vector<W> & buckets = entry.buckets;
        
        // If we weren't passed in our buckets, we re-initialize them here.
        // Passing them in requires much more memory, but saves some work in
        // initializing them.
        if (buckets.empty()) {
            buckets.resize(numActiveBuckets);
            
            for (int f: activeFeatures) {
                //cerr << "initializing feature " << f << " ofs "
                //     << bucketOffsets[f] << endl;
                W * featureBuckets = buckets.data() + bucketOffsets[f];
                for (int i = 0;  i < numExamples;  ++i) {
                    float weight = fabs(ex[i].row);
                    bool label = ex[i].row < 0;
                    int exampleNum = ex[i].ex;
                
                    int bucket = features[f].buckets[exampleNum];

                    //cerr << "ex " << i << " weight " << weight << " lbl "
                    //     << label << " exnum " << exampleNum << " bucket "
                    //     << bucket << endl;

                    featureBuckets[bucket].add(label, weight);
                }

                //for (int i = bucketOffsets[f];  i < bucketOffsets[f + 1];  ++i) {
                //    if (buckets[i].count() > 0) {
                //        cerr << "bucket " << i - bucketOffsets[f]
                //             << " value " << jsonEncodeStr(buckets[i]) << endl;
                //    }
                //}
            }
        }

        // Loop over all depths, keeping to the widest at each point
        for (;;) {
            //cerr << "looping with index " << index.path() << endl;

            static constexpr bool verifyBuckets = false;
            
            if (verifyBuckets) {
                for (int f: activeFeatures) {
                    int numBuckets = bucketOffsets[f + 1] - bucketOffsets[f];
                    std::vector<W> testFeatureBuckets(numBuckets);
                    for (int i = 0;  i < numExamples;  ++i) {
                        float weight = fabs(ex[i].row);
                        bool label = ex[i].row < 0;
                        int exampleNum = ex[i].ex;
                
                        int bucket = features[f].buckets[exampleNum];

                        //cerr << "ex " << i << " weight " << weight << " lbl "
                        //     << label << " exnum " << exampleNum << " bucket "
                        //     << bucket << endl;
                        
                        testFeatureBuckets[bucket].add(label, weight);
                    }

                    const W * featureBuckets = buckets.data() + bucketOffsets[f];

                    bool different = false;
                    
                    for (int i = 0;  i < numBuckets;  ++i) {
                        if (testFeatureBuckets[i] != featureBuckets[i]) {
                            cerr << "difference on feature " << f
                                 << " bucket " << i << ": is "
                                 << jsonEncodeStr(featureBuckets[i])
                                 << " should be "
                                 << jsonEncodeStr(testFeatureBuckets[i])
                                 << endl;
                            different = true;
                        }
                    }

                    ExcAssert(!different);
                }
            }

            
            PartitionSplit split;

            for (int f: activeFeatures) {
                W * featureBuckets = buckets.data() + bucketOffsets[f];
                int maxBucket = bucketOffsets[f + 1] - bucketOffsets[f] - 1;
            
                // Get the best split
                float featureScore;
                int featureSplit;
                W featureLeft, featureRight;
                    
                std::tie(featureScore, featureSplit, featureLeft, featureRight)
                    = chooseSplitKernel(featureBuckets, maxBucket,
                                        features[f].ordinal, wAll);

#if 0                
                cerr << "  feature " << f << " score " << featureScore
                     << " maxBucket " << maxBucket
                     << " feat " << features[f].info->columnName;
                if (featureSplit != -1) {
                    cerr << " "
                         << features[f].info->bucketDescriptions.getSplit(featureSplit);
                }
                cerr << endl;
#endif
                
                if (featureScore < split.score) {
                    //cerr << "    best" << endl;
                    split.score = featureScore;
                    split.feature = f;
                    split.value = featureSplit;
                    split.left = featureLeft;
                    split.right = featureRight;
                }
            }

            split.index = index;
            
            //cerr << "  split = " << jsonEncodeStr(split) << endl;
            
            if (split.feature == -1)
                break;

            //cerr << "produced split " << jsonEncodeStr(split) << endl;
            
            splits.emplace(index, std::move(split));

            if (index.depth() == maxDepth - 1
                || split.left.count() == 0 || split.right.count() == 0) {
                // We've finished
                break;
            }
                
            // Now we have our best split for the bucket.  We need to
            // split the partition, decide which side we recurse on,
            // push a new work item for the one we don't keep and fix
            // up our buckets.
                
            //cerr << "direction = " << direction << endl;

            const auto & featureBuckets = features[split.feature].buckets;
            bool splitOrdinal = features[split.feature].ordinal;
            
            // Works out which side of a split an element will be on
            auto getSide = [&] (const PartitionExample & example) -> bool
                {
                    int exampleNum = example.ex;
                    int bucket = featureBuckets[exampleNum];
                    int side = splitOrdinal
                        ? bucket >= split.value : bucket != split.value;
                    return side;
                };

            // Partition them into left and right
            PartitionExample * midpoint
                = std::partition(ex, ex + numExamples, getSide);

            PartitionExample * big, * small;
            size_t bigLen, smallLen;
            PartitionIndex bigIndex, smallIndex;
            W bigWAll, smallWAll;
            if (split.transferDirection() == RL) {
                // Biggest on right
                big = ex;
                bigLen = midpoint - ex;
                small = midpoint;
                smallLen = numExamples - bigLen;  

                bigIndex = index.rightChild();
                smallIndex = index.leftChild();
                bigWAll = split.right;
                smallWAll = split.left;
                
                //cerr << "smallLen = " << smallLen << " bigLen = " << bigLen
                //     << endl;

                ExcAssertEqual(bigLen, split.right.count());
                ExcAssertEqual(smallLen, split.left.count());
            }
            else {
                // Smallest on right
                small = ex;
                smallLen = midpoint - ex;
                big = midpoint;
                bigLen = numExamples - smallLen;

                smallIndex = index.rightChild();
                bigIndex = index.leftChild();
                smallWAll = split.right;
                bigWAll = split.left;

                //cerr << "smallLen = " << smallLen << " bigLen = " << bigLen
                //     << endl;

                ExcAssertEqual(smallLen, split.right.count());
                ExcAssertEqual(bigLen, split.left.count());
            }

            ExcAssertGreaterEqual(bigLen, smallLen);
            
            // Finally, update our W for the big one by subtracting all
            // of the values that were moved out for the small length
                
            for (int f: activeFeatures) {
                W * featureBuckets = buckets.data() + bucketOffsets[f];
                for (int i = 0;  i < smallLen;  ++i) {
                    float weight = fabs(small[i].row);
                    bool label = small[i].row < 0;
                    int exampleNum = small[i].ex;
                    
                    int bucket = features[f].buckets[exampleNum];
                    featureBuckets[bucket].sub(label, weight);
                }
            }

            // At this point, we can:
            // - recurse for the big bucket
            // - push a new work item for the small bucket
            work.push_back({smallIndex, small, smallLen, smallWAll,
                           {} /* buckets */});
            
            // Update for next iteration
            index = bigIndex;
            wAll = bigWAll;
            ex = big;
            numExamples = bigLen;
        }
    }

    return splits;
}            

std::map<PartitionIndex, ML::Tree::Ptr>
trainSmallPartitions(int depth, int maxDepth,
                     ML::Tree & tree,
                     const std::span<const Feature> & features,
                     std::vector<RowPartitionInfo> & partitions,
                     const std::span<W> & buckets,
                     uint32_t numActiveBuckets,
                     const std::span<const uint32_t> & bucketOffsets,
                     const std::span<const float> & decodedRows,
                     const std::span<const int> & activeFeatures,
                     const std::span<const std::pair<int, PartitionSplit> > & smallPartitions,
                     const DatasetFeatureSpace & fs)
{
    std::map<PartitionIndex, ML::Tree::Ptr> result;

    // Mapping from partition number to small partition number
    std::vector<int> smallPartitionNumbers(buckets.size(), -1);

    size_t keptRows = 0;

    //for (auto & p: smallPartitions) {
    //    cerr << "small part " << p.first << " split " << jsonEncodeStr(p.second)
    //         << endl;
    //}
    
    // First, split into data structure for each partition    
    for (int i = 0;  i < smallPartitions.size();  ++i) {
        int partitionNumber = smallPartitions[i].first;
        const PartitionSplit & split = smallPartitions[i].second;
        keptRows += split.left.count() + split.right.count();
        smallPartitionNumbers.at(partitionNumber) = i;
    }

    // We now have twice as many partitions... one for the left and
    // one for the right of each
    int numPartitions = smallPartitions.size() * 2;

    // Structure containing the
    struct AugmentedPartitionExample: public PartitionExample {
        AugmentedPartitionExample() = default;
        AugmentedPartitionExample(int ex, float row, int part)
            : PartitionExample{ex,row}, part(part)
        {
        }
        
        int part;  // partition number

        bool operator < (const AugmentedPartitionExample & other) const
        {
            return part < other.part
               || (part == other.part && ex < other.ex);
        }
    };

    //cerr << "smallPartitionNumbers = " << jsonEncodeStr(smallPartitionNumbers)
    //     << endl;
    
    // Now extract for each partition.  This means applying the split
    std::vector<AugmentedPartitionExample> allExamples;
    allExamples.reserve(keptRows);
    
    int rowCount = decodedRows.size();

    for (int i = 0;  i < rowCount;  ++i) {

        auto partition = partitions[i].partition();

        // Example is not in a partition
        if (partition >= smallPartitionNumbers.size()) {
            ExcAssertEqual(partition, RowPartitionInfo::max());
            continue;
        }

        int smallNum = smallPartitionNumbers.at(partition);

        // Example is not in a small partition we're dealing with
        if (smallNum == -1)
            continue;

        // Update the partition number to say that we're no longer
        // part of it.  TODO: don't require mutable references to be passed
        // in.
        partitions[i] = -1;
        
        int splitFeature = smallPartitions[smallNum].second.feature;

        if (splitFeature == -1) {
            // reached a leaf here, nothing to split                    
            continue;
        }

        int exampleNum = i;
            
        int splitValue = smallPartitions[smallNum].second.value;
        bool ordinal = features[splitFeature].ordinal;
        int bucket = features[splitFeature].buckets[exampleNum];
        int side = ordinal ? bucket >= splitValue : bucket != splitValue;

        int partitionNum = smallNum * 2 + side;

        //cerr << "example " << i << " partition " << partition
        //     << "  smallNum " << smallNum
        //     << "  splitFeature " << splitFeature
        //     << "  kept in partition " << partitionNum << endl;
        
        allExamples.emplace_back(exampleNum, decodedRows[i], partitionNum);
    }

    //cerr << "allExamples.size() = " << allExamples.size()
    //     << " keptRows = " << keptRows << endl;
    
    ExcAssertEqual(allExamples.size(), keptRows);
    
    // Now we have our entire set of examples extracted.  We go through and
    // sort them by partition number.
    std::sort(allExamples.begin(), allExamples.end());

    // Index the example number for the beginning of each partition.  This
    // enables us to know where to to for each one.
    std::vector<int> partitionStarts;
    int currentPartition = -1;

    for (int i = 0;  i < allExamples.size();  ++i) {
        int part = allExamples[i].part;
        //cerr << "row " << i << " example " << allExamples[i].ex
        //     << " in partition " << part << endl;
        while (currentPartition < part) {
            partitionStarts.push_back(i);
            ++currentPartition;
        }
    }

    while (currentPartition < numPartitions) {
        partitionStarts.push_back(allExamples.size());
        ++currentPartition;
    }

    //cerr << "partitionStarts = " << jsonEncodeStr(partitionStarts) << endl;
    
    std::vector<PartitionExample> examples
        { allExamples.begin(), allExamples.end() };
    
    for (int i = 0;  i < numPartitions;  ++i) {
        
        int smallPartitionNum = i / 2;
        int lr = i % 2;

        const PartitionSplit & split
            = smallPartitions[smallPartitionNum].second;
        
        PartitionIndex index
            = lr == 0 ? split.index.leftChild() : split.index.rightChild();

        const W & wAll = lr == 0 ? split.left : split.right;

        std::vector<W> buckets;  // TODO: pass this; they can be constructed
        
        PartitionWorkEntry entry {
            index,
            examples.data() + partitionStarts[i],
            static_cast<size_t>(partitionStarts[i + 1] - partitionStarts[i]),
            wAll,
            std::move(buckets)
        };
        
        std::map<PartitionIndex, PartitionSplit> splits
            = descendSmallPartition(entry, maxDepth, numActiveBuckets,
                                    features, activeFeatures, bucketOffsets);

        ML::Tree::Ptr leaf
            = extractTree(depth + 1, maxDepth, tree, index, splits,
                          {} /* leaves */, features, fs);
            
        result[index] = leaf;
    }

    return result;
}

// Maximum number of partitions to handle in parallel in the breadth
// first algorithm?  If it's wider than this, the smaller partitions
// will be handled depth first.
EnvOption<int>
MLDB_RF_CPU_PARTITION_MAX_WIDTH("MLDB_RF_CPU_PARTITION_MAX_WIDTH", 4096);

// Threshold to decide a partition is too small to handle in parallel,
// expressed as a multiple of the number of buckets.  If there are less
// than this number of rows in the bucket, then it will be handled depth
// first in a more memory efficient manner.  The intuition is that there
// is no point keeping large amounts of memory for buckets when there
// are more buckets than examples.  Set to zero to disable the breadth
// first algorithm.
EnvOption<float>
MLDB_RF_CPU_MIN_PARTITION_EXAMPLE_MULTIPLIER
    ("MLDB_RF_CPU_MIN_PARTITION_EXAMPLE_MULTIPLIER", 1.0);

ML::Tree::Ptr
trainPartitionedRecursiveCpu(int depth, int maxDepth,
                             ML::Tree & tree,
                             MappedSerializer & serializer,
                             const std::span<const uint32_t> & bucketOffsets,
                             const std::span<const int> & activeFeatures,
                             const std::span<const W> bucketsIn,
                             const std::span<const float> & decodedRows,
                             const W & wAllInput,
                             PartitionIndex root,
                             const DatasetFeatureSpace & fs,
                             const std::span<const Feature> & features,
                             FrozenMemoryRegionT<uint32_t> bucketMemory)
{
    constexpr bool verifyBuckets = false;
    constexpr bool debug = false;

    using namespace std;

    //cerr << "trainPartitionedRecursiveCpu: root = " << root << " depth = " << depth << " maxDepth = " << maxDepth << endl;

    int rowCount = decodedRows.size();

    int maxDepthBeforeRecurse = std::min(maxDepth - depth, 20);

    uint32_t numPartitions = 1 << maxDepthBeforeRecurse;
    uint32_t numActiveBuckets = bucketsIn.size();

    // This is our total for each bucket across the whole lot
    // We keep track of it per-partition (there are up to 256
    // partitions, which corresponds to a depth of 8)
    std::vector<W> buckets;
    buckets.reserve(numPartitions * numActiveBuckets);  // it's a 2d matrix
    buckets.insert(buckets.end(), bucketsIn.begin(), bucketsIn.end());  // initialize first row

    // Which partition is each row in?  Initially, everything
    // is in partition zero, but as we start to split, we end up
    // splitting them amongst many partitions.  Each partition has
    // its own set of buckets that we maintain.
    // Note that these are indexes into the partition list, to
    // enable sparseness in the tree.
    std::vector<RowPartitionInfo> partitions(rowCount);

    // What is the index (position in the tree) for each partition?
    std::vector<PartitionIndex> indexes;
    indexes.reserve(numPartitions);
    indexes.emplace_back(root);  // we're at the root of the tree
    
    // What is our weight total for each of our partitions?
    std::vector<W> wAll = { wAllInput };
    wAll.reserve(numPartitions);
        
    // Record the split, per level, per partition
    std::map<PartitionIndex, PartitionSplit> allSplits;

    // Maximum width to process.  If we exceed this width, we divide and
    // conquer before preceeding.  This stops us from having a set of
    // buckets so wide that we trash the cache.
    int maxWidth MLDB_UNUSED = MLDB_RF_CPU_PARTITION_MAX_WIDTH;

    // Minimum number of examples in a bucket to use the parallel algorithm.
    // Once a part of the tree gets below this threshold, it's processed
    // using an iterative algorithm.
    int minExamplesPerBucket
        = MLDB_RF_CPU_MIN_PARTITION_EXAMPLE_MULTIPLIER * numActiveBuckets;

    Date start = Date::now();

    int originalDepth = depth;

    std::map<PartitionIndex, ML::Tree::Ptr> leaves;
    
    // We go down level by level
    for (int myDepth = 0; myDepth < maxDepthBeforeRecurse && depth < maxDepth;
         ++depth, ++myDepth) {

        // Check the preconditions if we're in testing mode
        if (verifyBuckets && myDepth < 16) {
            verifyPartitionBuckets(partitions, wAll);
        }

        // Run a kernel to find the new split point for each partition,
        // best feature and kernel.
        std::vector<PartitionSplit> splits
            = getPartitionSplits(buckets, numActiveBuckets, activeFeatures, bucketOffsets,
                                 features, wAll, indexes,
                                 depth < 4 /* parallel */); 


#if 0        
        if (verifyBuckets || true) {
            // At this point, the partitions are already sorted by their
            // row count.  Verify it's true.

            int lastCount = -1;
            for (auto & s: splits) {
                int count = s.left.count() + s.right.count();
                if (count == 0)
                    continue;
                if (s.feature == -1)
                    continue;
                if (lastCount == -1)
                    lastCount = count;
                ExcAssertLessEqual(count, lastCount);
                lastCount = count;
            }
        }
#endif
        
        // Identify new left and right buckets for each of the partitions.
        // A -1 number means that the partition is not kept.  A -2 or below
        // number means that the partition is processed recursively as a
        // leaf.
        std::vector<std::pair<int32_t, int32_t> >
            newPartitionNumbers(splits.size(), { -1, -1 });
        
        int numActivePartitions = 0;
        uint32_t numRowsInActivePartition = 0;

        // first = count, second = index * 2 + (right ? 1 : 0)
        std::vector<std::pair<uint32_t, uint32_t> > partitionCounts;

        // These are the partitions that are too small to be handled by the
        // main loop or that result in too many active partitions for our
        // configured maximum width.  They are handled in a separate algorithm
        // in parallel with running the main algorithm.
        // first = partition number, second = split
        std::vector<std::pair<int, PartitionSplit> > smallPartitions;

        int smallPartitionRows = 0;
        
        for (size_t i = 0;  i < splits.size();  ++i) {
            const PartitionSplit & p = splits[i];

            //cerr << "split " << i << " feat " << p.feature
            //     << " all " << wAll[i].count() << " left "
            //     << p.left.count() << " right " << p.right.count()
            //     << " dir " << p.direction << endl;

            if (p.left.count() > 0 && p.right.count() > 0) {
                int count = p.left.count() + p.right.count();
                if (count >= minExamplesPerBucket) {
                    ++numActivePartitions;
                    partitionCounts.emplace_back(p.left.count(), i * 2);
                    partitionCounts.emplace_back(p.right.count(), i * 2 + 1);
                    numRowsInActivePartition += count;
                }
                else {
                    smallPartitionRows += count;
                    smallPartitions.emplace_back(i, p);
                }
            }
        }

        // Any partitions that don't make the width get handled
        // separately
        if (partitionCounts.size() > maxWidth) {
            int splitCount = partitionCounts[maxWidth].first
                + partitionCounts[maxWidth + 1].first;
            if (debug) {
                cerr << "splitting off " << partitionCounts.size() - maxWidth
                    << " partitions with " << splitCount
                    << " max count due to maximum width" << endl;
            }

            for (int i = maxWidth;  i < partitionCounts.size();  ++i) {
                uint32_t idx = partitionCounts[i].second;
                uint32_t part = idx >> 1;
                int lr = idx & 1;
                smallPartitionRows += partitionCounts[i].first;
                if (lr) continue;
                smallPartitions.emplace_back(part, splits[part]);
            }

            partitionCounts.resize(maxWidth);
        }

        if (debug) {
            cerr << smallPartitions.size() << " small partitions with "
                << smallPartitionRows << " rows" << endl;
        }

        // Do the small partitions
        std::map<PartitionIndex, ML::Tree::Ptr> smallPartitionsOut
            = trainSmallPartitions(depth, maxDepth, tree, features,
                                   partitions,
                                   buckets, numActiveBuckets,
                                   bucketOffsets,
                                   decodedRows, activeFeatures,
                                   smallPartitions, fs);

#if 0        
        cerr << "small partitions returned " << smallPartitionsOut.size()
             << " trees" << endl;

        for (auto & out: smallPartitionsOut) {
            cerr << "idx " << out.first << " --> "
                 << (out.second.isNode() ? "node" : "leaf")
                 << " ex " << out.second.examples()
                 << " pr " << out.second.pred() << endl;
        }
#endif
        
        leaves.merge(std::move(smallPartitionsOut));
        
        // Sort our larger partitions
        std::sort(partitionCounts.begin(), partitionCounts.end(),
                  [] (std::pair<uint32_t, uint32_t> p1,
                      std::pair<uint32_t, uint32_t> p2)
                  {
                      return p1.first > p2.first
                          || (p1.first == p2.first && p1.second < p2.second);
                  });

        std::vector<PartitionIndex> newIndexes(partitionCounts.size());

        for (int i = 0;  i < partitionCounts.size();  ++i) {
            uint32_t idx = partitionCounts[i].second;
            uint32_t part = idx >> 1;
            int lr = idx & 1;

            // Count for the whole partition, since we need to either have both
            // or none of the partition values in order for this to work.
            uint32_t count
                = splits[part].left.count()
                + splits[part].right.count();
            
            if (count >= minExamplesPerBucket) {
                if (lr) { // right
                    newPartitionNumbers[part].second = i; 
                    newIndexes[i] = splits[part].index.rightChild();
                }
                else { // left
                    newPartitionNumbers[part].first = i;
                    newIndexes[i] = splits[part].index.leftChild();
                }
            }
            
            if (i < 10 && debug) {
                cerr << "partition " << i << " index " << part << " path "
                     << splits[part].index.path()
                     << " lr " << lr << " count " << partitionCounts[i].first
                     << endl;
            }
        }

        indexes = std::move(newIndexes);
        
        if (debug) {
            std::vector<size_t> partitionCountsCum;
            size_t cum = 0;
            for (auto & c: partitionCounts) {
                cum += c.first;
                partitionCountsCum.push_back(cum);
            }

            std::vector<Path> activeFeatureNames;
            for (auto & f: activeFeatures) {
                activeFeatureNames.emplace_back(features[f].info->columnName);
            }
            
            cerr << "depth " << depth << " active partitions "
                << numActivePartitions
                << " of " << splits.size()
                << " rows " << numRowsInActivePartition
                << " of " << decodedRows.size()
                << " buckets " << numActiveBuckets
                << " features " << jsonEncodeStr(activeFeatures)
                << " " << jsonEncodeStr(activeFeatureNames)
                << endl;

            cerr << "dist: ";
            for (int i = 1;  i < partitionCounts.size();  i *= 2) {
                cerr << i << ": " << partitionCounts[i - 1].first << "->"
                    << partitionCountsCum[i - 1]
                    << "("
                    << 100.0 * partitionCountsCum[i - 1] / numRowsInActivePartition
                    << "%)" << endl;
            }
            cerr << endl;
        }

        // New buckets since we're wider...
        buckets.resize(buckets.size() * 2);

        //cerr << "newPartitionNumbers = " << jsonEncodeStr(newPartitionNumbers) << endl;
        // Double the number of partitions, create new W entries for the
        // new partitions, and transfer those examples that are in the
        // wrong partition to the right one
        updateBuckets(features,
                      partitions, buckets, numActiveBuckets, wAll,
                      bucketOffsets, splits,
                      newPartitionNumbers, partitionCounts.size(),
                      decodedRows, activeFeatures);

        if (debug) {
            cerr << "depth " << depth << " time "
                << 1000.0 * Date::now().secondsSince(start)
                << "ms" << endl;
        }
        start = Date::now();
        
        for (auto & s: splits) {
            allSplits.emplace(s.index, s);
        }

        // Ready for the next level
    }

    // If we're not at the lowest level, partition our data and recurse
    // par partition to create our leaves.
    std::map<PartitionIndex, ML::Tree::Ptr> newLeaves
        = splitAndRecursePartitioned(depth, maxDepth, tree, serializer,
                                     buckets, numActiveBuckets, bucketOffsets,
                                     features, activeFeatures,
                                     decodedRows,
                                     partitions, wAll, indexes, fs,
                                     bucketMemory);

    // Merge everything together
    leaves.merge(std::move(newLeaves));
    
    // Finally, extract a tree from the splits we've been accumulating
    // and our leaves.
    return extractTree(originalDepth, maxDepth,
                       tree, root,
                       allSplits, leaves, features, fs);
}

// Recursively go through and extract our tree.  There is no
// calculating going on here, just creation of the data structure.
ML::Tree::Ptr
extractTree(int depth, int maxDepth,
            ML::Tree & tree,
            PartitionIndex root,
            const std::map<PartitionIndex, PartitionSplit> & splits,
            const std::map<PartitionIndex, ML::Tree::Ptr> & leaves,
            const std::span<const Feature> & features,
            const DatasetFeatureSpace & fs)
{
    ExcAssertEqual(root.depth(), depth);
    
    //cerr << "extractTree: depth " << depth << " root " << root << endl;
    if (depth == 0 && false) {
        cerr << " with " << splits.size() << " splits and " << leaves.size() << " leaves" << endl;
        for (auto & s: splits) {
            cerr << "  split " << s.first << " --> " << s.second.feature << " " << s.second.index
                 << " " << s.second.left.count() << ":" << s.second.right.count() << " d: " << (s.second.transferDirection() == RL) << endl;
        }
        for (auto & l: leaves) {
            cerr << "  leaf " << l.first << " --> " << l.second.pred() << " " << l.second.examples() << endl;
        }
    }

    // First look for a leaf
    {
        auto it = leaves.find(root);
        if (it != leaves.end()) {
            return it->second;
        }
        //cerr << "  not found in leaves" << endl;
    }

    // Secondly look for a split
    auto it = splits.find(root);
    if (it == splits.end()) {
        //cerr << "  split not found" << endl;
        return ML::Tree::Ptr();
    }
    auto & s = it->second;

    //cerr << std::string(relativeDepth * 2, ' ')
    //     << relativeDepth << " " << partition << " "
    //     << jsonEncodeStr(s.left) << " " << jsonEncodeStr(s.right)
    //     << " " << s.feature << endl;

    if (s.left.count() + s.right.count() == 0) 
        return ML::Tree::Ptr();
    else if (s.feature == -1)
        return getLeaf(tree, s.left + s.right);
                
    W total = s.left + s.right;

    // No impurity
    if (total.v[false] == 0 || total.v[true] == 0)
        return ML::Tree::Ptr();
                
    ML::Tree::Ptr left
        = extractTree(depth + 1, maxDepth,
                      tree, root.leftChild(),
                      splits, leaves, features, fs);
    ML::Tree::Ptr right
        = extractTree(depth + 1, maxDepth,
                      tree, root.rightChild(),
                      splits, leaves, features, fs);
    
    // Fill in from the information in the split if we can't recurse
    if (!left)
        left = getLeaf(tree, s.left);
    if (!right)
        right = getLeaf(tree, s.right);
    
    return getNode(tree, s.score, s.feature, s.value,
                   left, right, s.left, s.right, features, fs);
}

std::vector<float> decodeRows(const Rows & rows)
{
    std::vector<float> decodedRows(rows.rowCount());
        
    auto it = rows.getRowIterator();
        
    for (size_t i = 0;  i < rows.rowCount();  ++i) {
        DecodedRow row = it.getDecodedRow();
        ExcAssertEqual(i, row.exampleNum);
        decodedRows[i] = row.weight * (1-2*row.label);
    }

    return decodedRows;
}

void decodeRowsKernelCpu(ComputeContext & context,
                         MemoryArrayHandleT<const uint64_t> rowData,
                         uint32_t rowDataLength,
                         uint16_t weightBits,
                         uint16_t exampleNumBits,
                         uint32_t numRows,
                         WeightFormat weightFormat,
                         float weightMultiplier,
                         MemoryArrayHandleT<const float> weightData,
                         std::span<float> decodedRowsOut)
{
    Rows rows;
    rows.rowData = context.transferToHostSync(rowData);
    rows.numRowEntries = numRows;
    rows.exampleNumBits = exampleNumBits;
    rows.exampleNumMask = (1ULL << exampleNumBits) - 1;
    rows.weightMask = (1ULL << weightBits) - 1;
    rows.totalBits = weightBits + exampleNumBits + 1;
    rows.weightEncoder.weightBits = weightBits;
    rows.weightEncoder.weightFormat = weightFormat;
    rows.weightEncoder.weightMultiplier = weightMultiplier;
    rows.weightEncoder.weightFormatTable = context.transferToHostSync(weightData);

    auto it = rows.getRowIterator();
    ExcAssertEqual(decodedRowsOut.size(), numRows);

    cerr << "decodedRowsOut.size() = " << decodedRowsOut.size() << endl;
    cerr << "numRows = " << numRows << endl;

    for (size_t i = 0;  i < rows.rowCount();  ++i) {
        DecodedRow row = it.getDecodedRow();
        ExcAssertEqual(i, row.exampleNum);
        decodedRowsOut[i] = row.weight * (1-2*row.label);
    }
}

ML::Tree::Ptr
trainPartitionedEndToEndCpu(int depth, int maxDepth,
                            ML::Tree & tree,
                            MappedSerializer & serializer,
                            const Rows & rows,
                            const std::span<const Feature> & features,
                            FrozenMemoryRegionT<uint32_t> bucketMemory,
                            const DatasetFeatureSpace & fs)
{
    // First, grab the bucket totals for each bucket across the whole
    // lot.
    size_t numActiveBuckets = 0;
    std::vector<uint32_t> bucketOffsets = { 0 };
    std::vector<int> activeFeatures;
    for (auto & f: features) {
        if (f.active) {
            activeFeatures.push_back(bucketOffsets.size() - 1);
            numActiveBuckets += f.buckets.numBuckets;
        }
        bucketOffsets.push_back(numActiveBuckets);
    }

    std::vector<W> buckets(numActiveBuckets);

    std::vector<std::vector<uint16_t> > featureBuckets(features.size());

    // Keep a cache of our decoded weights, with the sign
    // being the label
    std::vector<float> decodedRows = decodeRows(rows);
        
    //std::atomic<uint64_t> featureBucketMem(0);
    //std::atomic<uint64_t> compressedFeatureBucketMem(0);

    // Start by initializing the weights for each feature, if
    // this isn't passed in already
    auto initFeature = [&] (int f)
        {
#if 0
            const auto & feature = features[f];
            auto & fb = featureBuckets[f];
            size_t ne = feature.buckets.numEntries;
            fb.resize(ne);
            featureBucketMem += sizeof(featureBuckets[0][0]) * ne;
            compressedFeatureBucketMem += feature.buckets.storage.memusage();
            for (size_t i = 0;  i < ne;  ++i) {
                fb[i] = feature.buckets[i];
            }
#endif

            // Distribute weights into buckets for the first iteration
            int startBucket = bucketOffsets[f];
            bool active;
            int maxBucket;
            std::tie(active, maxBucket)
            = testFeatureKernel(decodedRows.data(),
                                decodedRows.size(),
                                features[f].buckets,
                                buckets.data() + startBucket);
                
#if 0 // debug
            W wAll1;
            auto it = rows.getRowIterator();
            for (size_t i = 0;  i < rows.rowCount();  ++i) {
                auto row = it.getDecodedRow();
                wAll1[row.label] += row.weight;
            }

            W wAll2;
            for (size_t i = startBucket;  i < bucketOffsets[f + 1];  ++i) {
                wAll2 += buckets[i];
            }

            ExcAssertEqual(jsonEncodeStr(rows.wAll), jsonEncodeStr(wAll1));
            ExcAssertEqual(jsonEncodeStr(rows.wAll), jsonEncodeStr(wAll2));
#endif // debug
        };

    parallelForEach(activeFeatures, initFeature);

    using namespace std;
    //cerr << "feature bucket mem = " << featureBucketMem.load() / 1000000.0
    //     << "mb; compressed = "
    //     << compressedFeatureBucketMem.load() / 1000000.0 << "mb" << endl;
        
    return trainPartitionedRecursive(depth, maxDepth, tree, serializer,
                                     bucketOffsets, activeFeatures,
                                     std::move(buckets),
                                     decodedRows,
                                     rows.wAll,
                                     PartitionIndex::root(),
                                     fs, features, bucketMemory);
}

ML::Tree::Ptr
trainPartitionedRecursive(int depth, int maxDepth,
                          ML::Tree & tree,
                          MappedSerializer & serializer,
                          const std::span<const uint32_t> & bucketOffsets,
                          const std::span<const int> & activeFeatures,
                          const std::span<const W> & bucketsIn,
                          const std::span<const float> & decodedRows,
                          const W & wAllInput,
                          PartitionIndex root,
                          const DatasetFeatureSpace & fs,
                          const std::span<const Feature> & features,
                          FrozenMemoryRegionT<uint32_t> bucketMemory)
{
    return trainPartitionedRecursiveCpu
        (depth, maxDepth, tree, serializer, bucketOffsets, activeFeatures,
         std::move(bucketsIn), decodedRows, wAllInput, root, fs, features,
         std::move(bucketMemory));
                                           
}

struct RegisterKernels {

    RegisterKernels()
    {
        auto createDecodeRowsKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "decodeRows";
            result->device = ComputeDevice::host();
            result->addParameter("rowData", "r", "u64[rowDataLength]");
            result->addParameter("rowDataLength", "r", "u32");
            result->addParameter("weightBits", "r", "u16");
            result->addParameter("exampleNumBits", "r", "u16");
            result->addParameter("numRows", "r", "u32");
            result->addParameter("weightFormat", "r", "MLDB::RF::WeightFormat");
            result->addParameter("weightMultiplier", "r", "f32");
            result->addParameter("weightData", "r", "f32[weightDataLength]");
            result->addParameter("decodedRowsOut", "w", "f32[numRows]");
            result->setComputeFunction(decodeRowsKernelCpu);
            return result;
        };

        registerHostComputeKernel("decodeRows", createDecodeRowsKernel);

        auto createTestFeatureKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "testFeature";
            result->device = ComputeDevice::host();
            result->addDimension("featureNum", "nf");
            result->addParameter("decodedRows", "r", "f32[numRows]");
            result->addParameter("numRows", "r", "u32");
            result->addParameter("bucketData", "r", "u32[bucketDataLength]");
            result->addParameter("bucketDataOffsets", "r", "u32[nf + 1]");
            result->addParameter("bucketNumbers", "r", "u32[nf]");
            result->addParameter("bucketEntryBits", "r", "u32[nf]");
            result->addParameter("featuresActive", "r", "u32[nf]");
            result->addParameter("partitionBuckets", "r", "MLDB::RF::WT<MLDB::FixedPointAccum32>[numBuckets]");
            result->set1DComputeFunction(testFeatureKernel);
            return result;
        };

        registerHostComputeKernel("testFeature", createTestFeatureKernel);

        auto createGetPartitionSplitsKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "getPartitionSplits";
            result->device = ComputeDevice::host();
            result->addDimension("f", "nf");
            result->addDimension("p", "np");
            result->addParameter("totalBuckets", "r", "u32");
            result->addParameter("bucketNumbers", "r", "u32[nf]");
            result->addParameter("featuresActive", "r", "u32[nf]");
            result->addParameter("featureIsOrdinal", "r", "u32[nf]");
            result->addParameter("buckets", "r", "MLDB::RF::WT<MLDB::FixedPointAccum32>[totalBuckets * np]");
            result->addParameter("wAll", "r", "MLDB::RF::WT<MLDB::FixedPointAccum32>[np]");
            result->addParameter("featurePartitionSplitsOut", "w", "MLDB::RF::PartitionSplit[np * nf]");
            result->set2DComputeFunction(getPartitionSplitsKernel);
            return result;
        };

        registerHostComputeKernel("getPartitionSplits", createGetPartitionSplitsKernel);

        auto createBestPartitionSplitKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "bestPartitionSplit";
            result->device = ComputeDevice::host();
            result->addDimension("p", "np");
            result->addParameter("numFeatures", "r", "u32");
            result->addParameter("featuresActive", "r", "u32[numFeatures]");
            result->addParameter("featurePartitionSplits", "r", "MLDB::RF::PartitionSplit[np * nf]");
            result->addParameter("allPartitionSplitsOut", "w", "MLDB::RF::PartitionSplit[maxPartitions]");
            result->addParameter("partitionSplitsOffset", "r", "u32");
            result->set1DComputeFunction(bestPartitionSplitKernel);
            return result;
        };

        registerHostComputeKernel("bestPartitionSplit", createBestPartitionSplitKernel);

        auto createClearBucketsKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "clearBuckets";
            result->device = ComputeDevice::host();
            result->addDimension("p", "partitionSplitsOffset");
            result->addDimension("b", "numActiveBuckets");
            result->addParameter("bucketsOut", "w", "MLDB::RF::WT<MLDB::FixedPointAccum32>[numActiveBuckets * np * 2]");
            result->addParameter("wAllOut", "w", "MLDB::RF::WT<MLDB::FixedPointAccum32>[np * 2]");
            result->addParameter("allPartitionSplits", "r", "MLDB::RF::PartitionSplit[np]");
            result->addParameter("numActiveBuckets", "r", "u32");
            result->addParameter("partitionSplitsOffset", "r", "u32");
            result->set2DComputeFunction(clearBucketsKernel);
            return result;
        };

        registerHostComputeKernel("clearBuckets", createClearBucketsKernel);

        auto createUpdatePartitionNumbersKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "updatePartitionNumbers";
            result->device = ComputeDevice::host();
            result->addDimension("r", "numRows");
            result->addParameter("partitionSplitsOffset", "r", "u32");
            result->addParameter("partitions", "r", "MLDB::RF::RowPartitionInfo[numRows]");
            result->addParameter("directions", "w", "u8[numRows]");
            result->addParameter("allPartitionSplits", "r", "MLDB::RF::PartitionSplit[np]");
            result->addParameter("bucketData", "r", "u32[bucketDataLength]");
            result->addParameter("bucketDataOffsets", "r", "u32[nf + 1]");
            result->addParameter("bucketNumbers", "r", "u32[nf]");
            result->addParameter("bucketEntryBits", "r", "u32[nf]");
            result->addParameter("featureIsOrdinal", "r", "u32[nf]");
            result->set1DComputeFunction(updatePartitionNumbersKernel);
            return result;
        };

        registerHostComputeKernel("updatePartitionNumbers", createUpdatePartitionNumbersKernel);

        auto createUpdateBucketsKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "updateBuckets";
            result->device = ComputeDevice::host();
            result->addDimension("f", "nf");
            result->addParameter("partitionSplitsOffset", "r", "u32");
            result->addParameter("numActiveBuckets", "r", "u32");
            result->addParameter("partitions", "r", "MLDB::RF::RowPartitionInfo[numRows]");
            result->addParameter("directions", "r", "u8[numRows]");
            result->addParameter("buckets", "w", "MLDB::RF::WT<MLDB::FixedPointAccum32>[numActiveBuckets * np * 2]");
            result->addParameter("wAll", "w", "MLDB::RF::WT<MLDB::FixedPointAccum32>[np * 2]");
            result->addParameter("allPartitionSplits", "r", "MLDB::RF::PartitionSplit[np]");
            result->addParameter("decodedRows", "r", "f32[nr]");
            result->addParameter("numRows", "r", "u32");
            result->addParameter("bucketData", "r", "u32[bucketDataLength]");
            result->addParameter("bucketDataOffsets", "r", "u32[nf + 1]");
            result->addParameter("bucketNumbers", "r", "u32[nf]");
            result->addParameter("bucketEntryBits", "r", "u32[nf]");
            result->addParameter("featuresActive", "r", "u32[numFeatures]");
            result->addParameter("featureIsOrdinal", "r", "u32[nf]");
            result->set1DComputeFunction(updateBucketsKernel);
            return result;
        };

        registerHostComputeKernel("updateBuckets", createUpdateBucketsKernel);

        auto createFixupBucketsKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "fixupBuckets";
            result->device = ComputeDevice::host();
            result->addDimension("partition", "np");
            result->addDimension("bucket", "numActiveBuckets");
            result->addParameter("buckets", "w", "MLDB::RF::WT<MLDB::FixedPointAccum32>[numActiveBuckets * np * 2]");
            result->addParameter("wAll", "w", "MLDB::RF::WT<MLDB::FixedPointAccum32>[np * 2]");
            result->addParameter("allPartitionSplits", "r", "MLDB::RF::PartitionSplit[np]");
            result->set2DComputeFunction(fixupBucketsKernel);
            return result;
        };

        registerHostComputeKernel("fixupBuckets", createFixupBucketsKernel);

    }

} registerKernels;

} // namespace RF
} // namespace MLDB
