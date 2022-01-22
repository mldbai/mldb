/** randomforest_kernels.cc                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#include "randomforest_kernels.h"
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
#include <bit>

using namespace std;


namespace MLDB {
namespace RF {

void decodeRowsKernelCpu(ComputeContext & context,
                         ComputeKernelGridRange & rowRange,
                         FrozenMemoryRegionT<uint64_t> rowData,
                         uint32_t rowDataLength,
                         uint16_t weightBits,
                         uint16_t exampleNumBits,
                         uint32_t numRows,
                         WeightFormat weightFormat,
                         float weightMultiplier,
                         FrozenMemoryRegionT<float> weightData,
                         std::span<float> decodedRowsOut)
{
    Rows rows;
    rows.rowData = rowData;
    rows.numRowEntries = numRows;
    rows.exampleNumBits = exampleNumBits;
    rows.exampleNumMask = (1ULL << exampleNumBits) - 1;
    rows.weightMask = (1ULL << weightBits) - 1;
    rows.totalBits = weightBits + exampleNumBits + 1;
    rows.weightEncoder.weightBits = weightBits;
    rows.weightEncoder.weightFormat = weightFormat;
    rows.weightEncoder.weightMultiplier = weightMultiplier;
    rows.weightEncoder.weightFormatTable = weightData;

    auto it = rows.getRowIterator();
    ExcAssertEqual(decodedRowsOut.size(), numRows);

    for (uint32_t i: rowRange) {
        it.skipTo(i);
        DecodedRow row = it.getDecodedRow();
        ExcAssertEqual(i, row.exampleNum);
        decodedRowsOut[i] = row.weight * (1-2*row.label);
    }
}

void
testFeatureKernel(ComputeContext & context,
                  uint32_t fidx, uint32_t naf,
                  ComputeKernelGridRange & rows,

                  std::span<const float> decodedRows,
                  uint32_t numRows,

                  std::span<const uint32_t> allBucketData,
                  std::span<const uint32_t> bucketDataOffsets,
                  std::span<const uint32_t> bucketNumbers,
                  std::span<const uint32_t> bucketEntryBits,

                  std::span<const uint32_t> activeFeatureList,

                  std::span<W> allWOut)
{
    uint32_t f = activeFeatureList[fidx];

    ExcAssertLessEqual(bucketNumbers[f + 1], allWOut.size());

    ExcAssertEqual(rows.range(), numRows);  // We fake having this argument...

    BucketList buckets;
    buckets.entryBits = bucketEntryBits[f];
    buckets.numBuckets = -1;  // unused but set to an invalid value to be sure
    buckets.numEntries = bucketNumbers[f + 1] - bucketNumbers[f];
    buckets.storagePtr = allBucketData.data() + bucketDataOffsets[f];

    testFeatureKernel(decodedRows.data(), numRows, buckets, allWOut.data() + bucketNumbers[f]);
}

// For each feature and partition, find the split that gives the best score and
// record it with one row per partition and one column per feature in the output
// matrix.  This involves testing each split point for the given feature and
// partition.

void
getPartitionSplitsKernel(ComputeContext & context,

                         uint32_t fidx, uint32_t naf,
                         
                         std::span<const TreeTrainingInfo> treeTrainingInfo, // [1]
                         std::span<const uint32_t> bucketNumbers, // [nf]
                         
                         std::span<const uint32_t> activeFeatureList, // [naf]
                         std::span<const uint32_t> featureIsOrdinal, // [nf]
                         
                         std::span<const W> buckets, // [np x totalBuckets]

                         std::span<const W> wAll,  // [np] one per partition
                         std::span<PartitionSplit> splitsOut, // [np x nf]
                         std::span<const TreeDepthInfo> treeDepthInfo) //[1]
{
    auto numActivePartitions = treeDepthInfo[0].numActivePartitions;
    auto totalBuckets = treeTrainingInfo[0].numActiveBuckets;

    for (uint32_t p = 0;  p < numActivePartitions;  ++p) {
        uint32_t f = activeFeatureList[fidx];

        PartitionSplit & result = splitsOut[p * naf + fidx];
        if (wAll[p].empty() || wAll[p].uniform()) {
            result = PartitionSplit();
            continue;
        }

        int startBucket = bucketNumbers[f];
        int endBucket = bucketNumbers[f + 1];
        int maxBucket = endBucket - startBucket - 1;
        const W * wFeature = buckets.data() + (p * totalBuckets) + startBucket;
        std::tie(result.score, result.value, result.left, result.right)
            = chooseSplitKernel(wFeature, maxBucket, featureIsOrdinal[f], wAll[p], false /* debug */);
        result.feature = result.score == INFINITY ? -1 : f;
    }

    if (false) {
        using namespace std;
        std::ofstream splitsStream("tree-splits-" + std::to_string(treeTrainingInfo[0].featureSampling)
                                + "-" + std::to_string(treeTrainingInfo[0].featureVectorSampling)
                                + "-depth" + std::to_string(treeDepthInfo[0].depth)
                                + ".txt", std::ios_base::app);
        splitsStream << "feat " << fidx << endl;
        for (size_t i = 0;  i < treeDepthInfo[0].numActivePartitions;  ++i) {
            splitsStream << "  " << i << jsonEncodeStr(splitsOut[i * naf + fidx]) << endl;
        }
        splitsStream << "wall " << endl;
        for (size_t i = 0;  i < treeDepthInfo[0].numActivePartitions;  ++i) {
            splitsStream << "  " << i << jsonEncodeStr(wAll[i]) << endl;
        }
    }

}

// Reduction over the feature columns from the getPartitionSplitsKernel, where
// we find which feature gives the best split for each partition and record that
// in the output.
void
bestPartitionSplitKernel(ComputeContext & context,
                         std::span<const TreeTrainingInfo> treeTrainingInfo, // [1]
                         std::span<const TreeDepthInfo> treeDepthInfo, //[1]
                         std::span<const uint32_t> featureActive, // [naf]
                         std::span<const PartitionSplit> featurePartitionSplits, // [np x nf]
                         std::span<const PartitionIndex> partitionIndexes, // [np]
                         std::span<IndexedPartitionSplit> partitionSplitsOut)  // np
{
    auto partitionSplitsOffset = treeDepthInfo[0].numFinishedPartitions;
    auto naf = treeTrainingInfo[0].numActiveFeatures;
    auto nap = treeDepthInfo[0].numActivePartitions;
    uint16_t depth = treeDepthInfo[0].depth;

    for (uint32_t p = 0;  p < nap;  ++p) {
        //cerr << "partition " << p << " of " << nap << endl;
        if (partitionSplitsOffset + p >= partitionSplitsOut.size()) {
            cerr << "overflow" << endl;
            cerr << "p = " << p << endl;
            cerr << "nap = " << nap << endl;
            cerr << "partitionSplitsOffset = " << partitionSplitsOffset << endl;
            cerr << "partitionSplitsOut.size() = " << partitionSplitsOut.size() << endl;
            cerr << "treeDepthInfo = " << jsonEncodeStr(treeDepthInfo[0]) << endl;
        }
        ExcAssertLess(partitionSplitsOffset + p, partitionSplitsOut.size());
        IndexedPartitionSplit & result = partitionSplitsOut[partitionSplitsOffset + p];
        result = IndexedPartitionSplit();
        result.index = (depth == 0 ? PartitionIndex::root() : partitionIndexes[p]);
        if (result.index == PartitionIndex::none())
            continue;

        for (size_t fidx = 0;  fidx < naf;  ++fidx) {
            const PartitionSplit & fp = featurePartitionSplits[p * naf + fidx];

            if (false) {
                using namespace std;
                std::ofstream splitsStream("tree-best-splits-" + std::to_string(treeTrainingInfo[0].featureSampling)
                                        + "-" + std::to_string(treeTrainingInfo[0].featureVectorSampling)
                                        + "-depth" + std::to_string(treeDepthInfo[0].depth)
                                        + ".txt", std::ios_base::app);
                splitsStream << "feat " << fidx << " part " << p << " " << jsonEncodeStr(fp) << endl;
            }

            if (fp.score == INFINITY)
                continue;
            //cerr << "kernel: partition " << p << " feature " << f << " score " << fp.score << endl;
            if (fp.score < result.score) {
                result.PartitionSplit::operator = (fp);
            }
        }

        if (false) {
            using namespace std;
            std::ofstream splitsStream("tree-best-splits-" + std::to_string(treeTrainingInfo[0].featureSampling)
                                    + "-" + std::to_string(treeTrainingInfo[0].featureVectorSampling)
                                    + "-depth" + std::to_string(treeDepthInfo[0].depth)
                                    + ".txt", std::ios_base::app);
            splitsStream << "part " << p << " best " << jsonEncodeStr(result) << endl;
        }

    }
}

void
assignPartitionNumbersKernel(ComputeContext & context,

                             std::span<const TreeTrainingInfo> treeTrainingInfo, // [1]
                             std::span<TreeDepthInfo> treeDepthInfo, //[1]

                             std::span<const IndexedPartitionSplit> partitionSplits,
                             std::span<PartitionIndex> partitionIndexesOut,
                             std::span<PartitionInfo> partitionInfoOut,
                             std::span<uint8_t> smallSideIndexesOut,
                             std::span<uint16_t> smallSideIndexToPartitionOut)
{
    uint16_t depth = treeDepthInfo[0].depth;

    uint32_t partitionSplitsOffset = treeDepthInfo[0].numFinishedPartitions;
    uint32_t numActivePartitions = treeDepthInfo[0].numActivePartitions;
    uint32_t maxNumActivePartitions = treeTrainingInfo[0].maxNumActivePartitions;

    ExcAssertGreaterEqual(partitionIndexesOut.size(), numActivePartitions);
    ExcAssertGreaterEqual(partitionInfoOut.size(), numActivePartitions);

    partitionSplits = partitionSplits.subspan(partitionSplitsOffset);

    std::fill(smallSideIndexToPartitionOut.begin(), smallSideIndexToPartitionOut.end(), 0);

    //cerr << "numPartitionsIn = " << numActivePartitions << endl;

    // First, accumulate a list of inactive partitions
    std::vector<uint32_t> inactivePartitions;
    uint32_t numSmallSideRows = 0;

    for (uint32_t p = 0;  p < numActivePartitions;  ++p) {
        const PartitionSplit & split = partitionSplits[p];
        if (!split.valid() || (split.left.uniform() && split.right.uniform())) {
            inactivePartitions.push_back(p);
            smallSideIndexesOut[p] = 255;
            continue;
        }

        numSmallSideRows += std::min(split.left.count(), split.right.count());

        smallSideIndexesOut[p] = false;
        //cerr << "partition " << p << " split: " << jsonEncodeStr(split) << endl;
    }

    //cerr << inactivePartitions << " of " << numActivePartitions << " are inactive" << endl;

    uint32_t numActive = 0;
    uint32_t countActive = 0;

    uint32_t n = 0, n2 = numActivePartitions;
    uint16_t ssi = 0;

    //uint32_t outIndex = 0;

    for (uint32_t p = 0;  p < numActivePartitions;  ++p) {
        const IndexedPartitionSplit & split = partitionSplits[p];
        PartitionInfo & info = partitionInfoOut[p];
        if (!split.valid() || (split.left.uniform() && split.right.uniform())) {
            info = PartitionInfo();
        }
        else {
            auto direction = split.transferDirection() == PartitionSplitDirection::RL;

            // both still valid.  One needs a new partition number
            uint32_t minorPartitionNumber;

            if (n < inactivePartitions.size()) {
                // Re-use an unused partition
                minorPartitionNumber = inactivePartitions[n++];
            }
            else if (n2 < maxNumActivePartitions) {
                // Extend the list of partitions
                minorPartitionNumber = n2++;
            }
            else {
                // Max width reached; ignore this partition
                //skippedRows += split.left.count + split.right.count;
                //skippedPartitions += 1;
                info.left = -1;
                info.right = -1;
                continue;
            }

            // Attempt to allocate a small side number, and if it's possible record the
            // mapping.
            if (ssi < 254) {
                uint8_t idx = ++ssi;
                //cerr << "minorPartitionNumber = " << minorPartitionNumber << " idx = " << (int)idx << endl;
                smallSideIndexesOut[minorPartitionNumber] = idx;
                smallSideIndexToPartitionOut[idx] = minorPartitionNumber;
            }
            else {
                smallSideIndexesOut[minorPartitionNumber] = 255;
            }

            if (direction == 0) {
                info.left = p;
                info.right = minorPartitionNumber;
            }
            else {
                info.left = minorPartitionNumber;
                info.right = p;
            }

            numActive += 2;
            countActive += split.left.count() + split.right.count();

            partitionIndexesOut[info.left] = split.index.leftChild();
            partitionIndexesOut[info.right] = split.index.rightChild();
        }
    }

    treeDepthInfo[0].depth = depth + 1;
    treeDepthInfo[0].prevNumActivePartitions = treeDepthInfo[0].numActivePartitions;
    treeDepthInfo[0].numActivePartitions = n2;
    treeDepthInfo[0].numSmallSideRows = numSmallSideRows;
    treeDepthInfo[0].prevNumFinishedPartitions = treeDepthInfo[0].numFinishedPartitions;
    treeDepthInfo[0].numFinishedPartitions = treeDepthInfo[0].prevNumFinishedPartitions + numActivePartitions;
    treeDepthInfo[0].status = 0;  // for now

    std::vector<uint32_t> newCounts(n2, 0);
    for (uint32_t i = 0;  i < numActivePartitions;  ++i) {
        //cerr << "old part " << i << " with count "
        //     << partitionSplits[i].left.count() + partitionSplits[i].right.count()
        //     << ": left goes to " << partitionInfoOut[i].left
        //     << " right goes to " << partitionInfoOut[i].right << endl;
        if (partitionInfoOut[i].left != -1) {
            newCounts[partitionInfoOut[i].left] = partitionSplits[i].left.count();
        }
        if (partitionInfoOut[i].right != -1) {
            newCounts[partitionInfoOut[i].right] = partitionSplits[i].right.count();
        }
    }

#if 0
    for (uint32_t i = 0;  i < numActivePartitions;  ++i) {
        const PartitionInfo & info = partitionInfoOut[i];
        cerr << "old partition " << i << " left " << info.left << " right " << info.right << endl;
    }

    for (uint32_t i = 0;  i < n2;  ++i) {
        cerr << "new part " << i << " index " << partitionIndexesOut[i] << " ssi " 
             << (int)smallSideIndexesOut[i] << " count " << newCounts[i] << endl;
    }

    for (uint32_t i = 1;  i <= ssi;  ++i) {
        cerr << "small side index " << i << " maps to partition " << smallSideIndexToPartitionOut[i] << endl;
    }
#endif

    //cerr << numActive << " active partitions (including " << (inactivePartitions.size() - n)
    //     << " gaps with " << countActive << " rows)" << endl;

    while (n < inactivePartitions.size()) {
        auto p = inactivePartitions[n++];
        partitionIndexesOut[p] = PartitionIndex::none();
        smallSideIndexesOut[p] = 0;
    }

    cerr << "on exit: treeDepthInfo = " << jsonEncodeStr(treeDepthInfo[0]) << endl;

    if (false) {
        using namespace std;
        std::ofstream splitsStream("tree-partition-numbers-" + std::to_string(treeTrainingInfo[0].featureSampling)
                                + "-" + std::to_string(treeTrainingInfo[0].featureVectorSampling)
                                + "-depth" + std::to_string(treeDepthInfo[0].depth)
                                + ".txt", std::ios_base::app);
        splitsStream << jsonEncodeStr(treeDepthInfo[0]) << endl;
        splitsStream << "partitionIndexes" << endl;
        for (size_t i = 0;  i < treeDepthInfo[0].numActivePartitions;  ++i) {
            splitsStream << i << " " << partitionIndexesOut[i].index << endl;
        }
        splitsStream << "partitionInfo" << endl;
        for (size_t i = 0;  i < treeDepthInfo[0].prevNumActivePartitions;  ++i) {
            splitsStream << i << " " << partitionInfoOut[i].left << "," << partitionInfoOut[i].right << endl;
        }
        splitsStream << "smallSideIndexes" << endl;
        for (size_t i = 0;  i < treeDepthInfo[0].numActivePartitions;  ++i) {
            splitsStream << i << " " << (int)smallSideIndexesOut[i] << endl;
        }
        splitsStream << "smallSideIndexToPartition" << endl;
        for (size_t i = 0;  i < min<size_t>(256, treeDepthInfo[0].numActivePartitions/2);  ++i) {
            splitsStream << i << " " << smallSideIndexToPartitionOut[i] << endl;
        }
    }
}

// After doubling the number of buckets, this clears the wAll and allPartitionBuckets
// entries corresponding to active but non-initialized post-split buckets.
void
clearBucketsKernel(ComputeContext & context,

                   ComputeKernelGridRange & bucketRange,

                   std::span<const TreeTrainingInfo> treeTrainingInfo, // [1]
                   std::span<TreeDepthInfo> treeDepthInfo, //[1]

                   std::span<W> allPartitionBuckets,
                   std::span<W> wAll,
                   std::span<uint32_t> numNonZeroDirectionIndices,
                   std::span<const uint8_t> smallSideIndexes)
{
    auto numActivePartitions = treeDepthInfo[0].numActivePartitions;
    auto numActiveBuckets = treeTrainingInfo[0].numActiveBuckets;

    ExcAssertEqual(numNonZeroDirectionIndices.size(), 1);
    numNonZeroDirectionIndices[0] = 0;

    for (uint32_t partition = 0;  partition < numActivePartitions;  ++partition) {

        if (!smallSideIndexes[partition]) {
            //cerr << "not clearing partition " << partition << ": Wall " << jsonEncodeStr(wAll[partition]) << endl;
            continue;
        }

        wAll[partition] = W();

        ExcAssertEqual(numActiveBuckets, bucketRange.range());

        auto partitionBucketRange = bucketRange;
        for (uint32_t bucket: partitionBucketRange) {
            allPartitionBuckets[partition * numActiveBuckets + bucket] = W();
        }
    }
}

void
updatePartitionNumbersKernel(ComputeContext & context,
                             ComputeKernelGridRange & rowRange,
          
                             std::span<const TreeTrainingInfo> treeTrainingInfo, // [1]
                             std::span<TreeDepthInfo> treeDepthInfo, //[1]
                             
                             std::span<RowPartitionInfo> partitions,
                             std::span<uint32_t> /* directions */,
                             std::span<uint32_t> numNonZeroDirectionIndices,
                             std::span<UpdateWorkEntry> nonZeroDirectionIndices,
                             std::span<uint8_t> smallSideIndexes,
          
                             std::span<const IndexedPartitionSplit> partitionSplits,
                             std::span<const PartitionInfo> partitionInfo,
                             
                             // Feature data
                             std::span<const uint32_t> allBucketData,
                             std::span<const uint32_t> bucketDataOffsets,
                             std::span<const uint32_t> bucketNumbers,
                             std::span<const uint32_t> bucketEntryBits,
          
                             std::span<const uint32_t> featureIsOrdinal,
                             std::span<const float> decodedRows)
{
    auto partitionSplitsOffset = treeDepthInfo[0].prevNumFinishedPartitions;
    partitionSplits = partitionSplits.subspan(partitionSplitsOffset);
    uint16_t depth = treeDepthInfo[0].depth - 1;

    // Skip to where we should be in our partition splits
    //partitionSplits = partitionSplits.subspan(partitionSplitsOffset);
    uint32_t nf = bucketEntryBits.size();

    BucketList featureBuckets[nf];

    //uint32_t currentBits = 0;
    //uint32_t bitIdx = 0;
    //uint32_t wordIdx = 0;
    uint32_t numNonZero = 0;

    auto writeDirection = [&] ()
    {
        //if (wordIdx < directions.size())
        //    directions[wordIdx++] = currentBits;
    };

    auto setDirection = [&] (uint32_t r, bool val)
    {
        //ExcAssertEqual(bitIdx, r % 32);
        if (val) {
            //currentBits |= (1 << bitIdx);
        }
        //++bitIdx;

        //if (bitIdx == 32) {
        //    writeDirection();
        //    currentBits = 0;
        //    bitIdx = 0;
        //}
    };

    for (uint32_t r: rowRange) {
        bool debug = false;//(r == 845 || r == 3006 || r == 3758);

        auto partition = depth == 0 ? 0 : partitions[r].partition();

        // Row is not in any partition
        if (partition == (uint16_t)-1) {
            if (depth == 0)
                partitions[r] = -1;
            setDirection(r, 0);
            continue;
        }

        const PartitionInfo & info = partitionInfo[partition];

        if (info.ignore()) {
            setDirection(r, 0);
            partitions[r] = -1;
            continue;
        }

        if (debug) {
            cerr << "r = " << r << " partition = " << partition << " split = "
                 << jsonEncodeStr(partitionSplits[partition])
                 << " info = " << jsonEncodeStr(partitionInfo[partition]) << endl;
        }

        int splitFeature = partitionSplits[partition].feature;

        if (splitFeature == -1) {
            // reached a leaf here, nothing to split
            setDirection(r, 0);
            partitions[r] = -1;
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
        uint32_t side = ordinal ? bucket >= splitValue : bucket != splitValue;  // 0 = left, 1 = right

        if (debug && side == 1) {
            cerr << "row " << r << " splitValue = " << splitValue << " bucket = " << bucket << " ordinal = " << ordinal
                 << " side = " << side << endl;
            //cerr << "left.count = " << partitionSplits[partition].left.count() << " right.count = "
            //     << partitionSplits[partition].right.count() << endl;
        }

        // Set the new partition number
        uint16_t newPartitionNumber = side ? info.right : info.left;
        if (depth == 0)
            partitions[r] = newPartitionNumber;
        if (newPartitionNumber != partition) {
            //cerr << "updatePartitionNumber: row " << r << " former partition "
            //     << partition << " has new partition number "
            //     << newPartitionNumber << " splitFeature " << splitFeature << " splitValue " << splitValue
            //     << " bucket " << bucket << endl;
            if (depth != 0)
                partitions[r] = newPartitionNumber;
            setDirection(r, newPartitionNumber != (uint16_t)-1);
            size_t idx = numNonZero++;
            uint16_t smallSideIndex = smallSideIndexes[newPartitionNumber];

            nonZeroDirectionIndices[idx] = { r, newPartitionNumber, smallSideIndex, decodedRows[r] };
        }
        else {
            setDirection(r, 0);
            continue;
        }

        //cerr << "row " << r << " side " << side << " currently in " << partitionSplits[partition].index
        //     << " goes from partition " << partition << " (" << PartitionIndex(partitionSplitsOffset + partition)
        //     << ") to partition " << partition + side * partitionSplitsOffset << " ("
        //     << PartitionIndex(partitionSplitsOffset + partition + side * partitionSplitsOffset) << ")" << endl;
    }

    writeDirection();
    numNonZeroDirectionIndices[0] =  numNonZero;

    if (false) {
        using namespace std;
        std::ofstream splitsStream("tree-partitions-" + std::to_string(treeTrainingInfo[0].featureSampling)
                                + "-" + std::to_string(treeTrainingInfo[0].featureVectorSampling)
                                + "-depth" + std::to_string(treeDepthInfo[0].depth)
                                + ".txt", std::ios_base::app);
        for (size_t i = 0;  i < treeTrainingInfo[0].numRows;  ++i) {
            splitsStream << "  " << i << " " << partitions[i].partition() << endl;
        }
    }

}

void
updateBucketsKernel(ComputeContext & context,
                    ComputeKernelGridRange & rowRange,
                    uint32_t fidxp1, uint32_t nafp1,

                    std::span<const TreeTrainingInfo> treeTrainingInfo, // [1]
                    std::span<TreeDepthInfo> treeDepthInfo, //[1]

                    std::span<const RowPartitionInfo> partitions,
                    std::span<const uint32_t> /* directions */,
                    std::span<const uint32_t> numNonZeroDirectionIndices, //[1]
                    std::span<UpdateWorkEntry> nonZeroDirectionIndices,

                    std::span<W> partitionBuckets,
                    std::span<W> wAll,
                    std::span<const uint8_t> smallSideIndexes,
                    std::span<const uint16_t> smallSideIndexToPartition,

                    // Row data
                    std::span<const float> decodedRows,
                    
                    // Feature data
                    std::span<const uint32_t> allBucketData,
                    std::span<const uint32_t> bucketDataOffsets,
                    std::span<const uint32_t> bucketNumbers,
                    std::span<const uint32_t> bucketEntryBits,

                    std::span<const uint32_t> featureActive,
                    std::span<const uint32_t> featureIsOrdinal)
{
    int fidx = fidxp1 - 1;  // -1 means wAll, otherwise it's the feature number
    int f = (fidx == -1 ? -1 : featureActive[fidx]);

    auto numActiveBuckets = treeTrainingInfo[0].numActiveBuckets;

    // We have to set up to access to buckets for the feature we're updating for the split (f)
    BucketList buckets;
    if (f != -1) {
        buckets.entryBits = bucketEntryBits[f];
        buckets.numBuckets = -1;  // unused but set to an invalid value to be sure
        buckets.numEntries = bucketNumbers[f + 1] - bucketNumbers[f];
        buckets.storagePtr = allBucketData.data() + bucketDataOffsets[f];
    }
    
    uint32_t startBucket;

    // Pointer to the global array we eventually want to update
    std::span<W> wGlobal;

    if (f == -1) {
        wGlobal = wAll;
        startBucket = 0;
    }
    else {
        wGlobal = partitionBuckets;
        startBucket = bucketNumbers[f];
    }

    //size_t numSkipped = 0;

    uint32_t numWorkItems = numNonZeroDirectionIndices[0];

    //cerr << "feature " << f << endl;

    // TODO: rowRange needs to be exclusive (we will need to make atomic if we process
    // the same feature from multiple threads)
    for (uint32_t i = 0;  i < numWorkItems;  ++i) {

        auto workItem = nonZeroDirectionIndices[i];
        uint32_t r = workItem.row;
        uint16_t partition = workItem.partition;
        float decodedRow = workItem.decodedRow;

        //if (f == -1)
        //    cerr << "row " << i << " has direction " << (int)direction
        //         << " partition " << partition << endl;

        // We only need to update features on the wrong side, as we
        // transfer the weight rather than sum it from the
        // beginning.  This means less work for unbalanced splits
        // (which are typically most of them, especially for discrete
        // buckets)

        uint32_t toBucket;
        
        if (f == -1) {
            toBucket = partition;
        }
        else {
            uint32_t bucket = buckets[r];
            toBucket = partition * numActiveBuckets + startBucket + bucket;
        }

        //using namespace std;
        //cerr << "transferring " << i << " of " << numWorkItems << " partition " << partition << " feature " << f
        //     << " row " << r << ":" << decodedRow << " toBucket " << toBucket << endl;

        //cerr << "  " << i << " transferring row " << r << " to partition " << partition << " bucket " << toBucket << endl;

        float weight = fabs(decodedRow);
        bool label = decodedRow < 0;

        // TODO: needs to be an atomic add when multi-threaded...
        wGlobal[toBucket].add(label, weight);
    }

    //cerr << "numSkipped = " << numSkipped << " of " << rowCount << endl;
}

// For each partition and each bucket, we up to now accumulated just the
// weight that needs to be transferred in the right hand side of the
// partition splits.  We need to fix this up by subtracting this weight from
// the left hand side.
//
// This is a 2 dimensional kernel:
// Dimension 0 = partition number (from 0 to the old number of partitions)
// Dimension 1 = bucket number (from 0 to the number of active buckets)
void
fixupBucketsKernel(ComputeContext & context,
                   ComputeKernelGridRange & bucketRange,

                   std::span<const TreeTrainingInfo> treeTrainingInfo, // [1]
                   std::span<TreeDepthInfo> treeDepthInfo, //[1]

                   std::span<W> allPartitionBuckets,
                   std::span<W> wAll,
                   std::span<const PartitionInfo> partitionInfo,
                   std::span<const uint8_t> smallSideIndexes)
{
    auto numActiveBuckets = treeTrainingInfo[0].numActiveBuckets;
    auto numActivePartitions = treeDepthInfo[0].prevNumActivePartitions;
    
    ExcAssertEqual(numActiveBuckets, bucketRange.range());

    for (uint32_t partition = 0;  partition < numActivePartitions;  ++partition) {

        const PartitionInfo & info = partitionInfo[partition];

        //using namespace std;
        //cerr << "partition " << partition << " info.left = " << info.left << " info.right = " << info.right << endl;

        if (info.ignore())
            continue;

        ExcAssertNotEqual(info.left, info.right);

        // The small side always gets subtracted from the big side
        int to, from;
        if (smallSideIndexes[info.left]) {
            ExcAssert(!smallSideIndexes[info.right]);
            to = info.right;
            from = info.left;
        }
        else {
            ExcAssert(!smallSideIndexes[info.left]);
            to = info.left;
            from = info.right;
        }

        ExcAssertGreaterEqual(wAll[to].count(), wAll[from].count());

        //cerr << "moving buckets from " << from << " to " << to << endl;

        std::span<W> bucketsFrom
            = allPartitionBuckets.subspan(from * numActiveBuckets, numActiveBuckets);
        std::span<W> bucketsTo
            = allPartitionBuckets.subspan(to * numActiveBuckets, numActiveBuckets);
        
        bool hasZero = false;
        for (uint32_t bucket: bucketRange) {
            hasZero = hasZero || bucket == 0;
            //if (bucketsFrom[bucket].count() > 0) {
            //    cerr << "  bucket " << bucket << " subtracting "
            //        << jsonEncodeStr(bucketsFrom[bucket]) << " from " << jsonEncodeStr(bucketsTo[bucket]) << endl;
            //}

            bucketsTo[bucket] -= bucketsFrom[bucket];
        }

        // Bucket zero also updates wAll
        if (hasZero) {
            // wAll: partitions " << from << " = " << jsonEncodeStr(wAll[from])
            //     << " by subtracting " << to << " = " << jsonEncodeStr(wAll[to]) << endl;
            wAll[to] -= wAll[from];
        }
    }

    if (false) {
        std::ofstream splitsStream("tree-buckets-" + std::to_string(treeTrainingInfo[0].featureSampling)
                                + "-" + std::to_string(treeTrainingInfo[0].featureVectorSampling)
                                + "-depth" + std::to_string(treeDepthInfo[0].depth)
                                + ".txt", std::ios_base::app);
        splitsStream << "wAll" << endl;
        for (size_t i = 0;  i < treeDepthInfo[0].numActivePartitions;  ++i) {
            splitsStream << "  " << i << " " << jsonEncodeStr(wAll[i]) << endl;
        }
        for (size_t i = 0;  i < treeDepthInfo[0].numActivePartitions;  ++i) {
            splitsStream << "  part " << i << endl;
            for (size_t j = 0;  j < treeTrainingInfo[0].numActiveBuckets;  ++j) {
                splitsStream << "    bucket " << j << jsonEncodeStr(allPartitionBuckets[i * treeTrainingInfo[0].numActiveBuckets + j]) << endl;
            }
        }
    }
}

static struct RegisterKernels {

    static void doNothingKernelCpu(ComputeContext & context)
    {
    }

    RegisterKernels()
    {
        auto createDoNothingKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "doNothing";
            result->device = ComputeDevice::host();
            result->setComputeFunction(doNothingKernelCpu);
            return result;
        };

        registerHostComputeKernel("doNothing", createDoNothingKernel);
        
        auto createDecodeRowsKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "decodeRows";
            result->device = ComputeDevice::host();
            result->addDimension("r", "numRows");
            result->addParameter("rowData", "r", "u64[rowDataLength]");
            result->addParameter("rowDataLength", "r", "u32");
            result->addParameter("weightBits", "r", "u16");
            result->addParameter("exampleNumBits", "r", "u16");
            result->addParameter("numRows", "r", "u32");
            result->addParameter("weightFormat", "r", "WeightFormat");
            result->addParameter("weightMultiplier", "r", "f32");
            result->addParameter("weightData", "r", "f32[weightDataLength]");
            result->addParameter("decodedRowsOut", "w", "f32[numRows]");
            result->set1DComputeFunction(decodeRowsKernelCpu);
            return result;
        };

        registerHostComputeKernel("decodeRows", createDecodeRowsKernel);

        auto createTestFeatureKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "testFeature";
            result->device = ComputeDevice::host();
            result->addDimension("featureIdx", "naf");
            result->addDimension("rowNum", "nr");
            result->addParameter("decodedRows", "r", "f32[numRows]");
            result->addParameter("numRows", "r", "u32");
            result->addParameter("bucketData", "r", "u32[bucketDataLength]");
            result->addParameter("bucketDataOffsets", "r", "u32[nf + 1]");
            result->addParameter("bucketNumbers", "r", "u32[nf]");
            result->addParameter("bucketEntryBits", "r", "u32[nf]");
            result->addParameter("activeFeatureList", "r", "u32[naf]");
            result->addParameter("partitionBuckets", "rw", "W32[numBuckets]");
            result->addConstraint("numBuckets", "==", "readArrayElement(treeDepthInfo, 0).numFuckingBuckets");
            result->set2DComputeFunction(testFeatureKernel);
            return result;
        };

        registerHostComputeKernel("testFeature", createTestFeatureKernel);

        auto createGetPartitionSplitsKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "getPartitionSplits";
            result->device = ComputeDevice::host();
            result->addDimension("f", "nf");
            result->addParameter("treeTrainingInfo", "r", "TreeTrainingInfo[=1]");
            result->addParameter("bucketNumbers", "r", "u32[nf]");
            result->addParameter("activeFeatureList", "r", "u32[nf]");
            result->addParameter("featureIsOrdinal", "r", "u32[nf]");
            result->addParameter("buckets", "r", "W32[numActiveBuckets * nap]");
            result->addParameter("wAll", "r", "W32[nap]");
            result->addParameter("featurePartitionSplitsOut", "w", "PartitionSplit[np * nf]");
            result->addParameter("treeDepthInfo", "r", "TreeDepthInfo[=1]");
            result->addPreConstraint("nap", "==", "readArrayElement(treeDepthInfo, 0).numActivePartitions");
            result->addPostConstraint("nap", "==", "readArrayElement(treeDepthInfo, 0).numActivePartitions");
            result->set1DComputeFunction(getPartitionSplitsKernel);
            return result;
        };

        registerHostComputeKernel("getPartitionSplits", createGetPartitionSplitsKernel);

        auto createBestPartitionSplitKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "bestPartitionSplit";
            result->device = ComputeDevice::host();
            result->addParameter("treeTrainingInfo", "r", "TreeTrainingInfo[1]");
            result->addParameter("treeDepthInfo", "r", "TreeDepthInfo[1]");
            result->addParameter("activeFeatureList", "r", "u32[numFeatures]");
            result->addParameter("featurePartitionSplits", "r", "PartitionSplit[np * numActiveFeatures]");
            result->addParameter("partitionIndexes", "r", "PartitionIndex[nap]");
            result->addParameter("allPartitionSplitsOut", "w", "IndexedPartitionSplit[nap]");
            result->addPreConstraint("nap", "==", "readArrayElement(treeDepthInfo, 0).numActivePartitions");
            result->addPostConstraint("nap", "==", "readArrayElement(treeDepthInfo, 0).numActivePartitions");
            result->setComputeFunction(bestPartitionSplitKernel);
            return result;
        };

        registerHostComputeKernel("bestPartitionSplit", createBestPartitionSplitKernel);

        auto createAssignPartitionNumbersKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "assignPartitionNumbers";
            result->device = ComputeDevice::host();
            result->addParameter("treeTrainingInfo", "r", "TreeTrainingInfo[1]");
            result->addParameter("treeDepthInfo", "rw", "TreeDepthInfo[1]");
            result->addParameter("allPartitionSplits", "r", "IndexedPartitionSplit[numActivePartitions]");
            result->addParameter("partitionIndexesOut", "w", "PartitionIndex[newNumActivePartitions]");
            result->addParameter("partitionInfoOut", "w", "PartitionInfo[numActivePartitions]");
            result->addParameter("smallSideIndexesOut", "w", "u8[newNumActivePartitions]");
            result->addParameter("smallSideIndexToPartitionOut", "w", "u16[nssi2p]");
            result->addPreConstraint("numActivePartitions", "==", "readArrayElement(treeDepthInfo, 0).numActivePartitions");
            result->addPostConstraint("numActivePartitions", "==", "readArrayElement(treeDepthInfo, 0).prevNumActivePartitions");
            result->addPostConstraint("newNumActivePartitions", "==", "readArrayElement(treeDepthInfo, 0).numActivePartitions");
            result->addPreConstraint("nssi2p", "==", "256");
            result->addPostConstraint("nssi2p", "==", "min(256, newNumActivePartitions/2)");
            result->setComputeFunction(assignPartitionNumbersKernel);
            return result;
        };

        registerHostComputeKernel("assignPartitionNumbers", createAssignPartitionNumbersKernel);
        
        auto createClearBucketsKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "clearBuckets";
            result->device = ComputeDevice::host();
            result->addDimension("b", "numActiveBuckets");
            result->addParameter("treeTrainingInfo", "r", "TreeTrainingInfo[=1]");
            result->addParameter("treeDepthInfo", "rw", "TreeDepthInfo[=1]");
            result->addParameter("bucketsOut", "w", "W32[numActiveBuckets * numActivePartitions]");
            result->addParameter("wAllOut", "rw", "W32[numActivePartitions]");
            result->addParameter("numNonZeroDirectionIndices", "w", "u32[=1]");
            result->addParameter("smallSideIndexes", "r", "u8[newNumActivePartitions]");
            result->addPreConstraint("numActivePartitions", "==", "readArrayElement(treeDepthInfo, 0).prevNumActivePartitions");
            result->addPostConstraint("numActivePartitions", "==", "readArrayElement(treeDepthInfo, 0).prevNumActivePartitions");

            result->set1DComputeFunction(clearBucketsKernel);
            return result;
        };

        registerHostComputeKernel("clearBuckets", createClearBucketsKernel);

        auto createUpdatePartitionNumbersKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "updatePartitionNumbers";
            result->device = ComputeDevice::host();
            result->addDimension("r", "numRows");
            result->addParameter("treeTrainingInfo", "r", "TreeTrainingInfo[1]");
            result->addParameter("treeDepthInfo", "rw", "TreeDepthInfo[1]");
            result->addParameter("partitions", "r", "RowPartitionInfo[numRows]");
            result->addParameter("directions", "w", "u32[(numRows+31)/32]");
            result->addParameter("numNonZeroDirectionIndices", "rw", "u32[1]");
            result->addParameter("nonZeroDirectionIndices", "w", "UpdateWorkEntry[nndi:unordered]");
            result->addParameter("smallSideIndexes", "r", "u8[numActivePartitions]");
            result->addParameter("allPartitionSplits", "r", "IndexedPartitionSplit[numActivePartitions]");
            result->addParameter("partitionInfo", "r", "PartitionInfo[numActivePartitions]");
            result->addParameter("bucketData", "r", "u32[bucketDataLength]");
            result->addParameter("bucketDataOffsets", "r", "u32[nf + 1]");
            result->addParameter("bucketNumbers", "r", "u32[nf]");
            result->addParameter("bucketEntryBits", "r", "u32[nf]");
            result->addParameter("featureIsOrdinal", "r", "u32[nf]");
            result->addParameter("decodedRows", "r", "f32[nr]");
            result->addPreConstraint("numActivePartitions", "==", "readArrayElement(treeDepthInfo, 0).prevNumActivePartitions");
            result->addPostConstraint("numActivePartitions", "==", "readArrayElement(treeDepthInfo, 0).prevNumActivePartitions");
            result->addPreConstraint("nndi", "==", "numRows/2 + 2", "Loose sizing pre-call");
            result->addPostConstraint("nndi", "==", "readArrayElement(numNonZeroDirectionIndices, 0)", "Tight sizing post-call");
            result->set1DComputeFunction(updatePartitionNumbersKernel);
            return result;
        };

        registerHostComputeKernel("updatePartitionNumbers", createUpdatePartitionNumbersKernel);

        auto createUpdateBucketsKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "updateBuckets";
            result->device = ComputeDevice::host();
            result->addDimension("r", "numRows");
            result->addDimension("fidx", "naf");
            result->addParameter("treeTrainingInfo", "r", "TreeTrainingInfo[1]");
            result->addParameter("treeDepthInfo", "rw", "TreeDepthInfo[1]");
            result->addParameter("partitions", "r", "RowPartitionInfo[numRows]");
            result->addParameter("directions", "r", "u8[(numRows+31)/32]");
            result->addParameter("numNonZeroDirectionIndices", "rw", "u32[1]");
            result->addParameter("nonZeroDirectionIndices", "r", "UpdateWorkEntry[nndi:unordered]");
            result->addParameter("buckets", "w", "W32[numActiveBuckets * newNumActivePartitions]");
            result->addParameter("wAll", "rw", "W32[newNumActivePartitions]");
            result->addParameter("smallSideIndexes", "r", "u8[numActivePartitions]");
            result->addParameter("smallSideIndexToPartition", "r", "u16[min(256,newNumActivePartitions/2)]");
            result->addParameter("decodedRows", "r", "f32[nr]");
            result->addParameter("bucketData", "r", "u32[bucketDataLength]");
            result->addParameter("bucketDataOffsets", "r", "u32[nf + 1]");
            result->addParameter("bucketNumbers", "r", "u32[nf]");
            result->addParameter("bucketEntryBits", "r", "u32[nf]");
            result->addParameter("activeFeatureList", "r", "u32[numActiveFeatures]");
            result->addParameter("featureIsOrdinal", "r", "u32[nf]");
            result->addConstraint("nndi", "==", "readArrayElement(numNonZeroDirectionIndices, 0)");
            result->addConstraint("numActivePartitions", "==", "readArrayElement(treeDepthInfo, 0).prevNumActivePartitions");
            result->addConstraint("newNumActivePartitions", "==", "readArrayElement(treeDepthInfo, 0).numActivePartitions");
            result->set2DComputeFunction(updateBucketsKernel);
            return result;
        };

        registerHostComputeKernel("updateBuckets", createUpdateBucketsKernel);

        auto createFixupBucketsKernel = [] () -> std::shared_ptr<ComputeKernel>
        {
            auto result = std::make_shared<HostComputeKernel>();
            result->kernelName = "fixupBuckets";
            result->device = ComputeDevice::host();
            result->addDimension("bucket", "numActiveBuckets");
            result->addParameter("treeTrainingInfo", "r", "TreeTrainingInfo[=1]");
            result->addParameter("treeDepthInfo", "rw", "TreeDepthInfo[=1]");
            result->addParameter("buckets", "rw", "W32[numActiveBuckets * numActivePartitions]");
            result->addParameter("wAll", "rw", "W32[newNumActivePartitions]");
            result->addParameter("partitionInfo", "r", "PartitionInfo[oldNumActivePartitions]");
            result->addParameter("smallSideIndexes", "r", "u8[oldNumActivePartitions]");
            result->addConstraint("oldNumActivePartitions", "==", "readArrayElement(treeDepthInfo, 0).prevNumActivePartitions");
            result->addConstraint("newNumActivePartitions", "==", "readArrayElement(treeDepthInfo, 0).numActivePartitions");
            result->addPreConstraint("numActivePartitions", "==", "oldNumActivePartitions");
            result->addPostConstraint("numActivePartitions", "==", "newNumActivePartitions");
            result->set1DComputeFunction(fixupBucketsKernel);
            return result;
        };

        registerHostComputeKernel("fixupBuckets", createFixupBucketsKernel);

    }

} registerKernels;

} // namespace RF
} // namespace MLDB
