/** randomforest_kernels.h                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#pragma once

#include "mldb/compiler/compiler.h"
#include "randomforest_types.h"
#include "mldb/block/compute_kernel.h"
#include <span>

namespace MLDB {
namespace RF {

// Take a rows list and turn it into an array of float, with the sign
// being the label and the magnitude being the example weight for the
// row.
std::vector<float> decodeRows(const Rows & rows);

void decodeRowsKernelCpu(ComputeContext & context,
                         MemoryArrayHandleT<const uint64_t> rowData,
                         uint32_t rowDataLength,
                         uint16_t weightBits,
                         uint16_t exampleNumBits,
                         uint32_t numRows,
                         WeightFormat weightFormat,
                         float weightMultiplier,
                         MemoryArrayHandleT<const float> weightData,
                         MemoryArrayHandleT<float> decodedRowsOut);

// Core kernel of the decision tree search algorithm.  Transfer the
// example weight into the appropriate (bucket,label) accumulator.
// Returns whether
std::pair<bool, int>
testFeatureKernelCpu(Rows::RowIterator rowIterator,
                  size_t numRows,
                  const BucketList & buckets,
                  W * w /* buckets.numBuckets entries */);

std::pair<bool, int>
testFeatureKernelCpu(const float * decodedRows,
                  size_t numRows,
                  const BucketList & buckets,
                  W * w /* buckets.numBuckets entries */);

std::pair<bool, int>
testFeatureKernel(Rows::RowIterator rowIterator,
                  size_t numRows,
                  const BucketList & buckets,
                  W * w /* buckets.numBuckets entries */);

std::pair<bool, int>
testFeatureKernel(const float * decodedRows,
                  size_t numRows,
                  const BucketList & buckets,
                  W * w /* buckets.numBuckets entries */);

std::pair<bool, int>
testFeatureKernel(Rows::RowIterator rowIterator,
                  size_t numRows,
                  const uint16_t * buckets,
                  W * w /* buckets.numBuckets entries */);

std::pair<bool, int>
testFeatureKernel(Rows::RowIterator rowIterator,
                  size_t numRows,
                  const uint32_t * buckets,
                  W * w /* buckets.numBuckets entries */);

void
testFeatureKernel(ComputeContext & context,
                  uint32_t f, uint32_t nf,
                  MemoryArrayHandleT<const float> expandedRows,
                  uint32_t numRows,

                  std::span<const uint32_t> allBucketData,
                  MemoryArrayHandleT<const uint32_t> bucketDataOffsets,
                  MemoryArrayHandleT<const uint32_t> bucketNumbers,
                  MemoryArrayHandleT<const uint32_t> bucketEntryBits,

                  MemoryArrayHandleT<const uint32_t> featureActive,

                  MemoryArrayHandleT<W> allWOut);

// Calculates the score of a split, which is a measure of the
// amount of mutual entropy between the label and the given
// candidate split point.
inline float scoreSplit(const W & wFalse, const W & wTrue)
{
    float score
        = 2.0f *  (  sqrt(wFalse[0] * wFalse[1])
                   + sqrt(wTrue[0]  * wTrue[1]));
    return score;
};

// Chooses which is the best split for a given feature.
MLDB_NEVER_INLINE
std::tuple<double /* bestScore */,
           int /* bestSplit */,
           W /* bestLeft */,
           W /* bestRight */>
chooseSplitKernel(const W * w /* at least maxBucket + 1 entries */,
                  int maxBucket,
                  bool ordinal,
                  const W & wAll,
                  bool debug = false);
    
std::tuple<double /* bestScore */,
           int /* bestSplit */,
           W /* bestLeft */,
           W /* bestRight */,
           bool /* feature is still active */ >
testFeatureNumber(int featureNum,
                  const std::span<const Feature> & features,
                  Rows::RowIterator rowIterator,
                  size_t numRows,
                  const W & wAll);

/** Main kernel for random forest training.

    Test all features for a split.  Returns the feature number,
    the bucket number and the goodness of the split.

    Outputs
    - Z score of split
    - Feature number
    - Split point (bucket number)
    - W for the left side of the split
    - W from the right side of the split
    - A new list of active features

    Features that are inactive from here on are recorded by mutating
    the active flag in the features argument.
*/

std::tuple<double, int, int, W, W, std::vector<uint8_t> >
testAll(int depth,
        const std::span<const Feature> & features,
        const Rows & rows,
        FrozenMemoryRegionT<uint32_t> bucketMemory,
        bool trace = false);


/*****************************************************************************/
/* RECURSIVE RANDOM FOREST KERNELS                                           */
/*****************************************************************************/

/** Given the set of splits that have been identified per partition,
    update the list of partition numbers, buckets and W values
    by applying the splits over each row.

    Returns the number of rows that are within an active bucket.
*/
void
updateBuckets(const std::span<const Feature> & features,
              std::vector<uint32_t> & partitions, // per row
              std::vector<std::vector<W> > & buckets,  // par part
              std::vector<W> & wAll,  // per part
              const std::span<const uint32_t> & bucketOffsets,
              const std::span<const PartitionSplit> & partitionSplits,
              const std::span<const std::pair<int32_t, int32_t> > & newPartitionNumbers,
              int newNumPartitions,
              const std::span<const float> & decodedRows,
              const std::span<const int> & activeFeatures);

/** Once we've reached the deepest possible level for a breadth first
    split, we need to compact the dataset back down into one per
    partition and continue recursively.  This function accomplishes
    that.
*/
std::pair<std::vector<PartitionEntry>,
          FrozenMemoryRegionT<uint32_t> >
splitPartitions(const std::span<const Feature> features,
                const std::span<const int> & activeFeatures,
                const std::span<const float> & decodedRows,
                const std::span<const uint32_t> & partitions,
                const std::span<const W> & w,
                const std::span<const PartitionIndex> & indexes,
                MappedSerializer & serializer);


/** This function takes the W values of each bucket of a number of
    partitions, and calculates the optimal split feature and value
    for each of the partitions.  It's the "search" part of the
    decision tree algorithm, but across a full set of rows split into
    multiple partitions.
*/
std::vector<PartitionSplit>
getPartitionSplits(const std::vector<std::vector<W> > & buckets,
                   const std::span<const int> & activeFeatures,
                   const std::span<const uint32_t> & bucketOffsets,
                   const std::span<const Feature> & features,
                   const std::span<const W> & wAll,
                   const std::span<const PartitionIndex> & indexes,
                   bool parallel);

// Check that the partition counts match the W counts.
void verifyPartitionBuckets(const std::span<const uint32_t> & partitions,
                            const std::span<const W> & wAll);


// Split our dataset into a separate dataset for each leaf, and
// recurse to create a leaf node for each.  This is mutually
// recursive with trainPartitionedRecursive.
std::map<PartitionIndex, ML::Tree::Ptr>
splitAndRecursePartitioned(int depth, int maxDepth,
                           ML::Tree & tree,
                           MappedSerializer & serializer,
                           std::vector<std::vector<W>> buckets,
                           const std::span<const uint32_t> & bucketOffsets,
                           const std::span<const Feature> & features,
                           const std::span<const int> & activeFeatures,
                           const std::span<const float> & decodedRows,
                           const std::span<const uint32_t> & partitions,
                           const std::span<const W> & wAll,
                           const std::span<const PartitionIndex> & indexes,
                           const DatasetFeatureSpace & fs,
                           FrozenMemoryRegionT<uint32_t> bucketMemory);

ML::Tree::Ptr
trainPartitionedRecursive(int depth, int maxDepth,
                          ML::Tree & tree,
                          MappedSerializer & serializer,
                          const std::span<const uint32_t> & bucketOffsets,
                          const std::span<const int> & activeFeatures,
                          std::vector<W> bucketsIn,
                          const std::span<const float> & decodedRows,
                          const W & wAllInput,
                          PartitionIndex root,
                          const DatasetFeatureSpace & fs,
                          const std::span<const Feature> & features,
                          FrozenMemoryRegionT<uint32_t> bucketMemory);

// Recursively go through and extract a tree from a set of recorded
// PartitionSplits.  There is no calculating going on here, just
// translation of the non-recuirsive data structure into a recursive
// version.
ML::Tree::Ptr
extractTree(int depth, int maxDepth,
            ML::Tree & tree,
            PartitionIndex root,
            const std::map<PartitionIndex, PartitionSplit> & allSplits,
            const std::map<PartitionIndex, ML::Tree::Ptr> & leaves,
            const std::span<const Feature> & features,
            const DatasetFeatureSpace & fs);


ML::Tree::Ptr
trainPartitionedEndToEnd(int depth, int maxDepth,
                         ML::Tree & tree,
                         MappedSerializer & serializer,
                         const Rows & rows,
                         const std::span<const Feature> & features,
                         FrozenMemoryRegionT<uint32_t> bucketMemory,
                         const DatasetFeatureSpace & fs);

ML::Tree::Ptr
trainPartitionedRecursiveCpu(int depth, int maxDepth,
                             ML::Tree & tree,
                             MappedSerializer & serializer,
                             const std::span<const uint32_t> & bucketOffsets,
                             const std::span<const int> & activeFeatures,
                             std::vector<W> bucketsIn,
                             const std::span<const float> & decodedRows,
                             const W & wAllInput,
                             PartitionIndex root,
                             const DatasetFeatureSpace & fs,
                             const std::span<const Feature> & features,
                             FrozenMemoryRegionT<uint32_t> bucketMemory);

ML::Tree::Ptr
trainPartitionedEndToEndCpu(int depth, int maxDepth,
                            ML::Tree & tree,
                            MappedSerializer & serializer,
                            const Rows & rows,
                            const std::span<const Feature> & features,
                            FrozenMemoryRegionT<uint32_t> bucketMemory,
                            const DatasetFeatureSpace & fs);

} // namespace RF
} // namespace MLDB
