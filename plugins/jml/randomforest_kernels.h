/** randomforest_kernels.h                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#pragma once

#include "mldb/compiler/compiler.h"
#include "randomforest_types.h"

namespace MLDB {
namespace RF {





// Core kernel of the decision tree search algorithm.  Transfer the
// example weight into the appropriate (bucket,label) accumulator.
// Returns whether
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

// Calculates the score of a split, which is a measure of the
// amount of mutual entropy between the label and the given
// candidate split point.
inline double scoreSplit(const W & wFalse, const W & wTrue)
{
    double score
        = 2.0 * (  sqrt(wFalse[0] * wFalse[1])
                   + sqrt(wTrue[0] * wTrue[1]));
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
                  const W & wAll);
    
std::tuple<double /* bestScore */,
           int /* bestSplit */,
           W /* bestLeft */,
           W /* bestRight */,
           bool /* feature is still active */ >
testFeatureNumber(int featureNum,
                  const std::vector<Feature> & features,
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
        const std::vector<Feature> & features,
        const Rows & rows,
        FrozenMemoryRegionT<uint32_t> bucketMemory);


/*****************************************************************************/
/* RECURSIVE RANDOM FOREST KERNELS                                           */
/*****************************************************************************/

struct PartitionSplit {
    float score = INFINITY;
    int feature = -1;
    int value = -1;
    W left;
    W right;
    bool direction;  // 0 = left to right, 1 = right to left
    std::vector<int> activeFeatures;
    char padding[24 - sizeof(std::vector<int>)];  // ensure size for OpenCL
};

DECLARE_STRUCTURE_DESCRIPTION(PartitionSplit);

struct PartitionEntry {
    std::vector<float> decodedRows;
    std::vector<Feature> features;
    std::vector<int> activeFeatures;
    std::set<int> activeFeatureSet;
    W wAll;

    // Get a contiguous block of memory for all of the feature
    // blocks on each side; this enables a single GPU transfer
    // and a single GPU argument list.
    std::vector<size_t> bucketMemoryOffsets;
    MutableMemoryRegionT<uint32_t> mutableBucketMemory;
    FrozenMemoryRegionT<uint32_t> bucketMemory;
};

/** Given the set of splits that have been identified per partition,
    update the list of partition numbers, buckets and W values
    by applying the splits over each row.
*/
void updateBuckets(const std::vector<Feature> & features,
                   std::vector<uint16_t> & partitions,
                   std::vector<std::vector<W> > & buckets,
                   std::vector<W> & wAll,
                   const std::vector<uint32_t> & bucketOffsets,
                   const std::vector<PartitionSplit> & partitionSplits,
                   int rightOffset,
                   const std::vector<float> & decodedRows);

/** Once we've reached the deepest possible level for a breadth first
    split, we need to compact the dataset back down into one per
    partition and continue recursively.  This function accomplishes
    that.
*/
std::pair<std::vector<PartitionEntry>,
          FrozenMemoryRegionT<uint32_t> >
splitPartitions(const std::vector<Feature> features,
                const std::vector<int> & activeFeatures,
                const std::vector<float> & decodedRows,
                const std::vector<uint16_t> & partitions,
                const std::vector<W> & w,
                MappedSerializer & serializer);


/** This function takes the W values of each bucket of a number of
    partitions, and calculates the optimal split feature and value
    for each of the partitions.  It's the "search" part of the
    decision tree algorithm, but across a full set of rows split into
    multiple partitions.
*/
std::vector<PartitionSplit>
getPartitionSplits(const std::vector<std::vector<W> > & buckets,
                   const std::vector<int> & activeFeatures,
                   const std::vector<uint32_t> & bucketOffsets,
                   const std::vector<Feature> & features,
                   const std::vector<W> & wAll,
                   bool parallel);

// Check that the partition counts match the W counts.
void verifyPartitionBuckets(const std::vector<uint16_t> & partitions,
                            const std::vector<W> & wAll);


// Split our dataset into a separate dataset for each leaf, and
// recurse to create a leaf node for each.  This is mutually
// recursive with trainPartitionedRecursive.
std::vector<ML::Tree::Ptr>
splitAndRecursePartitioned(int depth, int maxDepth,
                           ML::Tree & tree,
                           MappedSerializer & serializer,
                           std::vector<std::vector<W> > buckets,
                           const std::vector<uint32_t> & bucketOffsets,
                           const std::vector<Feature> & features,
                           const std::vector<int> & activeFeatures,
                           const std::vector<float> & decodedRows,
                           const std::vector<uint16_t> & partitions,
                           const std::vector<W> & wAll,
                           const DatasetFeatureSpace & fs,
                           FrozenMemoryRegionT<uint32_t> bucketMemory);

ML::Tree::Ptr
trainPartitionedRecursive(int depth, int maxDepth,
                          ML::Tree & tree,
                          MappedSerializer & serializer,
                          const std::vector<uint32_t> & bucketOffsets,
                          const std::vector<int> & activeFeatures,
                          std::vector<W> bucketsIn,
                          const std::vector<float> & decodedRows,
                          const W & wAllInput,
                          const DatasetFeatureSpace & fs,
                          const std::vector<Feature> & features,
                          FrozenMemoryRegionT<uint32_t> bucketMemory);

// Recursively go through and extract a tree from a set of recorded
// PartitionSplits.  There is no calculating going on here, just
// translation of the non-recuirsive data structure into a recursive
// version.
ML::Tree::Ptr
extractTree(int relativeDepth, int depth, int maxDepth,
            int partition,
            ML::Tree & tree,
            const std::vector<std::vector<PartitionSplit> > & depthSplits,
            const std::vector<ML::Tree::Ptr> & leaves,
            const std::vector<Feature> & features,
            const DatasetFeatureSpace & fs);


ML::Tree::Ptr
trainPartitionedEndToEnd(int depth, int maxDepth,
                         ML::Tree & tree,
                         MappedSerializer & serializer,
                         const Rows & rows,
                         const std::vector<Feature> & features,
                         FrozenMemoryRegionT<uint32_t> bucketMemory,
                         const DatasetFeatureSpace & fs);


} // namespace RF
} // namespace MLDB
