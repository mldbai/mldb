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
        FrozenMemoryRegionT<uint32_t> bucketMemory,
        bool trace = false);


/*****************************************************************************/
/* RECURSIVE RANDOM FOREST KERNELS                                           */
/*****************************************************************************/

/** Partition indexes indicate where a tree fragment exists in the
    tree. They cover the tree space up to a depth of 32.

    Index 0 is null; no partition has that index.
    Index 1 is the root (depth 0).
    Indexes 2 and 3 are the left and right of the root (depth 1).
    Indexes 4 and 5 are the left children of 2 and 3; indexes 6 and 7 are
    the right children of 2 and 3.

    index 0: 0000: left =  0, right =  0, parent = 0, depth = -1
    index 1: 0001: left =  2, right =  3, parent = 0, depth =  0
    index 2: 0010: left =  4, right =  6, parent = 1, depth =  1
    index 3: 0011: left =  5, right =  7, parent = 1, depth =  1
    index 4: 0100: left =  8, right = 12, parent = 2, depth =  2
    index 5: 0101: left =  9, right = 13, parent = 3, depth =  2
    index 6: 0110: left = 10, right = 14, parent = 2, depth =  2
    index 7: 0111: left = 11, right = 15, parent = 3, depth =  2

    And so on down.  The advantages of this scheme are that a static array of
    2^depth elements can be allocated that is sufficient to hold a tree of
    that depth, with each index uniquely identifying a place in that array.
*/

struct PartitionIndex {
    constexpr PartitionIndex() = default;
    constexpr PartitionIndex(uint32_t index)
        : index(index)
    {
    }

    static constexpr PartitionIndex none() { return { 0 }; };
    static constexpr PartitionIndex root() { return { 1 }; };
    
    uint32_t index = 0;

    int32_t depth() const
    {
        return index == 0 ? -1 : 31 - __builtin_clz(index);
    }
    
    PartitionIndex leftChild() const
    {
        return PartitionIndex(index * 2);
    }
    
    PartitionIndex rightChild() const
    {
        return PartitionIndex(index * 2 + 1);
    }
    
    PartitionIndex parent() const
    {
        return PartitionIndex(index >> 1);
    }

    PartitionIndex parentAtDepth(int32_t depth) const
    {
        return PartitionIndex(index >> (this->depth() - depth));
    }

    bool operator == (const PartitionIndex & other) const
    {
        return index == other.index;
    }

    bool operator != (const PartitionIndex & other) const
    {
        return index != other.index;
    }

    bool operator < (const PartitionIndex & other) const
    {
        return index < other.index;
    }

    std::string path() const
    {
        std::string result;

        if (index == 0)
            return result = "none";
        if (index == 1)
            return result = "root";

        for (int d = depth() - 1;  d >= 0;  --d) {
            result += (index & (1 << d) ? 'r' : 'l');
        }

        return result;
    }
};

PREDECLARE_VALUE_DESCRIPTION(PartitionIndex);

std::ostream & operator << (std::ostream & stream, PartitionIndex idx);

/** Holds the split for a partition. */

struct PartitionSplit {
    PartitionIndex index;
    float score = INFINITY;
    int feature = -1;
    int value = -1;
    W left;
    W right;
    bool direction;  // 0 = left to right, 1 = right to left

    bool valid() const { return left.count() > 0 || right.count() > 0; }

    operator std::pair<PartitionIndex, PartitionSplit> const ()
    {
        return { index, *this };
    }
};

DECLARE_STRUCTURE_DESCRIPTION(PartitionSplit);

struct PartitionEntry {
    std::vector<float> decodedRows;
    std::vector<Feature> features;
    std::vector<int> activeFeatures;
    std::set<int> activeFeatureSet;
    W wAll;
    PartitionIndex index;
    
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

    Returns the number of rows that are within an active bucket.
*/
void
updateBuckets(const std::vector<Feature> & features,
              std::vector<uint32_t> & partitions, // per row
              std::vector<std::vector<W> > & buckets,  // par part
              std::vector<W> & wAll,  // per part
              const std::vector<uint32_t> & bucketOffsets,
              const std::vector<PartitionSplit> & partitionSplits,
              const std::vector<std::pair<int32_t, int32_t> > & newPartitionNumbers,
              int newNumPartitions,
              const std::vector<float> & decodedRows,
              const std::vector<int> & activeFeatures);

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
                const std::vector<uint32_t> & partitions,
                const std::vector<W> & w,
                const std::vector<PartitionIndex> & indexes,
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
                   const std::vector<PartitionIndex> & indexes,
                   bool parallel);

// Check that the partition counts match the W counts.
void verifyPartitionBuckets(const std::vector<uint32_t> & partitions,
                            const std::vector<W> & wAll);


// Split our dataset into a separate dataset for each leaf, and
// recurse to create a leaf node for each.  This is mutually
// recursive with trainPartitionedRecursive.
std::map<PartitionIndex, ML::Tree::Ptr>
splitAndRecursePartitioned(int depth, int maxDepth,
                           ML::Tree & tree,
                           MappedSerializer & serializer,
                           std::vector<std::vector<W> > buckets,
                           const std::vector<uint32_t> & bucketOffsets,
                           const std::vector<Feature> & features,
                           const std::vector<int> & activeFeatures,
                           const std::vector<float> & decodedRows,
                           const std::vector<uint32_t> & partitions,
                           const std::vector<W> & wAll,
                           const std::vector<PartitionIndex> & indexes,
                           PartitionIndex root,
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
                          PartitionIndex root,
                          const DatasetFeatureSpace & fs,
                          const std::vector<Feature> & features,
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
