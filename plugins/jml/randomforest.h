/** randomforest.h                                             -*- C++ -*-
    Mathieu Marquis Bolduc, 11 Mars 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Optimized random forest algorithm for dense data and binary classification

*/

#pragma once

#include "randomforest_types.h"
#include "randomforest_kernels.h"

namespace MLDB {
namespace RF {

// Compare the two trees, returning true iff they are (recursively) equal, and if not
// writing a summary of the differences to the stream.
bool compareTrees(ML::Tree::Ptr left, ML::Tree::Ptr right, PartitionIndex index, const DatasetFeatureSpace & fs,
                  std::ostream & stream = std::cerr);

/** Holds the set of data for a partition of a decision tree. */
struct PartitionData {

    PartitionData() = default;

    PartitionData(std::shared_ptr<const DatasetFeatureSpace> fs);

    void clear();

    /** Create a new dataset with the same labels, different weights
        (by element-wise multiplication), and with
        zero weights filtered out such that example numbers are strictly
        increasing.
    */
    PartitionData reweightAndCompact(const std::vector<uint8_t> & counts,
                                     size_t numNonZero,
                                     double scale,
                                     MappedSerializer & serializer) const;

    /// Feature space
    std::shared_ptr<const DatasetFeatureSpace> fs;

    // All rows of data in this partition
    Rows rows;

    /// Memory for all feature buckets
    FrozenMemoryRegionT<uint32_t> bucketMemory;

    // All known features in this partition
    std::vector<Feature> features;

    // Split the partition data into two partitions (left=sides[0], right=sides[1])
    // but without reindexing (row numbers stay the same).  This is quick, but
    // leads to each partition being sparse.
    void splitWithoutReindex(PartitionData * sides,
                             int featureToSplitOn, int splitValue,
                             MappedSerializer & serializer) const;

    // Split the partition data into two partitions (left=sides[0], right=sides[1])
    // and reindex so that each of the sub-partitions is dense.  This takes more time
    // and memory but leads to subsequent operations being faster and requiring less
    // memory bandwidth.
    void splitAndReindex(PartitionData * sides,
                         int featureToSplitOn, int splitValue,
                         MappedSerializer & serializer) const;

    // Split the partition here.   This may reindex depending upon a heuristic to
    // control the trade-off between sparsity vs work in reindexing.  It will call
    // one of the above two functions.
    std::pair<PartitionData, PartitionData>
    split(int featureToSplitOn, int splitValue,
          const W & wLeft, const W & wRight,
          MappedSerializer & serializer) const;

    // Calls the trainPartitionedEndToEnd() kernel driver on this partition.
    ML::Tree::Ptr
    trainPartitioned(int featureVectorSampling,
                     int featureSampling,
                     const std::string & debugName,
                     int depth, int maxDepth,
                     ML::Tree & tree,
                     MappedSerializer & serializer) const;
    
    /// How do we train this?
    enum TrainingScheme {
        PARTITIONED = 1,      ///< Train by partitioning and doing a single pass through the partitions
        RECURSIVE = 2,        ///< Train by recursively descending each split
        BOTH_AND_COMPARE = 3 ///< Train both of the above, and compare the two results
    };

    // Train a decision tree from this partition.
    ML::Tree::Ptr train(int featureVectorSampling,
                        int featureSampling,
                        const std::string & debugName,
                        int depth, int maxDepth,
                        ML::Tree & tree,
                        MappedSerializer & serializer,
                        TrainingScheme trainingScheme = PARTITIONED) const;
    
    // Train a small forest, with the same rows but a different feature sampling
    std::vector<ML::Tree>
    trainMultipleSamplings(int featureVectorSampling,
                           const std::string & debugName,
                           int maxDepth, const std::vector<std::vector<int>> & featuresActive,
                           MappedSerializer & serializer,
                           TrainingScheme trainingScheme = PARTITIONED) const;
};


} // namespace RF
} // namespace MLDB
