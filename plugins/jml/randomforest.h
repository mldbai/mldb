/** randomforest.h                                             -*- C++ -*-
    Mathieu Marquis Bolduc, 11 Mars 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Optimized random forest algorithm for dense data and binary classification

*/

#pragma once

#include "randomforest_types.h"
#include "randomforest_kernels.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/plugins/jml/dataset_feature_space.h"
#include "mldb/plugins/jml/jml/tree.h"
#include "mldb/plugins/jml/jml/stump_training_bin.h"
#include "mldb/plugins/jml/jml/decision_tree.h"
#include "mldb/plugins/jml/jml/committee.h"
#include "mldb/base/parallel.h"
#include "mldb/base/map_reduce.h"
#include "mldb/base/thread_pool.h"
#include "mldb/engine/column_scope.h"
#include "mldb/engine/bucket.h"
#include "mldb/utils/lightweight_hash.h"
#include "mldb/arch/timers.h"
#include <cmath>


namespace MLDB {
namespace RF {

/** Holds the set of data for a partition of a decision tree. */
struct PartitionData {

    PartitionData()
        : fs(nullptr)
    {
    }

    PartitionData(std::shared_ptr<const DatasetFeatureSpace> fs)
        : fs(fs), features(fs->columnInfo.size())
    {
        for (auto & c: fs->columnInfo) {
            Feature & f = features.at(c.second.index);
            f.active = c.second.distinctValues > 1;
            f.buckets = c.second.buckets;
            f.info = &c.second;
            f.ordinal = c.second.bucketDescriptions.isOnlyNumeric();
        }
    }

    void clear()
    {
        rows.clear();
        features.clear();
        fs.reset();
    }
    
    /** Create a new dataset with the same labels, different weights
        (by element-wise multiplication), and with
        zero weights filtered out such that example numbers are strictly
        increasing.

        Each weight is multiplied by counts[i] * scale to get the new
        weight; since this method is called from a sampling with replacement
        procedure we only need integer counts.
    */
    PartitionData reweightAndCompact(const std::vector<uint8_t> & counts,
                                     size_t numNonZero,
                                     double scale,
                                     MappedSerializer & serializer) const
    {
        PartitionData data;
        data.features = this->features;
        data.fs = this->fs;

        ExcAssertEqual(counts.size(), rows.rowCount());
        
        size_t chunkSize
            = std::min<size_t>(100000, rows.rowCount() / numCpus() / 4);

        using namespace std;
        cerr << "chunkSize = " << chunkSize << endl;

        // Analyze the weights.  This may allow us to store them in a lot
        // less bits than we would have otherwise.

        auto doWeightChunk = [=] (int chunk)
            -> std::tuple<LightweightHashSet<float> /* uniques */,
                          float /* minWeight */,
                          size_t /* numValues */>
            {
                size_t start = chunk * chunkSize;
                size_t end = std::min(start + chunkSize, rows.rowCount());

                LightweightHashSet<float> uniques;
                float minWeight = INFINITY;
                size_t numValues = 0;
                
                for (size_t i = start;  i < end;  ++i) {
                    float weight = rows.getWeight(i) * counts[i] * scale;
                    ExcAssert(!std::isnan(weight));
                    ExcAssertGreaterEqual(weight, 0);
                    if (weight != 0) {
                        numValues += 1;
                        uniques.insert(weight);
                        minWeight = std::min(minWeight, weight);
                    }
                }

                return std::make_tuple(std::move(uniques), minWeight,
                                       numValues);
            };

        float minWeight = INFINITY;
        LightweightHashSet<float> allUniques;
        size_t totalNumValues = 0;
        
        auto reduceWeights = [&] (size_t start,
                                  std::tuple<LightweightHashSet<float> /* uniques */,
                                  float /* minWeight */,
                                  size_t /* numValues */> & info)
            {
                minWeight = std::min(minWeight, std::get<1>(info));
                totalNumValues += std::get<2>(info);
                LightweightHashSet<float> & uniques = std::get<0>(info);
                allUniques.insert(uniques.begin(), uniques.end());
            };
        
        parallelMapInOrderReduce
            (0, rows.rowCount() / chunkSize + 1, doWeightChunk, reduceWeights);

        using namespace std;
        cerr << "minWeight = " << minWeight << endl;
        cerr << allUniques.size() << " uniques" << endl;

        std::vector<float> uniqueWeights(allUniques.begin(), allUniques.end());
        std::sort(uniqueWeights.begin(), uniqueWeights.end());
        
        bool integerUniqueWeights = true;
        int maxIntFactor = 0;
        for (float w: uniqueWeights) {
            float floatMult = w / minWeight;
            int intMult = round(floatMult);
            bool isInt = intMult * minWeight == w;//floatMult == intMult;
            if (!isInt && false) {
                cerr << "intMult = " << intMult << " floatMult = "
                     << floatMult << " intMult * minWeight = "
                     << intMult * minWeight << endl;
                integerUniqueWeights = false;
            }
            else maxIntFactor = intMult;
        }

        cerr << "total of " << uniqueWeights.size() << " unique weights"
             << endl;
        cerr << "integerUniqueWeights = " << integerUniqueWeights << endl;
        cerr << "maxIntFactor = " << maxIntFactor << endl;

        int numWeightsToEncode = 0;
        if (integerUniqueWeights) {
            data.rows.weightEncoder.weightFormat = WF_INT_MULTIPLE;
            data.rows.weightEncoder.weightMultiplier = minWeight;
            data.rows.weightEncoder.weightBits = MLDB::highest_bit(maxIntFactor, -1) + 1;
        }
        else if (uniqueWeights.size() < 4096) { /* max 16kb of cache */
            data.rows.weightEncoder.weightFormat = WF_TABLE;
            auto mutableWeightFormatTable
                = serializer.allocateWritableT<float>(uniqueWeights.size());
            std::memcpy(mutableWeightFormatTable.data(),
                        uniqueWeights.data(),
                        uniqueWeights.size() * sizeof(float));
            data.rows.weightEncoder.weightFormatTable = mutableWeightFormatTable.freeze();
            data.rows.weightEncoder.weightBits = MLDB::highest_bit(numWeightsToEncode, -1) + 1;
        }
        else {
            data.rows.weightEncoder.weightFormat = WF_FLOAT;
            data.rows.weightEncoder.weightBits = 32;
        }

        // We split the rows up into tranches to increase parallism
        // To do so, we need to know how many items are in each
        // tranche and where its items start
        size_t numTranches = std::min<size_t>(8, rows.rowCount() / 1024);
        if (numTranches == 0)
            numTranches = 1;

        //numTranches = 1;
        size_t numPerTranche = rows.rowCount() / numTranches;

        // Find the splits such that each one has a multiple of 64
        // non-zero entries (this is a requirement to write them
        // from multiple threads).
        std::vector<size_t> trancheSplits;
        std::vector<size_t> trancheCounts;
        std::vector<size_t> trancheOffsets;
        size_t start = 0;
        size_t offset = 0;

        while (offset < numNonZero) {
            trancheSplits.push_back(start);
            size_t n = 0;
            size_t end = start;
            for (; end < rows.rowCount()
                     && (end < start + numPerTranche
                         || n == 0
                         || n % 64 != 0);  ++end) {
                n += counts[end] != 0;
            }
            trancheCounts.push_back(n);
            trancheOffsets.push_back(offset);
            offset += n;
            start = end;
            ExcAssertLessEqual(start, rows.rowCount());
        }
        ExcAssertEqual(offset, numNonZero);
        trancheOffsets.push_back(offset);
        trancheSplits.push_back(start);

        // Get a contiguous block of memory for all of the feature blocks;
        // this enables a single GPU transfer and a single GPU argument
        // list
        std::vector<size_t> bucketMemoryOffsets(1, 0);
        size_t bucketMemoryRequired = 0;

        for (int f = 0;  f < features.size();  ++f) {
            size_t bytesRequired = 0;
            if (data.features[f].active) {
                size_t wordsRequired
                    = WritableBucketList::wordsRequired
                        (numNonZero,
                         data.features[f].info->distinctValues);
                bytesRequired = wordsRequired * 4;
            }
            bucketMemoryRequired += bytesRequired;
            bucketMemoryOffsets.push_back(bucketMemoryRequired);
        }

        MutableMemoryRegionT<uint32_t> mutableBucketMemory
            = serializer.allocateWritableT<uint32_t>
            (bucketMemoryRequired / 4, 4096 /* page aligned */);

        auto myRange = mutableBucketMemory.rangeBytes(0, bucketMemoryRequired);

        ExcAssertEqual(myRange.length(),  bucketMemoryRequired / 4);

        
        // This gets called for each feature.  It's further subdivided
        // per tranche.
        auto doFeature = [&] (size_t f)
            {
                if (f == data.features.size()) {
                    // Do the row index
                    size_t n = 0;

                    RowWriter writer
                        = data.rows.getRowWriter(numNonZero, numNonZero,
                                                 serializer,
                                                 false /* sequential example num */);

                    Rows::RowIterator rowIterator
                        = rows.getRowIterator();

                    for (size_t i = 0;  i < rows.rowCount();  ++i) {
                        DecodedRow row = rowIterator.getDecodedRow();
                        if (counts[i] == 0 || row.weight == 0)
                            continue;
                        writer.addRow(row.label,
                                      row.weight * counts[i] * scale,
                                      n++);
                    }
                    
                    ExcAssertEqual(n, numNonZero);
                    data.rows = writer.freeze(serializer);
                    return;
                }

                if (!data.features[f].active)
                    return;

#if 0                
                using namespace std;
                cerr << "mem.length()= " << mutableBucketMemory.length()
                     << endl;
                cerr << "mem.data() = " << mutableBucketMemory.data()
                     << endl;
                cerr << "offset from " << bucketMemoryOffsets[f]
                     << " to " << bucketMemoryOffsets[f + 1] << endl;
#endif
                
                auto mem
                    = mutableBucketMemory
                      .rangeBytes(bucketMemoryOffsets[f],
                                  bucketMemoryOffsets[f + 1]);
                
                ParallelWritableBucketList featureBuckets
                    (numNonZero,
                     data.features[f].info->distinctValues,
                     mem);

                auto onTranche = [&] (size_t tr)
                {
                    size_t start = trancheSplits[tr];
                    size_t end = trancheSplits[tr + 1];
                    size_t offset = trancheOffsets[tr];
                    
                    auto writer = featureBuckets.atOffset(offset);

                    size_t n = 0;
                    for (size_t i = start;  i < end;  ++i) {
                        if (counts[i] == 0)
                            continue;
                        
                        uint32_t bucket = features[f].buckets[rows.getExampleNum(i)];
                        writer.write(bucket);
                        ++n;
                    }
                    
                    ExcAssertEqual(n, trancheCounts[tr]);
                };

                parallelMap(0, numTranches, onTranche);

                data.features[f].buckets = featureBuckets.freeze(serializer);
            };

        parallelMap(0, data.features.size() + 1, doFeature);

        data.bucketMemory = mutableBucketMemory.freeze();

        for (size_t i = 0;  i < data.features.size();  ++i) {
            if (features[i].active) {
                ExcAssertGreaterEqual(data.features[i].buckets.storage.data(),
                                      data.bucketMemory.data());
            }
        }
        
        return data;
    }

    /// Feature space
    std::shared_ptr<const DatasetFeatureSpace> fs;

    /// Rows in this partition
    Rows rows;

    /// Memory for all feature buckets
    FrozenMemoryRegionT<uint32_t> bucketMemory;
    
    /// All known features in this partition
    std::vector<Feature> features;

    void splitWithoutReindex(PartitionData * sides,
                             int featureToSplitOn, int splitValue,
                             MappedSerializer & serializer) const
    {
        bool ordinal = features[featureToSplitOn].ordinal;

        RowWriter writer[2]
            = { rows.getRowWriter(rows.rowCount(),
                                  rows.highestExampleNum(),
                                  serializer,
                                  false /* sequential example nums */),
                rows.getRowWriter(rows.rowCount(),
                                  rows.highestExampleNum(),
                                  serializer,
                                  false /* sequential example nums */) };

        Rows::RowIterator rowIterator = rows.getRowIterator();
            
        for (size_t i = 0;  i < rows.rowCount();  ++i) {
            Row row = rowIterator.getRow();
            int bucket
                = features[featureToSplitOn]
                .buckets[row.exampleNum_];
            int side = ordinal ? bucket >= splitValue : bucket != splitValue;
            writer[side].addRow(row);
        }

        sides[0].rows = writer[0].freeze(serializer);
        sides[1].rows = writer[1].freeze(serializer);

        sides[0].bucketMemory = sides[1].bucketMemory = bucketMemory;
    }

    void splitAndReindex(PartitionData * sides,
                         int featureToSplitOn, int splitValue,
                         MappedSerializer & serializer) const
    {
        int nf = features.size();

        // For each example, it goes either in left or right, depending
        // upon the value of the chosen feature.

        std::vector<uint8_t> lr(rows.rowCount());
        bool ordinal = features[featureToSplitOn].ordinal;
        size_t numOnSide[2] = { 0, 0 };

        // TODO: could reserve less than this...
        RowWriter writer[2]
            = { rows.getRowWriter(rows.rowCount(),
                                  rows.rowCount(),
                                  serializer,
                                  true /* sequential example nums */),
                rows.getRowWriter(rows.rowCount(),
                                  rows.rowCount(),
                                  serializer,
                                  true /* sequential example nums */) };

        Rows::RowIterator rowIterator = rows.getRowIterator();

        for (size_t i = 0;  i < rows.rowCount();  ++i) {
            Row row = rowIterator.getRow();
            int bucket = features[featureToSplitOn].buckets[row.exampleNum()];
            int side = ordinal ? bucket >= splitValue : bucket != splitValue;
            lr[i] = side;
            row.exampleNum_ = numOnSide[side]++;
            writer[side].addRow(row);
        }

        // Get a contiguous block of memory for all of the feature
        // blocks on each side; this enables a single GPU transfer
        // and a single GPU argument list.
        std::vector<size_t> bucketMemoryOffsets[2];
        MutableMemoryRegionT<uint32_t> mutableBucketMemory[2];
            
        for (int side = 0;  side < 2;  ++side) {

            bucketMemoryOffsets[side].resize(1, 0);
            size_t bucketMemoryRequired = 0;

            for (int f = 0;  f < nf;  ++f) {
                size_t bytesRequired = 0;
                if (features[f].active) {
                    size_t wordsRequired
                        = WritableBucketList::wordsRequired
                        (numOnSide[side],
                         features[f].info->distinctValues);
                    bytesRequired = wordsRequired * 4;
                }
                bucketMemoryRequired += bytesRequired;
                bucketMemoryOffsets[side].push_back(bucketMemoryRequired);
            }

            mutableBucketMemory[side]
                = serializer.allocateWritableT<uint32_t>
                (bucketMemoryRequired / 4, 4096 /* page aligned */);
        }
            
                
        for (unsigned i = 0;  i < nf;  ++i) {
            if (!features[i].active)
                continue;

            WritableBucketList newFeatures[2];

            newFeatures[0]
                .init(numOnSide[0],
                      features[i].info->distinctValues,
                      mutableBucketMemory[0]
                      .rangeBytes(bucketMemoryOffsets[0][i],
                                  bucketMemoryOffsets[0][i + 1]));
            newFeatures[1]
                .init(numOnSide[1],
                      features[i].info->distinctValues,
                      mutableBucketMemory[1]
                      .rangeBytes(bucketMemoryOffsets[1][i],
                                  bucketMemoryOffsets[1][i + 1]));

            for (size_t j = 0;  j < rows.rowCount();  ++j) {
                int side = lr[j];
                newFeatures[side].write(features[i].buckets[rows.getExampleNum(j)]);
            }

            sides[0].features[i].buckets = newFeatures[0].freeze(serializer);
            sides[1].features[i].buckets = newFeatures[1].freeze(serializer);
        }

        sides[0].rows = writer[0].freeze(serializer);
        sides[1].rows = writer[1].freeze(serializer);

        sides[0].bucketMemory = mutableBucketMemory[0].freeze();
        sides[1].bucketMemory = mutableBucketMemory[1].freeze();
    }
    
    /** Split the partition here. */
    std::pair<PartitionData, PartitionData>
    split(int featureToSplitOn, int splitValue,
          const W & wLeft, const W & wRight,
          MappedSerializer & serializer) const
    {
     //   std::cerr << "spliting on feature " << featureToSplitOn << " bucket " << splitValue << std::endl;
        
        ExcAssertGreaterEqual(featureToSplitOn, 0);
        ExcAssertLess(featureToSplitOn, features.size());

        PartitionData sides[2];
        PartitionData & left = sides[0];
        PartitionData & right = sides[1];

        left.fs = fs;
        right.fs = fs;
        left.features = features;
        right.features = features;
        left.rows.weightEncoder = this->rows.weightEncoder;
        right.rows.weightEncoder = this->rows.weightEncoder;
        
        // Density of example numbers within our set of rows.  When this
        // gets too low, we do essentially random accesses and it kills
        // our cache performance.  In that case we can re-index to reduce
        // the size.
        double useRatio = 1.0 * rows.rowCount() / rows.highestExampleNum();

        //todo: Re-index when usable data fits inside cache
        bool reIndex = useRatio < 0.25;
        //reIndex = false;
        //using namespace std;
        //cerr << "useRatio = " << useRatio << endl;

        if (!reIndex) {
            splitWithoutReindex(sides, featureToSplitOn, splitValue,
                                serializer);
        }
        else {
            splitAndReindex(sides, featureToSplitOn, splitValue, serializer);
        }

        return { std::move(left), std::move(right) };
    }

    static void fillinBase(ML::Tree::Base * node, const W & wAll)
    {

        float total = float(wAll[0]) + float(wAll[1]);
        node->examples = wAll.count();
        node->pred = {
             float(wAll[0]) / total,
             float(wAll[1]) / total };
    }

    ML::Tree::Ptr
    getNode(ML::Tree & tree, float bestScore,
            int bestFeature, int bestSplit,
            ML::Tree::Ptr left, ML::Tree::Ptr right,
            W wLeft, W wRight) const
    {
        ML::Tree::Node * node = tree.new_node();
        ML::Feature feature = fs->getFeature(features[bestFeature].info->columnName);
        float splitVal = 0;
        if (features[bestFeature].ordinal) {
            auto splitCell = features[bestFeature].info->bucketDescriptions
                .getSplit(bestSplit);
            if (splitCell.isNumeric())
                splitVal = splitCell.toDouble();
            else splitVal = bestSplit;
        }
        else {
            splitVal = bestSplit;
        }

        ML::Split split(feature, splitVal,
                        features[bestFeature].ordinal
                        ? ML::Split::LESS : ML::Split::EQUAL);
            
        node->split = split;
        node->child_true = left;
        node->child_false = right;
        W wMissing;
        node->child_missing = getLeaf(tree, wMissing);
        node->z = bestScore;            
        fillinBase(node, wLeft + wRight);

        return node;
    }
        
    ML::Tree::Ptr getLeaf(ML::Tree & tree, const W& w) const
    {     
        ML::Tree::Leaf * node = tree.new_leaf();
        fillinBase(node, w);
        return node;
    }

    ML::Tree::Ptr getLeaf(ML::Tree & tree) const
    {
       return getLeaf(tree, rows.wAll);
    }  

    struct PartitionSplit {
        double score = INFINITY;
        int feature = -1;
        int value = -1;
        W left;
        W right;
        std::vector<int> activeFeatures;
        bool direction;  // 0 = left to right, 1 = right to left
    };

    static void updateBuckets(const Rows & rows,
                              const std::vector<Feature> & features,
                              std::vector<uint8_t> & partitions,
                              std::vector<std::vector<W> > & buckets,
                              std::vector<W> & wAll,
                              const std::vector<uint32_t> & bucketOffsets,
                              const std::vector<PartitionSplit> & partitionSplits,
                              int rightOffset)
    {
        Rows::RowIterator rowIterator = rows.getRowIterator();

        bool checkPartitionCounts = true;
        
        std::vector<uint32_t> numInPartition(buckets.size());
        
        for (size_t i = 0;  i < rows.rowCount();  ++i) {
            // TODO: for each feature, choose right to left vs left to
            // right based upon processing the smallest number of
            // shifts

            int partition = partitions[i];
            int splitFeature = partitionSplits[partition].feature;
                
            if (splitFeature == -1) {
                // reached a leaf here, nothing to split                    
                rowIterator.skipRow();
                continue;
            }
                
            DecodedRow row = rowIterator.getDecodedRow();

            int leftPartition = partition;
            int rightPartition = partition + rightOffset;

            int splitValue = partitionSplits[partition].value;
            bool ordinal = features[splitFeature].ordinal;
            int bucket = features[splitFeature].buckets[row.exampleNum];
            int side = ordinal ? bucket >= splitValue : bucket != splitValue;
            float weight = row.weight;
            bool label = row.label;

            // Set the new partition number
            partitions[i] = partition + side * rightOffset;

            // Verify partition counts?
            if (checkPartitionCounts)
                ++numInPartition[partition + side * rightOffset];
            
            // 0 = left to right, 1 = right to left
            int direction = partitionSplits[partition].direction;
                
            // We only need to update features on the wrong side, as we
            // transfer the weight rather than sum it from the
            // beginning.  This means less work for unbalanced splits
            // (which are typically most of them, especially for discrete
            // buckets)

            if (direction != side) {
                int fromPartition, toPartition;
                if (direction == 0 && side == 1) {
                    // Transfer from left to right
                    fromPartition = leftPartition;
                    toPartition   = rightPartition;
                }
                else {
                    // Transfer from right to left
                    fromPartition = rightPartition;
                    toPartition   = leftPartition;
                }

                // Update the wAll, transfering weight
                wAll[fromPartition].sub(label, weight);
                wAll[toPartition  ].add(label, weight);
                    
                // Transfer the weight from each of the features
                for (auto & f: partitionSplits[partition].activeFeatures) {
                    int startBucket = bucketOffsets[f];
                    int bucket = features[f].buckets[row.exampleNum];
                    buckets[fromPartition][startBucket + bucket]
                        .sub(label, weight);
                    buckets[toPartition  ][startBucket + bucket]
                        .add(label, weight);
                }
            }               
        }

        // Make sure that the number in the buckets is actually what we
        // expected when we calculated the split.
        if (checkPartitionCounts) {
            for (int i = 0;  i < partitionSplits.size();  ++i) {
                int leftPartition = i;
                int rightPartition = i + rightOffset;

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
            }
        }
    }
    
    static void updateBucketsFeature(const Rows & rows,
                                     int f,
                                     const std::vector<Feature> & features,
                                     const std::vector<uint8_t> & partitions,
                                     std::vector<std::vector<W> > & buckets,
                                     const std::vector<uint32_t> & bucketOffsets,
                                     const std::vector<PartitionSplit> & partitionSplits,
                                     int rightOffset)
    {
        Rows::RowIterator rowIterator = rows.getRowIterator();

        // Mask to get the original partition number
        int partitionMask = rightOffset - 1;

        std::vector<uint8_t> featureIsActive(partitionSplits.size());

        for (size_t i = 0;  i < partitionSplits.size();  ++i) {
            featureIsActive[i]
                = std::find(partitionSplits[i].activeFeatures.begin(),
                            partitionSplits[i].activeFeatures.end(),
                            f)
                != partitionSplits[i].activeFeatures.begin();
        }
        
        int startBucket = bucketOffsets[f];
        
        for (size_t i = 0;  i < rows.rowCount();  ++i) {
            int partition = partitions[i] & partitionMask;
            int splitFeature = partitionSplits[partition].feature;
                
            if (splitFeature == -1 || !featureIsActive[partition]) {
                // reached a leaf here, nothing to split                    
                rowIterator.skipRow();
                continue;
            }
                
            DecodedRow row = rowIterator.getDecodedRow();

            int leftPartition = partition;
            int rightPartition = partition + rightOffset;

            int splitValue = partitionSplits[partition].value;
            int bucket = features[splitFeature].buckets[row.exampleNum];
            bool ordinal = features[splitFeature].ordinal;
            int side = ordinal ? bucket >= splitValue : bucket != splitValue;
            double weight = row.weight;
            bool label = row.label;

            // 0 = left to right, 1 = right to left
            int direction = partitionSplits[partition].direction;
                
            // We only need to update features on the wrong side, as we
            // transfer the weight rather than sum it from the
            // beginning.  This means less work for unbalanced splits
            // (which are typically most of them, especially for discrete
            // buckets)

            if (direction != side) {
                int fromPartition, toPartition;
                if (direction == 0 && side == 1) {
                    // Transfer from left to right
                    fromPartition = leftPartition;
                    toPartition   = rightPartition;
                }
                else {
                    // Transfer from right to left
                    fromPartition = rightPartition;
                    toPartition   = leftPartition;
                }

                // Transfer the weight from each of the features
                int bucket = features[f].buckets[row.exampleNum];
                buckets[fromPartition][startBucket + bucket]
                    .sub(label, weight);
                buckets[toPartition  ][startBucket + bucket]
                    .add(label, weight);
            }
        }
    }

    static void updateBucketsCommon(const Rows & rows,
                                    const std::vector<Feature> & features,
                                    std::vector<uint8_t> & partitions,
                                    std::vector<W> & wAll,
                                    const std::vector<PartitionSplit> & partitionSplits,
                                    int rightOffset)
    {
        Rows::RowIterator rowIterator = rows.getRowIterator();

        for (size_t i = 0;  i < rows.rowCount();  ++i) {
            // TODO: for each feature, choose right to left vs left to
            // right based upon processing the smallest number of
            // shifts

            int partition = partitions[i];
            int splitFeature = partitionSplits[partition].feature;
                
            if (splitFeature == -1) {
                // reached a leaf here, nothing to split                    
                rowIterator.skipRow();
                continue;
            }
                
            DecodedRow row = rowIterator.getDecodedRow();

            int leftPartition = partition;
            int rightPartition = partition + rightOffset;

            int splitValue = partitionSplits[partition].value;
            bool ordinal = features[splitFeature].ordinal;
            int bucket = features[splitFeature].buckets[row.exampleNum];
            int side = ordinal ? bucket >= splitValue : bucket != splitValue;
            double weight = row.weight;
            bool label = row.label;

            // Set the new partition number
            partitions[i] = partition + side * rightOffset;

            // 0 = left to right, 1 = right to left
            int direction = partitionSplits[partition].direction;
                
            // We only need to update features on the wrong side, as we
            // transfer the weight rather than sum it from the
            // beginning.  This means less work for unbalanced splits
            // (which are typically most of them, especially for discrete
            // buckets)

            if (direction != side) {
                int fromPartition, toPartition;
                if (direction == 0 && side == 1) {
                    // Transfer from left to right
                    fromPartition = leftPartition;
                    toPartition   = rightPartition;
                }
                else {
                    // Transfer from right to left
                    fromPartition = rightPartition;
                    toPartition   = leftPartition;
                }

                // Update the wAll, transfering weight
                wAll[fromPartition].sub(label, weight);
                wAll[toPartition  ].add(label, weight);
            }               
        }
    }

    static void updateBuckets2(const Rows & rows,
                               const std::vector<Feature> & features,
                               std::vector<uint8_t> & partitions,
                               std::vector<std::vector<W> > & buckets,
                               std::vector<W> & wAll,
                               const std::vector<uint32_t> & bucketOffsets,
                               const std::vector<PartitionSplit> & partitionSplits,
                               int rightOffset,
                               const std::vector<int> & activeFeatures)
    {
        auto doUpdate = [&] (int i)
            {
                if (i == 0)
                    updateBucketsCommon(rows, features, partitions, wAll,
                                        partitionSplits, rightOffset);
                else
                    updateBucketsFeature(rows, activeFeatures[i - 1],
                                         features, partitions,
                                         buckets, bucketOffsets,
                                         partitionSplits, rightOffset);
            };

        parallelMap(0, activeFeatures.size() + 1, doUpdate);
    }
    
    ML::Tree::Ptr
    trainPartitionedRecursive(int depth, int maxDepth,
                              ML::Tree & tree,
                              MappedSerializer & serializer,
                              const std::vector<uint32_t> & bucketOffsets,
                              const std::vector<int> & activeFeatures,
                              std::vector<W> bucketsIn) const
    {
        using namespace std;

        cerr << activeFeatures.size() << " active features "
             << jsonEncodeStr(activeFeatures) << " active buckets "
             << bucketsIn.size() << endl;
        
        int numActiveBuckets = bucketsIn.size();

        // This is our total for each bucket across the whole lot
        // We keep track of it per-partition (there are up to 256
        // partitions, which corresponds to a fanout of 8)
        std::vector<std::vector<W> > buckets;
        buckets.reserve(256);
        buckets.emplace_back(std::move(bucketsIn));

        // Which partition is each row in?  Initially, everything
        // is in partition zero, but as we start to split, we end up
        // splitting them amongst many partitions.  Each partition has
        // its own set of buckets that we maintain.
        std::vector<uint8_t> partitions(rows.rowCount(), 0);

        // What is our weight total for each of our partitions?
        std::vector<W> wAll = { rows.wAll };

        // Record the split, per level, per partition
        std::vector<std::vector<PartitionSplit> > depthSplits;
        depthSplits.reserve(8);
        
        // We go down level by level
        for (int myDepth = 0; myDepth < 8 && depth < maxDepth;
             ++depth, ++myDepth) {

            //cerr << "depth " << depth << endl;
            
            // Find the new split point for each partition
            depthSplits.emplace_back(buckets.size());
            std::vector<PartitionSplit> & partitionSplits = depthSplits.back();
            
            for (int partition = 0;  partition < buckets.size();  ++partition) {

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
                                     const std::tuple<int, double, int, W, W>
                                         & val)
                    {
                        double score = std::get<1>(val);

                        //cerr << "af " << af << " f " << std::get<0>(val)
                        //     << " score " << std::get<1>(val) << " split "
                        //     << std::get<2>(val) << " nb "
                        //     << bucketOffsets[std::get<0>(val) + 1]
                        //        - bucketOffsets[std::get<0>(val)]
                        //     << endl;
                        
                        if (score < bestScore) {
                            std::tie(bestFeature, bestScore, bestSplit, bestLeft,
                                     bestRight) = val;
                        }
                    };
            
                // Finally, we re-split
                auto doFeature = [&] (int af)
                    {
                        int f = activeFeatures.at(af);
                        int startBucket = bucketOffsets[f];
                        int endBucket MLDB_UNUSED = bucketOffsets[f + 1];
                        W * wFeature = buckets[partition].data() + startBucket;
                        int maxBucket = endBucket - startBucket - 1;
                        bool isActive = true;
                        double bestScore = INFINITY;
                        int bestSplit = -1;
                        W bestLeft;
                        W bestRight;

                        if (isActive) {
                            std::tie(bestScore, bestSplit, bestLeft, bestRight)
                                = chooseSplitKernel(wFeature, maxBucket,
                                                    features[f].ordinal,
                                                    wAll[partition]);
                        }

                        //cerr << " score " << bestScore << " split "
                        //     << bestSplit << endl;
                        
                        return std::make_tuple(f, bestScore, bestSplit,
                                               bestLeft, bestRight);
                    };
            

                parallelMapInOrderReduce(0, activeFeatures.size(),
                                         doFeature, findBest);

                partitionSplits[partition] =
                    { bestScore, bestFeature, bestSplit, bestLeft, bestRight,
                      std::move(activeFeatures),
                      bestLeft.count() <= bestRight.count() };

#if 0                
                cerr << "partition " << partition << " of " << buckets.size()
                     << " with " << bestLeft.count() + bestRight.count()
                     << " rows: " << bestScore << " " << bestFeature;
                if (bestFeature != -1) {
                    cerr << " " << features[bestFeature].info->columnName
                         << " " << bestSplit
                         << " " << features[bestFeature].info->bucketDescriptions.getSplit(bestSplit);
                }
                cerr << " " << jsonEncodeStr(bestLeft) << " "
                     << jsonEncodeStr(bestRight)
                     << endl;
#endif
            }
            
            // Firstly, we double the number of partitions.  The left all
            // go with the lower partition numbers, the right have the higher
            // partition numbers.
            int rightOffset = buckets.size();
            
            buckets.resize(buckets.size() * 2,
                           std::vector<W>(numActiveBuckets));
            wAll.resize(wAll.size() * 2);

            // Those buckets which are transfering right to left should
            // start with the weight on the right
            for (size_t i = 0;  i < rightOffset;  ++i) {
                if (partitionSplits[i].direction) {
                    std::swap(buckets[i], buckets[i + rightOffset]);
                    std::swap(wAll[i], wAll[i + rightOffset]);
                }
            }

#if 1    
            updateBuckets(rows, features,
                          partitions, buckets, wAll,
                          bucketOffsets, partitionSplits, rightOffset);
#else
            updateBuckets2(rows, features,
                           partitions, buckets, wAll,
                           bucketOffsets, partitionSplits, rightOffset,
                           activeFeatures);
#endif

            // Create the nodes for the tree, and continue on to the next
            // iteration.

            // ...

            // Ready for the next level
        }

        // Recursively go through and extract our tree.  There is no
        // calculating going on here, just creation of the data structure.
        std::function<ML::Tree::Ptr (int depth, int partition)> getPtr
            = [&] (int relativeDepth, int partition)
            {
                auto & s = depthSplits.at(relativeDepth).at(partition);

                if (s.left.count() + s.right.count() == 0) 
                    return ML::Tree::Ptr();
                else if (s.feature == -1)
                    return getLeaf(tree, s.left + s.right);
                
                ML::Tree::Ptr left, right;

                if (relativeDepth == depthSplits.size() - 1) {
                    // asking for the last level
                    // For now, it's a leaf.  Later, we need to recurse
                    // by compacting our data and creating sub-trees
                    left = getLeaf(tree, s.left);
                    right = getLeaf(tree, s.right);
                }
                else {
                    // Not the last level
                    left = getPtr(relativeDepth + 1, partition);
                    right = getPtr(relativeDepth + 1,
                                   partition + (1 << relativeDepth));
                }

                if (!left && !right) {
                    return getLeaf(tree, s.left + s.right);
                }

                if (!left)
                    left = getLeaf(tree, s.left);
                if (!right)
                    right = getLeaf(tree, s.right);
                
                return getNode(tree, s.score, s.feature, s.value,
                               left, right, s.left, s.right);
#if 0
                else {
                    cerr << "depth " << relativeDepth << " partition "
                         << partition << " right "
                         << partition + (1 << relativeDepth)
                         << " feature "
                         << s.score << " " << s.feature << " " << s.value
                         << " " << jsonEncodeStr(s.left)
                         << " " << jsonEncodeStr(s.right) << endl;
                    throw Exception("Logic error: one of left and right");
                }
#endif
                
            };

        return getPtr(0, 0);

#if 0        
        std::vector<Tree::Ptr> leafPtrs;
        
        // Time to go to the next level
        if (depth < maxDepth) {
            throw Exception("TODO: deeper trees");
        }
        else {
            // extract bottom layer of node pointers
            leafPtrs.reserve(splits.back().size());
            for (auto & s: splits.back()) {
                Tree::Ptr left = getLeaf(tree, s.left);
                Tree::Ptr right = getLeaf(tree, s.right);
                Tree::Ptr node = getNode(tree, s.score, s.feature, s.split,
                                         left, right, s.left, s.right);
                leafPtrs.emplace_back(node);
            }
        }

        // Go back up the levels to create the output
        for (int i = splits.size() - 1;  i >= 0;  ++i) {
            std::vector<Tree::Ptr> newLeafPtrs(1 << i);
            ExcAssertEqual(newLeafPtrs.size() * 2, leafPtrs.size());
            for (int j = 0;  j < newLeafPtrs.size();  ++i) {
                int leftPart = j;
                int rightPart = j + newLeafPtrs.size();

                Tree::Ptr node
                    = getNode(tree,
                              splits[i].score, splits[i].feature,
                              splits[i].split,
                              leafPtrs[leftPart], leafPtrs[rightPart],
                              splits[i].left, splits[i].right);
                newLeafPtrs[j] = node;
            }
        }
#endif
    }
    
    ML::Tree::Ptr
    trainPartitioned(int depth, int maxDepth,
                     ML::Tree & tree,
                     MappedSerializer & serializer) const
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
        
        // Start by initializing the weights for each feature, if
        // this isn't passed in already
        auto initFeature = [&] (int f)
            {
                int startBucket = bucketOffsets[f];
                bool active;
                int maxBucket;
                std::tie(active, maxBucket)
                    = testFeatureKernel(rows.getRowIterator(),
                                        rows.rowCount(),
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

        return trainPartitionedRecursive(depth, maxDepth, tree, serializer,
                                         bucketOffsets, activeFeatures,
                                         std::move(buckets));
    }
    
    ML::Tree::Ptr train(int depth, int maxDepth,
                        ML::Tree & tree,
                        MappedSerializer & serializer) const
    {
        using namespace std;
        
        if (rows.rowCount() == 0)
            return ML::Tree::Ptr();
        if (rows.rowCount() < 2)
            return getLeaf(tree);

        if (depth >= maxDepth)
            return getLeaf(tree);

        ML::Tree::Ptr part;

#if 0
        return trainPartitioned(depth, maxDepth, tree, serializer);
#elif 0
        if (depth == 0) {
            Timer timer;
            Date before = Date::now();
            part = trainPartitioned(depth, maxDepth, tree, serializer);
            Date after = Date::now();
            cerr << "partitioned took " << after.secondsSince(before) * 1000.0
                 << "ms " << timer.elapsed() << endl;
        }

        if (depth < 2)
            cerr << "depth " << depth << endl;
#endif
        
        Date before;
        unique_ptr<Timer> timer;
        if (depth == 0) {
            before = Date::now();
            timer.reset(new Timer());
        }
        
        double bestScore;
        int bestFeature;
        int bestSplit;
        W wLeft;
        W wRight;
        std::vector<uint8_t> newActive;
        
        std::tie(bestScore, bestFeature, bestSplit, wLeft, wRight,
                 newActive)
            = testAll(depth, features, rows, bucketMemory);

        ExcAssertEqual(newActive.size(), features.size());
        
        // Record the active flag back
        //for (size_t i = 0;  i < features.size();  ++i) {
        //    features[i].active = newActive[i];
        //}
        
        if (bestFeature == -1) {
            ML::Tree::Leaf * leaf = tree.new_leaf();
            fillinBase(leaf, /*wLeft + wRight*/ rows.wAll);
            
            return leaf;
        }

        std::pair<PartitionData, PartitionData> splits
            = split(bestFeature, bestSplit, wLeft, wRight, serializer);

        
        //cerr << "done split in " << timer.elapsed() << endl;

        //cerr << "left had " << splits.first.rows.size() << " rows" << endl;
        //cerr << "right had " << splits.second.rows.size() << " rows" << endl;

        ML::Tree::Ptr left, right;
        auto runLeft = [&] () { left = splits.first.train(depth + 1, maxDepth, tree, serializer); splits.first.clear(); };
        auto runRight = [&] () { right = splits.second.train(depth + 1, maxDepth, tree, serializer); splits.second.clear(); };

        size_t leftRows = splits.first.rows.rowCount();
        size_t rightRows = splits.second.rows.rowCount();

        if (leftRows == 0 || rightRows == 0) {
            throw AnnotatedException(400,
                                     "Invalid split in random forest",
                                     "leftRows", leftRows,
                                     "rightRows", rightRows,
                                     "bestFeature", bestFeature,
                                     "name", features[bestFeature].info->columnName,
                                     "bestSplit", bestSplit,
                                     "wLeft0", (double)wLeft.v[0],
                                     "wLeft1", (double)wLeft.v[1],
                                     "wRight0", (double)wRight.v[0],
                                     "wRight1", (double)wRight.v[1]);
        }

        if (leftRows + rightRows < 1000) {
            runLeft();
            runRight();
        }
        else {
            ThreadPool tp;
            // Put the smallest one on the thread pool, so that we have the highest
            // probability of running both on our thread in case of lots of work.
            if (leftRows < rightRows) {
                tp.add(runLeft);
                runRight();
            }
            else {
                tp.add(runRight);
                runLeft();
            }

            tp.waitForAll();
        }

        ML::Tree::Ptr result;
        
        if (left && right) {
            result = getNode(tree, bestScore, bestFeature, bestSplit,
                             left, right, wLeft, wRight);
        }
        else {
            result = getLeaf(tree, wLeft + wRight);
        }

        if (depth == 0 && part) {
            Date after = Date::now();
            cerr << "recursive took " << after.secondsSince(before) * 1000.0
                 << "ms " << timer->elapsed() << endl;

            std::function<bool (ML::Tree::Ptr, ML::Tree::Ptr, int)> compareTrees
                = [&] (ML::Tree::Ptr left, ML::Tree::Ptr right, int depth)
                {
                    auto printSummary = [&] (ML::Tree::Ptr ptr) -> std::string
                    {
                        std::string result;
                        if (!ptr)
                            return "null";

                        result = "ex=" + std::to_string(ptr.examples())
                        + " pred=" + std::to_string(ptr.pred()[0])
                        + " " + std::to_string(ptr.pred()[1]);

                        if (ptr.isNode()) {
                            auto & n = *ptr.node();
                            result += " " + n.split.print(*fs);
                        }

                        return result;
                    };

                    auto doPrint = [&] () -> bool
                    {
                        cerr << "difference at depth " << depth << endl;
                        cerr << "  left:  " << printSummary(left) << endl;
                        cerr << "  right: " << printSummary(right) << endl;
                        return false;
                    };

                    if ((left.pred() != right.pred()).any()) {
                        cerr << "different predictions at depth "
                             << depth << ": " << left.pred()
                             << " vs " << right.pred() << endl;
                        return doPrint();
                    }
                    if (left.examples() != right.examples()) {
                        cerr << "different examples at depth " << depth << ": "
                             << left.examples() << " vs " << right.examples()
                             << endl;
                        return doPrint();
                    }
                    if (left.isNode() && right.isNode()) {
                        const ML::Tree::Node & l = *left.node();
                        const ML::Tree::Node & r = *right.node();

                        if (l.split != r.split) {
                            cerr << "different split: "
                                 << l.split.print(*fs) << " vs "
                                 << r.split.print(*fs) << endl;
                            return doPrint();
                        }
                        if (l.z != r.z) {
                            cerr << "different z: "
                                 << l.z << " vs " << r.z << endl;
                            return doPrint();
                        }

                        if (!compareTrees(l.child_true, r.child_true, depth + 1)) {
                            cerr << "different left" << endl;
                            return doPrint();
                        }
                        if (!compareTrees(l.child_false, r.child_false, depth + 1)) {
                            cerr << "diffrent right" << endl;
                            return doPrint();
                        }
                        return true;
                    }
                    else if (left.isLeaf() && right.isLeaf()) {
                        // already compared
                        return true;
                    }
                    else {
                        cerr << "different type at depth " << depth << endl;
                        return doPrint();
                    }
                };

            ExcAssert(compareTrees(result, part, 0));
            
        }

        
        return result;
    }
};

} // namespace RF
} // namespace MLDB
