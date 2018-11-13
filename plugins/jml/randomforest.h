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

        auto doWeightChunk = [&] (size_t start)
            -> std::tuple<LightweightHashSet<float> /* uniques */,
                          float /* minWeight */,
                          size_t /* numValues */>
            {
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

        MLDB::parallelMap(0, data.features.size() + 1, doFeature);

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

    /** Split the partition here. */
    std::pair<PartitionData, PartitionData>
    split(int featureToSplitOn, int splitValue,
          const W & wLeft, const W & wRight,
          MappedSerializer & serializer)
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
        
        bool ordinal = features[featureToSplitOn].ordinal;

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
            //this is for debug only
            //int maxBucket = 0;
            //int minBucket = INFINITY;

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
                //maxBucket = std::max(maxBucket, bucket);
                //minBucket = std::min(minBucket, bucket);
                int side = ordinal ? bucket > splitValue : bucket != splitValue;
                writer[side].addRow(row);
            }

            rows.clear();
            features.clear();

            sides[0].rows = writer[0].freeze(serializer);
            sides[1].rows = writer[1].freeze(serializer);

            sides[0].bucketMemory = sides[1].bucketMemory = bucketMemory;
            
            /*if (right.rows.size() == 0 || left.rows.size() == 0)
            {
                std::cerr << wLeft[0] << "," << wLeft[1] << "," << wRight[0] << "," << wRight[1] << std::endl;
                std::cerr << wAll[0] << "," << wAll[1] << std::endl;
                std::cerr << "splitValue: " << splitValue << std::endl;
                std::cerr << "isordinal: " << ordinal << std::endl;
                std::cerr << "max bucket" << maxBucket << std::endl;
                std::cerr << "min bucket" << minBucket << std::endl;
            }

            ExcAssert(left.rows.size() > 0);
            ExcAssert(right.rows.size() > 0);*/
        }
        else {

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
                int side = ordinal ? bucket > splitValue : bucket != splitValue;
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

            rows.clear();
            features.clear();

            sides[0].rows = writer[0].freeze(serializer);
            sides[1].rows = writer[1].freeze(serializer);

            sides[0].bucketMemory = mutableBucketMemory[0].freeze();
            sides[1].bucketMemory = mutableBucketMemory[1].freeze();

#if 0            
            using namespace std;
            for (int side = 0;  side < 2;  ++side) {
                for (size_t i = 0;  i < nf;  ++i) {
                    if (!features[i].active)
                        continue;
                    ostringstream str;
                    str  << "side " << side << " feature " << i << " offset "
                         << sides[side].features[i].buckets.storage.data()
                          - sides[side].bucketMemory.data()
                         << " should be "
                         << bucketMemoryOffsets[side][i] / 4 << endl;
                    cerr << str.str();

                }
            }
#endif
        }

        return { std::move(left), std::move(right) };
    }

    static void fillinBase(ML::Tree::Base * node, const W & wAll)
    {

        float total = float(wAll[0]) + float(wAll[1]);
        node->examples = total;
        node->pred = {
             float(wAll[0]) / total,
             float(wAll[1]) / total };
    }

    ML::Tree::Ptr getLeaf(ML::Tree & tree, const W& w)
    {     
        ML::Tree::Leaf * node = tree.new_leaf();
        fillinBase(node, w);
        return node;
    }

    ML::Tree::Ptr getLeaf(ML::Tree & tree)
    {
       return getLeaf(tree, rows.wAll);
    }  

    ML::Tree::Ptr train(int depth, int maxDepth,
                        ML::Tree & tree,
                        MappedSerializer & serializer)
    {
        if (rows.rowCount() == 0)
            return ML::Tree::Ptr();
        if (rows.rowCount() < 2)
            return getLeaf(tree);

        if (depth >= maxDepth)
            return getLeaf(tree);

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
        for (size_t i = 0;  i < features.size();  ++i) {
            features[i].active = newActive[i];
        }
        
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
        auto runLeft = [&] () { left = splits.first.train(depth + 1, maxDepth, tree, serializer); };
        auto runRight = [&] () { right = splits.second.train(depth + 1, maxDepth, tree, serializer); };

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
        
        if (left && right) {
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
            wMissing[0] = 0.0f;
            wMissing[1] = 0.0f;
            node->child_missing = getLeaf(tree, wMissing);
            node->z = bestScore;            
            fillinBase(node, wLeft + wRight);

            return node;
        }
        else {
            ML::Tree::Leaf * leaf = tree.new_leaf();
            fillinBase(leaf, wLeft + wRight);

            return leaf;
        }
    }
};

} // namespace RF
} // namespace MLDB
