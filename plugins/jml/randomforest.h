/** randomforest.h                                             -*- C++ -*-
    Mathieu Marquis Bolduc, 11 Mars 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Optimized random forest algorithm for dense data and binary classification

*/

#pragma once

#include "randomforest_types.h"
#include "randomforest_kernels.h"
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
                    ExcAssert(!isnan(weight));
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

        std::vector<WritableBucketList>
            featureBuckets(features.size());

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

        // This gets called for each feature.  It's further subdivided
        // per tranche.
        auto doFeature = [&] (size_t f)
            {
                if (f == data.features.size()) {
                    // Do the row index
                    size_t n = 0;

                    RowWriter writer
                        = data.rows.getRowWriter(numNonZero, numNonZero,
                                                 serializer);
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

                featureBuckets[f].init(numNonZero,
                                       data.features[f].info->distinctValues);

                auto onTranche = [&] (size_t tr)
                {
                    size_t start = trancheSplits[tr];
                    size_t end = trancheSplits[tr + 1];
                    size_t offset = trancheOffsets[tr];
                    
                    auto writer = featureBuckets[f].atOffset(offset);

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

                data.features[f].buckets = std::move(featureBuckets[f]);
            };

        MLDB::parallelMap(0, data.features.size() + 1, doFeature);

        return data;
    }

    /// Feature space
    std::shared_ptr<const DatasetFeatureSpace> fs;

    /// Rows in this partition
    Rows rows;
    
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
                                      serializer),
                    rows.getRowWriter(rows.rowCount(),
                                      rows.highestExampleNum(),
                                      serializer) };

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

            struct LREx {
                uint32_t exampleNum:31;
                uint32_t side:1;
            };
            
            std::vector<LREx> lr(rows.rowCount());
            bool ordinal = features[featureToSplitOn].ordinal;
            size_t numOnSide[2] = { 0, 0 };

            // TODO: could reserve less than this...
            RowWriter writer[2]
                = { rows.getRowWriter(rows.rowCount(),
                                      rows.rowCount(),
                                      serializer),
                    rows.getRowWriter(rows.rowCount(),
                                      rows.rowCount(),
                                      serializer) };

            Rows::RowIterator rowIterator = rows.getRowIterator();

            for (size_t i = 0;  i < rows.rowCount();  ++i) {
                Row row = rowIterator.getRow();
                int bucket = features[featureToSplitOn].buckets[row.exampleNum()];
                int side = ordinal ? bucket > splitValue : bucket != splitValue;
                lr[i].side = side;
                lr[i].exampleNum = row.exampleNum();
                row.exampleNum_ = numOnSide[side]++;
                writer[side].addRow(row);
            }

            for (unsigned i = 0;  i < nf;  ++i) {
                if (!features[i].active)
                    continue;

                WritableBucketList newFeatures[2];
                newFeatures[0].init(numOnSide[0],
                                    features[i].info->distinctValues);
                newFeatures[1].init(numOnSide[1],
                                    features[i].info->distinctValues);

                for (size_t j = 0;  j < rows.rowCount();  ++j) {
                    int side = lr[j].side;
                    newFeatures[side].write(features[i].buckets[lr[j].exampleNum]);
                }

                sides[0].features[i].buckets = std::move(newFeatures[0]);
                sides[1].features[i].buckets = std::move(newFeatures[1]);
            }

            rows.clear();
            features.clear();

            sides[0].rows = writer[0].freeze(serializer);
            sides[1].rows = writer[1].freeze(serializer);
        }

        return { std::move(left), std::move(right) };
    }

    template<typename GetNextRowFn>
    static
    std::tuple<double /* bestScore */,
               int /* bestSplit */,
               W /* bestLeft */,
               W /* bestRight */,
               bool /* feature is still active */ >
    testFeatureNumber(int featureNum,
                      const std::vector<Feature> & features,
                      GetNextRowFn&& getNextRow,
                      size_t numRows,
                      const W & wAll)
    {
        const Feature & feature = features.at(featureNum);
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
            = testFeatureKernel(getNextRow, numRows,
                                buckets, w.data());

        if (isActive) {
            std::tie(bestScore, bestSplit, bestLeft, bestRight)
                = chooseSplitKernel(w.data(), maxBucket, feature.ordinal,
                                    wAll);
        }

        return { bestScore, bestSplit, bestLeft, bestRight, isActive };
    }
        
    /** Test all features for a split.  Returns the feature number,
        the bucket number and the goodness of the split.

        Outputs
        - Z score of split
        - Feature number
        - Split point
        - W for the left side of the split
        - W from the right side of the split
        - W total (in case no split is found)
    */
    std::tuple<double, int, int, W, W>
    testAll(int depth)
    {
        // We have no impurity in our bucket.  Time to stop
        if (rows.wAll[0] == 0 || rows.wAll[1] == 0) {
            return std::make_tuple(1.0, -1, -1, rows.wAll, W());
        }

        bool debug = false;

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
                if (score < bestScore) {
                    bestFeature = feature;
                    std::tie(bestScore, bestSplit, bestLeft, bestRight) = val;
                }
            };

        // Parallel map over all features
        auto doFeature = [&] (int i)
            {
                double score;
                int split = -1;
                W bestLeft, bestRight;

                Rows::RowIterator rowIterator = rows.getRowIterator();
                
                auto getNextRow = [&] () -> DecodedRow
                {
                    return rowIterator.getDecodedRow();
                };
                
                std::tie(score, split, bestLeft, bestRight, features[i].active)
                    = testFeatureNumber(i, features, getNextRow, rows.rowCount(), rows.wAll);
                return std::make_tuple(score, split, bestLeft, bestRight);
            };

        if (depth < 4 || rows.rowCount() * nf > 20000) {
            parallelMapInOrderReduce(0, nf, doFeature, findBest);
        }
        else {
            for (unsigned i = 0;  i < nf;  ++i)
                doFeature(i);
        }

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
                 << features[bestFeature].info->bucketDescriptions.getValue(bestSplit)
                 << std::endl;
        }

        return std::make_tuple(bestScore, bestFeature, bestSplit, bestLeft, bestRight);
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
        
        std::tie(bestScore, bestFeature, bestSplit, wLeft, wRight)
            = testAll(depth);

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

        if (leftRows == 0 || rightRows == 0)
            throw MLDB::Exception("Invalid split in random forest");

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
