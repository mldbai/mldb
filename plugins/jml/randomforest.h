/** randomforest.h                                             -*- C++ -*-
    Mathieu Marquis Bolduc, 11 Mars 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Optimized random forest algorithm for dense data and binary classification

*/

#pragma once

#include "mldb/plugins/jml/dataset_feature_space.h"
#include "mldb/utils/fixed_point_accum.h"
#include "mldb/plugins/jml/jml/tree.h"
#include "mldb/plugins/jml/jml/stump_training_bin.h"
#include "mldb/plugins/jml/jml/decision_tree.h"
#include "mldb/plugins/jml/jml/committee.h"
#include "mldb/base/parallel.h"
#include "mldb/base/thread_pool.h"
#include "mldb/engine/column_scope.h"
#include "mldb/core/bucket.h"

namespace MLDB {

/** Holds the set of data for a partition of a decision tree. */
struct PartitionData {

    PartitionData() = default;

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

    /** Create a new dataset with the same labels, different weights
        (by element-wise multiplication), and with
        zero weights filtered out such that example numbers are strictly
        increasing.
    */
    PartitionData reweightAndCompact(const std::vector<float> & weights,
                                     size_t numNonZero,
                                     MappedSerializer & serializer) const
    {
        PartitionData data;
        data.features = this->features;
        data.fs = this->fs;
        data.reserve(numNonZero);

        std::vector<WritableBucketList>
            featureBuckets(features.size());

        // We split the rows up into tranches to increase parallism
        // To do so, we need to know how many items are in each
        // tranche and where its items start
        size_t numTranches = std::min<size_t>(8, weights.size() / 1024);
        if (numTranches == 0)
            numTranches = 1;
        //numTranches = 1;
        size_t numPerTranche = weights.size() / numTranches;

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
            for (; end < weights.size()
                     && (end < start + numPerTranche
                         || n == 0
                         || n % 64 != 0);  ++end) {
                n += weights[end] != 0;
            }
            trancheCounts.push_back(n);
            trancheOffsets.push_back(offset);
            offset += n;
            start = end;
            ExcAssertLessEqual(start, weights.size());
        }
        ExcAssertEqual(offset, numNonZero);
        trancheOffsets.push_back(offset);
        trancheSplits.push_back(start);

        // Get a contiguous block of memory for all of the feature blocks;
        // this enables a single GPU transfer and a single GPU argument
        // list (for when we do GPUs)
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
                    for (size_t i = 0;  i < rows.size();  ++i) {
                        if (weights[i] == 0)
                            continue;
                        data.addRow(rows[i].label, rows[i].weight * weights[i],
                                    n++);
                    }
                    ExcAssertEqual(n, numNonZero);
                    return;
                }

                if (!data.features[f].active)
                    return;

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
                        if (weights[i] == 0)
                            continue;
                        
                        uint32_t bucket = features[f].buckets[rows[i].exampleNum];
                        //ExcAssertLess(bucket, features[f].info->distinctValues);
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

    std::shared_ptr<const DatasetFeatureSpace> fs;

    /// Entry for an individual row
    struct Row {
        bool label;                 ///< Label associated with
        float weight;               ///< Weight of the example 
        int exampleNum;             ///< index into feature array
    };
    
    // Entry for an individual feature
    struct Feature {
        Feature()
            : active(false), ordinal(true),
              info(nullptr)
        {
        }

        bool active;  ///< If true, the feature can be split on
        bool ordinal; ///< If true, it's continuous valued; otherwise categ.
        const DatasetFeatureSpace::ColumnInfo * info;
        BucketList buckets;  ///< List of bucket numbers, per example
    };

    // All rows of data in this partition
    std::vector<Row> rows;

    // All features that are active
    std::vector<Feature> features;

    /// Memory for all feature buckets
    FrozenMemoryRegionT<uint32_t> bucketMemory;

    /** Reserve enough space for the given number of rows. */
    void reserve(size_t n)
    {
        rows.reserve(n);
    }

    /** Add the given row. */
    void addRow(const Row & row)
    {
        rows.push_back(row);
    }

    void addRow(bool label, float weight, int exampleNum)
    {
        rows.emplace_back(Row{label, weight, exampleNum});
    }

    //This structure hold the weights (false and true) for any particular split
    template<typename Float>
    struct WT {
        WT()
            : v { 0, 0 }
        {
        }

        Float v[2];

        Float & operator [] (bool i)
        {
            return v[i];
        }

        const Float & operator [] (bool i) const
        {
            return v[i];
        }

        bool empty() const { return total() == 0; }

        Float total() const { return v[0] + v[1]; }

        constexpr size_t nl() const { return 2; }

        WT & operator += (const WT & other)
        {
            v[0] += other.v[0];
            v[1] += other.v[1];
            return *this;
        }

        WT operator + (const WT & other) const
        {
            WT result = *this;
            result += other;
            return result;
        }

        WT & operator -= (const WT & other)
        {
            v[0] -= other.v[0];
            v[1] -= other.v[1];
            return *this;
        }

        typedef Float FloatType;
    };

    typedef WT<MLDB::FixedPointAccum64> W;

    /** Split the partition here. */
    std::pair<PartitionData, PartitionData>
    split(int featureToSplitOn, int splitValue, const W & wLeft, const W & wRight, const W & wAll,
          MappedSerializer & serializer)
    {
     //   std::cerr << "spliting on feature " << featureToSplitOn << " bucket " << splitValue << std::endl;

        ExcAssertGreaterEqual(featureToSplitOn, 0);
        ExcAssertLess(featureToSplitOn, features.size());

        PartitionData sides[2];
        PartitionData & left = sides[0];
        PartitionData & right = sides[1];

        ExcAssert(fs);
        ExcAssert(!features.empty());

        left.fs = fs;
        right.fs = fs;
        left.features = features;
        right.features = features;

        ExcAssertGreaterEqual(featureToSplitOn, 0);
        ExcAssertLess(featureToSplitOn, features.size());

        bool ordinal = features.at(featureToSplitOn).ordinal;

        double useRatio = 1.0 * rows.size() / rows.back().exampleNum;

        //todo: Re-index when usable data fits inside cache
        bool reIndex = useRatio < 0.1;
        //reIndex = false;
        //cerr << "useRatio = " << useRatio << endl;

        if (!reIndex) {

            sides[0].rows.reserve(rows.size());
            sides[1].rows.reserve(rows.size());

            for (size_t i = 0;  i < rows.size();  ++i) {
                int bucket = features[featureToSplitOn].buckets[rows[i].exampleNum];
                int side = ordinal ? bucket > splitValue : bucket != splitValue;
                sides[side].addRow(rows[i]);
            }

            rows.clear();
            rows.shrink_to_fit();
        }
        else {

            int nf = features.size();

            // For each example, it goes either in left or right, depending
            // upon the value of the chosen feature.

            std::vector<uint8_t> lr(rows.size());
            bool ordinal = features[featureToSplitOn].ordinal;
            size_t numOnSide[2] = { 0, 0 };

            // TODO: could reserve less than this...
            sides[0].rows.reserve(rows.size());
            sides[1].rows.reserve(rows.size());

            for (size_t i = 0;  i < rows.size();  ++i) {
                int bucket = features[featureToSplitOn].buckets[rows[i].exampleNum];
                int side = ordinal ? bucket > splitValue : bucket != splitValue;
                lr[i] = side;
                sides[side].addRow(rows[i].label, rows[i].weight, numOnSide[side]++);
            }

            for (unsigned i = 0;  i < nf;  ++i) {
                if (!features[i].active)
                    continue;

                WritableBucketList newFeatures[2];
                newFeatures[0].init(numOnSide[0], features[i].info->distinctValues, serializer);
                newFeatures[1].init(numOnSide[1], features[i].info->distinctValues, serializer);
                size_t index[2] = { 0, 0 };

                for (size_t j = 0;  j < rows.size();  ++j) {
                    int side = lr[j];
                    newFeatures[side].write(features[i].buckets[rows[j].exampleNum]);
                    ++index[side];
                }

                sides[0].features[i].buckets = newFeatures[0].freeze(serializer);
                sides[1].features[i].buckets = newFeatures[1].freeze(serializer);
            }

            rows.clear();
            rows.shrink_to_fit();
        }

        return { std::move(left), std::move(right) };
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
    std::tuple<double, int, int, W, W, W>
    testAll(int depth)
    {
        bool debug = false;

        int nf = features.size();

        // For each feature, for each bucket, for each label
        // weight for each bucket and last valid bucket
        std::vector< std::vector<W> > w(nf);
        std::vector< int > maxSplits(nf);

        size_t totalNumBuckets = 0;
        size_t activeFeatures = 0;

        for (unsigned i = 0;  i < nf;  ++i) {
            if (!features[i].active)
                continue;
            ++activeFeatures;
            w[i].resize(features[i].buckets.numBuckets);
            totalNumBuckets += features[i].buckets.numBuckets;
        }

        if (debug) {
            std::cerr << "total of " << totalNumBuckets << " buckets" << std::endl;
            std::cerr << activeFeatures << " of " << nf << " features active"
                 << std::endl;
        }

        W wAll;

        auto doFeature = [&] (int i)
            {
                int maxBucket = -1;

                if (i == nf) {
                    for (auto & r: rows) {
                        wAll[r.label] += r.weight;
                    }
                    return;
                }

                if (!features[i].active)
                    return;
                bool twoBuckets = false;
                int lastBucket = -1;

                for (size_t j = 0;  j < rows.size();  ++j) {
                    auto & r = rows[j];
                    int bucket = features[i].buckets[r.exampleNum];

                    twoBuckets = twoBuckets
                        || (lastBucket != -1 && bucket != lastBucket);
                    lastBucket = bucket;

                    //ExcAssertLess(i, w.size());
                    //ExcAssertLess(bucket, w[i].size());
                    //ExcAssertLess(r.label, w[i][bucket].nl());
                    w[i][bucket][r.label] += r.weight;
                    maxBucket = std::max(maxBucket, bucket);
                }

                // If all examples were in a single bucket, then the
                // feature is no longer active.
                if (!twoBuckets)
                    features[i].active = false;

                maxSplits[i] = maxBucket;
            };

        if (depth < 4 || true) {
            parallelMap(0, nf + 1, doFeature);
        }
        else {
            for (unsigned i = 0;  i <= nf;  ++i)
                doFeature(i);
        }

        // We have no impurity in our bucket.  Time to stop
        if (wAll[0] == 0 || wAll[1] == 0)
            return std::make_tuple(1.0, -1, -1, wAll, W(), wAll);

        double bestScore = INFINITY;
        int bestFeature = -1;
        int bestSplit = -1;
        
        W bestLeft;
        W bestRight;

        int bucketsEmpty = 0;
        int bucketsOne = 0;
        int bucketsBoth = 0;

        // Score each feature
        for (unsigned i = 0;  i < nf;  ++i) {
            if (!features[i].active)
                continue;

            W wAll; // TODO: do we need this?
            for (auto & wt: w[i]) {
                wAll += wt;
                bucketsEmpty += wt[0] == 0 && wt[1] == 0;
                bucketsBoth += wt[0] != 0 && wt[1] != 0;
                bucketsOne += (wt[0] == 0) ^ (wt[1] == 0);
            }

            auto score = [] (const W & wFalse, const W & wTrue) -> double
                {
                    double score
                    = 2.0 * (  sqrt(wFalse[0] * wFalse[1])
                             + sqrt(wTrue[0] * wTrue[1]));
                    return score;
                };
            
            if (debug) {
                std::cerr << "feature " << i << " " << features[i].info->columnName
                     << std::endl;
                std::cerr << "    all: " << wAll[0] << " " << wAll[1] << std::endl;
            }

            int maxBucket = maxSplits[i];

            if (features[i].ordinal) {
                // Calculate best split point for ordered values
                W wFalse = wAll, wTrue;

                // Now test split points one by one
                for (unsigned j = 0;  j < maxBucket;  ++j) {
                    if (w[i][j].empty())
                        continue;                   

                    double s = score(wFalse, wTrue);

                    if (debug) {
                        std::cerr << "  ord split " << j << " "
                             << features[i].info->bucketDescriptions.getValue(j)
                             << " had score " << s << std::endl;
                        std::cerr << "    false: " << wFalse[0] << " " << wFalse[1] << std::endl;
                        std::cerr << "    true:  " << wTrue[0] << " " << wTrue[1] << std::endl;
                    }

                    if (s < bestScore) {
                        bestScore = s;
                        bestFeature = i;
                        bestSplit = j;
                        bestRight = wFalse;
                        bestLeft = wTrue;
                    }

                   wFalse -= w[i][j];
                   wTrue += w[i][j];

                }
            }
            else {
                // Calculate best split point for non-ordered values
                // Now test split points one by one

                for (unsigned j = 0;  j <= maxBucket;  ++j) {

                    if (w[i][j].empty())
                        continue;

                    W wFalse = wAll;
                    wFalse -= w[i][j];                    

                    double s = score(wFalse, w[i][j]);

                    if (debug) {
                        std::cerr << "  non ord split " << j << " "
                             << features[i].info->bucketDescriptions.getValue(j)
                             << " had score " << s << std::endl;
                        std::cerr << "    false: " << wFalse[0] << " " << wFalse[1] << std::endl;
                        std::cerr << "    true:  " << w[i][j][0] << " " << w[i][j][1] << std::endl;
                    }
             
                    if (s < bestScore) {
                        bestScore = s;
                        bestFeature = i;
                        bestSplit = j;
                        bestRight = wFalse;
                        bestLeft = w[i][j];
                    }
                }

            }
        }
        
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

        return std::make_tuple(bestScore, bestFeature, bestSplit, bestLeft, bestRight, wAll);
    }

    static void fillinBase(MLDB::Tree::Base * node, const W & wAll)
    {

        float total = float(wAll[0]) + float(wAll[1]);
        node->examples = total;
        node->pred = {
             float(wAll[0]) / total,
             float(wAll[1]) / total };
    }

    MLDB::Tree::Ptr getLeaf(MLDB::Tree & tree, const W& w)
    {     
        MLDB::Tree::Leaf * node = tree.new_leaf();
        fillinBase(node, w);
        return node;
    }

    MLDB::Tree::Ptr getLeaf(MLDB::Tree & tree)
    {
        W wAll;
        for (auto & r: rows) {
            int label = r.label;
            ExcAssert(label >= 0 && label < 2);
            ExcAssert(r.weight > 0);
            wAll[r.label] += r.weight;
        }
        
       return getLeaf(tree, wAll);
    }  

    /** Trains the tree.  Note that this is destructive; it can only be called once as it
     *  frees its internal memory as it's going to ensure that memory usage is reasonable.
     */
    MLDB::Tree::Ptr train(int depth, int maxDepth,
                        MLDB::Tree & tree,
                        MappedSerializer & serializer)
    {
        //std::cerr << format("depth=%d maxDepth=%d this=%p features.size()=%zd\n",
        //                    depth, maxDepth, this, features.size());

        if (rows.empty())
            return MLDB::Tree::Ptr();
        if (rows.size() < 2)
            return getLeaf(tree);

        if (depth >= maxDepth)
            return getLeaf(tree);

        double bestScore;
        int bestFeature;
        int bestSplit;
        W wLeft;
        W wRight;
        W wAll;
        
        std::tie(bestScore, bestFeature, bestSplit, wLeft, wRight, wAll)
            = testAll(depth);

        if (bestFeature == -1) {
            MLDB::Tree::Leaf * leaf = tree.new_leaf();
            fillinBase(leaf, /*wLeft + wRight*/ wAll);
            
            return leaf;
        }

        ExcAssertGreaterEqual(bestFeature, 0);
        ExcAssertLessEqual(bestFeature, features.size());

        std::pair<PartitionData, PartitionData> splits
            = split(bestFeature, bestSplit, wLeft, wRight, wAll,
                    serializer);

        ExcAssertGreaterEqual(bestFeature, 0);
        ExcAssertLessEqual(bestFeature, features.size());

        //cerr << "done split in " << timer.elapsed_wall() << endl;

        //cerr << "left had " << splits.first.rows.size() << " rows" << endl;
        //cerr << "right had " << splits.second.rows.size() << " rows" << endl;

        MLDB::Tree::Ptr left, right;
        auto runLeft = [&] () { left = splits.first.train(depth + 1, maxDepth, tree, serializer); };
        auto runRight = [&] () { right = splits.second.train(depth + 1, maxDepth, tree, serializer); };

        size_t leftRows = splits.first.rows.size();
        size_t rightRows = splits.second.rows.size();

        if (leftRows == 0 || rightRows == 0)
            throw MLDB::Exception("Invalid split in random forest");

        ThreadPool tp;
        try {
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
        }
        catch (...) {
            tp.waitForAll();
            throw;
        }        
        tp.waitForAll();

        ExcAssertGreaterEqual(bestFeature, 0);
        ExcAssertLessEqual(bestFeature, features.size());

        if (left && right) {
            MLDB::Tree::Node * node = tree.new_node();
            MLDB::Feature feature = fs->getFeature(features.at(bestFeature).info->columnName);
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

            MLDB::Split split(feature, splitVal,
                            features.at(bestFeature).ordinal
                            ? MLDB::Split::LESS : MLDB::Split::EQUAL);
            
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
            MLDB::Tree::Leaf * leaf = tree.new_leaf();
            fillinBase(leaf, wLeft + wRight);

            return leaf;
        }
    }
};

} // namespace MLDB
