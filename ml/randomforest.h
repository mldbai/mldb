/** randomforest.h                                             -*- C++ -*-
    Mathieu Marquis Bolduc, 11 Mars 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Optimized random forest algorithm for dense data and binary classification

*/

#pragma once

#include "mldb/plugins/dataset_feature_space.h"
#include "mldb/ml/jml/fixed_point_accum.h"
#include "mldb/ml/jml/tree.h"
#include "mldb/ml/jml/stump_training_bin.h"
#include "mldb/ml/jml/decision_tree.h"
#include "mldb/ml/jml/committee.h"
#include "mldb/base/parallel.h"
#include "mldb/base/thread_pool.h"
#include "mldb/server/column_scope.h"
#include "mldb/server/bucket.h"

namespace MLDB {

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

    /** Create a new dataset with the same labels, different weights
        (by element-wise multiplication), and with
        zero weights filtered out such that example numbers are strictly
        increasing.
    */
    PartitionData reweightAndCompact(const std::vector<float> & weights,
                                     size_t numNonZero) const
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

                featureBuckets[f].init(numNonZero,
                                       data.features[f].info->distinctValues);

                data.features[f].buckets = std::move(featureBuckets[f]);

                auto onTranche = [&] (size_t tr)
                {
                    size_t start = trancheSplits[tr];
                    size_t end = trancheSplits[tr + 1];
                    size_t offset = trancheOffsets[tr];
                    
                    auto writer = featureBuckets[f].atOffset(offset);

                    size_t n = 0;
                    for (size_t i = start;  i < end;  ++i) {
                        if (weights[i] == 0)
                            continue;
                        
                        uint32_t bucket = features[f].buckets[rows[i].exampleNum];
                        writer.write(bucket);
                        ++n;
                    }
                    
                    ExcAssertEqual(n, trancheCounts[tr]);
                };

                parallelMap(0, numTranches, onTranche);
            };

        MLDB::parallelMap(0, data.features.size() + 1, doFeature);

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

    typedef WT<ML::FixedPointAccum64> W;

    /** Split the partition here. */
    std::pair<PartitionData, PartitionData>
    split(int featureToSplitOn, int splitValue, const W & wLeft, const W & wRight, const W & wAll)
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

        bool ordinal = features[featureToSplitOn].ordinal;

        double useRatio = 1.0 * rows.size() / rows.back().exampleNum;

        //todo: Re-index when usable data fits inside cache
        bool reIndex = useRatio < 0.1;
        //reIndex = false;
        //cerr << "useRatio = " << useRatio << endl;

        if (!reIndex) {

            sides[0].rows.reserve(rows.size());
            sides[1].rows.reserve(rows.size());

            //this is for debug only
            //int maxBucket = 0;
            //int minBucket = INFINITY;

            for (size_t i = 0;  i < rows.size();  ++i) {
                int bucket = features[featureToSplitOn].buckets[rows[i].exampleNum];
                //maxBucket = std::max(maxBucket, bucket);
                //minBucket = std::min(minBucket, bucket);
                int side = ordinal ? bucket > splitValue : bucket != splitValue;
                sides[side].addRow(rows[i]);
            }

            rows.clear();
            rows.shrink_to_fit();
            features.clear();

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
                newFeatures[0].init(numOnSide[0], features[i].info->distinctValues);
                newFeatures[1].init(numOnSide[1], features[i].info->distinctValues);
                size_t index[2] = { 0, 0 };

                for (size_t j = 0;  j < rows.size();  ++j) {
                    int side = lr[j];
                    newFeatures[side].write(features[i].buckets[rows[j].exampleNum]);
                    ++index[side];
                }

                sides[0].features[i].buckets = newFeatures[0];
                sides[1].features[i].buckets = newFeatures[1];
            }

            rows.clear();
            rows.shrink_to_fit();
            features.clear();
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
        W wAll;
        for (auto & r: rows) {
            int label = r.label;
            ExcAssert(label >= 0 && label < 2);
            ExcAssert(r.weight > 0);
            wAll[r.label] += r.weight;
        }
        
       return getLeaf(tree, wAll);
    }  

    ML::Tree::Ptr train(int depth, int maxDepth,
                        ML::Tree & tree)
    {
        if (rows.empty())
            return ML::Tree::Ptr();
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
            ML::Tree::Leaf * leaf = tree.new_leaf();
            fillinBase(leaf, /*wLeft + wRight*/ wAll);
            
            return leaf;
        }

        std::pair<PartitionData, PartitionData> splits
            = split(bestFeature, bestSplit, wLeft, wRight, wAll);

        //cerr << "done split in " << timer.elapsed() << endl;

        //cerr << "left had " << splits.first.rows.size() << " rows" << endl;
        //cerr << "right had " << splits.second.rows.size() << " rows" << endl;

        ML::Tree::Ptr left, right;
        auto runLeft = [&] () { left = splits.first.train(depth + 1, maxDepth, tree); };
        auto runRight = [&] () { right = splits.second.train(depth + 1, maxDepth, tree); };

        size_t leftRows = splits.first.rows.size();
        size_t rightRows = splits.second.rows.size();

        if (leftRows == 0 || rightRows == 0)
            throw MLDB::Exception("Invalid split in random forest");

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

} // namespace MLDB
