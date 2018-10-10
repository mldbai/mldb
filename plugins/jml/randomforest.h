/** randomforest.h                                             -*- C++ -*-
    Mathieu Marquis Bolduc, 11 Mars 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Optimized random forest algorithm for dense data and binary classification

*/

#pragma once

#include "mldb/plugins/jml/dataset_feature_space.h"
#include "mldb/plugins/jml/jml/fixed_point_accum.h"
#include "mldb/plugins/jml/jml/tree.h"
#include "mldb/plugins/jml/jml/stump_training_bin.h"
#include "mldb/plugins/jml/jml/decision_tree.h"
#include "mldb/plugins/jml/jml/committee.h"
#include "mldb/base/parallel.h"
#include "mldb/base/map_reduce.h"
#include "mldb/base/thread_pool.h"
#include "mldb/engine/column_scope.h"
#include "mldb/engine/bucket.h"

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
                        data.addRow(rows[i].label(),
                                    rows[i].weight * weights[i],
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
                        
                        uint32_t bucket = features[f].buckets[rows[i].exampleNum()];
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
        Row(bool label, float weight, unsigned exampleNum)
            : weight(weight), exampleNum_(label | (exampleNum << 1))
        {
        }
        
        float weight;               ///< Weight of the example 
        unsigned exampleNum_;       ///< index into feature array

        ///< Label associated with
        bool label() const { return exampleNum_ & 1; }
        unsigned exampleNum() const { return exampleNum_ >> 1; }
    };
    
    // Entry for an individual feature
    struct Feature {
        bool active = false;  ///< If true, the feature can be split on
        bool ordinal = true; ///< If true, it's continuous valued; else categ.
        const DatasetFeatureSpace::ColumnInfo * info = nullptr;
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
        wAll[row.label()] += row.weight;
    }

    void addRow(bool label, float weight, int exampleNum)
    {
        rows.emplace_back(label, weight, exampleNum);
        wAll[label] += weight;
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

    // Weights matrix of all rows
    W wAll;
    
    /** Split the partition here. */
    std::pair<PartitionData, PartitionData>
    split(int featureToSplitOn, int splitValue,
          const W & wLeft, const W & wRight)
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

        // Density of example numbers within our set of rows.  When this
        // gets too low, we do essentially random accesses and it kills
        // our cache performance.  In that case we can re-index to reduce
        // the size.
        double useRatio = 1.0 * rows.size() / rows.back().exampleNum();

        //todo: Re-index when usable data fits inside cache
        bool reIndex = useRatio < 0.25;
        //reIndex = false;
        //using namespace std;
        //cerr << "useRatio = " << useRatio << endl;

        if (!reIndex) {

            sides[0].rows.reserve(rows.size());
            sides[1].rows.reserve(rows.size());

            //this is for debug only
            //int maxBucket = 0;
            //int minBucket = INFINITY;

            for (size_t i = 0;  i < rows.size();  ++i) {
                int bucket = features[featureToSplitOn].buckets[rows[i].exampleNum()];
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
                int bucket = features[featureToSplitOn].buckets[rows[i].exampleNum()];
                int side = ordinal ? bucket > splitValue : bucket != splitValue;
                lr[i] = side;
                sides[side].addRow(rows[i].label(), rows[i].weight, numOnSide[side]++);
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
                    newFeatures[side].write(features[i].buckets[rows[j].exampleNum()]);
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

    // Core kernel of the decision tree search algorithm.  Transfer the
    // example weight into the appropriate (bucket,label) accumulator.
    // Returns whether
    static std::pair<bool, int>
    testFeatureKernel(const Row * rows,
                      size_t numRows,
                      const BucketList & buckets,
                      W * w /* buckets.numBuckets entries */)
    {
        // Number of the last bucket we saw.  Enables us to determine if
        // we change buckets at any point.
        int lastBucket = -1;

        // Number of times we've changed bucket numbers.  Since lastBucket
        // starts off at -1, this will be incremented to 0 on the first loop
        // iteration.
        int bucketTransitions = -1;

        // Maximum bucket number we've seen.  Can significantly reduce the
        // work required to search the buckets later on, as those without
        // an example have no possible split point.
        int maxBucket = -1;
        
        for (size_t j = 0;  j < numRows;  ++j) {
            const Row & r = rows[j];
            int bucket = buckets[r.exampleNum()];
            bucketTransitions += (bucket != lastBucket);
            lastBucket = bucket;

            w[bucket][r.label()] += r.weight;
            maxBucket = std::max(maxBucket, bucket);
        }
        
        return { bucketTransitions > 0, maxBucket };
    }

    static double scoreSplit(const W & wFalse, const W & wTrue)
    {
        double score
            = 2.0 * (  sqrt(wFalse[0] * wFalse[1])
                       + sqrt(wTrue[0] * wTrue[1]));
        return score;
    };

    static std::tuple<double /* bestScore */,
                      int /* bestSplit */,
                      W /* bestLeft */,
                      W /* bestRight */>
    chooseSplitKernel(const W * w /* buckets.numBuckets entries */,
                      int maxBucket,
                      bool ordinal,
                      const W & wAll)
    {
        double bestScore = INFINITY;
        int bestSplit = -1;
        
        W bestLeft;
        W bestRight;

        if (ordinal) {
            // Calculate best split point for ordered values
            W wFalse = wAll, wTrue;

            // Now test split points one by one
            for (unsigned j = 0;  j < maxBucket;  ++j) {
                if (w[j].empty())
                    continue;                   

                double s = scoreSplit(wFalse, wTrue);

#if 0                
                if (debug) {
                    std::cerr << "  ord split " << j << " "
                              << features.info->bucketDescriptions.getValue(j)
                              << " had score " << s << std::endl;
                    std::cerr << "    false: " << wFalse[0] << " " << wFalse[1] << std::endl;
                    std::cerr << "    true:  " << wTrue[0] << " " << wTrue[1] << std::endl;
                }
#endif
                
                if (s < bestScore) {
                    bestScore = s;
                    bestSplit = j;
                    bestRight = wFalse;
                    bestLeft = wTrue;
                }
                
                wFalse -= w[j];
                wTrue += w[j];
            }
        }
        else {
            // Calculate best split point for non-ordered values
            // Now test split points one by one

            for (unsigned j = 0;  j <= maxBucket;  ++j) {
                    
                if (w[j].empty())
                    continue;

                W wFalse = wAll;
                wFalse -= w[j];                    

                double s = scoreSplit(wFalse, w[j]);

#if 0                    
                if (debug) {
                    std::cerr << "  non ord split " << j << " "
                              << features.info->bucketDescriptions.getValue(j)
                              << " had score " << s << std::endl;
                    std::cerr << "    false: " << wFalse[0] << " " << wFalse[1] << std::endl;
                    std::cerr << "    true:  " << w[j][0] << " " << w[j][1] << std::endl;
                }
#endif
                    
                if (s < bestScore) {
                    bestScore = s;
                    bestSplit = j;
                    bestRight = wFalse;
                    bestLeft = w[j];
                }
            }

        }

        return { bestScore, bestSplit, bestLeft, bestRight };
    }
    
    static
    std::tuple<double /* bestScore */,
               int /* bestSplit */,
               W /* bestLeft */,
               W /* bestRight */,
               bool /* feature is still active */ >
    testFeatureNumber(int featureNum,
                      const std::vector<Feature> & features,
                      const std::vector<Row> & rows,
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
            = testFeatureKernel(rows.data(), rows.size(),
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
        if (wAll[0] == 0 || wAll[1] == 0)
            return std::make_tuple(1.0, -1, -1, wAll, W());

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

                std::tie(score, split, bestLeft, bestRight, features[i].active)
                    = testFeatureNumber(i, features, rows, wAll);
                return std::make_tuple(score, split, bestLeft, bestRight);
            };

        if (depth < 4 || rows.size() * nf > 100000) {
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
        W wAll;
        for (auto & r: rows) {
            int label = r.label();
            ExcAssert(label >= 0 && label < 2);
            ExcAssert(r.weight > 0);
            wAll[label] += r.weight;
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
        
        std::tie(bestScore, bestFeature, bestSplit, wLeft, wRight)
            = testAll(depth);

        if (bestFeature == -1) {
            ML::Tree::Leaf * leaf = tree.new_leaf();
            fillinBase(leaf, /*wLeft + wRight*/ wAll);
            
            return leaf;
        }

        std::pair<PartitionData, PartitionData> splits
            = split(bestFeature, bestSplit, wLeft, wRight);

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

} // namespace MLDB
