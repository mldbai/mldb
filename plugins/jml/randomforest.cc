/** randomforest.cc                                             -*- C++ -*-
    Mathieu Marquis Bolduc, 11 Mars 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/

#include "randomforest.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/plugins/jml/dataset_feature_space.h"
#include "mldb/utils/fixed_point_accum.h"
#include "mldb/plugins/jml/jml/tree.h"
#include "mldb/plugins/jml/jml/stump_training_bin.h"
#include "mldb/base/parallel.h"
#include "mldb/base/map_reduce.h"
#include "mldb/base/thread_pool.h"
#include "mldb/core/bucket.h"
#include "mldb/utils/lightweight_hash.h"
#include "mldb/arch/timers.h"
#include "mldb/utils/environment.h"
#include "mldb/types/span_description.h"
#include <any>

using namespace std;

namespace MLDB {
namespace RF {

bool compareTrees(ML::Tree::Ptr left, ML::Tree::Ptr right, PartitionIndex index,
                  const DatasetFeatureSpace & fs, std::ostream & stream)
{
    int depth = index.depth();

    auto printSummary = [&] (ML::Tree::Ptr ptr) -> std::string
    {
        std::string result;
        if (!ptr)
            return "null";

        result = 
        "ex=" + std::to_string(ptr.examples())
        + " pred=" + std::to_string(ptr.pred()[0])
        + " " + std::to_string(ptr.pred()[1]);

        if (ptr.isNode()) {
            auto & n = *ptr.node();
            result += " z=" + MLDB::format("%.12f", n.z);
            result += " " + n.split.print(fs);
            result += " ex " + std::to_string((int)n.child_false.examples())
                + ":"+ std::to_string((int)n.child_true.examples());
        }

        return result;
    };

    auto doPrint = [&] () -> bool
    {
        stream << "difference at depth " << depth << " index "
                << index << endl;
        stream << "  rec:  " << printSummary(left) << endl;
        stream << "  part: " << printSummary(right) << endl;
        return false;
    };

    bool different = false;
    
    if ((left.pred() != right.pred()).any()) {
        stream << "different predictions at depth "
                << depth << " index " << index
                << ": " << left.pred()
                << " vs " << right.pred() << endl;
        different = true;
    }
    if (left.examples() != right.examples()) {
        stream << "different examples at depth "
        << depth << " index " << index << ": "
                << left.examples() << " vs " << right.examples()
                << endl;
        different = true;
    }
    if (left.isNode() && right.isNode()) {
        const ML::Tree::Node & l = *left.node();
        const ML::Tree::Node & r = *right.node();

        if (l.split != r.split) {
            stream << "different split: "
                    << l.split.print(fs) << " vs "
                    << r.split.print(fs) << endl;

            different = true;
        }
        if (l.z != r.z) {
            stream << "different z: "
                    << l.z << " vs " << r.z << endl;
            different = true;
        }

        if (!different) {
            if (!compareTrees(l.child_true, r.child_true, index.leftChild(), fs, stream)) {
                different = true;
                stream << "different left" << endl;
                return doPrint();
            }
            
            if (!compareTrees(l.child_false, r.child_false, index.rightChild(), fs, stream)) {
                different = true;
                stream << "different right" << endl;
                return doPrint();
            }
        }
        
        return different ? doPrint() : true;
    }
    else if (left.isLeaf() && right.isLeaf()) {
        // already compared
        return different ? doPrint() : true;
    }
    else {
        stream << "different type at depth " << depth << endl;
        return doPrint();
    }
}


PartitionData::PartitionData(std::shared_ptr<const DatasetFeatureSpace> fs)
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

void PartitionData::clear()
{
    rows.clear();
    features.clear();
    fs.reset();
}

PartitionData PartitionData::
reweightAndCompact(const std::vector<uint8_t> & counts,
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
    chunkSize += (chunkSize == 0);

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
    std::vector<float> uniqueWeights(allUniques.begin(), allUniques.end());
    std::sort(uniqueWeights.begin(), uniqueWeights.end());
    
    using namespace std;
    
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
    // this enables a single device transfer and a single device argument
    // list (for when we do devices)
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

void PartitionData::
splitWithoutReindex(PartitionData * sides,
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

void PartitionData::
splitAndReindex(PartitionData * sides,
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
    // blocks on each side; this enables a single device transfer
    // and a single device argument list.
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

std::pair<PartitionData, PartitionData>
PartitionData::
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

    ExcAssert(fs);
    ExcAssert(!features.empty());

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

ML::Tree::Ptr
PartitionData::
trainPartitioned(int depth, int maxDepth,
                    ML::Tree & tree,
                    MappedSerializer & serializer) const
{
    return trainPartitionedEndToEnd(depth, maxDepth, tree, serializer,
                                    rows, features, bucketMemory, *fs);
}

ML::Tree::Ptr
PartitionData::
train(int depth, int maxDepth,
      ML::Tree & tree,
      MappedSerializer & serializer,
      TrainingScheme trainingScheme) const
{
    constexpr bool singleThreadOnly = true;

    using namespace std;
    
    if (rows.rowCount() == 0)
        return ML::Tree::Ptr();
    if (rows.rowCount() < 2)
        return getLeaf(tree, rows.wAll);

    if (depth >= maxDepth)
        return getLeaf(tree, rows.wAll);

    ML::Tree::Ptr part;

    if (trainingScheme == PARTITIONED) {
        return trainPartitioned(depth, maxDepth, tree, serializer);
    }
    else if (trainingScheme == BOTH_AND_COMPARE) {
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
    }
    // else we're in recursive only...
    
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

    if (leftRows + rightRows < 1000 || singleThreadOnly) {
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
                            left, right, wLeft, wRight, features, *fs);
    }
    else {
        result = getLeaf(tree, wLeft + wRight);
    }

    if (depth == 0 && part) {
        Date after = Date::now();
        cerr << "recursive took " << after.secondsSince(before) * 1000.0
                << "ms " << timer->elapsed() << endl;
        ExcAssert(compareTrees(result, part, PartitionIndex::root(), *fs));
        
    }
    
    return result;
}

EnvOption<bool> DEBUG_RF_KERNELS("DEBUG_RF_KERNELS", 1);

ML::Tree::Ptr
trainPartitionedEndToEndKernel(int depth, int maxDepth,
                               ML::Tree & tree,
                               MappedSerializer & serializer,
                               const Rows & rows,
                               const std::span<const Feature> & features,
                               FrozenMemoryRegionT<uint32_t> bucketMemory,
                               const DatasetFeatureSpace & fs,
                               const ComputeDevice & device)
{
    const bool debugKernelOutput = DEBUG_RF_KERNELS;
    constexpr uint32_t maxIterations = 16;

    // First, figure out the memory requirements.  This means sizing all
    // kinds of things so that we can make our allocations statically.

    // How much total memory was allocated?
    uint64_t totalDeviceAllocation = 0;

    // How many rows in this partition?
    size_t numRows = rows.rowCount();

    // How many features?
    uint32_t nf = features.size();
    
    // Which of our features do we need to consider?  This allows us to
    // avoid sizing things too large for the number of features that are
    // actually active.
    std::vector<int> activeFeatures;

    // How many iterations can we run for?  This may be reduced if the
    // memory is not available to run the full width
    int numIterations = std::min(maxIterations, uint32_t(maxDepth - depth));

    // How many partitions will we have at our widest?
    int maxPartitionCount = 1 << numIterations;
    
    size_t rowCount = rows.rowCount();
    
    // Maximum number of buckets
    size_t maxBuckets = 0;

    // Number of active features
    size_t numActiveFeatures = 0;
    
    // Now we figure out how to onboard a variable number of variable
    // lengthed data segments for the feature bucket information.
    uint32_t numActiveBuckets = 0;
    
    uint32_t lastBucketDataOffset = 0;
    std::vector<uint32_t> bucketMemoryOffsets;   ///< Offset in the buckets memory blob per feature [nf + 1]
    std::vector<uint32_t> bucketEntryBits;       ///< How many bits per bucket [nf]
    std::vector<uint32_t> bucketNumbers(1, 0);   ///< Range of bucket numbers for feature [nf + 1]
    std::vector<uint32_t> featuresActive;        ///< For each feature: which are active? [nf]
    std::vector<uint32_t> featureIsOrdinal;      ///< For each feature: is it ordinal (1) vs categorical(0) [nf]
    
    // For each feature, we set up a table of offsets which will allow our OpenCL kernel
    // to know where in a flat buffer of memory the data for that feature resides.
    for (int i = 0;  i < nf;  ++i) {
        const BucketList & buckets = features[i].buckets;

        bucketEntryBits.push_back(buckets.entryBits);

        featuresActive.push_back(features[i].active);
        featureIsOrdinal.push_back(features[i].ordinal);
        
        if (features[i].active) {
            //cerr << "feature " << i << " buckets " << features[i].buckets.numBuckets << endl;
            ExcAssertGreaterEqual((void *)buckets.storage.data(),
                                  (void *)bucketMemory.data());
        
            activeFeatures.push_back(i);

            uint32_t offset
                = buckets.storage.data()
                - bucketMemory.data();

            //cerr << "feature = " << i << " offset = " << offset << " numActiveBuckets = " << numActiveBuckets << endl;
            bucketMemoryOffsets.push_back(offset);
            lastBucketDataOffset = offset + bucketMemory.length();
            
            ++numActiveFeatures;
            numActiveBuckets += features[i].buckets.numBuckets;
            maxBuckets = std::max<size_t>(maxBuckets,
                                          features[i].buckets.numBuckets);
        }
        else {
            bucketMemoryOffsets.push_back(lastBucketDataOffset);
        }

        bucketNumbers.push_back(numActiveBuckets);
    }

    bucketMemoryOffsets.push_back(lastBucketDataOffset);

    ExcAssertEqual(bucketMemoryOffsets.size(), nf + 1);
    ExcAssertEqual(bucketEntryBits.size(), nf);
    ExcAssertEqual(bucketNumbers.size(), nf + 1);
    ExcAssertEqual(featuresActive.size(), nf);

    // How much memory to accumulate W over all features per partition?
    size_t bytesPerPartition = sizeof(W) * numActiveBuckets;

    // How much memory to accumulate W over all features over the maximum
    // number of partitions?
    size_t bytesForAllPartitions = bytesPerPartition * maxPartitionCount;

    cerr << "numActiveBuckets = " << numActiveBuckets << endl;
    cerr << "sizeof(W) = " << sizeof(W) << endl;
    cerr << "bytesForAllPartitions = " << bytesForAllPartitions * 0.000001
         << "mb" << endl;
    cerr << "numIterations = " << numIterations << endl;
    
    Date before = Date::now();

    std::vector<std::pair<std::string, std::shared_ptr<ComputeEvent>>> allEvents;

    ComputeContext context;

    ComputeQueue queue(&context);

    // First, we need to send over the rows, as the very first thing to
    // be done is to expand them.
    auto [deviceRowData, copyRowData, rowMemorySizePageAligned]
        = context.transferToDeviceImmutable(rows.rowData, "copyRowData");

    // Same for our weight data
    auto [deviceWeightData, copyWeightData, unused1]
        = context.transferToDeviceImmutable(rows.weightEncoder.weightFormatTable, "copyWeightData");

    // We transfer the bucket data as early as possible, as it's one of the
    // longest things to transfer
    auto [deviceBucketData, transferBucketData, bucketMemorySizePageAligned]
        = context.transferToDeviceImmutable(bucketMemory, "transferBucketData");
    
    // This one contains an expanded version of the row data, with one float
    // per row rather than bit-compressed.  It's expanded on the device so that
    // the compressed version can be passed over the PCIe bus and not the
    // expanded version.
    MemoryArrayHandleT<float> deviceExpandedRowData = context.allocArray<float>(rowCount);

    // Our first kernel expands the data.  It's pretty simple, as a warm
    // up for the rest.
    auto decodeRowsKernel = context.getKernel("decodeRows", device);

    cerr << "numRows = " << numRows << endl;

    cerr << "deviceExpandedRowData = " << deviceExpandedRowData.handle << endl;

    auto boundKernel = decodeRowsKernel
        ->bind(  "rowData",          deviceRowData,
                 "rowDataLength",    (uint32_t)rows.rowData.length(),
                 "weightBits",       (uint16_t)rows.weightEncoder.weightBits,
                 "exampleNumBits",   (uint16_t)rows.exampleNumBits,
                 "numRows",          (uint32_t)numRows,
                 "weightFormat",     rows.weightEncoder.weightFormat,
                 "weightMultiplier", rows.weightEncoder.weightMultiplier,
                 "weightData",       deviceWeightData,
                 "decodedRowsOut",   deviceExpandedRowData);

    std::shared_ptr<ComputeEvent> runDecodeRows
        = queue.launch(boundKernel, {}, { copyRowData });
    
    allEvents.emplace_back("runDecodeRows", runDecodeRows);

    std::vector<float> debugExpandedRowsCpu;

    if (debugKernelOutput) {
        // Verify that the kernel version gives the same results as the non-kernel version
        debugExpandedRowsCpu = decodeRows(rows);
        auto frozenExpandedRowsDevice = context.transferToCpuSync(deviceExpandedRowData);
        auto expandedRowsDevice = frozenExpandedRowsDevice.getConstSpan();
        ExcAssertEqual(expandedRowsDevice.size(), debugExpandedRowsCpu.size());
        bool different = false;
        
        for (size_t i = 0;  i < rowCount;  ++i) {
            if (debugExpandedRowsCpu[i] != expandedRowsDevice[i]) {
                cerr << "row " << i << " CPU " << debugExpandedRowsCpu[i]
                     << " Device " << expandedRowsDevice[i] << endl;
                different = true;
            }
        }

        ExcAssert(!different && "runExpandRows");
    }

    // Next we need to distribute the weignts into the first set of
    // buckets.  This is done with the testFeature kernel.

    // Before that, we need to set up some memory objects to be used
    // by the kernel.

    auto deviceBucketNumbers = context.manageMemoryRegion(bucketNumbers);
    auto deviceBucketEntryBits = context.manageMemoryRegion(bucketEntryBits);
    auto deviceFeaturesActive = context.manageMemoryRegion(featuresActive);
    auto deviceBucketDataOffsets = context.manageMemoryRegion(bucketMemoryOffsets);

    // Our wAll array contains the sum of all of the W buckets across
    // each partition.  We allocate a single array at the start and just
    // use more and more each iteration.
    auto deviceWAll = context.allocArray<W>(maxPartitionCount);

    // The first one is initialized by the input wAll
    std::shared_ptr<ComputeEvent> copyWAll
        = queue.enqueueFillArray(deviceWAll, rows.wAll, 0 /* offset */, 1 /* size */);

    allEvents.emplace_back("copyWAll", copyWAll);

    
    // The rest are initialized to zero
    std::shared_ptr<ComputeEvent> initializeWAll
        = queue.enqueueFillArray(deviceWAll, W(), 1 /* offset */);

    allEvents.emplace_back("initializeWAll", initializeWAll);
    
    // Our W buckets live here, per partition.  We never need to see it on
    // the host, so we allow it to be initialized and live on the device.
    // Note that we only use the beginning 2 at the start, and we
    // double the amount of what we use until we're using the whole lot
    // on the last iteration
    MemoryArrayHandleT<W> devicePartitionBuckets = context.allocArray<W>(maxPartitionCount * numActiveBuckets);

    // Before we use this, it needs to be zero-filled (only the first
    // set for a single partition)
    std::shared_ptr<ComputeEvent> fillFirstBuckets
        = queue.enqueueFillArray(devicePartitionBuckets, W());

    allEvents.emplace_back("fillFirstBuckets", fillFirstBuckets);

    auto testFeatureKernel
        = context.getKernel("testFeature", device);

    cerr << "deviceExpandedRowData = " << deviceExpandedRowData.handle << endl;
    cerr << "maxPartitionCount = " << maxPartitionCount << endl;

    auto boundTestFeatureKernel = testFeatureKernel
        ->bind( "decodedRows",                      deviceExpandedRowData,
                "numRows",                          (uint32_t)numRows,
                "bucketData",                       deviceBucketData,
                "bucketDataOffsets",                deviceBucketDataOffsets,
                "bucketNumbers",                    deviceBucketNumbers,
                "bucketEntryBits",                  deviceBucketEntryBits,
                "featuresActive",                   deviceFeaturesActive,
                "partitionBuckets",                 devicePartitionBuckets);

    //cerr << jsonEncode(testFeatureKernel.getInfo()) << endl;
    //cerr << jsonEncode(OpenCLKernelWorkgroupInfo(testFeatureKernel, kernelContext.devices[0])) << endl;

    std::shared_ptr<ComputeEvent> runTestFeatureKernel
        = queue.launch(boundTestFeatureKernel,
                       { nf },
                       { transferBucketData, fillFirstBuckets, runDecodeRows });

    allEvents.emplace_back("runTestFeatureKernel", runTestFeatureKernel);

    if (debugKernelOutput) {
        // Get that data back (by mapping), and verify it against the
        // CPU-calcualted version.
        
        auto frozenPartitionBuckets = context.transferToCpuSync(devicePartitionBuckets);
        auto allWDevice = frozenPartitionBuckets.getConstSpan();

        std::vector<W> allWCpu(numActiveBuckets);
        
        bool different = false;
            
        // Print out the buckets that differ from CPU to Device
        for (int i = 0;  i < nf;  ++i) {
            int start = bucketNumbers[i];
            int end = bucketNumbers[i + 1];
            int n = end - start;

            if (n == 0)
                continue;

            testFeatureKernelCpu(rows.getRowIterator(),
                                 rowCount,
                                 features[i].buckets,
                                 allWCpu.data() + start);

            const W * pDevice = allWDevice.data() + start;
            const W * pCpu = allWCpu.data() + start;

            for (int j = 0;  j < n;  ++j) {
                if (pCpu[j] != pDevice[j]) {
                    cerr << "feat " << i << " bucket " << j << " w "
                         << jsonEncodeStr(pDevice[j]) << " != "
                         << jsonEncodeStr(pCpu[j]) << endl;
                    different = true;
                }
            }
        }

        ExcAssert(!different && "runTestFeatureKernel");
    }

    // Which partition is each row in?  Initially, everything
    // is in partition zero, but as we start to split, we end up
    // splitting them amongst many partitions.  Each partition has
    // its own set of buckets that we maintain.
    MemoryArrayHandleT<uint32_t> devicePartitions
        = context.allocZeroInitializedArray<uint32_t>(rowCount);

    // Array to cache transfer directions to avoid re-calculating
    MemoryArrayHandleT<uint8_t> deviceDirections
        = context.allocArray<uint8_t>(rowCount);

    // Record the split, per level, per partition
    std::vector<std::vector<PartitionSplit> > depthSplits;
    depthSplits.reserve(16);

    MemoryArrayHandleT<uint32_t> deviceFeatureIsOrdinal
        = context.manageMemoryRegion(featureIsOrdinal);
    
    totalDeviceAllocation += featureIsOrdinal.size() * sizeof(int);

    // How many partitions at the current depth?
    unsigned numPartitionsAtDepth = 1;

    // Which event represents that the previous iteration of partitions
    // are available?
    std::shared_ptr<ComputeEvent> previousIteration = runTestFeatureKernel;

    // Event list for all of the buckets
    std::vector<std::shared_ptr<ComputeEvent>> deviceDepthSplitsEvents;

    // Each of the numIterations partitions has double the number of buckets,
    // so the total number is 2^(numIterations + 1) - 1.
    MemoryArrayHandleT<PartitionSplit> deviceAllPartitionSplits
        = context.allocArray<PartitionSplit>(2 << numIterations);
    
    // Pre-allocate partition buckets for the widest bucket
    // We need to store partition splits for each partition and each
    // feature.  Get the memory.  It doesn't need to be initialized.
    // Layout is partition-major.
    MemoryArrayHandleT<PartitionSplit> deviceFeaturePartitionSplits
        = context.allocArray<PartitionSplit>((1 << numIterations) * nf);

    Date startDepth = Date::now();

    // We go down level by level
    for (int myDepth = 0;
         myDepth < numIterations && depth < maxDepth;
         ++depth, ++myDepth, numPartitionsAtDepth *= 2) {

        cerr << "depth = " << depth << " myDepth = " << myDepth << " numPartitions " << numPartitionsAtDepth << endl;

        // Run a kernel to find the new split point for each partition,
        // best feature and kernel

        // Now we have initialized our data, we can get to running the
        // kernel.  This kernel is dimensioned on bucket number,
        // feature number and partition number (ie, a 3d invocation).
        
        auto getPartitionSplitsKernel
            = context.getKernel("getPartitionSplits", device);

        auto boundGetPartitionSplitsKernel = getPartitionSplitsKernel
            ->bind("totalBuckets",                   (uint32_t)numActiveBuckets,
                   "bucketNumbers",                  deviceBucketNumbers,
                   "featuresActive",                 deviceFeaturesActive,
                   "featureIsOrdinal",               deviceFeatureIsOrdinal,
                   "buckets",                        devicePartitionBuckets,
                   "wAll",                           deviceWAll,
                   "featurePartitionSplitsOut",      deviceFeaturePartitionSplits);

        //cerr << endl << endl << " depth " << depth << " numPartitions "
        //     << numPartitionsAtDepth << " buckets "
        //     << numPartitionsAtDepth * numActiveBuckets << endl;

        std::shared_ptr<ComputeEvent> runPartitionSplitsKernel
            = queue.launch(boundGetPartitionSplitsKernel,
                           { nf, numPartitionsAtDepth },
                           { previousIteration, copyWAll, initializeWAll});

        allEvents.emplace_back("runPartitionSplitsKernel "
                               + std::to_string(myDepth),
                               runPartitionSplitsKernel);

        // Now we have the best split for each feature for each partition,
        // find the best one per partition and finally record it.
        auto bestPartitionSplitKernel
            = context.getKernel("bestPartitionSplit", device);

        // What is our offset into partitionSplits?  In other words, where do
        // the partitions for this iteration start?  By skipping the first
        // bucket, this becomes trivially numPartitionsAtDepth: 1, 2, 4, 8, ...
        // It's not technically necessary to pass it since it's one of the
        // launch parameters, but this way is more clear.
        uint32_t partitionSplitsOffset = numPartitionsAtDepth;
        
        auto boundBestPartitionSplitKernel = bestPartitionSplitKernel
            ->bind("numFeatures",            (uint32_t)nf,
                  "featuresActive",         deviceFeaturesActive,
                  "featurePartitionSplits", deviceFeaturePartitionSplits,
                  "allPartitionSplitsOut",  deviceAllPartitionSplits,
                  "partitionSplitsOffset",  partitionSplitsOffset);

        std::shared_ptr<ComputeEvent> runBestPartitionSplitKernel
            = queue.launch(boundBestPartitionSplitKernel,
                           { numPartitionsAtDepth },
                           { runPartitionSplitsKernel });

        allEvents.emplace_back("runBestPartitionSplitKernel "
                               + std::to_string(myDepth),
                               runBestPartitionSplitKernel);

        // These are parallel CPU data structures for the on-device ones,
        // into which we copy the input data required to re-run the
        // computation on the CPU so we can verify the output of the device
        // algorithm.
        std::vector<PartitionSplit> debugPartitionSplitsCpu;
        std::vector<std::vector<W> > debugBucketsCpu;
        std::vector<W> debugWAllCpu;
        std::vector<uint32_t> debugPartitionsCpu;
        std::set<int> okayDifferentPartitions;

        if (debugKernelOutput) {
            // Map back the device partition splits (note that we only use those between
            // numPartitionsAtDepth and 2 * numPartitionsAtDepth)
            auto mappedPartitionSplits = context.transferToCpuSync(deviceAllPartitionSplits);
            auto partitionSplitsDevice
                 = mappedPartitionSplits.getConstSpan(numPartitionsAtDepth, numPartitionsAtDepth);

            // Map back the device partition numbers
            auto mappedPartitions = context.transferToCpuSync(devicePartitions);
            auto partitionsDevice = mappedPartitions.getConstSpan();

            debugPartitionsCpu = { partitionsDevice.begin(), partitionsDevice.end() };
            
            // Construct the CPU version of buckets
            auto mappedBuckets = context.transferToCpuSync(devicePartitionBuckets);
            auto bucketsDevice = mappedBuckets.getConstSpan();

            for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
                const W * partitionBuckets = bucketsDevice.data() + numActiveBuckets * i;
                debugBucketsCpu.emplace_back(partitionBuckets,
                                             partitionBuckets + numActiveBuckets);
            }
            
            // Get back the CPU version of wAll
            auto mappedWAll = context.transferToCpuSync(deviceWAll);
            auto wAllDevice = mappedWAll.getConstSpan();
            debugWAllCpu = { wAllDevice.begin(),
                             wAllDevice.begin() + numPartitionsAtDepth };

            std::vector<PartitionIndex> indexes;
            
            for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
                indexes.push_back(numPartitionsAtDepth + i);
            }

            // Run the CPU version... first getting the data in place
            debugPartitionSplitsCpu
                = getPartitionSplits(debugBucketsCpu,
                                     activeFeatures, bucketNumbers,
                                     features, debugWAllCpu,
                                     indexes, 
                                     false /* parallel */);

            // Make sure we got the right thing back out
            ExcAssertEqual(debugPartitionSplitsCpu.size(),
                           numPartitionsAtDepth);

            bool different = false;
            
            for (int p = 0;  p < numPartitionsAtDepth;  ++p) {

                //cerr << "p = " << p << " of " << numPartitionsAtDepth << endl
                //     << " CPU " << jsonEncodeStr(debugPartitionSplitsCpu[p]) << endl
                //     << " device " << jsonEncodeStr(partitionSplitsDevice[p])
                //     << endl;

                if ((partitionSplitsDevice[p].left
                     != debugPartitionSplitsCpu[p].left)
                    || (partitionSplitsDevice[p].right
                        != debugPartitionSplitsCpu[p].right)
                    || (partitionSplitsDevice[p].feature
                        != debugPartitionSplitsCpu[p].feature)
                    || (partitionSplitsDevice[p].value
                        != debugPartitionSplitsCpu[p].value)
                    || (partitionSplitsDevice[p].index
                        != debugPartitionSplitsCpu[p].index)
                    || (partitionSplitsDevice[p].score
                        != debugPartitionSplitsCpu[p].score)) {
                    float score1 = partitionSplitsDevice[p].score;
                    float score2 = debugPartitionSplitsCpu[p].score;

                    okayDifferentPartitions.insert(p);
                    okayDifferentPartitions.insert(p + numPartitionsAtDepth);

                    float relativeDifference = fabs(score1 - score2) / max(score1, score2);
                    if (!partitionSplitsDevice[p].valid() && !debugPartitionSplitsCpu[p].valid())
                        continue;

                    different = different || isnan(relativeDifference) || relativeDifference >= 0.001;
                    cerr << "partition " << p << "\ndevice "
                         << jsonEncodeStr(partitionSplitsDevice[p])
                         << "\nCPU " << jsonEncodeStr(debugPartitionSplitsCpu[p])
                         << " score relative difference " << relativeDifference << endl;
                    if ((partitionSplitsDevice[p].score != debugPartitionSplitsCpu[p].score))
                        cerr << "score device: " << *(uint32_t *)(&partitionSplitsDevice[p].score)
                             << " score CPU: " << *(uint32_t *)(&debugPartitionSplitsCpu[p].score)
                             << endl;
                }
            }
            
            ExcAssert(!different);
        }

        // Double the number of partitions, create new W entries for the
        // new partitions, and transfer those examples that are in the
        // wrong partition to the right one

        // To update buckets, we first transfer the smallest number of
        // examples possible into the new partitions, without subtracting
        // them from the original partitions.  Afterwards, we subtract them
        // and swap any which are in the wrong order.  This means that we
        // only need to keep one set of partition buckets in shared memory
        // and saves lots of atomic operations.

        // First we clear everything on the right side, ready to accumulate
        // the new buckets there.

        auto clearBucketsKernel
            = context.getKernel("clearBuckets", device);

        auto boundClearBucketsKernel = clearBucketsKernel
            ->bind("bucketsOut",             devicePartitionBuckets,
                   "wAllOut",                deviceWAll,
                   "allPartitionSplits",     deviceAllPartitionSplits,
                   "numActiveBuckets",       (uint32_t)numActiveBuckets,
                   "partitionSplitsOffset",  (uint32_t)numPartitionsAtDepth);

        std::shared_ptr<ComputeEvent> runClearBucketsKernel
            = queue.launch(boundClearBucketsKernel,
                           { numPartitionsAtDepth, numActiveBuckets },
                           { runPartitionSplitsKernel });
 
        allEvents.emplace_back("runClearBucketsKernel "
                               + std::to_string(myDepth),
                               runClearBucketsKernel);

        // While we're doint that, we can also calculate our new
        // partition numbers (for each row)
        auto updatePartitionNumbersKernel
            = context.getKernel("updatePartitionNumbers", device);

        auto boundUpdatePartitionNumbersKernel = updatePartitionNumbersKernel
            ->bind("partitionSplitsOffset",          (uint32_t)numPartitionsAtDepth,  // rightOffset
                   "partitions",                     devicePartitions,
                   "directions",                     deviceDirections,
                   "allPartitionSplits",             deviceAllPartitionSplits,
                   "bucketData",                     deviceBucketData,
                   "bucketDataOffsets",              deviceBucketDataOffsets,
                   "bucketNumbers",                  deviceBucketNumbers,
                   "bucketEntryBits",                deviceBucketEntryBits,
                   "featureIsOrdinal",               deviceFeatureIsOrdinal);

        std::shared_ptr<ComputeEvent> runUpdatePartitionNumbersKernel
            = queue.launch(boundUpdatePartitionNumbersKernel, { (uint32_t)numRows },
                            { runPartitionSplitsKernel });

        // Now the right side buckets are clear, we can transfer the weights
        // for the examples who have changed bucket from the left to the right.
        auto updateBucketsKernel
            = context.getKernel("updateBuckets", device);

        auto boundUpdateBucketsKernel = updateBucketsKernel
            ->bind("partitionSplitsOffset",         (uint32_t)numPartitionsAtDepth,  // rightOffset
                  "numActiveBuckets",               (uint32_t)numActiveBuckets,
                  "partitions",                     devicePartitions,
                  "directions",                     deviceDirections,
                  "buckets",                        devicePartitionBuckets,
                  "wAll",                           deviceWAll,
                  "allPartitionSplits",             deviceAllPartitionSplits,
                  "decodedRows",                    deviceExpandedRowData,
                  "numRows",                        (uint32_t)numRows,
                  "bucketData",                     deviceBucketData,
                  "bucketDataOffsets",              deviceBucketDataOffsets,
                  "bucketNumbers",                  deviceBucketNumbers,
                  "bucketEntryBits",                deviceBucketEntryBits,
                  "featuresActive",                 deviceFeaturesActive,
                  "featureIsOrdinal",               deviceFeatureIsOrdinal);

        std::shared_ptr<ComputeEvent> runUpdateBucketsKernel
            = queue.launch(boundUpdateBucketsKernel, { nf + 1 /* +1 is wAll */},
                            { runClearBucketsKernel, runUpdatePartitionNumbersKernel });
        
        allEvents.emplace_back("runUpdateBucketsKernel "
                               + std::to_string(myDepth),
                               runUpdateBucketsKernel);
        
        auto fixupBucketsKernel
            = context.getKernel("fixupBuckets", device);

        auto boundFixupBucketsKernel = fixupBucketsKernel
            ->bind("buckets",                   devicePartitionBuckets,
                   "wAll",                       deviceWAll,
                   "allPartitionSplits",         deviceAllPartitionSplits);

        std::shared_ptr<ComputeEvent> runFixupBucketsKernel
            = queue.launch(boundFixupBucketsKernel,
                           { numPartitionsAtDepth, numActiveBuckets },
                           { runUpdateBucketsKernel });
 
        allEvents.emplace_back("runFixupBucketsKernel "
                               + std::to_string(myDepth),
                               runFixupBucketsKernel);

        if (debugKernelOutput) {

            // These give the partition numbers for the left (.first) and right (.second) of each partition
            // or -1 if it's not active or < -2 if it's handled as a leaf
            std::vector<std::pair<int32_t, int32_t> > newPartitionNumbers(numPartitionsAtDepth, {-1,-1});
 
            ExcAssertEqual(numPartitionsAtDepth, debugPartitionSplitsCpu.size());

            for (int i = 0;  i < numPartitionsAtDepth;  ++i) {
                const PartitionSplit & split = debugPartitionSplitsCpu.at(i);
                if (!split.valid())
                    continue;
                int left = i;
                int right = i + numPartitionsAtDepth;
                newPartitionNumbers[i] = { left, right };
            }
            
            //cerr << "newPartitionNumbers = " << jsonEncodeStr(newPartitionNumbers) << endl;

            // Run the CPU version of the computation
            updateBuckets(features,
                          debugPartitionsCpu,
                          debugBucketsCpu,
                          debugWAllCpu,
                          bucketNumbers,
                          debugPartitionSplitsCpu,
                          newPartitionNumbers,
                          numPartitionsAtDepth * 2,
                          debugExpandedRowsCpu,
                          activeFeatures);

            // There are three things that we modify (in-place):
            // 1) The per-partition, per-feature W buckets
            // 2) The per-partition wAll array
            // 3) The per-row partition number array
            //
            // Each of these three will be mapped back from the device and
            // its accuracy verified.

            bool different = false;

            // 1.  Map back the W buckets and compare against the CPU
            // version.

            // Construct the CPU version of buckets
            auto mappedBuckets = context.transferToCpuSync(devicePartitionBuckets);
            auto bucketsDevice = mappedBuckets.getConstSpan();

            for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
                std::span<const W> partitionBuckets = bucketsDevice.subspan(numActiveBuckets * i);
                if (newPartitionNumbers[i].first == -1)
                    continue;  // dead partition, don't verify...
                if (okayDifferentPartitions.count(i))
                    continue;  // is different due to a different split caused by numerical errors

                if (!debugBucketsCpu[i].empty()) {
                    for (size_t j = 0;  j < numActiveBuckets;  ++j) {
                        if (partitionBuckets[j] != debugBucketsCpu[i].at(j)) {
                            cerr << "part " << i << " bucket " << j
                                 << " num " << numActiveBuckets * i + j
                                 << " update error: CPU "
                                 << jsonEncodeStr(debugBucketsCpu[i][j])
                                 << " device "
                                 << jsonEncodeStr(partitionBuckets[j])
                                 << endl;
                            different = true;
                        }
                    }
                } else {
                    for (size_t j = 0;  j < numActiveBuckets;  ++j) {
                        if (partitionBuckets[j].count() != 0) {
                            cerr << "part " << i << " bucket " << j
                                 << " update error: CPU empty "
                                 << " device "
                                 << jsonEncodeStr(partitionBuckets[j])
                                 << endl;
                            different = true;
                        }
                    }
                }
            }

            // 2.  Map back the wAll values and compare against the CPU version
            auto mappedWAll = context.transferToCpuSync(deviceWAll);
            auto wAllDevice = mappedWAll.getConstSpan();

            for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
                if (newPartitionNumbers[i].first == -1 || okayDifferentPartitions.count(i))
                    continue;  // dead partition, don't verify...
                if (wAllDevice[i] != debugWAllCpu[i]) {
                    cerr << "part " << i << " wAll update error: CPU "
                         << jsonEncodeStr(debugWAllCpu[i])
                         << " device " << jsonEncodeStr(wAllDevice[i])
                         << endl;
                    different = true;
                }
            }

            // 3.  Map back the device partition numbers and compare against
            // the CPU version
            auto mappedPartitions = context.transferToCpuSync(devicePartitions);
            auto partitionsDevice = mappedPartitions.getConstSpan();

            int numDifferences = 0;
            std::map<std::pair<int, int>, int> differenceStats;
            for (size_t i = 0;  i < rowCount;  ++i) {
                if (partitionsDevice[i] != debugPartitionsCpu[i] && debugPartitionsCpu[i] != -1) {
                    if (okayDifferentPartitions.count(debugPartitionsCpu[i]))
                        continue;  // caused by known numerical issues
                    different = true;
                    differenceStats[{debugPartitionsCpu[i], partitionsDevice[i]}] += 1;
                    if (++numDifferences < 10) {
                        cerr << "row " << i << " partition difference: CPU "
                            << (int)debugPartitionsCpu[i]
                            << " device " << (int)partitionsDevice[i]
                            << endl;
                    }
                    else if (numDifferences == 11) {
                        cerr << "..." << endl;
                    }
                }
            }

            if (numDifferences > 0) {
                cerr << "partition number error stats (total " << numDifferences << ")" << endl;
                for (auto & s: differenceStats) {
                    cerr << "  cpu: " << s.first.first << " device: " << s.first.second << " count: " << s.second << endl;
                }
            }

            ExcAssert(!different);
        }

        Date doneDepth = Date::now();

        cerr << "depth " << depth << " wall time is " << doneDepth.secondsSince(startDepth) * 1000 << endl;
        startDepth = doneDepth;

        // Ready for the next level
        previousIteration = runUpdateBucketsKernel;
    }

    queue.flush();

    // If we're not at the lowest level, partition our data and recurse
    // par partition to create our leaves.
    Date beforeMapping = Date::now();


    // Get all the data back...
    auto allPartitionSplitsRegion = context.transferToCpuSync(deviceAllPartitionSplits);
    std::span<const PartitionSplit> allPartitionSplits = allPartitionSplitsRegion.getConstSpan();
    auto bucketsUnrolledRegion = context.transferToCpuSync(devicePartitionBuckets);
    std::span<const W> bucketsUnrolled = bucketsUnrolledRegion.getConstSpan();
    auto partitionsRegion = context.transferToCpuSync(devicePartitions);
    std::span<const uint32_t> partitions = partitionsRegion.getConstSpan();
    auto wAllRegion = context.transferToCpuSync(deviceWAll);
    std::span<const W> wAll = wAllRegion.getConstSpan();
    auto decodedRowsRegion = context.transferToCpuSync(deviceExpandedRowData);
    std::span<const float> decodedRows = decodedRowsRegion.getConstSpan();

    cerr << "kernel wall time is " << Date::now().secondsSince(before) * 1000
         << "ms with " << totalDeviceAllocation / 1000000.0 << "Mb allocated" << endl;
    cerr << "numPartitionsAtDepth = " << numPartitionsAtDepth << endl;

#if 0
    // Look what's leftover for partition splits
    std::vector<std::tuple<PartitionIndex, W64, uint32_t>> activePartitions;
    size_t examplesInActivePartitions = 0;
    for (size_t i = numPartitionsAtDepth / 2;  i < numPartitionsAtDepth;  ++i) {
        auto & part = allPartitionSplits[i];
        //cerr << "doing part " << part.index << endl;
        if (part.index == PartitionIndex::none())
            continue;
        if (!part.valid())
            continue;
        if (part.left.empty() || part.right.empty())
            continue;
        if (part.left.uniform() && part.right.uniform())
            continue;
        if (!part.left.uniform()) {
            activePartitions.emplace_back(part.index.leftChild(), part.left);
            examplesInActivePartitions += part.left.count();
        }
        if (!part.right.uniform()) {
            activePartitions.emplace_back(part.index.rightChild(), part.right);
            examplesInActivePartitions += part.right.count();
        }
    }

    auto sortByCount = [] (auto & l, auto & r) { auto [i1, w1, n1] = l; auto [i2, w2, n2] = r; return w1.count() > w2.count(); };
    std::sort(activePartitions.begin(), activePartitions.end(), sortByCount);

    cerr << activePartitions.size() << " active partitions; " << examplesInActivePartitions << " active rows of "
         << rowCount << endl;

    for (auto [i, w, n]: activePartitions) {
        cerr << i << ": " << w.count() << " " << jsonEncodeStr(w) << endl;
    }

    uint32_t numToKeep = activePartitions.size();

    uint32_t levelsNeeded = maxDepth - depth;
    cerr << "need " << levelsNeeded << " more levels" << endl;

    uint32_t maxNumToKeep = 1 << (maxIterations / 2);
    if (levelsNeeded < maxIterations) {
        maxNumToKeep = std::max(maxNumToKeep, uint32_t(1 << (maxIterations - levelsNeeded)));
    }

    // We need to be able to fill at least half of the levels with new splits
    if (numToKeep > maxNumToKeep) {
        numToKeep = maxNumToKeep;
    }

    size_t numRowsToKeep = 0;
    size_t numRowsToProcess = 0;
    for (uint32_t i = 0;  i < activePartitions.size();  ++i) {
        auto & [idx, w, n] = activePartitions[i];
        if (i < numToKeep) {
            numRowsToKeep += w.count();
        }
        else {
            numRowsToProcess += w.count();
        }
    }

    cerr << "keeping " << numToKeep << " of " << activePartitions.size() << " partitions with "
         << numRowsToKeep << " rows kept and " << numRowsToProcess << " rows to remove" << endl;
#endif

#if 0
    struct ToProcessPartition {
        uint32_t rowsOffset;      // Where we start in the toProcessRows
        uint32_t numRows;         // Where we finish in the toProcessRows
        uint32_t numRowsDone = 0; // How many rows have we filled?
        uint32_t partitionNumber; // Which partition number we get our buckets from
        PartitionIndex index;     // Partition index of the root
    };

    std::vector<ToProcessPartition> partitionsToProcess;
    std::vector<uint32_t> partitionToToProcessIndex(numPartitionsAtDepth, -1);

    size_t rowsOffset = 0;
    for (uint32_t i = numToKeep;  i < activePartitions.size();  ++i) {
        auto & [idx, w, n] = activePartitions[i];
        ToProcessPartition toProcess;
        toProcess.rowsOffset = rowsOffset;
        toProcess.numRows = activePartitions.size();
        toProcess.partitionNumber = n;
        toProcess.index = idx;

        partitionToToProcessIndex[n] = partitionsToProcess.size();
        partitionsToProcess.push_back(toProcess);
        rowsOffset += activePartitions.size();
    }

    // Now extract the rows that belong to each of our partitions to process
    std::vector<uint32_t> toProcessRows(rowsOffset);  // row numbers

    for (uint32_t r = 0;  r < numRows;  ++r) {
        auto partition = partitions[r];
        auto toProcessIndex = partitionToToProcessIndex[partition];
        if (toProcessIndex == (uint32_t)-1)
            continue;  // not in any partition
        ToProcessPartition & toProcessEntry = partitionsToProcess[toProcessIndex];
        uint32_t rowIndex = toProcessEntry.rowsOffset + toProcessEntry.numRowsDone++;
        toProcessRows[rowIndex] = r;
    }
    
    // Now we can process each of our small partitions
    auto processSmallPartition = [&] (ToProcessPartition & partition)
    {
        // 1.  Calculate buckets

        // 2.  Find split


    };

    for (size_t i = 0;  i < partitionsToProcess.size();  ++i) {
        processSmallPartition(partitionsToProcess[i]);
    }
#endif

#if 0
    cerr << "  submit    queue    start      end  elapsed name" << endl;
    
    auto first = allEvents.at(0).second.getProfilingInfo();
    for (auto & e: allEvents) {
        std::string name = e.first;
        const std::shared_ptr<ComputeEvent> & ev = e.second;
        
        auto info = ev.getProfilingInfo() - first.queued;

        auto ms = [&] (int64_t ns) -> double
            {
                return ns / 1000000.0;
            };
        
        cerr << format("%8.3f %8.3f %8.3f %8.3f %8.3f ",
                       ms(info.queued), ms(info.submit), ms(info.start),
                       ms(info.end),
                       ms(info.end - info.start))
             << name << endl;
    }
#endif

    std::map<PartitionIndex, PartitionSplit> allSplits;

    for (size_t i = 1;  i < numPartitionsAtDepth;  ++i) {
        break;
        cerr << "PARTITION " << i << " " << PartitionIndex(i) << endl;
        cerr << jsonEncode(allPartitionSplits[i]) << endl;
    }

    std::map<PartitionIndex, ML::Tree::Ptr> leaves;

    std::set<int> donePositions;

    std::function<void (PartitionIndex, int)> extractSplits = [&] (PartitionIndex index, int position)
    {
        donePositions.insert(position);
        PartitionIndex leftIndex = index.leftChild();
        PartitionIndex rightIndex = index.rightChild();

        //cerr << "position = " << position << " numPartitionsAtDepth = " << numPartitionsAtDepth << endl;

        auto & split = allPartitionSplits[position];

        //cerr << "  split " << split.index << " = " << jsonEncodeStr(split) << endl;

        ExcAssertEqual(index, split.index);

        int leftPosition = leftIndex.index;
        int rightPosition = rightIndex.index;

        allSplits[index] = split;

        if (split.left.count() > 0 && split.right.count() > 0) {

            //cerr << "  testing left " << leftIndex << " with position " << leftPosition << " of " << numPartitionsAtDepth << endl;
            if (leftPosition < numPartitionsAtDepth) {
                auto & lsplit = allPartitionSplits[leftPosition];
                if (lsplit.valid()) {
                    //cerr << "  has split " << jsonEncodeStr(lsplit) << endl;
                    ExcAssertEqual(lsplit.left.count() + lsplit.right.count(), split.left.count());
                }
            }
            if (leftPosition >= numPartitionsAtDepth || !allPartitionSplits[leftPosition].valid()) {
                //cerr << "    not valid; leaf" << endl;
                leaves[leftIndex] = getLeaf(tree, split.left);
            }
            else {
                //cerr << "    valid; recurse" << endl;
                extractSplits(leftIndex, leftPosition);
            }

            //cerr << "  testing right " << rightIndex << " with position " << rightPosition << " of " << numPartitionsAtDepth << endl;
            if (rightPosition < numPartitionsAtDepth) {
                auto & rsplit = allPartitionSplits[rightPosition];
                if (rsplit.valid()) {
                    //cerr << "  has split " << jsonEncodeStr(rsplit) << endl;
                    ExcAssertEqual(rsplit.left.count() + rsplit.right.count(), split.right.count());
                }
            }
            if (rightPosition >= numPartitionsAtDepth || !allPartitionSplits[rightPosition].valid()) {
                leaves[rightIndex] = getLeaf(tree, split.right);
            }
            else {
                extractSplits(rightIndex, rightPosition);
            }
        }
        else {
            leaves[index] = getLeaf(tree, split.left + split.right);
        }
    };

    extractSplits(PartitionIndex::root(), 1 /* index */);

    for (size_t i = 1;  i < numPartitionsAtDepth;  ++i) {
        if (allPartitionSplits[i].valid() && !donePositions.count(i)) {
            cerr << "ERROR: valid split " << i << " was not extracted" << endl;
        }
    }

    
    std::vector<PartitionIndex> indexes(numPartitionsAtDepth);
    for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
        indexes[i] = PartitionIndex(i + numPartitionsAtDepth);
        ExcAssertEqual(indexes[i].depth(), depth);
    }

    //throw Exception("TODO allSplits");
    
    //for (int i = 0;  i < numIterations;  ++i) {
    //    depthSplits.emplace_back
    //        (mappedAllPartitionSplitsCast.get() + (1 << i),
    //         mappedAllPartitionSplitsCast.get() + (2 << i));
    //}
    
    cerr << "got " << allSplits.size() << " splits" << endl;


    std::vector<std::vector<W>> buckets;  // TODO: stop double copying...
    for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
        const W * partitionBuckets = bucketsUnrolled.data() + numActiveBuckets * i;
        buckets.emplace_back(partitionBuckets, partitionBuckets + numActiveBuckets);
    }

    Date beforeSplitAndRecurse = Date::now();

    std::map<PartitionIndex, ML::Tree::Ptr> newLeaves
        = splitAndRecursePartitioned(depth, maxDepth, tree, serializer,
                                     std::move(buckets), bucketNumbers,
                                     features, activeFeatures,
                                     decodedRows,
                                     partitions, wAll, indexes, fs,
                                     bucketMemory);

#if 0
    auto printTree = [&fs] (const ML::Tree::Ptr & ptr) -> std::string
    {
        std::string result;
        if (ptr.isNode()) {
            result += "node ";
            ML::Tree::Node * node = ptr.node();
            result += node->split.print(fs);
        }
        else if (ptr.isLeaf()) {
            result += "leaf ";
        }
        result += " pred " + jsonEncodeStr(ptr.pred());
        return result;
    };
#endif

    for (auto & [index, ptr]: newLeaves) {
        //cerr << "got new leaf: " << index << " -> " << printTree(ptr) << endl;
        leaves[index] = ptr;
    }

    Date afterSplitAndRecurse = Date::now();

    // Finally, extract a tree from the splits we've been accumulating
    // and our leaves.
    auto result = extractTree(0, maxDepth,
                              tree, PartitionIndex::root(),
                              allSplits, leaves, features, fs);

    Date afterExtract = Date::now();

    cerr << "finished train: finishing tree took " << afterExtract.secondsSince(beforeMapping) * 1000
         << "ms ("
          << beforeSplitAndRecurse.secondsSince(beforeMapping) * 1000 << "ms in mapping and "
          << afterSplitAndRecurse.secondsSince(beforeSplitAndRecurse) * 1000 << "ms in split and recurse)" << endl;

    return result;
}

EnvOption<bool> RF_USE_OPENCL("RF_USE_OPENCL", 1);

ML::Tree::Ptr
trainPartitionedEndToEnd(int depth, int maxDepth,
                         ML::Tree & tree,
                         MappedSerializer & serializer,
                         const Rows & rows,
                         const std::span<const Feature> & features,
                         FrozenMemoryRegionT<uint32_t> bucketMemory,
                         const DatasetFeatureSpace & fs)
{
#if OPENCL_ENABLED
    if (RF_USE_OPENCL) {
        return trainPartitionedEndToEndOpenCL(depth, maxDepth, tree, serializer,
                                              rows, features, bucketMemory, fs);
    }
#endif
    return trainPartitionedEndToEndKernel(depth, maxDepth, tree, serializer,
                                          rows, features, bucketMemory, fs,
                                          ComputeDevice::CPU);

    return trainPartitionedEndToEndCpu(depth, maxDepth, tree, serializer,
                                       rows, features, bucketMemory, fs);

}


} // namespace RF
} // namespace MLDB
