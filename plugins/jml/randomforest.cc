/** randomforest.cc                                             -*- C++ -*-
    Mathieu Marquis Bolduc, 11 Mars 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/

#include "randomforest.h"
#include "randomforest_kernels.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/map_description.h"
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
#include "mldb/arch/ansi.h"
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
        data.rows.weightEncoder.weightFormatTable = serializer.freeze(mutableWeightFormatTable);
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

    data.bucketMemory = serializer.freeze(mutableBucketMemory);

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

    sides[0].bucketMemory = serializer.freeze(mutableBucketMemory[0]);
    sides[1].bucketMemory = serializer.freeze(mutableBucketMemory[1]);
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

extern EnvOption<bool> DEBUG_RF_KERNELS;

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

    if (trainingScheme == PARTITIONED && !DEBUG_RF_KERNELS) {
        return trainPartitioned(depth, maxDepth, tree, serializer);
    }
    else if (trainingScheme == BOTH_AND_COMPARE || DEBUG_RF_KERNELS) {
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

struct FeatureSamplingTrainerKernel {

    void init(int maxDepth,
              MappedSerializer & serializer,
              const Rows & rows,
              const std::span<const Feature> & features,
              FrozenMemoryRegionT<uint32_t> bucketMemory,
              const DatasetFeatureSpace & fs,
              const ComputeDevice & device);

    ML::Tree trainPartitioned(const std::vector<int> & featuresActive);

    // Packed feature buckets (used for recursive calls)
    FrozenMemoryRegionT<uint32_t> bucketMemory;

    // Feature space (used for recursive calls)
    const DatasetFeatureSpace * fs = nullptr;

    // Serializer (used for recursive calls)
    MappedSerializer * serializer = nullptr;

    // Shared OpenCL runtime
    std::shared_ptr<ComputeRuntime> runtime;

    // Shared OpenCL context
    std::shared_ptr<ComputeContext> context;

    // Queue for initialization
    std::shared_ptr<ComputeQueue> queue;

    // How many partitions will we have at our widest?
    uint32_t maxPartitionCount;

    // Do we debug things?
    bool debugKernelOutput = DEBUG_RF_KERNELS;

    // Max iterations we do before recursing
    static constexpr uint32_t maxIterations = 16;

    // Rows (common) which we train on
    Rows rows;

    // Full set of features we're training on
    std::span<const Feature> features;

    // How many rows in this partition?
    uint32_t numRows;

    // How many features?
    uint32_t nf;

    // Maximum depth we want to recurse to?
    uint32_t maxDepth;

    // How many iterations can we run for?  This may be reduced if the
    // memory is not available to run the full width
    uint32_t numIterations;

    // Kernels we use to do our work
    std::shared_ptr<ComputeKernel> testFeatureKernel;
    std::shared_ptr<ComputeKernel> getPartitionSplitsKernel;
    std::shared_ptr<ComputeKernel> bestPartitionSplitKernel;
    std::shared_ptr<ComputeKernel> clearBucketsKernel;
    std::shared_ptr<ComputeKernel> updatePartitionNumbersKernel;
    std::shared_ptr<ComputeKernel> updateBucketsKernel;
    std::shared_ptr<ComputeKernel> fixupBucketsKernel;

    // This one contains an expanded version of the row data, with one float
    // per row rather than bit-compressed.  It's expanded on the device so that
    // the compressed version can be passed over the PCIe bus and not the
    // expanded version.
    MemoryArrayHandleT<const float> expandedRowData;

    // Event to wait on for expandedRowData to be ready
    std::shared_ptr<ComputeEvent> runDecodeRows;

    // Data we map in-place so we have to ensure it sticks around
    std::vector<uint32_t> bucketMemoryOffsets;   ///< Offset in the buckets memory blob per feature [nf + 1]
    std::vector<uint32_t> bucketEntryBits;       ///< How many bits per bucket [nf]
    std::vector<uint32_t> featureIsOrdinal;      ///< For each feature: is it ordinal (1) vs categorical(0) [nf]

    // Some memory objects to be used by the kernel.
    ComputePromiseT<MemoryArrayHandleT<const uint32_t>> bucketDataPromise;
    ComputePromiseT<MemoryArrayHandleT<const uint64_t>> rowDataPromise;
    ComputePromiseT<MemoryArrayHandleT<const float>>    weightDataPromise;
    ComputePromiseT<MemoryArrayHandleT<uint32_t>> deviceBucketEntryBits;
    ComputePromiseT<MemoryArrayHandleT<uint32_t>> deviceBucketDataOffsets;
    ComputePromiseT<MemoryArrayHandleT<uint32_t>> deviceFeatureIsOrdinal;

    // If we're debugging our kernels, this is where we keep the expanded rows
    std::vector<float> debugExpandedRowsCpu;
};

void
FeatureSamplingTrainerKernel::
init(int maxDepth,
    MappedSerializer & serializer,
    const Rows & rows,
    const std::span<const Feature> & features,
    FrozenMemoryRegionT<uint32_t> bucketMemory,
    const DatasetFeatureSpace & fs,
    const ComputeDevice & device)
{
    this->rows = rows;
    this->features = features;
    this->maxDepth = maxDepth;
    this->bucketMemory = bucketMemory;
    this->fs = &fs;
    this->serializer = &serializer;

    const bool debugKernelOutput = DEBUG_RF_KERNELS;
    constexpr uint32_t maxIterations = 16;

    // First, figure out the memory requirements.  This means sizing all
    // kinds of things so that we can make our allocations statically.

    // How many rows in this partition?
    numRows = rows.rowCount();

    // How many features?
    nf = features.size();
    
    // How many iterations can we run for?  This may be reduced if the
    // memory is not available to run the full width
    numIterations = std::min<uint32_t>(maxIterations, maxDepth);

    // How many partitions will we have at our widest?
    maxPartitionCount = 1 << numIterations;
    
    // Maximum number of buckets
    uint32_t maxBuckets = 0;
    
    uint32_t lastBucketDataOffset = 0;
    
    // Total number of buckets (active plus inactive)
    size_t numBuckets = 0;

    // For each feature, we set up a table of offsets which will allow our OpenCL kernel
    // to know where in a flat buffer of memory the data for that feature resides.
    for (int i = 0;  i < nf;  ++i) {
        const BucketList & buckets = features[i].buckets;

        bucketEntryBits.push_back(buckets.entryBits);

        featureIsOrdinal.push_back(features[i].ordinal);
        
        //cerr << "feature " << i << " buckets " << features[i].buckets.numBuckets << endl;
        ExcAssertGreaterEqual((void *)buckets.storage.data(),
                                (void *)bucketMemory.data());
    
        uint32_t offset = buckets.storage.data() - bucketMemory.data();

        bucketMemoryOffsets.push_back(offset);
        lastBucketDataOffset = offset + bucketMemory.length();
        
        numBuckets += features[i].buckets.numBuckets;
        maxBuckets = std::max<size_t>(maxBuckets,
                                        features[i].buckets.numBuckets);
    }

    bucketMemoryOffsets.push_back(lastBucketDataOffset);

    ExcAssertEqual(bucketMemoryOffsets.size(), nf + 1);
    ExcAssertEqual(bucketEntryBits.size(), nf);

    //Date before = Date::now();

    runtime = ComputeRuntime::getRuntimeForDevice(device);

    context = runtime->getContext(array{device});

    queue = context->getQueue();

    // First, we need to send over the rows, as the very first thing to
    // be done is to expand them.
    rowDataPromise
        = context->transferToDeviceImmutable("copyRowData", rows.rowData);

    // Same for our weight data
    weightDataPromise
        = context->transferToDeviceImmutable("copyWeightData", rows.weightEncoder.weightFormatTable);

    // We transfer the bucket data as early as possible, as it's one of the
    // longest things to transfer
    bucketDataPromise
        = context->transferToDeviceImmutable("copyBucketData", bucketMemory);

    deviceFeatureIsOrdinal
        = context->manageMemoryRegion("featuresIsOrdinal", featureIsOrdinal);
    

    // Our first kernel expands the data.  It's pretty simple, as a warm
    // up for the rest.
    auto doNothingKernel = context->getKernel("doNothing");
    auto decodeRowsKernel = context->getKernel("decodeRows");
    testFeatureKernel = context->getKernel("testFeature");
    getPartitionSplitsKernel = context->getKernel("getPartitionSplits");
    bestPartitionSplitKernel = context->getKernel("bestPartitionSplit");
    clearBucketsKernel = context->getKernel("clearBuckets");
    updatePartitionNumbersKernel = context->getKernel("updatePartitionNumbers");
    updateBucketsKernel = context->getKernel("updateBuckets");
    fixupBucketsKernel = context->getKernel("fixupBuckets");

    std::shared_ptr<ComputeEvent> runDoNothing;
    {
        auto boundDoNothingKernel = doNothingKernel->bind();
        runDoNothing = queue->launch("loadKernels", boundDoNothingKernel, {}, {});
    }
    
    // We take a non-const version here for this call
    auto expandedRowData = context->allocUninitializedArray<float>("expandedRowData", numRows).get();

    // Our first kernel expands the data.  It's pretty simple, as a warm
    // up for the rest.
    {
        auto boundKernel = decodeRowsKernel
            ->bind(  "rowData",          rowDataPromise,
                    "rowDataLength",    (uint32_t)rows.rowData.length(),
                    "weightBits",       (uint16_t)rows.weightEncoder.weightBits,
                    "exampleNumBits",   (uint16_t)rows.exampleNumBits,
                    "numRows",          (uint32_t)numRows,
                    "weightFormat",     rows.weightEncoder.weightFormat,
                    "weightMultiplier", rows.weightEncoder.weightMultiplier,
                    "weightData",       weightDataPromise,
                    "decodedRowsOut",   expandedRowData);

        runDecodeRows = queue->launch("decode rows", boundKernel, { (uint32_t)numRows },
                            { rowDataPromise.event(), weightDataPromise.event() });
    }

    // Assign to the const version here
    this->expandedRowData = expandedRowData;

    if (debugKernelOutput) {
        // Verify that the kernel version gives the same results as the non-kernel version
        debugExpandedRowsCpu = decodeRows(rows);
        auto frozenExpandedRowsDevice = context->transferToHostSync("debugExpandedRows", expandedRowData);
        auto expandedRowsDevice = frozenExpandedRowsDevice.getConstSpan();
        ExcAssertEqual(expandedRowsDevice.size(), debugExpandedRowsCpu.size());
        bool different = false;
        
        for (size_t i = 0;  i < numRows;  ++i) {
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
    deviceBucketEntryBits = context->manageMemoryRegion("bucketEntryBits", bucketEntryBits);
    deviceBucketDataOffsets = context->manageMemoryRegion("bucketMemoryOffsets", bucketMemoryOffsets);
}

ML::Tree
FeatureSamplingTrainerKernel::
trainPartitioned(const std::vector<int> & activeFeatures)
{
    ML::Tree tree;

    uint32_t depth = 0;

    // Maximum number of buckets
    uint32_t maxBuckets = 0;
    
    std::vector<uint32_t> bucketNumbers(1, 0);   ///< Range of bucket numbers for feature [nf + 1]
    std::vector<uint32_t> featuresActive(nf, false); ///< For each feature: which are active? [nf]
    
    std::set<int> activeFeatureSet{activeFeatures.begin(), activeFeatures.end()};

    uint32_t numActiveFeatures = activeFeatures.size();
    uint32_t numActiveBuckets = 0;

    // For each feature, we set up a table of offsets which will allow our OpenCL kernel
    // to know where in a flat buffer of memory the data for that feature resides.
    for (int i = 0;  i < nf;  ++i) {
        if (activeFeatureSet.count(i)) {
            featuresActive[i] = true;
            numActiveBuckets += features[i].buckets.numBuckets;
            maxBuckets = std::max<size_t>(maxBuckets,
                                          features[i].buckets.numBuckets);
        }

        bucketNumbers.push_back(numActiveBuckets);
    }

    ExcAssertEqual(bucketNumbers.size(), nf + 1);
    ExcAssertEqual(featuresActive.size(), nf);

    ExcAssertGreater(numActiveFeatures, 0);

    auto deviceBucketNumbers = context->manageMemoryRegion("bucketNumbers", bucketNumbers);

    // How much memory to accumulate W over all features per partition?
    size_t bytesPerPartition = sizeof(W) * numActiveBuckets;

    // How much memory to accumulate W over all features over the maximum
    // number of partitions?
    size_t bytesForAllPartitions = bytesPerPartition * maxPartitionCount;

    cerr << "numActiveBuckets = " << numActiveBuckets << endl;
    cerr << "sizeof(W) = " << sizeof(W) << endl;
    cerr << "bytesForAllPartitions = " << bytesForAllPartitions * 0.000001
         << "mb" << endl;
    
    Date before = Date::now();

    // Each partition has its own queue
    auto queue = context->getQueue();

    // Which of our features do we need to consider?  This allows us to
    // avoid sizing things too large for the number of features that are
    // actually active.
    auto deviceFeaturesActive = context->manageMemoryRegion("featuresActive", featuresActive);

    // Our wAll array contains the sum of all of the W buckets across
    // each partition.  We allocate a single array at the start and just
    // use more and more each iteration.
    auto deviceWAllPool = context->allocUninitializedArray<W>("wAll", maxPartitionCount).get();

    // The first one is initialized by the input wAll
    auto copyWAllPromise = queue->enqueueFillArray("initialize wAll[0]", deviceWAllPool, rows.wAll, 0 /* offset */, 1 /* size */);

    // The rest are initialized to zero
    auto initializeWAllPromise = queue->enqueueFillArray("zero rest of wAll", deviceWAllPool, W(), 1 /* offset */);

    // Our W buckets live here, per partition.  We never need to see it on
    // the host, so we allow it to be initialized and live on the device.
    // Note that we only use the beginning 2 at the start, and we
    // double the amount of what we use until we're using the whole lot
    // on the last iteration
    MemoryArrayHandleT<W> devicePartitionBucketPool
        = context->allocUninitializedArray<W>("partitionBucketPool", maxPartitionCount * numActiveBuckets).get();

    auto firstPartitionBuckets
        = context->getArraySlice(devicePartitionBucketPool, "firstPartitionBuckets", 0, numActiveBuckets);

    // Before we use this, it needs to be zero-filled (only the first
    // set for a single partition)
    auto fillFirstBuckets
        = queue->enqueueFillArray("fill firstPartitionBuckets", firstPartitionBuckets, W());
    //auto fillFirstBuckets
    //    = queue->enqueueFillArray("fill firstPartitionBuckets", devicePartitionBucketPool, W());

    std::shared_ptr<ComputeEvent> runTestFeatureKernel;
    {
        auto boundTestFeatureKernel = testFeatureKernel
            ->bind( "decodedRows",                      expandedRowData,
                    "numRows",                          (uint32_t)numRows,
                    "bucketData",                       bucketDataPromise,
                    "bucketDataOffsets",                deviceBucketDataOffsets,
                    "bucketNumbers",                    deviceBucketNumbers,
                    "bucketEntryBits",                  deviceBucketEntryBits,
                    "featuresActive",                   deviceFeaturesActive,
                    "partitionBuckets",                 firstPartitionBuckets);

        runTestFeatureKernel = queue->launch("testFeature",
                            boundTestFeatureKernel,
                        { nf, numRows },
                        { bucketDataPromise.event(), fillFirstBuckets.event(), runDecodeRows });
    }

    if (debugKernelOutput) {
        // Get that data back (by mapping), and verify it against the
        // CPU-calcualted version.
        
        auto frozenPartitionBuckets
             = context->transferToHostSync("debug transfer partitionBuckets", firstPartitionBuckets);
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
                                 numRows,
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
    auto devicePartitions
        = context->allocZeroInitializedArray<RowPartitionInfo>("partitions", numRows).get();

    // Array to cache transfer directions to avoid re-calculating
    auto directions
        = context->allocUninitializedArray<uint8_t>("directions", numRows).get();

    // How many partitions at the current depth?
    unsigned numPartitionsAtDepth = 1;

    // Which event represents that the previous iteration of partitions
    // are available?
    std::shared_ptr<ComputeEvent> previousIteration = runTestFeatureKernel;

    // Event list for all of the buckets
    std::vector<std::shared_ptr<ComputeEvent>> deviceDepthSplitsEvents;

    // Each of the numIterations partitions has double the number of buckets,
    // so the total number is 2^(numIterations + 1) - 1.
    MemoryArrayHandleT<PartitionSplit> deviceAllPartitionSplitsPool
        = context->allocUninitializedArray<PartitionSplit>("allPartitionSplits", 2 << numIterations).get();
    
    // Pre-allocate partition buckets for the widest bucket
    // We need to store partition splits for each partition and each
    // feature.  Get the memory.  It doesn't need to be initialized.
    // Layout is partition-major.
    MemoryArrayHandleT<PartitionSplit> deviceFeaturePartitionSplitsPool
        = context->allocUninitializedArray<PartitionSplit>("featurePartitionSplits", (1 << numIterations) * nf).get();

    // DEBUG ONLY, stops spurious differences between kernels
    if (debugKernelOutput) {
        queue->enqueueFillArray("debug clear partition splits", deviceFeaturePartitionSplitsPool, PartitionSplit());
        queue->enqueueFillArray("debug clear allPartitionSplits", deviceAllPartitionSplitsPool, PartitionSplit());
    }

    Date startDepth = Date::now();

    // We go down level by level
    for (int myDepth = 0;
         myDepth < numIterations && depth < maxDepth;
         ++depth, ++myDepth, numPartitionsAtDepth *= 2) {

        cerr << endl << ansi::bright_blue << "depth = " << depth << " myDepth = " << myDepth << " numPartitions " << numPartitionsAtDepth << ansi::reset << endl;

        // Run a kernel to find the new split point for each partition,
        // best feature and kernel

        // Now we have initialized our data, we can get to running the
        // kernel.  This kernel is dimensioned on bucket number,
        // feature number and partition number (ie, a 3d invocation).

        // Take slices of the larger data structures we allocated for use at the current depth

        auto depthPartitionBuckets
            = context->getArraySlice(devicePartitionBucketPool,
                                     "partitionBuckets depth " + std::to_string(myDepth),
                                     0, numActiveBuckets * numPartitionsAtDepth);

        auto depthWAll
            = context->getArraySlice(deviceWAllPool,
                                     "wAll depth " + std::to_string(myDepth),
                                     0, numPartitionsAtDepth);

        auto depthFeaturePartitionSplits
            = context->getArraySlice(deviceFeaturePartitionSplitsPool,
                                     "featurePartitionSplits depth " + std::to_string(myDepth),
                                     0, nf * numPartitionsAtDepth);

        auto depthAllPartitionSplits
            = context->getArraySlice(deviceAllPartitionSplitsPool,
                                     "allPartitionSplits depth " + std::to_string(myDepth),
                                     0, 2 * numPartitionsAtDepth);


        std::shared_ptr<ComputeEvent> runPartitionSplitsKernel;
        {
            auto boundGetPartitionSplitsKernel = getPartitionSplitsKernel
                ->bind("totalBuckets",                   (uint32_t)numActiveBuckets,
                    "numPartitions",                  (uint32_t)numPartitionsAtDepth,
                    "bucketNumbers",                  deviceBucketNumbers,
                    "featuresActive",                 deviceFeaturesActive,
                    "featureIsOrdinal",               deviceFeatureIsOrdinal,
                    "buckets",                        depthPartitionBuckets,
                    "wAll",                           depthWAll,
                    "featurePartitionSplitsOut",      depthFeaturePartitionSplits);

            //cerr << endl << endl << " depth " << depth << " numPartitions "
            //     << numPartitionsAtDepth << " buckets "
            //     << numPartitionsAtDepth * numActiveBuckets << endl;

            runPartitionSplitsKernel
                = queue->launch("getPartitionSplits",
                                boundGetPartitionSplitsKernel,
                            { 64 /* worker ID */, nf, numPartitionsAtDepth },
                            { previousIteration, copyWAllPromise.event(), initializeWAllPromise.event()});
        }

        // Now we have the best split for each feature for each partition,
        // find the best one per partition and finally record it.

        // What is our offset into partitionSplits?  In other words, where do
        // the partitions for this iteration start?  By skipping the first
        // bucket, this becomes trivially numPartitionsAtDepth: 1, 2, 4, 8, ...
        // It's not technically necessary to pass it since it's one of the
        // launch parameters, but this way is more clear.
        uint32_t partitionSplitsOffset = numPartitionsAtDepth;
        
        std::shared_ptr<ComputeEvent> runBestPartitionSplitKernel;
        {
            auto boundBestPartitionSplitKernel = bestPartitionSplitKernel
                ->bind("numFeatures",            (uint32_t)nf,
                    "featuresActive",         deviceFeaturesActive,
                    "featurePartitionSplits", depthFeaturePartitionSplits,
                    "allPartitionSplitsOut",  depthAllPartitionSplits,
                    "partitionSplitsOffset",  partitionSplitsOffset);

            runBestPartitionSplitKernel
                = queue->launch("bestPartitionSplit",
                                boundBestPartitionSplitKernel,
                                { numPartitionsAtDepth },
                                { runPartitionSplitsKernel });
        }

        // These are parallel CPU data structures for the on-device ones,
        // into which we copy the input data required to re-run the
        // computation on the CPU so we can verify the output of the device
        // algorithm.
        std::vector<PartitionSplit> debugPartitionSplitsCpu;
        MutableMemoryRegionT<W> mappedBuckets;
        std::vector<W> debugBucketsCpu;
        std::vector<W> debugWAllCpu;
        std::vector<RowPartitionInfo> debugPartitionsCpu;
        std::set<int> okayDifferentPartitions;

        if (debugKernelOutput) {
            // Map back the device partition splits (note that we only use those between
            // numPartitionsAtDepth and 2 * numPartitionsAtDepth)
            auto mappedPartitionSplits = context->transferToHostSync("debug partitionSplits", depthAllPartitionSplits);
            auto partitionSplitsDevice
                 = mappedPartitionSplits.getConstSpan(numPartitionsAtDepth, numPartitionsAtDepth);

            // Map back the device partition numbers
            auto mappedPartitions = context->transferToHostSync("debug partitions", devicePartitions);
            auto partitionsDevice = mappedPartitions.getConstSpan();

            debugPartitionsCpu = { partitionsDevice.begin(), partitionsDevice.end() };
            
            // Construct the CPU version of buckets (and keep it around)
            auto mappedBuckets = context->transferToHostSync("debug partitionBuckets", depthPartitionBuckets);
            auto mappedBucketsSpan = mappedBuckets.getConstSpan();
            debugBucketsCpu = { mappedBucketsSpan.begin(), mappedBucketsSpan.end() };
            debugBucketsCpu.resize(debugBucketsCpu.size() * 2);

            // Get back the CPU version of wAll
            auto mappedWAll = context->transferToHostSync("debug wAll", depthWAll);
            auto wAllDevice = mappedWAll.getConstSpan();
            debugWAllCpu = { wAllDevice.begin(),
                             wAllDevice.begin() + numPartitionsAtDepth };

            // Verify preconditions
            std::vector<size_t> partitionCounts(numPartitionsAtDepth, 0);
            std::vector<W> testW(numPartitionsAtDepth);
            for (size_t i = 0;  i < numRows;  ++i) {
                uint16_t p = partitionsDevice[i];
                if (p >= numPartitionsAtDepth)
                    continue;
                partitionCounts[p] += 1;
                float weight = fabs(debugExpandedRowsCpu[i]);
                bool label = debugExpandedRowsCpu[i] < 0;

                testW[p].add(label, weight);
            }

            for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {

                if (partitionCounts[i] != wAllDevice[i].count()) {
                    cerr << "partition " << i << ": partition count " << partitionCounts[i]
                         << " wAll count " << wAllDevice[i].count() << endl;
                    ExcAssertEqual(partitionCounts[i], wAllDevice[i].count());
                }

                if (wAllDevice[i] != testW[i]) {
                    cerr << "partition " << i << ": wAll " << jsonEncodeStr(wAllDevice[i])
                         << " recalc " << jsonEncodeStr(testW[i]) << endl;
                    ExcAssert(wAllDevice[i] == testW[i]);
                }

                if (partitionCounts[i] == 0)
                    continue;

                if (wAllDevice[i] != testW[i]) {
                    cerr << "partition " << i << ": wAll " << jsonEncodeStr(wAllDevice[i])
                         << " recalc " << jsonEncodeStr(testW[i]) << endl;
                    ExcAssert(wAllDevice[i] == testW[i]);
                }

                // Verify that buckets sum to wAll for each feature
                for (uint32_t f = 0;  f < nf;  ++f) {
                    if (!featuresActive[f])
                        continue;

                    uint32_t bucketStart = bucketNumbers[f];
                    uint32_t bucketEnd = bucketNumbers[f + 1];

                    W sumBuckets;
                    for (size_t b = bucketStart;  b < bucketEnd;  ++b) {
                        sumBuckets += mappedBucketsSpan[i * numActiveBuckets + b];
                    }

                    if (sumBuckets != testW[i]) {
                        cerr << "partition " << i << " feature " << f << ": sumBuckets " << jsonEncodeStr(sumBuckets)
                            << " recalc " << jsonEncodeStr(testW[i]) << endl;
                        cerr << "bucketStart = " << bucketStart << " bucketEnd = " << bucketEnd << endl;
                        ExcAssert(sumBuckets == testW[i]);
                    }
                }
            }

            std::vector<PartitionIndex> indexes;
            
            for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
                indexes.push_back(numPartitionsAtDepth + i);
            }

            // Run the CPU version... first getting the data in place
            debugPartitionSplitsCpu
                = getPartitionSplits(debugBucketsCpu,
                                     numActiveBuckets,
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

        auto nextDepthPartitionBuckets
            = context->getArraySlice(devicePartitionBucketPool,
                                     "partitionBuckets depth " + std::to_string(myDepth + 1),
                                     0, numActiveBuckets * numPartitionsAtDepth * 2);

        auto nextDepthWAll
            = context->getArraySlice(deviceWAllPool,
                                     "wAll depth " + std::to_string(myDepth + 1),
                                     0, numPartitionsAtDepth * 2);

        // First we clear everything on the right side, ready to accumulate
        // the new buckets there.

        std::shared_ptr<ComputeEvent> runClearBucketsKernel;
        {
            auto boundClearBucketsKernel = clearBucketsKernel
                ->bind("bucketsOut",             nextDepthPartitionBuckets,
                    "wAllOut",                nextDepthWAll,
                    "allPartitionSplits",     depthAllPartitionSplits,
                    "numActiveBuckets",       (uint32_t)numActiveBuckets,
                    "partitionSplitsOffset",  (uint32_t)numPartitionsAtDepth);

            runClearBucketsKernel
                = queue->launch("clear buckets",
                                boundClearBucketsKernel,
                                { numPartitionsAtDepth, numActiveBuckets },
                                { runPartitionSplitsKernel });
        }

        std::shared_ptr<ComputeEvent> runUpdatePartitionNumbersKernel;
        {
            // While we're doint that, we can also calculate our new
            // partition numbers (for each row)
            auto boundUpdatePartitionNumbersKernel = updatePartitionNumbersKernel
                ->bind("partitionSplitsOffset",          (uint32_t)numPartitionsAtDepth,  // rightOffset
                    "partitions",                     devicePartitions,
                    "directions",                     directions,
                    "numRows",                        numRows,
                    "allPartitionSplits",             depthAllPartitionSplits,
                    "bucketData",                     bucketDataPromise,
                    "bucketDataOffsets",              deviceBucketDataOffsets,
                    "bucketNumbers",                  deviceBucketNumbers,
                    "bucketEntryBits",                deviceBucketEntryBits,
                    "featureIsOrdinal",               deviceFeatureIsOrdinal);

            runUpdatePartitionNumbersKernel
                = queue->launch("update partition numbers",
                                boundUpdatePartitionNumbersKernel, { numRows },
                                { runPartitionSplitsKernel });
        }
        
        // Now the right side buckets are clear, we can transfer the weights
        // for the examples who have changed bucket from the left to the right.

        std::shared_ptr<ComputeEvent> runUpdateBucketsKernel;
        {
            auto boundUpdateBucketsKernel = updateBucketsKernel
                ->bind("partitionSplitsOffset",         (uint32_t)numPartitionsAtDepth,  // rightOffset
                    "numActiveBuckets",               (uint32_t)numActiveBuckets,
                    "partitions",                     devicePartitions,
                    "directions",                     directions,
                    "buckets",                        nextDepthPartitionBuckets,
                    "wAll",                           nextDepthWAll,
                    "decodedRows",                    expandedRowData,
                    "numRows",                        (uint32_t)numRows,
                    "bucketData",                     bucketDataPromise,
                    "bucketDataOffsets",              deviceBucketDataOffsets,
                    "bucketNumbers",                  deviceBucketNumbers,
                    "bucketEntryBits",                deviceBucketEntryBits,
                    "featuresActive",                 deviceFeaturesActive,
                    "featureIsOrdinal",               deviceFeatureIsOrdinal);

            runUpdateBucketsKernel
                = queue->launch("update buckets",
                                boundUpdateBucketsKernel, { numRows, nf + 1 /* +1 is wAll */},
                                { runClearBucketsKernel, runUpdatePartitionNumbersKernel });
        }

        std::shared_ptr<ComputeEvent> runFixupBucketsKernel;
        {
            auto boundFixupBucketsKernel = fixupBucketsKernel
                ->bind("buckets",                    nextDepthPartitionBuckets,
                    "wAll",                       nextDepthWAll,
                    "allPartitionSplits",         depthAllPartitionSplits,
                    "numActiveBuckets",           (uint32_t)numActiveBuckets,
                    "partitionSplitsOffset",      (uint32_t)numPartitionsAtDepth);

            runFixupBucketsKernel
                = queue->launch("fixup buckets",
                                boundFixupBucketsKernel,
                                { numPartitionsAtDepth, numActiveBuckets },
                                { runUpdateBucketsKernel });
        }

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
                          numActiveBuckets,
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
            auto mappedBuckets = context->transferToHostSync("debug partitionBuckets", nextDepthPartitionBuckets);
            auto bucketsDevice = mappedBuckets.getConstSpan();

            for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
                std::span<const W> partitionBuckets = bucketsDevice.subspan(numActiveBuckets * i);
                if (newPartitionNumbers[i].first == -1)
                    continue;  // dead partition, don't verify...
                if (okayDifferentPartitions.count(i))
                    continue;  // is different due to a different split caused by numerical errors
                std::span<const W> cpuBuckets = span{debugBucketsCpu}.subspan(i * numActiveBuckets, numActiveBuckets);

                for (size_t j = 0;  j < numActiveBuckets;  ++j) {
                    if (partitionBuckets[j] != cpuBuckets[j]) {
                        cerr << "part " << i << " bucket " << j
                                << " num " << numActiveBuckets * i + j
                                << " update error: CPU "
                                << jsonEncodeStr(cpuBuckets[j])
                                << " device "
                                << jsonEncodeStr(partitionBuckets[j])
                                << endl;
                        different = true;
                    }
                }
            }

            // 2.  Map back the wAll values and compare against the CPU version
            auto mappedWAll = context->transferToHostSync("debug wAll", nextDepthWAll);
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
            auto mappedPartitions = context->transferToHostSync("debug partitions", devicePartitions);
            auto partitionsDevice = mappedPartitions.getConstSpan();

            int numDifferences = 0;
            std::map<std::pair<int, int>, int> differenceStats;
            for (size_t i = 0;  i < numRows;  ++i) {
                if (partitionsDevice[i] != debugPartitionsCpu[i] && debugPartitionsCpu[i] != RowPartitionInfo::max()) {
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

        //if (true)
        //    queue->finish();
    }

    queue->flush();
    queue->finish();


    // If we're not at the lowest level, partition our data and recurse
    // par partition to create our leaves.
    Date beforeMapping = Date::now();


    //cerr << "kernel wall time is " << Date::now().secondsSince(before) * 1000 << endl;
    //cerr << "numPartitionsAtDepth = " << numPartitionsAtDepth << endl;

    // Get all the data back...
    auto allPartitionSplitsRegion = context->transferToHostSync("partitionSplits to CPU", deviceAllPartitionSplitsPool);
    std::span<const PartitionSplit> allPartitionSplits = allPartitionSplitsRegion.getConstSpan();

    auto bucketsUnrolledRegion = context->transferToHostSync("partitionBuckets to CPU", devicePartitionBucketPool);
    std::span<const W> bucketsUnrolled = bucketsUnrolledRegion.getConstSpan();
    auto partitionsRegion = context->transferToHostSync("partitions to CPU", devicePartitions);
    std::span<const RowPartitionInfo> partitions = partitionsRegion.getConstSpan();
    auto wAllRegion = context->transferToHostSync("wAll to CPU", deviceWAllPool);
    std::span<const W> wAll = wAllRegion.getConstSpan();
    auto decodedRowsRegion = context->transferToHostSync("expandedRowData to CPU", expandedRowData);
    std::span<const float> decodedRows = decodedRowsRegion.getConstSpan();

    //for (auto & row: decodedRows) {
    //    ExcAssertLessEqual(fabs(row), 1);
    //}

    Date beforeSetupRecurse = Date::now();

    std::map<PartitionIndex, PartitionSplit> allSplits;

    //for (size_t i = 1;  i < numPartitionsAtDepth & i < 16;  ++i) {
    //    cerr << "PARTITION " << i << " " << PartitionIndex(i) << endl;
    //    cerr << jsonEncode(allPartitionSplits[i]) << endl;
    //}

    std::map<PartitionIndex, ML::Tree::Ptr> leaves;

    std::set<int> donePositions;

    std::function<void (PartitionIndex, int)> extractSplits = [&] (PartitionIndex index, int position)
    {
        donePositions.insert(position);
        PartitionIndex leftIndex = index.leftChild();
        PartitionIndex rightIndex = index.rightChild();

        //cerr << "position = " << position << " numPartitionsAtDepth = " << numPartitionsAtDepth << endl;

        auto & split = allPartitionSplits[position];

        if (index != split.index) {
            cerr << "index " << index << " split.index " << split.index << " split " << jsonEncodeStr(split) << endl;
        }
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
        if (debugKernelOutput && allPartitionSplits[i].valid() && !donePositions.count(i)) {
            cerr << "ERROR: valid split " << i << " was not extracted" << endl;
        }
    }

    
    std::vector<PartitionIndex> indexes(numPartitionsAtDepth);
    for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
        indexes[i] = PartitionIndex(i + numPartitionsAtDepth);
        ExcAssertEqual(indexes[i].depth(), depth);
    }

    cerr << "got " << allSplits.size() << " splits" << endl;

    Date beforeSplitAndRecurse = Date::now();

    std::map<PartitionIndex, ML::Tree::Ptr> newLeaves
        = splitAndRecursePartitioned(depth, maxDepth, tree, *serializer,
                                     bucketsUnrolled, numActiveBuckets,
                                     bucketNumbers,
                                     features, activeFeatures,
                                     decodedRows,
                                     partitions, wAll, indexes, *fs,
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
                              allSplits, leaves, features, *fs);

    Date afterExtract = Date::now();

    cerr << "finished train: setup took " << startDepth.secondsSince(before) * 1000
         << "ms, depths took " << beforeMapping.secondsSince(startDepth) * 1000
         << "ms, finishing tree took " << afterExtract.secondsSince(beforeMapping) * 1000
         << "ms ("
         << beforeSetupRecurse.secondsSince(beforeMapping) * 1000 << "ms in mapping and "
         << beforeSplitAndRecurse.secondsSince(beforeSetupRecurse) * 1000 << "ms in setup and "
         << afterSplitAndRecurse.secondsSince(beforeSplitAndRecurse) * 1000 << "ms in split and recurse)"
         << " full training took " << afterExtract.secondsSince(before) * 1000 << "ms"
         << endl;

    cerr << jsonEncode(queue->kernelWallTimes) << queue->totalKernelTime << "ms total in kernels" << endl;

    tree.root = result;

    return tree;
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
    if (depth != 0) {
        return trainPartitionedEndToEndCpu(depth, maxDepth, tree, serializer,
                                        rows, features, bucketMemory, fs);
    }

    ComputeDevice device;
    if (DEBUG_RF_KERNELS && RF_USE_OPENCL) {
        device = ComputeDevice::defaultFor(ComputeRuntimeId::MULTI);
    }
    else if (RF_USE_OPENCL) {
        device = ComputeDevice::defaultFor(ComputeRuntimeId::OPENCL);
    }
    else {
        device = ComputeDevice::host();
    }

    cerr << "training partitioned on " << device << endl;

    std::vector<int> featuresActive;
    for (size_t i = 0;  i < features.size();  ++i) {
        if (features[i].active)
            featuresActive.push_back(i);
    }

    FeatureSamplingTrainerKernel trainer;
    trainer.init(maxDepth, serializer, rows, features, bucketMemory, fs, device);
    tree = trainer.trainPartitioned(featuresActive);
    return tree.root;
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

// Train a small forest, with the same rows but a different feature sampling
// Will eventually reuse much of the work in the partitioned case
std::vector<ML::Tree>
PartitionData::
trainMultipleSamplings(int maxDepth, const std::vector<std::vector<int>> & featuresActive,
                        MappedSerializer & serializer,
                        TrainingScheme trainingScheme) const
{
    std::vector<ML::Tree> result(featuresActive.size());

    if (trainingScheme == PARTITIONED) {
        ComputeDevice device;
        if (DEBUG_RF_KERNELS && RF_USE_OPENCL) {
            device = ComputeDevice::defaultFor(ComputeRuntimeId::MULTI);
        }
        else if (RF_USE_OPENCL) {
            device = ComputeDevice::defaultFor(ComputeRuntimeId::OPENCL);
        }
        else {
            device = ComputeDevice::host();
        }

        cerr << "training multiple samplings on " << device << endl;

        FeatureSamplingTrainerKernel trainer;
        trainer.init(maxDepth, serializer, rows, features, bucketMemory, *fs, device);

        auto buildTree = [&] (int i)
        {
            result[i] = trainer.trainPartitioned(featuresActive[i]);
            ExcAssert(result[i].root);
        };

        MLDB::parallelMap(0, featuresActive.size(), buildTree);

        return result;
    }
    else {
        auto buildTree = [&] (int i)
        {
            PartitionData myData = *this;
            for (auto & f: myData.features)
                f.active = false;
            for (auto & f: featuresActive[i])
                myData.features.at(f).active = true;
            
            result[i].root = myData.train(0, maxDepth, result[i], serializer, trainingScheme);
            ExcAssert(result[i].root);
        };

        MLDB::parallelMap(0, featuresActive.size(), buildTree);

        for (auto & r: result)
            ExcAssert(r.root);
    }
    return result;
}



} // namespace RF
} // namespace MLDB
