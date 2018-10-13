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
#include "mldb/arch/bitops.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/block/memory_region.h"
#include "mldb/utils/lightweight_hash.h"

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

    void clear()
    {
        clearRows();
        features.clear();
        fs.reset();
    }
    
    static MemorySerializer serializer;
    
    /// Defines how weights are stored in rows
    enum WeightFormat {
        WF_INT_MULTIPLE,  ///< Stored as an int multiplied by a constant
        WF_TABLE,         ///< Stored as an index into a fixed table
        WF_FLOAT          ///< Stored directly as a 32 bit float
    };

    WeightFormat weightFormat = WF_FLOAT;
    int weightBits = 32;
    FrozenMemoryRegionT<float> weightFormatTable;
    float weightMultiplier = 0;

    void copyWeights(const PartitionData & other)
    {
        weightFormat = other.weightFormat;
        weightBits = other.weightBits;
        weightFormatTable = other.weightFormatTable;
        weightMultiplier = other.weightMultiplier;
    }
    
    union u_f {
        uint32_t u;
        float f;
    };

    float decodeWeight(uint32_t weightBits) const
    {
        if (MLDB_LIKELY(weightFormat == WF_INT_MULTIPLE)) {
            return weightMultiplier * weightBits;
        }

        switch (weightFormat) {
        case WF_INT_MULTIPLE:
            return weightMultiplier * weightBits;
        case WF_TABLE:
            return weightFormatTable.data()[weightBits];
        case WF_FLOAT: {
            u_f uf;
            uf.u = weightBits;
            return uf.f;
        }
        }
        throw Exception("Unknown weight encoding");
    }

    uint32_t encodeWeight_(float weight) const
    {
        switch (weightFormat) {
        case WF_INT_MULTIPLE:
            return weight / weightMultiplier;
        case WF_TABLE:
            throw Exception("WF_TABLE not encoding yet");
        case WF_FLOAT: {
            u_f uf;
            uf.f = weight;
            return uf.u;
        }
        }
        throw Exception("Unknown weight encoding");
    }

    uint32_t encodeWeight(float weight) const
    {
        uint32_t result = encodeWeight_(weight);
        //ExcAssertEqual(weight, decodeWeight(result));
        return result;
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
                                     double scale) const
    {
        PartitionData data;
        data.features = this->features;
        data.fs = this->fs;

        ExcAssertEqual(counts.size(), rowCount());
        
        size_t chunkSize
            = std::min<size_t>(100000, rowCount() / numCpus() / 4);

        using namespace std;
        cerr << "chunkSize = " << chunkSize << endl;

        // Analyze the weights.  This may allow us to store them in a lot
        // less bits than we would have otherwise.

        auto doWeightChunk = [&] (size_t start)
            -> std::tuple<LightweightHashSet<float> /* uniques */,
                          float /* minWeight */,
                          size_t /* numValues */>
            {
                size_t end = std::min(start + chunkSize, rowCount());

                LightweightHashSet<float> uniques;
                float minWeight = INFINITY;
                size_t numValues = 0;
                
                for (size_t i = start;  i < end;  ++i) {
                    float weight = getWeight(i) * counts[i] * scale;
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
            (0, rowCount() / chunkSize + 1, doWeightChunk, reduceWeights);

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
            data.weightFormat = WF_INT_MULTIPLE;
            data.weightMultiplier = minWeight;
            data.weightBits = MLDB::highest_bit(maxIntFactor, -1) + 1;
        }
        else if (uniqueWeights.size() < 4096) { /* max 16kb of cache */
            data.weightFormat = WF_TABLE;
            auto mutableWeightFormatTable
                = serializer.allocateWritableT<float>(uniqueWeights.size());
            std::memcpy(mutableWeightFormatTable.data(),
                        uniqueWeights.data(),
                        uniqueWeights.size() * sizeof(float));
            data.weightFormatTable = mutableWeightFormatTable.freeze();
            data.weightBits = MLDB::highest_bit(numWeightsToEncode, -1) + 1;
        }
        else {
            data.weightFormat = WF_FLOAT;
            data.weightBits = 32;
        }

        std::vector<WritableBucketList>
            featureBuckets(features.size());

        // We split the rows up into tranches to increase parallism
        // To do so, we need to know how many items are in each
        // tranche and where its items start
        size_t numTranches = std::min<size_t>(8, rowCount() / 1024);
        if (numTranches == 0)
            numTranches = 1;
        //numTranches = 1;
        size_t numPerTranche = rowCount() / numTranches;

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
            for (; end < rowCount()
                     && (end < start + numPerTranche
                         || n == 0
                         || n % 64 != 0);  ++end) {
                n += counts[end] != 0;
            }
            trancheCounts.push_back(n);
            trancheOffsets.push_back(offset);
            offset += n;
            start = end;
            ExcAssertLessEqual(start, rowCount());
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
                        = data.getRowWriter(numNonZero, numNonZero);
                    RowIterator rowIterator
                        = getRowIterator();

                    for (size_t i = 0;  i < rowCount();  ++i) {
                        DecodedRow row = rowIterator.getDecodedRow();
                        if (counts[i] == 0 || row.weight == 0)
                            continue;
                        writer.addRow(row.label,
                                      row.weight * counts[i] * scale,
                                      n++);
                    }
                    
                    ExcAssertEqual(n, numNonZero);
                    writer.commit();
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
                        if (counts[i] == 0)
                            continue;
                        
                        uint32_t bucket = features[f].buckets[getExampleNum(i)];
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
        //Row(bool label, uint32_t encodedWeight, uint32_t exampleNum)
        //    : label_(label), encodedWeight_(encodedWeight),
        //      exampleNum_(exampleNum)
        //{
        //}
        
        bool label_;                 ///< Label associated with example
        uint32_t encodedWeight_;    ///< Weight of the example 
        uint32_t exampleNum_;        ///< index into feature array

        ///< Label associated with
        bool label() const { return label_; }
        uint32_t exampleNum() const { return exampleNum_; }
        uint32_t encodedWeight() const { return encodedWeight_; }
    };

    /// Version of Row that has the weight decoded
    struct DecodedRow {
        bool label;
        float weight;
        uint32_t exampleNum;
    };
    
    // Entry for an individual feature
    struct Feature {
        bool active = false;  ///< If true, the feature can be split on
        bool ordinal = true; ///< If true, it's continuous valued; else categ.
        const DatasetFeatureSpace::ColumnInfo * info = nullptr;
        BucketList buckets;  ///< List of bucket numbers, per example
    };

    // All rows of data in this partition
    //std::vector<Row> rows_;

    // Encoded data for our row
    FrozenMemoryRegionT<uint64_t> rowData;

    // Number of entries in rowData
    size_t numRowEntries;

    int exampleNumBits;
    uint64_t exampleNumMask;
    uint64_t weightMask;
    int totalBits;

    struct RowIterator {
        static constexpr size_t BUFFER_SIZE = 64;

        static constexpr size_t CACHE_LINE_SIZE = 64;
        
        RowIterator(const PartitionData * owner)
            : owner(owner),
              extractor(owner->rowData.data()),
              totalBits(owner->totalBits),
              numRows(owner->numRowEntries)
        {
            linesToPrefetch = 0; /* BUFFER_SIZE * totalBits / CACHE_LINE_SIZE;*/

            for (int i = 0;  i < linesToPrefetch * 2;  ++i)
                extractor.prefetch(i * CACHE_LINE_SIZE);
        }

        void prefetch()
        {
            ExcAssertEqual(bufferFirst, bufferLast);
            bufferFirst = 0;
            bufferLast = std::min<size_t>(BUFFER_SIZE, numRows - rowNumber);
            if (bufferLast == BUFFER_SIZE)
                for (int i = 0;  i <= linesToPrefetch;  ++i)
                    extractor.prefetch(linesToPrefetch + i * CACHE_LINE_SIZE);
            for (size_t i = 0;  i < bufferLast;  ++i) {
                buffer[i] = extractor.extractFastUnmasked<uint64_t>(totalBits);
            }
            rowNumber += bufferLast;
        }
        
        MLDB_ALWAYS_INLINE uint64_t getRowBits()
        {
            if (bufferFirst == bufferLast) {
                prefetch();
            }
            return buffer[bufferFirst++];
        }

        MLDB_ALWAYS_INLINE Row getRow()
        {
            return owner->decodeRow(getRowBits());
        }

        MLDB_ALWAYS_INLINE DecodedRow getDecodedRow()
        {
            Row row = getRow();
            return { row.label(),
                     owner->decodeWeight(row.encodedWeight_),
                     row.exampleNum()
                   };
        }

        const PartitionData * owner;
        ML::Bit_Extractor<uint64_t> extractor;
        int totalBits;
        uint64_t buffer[BUFFER_SIZE];
        size_t bufferFirst = 0;
        size_t bufferLast = 0;
        size_t rowNumber = 0;
        size_t numRows;
        int linesToPrefetch = 0;  // How many cache lines do we prefetch?
    };

    RowIterator getRowIterator() const
    {
        return RowIterator(this);
    }
        
    MLDB_ALWAYS_INLINE Row decodeRow(uint64_t allBits) const
    {
        Row result;
        result.exampleNum_ = allBits & exampleNumMask;
        allBits >>= exampleNumBits;
        result.encodedWeight_ = allBits & weightMask;
        allBits >>= weightBits;
        //ExcAssertLessEqual(allBits, 1);
        result.label_ = allBits & 1;
        return result;
    }
    
    MLDB_ALWAYS_INLINE uint64_t getRowBits(size_t i) const
    {
        ML::Bit_Extractor<uint64_t> extractor(rowData.data());
        extractor.advance(totalBits * i);
        return extractor.extractFast<uint64_t>(totalBits);
    }
    
    MLDB_ALWAYS_INLINE Row getRow(size_t i) const
    {
        return decodeRow(getRowBits(i));
    }

    MLDB_ALWAYS_INLINE DecodedRow getDecodedRow(size_t i) const
    {
        Row row = getRow(i);
        return { row.label(),
                 decodeWeight(row.encodedWeight_),
                 row.exampleNum()
               };
    }
    
    float getWeight(size_t i) const
    {
        uint64_t allBits = getRowBits(i);
        allBits >>= exampleNumBits;
        return decodeWeight(allBits & weightMask);
    }

    uint32_t getExampleNum(size_t i) const
    {
        uint64_t allBits = getRowBits(i);
        return allBits & exampleNumMask;
    }

    size_t rowCount() const
    {
        return numRowEntries;
    }

    uint32_t highestExampleNum() const
    {
        return getExampleNum(numRowEntries - 1);
    }

    void clearRows()
    {
        rowData = FrozenMemoryRegionT<uint64_t>();
    }
    
    // All features that are active
    std::vector<Feature> features;

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
    //typedef WT<float> W;
    //typedef WT<double> W;

    struct RowWriter {
        RowWriter(PartitionData * owner,
                  size_t maxRows,
                  size_t maxExampleCount)
            : owner(owner),
              weightBits(owner->weightBits),
              exampleNumBits(MLDB::highest_bit(maxExampleCount, -1) + 1),
              totalBits(weightBits + exampleNumBits + 1),
              toAllocate((totalBits * maxRows + 63) / 64 + 1 /* +1 allows extractFast */),
              data(serializer
                   .allocateWritableT<uint64_t>(toAllocate)),
              writer(data.data())
        {
        }

        void addRow(const Row & row)
        {
            uint64_t toWrite = row.label();
            toWrite = (toWrite << weightBits) | row.encodedWeight_;
            toWrite = (toWrite << exampleNumBits) | row.exampleNum();
            writer.write(toWrite, totalBits);
            float weight = owner->decodeWeight(row.encodedWeight_);

            if (false) {
                owner->exampleNumBits = exampleNumBits;
                owner->exampleNumMask = (1ULL << exampleNumBits) - 1;
                owner->weightMask = (1ULL << weightBits) - 1;
                owner->totalBits = totalBits;

                Row row2 = owner->decodeRow(toWrite);

                ExcAssertEqual(row.label(), row2.label());
                ExcAssertEqual(row.encodedWeight(), row2.encodedWeight());
                ExcAssertEqual(row.exampleNum(), row2.exampleNum());
                ExcAssertGreater(weight, 0);
            }

            //using namespace std;
            //if (weight != 1.0)
            //    cerr << "wall " << row.label() << " " << weight << endl;
            wAll[row.label()] += weight;
        }

        void addRow(bool label, float weight, uint32_t exampleNum)
        {
            Row toAdd{label, owner->encodeWeight(weight), exampleNum};
            addRow(toAdd);
        }
            
        void commit()
        {
            owner->numRowEntries
                = writer.current_offset(data.data()) / totalBits;

            //using namespace std;
            //cerr << "commit with " << owner->numRowEntries << " entries and "
            //     << totalBits << " total bits" << endl;
            //cerr << "wall = " << wAll[false] << " " << wAll[true] << endl;
            
            // fill in extra guard word at end, required for extractFast
            // to work
            writer.write(0, 64);
            writer.zeroExtraBits();
            owner->rowData = data.freeze();

            owner->exampleNumBits = exampleNumBits;
            owner->exampleNumMask = (1ULL << exampleNumBits) - 1;
            owner->weightMask = (1ULL << weightBits) - 1;
            owner->totalBits = totalBits;
            owner->wAll = wAll;
        }

    private:
        PartitionData * owner;
        int weightBits;
        int exampleNumBits;
        int totalBits;
        size_t toAllocate;
        MutableMemoryRegionT<uint64_t> data;
        Bit_Writer<uint64_t> writer;
        W wAll;
    };
    
    RowWriter getRowWriter(size_t maxRows,
                           size_t maxExampleCount)
    {
        return RowWriter(this, maxRows, maxExampleCount);
    }

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
        left.copyWeights(*this);
        right.copyWeights(*this);
        
        bool ordinal = features[featureToSplitOn].ordinal;

        // Density of example numbers within our set of rows.  When this
        // gets too low, we do essentially random accesses and it kills
        // our cache performance.  In that case we can re-index to reduce
        // the size.
        double useRatio = 1.0 * rowCount() / highestExampleNum();

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
                = { sides[0].getRowWriter(rowCount(), highestExampleNum()),
                    sides[1].getRowWriter(rowCount(), highestExampleNum()) };

            RowIterator rowIterator = getRowIterator();
            
            for (size_t i = 0;  i < rowCount();  ++i) {
                Row row = rowIterator.getRow();
                int bucket
                    = features[featureToSplitOn]
                    .buckets[row.exampleNum_];
                //maxBucket = std::max(maxBucket, bucket);
                //minBucket = std::min(minBucket, bucket);
                int side = ordinal ? bucket > splitValue : bucket != splitValue;
                writer[side].addRow(row);
            }

            clearRows();
            features.clear();

            writer[0].commit();
            writer[1].commit();
        
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

            std::vector<uint8_t> lr(rowCount());
            bool ordinal = features[featureToSplitOn].ordinal;
            size_t numOnSide[2] = { 0, 0 };

            // TODO: could reserve less than this...
            RowWriter writer[2]
                = { sides[0].getRowWriter(rowCount(), rowCount()),
                    sides[1].getRowWriter(rowCount(), rowCount()) };

            RowIterator rowIterator = getRowIterator();

            for (size_t i = 0;  i < rowCount();  ++i) {
                Row row = rowIterator.getRow();
                int bucket = features[featureToSplitOn].buckets[row.exampleNum()];
                int side = ordinal ? bucket > splitValue : bucket != splitValue;
                lr[i] = side;
                row.exampleNum_ = numOnSide[side]++;
                writer[side].addRow(row);
            }

            for (unsigned i = 0;  i < nf;  ++i) {
                if (!features[i].active)
                    continue;

                WritableBucketList newFeatures[2];
                newFeatures[0].init(numOnSide[0], features[i].info->distinctValues);
                newFeatures[1].init(numOnSide[1], features[i].info->distinctValues);
                size_t index[2] = { 0, 0 };

                for (size_t j = 0;  j < rowCount();  ++j) {
                    int side = lr[j];
                    newFeatures[side].write(features[i].buckets[getExampleNum(j)]);
                    ++index[side];
                }

                sides[0].features[i].buckets = newFeatures[0];
                sides[1].features[i].buckets = newFeatures[1];
            }

            clearRows();
            features.clear();

            writer[0].commit();
            writer[1].commit();
        }

        return { std::move(left), std::move(right) };
    }

    // Core kernel of the decision tree search algorithm.  Transfer the
    // example weight into the appropriate (bucket,label) accumulator.
    // Returns whether
    template<typename GetNextRowFn>
    static std::pair<bool, int>
    testFeatureKernel(GetNextRowFn&& getNextRow,
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
            DecodedRow r = getNextRow();
            int bucket = buckets[r.exampleNum];
            bucketTransitions += (bucket != lastBucket ? 1 : 0);
            lastBucket = bucket;

            w[bucket][r.label] += r.weight;
            maxBucket = std::max(maxBucket, bucket);
        }

        return { bucketTransitions > 0, maxBucket };
    }

    // Calculates the score of a split, which is a measure of the
    // amount of mutual entropy between the label and the given
    // candidate split point.
    static double scoreSplit(const W & wFalse, const W & wTrue)
    {
        double score
            = 2.0 * (  sqrt(wFalse[0] * wFalse[1])
                       + sqrt(wTrue[0] * wTrue[1]));
        return score;
    };

    // Chooses which is the best split for a given feature.
    static std::tuple<double /* bestScore */,
                      int /* bestSplit */,
                      W /* bestLeft */,
                      W /* bestRight */>
    chooseSplitKernel(const W * w /* at least maxBucket + 1 entries */,
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
        if (wAll[0] == 0 || wAll[1] == 0) {
            return std::make_tuple(1.0, -1, -1, wAll, W());
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

                RowIterator rowIterator = getRowIterator();
                
                auto getNextRow = [&] () -> DecodedRow
                {
                    return rowIterator.getDecodedRow();
                };
                
                std::tie(score, split, bestLeft, bestRight, features[i].active)
                    = testFeatureNumber(i, features, getNextRow, rowCount(), wAll);
                return std::make_tuple(score, split, bestLeft, bestRight);
            };

        if (depth < 4 || rowCount() * nf > 100000) {
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
       return getLeaf(tree, wAll);
    }  

    ML::Tree::Ptr train(int depth, int maxDepth,
                        ML::Tree & tree)
    {
        if (rowCount() == 0)
            return ML::Tree::Ptr();
        if (rowCount() < 2)
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

        size_t leftRows = splits.first.rowCount();
        size_t rightRows = splits.second.rowCount();

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
