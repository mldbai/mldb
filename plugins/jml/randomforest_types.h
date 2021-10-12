/** randomforest_types.h                                           -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#pragma once

#include "mldb/core/bucket.h"
#include "mldb/block/memory_region.h"
#include "dataset_feature_space.h"
#include "mldb/arch/bitops.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/utils/fixed_point_accum.h"
#include "mldb/plugins/jml/jml/tree.h"
#include <span>

namespace MLDB {

PREDECLARE_VALUE_DESCRIPTION(FixedPointAccum32);
PREDECLARE_VALUE_DESCRIPTION(FixedPointAccum64);

namespace RF {

//This structure hold the weights (false and true) for any particular split
template<typename Float>
struct WT {
    std::array<Float, 2> v = { 0, 0};
    int32_t c = 0;

    void add(bool i, Float val)
    {
        v[i] += val;
        c += 1;
    }

    void sub(bool i, Float val)
    {
        v[i] -= val;
        c -= 1;
    }

    const Float & operator [] (bool i) const
    {
        return v[i];
    }

    bool empty() const { return total() == 0; }

    // Is the label uniform, meaning we can learn nothing more from this region of the tree?
    bool uniform() const { return v[0] == 0 || v[1] == 0; }

    Float total() const { return v[0] + v[1]; }

    uint32_t count() const { return c; }
    
    WT & operator += (const WT & other)
    {
        v[0] += other.v[0];
        v[1] += other.v[1];
        c += other.c;
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
        c -= other.c;
        return *this;
    }

    bool operator == (const WT & other) const
    {
        return v[0] == other.v[0] && v[1] == other.v[1] && c == other.c;
    }

    bool operator != (const WT & other) const
    {
        return ! operator == (other);
    }

    typedef Float FloatType;
}; // MLDB_PACKED MLDB_ALIGNED(4);


//typedef WT<FixedPointAccum32> W;
using W64 = WT<FixedPointAccum64>;
using W32 = WT<FixedPointAccum32>;
using W = W32;
//typedef WT<float> W;
//typedef WT<double> W;

DECLARE_STRUCTURE_DESCRIPTION(W);

// Version of W with an index field; index is used in local GPU memory for
// metadata
struct WIndexed: public W {
    int32_t index;
};

DECLARE_STRUCTURE_DESCRIPTION(WIndexed);

// Entry for an individual feature
struct Feature {
    bool active = false;  ///< If true, the feature can be split on
    bool ordinal = true; ///< If true, it's continuous valued; else categ.
    const DatasetFeatureSpace::ColumnInfo * info = nullptr;
    BucketList buckets;  ///< List of bucket numbers, per example
};

/// Defines how weights are stored in rows
enum WeightFormat {
    WF_INT_MULTIPLE,  ///< Stored as an int multiplied by a constant
    WF_TABLE,         ///< Stored as an index into a fixed table
    WF_FLOAT          ///< Stored directly as a 32 bit float
};

DECLARE_ENUM_DESCRIPTION(WeightFormat);

struct WeightEncoder {
    WeightFormat weightFormat = WF_FLOAT;
    int weightBits = 32;
    FrozenMemoryRegionT<float> weightFormatTable;
    float weightMultiplier = 0;

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
};

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

struct Rows;

struct RowWriter {
    RowWriter(const WeightEncoder * weightEncoder,
              size_t maxRows,
              size_t maxExampleCount,
              MappedSerializer & serializer,
              bool sequentialExampleNums,
              float weightScalingFactor = 1.0)
        : weightEncoder(weightEncoder),
          weightBits(weightEncoder->weightBits),
          exampleNumBits(sequentialExampleNums
                         ? 0 : MLDB::highest_bit(maxExampleCount, -1) + 1),
          totalBits(weightBits + exampleNumBits + 1 /* for label */),
          toAllocate((totalBits * maxRows + 63) / 64 + 1 /* +1 allows extractFast */),
          data(serializer
               .allocateWritableT<uint64_t>(toAllocate, 4096 /* page aligned */)),
          writer(data.data()),
          weightScalingFactor(weightScalingFactor)
    {
    }

    void addRow(const Row & row)
    {
        uint64_t toWrite = row.label();
        toWrite = (toWrite << weightBits) | row.encodedWeight_;
        if (exampleNumBits > 0)
            toWrite = (toWrite << exampleNumBits) | row.exampleNum();
        writer.write(toWrite, totalBits);
        float weight = weightEncoder->decodeWeight(row.encodedWeight_) * weightScalingFactor;
        wAll.add(row.label(), weight);
    }

    void addRow(bool label, float weight, uint32_t exampleNum)
    {
        Row toAdd{label, weightEncoder->encodeWeight(weight), exampleNum};
        addRow(toAdd);
    }
            
    Rows freeze(MappedSerializer & serializer);

private:
    const WeightEncoder * weightEncoder;
    int weightBits;
    int exampleNumBits;
    int totalBits;
    size_t toAllocate;
    MutableMemoryRegionT<uint64_t> data;
    Bit_Writer<uint64_t> writer;
    W wAll;
    float weightScalingFactor;
};

struct Rows {
    // Encoded data for our row
    FrozenMemoryRegionT<uint64_t> rowData;

    // Number of entries in rowData
    size_t numRowEntries;

    int exampleNumBits;
    uint64_t exampleNumMask;
    uint64_t weightMask;
    int totalBits;

    WeightEncoder weightEncoder;

    W wAll;
    
    
    struct RowIterator {
        RowIterator(const Rows * owner,
                    const uint64_t * data,
                    int totalBits)
            : owner(owner),
              extractor(data),
              totalBits(totalBits)
        {
        }

        MLDB_ALWAYS_INLINE uint64_t getRowBits()
        {
            return extractor.extractFastUnmasked<uint64_t>(totalBits);
        }

        MLDB_ALWAYS_INLINE Row getRow()
        {
            return owner->decodeRow(getRowBits(), rowNumber++);
        }

        MLDB_ALWAYS_INLINE DecodedRow getDecodedRow()
        {
            Row row = getRow();
            return { row.label(),
                     owner->weightEncoder.decodeWeight(row.encodedWeight_),
                     row.exampleNum()
                   };
        }

        MLDB_ALWAYS_INLINE void skipRow()
        {
            extractor.advance(totalBits);
            ++rowNumber;
        }

        void skipTo(size_t newRowNumber)
        {
            ExcAssertGreaterEqual(newRowNumber, rowNumber);
            extractor.advance(totalBits * uint64_t(newRowNumber - rowNumber));
        }

        const Rows * owner;
        ML::Bit_Extractor<uint64_t> extractor;
        int totalBits;
        uint32_t rowNumber = 0;
    };

    RowIterator getRowIterator() const
    {
        return RowIterator(this, rowData.data(), totalBits);
    }
        
    MLDB_ALWAYS_INLINE Row
    decodeRow(uint64_t allBits, uint32_t rowNumber) const
    {
        Row result;
        if (exampleNumBits == 0) {
            result.exampleNum_ = rowNumber;
        }
        else {
            result.exampleNum_ = allBits & exampleNumMask;
            allBits >>= exampleNumBits;
        }
        result.encodedWeight_ = allBits & weightMask;
        allBits >>= weightEncoder.weightBits;
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
        return decodeRow(getRowBits(i), i);
    }

    MLDB_ALWAYS_INLINE DecodedRow getDecodedRow(size_t i) const
    {
        Row row = getRow(i);
        return { row.label(),
                 weightEncoder.decodeWeight(row.encodedWeight_),
                 row.exampleNum()
               };
    }
    
    float getWeight(size_t i) const
    {
        uint64_t allBits = getRowBits(i);
        allBits >>= exampleNumBits;
        return weightEncoder.decodeWeight(allBits & weightMask);
    }

    uint32_t getExampleNum(size_t i) const
    {
        if (exampleNumBits == 0)
            return i;
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

    void clear()
    {
        rowData = FrozenMemoryRegionT<uint64_t>();
    }

    RowWriter getRowWriter(size_t maxRows,
                           size_t maxExampleCount,
                           MappedSerializer & serializer,
                           bool sequentialExampleNums,
                           float weightScalingFactor = 1.0) const
    {
        return RowWriter(&weightEncoder, maxRows, maxExampleCount, serializer,
                         sequentialExampleNums, weightScalingFactor);
    }
};

inline
Rows
RowWriter::
freeze(MappedSerializer & serializer)
{
    Rows result;
    result.numRowEntries
        = writer.current_offset(data.data()) / totalBits;

    //using namespace std;
    //cerr << "commit with " << result.numRowEntries << " entries and "
    //     << totalBits << " total bits" << endl;
    //cerr << "wall = " << wAll[false] << " " << wAll[true] << endl;
            
    // fill in extra guard word at end, required for extractFast
    // to work
    writer.write(0, 64);
    writer.zeroExtraBits();
    result.rowData = data.freeze();

    result.exampleNumBits = exampleNumBits;
    result.exampleNumMask = (1ULL << exampleNumBits) - 1;
    result.weightMask = (1ULL << weightBits) - 1;
    result.totalBits = totalBits;
    result.wAll = wAll;
    result.weightEncoder = *this->weightEncoder;

    return result;
}


/** Partition indexes indicate where a tree fragment exists in the
    tree. They cover the tree space up to a depth of 32.

    Index 0 is null; no partition has that index.
    Index 1 is the root (depth 0).
    Indexes 2 and 3 are the left and right of the root (depth 1).
    Indexes 4 and 5 are the left children of 2 and 3; indexes 6 and 7 are
    the right children of 2 and 3.

    index 0: 0000: left =  0, right =  0, parent = 0, depth = -1
    index 1: 0001: left =  2, right =  3, parent = 0, depth =  0
    index 2: 0010: left =  4, right =  6, parent = 1, depth =  1
    index 3: 0011: left =  5, right =  7, parent = 1, depth =  1
    index 4: 0100: left =  8, right = 12, parent = 2, depth =  2
    index 5: 0101: left =  9, right = 13, parent = 3, depth =  2
    index 6: 0110: left = 10, right = 14, parent = 2, depth =  2
    index 7: 0111: left = 11, right = 15, parent = 3, depth =  2

    And so on down.  The advantages of this scheme are that a static array of
    2^depth elements can be allocated that is sufficient to hold a tree of
    that depth, with each index uniquely identifying a place in that array.
*/

struct PartitionIndex {
    constexpr PartitionIndex() = default;
    constexpr PartitionIndex(uint32_t index)
        : index(index)
    {
    }

    static constexpr PartitionIndex none() { return { 0 }; };
    static constexpr PartitionIndex root() { return { 1 }; };
    
    uint32_t index = 0;

    int32_t depth() const
    {
        return index == 0 ? -1 : 31 - __builtin_clz(index);
    }
    
    // Left child is in the same position but bumped up
    PartitionIndex leftChild() const
    {
        ExcAssertNotEqual(index, 0);
        return PartitionIndex(index + (1 << depth()));
    }
    
    PartitionIndex rightChild() const
    {
        ExcAssertNotEqual(index, 0);
        return PartitionIndex(index + (2 << depth()));
    }
    
    PartitionIndex parent() const
    {
        // depth -1: parent of 0 (none) is undefined
        // depth 0: parent of 1 1 (root) is undefined
        // depth 1: parent of 2 10 (l) = 1 1(root)
        // depth 1: parent of 3 11 (r) = 1 1 (root)
        // depth 2: parent of 4 100 (ll) = 2 10 (l)
        // depth 2: parent of 5 101 (rl) = 3 11 (r)
        // depth 2: parent of 6 110 (lr) = 2 10 (l)
        // depth 2: parent of 7 111 (rr) = 3 11 (r)
        ExcAssertGreater(index, 1);
        uint32_t depthBit = 1 << (depth() - 1);
        uint32_t mask = depthBit - 1;
        uint32_t newIndex = index & mask;
        return PartitionIndex(depthBit + newIndex);
    }

    //PartitionIndex parentAtDepth(int32_t depth) const
    //{
    //    ExcAssertGreater(index, 1);
    //    ExcAssertGreater(depth, 1);
    //    return PartitionIndex(index >> (this->depth() - depth));
    //}

    bool operator == (const PartitionIndex & other) const
    {
        return index == other.index;
    }

    bool operator != (const PartitionIndex & other) const
    {
        return index != other.index;
    }

    bool operator < (const PartitionIndex & other) const
    {
        return index < other.index;
    }

    std::string path() const
    {
        std::string result;

        if (index == 0)
            return result = "none";
        if (index == 1)
            return result = "root";

        result.reserve(depth());
        for (int d = 0;  d < depth();  ++d) {
            result += (index & (1 << d) ? 'r' : 'l');
        }

        return result;
    }
};

PREDECLARE_VALUE_DESCRIPTION(PartitionIndex);

std::ostream & operator << (std::ostream & stream, PartitionIndex idx);

/** Holds the split for a partition. */

enum PartitionSplitDirection: uint8_t {
    LR = 0,
    RL = 1
};

struct PartitionSplit {
    PartitionIndex index;      // 4 bytes
    float score = INFINITY;    // 4 bytes
    int16_t feature = -1;      // 2 bytes
    int16_t value = -1;        // 2 bytes
    W left;                    // 12 bytes
    W right;                   // 12 bytes

    // Tells us which is the most efficient way to transfer weight:
    // from left to right, or from right to left.  This doesn't affect
    // how things are layed out.  For example, if on the right we have
    // a single row and on the left we have 1,000 rows, then we can
    // accumulate the single row on the right, and then infer the left
    // by subtracting the right from wAll, meaning we do no work at all
    // for the 999 rows on the left.  This is a big optimization and
    // leads to a major speedup.  But to be very clear, the direction
    // does not swap the buckets: the left is on the left, and the right
    // is on the right.  It just affects which one we acumulate.
    PartitionSplitDirection transferDirection() const { return PartitionSplitDirection(left.count() < right.count()); }

    bool valid() const { return feature != -1; }

    operator std::pair<PartitionIndex, PartitionSplit> const ()
    {
        return { index, *this };
    }
};

DECLARE_STRUCTURE_DESCRIPTION(PartitionSplit);

// Structure giving information about a given row (which partition it's in).
struct RowPartitionInfo {
    uint16_t partition_ = 0;
    uint16_t partition() const { return partition_; }
    static constexpr uint16_t max() { return -1; }
    RowPartitionInfo & operator = (uint16_t p) { this->partition_ = p;  return *this; }
    operator uint16_t () const { return partition_; }
};

DECLARE_STRUCTURE_DESCRIPTION(RowPartitionInfo);

struct PartitionEntry {
    std::vector<float> decodedRows;
    std::vector<Feature> features;
    std::span<const int> activeFeatures;
    std::set<int> activeFeatureSet;
    W wAll;
    PartitionIndex index;
    
    // Get a contiguous block of memory for all of the feature
    // blocks on each side; this enables a single GPU transfer
    // and a single GPU argument list.
    std::vector<size_t> bucketMemoryOffsets;
    MutableMemoryRegionT<uint32_t> mutableBucketMemory;
    FrozenMemoryRegionT<uint32_t> bucketMemory;
};



/*****************************************************************************/
/* TREE MANIPULATION                                                         */
/*****************************************************************************/

inline void fillinBase(ML::Tree::Base * node, const W & wAll)
{

    float total = float(wAll[0]) + float(wAll[1]);
    node->examples = wAll.count();
    node->pred = {
        float(wAll[0]) / total,
        float(wAll[1]) / total };
}

inline ML::Tree::Ptr getLeaf(ML::Tree & tree, const W & w)
{     
    ML::Tree::Leaf * node = tree.new_leaf();
    fillinBase(node, w);
    return node;
}

inline ML::Tree::Ptr
getNode(ML::Tree & tree, float bestScore,
        int bestFeature, int bestSplit,
        ML::Tree::Ptr left, ML::Tree::Ptr right,
        W wLeft, W wRight,
        const std::span<const Feature> & features,
        const DatasetFeatureSpace & fs)
{
    ML::Tree::Node * node = tree.new_node();
    ML::Feature feature = fs.getFeature(features[bestFeature].info->columnName);
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


inline DecodedRow decodeRow(Rows::RowIterator & rowIterator,
                     size_t rowNumber)
{
    return rowIterator.getDecodedRow();
}

inline DecodedRow decodeRow(const float * arr,
                     size_t rowNumber)
{
    float f = arr[rowNumber];
    DecodedRow result;
    result.label = f < 0;
    result.exampleNum = rowNumber;
    result.weight = fabs(f);
    return result;
}




} // namespace RF
} // namespace MLDB
