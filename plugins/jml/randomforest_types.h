/** randomforest_types.h                                           -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#pragma once

#include "mldb/engine/bucket.h"
#include "mldb/block/memory_region.h"
#include "dataset_feature_space.h"
#include "mldb/arch/bitops.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/utils/fixed_point_accum.h"

namespace MLDB {
namespace RF {

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

    bool operator == (const WT & other) const
    {
        return v[0] == other.v[0] && v[1] == other.v[1];
    }

    bool operator != (const WT & other) const
    {
        return ! operator == (other);
    }

    typedef Float FloatType;
};

typedef WT<ML::FixedPointAccum64> W;
//typedef WT<float> W;
//typedef WT<double> W;

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
              bool sequentialExampleNums)
        : weightEncoder(weightEncoder),
          weightBits(weightEncoder->weightBits),
          exampleNumBits(sequentialExampleNums
                         ? 0 : MLDB::highest_bit(maxExampleCount, -1) + 1),
          totalBits(weightBits + exampleNumBits + 1),
          toAllocate((totalBits * maxRows + 63) / 64 + 1 /* +1 allows extractFast */),
          data(serializer
               .allocateWritableT<uint64_t>(toAllocate, 4096 /* page aligned */)),
          writer(data.data())
    {
    }

    void addRow(const Row & row)
    {
        uint64_t toWrite = row.label();
        toWrite = (toWrite << weightBits) | row.encodedWeight_;
        if (exampleNumBits > 0)
            toWrite = (toWrite << exampleNumBits) | row.exampleNum();
        writer.write(toWrite, totalBits);
        float weight = weightEncoder->decodeWeight(row.encodedWeight_);
        wAll[row.label()] += weight;
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
            return extractor.extractFastUnmaskedUnsafe<uint64_t>(totalBits);
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
                           bool sequentialExampleNums)
    {
        return RowWriter(&weightEncoder, maxRows, maxExampleCount, serializer,
                         sequentialExampleNums);
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

} // namespace RF
} // namespace MLDB
