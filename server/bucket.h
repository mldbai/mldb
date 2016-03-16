/** bucket.h                                                       -*- C++ -*-
    Mathieu Marquis Bolduc, March 11th 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Structures to bucketize sets of cellvalues
*/

#pragma once

#include <cstddef>
#include <stdint.h>
#include <memory>
#include <vector>

namespace Datacratic {
namespace MLDB {

struct CellValue;

/** Holds an array of bucket indexes, efficiently. */
struct BucketList {

    BucketList()
        : entryBits(0), numBuckets(0), numEntries(0)
    {
    }

    inline uint32_t operator [] (uint32_t i) const
    {
        //ExcAssertLess(i, numEntries);
        size_t wordNum = (i * entryBits) / 64;
        size_t bitNum = (i * entryBits) % 64;
        uint32_t result = (storage.get()[wordNum] >> bitNum) & ((1ULL << entryBits) - 1);
        //ExcAssertLess(result, numBuckets);
        return result;
    }

    std::shared_ptr<const uint64_t> storage;
    int entryBits;
    int numBuckets;
    size_t numEntries;
};

/** Writable version of the above.  OK to slice. */
struct WritableBucketList: public BucketList {
    WritableBucketList()
        : current(0), bitsWritten(0)
    {
    }

    WritableBucketList(size_t numElements, uint32_t numBuckets)
        : WritableBucketList()
    {
        init(numElements, numBuckets);
    }

    void init(size_t numElements, uint32_t numBuckets);

    inline void write(uint64_t value)
    {
        //ExcAssertLess(value, numBuckets);
        uint64_t already = bitsWritten ? *current : 0;
        *current = already | (value << bitsWritten);
        bitsWritten += entryBits;
        current += (bitsWritten >= 64);
        bitsWritten *= (bitsWritten < 64);

        //ExcAssertEqual(this->operator [] (numWritten), value);
        //ExcAssertLess(numWritten, numEntries);
        numWritten += 1;
    }

    uint64_t * current;
    int bitsWritten;
    size_t numWritten;
};

struct NumericValues {
    NumericValues()
        : active(false), offset(0)
    {
    }

    bool active;
    uint32_t offset;
    std::vector<double> splits;

    size_t numBuckets() const
    {
        return splits.size() + active;
    }

    uint32_t getBucket(double val) const;

};

struct OrdinalValues {
    OrdinalValues()
        : active(false), offset(0)
    {
    }

    bool active;
    uint32_t offset;
    std::vector<CellValue> splits;
    size_t numBuckets() const
    {
        return splits.size() + active;
    }

    uint32_t getBucket(const CellValue & val) const;
};

struct CategoricalValues {
    CategoricalValues()
        : offset(0)
    {
    }

    uint32_t offset;
    std::vector<CellValue> buckets;

    size_t numBuckets() const
    {
        return buckets.size();
    }

    uint32_t getBucket(const CellValue & val) const;
};

struct BucketDescriptions {
    BucketDescriptions();
    bool hasNulls;
    NumericValues numeric;
    CategoricalValues strings, blobs;
    OrdinalValues timestamps, intervals;

    void initialize(std::vector<CellValue> values,
                    int numBuckets = -1);

    /// Initialize from a set of pre-discretized
    std::pair<BucketDescriptions, BucketList>
    discretize(BucketList input, int numBuckets = -1);
    
    /// Look up the bucket number for the given value
    uint32_t getBucket(const CellValue & val) const;
    CellValue getValue(uint32_t bucket) const;
    CellValue getSplit(uint32_t bucket) const;
    size_t numBuckets() const;

    bool isOnlyNumeric() const;

};

}
}