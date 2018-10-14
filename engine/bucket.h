/** bucket.h                                                       -*- C++ -*-
    Mathieu Marquis Bolduc, March 11th 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Structures to bucketize sets of cellvalues
*/

#pragma once

#include <cstddef>
#include <stdint.h>
#include <memory>
#include <vector>
#include "mldb/base/exc_assert.h"
#include "mldb/block/memory_region.h"

namespace MLDB {

struct CellValue;
struct Utf8String;

/** Holds an array of bucket indexes, efficiently. */
struct BucketList {

    MLDB_ALWAYS_INLINE uint32_t operator [] (uint32_t i) const
    {
        //ExcAssertLess(i, numEntries);
        size_t wordNum = (i << entryShift) / 64;
        size_t bitNum = (i << entryShift) % 64;
        uint32_t result = (storagePtr[wordNum] >> bitNum) & ((1ULL << entryBits) - 1);
        return result;
    }

    size_t rowCount() const
    {
        return numEntries;
    }

private:
    friend class WritableBucketList;
    friend class ParallelWritableBucketList;
    FrozenMemoryRegionT<uint64_t> storage;
    const uint64_t * storagePtr = nullptr;

public:
    int entryBits = 0;
    int entryShift = 0;
    int numBuckets = 0;
    size_t numEntries = 0;
};

/** Writable version of the above.  OK to slice. */
struct WritableBucketList {
    WritableBucketList() = default;

    WritableBucketList(size_t numElements, uint32_t numBuckets,
                       MappedSerializer & serializer)
        : WritableBucketList()
    {
        init(numElements, numBuckets, serializer);
    }

    void init(size_t numElements, uint32_t numBuckets,
              MappedSerializer & serializer);
    
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

    // Append all of the bits from buckets
    //void append(const WritableBucketList & buckets);

    BucketList freeze(MappedSerializer & serializer);

    size_t rowCount() const
    {
        return numWritten;
    }
    
    MLDB_ALWAYS_INLINE uint32_t operator [] (uint32_t i) const
    {
        //ExcAssertLess(i, numEntries);
        size_t wordNum = (i << entryShift) / 64;
        size_t bitNum = (i << entryShift) % 64;
        uint32_t result = (storage.data()[wordNum] >> bitNum) & ((1ULL << entryBits) - 1);
        return result;
    }
    
    uint64_t * current = nullptr;
    int bitsWritten = 0;
    size_t numWritten = 0;
    MutableMemoryRegionT<uint64_t> storage;

    int entryBits = 0;
    int entryShift = 0;
    int numBuckets = 0;
    size_t numEntries = 0;
};

struct ParallelWritableBucketList: public WritableBucketList {

    ParallelWritableBucketList() = default;
    
    ParallelWritableBucketList(size_t numElements, uint32_t numBuckets,
                               MappedSerializer & serializer)
        : WritableBucketList(numElements, numBuckets, serializer)
    {
    }

    // Return a writer at the given offset, which must be a
    // multiple of 64.  This allows the bucket list to be
    // written from multiple threads.  Must only be called
    // without any writing having taken place
    WritableBucketList atOffset(size_t offset);

    // Append all of the bits from buckets
    void append(const WritableBucketList & buckets);
};

struct NumericValues {
    NumericValues()
        : active(false), offset(0)
    {
    }

    NumericValues(std::vector<double> values);

    bool active;
    uint32_t offset;
    std::vector<double> splits;

    size_t numBuckets() const
    {
        return splits.size() + active;
    }

    uint32_t getBucket(double val) const;

    void merge(const NumericValues & other);
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

    void merge(const OrdinalValues & other);
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

struct StringValues {
    StringValues()
        : offset(0)
    {
    }

    uint32_t offset;
    std::vector<Utf8String> buckets;

    size_t numBuckets() const
    {
        return buckets.size();
    }

    uint32_t getBucket(const CellValue & val) const;
};


/*****************************************************************************/
/* BUCKET DESCRIPTIONS                                                       */
/*****************************************************************************/

struct BucketDescriptions {
    BucketDescriptions();
    bool hasNulls;
    NumericValues numeric;
    StringValues strings;
    CategoricalValues blobs, paths;
    OrdinalValues timestamps, intervals;

    void initialize(std::vector<CellValue> values,
                    int numBuckets = -1);

    void initialize(bool hasNulls,
                    std::vector<double> numericValues,
                    std::vector<Utf8String> stringValues,
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

    static std::tuple<BucketList, BucketDescriptions>
    merge(const std::vector<std::tuple<BucketList, BucketDescriptions> > & inputs,
          MappedSerializer & serializer,
          int numBuckets = -1);
};

}
