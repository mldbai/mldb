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
#include "mldb/sql/cell_value.h"
#include "mldb/base/exc_assert.h"
#include "mldb/block/memory_region.h"
#include "mldb/arch/bit_range_ops.h"

namespace MLDB {

struct Utf8String;

/** Holds an array of bucket indexes, efficiently. */
struct BucketList {

    MLDB_ALWAYS_INLINE uint32_t operator [] (uint32_t i) const
    {
        //ExcAssertLess(i, numEntries);
        Bit_Extractor<uint32_t> extractor(storagePtr);
        extractor.advance(i * entryBits);
        return extractor.extractFast<uint32_t>(entryBits);
    }

    MLDB_ALWAYS_INLINE uint32_t at (uint32_t i) const
    {
        ExcAssertLess(i, numEntries);
        return operator [] (i);
    }

    size_t rowCount() const
    {
        return numEntries;
    }

    friend class WritableBucketList;
    friend class ParallelWritableBucketList;
    FrozenMemoryRegionT<uint32_t> storage;
    const uint32_t * storagePtr = nullptr;
    
public:
    int entryBits = 0;
    int numBuckets = 0;
    size_t numEntries = 0;
    std::shared_ptr<void> backingStore;  ///< Used to hold a reference to memory backing this object
};

/** Writable version of the above.  OK to slice. */
struct WritableBucketList {
    WritableBucketList() = default;

    /** How many bytes are required (aligned at uint32_t) for the given
        number of buckets each containing a number up to numElements?
    */
    static size_t wordsRequired(size_t numElements, uint32_t numBuckets);
    
    /** Initialize from already-allocated memory.  There must be at least
        wordsRequired words already allocated.
    */
    WritableBucketList(size_t numElements, uint32_t numBuckets,
                       MutableMemoryRegionT<uint32_t> mem)
    {
        init(numElements, numBuckets, mem);
    }

    WritableBucketList(size_t numElements, uint32_t numBuckets,
                       MappedSerializer & serializer)
    {
        init(numElements, numBuckets, serializer);
    }

    void init(size_t numElements, uint32_t numBuckets,
              MappedSerializer & serializer);

    void init(size_t numElements, uint32_t numBuckets,
              MutableMemoryRegionT<uint32_t> mem);
    
    inline void write(uint32_t value)
    {
        writer.write(value, entryBits);
    }

    BucketList freeze(MappedSerializer & serializer);

    size_t rowCount() const
    {
        return numEntries;
    }
    
    MLDB_ALWAYS_INLINE uint32_t operator [] (uint32_t i) const
    {
        Bit_Extractor<uint32_t> extractor(storage.data());
        extractor.advance(i * entryBits);
        return extractor.extractFast<uint32_t>(entryBits);
    }

    Bit_Writer<uint32_t> writer = nullptr;
    MutableMemoryRegionT<uint32_t> storage;

    int entryBits = 0;
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

    ParallelWritableBucketList(size_t numElements, uint32_t numBuckets,
                               MutableMemoryRegionT<uint32_t> mem)
        : WritableBucketList(numElements, numBuckets, std::move(mem))
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

} // namespace MLDB


