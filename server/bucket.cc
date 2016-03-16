/** bucket.cc                                                       -*- C++ -*-
    Mathieu Marquis Bolduc, March 11th 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Structures to bucketize sets of cellvalues
*/

#include "bucket.h"
#include "mldb/arch/bitops.h"
#include "mldb/sql/cell_value.h"
#include "mldb/http/http_exception.h"
#include "mldb/ml/jml/buckets.h"
#include "mldb/base/exc_assert.h"

#include <algorithm>
#include <unordered_map>

namespace Datacratic {
namespace MLDB {

namespace{

template<typename T>
static std::shared_ptr<T>
makeSharedArray(size_t len)
{
    return std::shared_ptr<T>(new T[len],
                              [] (T * p) { delete[] p; });
}

}

/*****************************************************************************/
/* BUCKET LIST                                                               */
/*****************************************************************************/

void
WritableBucketList::
init(size_t numElements, uint32_t numBuckets)
{
    this->numBuckets = numBuckets;
    entryBits = ML::highest_bit(numBuckets) + 1;

    // Take a number of bits per entry that evenly divides into
    // 64 bits.
    if (entryBits == 0) ;
    else if (entryBits == 1) ;
    else if (entryBits == 2) ;
    else if (entryBits <= 4)
        entryBits = 4;
    else if (entryBits <= 8)
        entryBits = 8;
    else if (entryBits <= 16)
        entryBits = 16;
    else entryBits = 32;

    //cerr << "using " << entryBits << " bits for " << numBuckets
    //     << " buckets" << endl;

    size_t numWords = (entryBits * numElements + 63) / 64;
    auto writableStorage = makeSharedArray<uint64_t>(numWords);
    this->current = writableStorage.get();
    this->storage = writableStorage;
    this->bitsWritten = 0;
    this->numEntries = numElements;
    this->numWritten = 0;
}


/*****************************************************************************/
/* BUCKET DESCRIPTIONS                                                       */
/*****************************************************************************/

uint32_t
BucketDescriptions::
getBucket(const CellValue & val) const
{
    switch (val.cellType()) {
    case CellValue::EMPTY:
        if (!hasNulls)
            throw HttpReturnException(500, "Unknown type for bucket");
        return 0;

    case CellValue::INTEGER:
    case CellValue::FLOAT:
        return numeric.getBucket(val.toDouble());
    case CellValue::ASCII_STRING:
    case CellValue::UTF8_STRING:
        return strings.getBucket(val);
    case CellValue::BLOB:
        return blobs.getBucket(val);
    case CellValue::TIMESTAMP:
        return timestamps.getBucket(val);
    case CellValue::TIMEINTERVAL:
        return intervals.getBucket(val);

    case CellValue::NUM_CELL_TYPES:
        break;
    }

    throw HttpReturnException(500, "Unknown CellValue type for getBucket()");
}

uint32_t
NumericValues::
getBucket(double val) const
{
    if (!active)
        throw HttpReturnException(500, "Attempt to get bucket from non-numeric value");
    return std::lower_bound(splits.begin(), splits.end(), val)
        - splits.begin() + offset;
}

uint32_t
OrdinalValues::
getBucket(const CellValue & val) const
{
    if (!active)
        throw HttpReturnException(500, "Attempt to get bucket from non-ordinal value");
    return std::lower_bound(splits.begin(), splits.end(), val)
        - splits.begin() + offset;
}

uint32_t
CategoricalValues::
getBucket(const CellValue & val) const
{
    auto it = std::find(buckets.begin(), buckets.end(), val);
    if (it != buckets.end())
        return it - buckets.begin() + offset;
    throw HttpReturnException(500, "categorical value '" + val.toString() + "' not found in col");
}

BucketDescriptions::
BucketDescriptions()
{
}

void
BucketDescriptions::
initialize(std::vector<CellValue> values, int numBuckets)
{
    std::vector<std::unordered_map<CellValue, size_t> >
        typeValues(CellValue::NUM_CELL_TYPES);
    
    // Segment by type of value
    for (auto & v: values) {
        typeValues.at((int)v.cellType())[v] += 1;
    }

    // Bucketize each type.  Strings can't be bucketized.
    size_t n = 0;
    if (!typeValues[CellValue::EMPTY].empty()) {
        this->hasNulls = true;
        ++n;  // value zero is for nulls
    }

    this->numeric.offset = n;
    this->numeric.active = false;
    this->numeric.splits.clear();

    // Bucketize numeric values
    std::vector<CellValue> numerics;
    for (auto & v: typeValues[CellValue::INTEGER])
        numerics.push_back(v.first);
    for (auto & v: typeValues[CellValue::FLOAT])
        numerics.push_back(v.first);
    std::sort(numerics.begin(), numerics.end());

    if (!numerics.empty()) {

        std::vector<float> splitPoints;
        std::vector<std::pair<float, float> > freqPairs;
        for (auto & n: numerics)
            freqPairs.emplace_back(n.toDouble(), 1);
        
        ML::BucketFreqs freqs(freqPairs.begin(), freqPairs.end());
        
        if (numBuckets != -1 && numerics.size() > numBuckets) {
            ML::bucket_dist_reduced(splitPoints, freqs, numBuckets);
        } else {
            ML::bucket_dist_full(splitPoints, freqs);
        }

        numerics.clear();
        numerics.insert(numerics.begin(), splitPoints.begin(), splitPoints.end());
        this->numeric.active = true;

        for (auto & split: numerics) {
            this->numeric.splits.emplace_back(split.toDouble());
            ++n;
        }

        n += 1;
    }

    // Get string values
    std::vector<CellValue> stringValues;
    for (auto type: { CellValue::ASCII_STRING, CellValue::UTF8_STRING } ) {
        for (auto & v: typeValues[(int)type])
            stringValues.emplace_back(v.first);
    }
    std::sort(stringValues.begin(), stringValues.end(),
              [&] (const CellValue & v1, const CellValue & v2)
              {
                  return v1.toUtf8String() < v2.toUtf8String();
              });

    this->strings.offset = n;
    for (auto & v: stringValues) {
        this->strings.buckets.emplace_back(std::move(v));
        ++n;
    }

    // TODO: complete this (MLDB-1457)
    
    for (auto type: { CellValue::BLOB, CellValue::TIMESTAMP, CellValue::TIMEINTERVAL } ) {
        if (!typeValues[(int)type].empty())
            throw HttpReturnException(500, "Can't bucketize blob, timestamp, interval types");
    }

    this->blobs.offset = this->timestamps.offset = this->intervals.offset = n;
    this->blobs.buckets.clear();
    this->timestamps.splits.clear();
    this->intervals.splits.clear();
    this->timestamps.active = this->intervals.active = false;

    ExcAssertEqual(n, this->numBuckets());
}

CellValue
BucketDescriptions::
getSplit(uint32_t bucket) const
{
    if (bucket < numeric.offset)
        return CellValue(); //empty
    else if (bucket < strings.offset)
        return numeric.splits[bucket - numeric.offset];
    else if (bucket < blobs.offset)
        return strings.buckets[bucket - strings.offset];

    throw HttpReturnException(500, "Invalid bucket");
}

bool 
BucketDescriptions::
isOnlyNumeric() const
{
    return this->strings.buckets.empty(); //todo: complete this
}

size_t
BucketDescriptions::
numBuckets() const
{
    return intervals.offset + intervals.numBuckets();
}

}
}