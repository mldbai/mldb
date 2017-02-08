/** bucket.cc                                                       -*- C++ -*-
    Mathieu Marquis Bolduc, March 11th 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Structures to bucketize sets of cellvalues
*/

#include "bucket.h"
#include "mldb/arch/bitops.h"
#include "mldb/sql/cell_value.h"
#include "mldb/http/http_exception.h"
#include "mldb/ml/jml/buckets.h"
#include "mldb/base/exc_assert.h"
#include "mldb/types/string.h"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <cstring>


namespace MLDB {

namespace {

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
    case CellValue::PATH:
        return paths.getBucket(val);
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

struct CompareStrs {
    bool operator () (const Utf8String & str1, const char * str2) const
    {
        return strcmp(str1.rawData(), str2) < 0;
    }
};

uint32_t
StringValues::
getBucket(const CellValue & val) const
{
    const char * str = val.stringChars();
    auto it = std::lower_bound(buckets.begin(), buckets.end(), str, CompareStrs());
    if (it == buckets.end() || *it != str)
        throw HttpReturnException(500, "categorical value '" + val.toString() + "' not found in col");
    return it - buckets.begin() + offset;
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

    bool hasNulls = false;
    // Bucketize each type.  Strings can't be bucketized.
    size_t n = 0;
    if (!typeValues[CellValue::EMPTY].empty()) {
        hasNulls = true;
        ++n;  // value zero is for nulls
    }

    // Bucketize numeric values
    std::vector<double> numerics;
    for (auto & v: typeValues[CellValue::INTEGER])
        numerics.push_back(v.first.toDouble());
    for (auto & v: typeValues[CellValue::FLOAT])
        numerics.push_back(v.first.toDouble());
    
    // Get string values
    std::vector<Utf8String> stringValues;
    for (auto type: { CellValue::ASCII_STRING, CellValue::UTF8_STRING } ) {
        for (auto & v: typeValues[(int)type])
            stringValues.emplace_back(v.first.toUtf8String());
    }

    initialize(hasNulls, std::move(numerics), std::move(stringValues),
               numBuckets);
}

void
BucketDescriptions::
initialize(bool hasNulls,
           std::vector<double> numericValues,
           std::vector<Utf8String> stringValues,
           int numBuckets)
{
    this->hasNulls = hasNulls;
    size_t n = hasNulls;
    this->numeric.offset = n;
    this->numeric.active = false;
    this->numeric.splits.clear();

    if (!numericValues.empty()) {
        std::sort(numericValues.begin(), numericValues.end());
        numericValues.erase(std::unique(numericValues.begin(),
                                        numericValues.end()),
                            numericValues.end());
        std::vector<float> splitPoints;
        std::vector<std::pair<float, float> > freqPairs;
        for (auto & n: numericValues)
            freqPairs.emplace_back(n, 1);
        
        ML::BucketFreqs freqs(freqPairs.begin(), freqPairs.end());
        
        if (numBuckets != -1 && numericValues.size() > numBuckets) {
            ML::bucket_dist_reduced(splitPoints, freqs, numBuckets);
        } else {
            ML::bucket_dist_full(splitPoints, freqs);
        }

        numericValues.clear();
        numericValues.insert(numericValues.begin(),
                             splitPoints.begin(), splitPoints.end());
        this->numeric.active = true;
        this->numeric.splits = std::move(numericValues);
        n += this->numeric.splits.size() + 1;
    }
    
    // Get string values
    std::sort(stringValues.begin(), stringValues.end());
    stringValues.erase(std::unique(stringValues.begin(),
                                   stringValues.end()),
                       stringValues.end());

    this->strings.offset = n;
    n += stringValues.size();
    this->strings.buckets = std::move(stringValues);

    // TODO: complete this (MLDB-1457)

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

        
std::tuple<BucketList, BucketDescriptions>
BucketDescriptions::
merge(const std::vector<std::tuple<BucketList, BucketDescriptions> > & inputs,
      int numBuckets)
{
    BucketDescriptions desc;

    std::vector<double> allNumeric;
    std::unordered_set<Utf8String> allStrings;

    size_t totalRows = 0;
    bool hasNulls = false;
    for (auto & i: inputs) {
        const BucketDescriptions & d = std::get<1>(i);
        hasNulls = hasNulls || d.hasNulls;
        allNumeric.insert(allNumeric.end(),
                          d.numeric.splits.begin(), d.numeric.splits.end());
        for (auto & s: d.strings.buckets) {
            allStrings.insert(s);
        }
        totalRows += std::get<0>(i).rowCount();
    }
    
    std::vector<Utf8String> stringValues(allStrings.begin(), allStrings.end());
    
    desc.initialize(hasNulls, std::move(allNumeric),
                    std::move(stringValues), numBuckets);
    
    // Finally, perform the bucketed lookup
    WritableBucketList buckets(totalRows, desc.numBuckets());

    for (auto & i: inputs) {
        const BucketDescriptions & d = std::get<1>(i);
        std::vector<int> oldToNew(d.numBuckets());
        for (size_t j = 0;  j < d.numBuckets();  ++j) {
            oldToNew[j] = desc.getBucket(d.getSplit(j));
        }

        const BucketList & l = std::get<0>(i);
        for (size_t j = 0;  j < l.numEntries;  ++j) {
            buckets.write(oldToNew.at(l[j]));
        }
    }

    return std::make_tuple(std::move(buckets), std::move(desc));
}

} // namespace MLDB

