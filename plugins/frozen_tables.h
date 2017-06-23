/** frozen_tables.h                                                -*- C++ -*-
    Jeremy Barnes, 12 June 2017
    Copyright (c) 2017 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

    Implementations of frozen storage for various constructs.
*/

#pragma once

#include "frozen_column.h"
#include "mldb/arch/bitops.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/arch/endian.h"

namespace MLDB {

/** How many bits are required to hold a number up from zero up to count - 1
    inclusive?
*/
inline uint8_t bitsToHoldCount(uint64_t count)
{
    return ML::highest_bit(std::max<uint64_t>(count, 1) - 1, -1) + 1;
}

inline uint8_t bitsToHoldRange(uint64_t count)
{
    return ML::highest_bit(count, -1) + 1;
}


/*****************************************************************************/
/* INTEGER TABLES                                                            */
/*****************************************************************************/

struct FrozenIntegerTable {

    FrozenMemoryRegionT<uint64_t> storage;
    size_t numEntries = 0;
    uint8_t entryBits = 0;
    int64_t offset = 0;
    double slope = 0.0f;

    size_t memusage() const
    {
        return storage.memusage();
    }

    size_t size() const
    {
        return numEntries;
    }

    uint64_t decode(uint64_t i, uint64_t val) const
    {
        return uint64_t(i * slope) + val + offset;
    }

    template<typename Fn>
    bool forEach(Fn && onVal) const
    {
        ML::Bit_Extractor<uint64_t> bits(storage.data());

        for (size_t i = 0;  i < numEntries;  ++i) {
            int64_t val = bits.extract<uint64_t>(entryBits);
            //cerr << "got " << val << " for entry " << i << endl;
            if (!onVal(i, decode(i, val)))
                return false;
        }
        return true;
    }

    template<typename Fn>
    bool forEachDistinctValue(Fn && onValue) const
    {
        std::vector<uint64_t> allValues;
        allValues.reserve(size());
        forEach([&] (int, uint64_t val) { allValues.push_back(val); return true;});
        // TODO: shouldn't need to do 3 passes through, and we can also
        // make use of when it's monotonic...
        std::sort(allValues.begin(), allValues.end());
        auto endIt = std::unique(allValues.begin(), allValues.end());

        for (auto it = allValues.begin();  it != endIt; ++it) {
            if (!onValue(*it))
                return false;
        }

        return true;
    }

    uint64_t get(size_t i) const
    {
        ExcAssertLess(i, numEntries);
        ML::Bit_Extractor<uint64_t> bits(storage.data());
        bits.advance(i * entryBits);
        int64_t val = bits.extract<uint64_t>(entryBits);
        //cerr << "getting element " << i << " gave val " << val
        //     << " yielding " << decode(i, val) << " with offset "
        //     << offset << " and slope " << slope << endl;
        return decode(i, val);
    }

    void serialize(MappedSerializer & serializer) const
    {
        storage.reserialize(serializer);
    }
};

struct MutableIntegerTable {
    uint64_t add(uint64_t val)
    {
        values.emplace_back(val);
        minValue = std::min(minValue, val);
        monotonic = monotonic && val >= maxValue;
        maxValue = std::max(maxValue, val);
        return values.size() - 1;
    }

    void reserve(size_t numValues)
    {
        values.reserve(numValues);
    }
    
    size_t size() const
    {
        return values.size();
    }

    std::vector<uint64_t> values;
    uint64_t minValue = -1;
    uint64_t maxValue = 0;
    bool monotonic = true;

    size_t bytesRequired() const
    {
        // TODO: calculate with slope
        uint64_t range = maxValue - minValue;
        uint8_t bits = bitsToHoldRange(range);
        size_t numWords = (bits * values.size() + 63) / 64;
#if 0
        cerr << "**** MIT bytes required" << endl;
        cerr << "range = " << range << " minValue = " << minValue
             << " maxValue = " << maxValue << " bits = " << bits
             << " numWords = " << numWords << " values.size() = "
             << values.size() << endl;
#endif
        return numWords * 8;
    }

    FrozenIntegerTable freeze(MappedSerializer & serializer)
    {
        FrozenIntegerTable result;
        uint64_t range = maxValue - minValue;
        uint8_t bits = bitsToHoldRange(range);

#if 0
        cerr << "*** Freezing integer table" << endl;
        cerr << "minValue = " << minValue << " maxValue = "
             << maxValue << " range = " << range << endl;
        cerr << "bits = " << (int)bits << endl;
#endif
        result.offset = minValue;
        result.entryBits = bits;
        result.numEntries = values.size();
        result.slope = 0.0;

        if (values.size() > 1 && monotonic) {
            // TODO: what we are really trying to do here is find the
            // slope and intercept such that all values are above
            // the line, and the infinity norm is minimised.  We can
            // do that in a more principled way...
            double slope = (values.back() - values[0]) / (values.size() - 1.0);

            //static std::mutex mutex;
            //std::unique_lock<std::mutex> guard(mutex);
            
            //cerr << "monotonic " << values.size() << " from "
            //     << minValue << " to " << maxValue << " has slope "
            //     << slope << endl;

            uint64_t maxNegOffset = 0, maxPosOffset = 0;
            for (size_t i = 1;  i < values.size();  ++i) {
                uint64_t predicted = minValue + i * slope;
                uint64_t actual = values[i];

                //cerr << "i = " << i << " predicted " << predicted
                //     << " actual " << actual << endl;

                if (predicted < actual) {
                    maxPosOffset = std::max(maxPosOffset, actual - predicted);
                }
                else {
                    maxNegOffset = std::max(maxNegOffset, predicted - actual);
                }
            }

            uint8_t offsetBits = bitsToHoldCount(maxNegOffset + maxPosOffset + 2);
            if (offsetBits < bits) {
                result.offset = minValue - maxNegOffset;
                result.entryBits = offsetBits;
                result.slope = slope;

#if 0
                cerr << "integer range with slope " << slope
                     << " goes from " << (int)bits << " to "
                     << (int)offsetBits << " bits per entry" << endl;
                cerr << "maxNegOffset = " << maxNegOffset << endl;
                cerr << "maxPosOffset = " << maxPosOffset << endl;
                cerr << "minValue = " << minValue << endl;
                cerr << "offset = " << result.offset << endl;
                cerr << "slope = " << result.slope << endl;
#endif
            }
        }

        size_t numWords = (result.entryBits * values.size() + 63) / 64;
        auto mutableStorage = serializer.allocateWritableT<uint64_t>(numWords);
        uint64_t * data = mutableStorage.data();

        ML::Bit_Writer<uint64_t> writer(data);
        for (size_t i = 0;  i < values.size();  ++i) {
            uint64_t predicted = result.offset + uint64_t(i * result.slope);
            uint64_t residual = values[i] - predicted;
            //cerr << "value = " << values[i] << endl;
            //cerr << "predicted = " << predicted << endl;
            //cerr << "storing residual " << residual << " at " << i << endl;

            if (result.slope != 0.0) {
                //cerr << "predicted " << predicted << " val " << values[i]
                //     << endl;
                //cerr << "residual " << residual << " for entry " << i << endl;
            }
            writer.write(residual, result.entryBits);
        }

        values.clear();
        values.shrink_to_fit();

        result.storage = mutableStorage.freeze();

        return result;
    }
};


/*****************************************************************************/
/* DOUBLE TABLE                                                              */
/*****************************************************************************/

struct FrozenDoubleTable {

    // Nullable or double entry
    struct Entry {
        
        Entry()
            : val(NULL_BITS)
        {
        }

        Entry(double d)
        {
            U u { d: d };
            val = u.bits;
        }

        uint64_le val;

        static const uint64_t NULL_BITS
            = 0ULL  << 63 // sign
            | (0x7ffULL << 53) // exponent is all 1s for NaN
            | (0xe1a1ULL); // mantissa

        // Type-punning union declared once here so we don't need to
        // do so everywhere else anonymously.
        union U {
            double d;
            uint64_t bits;
        };

        bool isNull() const
        {
            return val == NULL_BITS;
        }

        double value() const
        {
            U u { bits: val };
            return u.d;
        }

        operator CellValue() const
        {
            return isNull() ? CellValue() : value();
        }
    };
};

struct MutableDoubleTable {

    size_t add(double val);

    FrozenDoubleTable freeze(MappedSerializer & serializer);
};



/*****************************************************************************/
/* BLOB TABLES                                                               */
/*****************************************************************************/

struct FrozenBlobTable {
    FrozenBlobTable();

    enum Format {
        UNCOMPRESSED = 0,
        ZSTD = 1
    };

    uint8_t format = UNCOMPRESSED;
    FrozenMemoryRegion formatData;
    FrozenMemoryRegion blobData;
    FrozenIntegerTable offset;

    size_t getSize(uint32_t index) const;
    size_t getBufferSize(uint32_t index) const;
    bool needsBuffer(uint32_t index) const;
    const char * getContents(uint32_t index,
                             char * tempBuffer,
                             size_t tempBufferSize) const;
    
    size_t memusage() const;
    size_t size() const;
    void serialize(MappedSerializer & serializer) const;

    struct Itl;
    std::shared_ptr<Itl> itl;
};


struct MutableBlobTable {

    size_t add(std::string blob);

    size_t size() const { return blobs.size(); }

    MutableIntegerTable offsets;
    std::vector<std::string> blobs;
    uint64_t totalBytes = 0;

    FrozenBlobTable freeze(MappedSerializer & serializer);

    FrozenBlobTable freezeCompressed(MappedSerializer & serializer);
    FrozenBlobTable freezeUncompressed(MappedSerializer & serializer);
};


/*****************************************************************************/
/* STRING TABLES                                                             */
/*****************************************************************************/

struct FrozenStringTable {
    FrozenBlobTable blobs;
};

struct MutableStringTable {

    size_t add(const char * contents, size_t length);
    size_t add(std::string && contents);
    size_t add(const std::string & contents);

    std::vector<std::pair<const char *, size_t> > strings;
    std::vector<std::string> ownedStrings;

    FrozenStringTable freeze(MappedSerializer & serializer);
};


/*****************************************************************************/
/* PATH TABLES                                                               */
/*****************************************************************************/

struct FrozenPathTable {
    FrozenBlobTable blobs;
};

struct MutablePathTable {

    std::vector<Path> paths;

    size_t add(Path path);

    FrozenPathTable freeze(MappedSerializer & serializer);
};


/*****************************************************************************/
/* TIMESTAMP TABLES                                                          */
/*****************************************************************************/

struct FrozenTimestampTable {
};

struct MutableTimestampTable {

    std::vector<Date> timestamp;

    size_t add(Date ts);

    FrozenTimestampTable freeze(MappedSerializer & serializer);
};


/*****************************************************************************/
/* CELL VALUE TABLE                                                          */
/*****************************************************************************/

struct FrozenCellValueTable {
    CellValue operator [] (size_t index) const;

    uint64_t memusage() const;

    size_t size() const;

    template<typename Fn>
    bool forEachDistinctValue(Fn && fn) const
    {
        std::vector<CellValue> vals;
        vals.reserve(size());
        for (size_t i = 0;  i < size();  ++i) {
            vals.emplace_back(operator [] (i));
        }
        std::sort(vals.begin(), vals.end());
        for (size_t i = 0;  i < vals.size();  ++i) {
            if (i > 0 && vals[i] == vals[i - 1])
                continue;
            if (!fn(vals[i]))
                return false;
        }
        return true;
    }

    void serialize(MappedSerializer & serializer) const;

    FrozenBlobTable blobs;
};

struct MutableCellValueTable {
    MutableCellValueTable()
    {
    }

    template<typename It>
    MutableCellValueTable(It begin, It end)
    {
        reserve(std::distance(begin, end));
        for (auto it = begin; it != end;  ++it) {
            add(std::move(*it));
        }
    }

    void reserve(size_t numValues);

    size_t add(const CellValue & val);

    void set(uint64_t index, const CellValue & val);

    FrozenCellValueTable
    freeze(MappedSerializer & serializer);

    MutableBlobTable blobs;
};


/*****************************************************************************/
/* CELL VALUE SET                                                            */
/*****************************************************************************/

struct FrozenCellValueSet {

    CellValue operator [] (size_t index) const
    {
        static uint8_t format
            = CellValue::serializationFormat(true /* known length */);
        size_t offset0 = (index == 0 ? 0 : offsets.get(index - 1));
        size_t offset1 = offsets.get(index);

        const char * data = cells.data() + offset0;
        size_t len = offset1 - offset0;
        return CellValue::reconstitute(data, len, format, true /* known length */).first;
    }

    uint64_t memusage() const
    {
        return offsets.memusage() + cells.memusage() + sizeof(*this);
    }

    size_t size() const
    {
        return offsets.size();
    }

    template<typename Fn>
    bool forEachDistinctValue(Fn && fn) const
    {
        std::vector<CellValue> vals;
        vals.reserve(size());
        for (size_t i = 0;  i < size();  ++i) {
            vals.emplace_back(operator [] (i));
        }
        std::sort(vals.begin(), vals.end());
        for (size_t i = 0;  i < vals.size();  ++i) {
            if (i > 0 && vals[i] == vals[i - 1])
                continue;
            if (!fn(vals[i]))
                return false;
        }
        return true;
    }

    void serialize(MappedSerializer & serializer) const
    {
        offsets.serialize(serializer);
        cells.reserialize(serializer);
    }

    FrozenIntegerTable offsets;
    FrozenMemoryRegion cells;
};

struct MutableCellValueSet {
    MutableCellValueSet()
    {
    }

    template<typename It>
    MutableCellValueSet(It begin, It end)
    {
        reserve(std::distance(begin, end));
        for (auto it = begin; it != end;  ++it) {
            add(std::move(*it));
        }
    }

    void reserve(size_t numValues)
    {
        // TODO
    }

    void add(CellValue val);

    std::pair<FrozenCellValueSet, std::vector<uint32_t> >
    freeze(MappedSerializer & serializer);

    struct IndexNumber {
        IndexNumber(uint32_t index = 0, uint32_t entry = 0)
            : index(index), entry(entry)
        {
        }

        uint32_t index:3;
        uint32_t entry:30;
    };

    std::vector<IndexNumber> indexes;

    enum {
        OTHER = 0,
        INT = 1,
        DOUBLE = 2,
        TIMESTAMP = 3,
        STRING = 4,
        BLOB = 5,
        PATH = 6
    };
    
    std::vector<CellValue> others;     ///< Index 0; ones that don't fit elsewhere
    MutableIntegerTable intValues;     ///< Index 1
    MutableDoubleTable doubleValues;   ///< Index 2
    MutableTimestampTable timestampValues;   ///< Index 3
    MutableStringTable stringValues;   ///< Index 4
    MutableBlobTable blobValues;       ///< Index 5
    MutablePathTable pathValues;       ///< Index 6
};



} // namespace MLDB
