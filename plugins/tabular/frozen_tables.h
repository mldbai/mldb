/** frozen_tables.h                                                -*- C++ -*-
    Jeremy Barnes, 12 June 2017
    Copyright (c) 2017 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

    Implementations of frozen storage for various constructs.
*/

#pragma once

#include "transducer.h"
#include "frozen_column.h"
#include "mldb/arch/bitops.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/arch/endian.h"
#include "mldb/sql/cell_value.h"
#include "mldb/compiler/string_view.h"
#include "int_table.h"
#include <span>


namespace MLDB {

/** How many bits are required to hold a number up from zero up to count - 1
    inclusive?
*/
inline uint8_t bitsToHoldCount(uint64_t count)
{
    return MLDB::highest_bit(std::max<uint64_t>(count, 1) - 1, -1) + 1;
}

inline uint8_t bitsToHoldRange(uint64_t count)
{
    return MLDB::highest_bit(count, -1) + 1;
}


/*****************************************************************************/
/* INTEGER TABLES                                                            */
/*****************************************************************************/

struct FrozenIntegerTableMetadata {
    size_t numEntries = 0;
    uint8_t entryBits = 0;
    int64_t offset = 0;
    double slope = 0.0f;
};

struct FrozenIntegerTable {

    FrozenIntegerTableMetadata md;
    FrozenMemoryRegionT<uint64_t> storage;

    size_t memusage() const;

    size_t size() const;

    uint64_t decode(uint64_t i, uint64_t val) const
    {
        return uint64_t(i * md.slope) + val + md.offset;
    }

    template<typename Fn>
    bool forEach(Fn && onVal) const
    {
        MLDB::Bit_Extractor<uint64_t> bits(storage.data());

        for (size_t i = 0;  i < md.numEntries;  ++i) {
            int64_t val = bits.extract<uint64_t>(md.entryBits);
            //cerr << "got " << val << " for entry " << i << endl;
            if (!onVal(i, decode(i, val)))
                return false;
        }
        return true;
    }

    template<typename Fn>
    bool forEachDistinctValue(Fn && onValue) const
    {
        auto fn2 = [&] (auto val, auto rowCount)
        {
            return onValue(val);
        };

        return forEachDistinctValueWithRowCount(fn2);
    }

    template<typename Fn>
    bool forEachDistinctValueWithRowCount(Fn && onValue) const
    {
        std::vector<uint64_t> allValues;
        allValues.reserve(size());
        forEach([&] (int, uint64_t val) { allValues.push_back(val); return true;});
        // TODO: shouldn't need to do 3 passes through, and we can also
        // make use of when it's monotonic...
        std::sort(allValues.begin(), allValues.end());

        for (auto it = allValues.begin(), endIt = allValues.end();  it != endIt; /* no inc */) {
            auto eit = it;
            ++eit;
            while (eit != endIt && *it == *eit) {
                ++eit;
            }
            if (!onValue(*it, std::distance(it, eit)))
                return false;
            it = eit;
        }

        return true;
    }

    uint64_t get(size_t i) const;
    uint64_t operator [] (size_t index) const { return get(index); }

    uint64_t getDefault(size_t i, uint64_t def = -1) const;
    
    void serialize(StructuredSerializer & serializer) const;

    void reconstitute(StructuredReconstituter & reconstituter);
};

struct MutableIntegerTable {
    uint64_t add(uint64_t val);

    void reserve(size_t numValues);
    
    size_t size() const;

    std::vector<uint64_t> values;
    uint64_t minValue = -1;
    uint64_t maxValue = 0;
    bool monotonic = true;

    size_t bytesRequired() const;

    FrozenIntegerTable freeze(MappedSerializer & serializer);

    std::pair<FrozenIntegerTable, std::vector<uint32_t>>
    freezeRemapped(MappedSerializer & serializer)
    {
        return { freeze(serializer), {} };
    }
};


/*****************************************************************************/
/* BLOB TABLES                                                               */
/*****************************************************************************/

struct FrozenBlobTable {
    FrozenBlobTable();
    ~FrozenBlobTable();
    
    size_t getSize(uint32_t index) const;
    size_t getBufferSize(uint32_t index) const;
    bool needsBuffer(uint32_t index) const;
    std::string_view
    getContents(uint32_t index,
                char * tempBuffer,
                size_t tempBufferSize) const;
    
    size_t memusage() const;
    size_t size() const;
    void serialize(StructuredSerializer & serializer) const;
    void reconstitute(StructuredReconstituter & reconstituter);

    CellValue operator [] (size_t index) const;

private:
    struct Itl;
    std::shared_ptr<Itl> itl;
    friend class MutableBlobTable;
};

struct MutableBlobTable {

    MutableBlobTable();
    
    size_t add(std::string && blob);
    size_t add(std::string_view blob);

    size_t size() const { return blobs.size(); }

    MutableIntegerTable offsets;
    std::vector<std::string> blobs;

    StringStats stats;
    
    FrozenBlobTable freeze(MappedSerializer & serializer);
    FrozenBlobTable freezeUncompressed(MappedSerializer & serializer);

    std::pair<FrozenBlobTable, std::vector<uint32_t>>
    freezeRemapped(MappedSerializer & serializer)
    {
        return { freeze(serializer), {} };
    }

};


/*****************************************************************************/
/* DOUBLE TABLE                                                              */
/*****************************************************************************/

struct FrozenDoubleTableMetadata {
};

struct FrozenDoubleTable: public FrozenDoubleTableMetadata {

    // Nullable or double entry
    struct Entry {
        
        Entry()
            : val(NULL_BITS)
        {
        }

        Entry(double d)
        {
            U u { .d = d };
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
            U u { .bits = val };
            return u.d;
        }

        operator CellValue() const
        {
            return isNull() ? CellValue() : value();
        }
    };

    FrozenBlobTable underlying;

    uint64_t memusage() const
    {
        return underlying.memusage();
    }

    double operator [] (size_t index) const
    {
        Entry result;
        char * rp = (char *)&result;
        auto blob = underlying.getContents(index, rp, sizeof(result));
        if (blob.empty())
            MLDB_THROW_LOGIC_ERROR("blob didn't return our double");

        if (blob.data() != rp) {
            std::memcpy(rp, blob.data(), sizeof(result));
        }
        return result.value();
    }
};

struct MutableDoubleTable {

    using Entry = FrozenDoubleTable::Entry;
    MutableBlobTable underlying;


    size_t add(double val)
    {
        Entry entry = val;
        return underlying.add(std::string_view{(const char *)&entry, sizeof(entry)});
    }

    size_t size() const { return underlying.size(); }

    FrozenDoubleTable freeze(MappedSerializer & serializer);

    std::pair<FrozenDoubleTable, std::vector<uint32_t>>
    freezeRemapped(MappedSerializer & serializer);
};


/*****************************************************************************/
/* STRING TABLES                                                             */
/*****************************************************************************/

struct FrozenStringTable {
    FrozenBlobTable underlying;

    Utf8String operator [] (size_t index) const
    {
        MLDB_THROW_UNIMPLEMENTED();
    }

    size_t memusage() const
    {
        return underlying.memusage();
    }
};

struct MutableStringTable {

    size_t add(const char * contents, size_t length);
    size_t add(std::string && contents);
    size_t add(const std::string & contents);

    MutableBlobTable underlying;

    size_t size() const { return underlying.size(); }

    FrozenStringTable freeze(MappedSerializer & serializer);
    std::pair<FrozenStringTable, std::vector<uint32_t>>
    freezeRemapped(MappedSerializer & serializer);
};


/*****************************************************************************/
/* PATH TABLES                                                               */
/*****************************************************************************/

struct FrozenPathTable {
    FrozenStringTable underlying;

    Path operator [] (size_t index) const
    {
        return Path::parse(underlying[index]);
    }

    uint64_t memusage() const { return underlying.memusage(); }
};

struct MutablePathTable {

    MutableStringTable underlying;

    size_t add(Path path)
    {
        return underlying.add(path.toUtf8String().stealRawString());
    }

    size_t size() const { return underlying.size(); }

    FrozenPathTable freeze(MappedSerializer & serializer);

    std::pair<FrozenPathTable, std::vector<uint32_t>>
    freezeRemapped(MappedSerializer & serializer);
};


/*****************************************************************************/
/* TIMESTAMP TABLES                                                          */
/*****************************************************************************/

struct FrozenTimestampTable {

    FrozenDoubleTable underlying;

    Date operator [] (size_t index) const
    {
        return Date::fromSecondsSinceEpoch(underlying[index]);
    }

    uint64_t memusage() const { return underlying.memusage(); }
};

struct MutableTimestampTable {

    MutableDoubleTable underlying;

    size_t size() const { return underlying.size(); }

    size_t add(Date ts)
    {
        return underlying.add(ts.secondsSinceEpoch());
    }

    FrozenTimestampTable freeze(MappedSerializer & serializer);
    std::pair<FrozenTimestampTable, std::vector<uint32_t>>

    freezeRemapped(MappedSerializer & serializer);
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
        auto fn2 = [&] (auto val, auto rowCount)
        {
            return fn(val);
        };
        return forEachDistinctValueWithRowCount(fn2);
    }

    template<typename Fn>
    bool forEachDistinctValueWithRowCount(Fn && fn) const
    {
        std::vector<CellValue> vals;
        vals.reserve(size());
        for (size_t i = 0;  i < size();  ++i) {
            vals.emplace_back(operator [] (i));
        }
        std::sort(vals.begin(), vals.end());
        for (size_t i = 0;  i < vals.size(); /* no inc */) {
            size_t i2 = i + 1;
            while (i2 < vals.size() && vals[i] == vals[i2])
                ++i2;
            if (!fn(vals[i], i2 - i))
                return false;
            i = i2;
        }
        return true;
    }

    void serialize(StructuredSerializer & serializer) const;
    void reconstitute(StructuredReconstituter & reconstituter);

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

    size_t size() const { return blobs.size(); }

    //const std::string & operator [] (size_t i) const
    //{
    //    return blobs.blobs.at(i);
    //}

    FrozenCellValueTable
    freeze(MappedSerializer & serializer);

    std::pair<FrozenCellValueTable, std::vector<uint32_t>>
    freezeRemapped(MappedSerializer & serializer)
    {
        return { freeze(serializer), {} };
    }

    MutableBlobTable blobs;
};


/*****************************************************************************/
/* CELL VALUE SET                                                            */
/*****************************************************************************/

struct FrozenCellValueSet {

    // How many bytes (approximately) to hold the given set of values?
    static size_t bytesForValues(std::span<const CellValue> vals);

    CellValue operator [] (size_t index) const;

    uint64_t memusage() const;

    size_t size() const
    {
        return startOffsets.get(startOffsets.size() - 1);
    }

    bool forEachDistinctValue(std::function<bool (CellValue &)> fn) const;

    void serialize(StructuredSerializer & serializer) const;
    void reconstitute(StructuredReconstituter & reconstituter);

    FrozenCellValueTable others;      ///< Index 0; ones that don't fit elsewhere
    FrozenIntegerTable intValues;     ///< Index 1
    FrozenDoubleTable doubleValues;   ///< Index 2
    FrozenTimestampTable timestampValues;   ///< Index 3
    FrozenStringTable stringValues;   ///< Index 4
    FrozenBlobTable blobValues;       ///< Index 5
    FrozenPathTable pathValues;       ///< Index 6

    FrozenIntegerTable startOffsets;
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

    void set(uint64_t index, const CellValue & val);

    std::pair<FrozenCellValueSet, std::vector<uint32_t> >
    freeze(MappedSerializer & serializer);

    struct IndexNumber {
        IndexNumber(uint32_t index = 0, uint32_t entry = 0)
            : index(index), entry(entry)
        {
        }

        uint32_t index:3;
        uint32_t entry:29;
    };

    std::vector<IndexNumber> indexes;

    enum {
        OTHER = 0,
        INT = 1,
        DOUBLE = 2,
        TIMESTAMP = 3,
        STRING = 4,
        BLOB = 5,
        PATH = 6,
        NUM_TYPES
    };
    
    MutableCellValueTable others;      ///< Index 0; ones that don't fit elsewhere
    MutableIntegerTable intValues;     ///< Index 1
    MutableDoubleTable doubleValues;   ///< Index 2
    MutableTimestampTable timestampValues;   ///< Index 3
    MutableStringTable stringValues;   ///< Index 4
    MutableBlobTable blobValues;       ///< Index 5
    MutablePathTable pathValues;       ///< Index 6
};


} // namespace MLDB
