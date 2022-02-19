/* raw_mapped_int_table.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "bit_compressed_int_table.h"
#include "run_length_int_table.h"
#include "factored_int_table.h"

namespace MLDB {

struct RawMappedIntTable {

    RawMappedIntTable()
        : bitcmp_{}
    {
    }

    typedef uint32_t value_type;

    // Which one depends upon type.  Each option takes exactly 16 bytes, with the first 3
    // bits being reserved by the RawMappedIntTable to store the type.
    union {
        struct {
            IntTableType type_:3 = BIT_COMPRESSED;  // first 3 bits are type
            uint32_t unused_:29 = 0;
            uint32_t fields_[3];
        };
        InternalMappedBitCompressedIntTable<2> bitcmp_;  // first 3 bits are type
        MappedRunLengthIntTable rle_;                    // first 3 bits are type
        InternalMappedFactoredIntTable<1> factored_;     // first 3 bits are type
    };
    
    uint32_t at(uint32_t pos) const;

    uint32_t countValues(uint32_t startPos, uint32_t endPos, uint32_t value) const;

    bool empty() const
    {
        return size() == 0;
    }

    uint32_t size() const;

    using Iterator = IntTableIterator<const RawMappedIntTable, uint32_t>;

    Iterator begin() const { return Iterator{ {0U}, this}; }
    Iterator end() const { return Iterator{ {size()}, this}; }

    static size_t indirectBytesRequired(const IntTableStats<uint32_t> & stats, bool allow64Bits = allow64BitFactors);
    static constexpr bool indirectBytesRequiredIsExact = true;
};

size_t raw_mapped_indirect_bytes(const IntTableStats<uint32_t> & stats, bool allow64Bits = allow64BitFactors);
size_t raw_mapped_indirect_bytes(size_t size, uint32_t maxValue, uint32_t numRuns, bool allow64Bits = allow64BitFactors);
size_t raw_mapped_indirect_bytes(size_t size, uint32_t maxValue, bool allow64Bits = allow64BitFactors);

// For a given int table, return the most efficient type and the number of bytes of indirect bytes
// required to store it.
std::pair<IntTableType, size_t> useTableOfType(const IntTableStats<uint32_t> & stats, bool allow64Bits = allow64BitFactors);

void freeze(MappingContext & context, RawMappedIntTable & output, std::span<const uint32_t> input);
void freeze(MappingContext & context, RawMappedIntTable & output, std::span<const uint16_t> input);
void freeze(MappingContext & context, RawMappedIntTable & output, std::span<const uint8_t> input);

} // namespace MLDB
