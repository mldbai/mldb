/* factored_int_table.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mmap.h"
#include "int_table.h"
#include <bit>
#include <cmath>
#include <span>
#include "mldb/types/value_description_fwd.h"


namespace MLDB {

// Do we allow factors across 64 bit words?  This can slightly improve compression where
// one more will fit in 64 bits than 32 bits (factors of 5, 10, 13, 19, 30, 56, 138, 565,
// 7131, 2642245 with a maximum improvement of 33%) at the expense of larger code size and
// more expensive accesses.  32 bits is automatically used whenever possible without
// decreasing storage efficiency; this flag controls whether we can use 64 bits when it
// would improve storage efficiency.
static constexpr bool allow64BitFactors = false;

// Option to control this when serializing
extern MappingOption<bool> ALLOW_64_BIT_FACTORS;

// Describes how to size a factored int table
struct FactoredIntTableSolution {
    bool is64;                  // Do we use 64 bit words?  (false means 32 bit words)
    uint32_t factor;            // Factor we're packing into
    uint32_t numFactorsPerWord; // How many values per 64/32 bit word
    size_t bytesToAllocate;     // How much memory to allocate
};

// Solve the optimal factor packing for a range with the given metrics.
FactoredIntTableSolution factored_int_table_solution(size_t size, uint32_t maxValue, bool allow64Bits);

struct FactoredIntTableImpl;
struct MappedFactoredIntTableImpl;

struct FactoredIntTable {
    FactoredIntTable() = default;
    FactoredIntTable(uint32_t size, uint32_t maxValue, bool allow64Bits = allow64BitFactors)
    {
        initialize(size, maxValue, allow64Bits);
    }

    uint32_t size_ = 0;
    uint32_t factor_ = 0;
    uint32_t numFactors_ = 0;
    bool is64_ = true;
    
    std::vector<uint32_t> data_;
    using value_type = uint32_t;

    bool is64Bits() const { return is64_; }
    uint32_t size() const { return size_; }
    bool empty() const { return size_ == 0; }
    uint32_t factor() const { return factor_; }
    uint32_t numFactors() const { return numFactors_; }

    uint32_t at(uint32_t pos) const;
    uint32_t countValues(uint32_t startPos, uint32_t endPos, uint32_t value) const;
    void set(uint32_t pos, uint32_t value);
    void set_range(std::span<const uint32_t> vals, uint32_t startPos = 0);

    using Iterator = IntTableIterator<const FactoredIntTable>;

    Iterator begin() const { return { {0}, this}; }
    Iterator end() const { return { {size()}, this }; }

    std::pair<uint32_t, uint32_t> divModNumFactors(uint32_t val) const;
    std::pair<uint32_t, uint32_t> divModFactor32(uint32_t val) const;
    std::pair<uint64_t, uint32_t> divModFactor64(uint64_t val) const;
    uint32_t extractFactor32(uint32_t idx, uint32_t val) const;
    uint32_t extractFactor64(uint32_t idx, uint64_t val) const;

    uint32_t getWord32(size_t idx) const;
    uint64_t getWord64(size_t idx) const;

    void setWord32(size_t idx, uint32_t val);
    void setWord64(size_t idx, uint64_t val);

    const uint32_t * data() const { return data_.data(); }    
    uint32_t * data() { return data_.data(); }

    friend struct FactoredIntTableImpl;
    FactoredIntTableImpl & impl() { return reinterpret_cast<FactoredIntTableImpl &>(*this); }
    const FactoredIntTableImpl & impl() const { return reinterpret_cast<const FactoredIntTableImpl &>(*this); }
    void initialize(uint32_t size, uint32_t maxValue, bool allow64Bits);
};

struct MappedFactoredIntTable {
    union {
        struct {
            IntTableType type_:3 = FACTORED;
            uint32_t size_:22 = 0;
            uint32_t is64_:1 = 1;
            uint32_t numFactors_:6 = 0;
            uint32_t factor_ = 0;
        };
        uint32_t flags_;
    };

    union {
        MappedPtr<uint32_t> data_ = MappedPtr<uint32_t>();
        uint32_t internal_[1];
    };

    using value_type = const uint32_t;

    static constexpr size_t INTERNAL_OFFSET = 1024;  // if < 1024, it's stored internally

    uint32_t size() const { return size_ < INTERNAL_OFFSET ? size_ : size_ - INTERNAL_OFFSET; }
    bool empty() const { return size() == 0; }
    uint32_t factor() const { return factor_; }
    uint32_t numFactors() const { return numFactors_; }
    bool is64Bits() const { return is64_; }

    uint32_t at(uint32_t pos) const;
    uint32_t countValues(uint32_t startPos, uint32_t endPos, uint32_t value) const;

    using Iterator = IntTableIterator<const MappedFactoredIntTable>;

    Iterator begin() const { return { {0}, this}; }
    Iterator end() const { return { {size()}, this }; }

    std::pair<uint32_t, uint32_t> divModNumFactors(uint32_t val) const;
    std::pair<uint32_t, uint32_t> divModFactor32(uint32_t val) const;
    std::pair<uint64_t, uint32_t> divModFactor64(uint64_t val) const;
    uint32_t extractFactor32(uint32_t idx, uint32_t val) const;
    uint32_t extractFactor64(uint32_t idx, uint64_t val) const;

    uint32_t getWord32(size_t idx) const;
    uint64_t getWord64(size_t idx) const;

    const uint32_t * data() const { return data_.get(); }   

    friend struct MappedFactoredIntTableImpl;
    const MappedFactoredIntTableImpl & impl() const { return reinterpret_cast<const MappedFactoredIntTableImpl &>(*this); }

    static size_t indirectBytesRequired(const IntTableStats<uint32_t> & stats, bool allow64Bits = allow64BitFactors);
    static constexpr bool indirectBytesRequiredIsExact = true;
};

template<size_t ExtraWords>
struct InternalMappedFactoredIntTable: public MappedFactoredIntTable {
    static_assert(ExtraWords <= 2, "Extra words must be less than three");
    static_assert(ExtraWords > 0,  "Extra words must be greater than zero");
    uint32_t extra_[ExtraWords];

    static size_t indirectBytesRequired(const IntTableStats<uint32_t> & stats, bool allow64Bits = allow64BitFactors);
    static constexpr bool indirectBytesRequiredIsExact = true;
};

template<size_t ExtraWords> struct InternalMappedFactoredIntTableDescription;
DECLARE_TEMPLATE_VALUE_DESCRIPTION_1(InternalMappedFactoredIntTableDescription, InternalMappedFactoredIntTable, size_t, ExtraWords, true /* enable */);

extern template struct InternalMappedFactoredIntTable<1>;
extern template struct InternalMappedFactoredIntTable<2>;

void freeze(MappingContext & context, MappedFactoredIntTable & output, const FactoredIntTable & input);
void freeze(MappingContext & context, InternalMappedFactoredIntTable<1> & output, const FactoredIntTable & input);
void freeze(MappingContext & context, InternalMappedFactoredIntTable<2> & output, const FactoredIntTable & input);

FactoredIntTable factored_encode(const std::span<const uint32_t> & values, bool trace = false);

void freeze(MappingContext & context, MappedFactoredIntTable & output, const std::span<const uint32_t> & input);
void freeze(MappingContext & context, InternalMappedFactoredIntTable<1> & output, const std::span<const uint32_t> & input);
void freeze(MappingContext & context, InternalMappedFactoredIntTable<2> & output, const std::span<const uint32_t> & input);

size_t factored_table_indirect_bytes(const IntTableStats<uint32_t> & stats, bool allow64Bits = allow64BitFactors);
size_t factored_table_indirect_bytes(size_t size, uint32_t maxValue, bool allow64Bits = allow64BitFactors);

DECLARE_STRUCTURE_DESCRIPTION(MappedFactoredIntTable);

} // namespace MLDB
