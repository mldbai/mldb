/* bit_compressed_bit_table.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "int_table.h"
#include "mmap.h"
#include "mldb/arch/demangle.h"
#include "mldb/types/value_description_fwd.h"

namespace MLDB {

// Hidden in bit_compressed_int_table_impl.hpp to avoid long compilation times
struct BitCompressedIntTableImpl;
struct MappedBitCompressedIntTableImpl;

struct BitCompressedIntTable {
    uint32_t size_ = 0;
    uint8_t width_ = 0;
    std::vector<uint32_t> data_;
    using value_type = uint32_t;
    uint32_t width() const { return width_; }
    uint32_t getWord(size_t n) const { return data_.at(n); }
    void setWord(size_t n, uint32_t value) { data_.at(n) = value; }
    uint32_t * data() { return data_.data(); }

    uint32_t at(uint32_t pos) const;
    uint32_t countValues(uint32_t startPos, uint32_t endPos, uint32_t value) const;
    uint32_t size() const { return size_; }
    bool empty() const { return size_ == 0; }

    void set(uint32_t pos, uint32_t value);

    using Iterator = IntTableIterator<const BitCompressedIntTable>;

    Iterator begin() const { return { {0}, this}; }
    Iterator end() const { return { {size()}, this }; }

    friend struct BitCompressedIntTableImpl;
    BitCompressedIntTableImpl & impl() { return reinterpret_cast<BitCompressedIntTableImpl &>(*this); }
    const BitCompressedIntTableImpl & impl() const { return reinterpret_cast<const BitCompressedIntTableImpl &>(*this); }
};

struct MappedBitCompressedIntTable {
    MappedBitCompressedIntTable()
        : type_(BIT_COMPRESSED), size_(0), width_(0)
    {
    }

    union {
        struct {
            IntTableType type_:3 = BIT_COMPRESSED;  // used externally, don't use here
            uint32_t size_:23 = 0;  // If <= 1024, it's internal storage only, otherwise external
            uint32_t width_:6 = 0;
        };
        uint32_t flags_;
        uint32_t bits_[1];
    };
    uint32_t width() const { return width_; }
    uint32_t getWord(size_t n) const;

    //const uint32_t * data() const { return data_.get(); }

    static constexpr uint32_t INTERNAL_OFFSET = 1024;

    union {
        MappedPtr<uint32_t> data_ = MappedPtr<uint32_t>();
        uint32_t internal_[1];  // may be more words in subclass
    };

    using value_type = const uint32_t;
    using size_type = uint32_t;

    uint32_t at(uint32_t pos) const;
    uint32_t countValues(uint32_t startPos, uint32_t endPos, uint32_t value) const;
    uint32_t size() const { return size_ < INTERNAL_OFFSET ? size_ : size_ - INTERNAL_OFFSET; }
    bool empty() const { return size() == 0; }

    using Iterator = IntTableIterator<const MappedBitCompressedIntTable>;

    Iterator begin() const { return { {0}, this}; }
    Iterator end() const { return { {size()}, this }; }

    static size_t indirectBytesRequired(const IntTableStats<uint32_t> & stats);
    static constexpr bool indirectBytesRequiredIsExact = true;

    friend struct MappedBitCompressedIntTableImpl;
    const MappedBitCompressedIntTableImpl & impl() const { return reinterpret_cast<const MappedBitCompressedIntTableImpl &>(*this); }
};

DECLARE_STRUCTURE_DESCRIPTION(MappedBitCompressedIntTable);

template<size_t ExtraWords>
struct InternalMappedBitCompressedIntTable: public MappedBitCompressedIntTable {
    static_assert(ExtraWords <= 3, "Extra words must be less than three");
    static_assert(ExtraWords > 0,  "Extra words must be greater than zero");
    uint32_t extra_[ExtraWords];

    static size_t indirectBytesRequired(const IntTableStats<uint32_t> & stats);
    static constexpr bool indirectBytesRequiredIsExact = true;
};

template<size_t ExtraWords> struct InternalMappedBitCompressedIntTableDescription;
DECLARE_TEMPLATE_VALUE_DESCRIPTION_1(InternalMappedBitCompressedIntTableDescription, InternalMappedBitCompressedIntTable, size_t, ExtraWords, true /* enable */);

extern template struct InternalMappedBitCompressedIntTable<1>;
extern template struct InternalMappedBitCompressedIntTable<2>;
extern template struct InternalMappedBitCompressedIntTable<3>;

void freeze(MappingContext & context, MappedBitCompressedIntTable & output, const BitCompressedIntTable & input);
void freeze(MappingContext & context, InternalMappedBitCompressedIntTable<1> & output, const BitCompressedIntTable & input);
void freeze(MappingContext & context, InternalMappedBitCompressedIntTable<2> & output, const BitCompressedIntTable & input);
void freeze(MappingContext & context, InternalMappedBitCompressedIntTable<3> & output, const BitCompressedIntTable & input);

size_t bit_compressed_indirect_bytes(const IntTableStats<uint32_t> & stats);
size_t bit_compressed_indirect_bytes(size_t size, uint32_t maxValue);

BitCompressedIntTable bit_compressed_encode(const std::span<const uint32_t> & values);

void freeze(MappingContext & context, MappedBitCompressedIntTable & output, const std::span<const uint32_t> & input);
void freeze(MappingContext & context, InternalMappedBitCompressedIntTable<1> & output, const std::span<const uint32_t> & input);
void freeze(MappingContext & context, InternalMappedBitCompressedIntTable<2> & output, const std::span<const uint32_t> & input);
void freeze(MappingContext & context, InternalMappedBitCompressedIntTable<3> & output, const std::span<const uint32_t> & input);

} // namespace MLDB
