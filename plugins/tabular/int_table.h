/* int_table.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <cctype>
#include <compare>
#include <iostream>
#include <cassert>
#include <span>
#include "mldb/utils/ostream_vector.h"
#include "mldb/utils/min_max.h"
#include "mldb/arch/demangle.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/base/exc_assert.h"


namespace MLDB {

struct IntTableStatsBase {
    size_t size = 0;
    size_t numRuns = 0;
    size_t maxRunLength = 0;
    size_t minElement = MAX_LIMIT, maxElement = MIN_LIMIT, minElementCount = 0, maxElementCount = 0;

protected:
    // From here on, they are part of the accumulation; not for the client to use
    size_t runLength = 0;
};

template<typename Int>
struct IntTableStats: public IntTableStatsBase {
    IntTableStats() = default;

    template<typename Container>
    IntTableStats(const Container & input, std::enable_if_t<std::is_convertible_v<Container, std::span<const Int>>> * = nullptr)
    {
        init(input);
    }

    IntTableStats(size_t size, Int minValue, Int maxValue, size_t numRuns, size_t maxRunLength)
    {
       this->size = size;
       this->minValue = minValue;
       this->maxValue = maxValue;
       this->numRuns = numRuns;
       this->maxRunLength = maxRunLength;
    }

    template<typename OtherInt>
    IntTableStats(const IntTableStats<OtherInt> & other)
        : IntTableStatsBase(other)
    {
        minValue = other.minValue;  ExcAssert(minValue == other.minValue);
        maxValue = other.maxValue;  ExcAssert(maxValue == other.maxValue);
    }

    void init(const std::span<const Int> & input);
    void add(Int val);

    template<typename OtherInt>
    IntTableStats & operator = (const IntTableStats<OtherInt> & other)
    {
        IntTableStatsBase::operator = (other);
        minValue = other.minValue;  ExcAssert(minValue == other.minValue);
        maxValue = other.maxValue;  ExcAssert(maxValue == other.maxValue);
        return *this;
    }

    Int minValue = MAX_LIMIT, maxValue = MIN_LIMIT;

protected:
    // Part of the accumulation; not for clients to use
    Int lastValue = 0;
};

std::ostream & operator << (std::ostream & stream, const IntTableStats<int64_t> & stats);

extern template struct IntTableStats<uint64_t>;
extern template struct IntTableStats<int64_t>;
extern template struct IntTableStats<uint32_t>;
extern template struct IntTableStats<int32_t>;
extern template struct IntTableStats<uint16_t>;
extern template struct IntTableStats<int16_t>;
extern template struct IntTableStats<uint8_t>;
extern template struct IntTableStats<int8_t>;

enum IntTableType: uint32_t {
    BIT_COMPRESSED = 0,
    RLE = 1,
    FACTORED = 2
};

DECLARE_ENUM_DESCRIPTION(IntTableType);

std::ostream & operator << (std::ostream & stream, IntTableType type);

struct IntTableIteratorBase {
    uint32_t pos = 0;

    using iterator_category = std::random_access_iterator_tag;
    using difference_type = int32_t;
    using pointer = const uint32_t *;
    using reference = const uint32_t &;

    auto operator <=> (const IntTableIteratorBase & other) const = default;
};

template<typename Owner, typename ValueT = typename Owner::value_type>
struct IntTableIterator: public IntTableIteratorBase {
    using value_type = ValueT;
    Owner * owner = nullptr;

    value_type operator * () const { return owner->at(pos); }

    IntTableIterator & operator++() { pos += 1; return *this; }
    IntTableIterator & operator--() { pos -= 1; return *this; }
    IntTableIterator & operator += (int n) { pos += n;  return *this; }
    IntTableIterator & operator -= (int n) { pos -= n;  return *this; }

    difference_type operator - (const IntTableIterator & other) const { return pos - other.pos; }
};

} // namespace MLDB
