/* bit_rank.h                                                 -*- C++ -*-
   Jeremy Barnes, 23 March 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Calculates how many set bits in a range.
*/

#pragma once

#include <type_traits>
#include <span>
#include "mldb/compiler/compiler.h"
#include "mldb/arch/exception.h"

namespace MLDB {

/// Find how many 1 bits there are in the x first bits of the array (inclusive).
/// So brank(0, {1}) == 1
/// Inverse of bit_select()
template<typename Unsigned>
inline uint32_t bit_rank(uint32_t x, std::span<const Unsigned> mem)
{
    static_assert(std::is_unsigned_v<Unsigned>, "brank only works on unsigned integral types");
    static constexpr size_t NBits = sizeof(Unsigned) * 8;
    // Naive... to be optimized
    // http://bitmagic.io/rank-select.html
    size_t p = x / NBits;  // word number
    uint32_t q = x % NBits;  // bit number within word
    uint32_t result = 0;

    if (MLDB_UNLIKELY(p >= mem.size()))
        MLDB_THROW_RANGE_ERROR("brank");

    // Scan a word at a time
    for (size_t i = 0;  i < p;  ++i)
        result += std::popcount(mem[i]);

    // Scan the last bits
    result += std::popcount(mem[p] << (NBits - 1 - q));

    return result;
}

template<typename Container>
inline uint32_t bit_rank(uint32_t x, const Container & mem)
{
    using Unsigned = decltype(*std::data(mem));
    return bit_rank<Unsigned>(x, std::span<const Unsigned>(std::data(mem), std::size(mem)));
}

template<typename Unsigned>
inline uint32_t bit_rank(uint32_t x, const std::initializer_list<Unsigned> & mem)
{
    return bit_rank<Unsigned>(x, std::span<const Unsigned>(std::data(mem), std::size(mem)));
}

} // namespace MLDB
