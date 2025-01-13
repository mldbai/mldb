/* bit_select.h                                                 -*- C++ -*-
   Jeremy Barnes, 23 March 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <type_traits>
#include <span>
#include "mldb/compiler/compiler.h"
#include "mldb/arch/exception.h"


namespace MLDB {

/// Find the first position such that there are x 1 bits in the array up to and including
/// that position.
/// Inverse of bit_rank()
template<typename Unsigned>
inline uint32_t bit_select(uint32_t x, std::span<const uint64_t> mem)
{
    static_assert(std::is_unsigned_v<Unsigned>, "brank only works on unsigned integral types");
    static constexpr size_t NBits = sizeof(Unsigned) * 8;

    // Naive... to be optimized
    // http://bitmagic.io/rank-select.html

    // Scan a word at a time
    size_t p = 0;  // word number
    if (MLDB_UNLIKELY(mem.size() == 0))
        MLDB_THROW_LOGIC_ERROR("bselect");

    while (x > 0 && x >= std::popcount(mem[p])) {
        x -= std::popcount(mem[p]);
        ++p;
    }

    // Scan a bit at a time; q is bit number
    for (uint32_t q = 0;  q < NBits && x > 0;  ++q) {
        if (std::popcount(mem[p] << (NBits - 1 - q)) == x)
            return p * 64 + q;
    }

    // Fall through: x == 0
    return 0;
}

template<typename Container>
inline uint32_t bit_select(uint32_t x, const Container & mem)
{
    using Unsigned = decltype(*std::data(mem));
    return bselect<Unsigned>(x, std::span<Unsigned>(std::data(mem), std::size(mem)));
}

} // namespace MLDB
