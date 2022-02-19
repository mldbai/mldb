/* factor_packing.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <iostream>
#include <cassert>
#include <limits>
#include "mldb/utils/bits.h"
#include "mldb/utils/pow.h"

namespace MLDB {

enum FactorPackSolutionType: uint32_t {
    BIT_PACKED = 0,
    FACTOR_PACKED = 1
};

struct FactorPackSolution {
    FactorPackSolutionType type;
    uint32_t factor;         // Factor
    float numFactors;     // How many factors fit in a 64 bit word?
};

constexpr uint32_t get_previous_factor(uint64_t factor)
{
    constexpr uint32_t factors[] = {
         0, 1, 2, 3, 4, 5, 6, 7,
         8, 9, 10, 11, 13,
         16, 19, 23, 30,
         32, 40, 56, 64, 84,
         128, 138, 256, 512, 565,
         1024, 1625, 2048, 4096,
         7131, 8192, 16384, 32768, 65536,
         1<<17, 1<<18, 1<<19, 1<<20, 1<<21, 2642245, 1<<22, 
         1<<23, 1<<24, 1<<25, 1<<26, 1<<27, 1<<28, 1<<29, 1<<30, 1U<<31 };

    constexpr size_t NUM_FACTORS = sizeof(factors) / sizeof(factors[0]);

    if (factor == 0)
        return 0;

    auto prevFactor = std::lower_bound(factors, factors + NUM_FACTORS, factor);
    if (prevFactor == factors + NUM_FACTORS) {
        return 1 << bits(factor);
    }

    if (*prevFactor == factor)
        --prevFactor;

    return *prevFactor; 
}

constexpr FactorPackSolution get_factor_packing_solution(uint64_t maxValue)
{
    switch (bits(maxValue)) {
        case 0:  // zero
            return { BIT_PACKED, 1, std::numeric_limits<float>::infinity() };
        case 1:  // one
            break;
        case 2:  // 2-3
            if (maxValue == 2)
                return { FACTOR_PACKED, 3, 40 };
            break;
        case 3: // 4-7
            if (maxValue < 5)
                return { FACTOR_PACKED, 5, 27 };
            else if (maxValue < 6)
                return { FACTOR_PACKED, 6, 24 };
            else if (maxValue < 7)
                return { FACTOR_PACKED, 7, 22 };
            break;
        case 4: // 8-15
            if (maxValue < 9)
                return { FACTOR_PACKED, 9, 20 };
            else if (maxValue < 10)
                return { FACTOR_PACKED, 10, 19 };
            else if (maxValue < 11)
                return { FACTOR_PACKED, 11, 18 };
            else if (maxValue < 13)
                return { FACTOR_PACKED, 13, 17 };
            break;
        case 5: // 16-31
            if (maxValue < 19)
                    return { FACTOR_PACKED, 19, 15 };
            else if (maxValue < 23)
                return { FACTOR_PACKED, 23, 14 };
            else if (maxValue < 30)
                return { FACTOR_PACKED, 30, 13 };
            break;
        case 6: // 32-63
            if (maxValue < 40)
                return { FACTOR_PACKED, 40, 12 };
            else if (maxValue < 56)
                return { FACTOR_PACKED, 56, 11 };
            break;
        case 7: // 64-127
            if (maxValue < 84)
                return { FACTOR_PACKED, 84, 10 };
            break;
        case 8: // 128-255
            if (maxValue < 138)
                return { FACTOR_PACKED, 138, 9 };
            break;
        case 9: // 256-511
            break;
        case 10: // 512-1023
            if (maxValue < 565)
                return { FACTOR_PACKED, 565, 7};
            break;
        case 11: // 1024-2047
            if (maxValue < 1625)
                return { FACTOR_PACKED, 1625, 6};
            break;
        case 12: // 2048-4095
            break;
        case 13:
            if (maxValue < 7131)
                return { FACTOR_PACKED, 7131, 5 };
            break;
        // ... handled by default...
        case 22:
            if (maxValue < 2642245)
                return { FACTOR_PACKED, 2642245, 3 };
            break;
        default:
            break;
    }

    return { BIT_PACKED, uint32_t(1ULL << bits(maxValue)), 64.0f / bits(maxValue) };
}

constexpr inline uint32_t extract_factor32(uint32_t factor, uint32_t index, uint32_t word)
{
    uint32_t valfactor = factor ? pow32(factor, index) : 1;
    uint32_t val = (word / valfactor);
    if (factor) val %= factor;
    return val;
}

constexpr inline uint64_t extract_factor64(uint32_t factor, uint32_t index, uint64_t word)
{
    uint64_t valfactor = pow64(factor ? factor : 1ULL << 32, index);
    uint64_t val = (word / valfactor);
    if (factor) val %= factor;
    else val &= 0xffffffff;
    return val;
}

constexpr inline uint32_t insert_factor32(uint32_t factor, uint32_t index, uint32_t word, uint32_t value)
{
    uint32_t curr = extract_factor32(factor, index, word);
    uint32_t valfactor = factor ? pow32(factor, index) : 1;
    int32_t delta = (value - curr) * valfactor;
    return word + delta;
}

constexpr inline uint64_t insert_factor64(uint32_t factor, uint32_t index, uint64_t word, uint64_t value)
{
    using namespace std;
    uint64_t curr = extract_factor64(factor, index, word);
    uint64_t valfactor = pow64(factor ? factor : 1ULL << 32, index);
    int64_t delta = (value - curr) * valfactor;

    //cerr << "inserting value " << value << " at index " << index << " of factor " << factor << " in word " << word << " of " << bits(word) << " bits" << endl;
    //cerr << "  valfactor = " << valfactor << " curr = " << curr << " change " << int64_t(value - curr) << " delta " << int64_t(value - curr) * int64_t(valfactor) << endl;

    //ExcAssert(extract_factor(factor, index, word + delta) == value);

    return word + delta;
}

} // namespace MLDB
