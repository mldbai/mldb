/* pow.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once
#include <cctype>

namespace  MLDB {

// Russian peasant algorithm
static constexpr inline uint64_t pow64(uint64_t x, uint64_t y)
{
    if (x == 0) return 0;
    uint64_t r = 1;
    while (y > 0) {
        r *= (y & 1 ? x : 1);
        x *= x;
        y >>= 1;
    }
    return r;
}

// Russian peasant algorithm, signed version
static constexpr inline int64_t pow64_signed(int64_t x, uint64_t y)
{
    int64_t result = pow64(abs(x), y);
    result *= x < 0 ? -1 : 1;
    return result;
}

// Russian peasant algorithm
static constexpr inline uint32_t pow32(uint32_t x, uint32_t y)
{
    if (x == 0) return 0;
    uint32_t r = 1;
    while (y > 0) {
        r *= (y & 1 ? x : 1);
        x *= x;
        y >>= 1;
    }
    return r;
}

// Russian peasant algorithm, signed version
static constexpr inline int64_t pow32_signed(int32_t x, uint32_t y)
{
    int32_t result = pow32(abs(x), y);
    result *= x < 0 ? -1 : 1;
    return result;
}

} // namespace MLDB
