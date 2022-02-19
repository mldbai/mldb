/* bits.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include <bit>
#include <cstdint>

namespace MLDB {

static constexpr inline uint64_t hi(uint64_t x) { return x >> 32; }
static constexpr inline uint64_t lo(uint64_t x) { return uint32_t(x); }
static constexpr inline uint32_t bits(uint64_t x) { return 64 - std::countl_zero(x); }

} // namespace MLDB
