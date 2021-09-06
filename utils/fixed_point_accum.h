/* fixed_point_accum.h                                             -*- C++ -*-
   Jeremy Barnes, 1 April 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Fixed-point accurate accumulators.
*/

#pragma once

#include "mldb/compiler/compiler.h"
#include "mldb/utils/float_traits.h"
#include "mldb/arch/format.h"
#include <cstdint>
#include <limits>

namespace MLDB {

void throwFixedPointOverflow() MLDB_NORETURN;


/** A structure to accumulate values between zero and one in a single 32
    bit integer. */

struct FixedPointAccum32Unsigned {
    uint32_t rep;

    static constexpr float VAL_2_REP = 1ULL << 31;
    static constexpr float REP_2_VAL = 1.0f / (1ULL << 31);
    static constexpr float ADD_TO_ROUND = 1.0f / (1ULL << 32);
    static constexpr unsigned MAX_REP = (unsigned)-1;

    FixedPointAccum32Unsigned()
        : rep(0)
    {
    }

    FixedPointAccum32Unsigned(float value)
        : rep((value + ADD_TO_ROUND)* VAL_2_REP)
    {
    }

    operator float() const { return rep * REP_2_VAL; }

    FixedPointAccum32Unsigned &
    operator += (const FixedPointAccum32Unsigned & other)
    {
        unsigned new_rep = rep + other.rep;
        rep = (new_rep < rep ? MAX_REP : new_rep);
        return *this;
    }

    FixedPointAccum32Unsigned
    operator + (const FixedPointAccum32Unsigned & other) const
    {
        FixedPointAccum32Unsigned result = *this;
        result += other;
        return result;
    }
};

struct FixedPointAccum32 {
    int32_t rep;

    static constexpr float VAL_2_REP = 1ULL << 30;
    static constexpr float REP_2_VAL = 1.0f / (1ULL << 30);
    static constexpr float ADD_TO_ROUND = 0.5f / (1ULL << 30);

    FixedPointAccum32()
        : rep(0)
    {
    }

    FixedPointAccum32(float value)
        : rep((value + ADD_TO_ROUND)* VAL_2_REP)
    {
    }

    operator float() const { return rep * REP_2_VAL; }

    FixedPointAccum32 & operator += (const FixedPointAccum32 & other)
    {
        if (MLDB_UNLIKELY(__builtin_add_overflow(rep, other.rep, &rep)))
            throwFixedPointOverflow();
        return *this;
    }

    FixedPointAccum32 operator + (const FixedPointAccum32 & other) const
    {
        FixedPointAccum32 result = *this;
        result += other;
        return result;
    }

    FixedPointAccum32 & operator -= (const FixedPointAccum32 & other)
    {
        if (MLDB_UNLIKELY(__builtin_sub_overflow(rep, other.rep, &rep)))
            throwFixedPointOverflow();
        return *this;
    }

    FixedPointAccum32 operator - (const FixedPointAccum32 & other) const
    {
        FixedPointAccum32 result = *this;
        result -= other;
        return result;
    }
};

template<typename F>
float operator / (const FixedPointAccum32 & f1, const F & f2)
{
    return f1.operator float() / float(f2);
}

template<typename F>
float operator / (const F & f1, const FixedPointAccum32 & f2)
{
    return float(f1) / f2.operator float();
}

inline float operator / (const FixedPointAccum32 & f1,
                          const FixedPointAccum32 & f2)
{
    return f1.operator float() / f2.operator float();
}

inline std::ostream & operator << (std::ostream & stream, const FixedPointAccum32 & accum)
{
    return stream << MLDB::format("%f (%016llx)", (double)accum, (long long)accum.rep);
}

struct FixedPointAccum64 {
    int64_t hl;

    static constexpr float VAL_2_HL = 1.0f * (1ULL << 60);
    static constexpr float HL_2_VAL = 1.0f / (1ULL << 60);
    static constexpr float VAL_2_H = (1ULL << 28);
    static constexpr float H_2_VAL = 1.0f / (1ULL << 28);
    static constexpr float ADD_TO_ROUND = 0.5f / (1ULL << 60);
    
    constexpr FixedPointAccum64()
        : hl(0)
    {
    }

    constexpr FixedPointAccum64(float value)
        : hl((value + ADD_TO_ROUND)* VAL_2_HL)
    {
    }

    static constexpr FixedPointAccum64 constructFromInt(int64_t i)
    {
        FixedPointAccum64 result;
        result.hl = i;
        return result;
    }

    static constexpr FixedPointAccum64 max() { return constructFromInt(std::numeric_limits<int64_t>::max()); }
    static constexpr FixedPointAccum64 min() { return constructFromInt(std::numeric_limits<int64_t>::min()); }

    operator float() const { return (hl >> 32) * H_2_VAL; }

    FixedPointAccum64 & operator += (const FixedPointAccum64 & other)
    {
        if (MLDB_UNLIKELY(__builtin_add_overflow(hl, other.hl, &hl)))
            throwFixedPointOverflow();
        return *this;
    }

    FixedPointAccum64 & operator -= (const FixedPointAccum64 & other)
    {
        if (MLDB_UNLIKELY(__builtin_sub_overflow(hl, other.hl, &hl)))
            throwFixedPointOverflow();
        return *this;
    }

    FixedPointAccum64 operator + (const FixedPointAccum64 & other) const
    {
        FixedPointAccum64 result = *this;
        result += other;
        return result;
    }

    FixedPointAccum64 operator + (float other) const
    {
        FixedPointAccum64 result = *this;
        result += FixedPointAccum64(other);
        return result;
    }

    FixedPointAccum64 operator + (double other) const
    {
        FixedPointAccum64 result = *this;
        result += FixedPointAccum64(other);
        return result;
    }
};

template<typename F>
float operator / (const FixedPointAccum64 & f1, const F & f2)
{
    return f1.operator float() / float(f2);
}

template<typename F>
float operator / (const F & f1, const FixedPointAccum64 & f2)
{
    return float(f1) / f2.operator float();
}

inline float operator / (const FixedPointAccum64 & f1,
                          const FixedPointAccum64 & f2)
{
    return f1.operator float() / f2.operator float();
}

inline std::ostream & operator << (std::ostream & stream, const FixedPointAccum64 & accum)
{
    return stream << MLDB::format("%f (%016llx)", (double)accum, (long long)accum.hl);
}

} // namespace MLDB
