/* floating_point.h                                                -*- C++ -*-
   Jeremy Barnes, 27 January 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   ---

   Utilities to deal with floating point numbers.
*/

#pragma once

#include "mldb/compiler/compiler.h"
#include <limits>
#include <stdint.h>
#include <cmath>
#include <algorithm>
#include <limits>
#include <type_traits>


namespace MLDB {

namespace {

struct int_float {
    MLDB_ALWAYS_INLINE int_float(uint32_t x) { i = x; }
    MLDB_ALWAYS_INLINE int_float(float x) { f = x; }
    union {
        float f;
        uint32_t i;
    };
};

struct int_double {
    MLDB_ALWAYS_INLINE int_double(uint64_t x) { i = x; }
    MLDB_ALWAYS_INLINE int_double(double x) { f = x; }
    union {
        double f;
        uint64_t i;
    };
};

} // file scope

/** Functions to get the bit patterns of floating point numbers in and out of
    integers for twiddling.
*/
MLDB_ALWAYS_INLINE float reinterpret_as_float(uint32_t val)
{
    return int_float(val).f;
}

MLDB_ALWAYS_INLINE double reinterpret_as_double(uint64_t val)
{
    return int_double(val).f;
}

MLDB_ALWAYS_INLINE uint32_t reinterpret_as_int(float val)
{
    return int_float(val).i;
}

MLDB_ALWAYS_INLINE uint64_t reinterpret_as_int(double val)
{
    return int_double(val).i;
}

/** Like std::less<Float>, but has a well defined order for nan values, which
    allows us to sort ranges that might contain nan values without crashing.
*/
template<typename Float>
struct safe_less {
    bool operator () (Float v1, Float v2) const
    {
        bool nan1 = std::isnan(v1), nan2 = std::isnan(v2);
        if (nan1 && nan2) return false;
        return (nan1 > nan2)
            || ((nan1 == nan2) && v1 < v2);
    }
};

struct float_hasher {
    MLDB_ALWAYS_INLINE int operator () (float val) const
    {
        return reinterpret_as_int(val);
    }
};

template<class C>
struct fp_traits
    : public std::numeric_limits<C> {
};

template<>
struct fp_traits<float>
    : public std::numeric_limits<float> {
    /** Maximum argument to exp() that doesn't result in an infinity.  Found
        using exp_test program in boosting/testing. */
    static const float max_exp_arg;
};

template<>
struct fp_traits<double>
    : public std::numeric_limits<double> {
    static const double max_exp_arg;
};

template<>
struct fp_traits<long double>
    : public std::numeric_limits<long double> {
    static const long double max_exp_arg;
};

template<typename Float, typename Int>
struct FloatIntegerClamper {
    static constexpr bool specialized = false;
    static constexpr Float min = static_cast<Float>(std::numeric_limits<Int>::min());
    static constexpr Float max = static_cast<Float>(std::numeric_limits<Int>::max());
    static constexpr bool in_range(Float val)
    {
        return val >= min && val <= max;
    }
    static constexpr Int clamp(Float val)
    {
        if (val < min) return std::numeric_limits<Int>::min();
        if (val > max) return std::numeric_limits<Int>::max();
        return val;
        //return static_cast<int64_t>(std::clamp(val, min, max));
    }
};

template<>
struct FloatIntegerClamper<double, int64_t> {
    static constexpr bool specialized = true;
    static constexpr double min = -0x1p+63;
    static constexpr double max = 0x1.fffffffffffffp+62;
    static constexpr bool in_range(double val)
    {
        return val >= min && val <= max;
    }
    static constexpr int64_t clamp(double val)
    {
        if (val < min) return std::numeric_limits<int64_t>::min();
        if (val > max) return std::numeric_limits<int64_t>::max();
        return val;
        //return static_cast<int64_t>(std::clamp(val, min, max));
    }
};

template<>
struct FloatIntegerClamper<double, uint64_t> {
    static constexpr bool specialized = true;
    static constexpr double min = 0;
    static constexpr double max = 0x1.fffffffffffffp+63;
    static constexpr bool in_range(double val)
    {
        return val >= min && val <= max;
    }
    static constexpr uint64_t clamp(double val)
    {
        if (val < min) return std::numeric_limits<uint64_t>::min();
        if (val > max) return std::numeric_limits<uint64_t>::max();
        return val;
        //return static_cast<uint64_t>(std::clamp(val, min, max));
    }
};

template<>
struct FloatIntegerClamper<float, int64_t> {
    static constexpr bool specialized = true;
    static constexpr float min = -0x1p+63;
    static constexpr float max = 0x1.fffffep+62;
    static constexpr bool in_range(float val)
    {
        return val >= min && val <= max;
    }
    static constexpr int64_t clamp(float val)
    {
        if (val < min) return std::numeric_limits<int64_t>::min();
        if (val > max) return std::numeric_limits<int64_t>::max();
        return val;
        //return static_cast<int64_t>(std::clamp(val, min, max));
    }
};

template<>
struct FloatIntegerClamper<float, uint64_t> { // here
    static constexpr bool specialized = true;
    static constexpr float min = 0;
    static constexpr float max = 0x1.fffffep+63;
    static constexpr bool in_range(float val)
    {
        return val >= min && val <= max;
    }
    static constexpr uint64_t clamp(float val)
    {
        if (val < min) return std::numeric_limits<uint64_t>::min();
        if (val > max) return std::numeric_limits<uint64_t>::max();
        return val;
        //return static_cast<uint64_t>(std::clamp(val, min, max));
    }
};

template<>
struct FloatIntegerClamper<float, int32_t> {
    static constexpr bool specialized = true;
    static constexpr float min = -0x1p+31;
    static constexpr float max = 0x1.fffffep+30;
    static constexpr bool in_range(float val)
    {
        return val >= min && val <= max;
    }
    static constexpr int32_t clamp(float val)
    {
        if (val < min) return std::numeric_limits<int32_t>::min();
        if (val > max) return std::numeric_limits<int32_t>::max();
        return val;
        //return static_cast<int32_t>(std::clamp(val, min, max));
    }
};

template<>
struct FloatIntegerClamper<float, uint32_t> {
    static constexpr bool specialized = true;
    static constexpr float min = 0;
    static constexpr float max = 0x1.fffffep+31;
    static constexpr bool in_range(float val)
    {
        return val >= min && val <= max;
    }
    static constexpr uint32_t clamp(float val)
    {
        if (val < min) return std::numeric_limits<uint32_t>::min();
        if (val > max) return std::numeric_limits<uint32_t>::max();
        return val;
        //return static_cast<uint32_t>(std::clamp(val, min, max));
    }
};

// Conversion that avoids undefined behavior if out of range
template<typename Int, typename Float>
constexpr Int safe_clamp(Float val)
{
    return FloatIntegerClamper<Float, Int>::clamp(val);
}

template<typename Float>
struct SafeClamper {
    Float f;

    template<typename Int, typename Enable = std::enable_if_t<std::is_integral_v<Int>>>
    constexpr operator Int () const { return safe_clamp<Int, Float>(f); }
};

template<typename Float>
constexpr SafeClamper<Float> safely_clamped(Float f)
{
    return SafeClamper<Float>{f};
}

} // namespace MLDB
