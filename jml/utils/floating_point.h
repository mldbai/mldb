// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* floating_point.h                                                -*- C++ -*-
   Jeremy Barnes, 27 January 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   


   ---

   Utilities to deal with floating point numbers.
*/

#ifndef __utils__floating_point_h__
#define __utils__floating_point_h__


#include "mldb/compiler/compiler.h"
#include <limits>
#include <stdint.h>
#include <cmath>

namespace ML {

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

} // namespace ML

#endif /* __utils__floating_point_h__ */
