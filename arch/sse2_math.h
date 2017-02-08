/* sse2_math.h                                                     -*- C++ -*-
   Jeremy Barnes, 23 January 2010
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Basic math functions for SSE2.
*/

#pragma once

#include "sse2.h"
#include "sse2_misc.h"

namespace MLDB {
namespace SIMD {

inline v4sf pow2f_unsafe(v4si n)
{
    n += vec_splat(127);
    v4si res = __builtin_ia32_pslldi128(n, 23);
    return (v4sf) res;
}

inline v2df pow2_unsafe(v4si n)
{
    v2di result = { 0, 0 };
    n += vec_splat(1023);
    result = (v2di)_mm_unpacklo_ps((v4sf)result, (v4sf)n);
    result = __builtin_ia32_psllqi128(result, 20);
    return (v2df)result;
}

inline v2df sse2_pow2(v4si n)
{
    return pow2_unsafe(n);
}

inline v2df ldexp(v2df x, v4si n)
{
    // ldexp: return x * 2^n (only the first 2 entries of n are used)
    // works by constructing 2^n and adding n

    return x * pow2_unsafe(n);
}


// Splits a number into a coefficient and an integral power of 2 such that
// x = y * 2^exp.  Doesn't handle denormals, zeros, NaN or infinities.
v4sf sse2_frexpf_unsafe_nz_nodn( v4sf x, v4si & exp)
{
    //cerr << "x = " << x << endl;

    v4si exps = (v4si)x;
    // Shift out the mantissa
    exps = __builtin_ia32_psrldi128(exps, 23);
    
    //cerr << "  exps = " << exps << endl;

    // Clear out the sign
    exp = (v4si)_mm_and_ps((v4sf)exps, (v4sf)vec_splat(0x000000ff));

    //cerr << "  exp = " << exp << endl;

    // Subtract the offset
    exp -= vec_splat(0x7e);

    //cerr << "  exp = " << exp << endl;

    // Mantissa: keep it and the sign, then add in a bias to make it one
    x = _mm_and_ps(x, (v4sf)vec_splat((int)0x807fffff));
    x = _mm_or_ps(x, (v4sf)vec_splat(0x3f000000));

    //cerr << "  result = " << x << endl;
    
    return x;
}

v4sf sse2_frexpf_unsafe_nodn( v4sf x, v4si & exp)
{
    v4sf iszero = (v4sf)_mm_cmpeq_ps(x, vec_splat(0.0f));
    v4sf res    = sse2_frexpf_unsafe_nz_nodn(x, exp);
    res         = _mm_andnot_ps((v4sf)iszero, res);
    exp         = (v4si)_mm_andnot_ps((v4sf)iszero, (v4sf)exp);
    return res;
}

v4sf sse2_frexpf_nodn( v4sf x, v4si & exp)
{
    return pass_nan_inf(x, sse2_frexpf_unsafe_nodn(x, exp));
}

v4sf sse2_frexpf( v4sf x, v4si & exp)
{
    return sse2_frexpf_nodn(x, exp);
}


inline v2df sse2_floor_unsafe(v2df x)
{
    v4si tr       = __builtin_ia32_cvttpd2dq(x);
    v2di neg      = (v2di)_mm_cmplt_pd(x, vec_splat(0.0));
    v2df res      = __builtin_ia32_cvtdq2pd(tr);
    v2df fix1     = _mm_and_pd((v2df)neg, vec_splat(1.0));
    v2di exact    = (v2di)_mm_cmpeq_pd(res, x);
    v2df fix      = _mm_andnot_pd((v2df)exact, (v2df)fix1);
    res -= fix; 
    return res;
}

inline v2df sse2_floor(v2df x)
{
    return pass_nan_inf_zero(x, sse2_floor_unsafe(x));
}


inline v4sf sse2_trunc_unsafe(v4sf x)
{
    return __builtin_ia32_cvtdq2ps(__builtin_ia32_cvttps2dq(x));
}


inline v4sf sse2_floor_unsafe(v4sf x)
{
    v4si tr       = __builtin_ia32_cvttps2dq(x);
    v4si neg      = (v4si)_mm_cmplt_ps(x, vec_splat(0.0f));
    v4sf res      = __builtin_ia32_cvtdq2ps(tr);
    v4si exact    = (v4si)_mm_cmpeq_ps(res, x);
    v4sf fixmask  = _mm_andnot_ps((v4sf)exact, (v4sf)neg);
    v4sf fix      = _mm_and_ps(fixmask, vec_splat(1.0f));
    res -= fix; 
    return res;
}

inline v4sf sse2_floor(v4sf x)
{
    return pass_nan_inf_zero(x, sse2_floor_unsafe(x));
}

inline v4sf sse2_trunc(v4sf x)
{
    return pass_nan_inf_zero(x, sse2_trunc_unsafe(x));
}

} // namespace SIMD
} // namespace MLDB
