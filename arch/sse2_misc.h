/* sse2_misc.h                                                     -*- C++ -*-
   Jeremy Barnes, 23 January 2010
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   SSE2 miscellaneous functions.
*/

#pragma once

#include "sse2.h"
#include <cmath>

namespace MLDB {
namespace SIMD {

inline v2df pass_nan(v2df input, v2df result)
{
    v2df mask_nan = (v2df)_mm_cmpunord_pd(input, input);
    auto nans_only = _mm_and_pd(mask_nan, input);
    result = _mm_andnot_pd(mask_nan, result);
    result = _mm_or_pd(result, nans_only);
    return result;
}

inline v2df pass_nan_inf_zero(v2df input, v2df result)
{
    v2df mask = (v2df)_mm_cmpunord_pd(input, input);
    mask = _mm_or_pd(mask, (v2df)_mm_cmpeq_pd(input, vec_splat(double(-INFINITY))));
    mask = _mm_or_pd(mask, (v2df)_mm_cmpeq_pd(input, vec_splat(double(INFINITY))));
    mask = _mm_or_pd(mask, (v2df)_mm_cmpeq_pd(input, vec_splat(0.0)));

    input = _mm_and_pd(mask, input);
    result = _mm_andnot_pd(mask, result);
    result = _mm_or_pd(result, input);
    return result;
}

inline int out_of_range_mask_cc(v2df input, v2df min_val, v2df max_val)
{
    v2df mask_too_low  = (v2df)_mm_cmplt_pd(input, min_val);
    v2df mask_too_high = (v2df)_mm_cmpgt_pd(input, max_val);

    return __builtin_ia32_movmskpd(_mm_or_pd(mask_too_low,
                                             mask_too_high));
}
 
inline int out_of_range_mask_cc(v2df input, double min_val, double max_val)
{
    return out_of_range_mask_cc(input, vec_splat(min_val), vec_splat(max_val));
}

inline int out_of_range_mask_oo(v2df input, v2df min_val, v2df max_val)
{
    auto mask_too_low  = _mm_cmple_pd(input, min_val);
    auto mask_too_high = _mm_cmpge_pd(input, max_val);

    return __builtin_ia32_movmskpd(_mm_or_pd(mask_too_low,
                                             mask_too_high));
}

inline int out_of_range_mask_oo(v2df input, double min_val, double max_val)
{
    return out_of_range_mask_oo(input, vec_splat(min_val), vec_splat(max_val));
}


inline int out_of_range_mask_cc(v4sf input, v4sf min_val, v4sf max_val)
{
    v4sf mask_too_low  = (v4sf)_mm_cmplt_ps(input, min_val);
    v4sf mask_too_high = (v4sf)_mm_cmpgt_ps(input, max_val);

    return __builtin_ia32_movmskps(_mm_or_ps(mask_too_low,
                                             mask_too_high));
}

inline int out_of_range_mask_cc(v4sf input, float min_val, float max_val)
{
    return out_of_range_mask_cc(input, vec_splat(min_val), vec_splat(max_val));
}

inline int out_of_range_mask_oo(v4sf input, v4sf min_val, v4sf max_val)
{
    v4sf mask_too_low  = (v4sf)_mm_cmple_ps(input, min_val);
    v4sf mask_too_high = (v4sf)_mm_cmpge_ps(input, max_val);
    v4sf mask_or = _mm_or_ps(mask_too_low, mask_too_high);
    int result = __builtin_ia32_movmskps(mask_or);
    return result;
}

inline int out_of_range_mask_oo(v4sf input, float min_val, float max_val)
{
    return out_of_range_mask_oo(input, vec_splat(min_val), vec_splat(max_val));
}

inline v4sf pass_nan(v4sf input, v4sf result)
{
    v4sf mask_nan = (v4sf)_mm_cmpunord_ps(input, input);
    input = _mm_and_ps(mask_nan, input);
    result = _mm_andnot_ps(mask_nan, result);
    result = _mm_or_ps(result, input);

    return result;
}

inline v4sf pass_nan_inf(v4sf input, v4sf result)
{
    v4sf mask = (v4sf)_mm_cmpunord_ps(input, input);
    mask = _mm_or_ps(mask, (v4sf)_mm_cmpeq_ps(input, vec_splat(-INFINITY)));
    mask = _mm_or_ps(mask, (v4sf)_mm_cmpeq_ps(input, vec_splat(INFINITY)));

    input = _mm_and_ps(mask, input);
    result = _mm_andnot_ps(mask, result);
    result = _mm_or_ps(result, input);
    return result;
}

inline v4sf pass_nan_inf_zero(v4sf input, v4sf result)
{
    v4sf mask = (v4sf)_mm_cmpunord_ps(input, input);
    mask = _mm_or_ps(mask, (v4sf)_mm_cmpeq_ps(input, vec_splat(-INFINITY)));
    mask = _mm_or_ps(mask, (v4sf)_mm_cmpeq_ps(input, vec_splat(INFINITY)));
    mask = _mm_or_ps(mask, (v4sf)_mm_cmpeq_ps(input, vec_splat(0.0f)));

    input = _mm_and_ps(mask, input);
    result = _mm_andnot_ps(mask, result);
    result = _mm_or_ps(result, input);
    return result;
}

} // namespace SIMD
} // namespace MLDB
