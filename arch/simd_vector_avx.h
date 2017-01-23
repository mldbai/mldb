/** simd_vector_avx.h                                              -*- C++ -*-
    Jeremy Barnes, 11 October 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    SIMD vector operations; AVX specializations.
*/

#pragma once

#include <cstddef>

namespace MLDB {
namespace SIMD {
namespace Avx {

/// Double precision vector dot product, avx version
double vec_dotprod(const double * x, const double * y, size_t n);

/// Single precision vector dot product, avx version
float vec_dotprod(const float * x, const float * y, size_t n);

/// Single precision vector dot product with internal summation in dp,
/// avx version
double vec_dotprod_dp(const float * x, const float * y, size_t n);

/// Single precision vector minus
void vec_minus(const float * x, const float * y, float * r, size_t n);

/// Single precision vector euclidean distance squared
double vec_euclid(const float * x, const float * y, size_t n);

} // namespace Avx
} // namespace SIMD
} // namespace MLDB
