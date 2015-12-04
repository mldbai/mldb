// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** simd_vector_avx.h                                              -*- C++ -*-
    Jeremy Barnes, 11 October 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    SIMD vector operations; AVX specializations.
*/

#pragma once

#include <cstddef>

namespace ML {
namespace SIMD {
namespace Avx {

/// Double precision vector dot product, avx version
double vec_dotprod(const double * x, const double * y, size_t n);

/// Single precision vector dot product, avx version
float vec_dotprod(const float * x, const float * y, size_t n);

} // namespace Avx
} // namespace SIMD
} // namespace ML
