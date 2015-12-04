// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** simd_vector_avx.h                                              -*- C++ -*-
    Jeremy Barnes, 11 October 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    SIMD vector operations; AVX specializations.
*/

#include "simd_vector_avx.h"
#include "simd_vector.h"

namespace ML {
namespace SIMD {

namespace Generic {

// Internal to simd_vector.cc.  Used here as a fallback for the
// avx2 version until we've implemented it.
float vec_dotprod_generic(const float * x, const float * y, size_t n);

} // namespace Generic

namespace Avx {

// Avx vector of double type
typedef double v4df __attribute__((__vector_size__(32)));

inline double horiz_sum_8_avx(v4df rr0, v4df rr1)
{
    double result = 0.0;

    double results[8];
    *(v4df *)(results + 0) = rr0;
    *(v4df *)(results + 4) = rr1;
    
    result += results[0] + results[1];
    result += results[2] + results[3];
    result += results[4] + results[5];
    result += results[6] + results[7];

    return result;
}

double vec_dotprod(const double * x, const double * y, size_t n)
{
    unsigned i = 0;
    double result = 0.0;

    if (true) {
        v4df rr0 = { 0.0, 0.0, 0.0, 0.0 }, rr1 = rr0;

        for (; i + 16 <= n;  i += 16) {
            v4df yy0 = __builtin_ia32_loadupd256(y + i + 0);
            v4df xx0 = __builtin_ia32_loadupd256(x + i + 0);
            yy0 *= xx0;
            rr0 += yy0;
            //rr0 += yy0 * xx0;

            v4df yy1 = __builtin_ia32_loadupd256(y + i + 4);
            v4df xx1 = __builtin_ia32_loadupd256(x + i + 4);
            yy1 *= xx1;
            rr1 += yy1;
            //rr1 += yy1 * xx1;
            
            v4df yy2 = __builtin_ia32_loadupd256(y + i + 8);
            v4df xx2 = __builtin_ia32_loadupd256(x + i + 8);
            yy2 *= xx2;
            rr0 += yy2;
            //rr0 += yy2 * xx2;

            v4df yy3 = __builtin_ia32_loadupd256(y + i + 12);
            v4df xx3 = __builtin_ia32_loadupd256(x + i + 12);
            yy3 *= xx3;
            rr1 += yy3;
            //rr1 += yy3 * xx3;
        }

        for (; i + 8 <= n;  i += 8) {
            v4df yy0 = __builtin_ia32_loadupd256(y + i + 0);
            v4df xx0 = __builtin_ia32_loadupd256(x + i + 0);
            rr0 += yy0 * xx0;

            v4df yy1 = __builtin_ia32_loadupd256(y + i + 4);
            v4df xx1 = __builtin_ia32_loadupd256(x + i + 4);
            rr1 += yy1 * xx1;
        }

        for (; i + 4 <= n;  i += 4) {
            v4df yy0 = __builtin_ia32_loadupd256(y + i + 0);
            v4df xx0 = __builtin_ia32_loadupd256(x + i + 0);
            rr0 += yy0 * xx0;
        }

        // This performs exactly the same operation as as the sse2 version,
        // and so gives exactly the same result (bit for bit).  That's
        // the reason for the convoluted code here.
        result += horiz_sum_8_avx(rr0, rr1);
    }

    for (; i < n;  ++i) result += x[i] * y[i];

    return result;
}

// TODO: vectorize this eventually...
float vec_dotprod(const float * x, const float * y, size_t n)
{
    return Generic::vec_dotprod_generic(x, y, n);
}

} // namespace Avx
} // namespace SIMD
} // namespace ML
