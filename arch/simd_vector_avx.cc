/** simd_vector_avx.h                               

    Jeremy Barnes, 11 October 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    SIMD vector operations; AVX specializations.
*/

#include "simd_vector_avx.h"
#include "simd_vector.h"
#include <immintrin.h>
#include <iostream>

using namespace std;

namespace MLDB {
namespace SIMD {

namespace Generic {

// Internal to simd_vector.cc.  Used here as a fallback for the
// avx2 version until we've implemented it.
float vec_dotprod_generic(const float * x, const float * y, size_t n);

} // namespace Generic

namespace Avx {

// Avx vector of double type
typedef double v4df __attribute__((__vector_size__(32)));

// Avx vector of float type
typedef float v8sf __attribute__((__vector_size__(32)));

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

inline double horiz_sum_8_avx(v8sf rr0, v8sf rr1)
{
    double result = 0.0;

    float results[16];
    *(v8sf *)(results + 0) = rr0;
    *(v8sf *)(results + 8) = rr1;
    
    result += results[0] + results[1];
    result += results[2] + results[3];
    result += results[4] + results[5];
    result += results[6] + results[7];
    result += results[8] + results[9];
    result += results[10] + results[11];
    result += results[12] + results[13];
    result += results[14] + results[15];

    return result;
}

double vec_dotprod(const double * x, const double * y, size_t n)
{
    unsigned i = 0;
    double result = 0.0;

    if (true) {
        v4df rr0 = _mm256_setzero_pd(), rr1 = rr0;

        for (; i + 16 <= n;  i += 16) {
            v4df yy0 = _mm256_loadu_pd(y + i);
            v4df xx0 = _mm256_loadu_pd(x + i);
            yy0 *= xx0;
            rr0 += yy0;

            v4df yy1 = _mm256_loadu_pd(y + i + 4);
            v4df xx1 = _mm256_loadu_pd(x + i + 4);
            yy1 *= xx1;
            rr1 += yy1;
            
            v4df yy2 = _mm256_loadu_pd(y + i + 8);
            v4df xx2 = _mm256_loadu_pd(x + i + 8);
            yy2 *= xx2;
            rr0 += yy2;

            v4df yy3 = _mm256_loadu_pd(y + i + 12);
            v4df xx3 = _mm256_loadu_pd(x + i + 12);
            yy3 *= xx3;
            rr1 += yy3;
        }

        for (; i + 8 <= n;  i += 8) {
            v4df yy0 = _mm256_loadu_pd(y + i);
            v4df xx0 = _mm256_loadu_pd(x + i);
            rr0 += yy0 * xx0;

            v4df yy1 = _mm256_loadu_pd(y + i + 4);
            v4df xx1 = _mm256_loadu_pd(x + i + 4);
            rr1 += yy1 * xx1;
        }

        for (; i + 4 <= n;  i += 4) {
            v4df yy0 = _mm256_loadu_pd(y + i);
            v4df xx0 = _mm256_loadu_pd(x + i);
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

/// Single precision vector minus
void vec_minus(const float * x, const float * y, float * r, size_t n)
{
    size_t i = 0;

    if (true) {
        for (; i + 32 <= n;  i += 32) {
            v8sf yy0 = _mm256_loadu_ps(y + i);
            v8sf xx0 = _mm256_loadu_ps(x + i);
            _mm256_storeu_ps(r + i, xx0 - yy0);

            v8sf yy1 = _mm256_loadu_ps(y + i + 8);
            v8sf xx1 = _mm256_loadu_ps(x + i + 8);
            _mm256_storeu_ps(r + i + 8, xx1 - yy1);
            
            v8sf yy2 = _mm256_loadu_ps(y + i + 16);
            v8sf xx2 = _mm256_loadu_ps(x + i + 16);
            _mm256_storeu_ps(r + i + 16, xx2 - yy2);

            v8sf yy3 = _mm256_loadu_ps(y + i + 24);
            v8sf xx3 = _mm256_loadu_ps(x + i + 24);
            _mm256_storeu_ps(r + i + 24, xx3 - yy3);
        }

        for (; i + 16 <= n;  i += 16) {
            v8sf yy0 = _mm256_loadu_ps(y + i);
            v8sf xx0 = _mm256_loadu_ps(x + i);
            _mm256_storeu_ps(r + i, xx0 - yy0);

            v8sf yy1 = _mm256_loadu_ps(y + i + 8);
            v8sf xx1 = _mm256_loadu_ps(x + i + 8);
            _mm256_storeu_ps(r + i + 8, xx1 - yy1);
        }

        for (; i + 8 <= n;  i += 8) {
            v8sf yy0 = _mm256_loadu_ps(y + i);
            v8sf xx0 = _mm256_loadu_ps(x + i);
            _mm256_storeu_ps(r + i, xx0 - yy0);
        }
    }

    for (; i < n;  ++i) r[i] = x[i] - y[i];
}

#if 0
struct v8df {
    v8df()
        : rr0(_mm256_setzero_ps()),
          rr1(_mm256_setzero_ps())
    {
    }

    v4df rr0, rr1;

    v8df & operator += (v8sf v)
    {
            v2df dd0a = __builtin_ia32_cvtps2pd(yyyy0);
            yyyy0 = _mm_shuffle_ps(yyyy0, yyyy0, 14);
            v2df dd0b = __builtin_ia32_cvtps2pd(yyyy0);
        
    }
};
#endif

/// Single precision vector euclidian distance squared
double vec_euclid(const float * x, const float * y, size_t n)
{
    double result = 0.0;

    size_t i = 0;

#if 1
    if (true) {
        v8sf rr0 = _mm256_setzero_ps(), rr1 = rr0;
        for (; i + 32 <= n;  i += 32) {
            v8sf yy0 = _mm256_loadu_ps(y + i);
            v8sf xx0 = _mm256_loadu_ps(x + i);
            xx0 -= yy0;
            rr0 += xx0 * xx0;

            v8sf yy1 = _mm256_loadu_ps(y + i + 8);
            v8sf xx1 = _mm256_loadu_ps(x + i + 8);
            xx1 -= yy1;
            rr1 += xx1 * xx1;
            
            v8sf yy2 = _mm256_loadu_ps(y + i + 16);
            v8sf xx2 = _mm256_loadu_ps(x + i + 16);
            xx2 -= yy2;
            rr0 += xx2 * xx2;

            v8sf yy3 = _mm256_loadu_ps(y + i + 24);
            v8sf xx3 = _mm256_loadu_ps(x + i + 24);
            xx3 -= yy3;
            rr1 += xx3 * xx3;
        }

        for (; i + 16 <= n;  i += 16) {
            v8sf yy0 = _mm256_loadu_ps(y + i);
            v8sf xx0 = _mm256_loadu_ps(x + i);
            xx0 -= yy0;
            rr0 += xx0 * xx0;

            v8sf yy1 = _mm256_loadu_ps(y + i + 8);
            v8sf xx1 = _mm256_loadu_ps(x + i + 8);
            xx1 -= yy1;
            rr1 += xx1 * xx1;
        }

        for (; i + 8 <= n;  i += 8) {
            v8sf yy0 = _mm256_loadu_ps(y + i);
            v8sf xx0 = _mm256_loadu_ps(x + i);
            xx0 -= yy0;
            rr0 += xx0 * xx0;
        }

        // This performs exactly the same operation as as the sse2 version,
        // and so gives exactly the same result (bit for bit).  That's
        // the reason for the convoluted code here.
        result += horiz_sum_8_avx(rr0, rr1);
    }
#endif

    for (; i < n;  ++i) result += (x[i] - y[i]) * (x[i] - y[i]);

    return result;
}

double vec_dotprod_dp(const float * x, const float * y, size_t n)
{
    double res = 0.0;
    size_t i = 0;

    if (true) {
        v4df rr0 = _mm256_setzero_pd(), rr1 = rr0, rr2 = rr0, rr3 = rr0;

        auto accum = [&] (size_t i)
            {
                v8sf yyyy0 = _mm256_loadu_ps(y + i);
                v8sf xxxx0 = _mm256_loadu_ps(x + i);
                yyyy0 *= xxxx0;
                v4df dd0a = _mm256_cvtps_pd(_mm256_extractf128_ps(yyyy0, 0));
                v4df dd0b = _mm256_cvtps_pd(_mm256_extractf128_ps(yyyy0, 1));
                return dd0a + dd0b;
            };

        for (; i + 32 <= n;  i += 32) {
            rr0 += accum(i);
            rr1 += accum(i + 8);
            rr2 += accum(i + 16);
            rr3 += accum(i + 24);
        }

        rr0 += rr1 + rr2 + rr3;

        for (; i + 8 <= n;  i += 8) {
            rr0 += accum(i);
        }

        double results[4];
        *(v4df *)results = rr0;

        res = results[0] + results[1] + results[2] + results[3];
    }
        

    for (;  i < n;  ++i) res += x[i] * y[i];

    return res;
}

} // namespace Avx
} // namespace SIMD
} // namespace MLDB
