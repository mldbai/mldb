/* simd_vector.cc
   Jeremy Barnes, 15 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Contains generic implementations (non-vectorised) of SIMD functionality.
*/

#include "mldb/arch/arch.h"
#include "mldb/compiler/compiler.h"
#include "exception.h"
#include <iostream>
#include <cmath>
#if MLDB_INTEL_ISA
# include "simd_vector.h"
# include "simd_vector_avx.h"
# include "sse2.h"
# include <immintrin.h>
#endif

using namespace std;


namespace MLDB {
namespace SIMD {
namespace Generic {

template<typename X>
int ptr_align(const X * p) 
{
    return size_t(p) & 15;
} MLDB_PURE_FN


void vec_scale(const float * x, float k, float * r, size_t n)
{
    size_t i = 0;

        if (false)
        ;

#if MLDB_INTEL_ISA
    else {
        v4sf kkkk = vec_splat(k);
        for (; i + 16 <= n;  i += 16) {
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            xxxx0 *= kkkk;
            v4sf xxxx1 = _mm_loadu_ps(x + i + 4);
            __builtin_ia32_storeups(r + i + 0, xxxx0);
            xxxx1 *= kkkk;
            v4sf xxxx2 = _mm_loadu_ps(x + i + 8);
            __builtin_ia32_storeups(r + i + 4, xxxx1);
            xxxx2 *= kkkk;
            v4sf xxxx3 = _mm_loadu_ps(x + i + 12);
            __builtin_ia32_storeups(r + i + 8, xxxx2);
            xxxx3 *= kkkk;
            __builtin_ia32_storeups(r + i + 12, xxxx3);
        }
    
        for (; i + 4 <= n;  i += 4) {
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            xxxx0 *= kkkk;
            __builtin_ia32_storeups(r + i + 0, xxxx0);
        }
    }
#endif
    
    for (; i < n;  ++i) r[i] = k * x[i];
}

void vec_add(const float * x, const float * y, float * r, size_t n)
{
    size_t i = 0;

        if (false)
        ;

#if MLDB_INTEL_ISA
    else {
        //cerr << "unoptimized" << endl;

        for (; i + 16 <= n;  i += 16) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            v4sf yyyy1 = _mm_loadu_ps(y + i + 4);
            yyyy0 += xxxx0;
            v4sf xxxx1 = _mm_loadu_ps(x + i + 4);
            __builtin_ia32_storeups(r + i + 0, yyyy0);
            v4sf yyyy2 = _mm_loadu_ps(y + i + 8);
            yyyy1 += xxxx1;
            v4sf xxxx2 = _mm_loadu_ps(x + i + 8);
            __builtin_ia32_storeups(r + i + 4, yyyy1);
            v4sf yyyy3 = _mm_loadu_ps(y + i + 12);
            yyyy2 += xxxx2;
            v4sf xxxx3 = _mm_loadu_ps(x + i + 12);
            __builtin_ia32_storeups(r + i + 8, yyyy2);
            yyyy3 += xxxx3;
            __builtin_ia32_storeups(r + i + 12, yyyy3);
        }

        for (; i + 4 <= n;  i += 4) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            yyyy0 += xxxx0;
            __builtin_ia32_storeups(r + i + 0, yyyy0);
        }
    }
#endif
    for (; i < n;  ++i) r[i] = x[i] + y[i];
}

void vec_prod(const float * x, const float * y, float * r, size_t n)
{
    size_t i = 0;

        if (false)
        ;

#if MLDB_INTEL_ISA
    else {
        //cerr << "unoptimized" << endl;
        
        for (; i + 16 <= n;  i += 16) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            v4sf yyyy1 = _mm_loadu_ps(y + i + 4);
            yyyy0 *= xxxx0;
            v4sf xxxx1 = _mm_loadu_ps(x + i + 4);
            __builtin_ia32_storeups(r + i + 0, yyyy0);
            v4sf yyyy2 = _mm_loadu_ps(y + i + 8);
            yyyy1 *= xxxx1;
            v4sf xxxx2 = _mm_loadu_ps(x + i + 8);
            __builtin_ia32_storeups(r + i + 4, yyyy1);
            v4sf yyyy3 = _mm_loadu_ps(y + i + 12);
            yyyy2 *= xxxx2;
            v4sf xxxx3 = _mm_loadu_ps(x + i + 12);
            __builtin_ia32_storeups(r + i + 8, yyyy2);
            yyyy3 *= xxxx3;
            __builtin_ia32_storeups(r + i + 12, yyyy3);
        }

        for (; i + 4 <= n;  i += 4) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            yyyy0 *= xxxx0;
            __builtin_ia32_storeups(r + i + 0, yyyy0);
        }
    }
#endif
    for (; i < n;  ++i) r[i] = x[i] * y[i];
}

void vec_prod(const float * x, const double * y, float * r, size_t n)
{
    size_t i = 0;

#if 0 // TODO: do
        if (false)
        ;

    else {
        //cerr << "unoptimized" << endl;
        
        for (; i + 16 <= n;  i += 16) {
            v2df yy0a  = _mm_loadu_pd(y + i + 0);
            v2df yy0b  = _mm_loadu_pd(y + i + 2);
            v4sf yyyy0 = __builtin_ia32_cvtpd2ps(yy0a);



            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            v4sf yyyy1 = _mm_loadu_ps(y + i + 4);
            yyyy0 *= xxxx0;
            v4sf xxxx1 = _mm_loadu_ps(x + i + 4);
            __builtin_ia32_storeups(r + i + 0, yyyy0);
            v4sf yyyy2 = _mm_loadu_ps(y + i + 8);
            yyyy1 *= xxxx1;
            v4sf xxxx2 = _mm_loadu_ps(x + i + 8);
            __builtin_ia32_storeups(r + i + 4, yyyy1);
            v4sf yyyy3 = _mm_loadu_ps(y + i + 12);
            yyyy2 *= xxxx2;
            v4sf xxxx3 = _mm_loadu_ps(x + i + 12);
            __builtin_ia32_storeups(r + i + 8, yyyy2);
            yyyy3 *= xxxx3;
            __builtin_ia32_storeups(r + i + 12, yyyy3);
        }

        for (; i + 4 <= n;  i += 4) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            yyyy0 *= xxxx0;
            __builtin_ia32_storeups(r + i + 0, yyyy0);
        }
        
    }
#endif

    for (; i < n;  ++i) r[i] = x[i] * y[i];
}

void vec_prod(const double * x, const double * y, float * r, size_t n)
{
    size_t i = 0;
    for (; i < n;  ++i) r[i] = x[i] * y[i];
}

void vec_add(const float * x, float k, const float * y, float * r, size_t n)
{
    size_t i = 0;

    //bool alignment_unimportant = true;  // nehalem?

#if MLDB_INTEL_ISA
    if (false && n >= 16 && (ptr_align(x) == ptr_align(y) && ptr_align(y) == ptr_align(r))) {
        v4sf kkkk = vec_splat(k);

        /* Align everything on 16 byte boundaries */
        if (ptr_align(x) != 0) {
            int needed_to_align = (16 - ptr_align(x)) / 4;
            
            for (size_t i = 0;  i < needed_to_align;  ++i)
                r[i] = x[i] + k * y[i];

            r += needed_to_align;  x += needed_to_align;  y += needed_to_align;
            n -= needed_to_align;
        }

        //cerr << "optimized" << endl;

        while (n > 16) {
            const v4sf * xx = reinterpret_cast<const v4sf *>(x);
            const v4sf * yy = reinterpret_cast<const v4sf *>(y);
            v4sf * rr = reinterpret_cast<v4sf *>(r);
            
            v4sf yyyy0 = yy[0];
            v4sf xxxx0 = xx[0];
            yyyy0 *= kkkk;
            v4sf yyyy1 = yy[1];
            yyyy0 += xxxx0;
            v4sf xxxx1 = xx[1];
            rr[0] = yyyy0;
            yyyy1 *= kkkk;
            v4sf yyyy2 = yy[2];
            yyyy1 += xxxx1;
            v4sf xxxx2 = xx[2];
            rr[1] = yyyy1;
            yyyy2 *= kkkk;
            v4sf yyyy3 = yy[3];
            yyyy2 += xxxx2;
            v4sf xxxx3 = xx[3];
            rr[2] = yyyy2;
            yyyy3 *= kkkk;
            yyyy3 += xxxx3;
            rr[3] = yyyy3;

            r += 16;  x += 16;  y += 16;  n -= 16;
        }

        for (size_t i = 0;  i < n;  ++i) r[i] = x[i] + k * y[i];
    }
    else {
        v4sf kkkk = vec_splat(k);
        //cerr << "unoptimized" << endl;

        for (; i + 16 <= n;  i += 16) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            yyyy0 *= kkkk;
            v4sf yyyy1 = _mm_loadu_ps(y + i + 4);
            yyyy0 += xxxx0;
            v4sf xxxx1 = _mm_loadu_ps(x + i + 4);
            __builtin_ia32_storeups(r + i + 0, yyyy0);
            yyyy1 *= kkkk;
            v4sf yyyy2 = _mm_loadu_ps(y + i + 8);
            yyyy1 += xxxx1;
            v4sf xxxx2 = _mm_loadu_ps(x + i + 8);
            __builtin_ia32_storeups(r + i + 4, yyyy1);
            yyyy2 *= kkkk;
            v4sf yyyy3 = _mm_loadu_ps(y + i + 12);
            yyyy2 += xxxx2;
            v4sf xxxx3 = _mm_loadu_ps(x + i + 12);
            __builtin_ia32_storeups(r + i + 8, yyyy2);
            yyyy3 *= kkkk;
            yyyy3 += xxxx3;
            __builtin_ia32_storeups(r + i + 12, yyyy3);
        }

        for (; i + 4 <= n;  i += 4) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            yyyy0 *= kkkk;
            yyyy0 += xxxx0;
            __builtin_ia32_storeups(r + i + 0, yyyy0);
        }
    }
#endif

    for (; i < n;  ++i) r[i] = x[i] + k * y[i];
}

void vec_add(const float * x, const float * k, const float * y, float * r,
             size_t n)
{
    size_t i = 0;

#if MLDB_INTEL_ISA
    if (true) {
        for (; i + 16 <= n;  i += 16) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf kkkk0 = _mm_loadu_ps(k + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            yyyy0 *= kkkk0;
            v4sf yyyy1 = _mm_loadu_ps(y + i + 4);
            v4sf kkkk1 = _mm_loadu_ps(k + i + 4);
            yyyy0 += xxxx0;
            v4sf xxxx1 = _mm_loadu_ps(x + i + 4);
            __builtin_ia32_storeups(r + i + 0, yyyy0);
            yyyy1 *= kkkk1;
            v4sf yyyy2 = _mm_loadu_ps(y + i + 8);
            v4sf kkkk2 = _mm_loadu_ps(k + i + 8);
            yyyy1 += xxxx1;
            v4sf xxxx2 = _mm_loadu_ps(x + i + 8);
            __builtin_ia32_storeups(r + i + 4, yyyy1);
            yyyy2 *= kkkk2;
            v4sf yyyy3 = _mm_loadu_ps(y + i + 12);
            v4sf kkkk3 = _mm_loadu_ps(k + i + 12);
            yyyy2 += xxxx2;
            v4sf xxxx3 = _mm_loadu_ps(x + i + 12);
            __builtin_ia32_storeups(r + i + 8, yyyy2);
            yyyy3 *= kkkk3;
            yyyy3 += xxxx3;
            __builtin_ia32_storeups(r + i + 12, yyyy3);
        }

        for (; i + 4 <= n;  i += 4) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            v4sf kkkk0 = _mm_loadu_ps(k + i + 0);
            yyyy0 *= kkkk0;
            yyyy0 += xxxx0;
            __builtin_ia32_storeups(r + i + 0, yyyy0);
        }
    }
#endif

    for (; i < n;  ++i) r[i] = x[i] + k[i] * y[i];
}

void vec_add(const float * x, float k, const double * y, float * r, size_t n)
{
    for (size_t i = 0; i < n;  ++i) r[i] = x[i] + k * y[i];
}


float vec_dotprod_generic(const float * x, const float * y, size_t n)
{
    double res = 0.0;
    for (size_t i = 0;  i < n;  ++i) res += x[i] * y[i];
    return res;
}

float vec_dotprod_sse2(const float * x, const float * y, size_t n)
{
    return vec_dotprod_generic(x, y, n);
}

float vec_dotprod(const float * x, const float * y, size_t n)
{
    return vec_dotprod_generic(x, y, n);
}

void vec_scale(const double * x, double k, double * r, size_t n)
{
    size_t i = 0;

        if (false)
        ;

#if MLDB_INTEL_ISA
    else {
        v2df kk = vec_splat(k);
        for (; i + 8 <= n;  i += 8) {
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            xx0 *= kk;
            v2df xx1 = _mm_loadu_pd(x + i + 2);
            __builtin_ia32_storeupd(r + i + 0, xx0);
            xx1 *= kk;
            v2df xx2 = _mm_loadu_pd(x + i + 4);
            __builtin_ia32_storeupd(r + i + 2, xx1);
            xx2 *= kk;
            v2df xx3 = _mm_loadu_pd(x + i + 6);
            __builtin_ia32_storeupd(r + i + 4, xx2);
            xx3 *= kk;
            __builtin_ia32_storeupd(r + i + 6, xx3);
        }
    
        for (; i + 2 <= n;  i += 2) {
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            xx0 *= kk;
            __builtin_ia32_storeupd(r + i + 0, xx0);
        }
    }
#endif
    
    for (; i < n;  ++i) r[i] = k * x[i];
}

void vec_add(const double * x, double k, const double * y, double * r,
             size_t n)
{
    size_t i = 0;

#if MLDB_INTEL_ISA
    if (true) {
        v2df kk = vec_splat(k);
        for (; i + 8 <= n;  i += 8) {
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            yy0 *= kk;
            yy0 += xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);

            v2df yy1 = _mm_loadu_pd(y + i + 2);
            v2df xx1 = _mm_loadu_pd(x + i + 2);
            yy1 *= kk;
            yy1 += xx1;
            __builtin_ia32_storeupd(r + i + 2, yy1);
            
            v2df yy2 = _mm_loadu_pd(y + i + 4);
            v2df xx2 = _mm_loadu_pd(x + i + 4);
            yy2 *= kk;
            yy2 += xx2;
            __builtin_ia32_storeupd(r + i + 4, yy2);

            v2df yy3 = _mm_loadu_pd(y + i + 6);
            v2df xx3 = _mm_loadu_pd(x + i + 6);
            yy3 *= kk;
            yy3 += xx3;
            __builtin_ia32_storeupd(r + i + 6, yy3);

        }

        for (; i + 2 <= n;  i += 2) {
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            yy0 *= kk;
            yy0 += xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);
        }
    }
#endif

    for (;  i < n;  ++i) r[i] = x[i] + k * y[i];
}

void vec_add(const double * x, const double * k, const double * y,
             double * r, size_t n)
{
    size_t i = 0;
#if MLDB_INTEL_ISA
    if (true) {
        for (; i + 8 <= n;  i += 8) {
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            v2df kk0 = _mm_loadu_pd(k + i + 0);
            yy0 *= kk0;
            yy0 += xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);

            v2df yy1 = _mm_loadu_pd(y + i + 2);
            v2df xx1 = _mm_loadu_pd(x + i + 2);
            v2df kk1 = _mm_loadu_pd(k + i + 2);
            yy1 *= kk1;
            yy1 += xx1;
            __builtin_ia32_storeupd(r + i + 2, yy1);
            
            v2df yy2 = _mm_loadu_pd(y + i + 4);
            v2df xx2 = _mm_loadu_pd(x + i + 4);
            v2df kk2 = _mm_loadu_pd(k + i + 4);
            yy2 *= kk2;
            yy2 += xx2;
            __builtin_ia32_storeupd(r + i + 4, yy2);

            v2df yy3 = _mm_loadu_pd(y + i + 6);
            v2df xx3 = _mm_loadu_pd(x + i + 6);
            v2df kk3 = _mm_loadu_pd(k + i + 6);
            yy3 *= kk3;
            yy3 += xx3;
            __builtin_ia32_storeupd(r + i + 6, yy3);
        }

        for (; i + 2 <= n;  i += 2) {
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            v2df kk0 = _mm_loadu_pd(k + i + 0);
            yy0 *= kk0;
            yy0 += xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);
        }
    }
#endif

    for (;  i < n;  ++i) r[i] = x[i] + k[i] * y[i];
}

#if MLDB_INTEL_ISA
double horiz_sum_8_sse2(v2df rr0, v2df rr1, v2df rr2, v2df rr3)
{
    double result = 0.0;

    double results[8];
    *(v2df *)(results + 0) = rr0;
    *(v2df *)(results + 2) = rr1;
    *(v2df *)(results + 4) = rr2;
    *(v2df *)(results + 6) = rr3;

    result += results[0] + results[1];
    result += results[2] + results[3];
    result += results[4] + results[5];
    result += results[6] + results[7];

    return result;
}

double vec_dotprod_sse2(const double * x, const double * y, size_t n)
{
    size_t i = 0;
    double result = 0.0;

    if (true) {
        v2df rr0 = { 0.0, 0.0 }, rr1 = rr0, rr2 = rr0, rr3 = rr0;

        for (; i + 8 <= n;  i += 8) {
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            rr0 += yy0 * xx0;

            v2df yy1 = _mm_loadu_pd(y + i + 2);
            v2df xx1 = _mm_loadu_pd(x + i + 2);
            rr1 += yy1 * xx1;
            
            v2df yy2 = _mm_loadu_pd(y + i + 4);
            v2df xx2 = _mm_loadu_pd(x + i + 4);
            rr2 += yy2 * xx2;

            v2df yy3 = _mm_loadu_pd(y + i + 6);
            v2df xx3 = _mm_loadu_pd(x + i + 6);
            rr3 += yy3 * xx3;
        }

        for (; i + 4 <= n;  i += 4) {
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            rr0 += yy0 * xx0;

            v2df yy1 = _mm_loadu_pd(y + i + 2);
            v2df xx1 = _mm_loadu_pd(x + i + 2);
            rr1 += yy1 * xx1;
        }

        // This performs exactly the same operation as as the avx version,
        // and so gives exactly the same result (bit for bit).  That's
        // the reason for the convoluted code here.
        result += horiz_sum_8_sse2(rr0, rr1, rr2, rr3);
    }

    for (; i < n;  ++i) result += x[i] * y[i];

    return result;
}

void vec_minus_sse2(const float * x, const float * y, float * r, size_t n)
{
    size_t i = 0;

        if (false)
        ;

    else {
        //cerr << "unoptimized" << endl;
        
        for (; i + 16 <= n;  i += 16) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            v4sf yyyy1 = _mm_loadu_ps(y + i + 4);
            xxxx0 -= yyyy0;
            v4sf xxxx1 = _mm_loadu_ps(x + i + 4);
            __builtin_ia32_storeups(r + i + 0, xxxx0);
            v4sf yyyy2 = _mm_loadu_ps(y + i + 8);
            xxxx1 -= yyyy1;
            v4sf xxxx2 = _mm_loadu_ps(x + i + 8);
            __builtin_ia32_storeups(r + i + 4, xxxx1);
            v4sf yyyy3 = _mm_loadu_ps(y + i + 12);
            xxxx2 -= yyyy2;
            v4sf xxxx3 = _mm_loadu_ps(x + i + 12);
            __builtin_ia32_storeups(r + i + 8, xxxx2);
            xxxx3 -= yyyy3;
            __builtin_ia32_storeups(r + i + 12, xxxx3);
        }

        for (; i + 4 <= n;  i += 4) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            xxxx0 -= yyyy0;
            __builtin_ia32_storeups(r + i + 0, xxxx0);
        }
    }
    for (; i < n;  ++i) r[i] = x[i] - y[i];
}
#endif

#if 0  // GCC 4.8 segfaults on initialization of loaded shlibs when multiversioning used.  It may actually be a linker problem... hard to tell
__attribute__ ((target ("default")))
double vec_dotprod_(const double * x, const double * y, size_t n)
{
    return vec_dotprod_sse2(x, y, n);
}

__attribute__ ((target ("avx")))
double vec_dotprod_(const double * x, const double * y, size_t n)
{
    return Avx::vec_dotprod(x, y, n);
}

double vec_dotprod(const double * x, const double * y, size_t n)
{
    // Dispatch it.  See here: https://gcc.gnu.org/wiki/FunctionMultiVersioning
    return vec_dotprod_(x, y, n);
}
#else
double vec_dotprod(const double * x, const double * y, size_t n)
{
    // Interrogate the cpuid flags directly to decide which one to use
        if (false)
        ;

#if MLDB_INTEL_ISA
    else if (has_avx()) {
        return Avx::vec_dotprod(x, y, n);
    }
    else if (true) {
        return vec_dotprod_sse2(x, y, n);
    }
#endif
    else {
        double result = 0.0;
        for (size_t i = 0; i < n;  ++i) result += x[i] * y[i];
        return result;
    }
}

void vec_minus(const float * x, const float * y, float * r, size_t n)
{
    // Interrogate the cpuid flags directly to decide which one to use
    if (false)
        ;
#if MLDB_INTEL_ISA
    if (has_avx()) {
        Avx::vec_minus(x, y, r, n);
    }
    else if (true) /* sse2 */ {
        vec_minus_sse2(x, y, r, n);
    }
#endif
    else {
        for (size_t i = 0;  i < n;  ++i) r[i] = x[i] - y[i];
    }
}

double vec_euclid(const float * x, const float * y, size_t n)
{
    // Interrogate the cpuid flags directly to decide which one to use
        if (false)
        ;

#if MLDB_INTEL_ISA
    if (has_avx()) {
        return Avx::vec_euclid(x, y, n);
    }
    else if (true) /* sse2 */ {
        float tmp[n];
        vec_minus(x, y, tmp, n);
        return vec_dotprod(tmp, tmp, n);
    }
#endif
    else {
        double result = 0.0;
        for (size_t i = 0;  i < n;  ++i)
            result += (x[i] - y[i]) * (x[i] - y[i]);
        return result;
    }
}

#endif // GCC 4.8 multiversion problems

double vec_accum_prod3(const float * x, const float * y, const float * z,
                       size_t n)
{
    double res = 0.0;
    size_t i = 0;

#if MLDB_INTEL_ISA
    if (true) {
        v2df rr = vec_splat(0.0);

        for (; i + 16 <= n;  i += 16) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            yyyy0 *= xxxx0;
            v4sf zzzz0 = _mm_loadu_ps(z + i + 0);
            yyyy0 *= zzzz0;
            v2df dd0a = __builtin_ia32_cvtps2pd(yyyy0);
            yyyy0 = _mm_shuffle_ps(yyyy0, yyyy0, 14);
            v2df dd0b = __builtin_ia32_cvtps2pd(yyyy0);
            rr += dd0a;
            rr += dd0b;

            v4sf yyyy1 = _mm_loadu_ps(y + i + 4);
            v4sf xxxx1 = _mm_loadu_ps(x + i + 4);
            yyyy1 *= xxxx1;
            v4sf zzzz1 = _mm_loadu_ps(z + i + 4);
            yyyy1 *= zzzz1;
            v2df dd1a = __builtin_ia32_cvtps2pd(yyyy1);
            yyyy1 = _mm_shuffle_ps(yyyy1, yyyy1, 14);
            v2df dd1b = __builtin_ia32_cvtps2pd(yyyy1);
            rr += dd1a;
            rr += dd1b;
            
            v4sf yyyy2 = _mm_loadu_ps(y + i + 8);
            v4sf xxxx2 = _mm_loadu_ps(x + i + 8);
            yyyy2 *= xxxx2;
            v4sf zzzz2 = _mm_loadu_ps(z + i + 8);
            yyyy2 *= zzzz2;
            v2df dd2a = __builtin_ia32_cvtps2pd(yyyy2);
            yyyy2 = _mm_shuffle_ps(yyyy2, yyyy2, 14);
            v2df dd2b = __builtin_ia32_cvtps2pd(yyyy2);
            rr += dd2a;
            rr += dd2b;

            v4sf yyyy3 = _mm_loadu_ps(y + i + 12);
            v4sf xxxx3 = _mm_loadu_ps(x + i + 12);
            yyyy3 *= xxxx3;
            v4sf zzzz3 = _mm_loadu_ps(z + i + 12);
            yyyy3 *= zzzz3;
            v2df dd3a = __builtin_ia32_cvtps2pd(yyyy3);
            yyyy3 = _mm_shuffle_ps(yyyy3, yyyy3, 14);
            v2df dd3b = __builtin_ia32_cvtps2pd(yyyy3);
            rr += dd3a;
            rr += dd3b;
        }

        for (; i + 4 <= n;  i += 4) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            yyyy0 *= xxxx0;
            v4sf zzzz0 = _mm_loadu_ps(z + i + 0);
            yyyy0 *= zzzz0;

            v2df dd1 = __builtin_ia32_cvtps2pd(yyyy0);
            yyyy0 = _mm_shuffle_ps(yyyy0, yyyy0, 14);
            v2df dd2 = __builtin_ia32_cvtps2pd(yyyy0);
            rr += dd1;
            rr += dd2;
        }

        double results[2];
        *(v2df *)results = rr;

        res = results[0] + results[1];
    }
#endif
        
    for (;  i < n;  ++i) res += x[i] * y[i] * z[i];
    return res;
}

double vec_accum_prod3(const float * x, const float * y, const double * z,
                       size_t n)
{
    double res = 0.0;
    size_t i = 0;

#if MLDB_INTEL_ISA
    if (true) {
        v2df rr = vec_splat(0.0);

        for (; i + 4 <= n;  i += 4) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            yyyy0 *= xxxx0;

            v2df zz0a  = _mm_loadu_pd(z + i + 0);
            v2df zz0b  = _mm_loadu_pd(z + i + 2);

            v2df dd0a = __builtin_ia32_cvtps2pd(yyyy0);
            yyyy0 = _mm_shuffle_ps(yyyy0, yyyy0, 14);
            v2df dd0b = __builtin_ia32_cvtps2pd(yyyy0);

            dd0a     *= zz0a;
            dd0b     *= zz0b;

            rr += dd0a;
            rr += dd0b;
        }

        double results[2];
        *(v2df *)results = rr;

        res = results[0] + results[1];
    }
#endif        

    for (;  i < n;  ++i) res += x[i] * y[i] * z[i];
    return res;
}

void vec_minus(const double * x, const double * y, double * r, size_t n)
{
    for (size_t i = 0;  i < n;  ++i) r[i] = x[i] - y[i];
}

double vec_accum_prod3(const double * x, const double * y, const double * z,
                      size_t n)
{
    size_t i = 0;
    double result = 0.0;

#if MLDB_INTEL_ISA
    if (true) {
        v2df rr = vec_splat(0.0);

        for (; i + 8 <= n;  i += 8) {
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            v2df zz0 = _mm_loadu_pd(z + i + 0);
            yy0 *= xx0;
            yy0 *= zz0;
            rr += yy0;

            v2df yy1 = _mm_loadu_pd(y + i + 2);
            v2df xx1 = _mm_loadu_pd(x + i + 2);
            v2df zz1 = _mm_loadu_pd(z + i + 2);
            yy1 *= xx1;
            yy1 *= zz1;
            rr += yy1;
            
            v2df yy2 = _mm_loadu_pd(y + i + 4);
            v2df xx2 = _mm_loadu_pd(x + i + 4);
            v2df zz2 = _mm_loadu_pd(z + i + 4);
            yy2 *= xx2;
            yy2 *= zz2;
            rr += yy2;

            v2df yy3 = _mm_loadu_pd(y + i + 6);
            v2df xx3 = _mm_loadu_pd(x + i + 6);
            v2df zz3 = _mm_loadu_pd(z + i + 6);
            yy3 *= xx3;
            yy3 *= zz3;
            rr += yy3;
        }

        for (; i + 2 <= n;  i += 2) {
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            v2df zz0 = _mm_loadu_pd(z + i + 0);
            yy0 *= xx0;
            yy0 *= zz0;
            rr += yy0;
        }

        double results[2];
        *(v2df *)results = rr;

        result = results[0] + results[1];
    }
#endif

    for (; i < n;  ++i) result += x[i] * y[i] * z[i];

    return result;
}

double vec_accum_prod3(const double * x, const double * y, const float * z,
                      size_t n)
{
    size_t i = 0;
    double result = 0.0;

#if MLDB_INTEL_ISA
    if (true) {
        v2df rr = vec_splat(0.0);

        for (; i + 4 <= n;  i += 4) {
            v4sf zzzz01 = _mm_loadu_ps(z + i + 0);
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df zz0 = __builtin_ia32_cvtps2pd(zzzz01);
            v2df xx0 = _mm_loadu_pd(x + i + 0);

            yy0 *= xx0;
            yy0 *= zz0;
            rr += yy0;

            zzzz01 = _mm_shuffle_ps(zzzz01, zzzz01, 14);
            v2df zz1 = __builtin_ia32_cvtps2pd(zzzz01);
            v2df yy1 = _mm_loadu_pd(y + i + 2);
            v2df xx1 = _mm_loadu_pd(x + i + 2);

            yy1 *= xx1;
            yy1 *= zz1;
            rr += yy1;
        }

        double results[2];
        *(v2df *)results = rr;

        result = results[0] + results[1];
    }
#endif

    for (; i < n;  ++i) result += x[i] * y[i] * z[i];

    return result;
}

double vec_sum(const double * x, size_t n)
{
    double res = 0.0;
    for (size_t i = 0;  i < n;  ++i)
        res += x[i];
    return res;
}

#if MLDB_INTEL_ISA
double vec_dotprod_dp_sse2(const float * x, const float * y, size_t n)
{
    double res = 0.0;
    size_t i = 0;

    if (true) {
        v2df rr = vec_splat(0.0);

        for (; i + 16 <= n;  i += 16) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            yyyy0 *= xxxx0;
            v2df dd0a = __builtin_ia32_cvtps2pd(yyyy0);
            yyyy0 = _mm_shuffle_ps(yyyy0, yyyy0, 14);
            v2df dd0b = __builtin_ia32_cvtps2pd(yyyy0);
            rr += dd0a;
            rr += dd0b;

            v4sf yyyy1 = _mm_loadu_ps(y + i + 4);
            v4sf xxxx1 = _mm_loadu_ps(x + i + 4);
            yyyy1 *= xxxx1;
            v2df dd1a = __builtin_ia32_cvtps2pd(yyyy1);
            yyyy1 = _mm_shuffle_ps(yyyy1, yyyy1, 14);
            v2df dd1b = __builtin_ia32_cvtps2pd(yyyy1);
            rr += dd1a;
            rr += dd1b;
            
            v4sf yyyy2 = _mm_loadu_ps(y + i + 8);
            v4sf xxxx2 = _mm_loadu_ps(x + i + 8);
            yyyy2 *= xxxx2;
            v2df dd2a = __builtin_ia32_cvtps2pd(yyyy2);
            yyyy2 = _mm_shuffle_ps(yyyy2, yyyy2, 14);
            v2df dd2b = __builtin_ia32_cvtps2pd(yyyy2);
            rr += dd2a;
            rr += dd2b;

            v4sf yyyy3 = _mm_loadu_ps(y + i + 12);
            v4sf xxxx3 = _mm_loadu_ps(x + i + 12);
            yyyy3 *= xxxx3;
            v2df dd3a = __builtin_ia32_cvtps2pd(yyyy3);
            yyyy3 = _mm_shuffle_ps(yyyy3, yyyy3, 14);
            v2df dd3b = __builtin_ia32_cvtps2pd(yyyy3);
            rr += dd3a;
            rr += dd3b;
        }

        for (; i + 4 <= n;  i += 4) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            yyyy0 *= xxxx0;
            
            v2df dd1 = __builtin_ia32_cvtps2pd(yyyy0);
            yyyy0 = _mm_shuffle_ps(yyyy0, yyyy0, 14);
            v2df dd2 = __builtin_ia32_cvtps2pd(yyyy0);
            rr += dd1;
            rr += dd2;
        }

        double results[2];
        *(v2df *)results = rr;

        res = results[0] + results[1];
    }
        

    for (;  i < n;  ++i) res += x[i] * y[i];
    return res;
}
#endif // MLDB_INTEL_ISA

double vec_dotprod_dp(const double * x, const float * y, size_t n)
{
    double res = 0.0;

    size_t i = 0;
        if (false)
        ;


#if MLDB_INTEL_ISA
    else if (true) {
        v2df rr0 = vec_splat(0.0), rr1 = vec_splat(0.0);
        
        for (; i + 8 <= n;  i += 8) {
            v4sf yyyy01 = _mm_loadu_ps(y + i + 0);
            v2df yy0    = __builtin_ia32_cvtps2pd(yyyy01);
            yyyy01      = _mm_shuffle_ps(yyyy01, yyyy01, 14);
            v2df yy1    = __builtin_ia32_cvtps2pd(yyyy01);

            v2df xx0    = _mm_loadu_pd(x + i + 0);
            rr0        += xx0 * yy0;

            v2df xx1    = _mm_loadu_pd(x + i + 2);
            rr1        += xx1 * yy1;

            v4sf yyyy23 = _mm_loadu_ps(y + i + 4);
            v2df yy2    = __builtin_ia32_cvtps2pd(yyyy23);
            yyyy23      = _mm_shuffle_ps(yyyy23, yyyy23, 14);
            v2df yy3    = __builtin_ia32_cvtps2pd(yyyy23);

            v2df xx2    = _mm_loadu_pd(x + i + 4);
            rr0        += xx2 * yy2;

            v2df xx3    = _mm_loadu_pd(x + i + 6);
            rr1        += xx3 * yy3;
        }

        for (; i + 4 <= n;  i += 4) {
            v4sf yyyy01 = _mm_loadu_ps(y + i + 0);
            v2df yy0    = __builtin_ia32_cvtps2pd(yyyy01);
            yyyy01      = _mm_shuffle_ps(yyyy01, yyyy01, 14);
            v2df yy1    = __builtin_ia32_cvtps2pd(yyyy01);

            v2df xx0    = _mm_loadu_pd(x + i + 0);
            rr0        += xx0 * yy0;

            v2df xx1    = _mm_loadu_pd(x + i + 2);
            rr1        += xx1 * yy1;
        }

        rr0 += rr1;

        double results[2];
        *(v2df *)results = rr0;
        
        res = results[0] + results[1];
    }
#endif

    for (;  i < n;  ++i) res += x[i] * y[i];

    return res;
}

double vec_dotprod_dp(const float * x, const float * y, size_t n)
{
    // Interrogate the cpuid flags directly to decide which one to use
        if (false)
        ;

#if MLDB_INTEL_ISA
    else if (has_avx()) {
        return Avx::vec_dotprod_dp(x, y, n);
    }
    else if (true) {
        return vec_dotprod_dp_sse2(x, y, n);
    }
#endif
    else {
        double res = 0.0;
        for (size_t i = 0;  i < n;  ++i) res += x[i] * y[i];
        return res;
    }
}

double vec_sum_dp(const float * x, size_t n)
{
    double res = 0.0;
    for (size_t i = 0;  i < n;  ++i)
        res += x[i];
    return res;
}

void vec_add(const double * x, const double * y, double * r, size_t n)
{
    size_t i = 0;
#if MLDB_INTEL_ISA
    if (true) {
        for (; i + 8 <= n;  i += 8) {
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            yy0 += xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);

            v2df yy1 = _mm_loadu_pd(y + i + 2);
            v2df xx1 = _mm_loadu_pd(x + i + 2);
            yy1 += xx1;
            __builtin_ia32_storeupd(r + i + 2, yy1);
            
            v2df yy2 = _mm_loadu_pd(y + i + 4);
            v2df xx2 = _mm_loadu_pd(x + i + 4);
            yy2 += xx2;
            __builtin_ia32_storeupd(r + i + 4, yy2);

            v2df yy3 = _mm_loadu_pd(y + i + 6);
            v2df xx3 = _mm_loadu_pd(x + i + 6);
            yy3 += xx3;
            __builtin_ia32_storeupd(r + i + 6, yy3);
        }

        for (; i + 2 <= n;  i += 2) {
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            yy0 += xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);
        }
    }
#endif

    for (;  i < n;  ++i) r[i] = x[i] + y[i];
}

void vec_add(const double * x, double k, const float * y, double * r, size_t n)
{
    size_t i = 0;

#if MLDB_INTEL_ISA
    if (true) {
        v2df kk = vec_splat(k);

        for (; i + 8 <= n;  i += 8) {
            v4sf yyyy01 = _mm_loadu_ps(y + i + 0);
            v2df yy0    = __builtin_ia32_cvtps2pd(yyyy01);
            yyyy01      = _mm_shuffle_ps(yyyy01, yyyy01, 14);
            v2df yy1    = __builtin_ia32_cvtps2pd(yyyy01);
            yy0        *= kk;
            yy1        *= kk;

            v2df xx0    = _mm_loadu_pd(x + i + 0);
            yy0        += xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);

            v2df xx1    = _mm_loadu_pd(x + i + 2);
            yy1        += xx1;
            __builtin_ia32_storeupd(r + i + 2, yy1);

            v4sf yyyy23 = _mm_loadu_ps(y + i + 4);
            v2df yy2    = __builtin_ia32_cvtps2pd(yyyy23);
            yyyy23      = _mm_shuffle_ps(yyyy23, yyyy23, 14);
            v2df yy3    = __builtin_ia32_cvtps2pd(yyyy23);
            yy2        *= kk;
            yy3        *= kk;

            v2df xx2    = _mm_loadu_pd(x + i + 4);
            yy2        += xx2;
            __builtin_ia32_storeupd(r + i + 4, yy2);

            v2df xx3    = _mm_loadu_pd(x + i + 6);
            yy3        += xx3;
            __builtin_ia32_storeupd(r + i + 6, yy3);
        }

        for (; i + 4 <= n;  i += 4) {
            v4sf yyyy01 = _mm_loadu_ps(y + i + 0);
            v2df yy0    = __builtin_ia32_cvtps2pd(yyyy01);
            yyyy01      = _mm_shuffle_ps(yyyy01, yyyy01, 14);
            v2df yy1    = __builtin_ia32_cvtps2pd(yyyy01);
            yy0        *= kk;
            yy1        *= kk;

            v2df xx0    = _mm_loadu_pd(x + i + 0);
            yy0        += xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);

            v2df xx1    = _mm_loadu_pd(x + i + 2);
            yy1        += xx1;
            __builtin_ia32_storeupd(r + i + 2, yy1);
        }
    }
#endif

    for (;  i < n;  ++i) r[i] = x[i] + k * y[i];
}

void vec_add(const double * x, const float * y, double * r, size_t n)
{
    for (size_t i = 0;  i < n;  ++i) r[i] = x[i] + y[i];
}

void vec_prod(const double * x, const double * y, double * r, size_t n)
{
    size_t i = 0;
#if MLDB_INTEL_ISA
    if (true) {
        for (; i + 8 <= n;  i += 8) {
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            yy0 *= xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);

            v2df yy1 = _mm_loadu_pd(y + i + 2);
            v2df xx1 = _mm_loadu_pd(x + i + 2);
            yy1 *= xx1;
            __builtin_ia32_storeupd(r + i + 2, yy1);
            
            v2df yy2 = _mm_loadu_pd(y + i + 4);
            v2df xx2 = _mm_loadu_pd(x + i + 4);
            yy2 *= xx2;
            __builtin_ia32_storeupd(r + i + 4, yy2);

            v2df yy3 = _mm_loadu_pd(y + i + 6);
            v2df xx3 = _mm_loadu_pd(x + i + 6);
            yy3 *= xx3;
            __builtin_ia32_storeupd(r + i + 6, yy3);
        }

        for (; i + 2 <= n;  i += 2) {
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            yy0 *= xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);
        }
    }
#endif
    for (;  i < n;  ++i) r[i] = x[i] * y[i];
}

void vec_prod(const double * x, const float * y, double * r, size_t n)
{
    size_t i = 0;

#if MLDB_INTEL_ISA
    if (true) {
        for (; i + 8 <= n;  i += 8) {
            v4sf yyyy01 = _mm_loadu_ps(y + i + 0);
            v2df yy0    = __builtin_ia32_cvtps2pd(yyyy01);
            yyyy01      = _mm_shuffle_ps(yyyy01, yyyy01, 14);
            v2df yy1    = __builtin_ia32_cvtps2pd(yyyy01);

            v2df xx0    = _mm_loadu_pd(x + i + 0);
            yy0        *= xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);

            v2df xx1    = _mm_loadu_pd(x + i + 2);
            yy1        *= xx1;
            __builtin_ia32_storeupd(r + i + 2, yy1);

            v4sf yyyy23 = _mm_loadu_ps(y + i + 4);
            v2df yy2    = __builtin_ia32_cvtps2pd(yyyy23);
            yyyy23      = _mm_shuffle_ps(yyyy23, yyyy23, 14);
            v2df yy3    = __builtin_ia32_cvtps2pd(yyyy23);

            v2df xx2    = _mm_loadu_pd(x + i + 4);
            yy2        *= xx2;
            __builtin_ia32_storeupd(r + i + 4, yy2);

            v2df xx3    = _mm_loadu_pd(x + i + 6);
            yy3        *= xx3;
            __builtin_ia32_storeupd(r + i + 6, yy3);
        }

        for (; i + 4 <= n;  i += 4) {
            v4sf yyyy01 = _mm_loadu_ps(y + i + 0);
            v2df yy0    = __builtin_ia32_cvtps2pd(yyyy01);
            yyyy01      = _mm_shuffle_ps(yyyy01, yyyy01, 14);
            v2df yy1    = __builtin_ia32_cvtps2pd(yyyy01);

            v2df xx0    = _mm_loadu_pd(x + i + 0);
            yy0        *= xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);

            v2df xx1    = _mm_loadu_pd(x + i + 2);
            yy1        *= xx1;
            __builtin_ia32_storeupd(r + i + 2, yy1);
        }
    }
#endif

    for (;  i < n;  ++i) r[i] = x[i] * y[i];
}

void vec_k1_x_plus_k2_y_z(double k1, const double * x,
                          double k2, const double * y, const double * z,
                          double * r, size_t n)
{
    size_t i = 0;

#if MLDB_INTEL_ISA
    if (true) {
        v2df kk1 = vec_splat(k1);
        v2df kk2 = vec_splat(k2);

        for (; i + 8 <= n;  i += 8) {
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            v2df zz0 = _mm_loadu_pd(z + i + 0);
            yy0 *= kk2;
            xx0 *= kk1;
            yy0 *= zz0;
            yy0 += xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);

            v2df yy1 = _mm_loadu_pd(y + i + 2);
            v2df xx1 = _mm_loadu_pd(x + i + 2);
            v2df zz1 = _mm_loadu_pd(z + i + 2);
            yy1 *= kk2;
            xx1 *= kk1;
            yy1 *= zz1;
            yy1 += xx1;
            __builtin_ia32_storeupd(r + i + 2, yy1);
            
            v2df yy2 = _mm_loadu_pd(y + i + 4);
            v2df xx2 = _mm_loadu_pd(x + i + 4);
            v2df zz2 = _mm_loadu_pd(z + i + 4);
            yy2 *= kk2;
            xx2 *= kk1;
            yy2 *= zz2;
            yy2 += xx2;
            __builtin_ia32_storeupd(r + i + 4, yy2);

            v2df yy3 = _mm_loadu_pd(y + i + 6);
            v2df xx3 = _mm_loadu_pd(x + i + 6);
            v2df zz3 = _mm_loadu_pd(z + i + 6);
            yy3 *= kk2;
            xx3 *= kk1;
            yy3 *= zz3;
            yy3 += xx3;
            __builtin_ia32_storeupd(r + i + 6, yy3);
        }

        for (; i + 2 <= n;  i += 2) {
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            v2df zz0 = _mm_loadu_pd(z + i + 0);
            yy0 *= kk2;
            xx0 *= kk1;
            yy0 *= zz0;
            yy0 += xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);
        }
    }
#endif

    for (;  i < n;  ++i) r[i] = k1 * x[i] + k2 * y[i] * z[i];
}

void vec_k1_x_plus_k2_y_z(float k1, const float * x,
                          float k2, const float * y, const float * z,
                          float * r, size_t n)
{
    size_t i = 0;

#if MLDB_INTEL_ISA
    if (true) {
        v4sf kkkk1 = vec_splat(k1);
        v4sf kkkk2 = vec_splat(k2);

        for (; i + 16 <= n;  i += 16) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            v4sf zzzz0 = _mm_loadu_ps(z + i + 0);
            yyyy0 *= kkkk2;
            xxxx0 *= kkkk1;
            yyyy0 *= zzzz0;
            yyyy0 += xxxx0;
            __builtin_ia32_storeups(r + i + 0, yyyy0);

            v4sf yyyy1 = _mm_loadu_ps(y + i + 4);
            v4sf xxxx1 = _mm_loadu_ps(x + i + 4);
            v4sf zzzz1 = _mm_loadu_ps(z + i + 4);
            yyyy1 *= kkkk2;
            xxxx1 *= kkkk1;
            yyyy1 *= zzzz1;
            yyyy1 += xxxx1;
            __builtin_ia32_storeups(r + i + 4, yyyy1);
            
            v4sf yyyy2 = _mm_loadu_ps(y + i + 8);
            v4sf xxxx2 = _mm_loadu_ps(x + i + 8);
            v4sf zzzz2 = _mm_loadu_ps(z + i + 8);
            yyyy2 *= kkkk2;
            xxxx2 *= kkkk1;
            yyyy2 *= zzzz2;
            yyyy2 += xxxx2;
            __builtin_ia32_storeups(r + i + 8, yyyy2);

            v4sf yyyy3 = _mm_loadu_ps(y + i + 12);
            v4sf xxxx3 = _mm_loadu_ps(x + i + 12);
            v4sf zzzz3 = _mm_loadu_ps(z + i + 12);
            yyyy3 *= kkkk2;
            xxxx3 *= kkkk1;
            yyyy3 *= zzzz3;
            yyyy3 += xxxx3;
            __builtin_ia32_storeups(r + i + 12, yyyy3);
        }

        for (; i + 4 <= n;  i += 4) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            v4sf zzzz0 = _mm_loadu_ps(z + i + 0);
            yyyy0 *= kkkk2;
            xxxx0 *= kkkk1;
            yyyy0 *= zzzz0;
            yyyy0 += xxxx0;
            __builtin_ia32_storeups(r + i + 0, yyyy0);
        }
    }
#endif

    for (;  i < n;  ++i) r[i] = k1 * x[i] + k2 * y[i] * z[i];
}

void vec_add_sqr(const float * x, float k, const float * y, float * r, size_t n)
{
    size_t i = 0;

#if MLDB_INTEL_ISA
    if (true) {
        v4sf kkkk = vec_splat(k);
        //cerr << "unoptimized" << endl;

        for (; i + 16 <= n;  i += 16) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            yyyy0 *= yyyy0;
            yyyy0 *= kkkk;
            v4sf yyyy1 = _mm_loadu_ps(y + i + 4);
            yyyy1 *= yyyy1;
            yyyy0 += xxxx0;
            v4sf xxxx1 = _mm_loadu_ps(x + i + 4);
            __builtin_ia32_storeups(r + i + 0, yyyy0);
            yyyy1 *= kkkk;
            v4sf yyyy2 = _mm_loadu_ps(y + i + 8);
            yyyy1 += xxxx1;
            yyyy2 *= yyyy2;
            v4sf xxxx2 = _mm_loadu_ps(x + i + 8);
            __builtin_ia32_storeups(r + i + 4, yyyy1);
            yyyy2 *= kkkk;
            v4sf yyyy3 = _mm_loadu_ps(y + i + 12);
            yyyy2 += xxxx2;
            yyyy3 *= yyyy3;
            v4sf xxxx3 = _mm_loadu_ps(x + i + 12);
            __builtin_ia32_storeups(r + i + 8, yyyy2);
            yyyy3 *= kkkk;
            yyyy3 += xxxx3;
            __builtin_ia32_storeups(r + i + 12, yyyy3);
        }

        for (; i + 4 <= n;  i += 4) {
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            yyyy0 *= yyyy0;
            yyyy0 *= kkkk;
            yyyy0 += xxxx0;
            __builtin_ia32_storeups(r + i + 0, yyyy0);
        }
    }
#endif
    for (; i < n;  ++i) r[i] = x[i] + k * (y[i] * y[i]);
}

void vec_add_sqr(const double * x, double k, const double * y, double * r,
                 size_t n)
{
    size_t i = 0;

#if MLDB_INTEL_ISA
    if (true) {
        v2df kk = vec_splat(k);
        for (; i + 8 <= n;  i += 8) {
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            yy0 *= yy0;
            yy0 *= kk;
            yy0 += xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);

            v2df yy1 = _mm_loadu_pd(y + i + 2);
            v2df xx1 = _mm_loadu_pd(x + i + 2);
            yy1 *= yy1;
            yy1 *= kk;
            yy1 += xx1;
            __builtin_ia32_storeupd(r + i + 2, yy1);
            
            v2df yy2 = _mm_loadu_pd(y + i + 4);
            v2df xx2 = _mm_loadu_pd(x + i + 4);
            yy2 *= yy2;
            yy2 *= kk;
            yy2 += xx2;
            __builtin_ia32_storeupd(r + i + 4, yy2);

            v2df yy3 = _mm_loadu_pd(y + i + 6);
            v2df xx3 = _mm_loadu_pd(x + i + 6);
            yy3 *= yy3;
            yy3 *= kk;
            yy3 += xx3;
            __builtin_ia32_storeupd(r + i + 6, yy3);

        }

        for (; i + 2 <= n;  i += 2) {
            v2df yy0 = _mm_loadu_pd(y + i + 0);
            v2df xx0 = _mm_loadu_pd(x + i + 0);
            yy0 *= yy0;
            yy0 *= kk;
            yy0 += xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);
        }
    }
#endif

    for (;  i < n;  ++i) r[i] = x[i] + k * (y[i] * y[i]);
}

void vec_add_sqr(const float * x, float k, const double * y, float * r, size_t n)
{
    for (size_t i = 0; i < n;  ++i) r[i] = x[i] + k * (y[i] * y[i]);
}

void vec_add_sqr(const double * x, double k, const float * y, double * r,
                 size_t n)
{
    size_t i = 0;

#if MLDB_INTEL_ISA
    if (true) {
        v2df kk = vec_splat(k);
        for (; i + 8 <= n;  i += 8) {
            v4sf yyyy01 = _mm_loadu_ps(y + i + 0);
            yyyy01     *= yyyy01;
            v2df yy0    = __builtin_ia32_cvtps2pd(yyyy01);
            yyyy01      = _mm_shuffle_ps(yyyy01, yyyy01, 14);
            v2df yy1    = __builtin_ia32_cvtps2pd(yyyy01);
            yy0        *= kk;
            yy1        *= kk;

            v2df xx0    = _mm_loadu_pd(x + i + 0);
            yy0        += xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);

            v2df xx1    = _mm_loadu_pd(x + i + 2);
            yy1        += xx1;
            __builtin_ia32_storeupd(r + i + 2, yy1);

            v4sf yyyy23 = _mm_loadu_ps(y + i + 4);
            yyyy23     *= yyyy23;
            v2df yy2    = __builtin_ia32_cvtps2pd(yyyy23);
            yyyy23      = _mm_shuffle_ps(yyyy23, yyyy23, 14);
            v2df yy3    = __builtin_ia32_cvtps2pd(yyyy23);
            yy2        *= kk;
            yy3        *= kk;

            v2df xx2    = _mm_loadu_pd(x + i + 4);
            yy2        += xx2;
            __builtin_ia32_storeupd(r + i + 4, yy2);

            v2df xx3    = _mm_loadu_pd(x + i + 6);
            yy3        += xx3;
            __builtin_ia32_storeupd(r + i + 6, yy3);
        }

        for (; i + 4 <= n;  i += 4) {
            v4sf yyyy01 = _mm_loadu_ps(y + i + 0);
            yyyy01     *= yyyy01;
            v2df yy0    = __builtin_ia32_cvtps2pd(yyyy01);
            yyyy01      = _mm_shuffle_ps(yyyy01, yyyy01, 14);
            v2df yy1    = __builtin_ia32_cvtps2pd(yyyy01);
            yy0        *= kk;
            yy1        *= kk;

            v2df xx0    = _mm_loadu_pd(x + i + 0);
            yy0        += xx0;
            __builtin_ia32_storeupd(r + i + 0, yy0);

            v2df xx1    = _mm_loadu_pd(x + i + 2);
            yy1        += xx1;
            __builtin_ia32_storeupd(r + i + 2, yy1);
        }
    }
#endif

    for (;  i < n;  ++i) r[i] = x[i] + k * (y[i] * y[i]);
}

void vec_add(const float * x, const double * k, const double * y, float * r,
             size_t n)
{
    size_t i = 0;

        if (false)
        ;

#if MLDB_INTEL_ISA
    else {
        for (; i + 4 <= n;  i += 4) {
            v2df yy0a  = _mm_loadu_pd(y + i + 0);
            v2df yy0b  = _mm_loadu_pd(y + i + 2);
            v2df kk0a  = _mm_loadu_pd(k + i + 0);
            v2df kk0b  = _mm_loadu_pd(k + i + 2);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);

            yy0a *= kk0a;
            yy0b *= kk0b;

            v2df xx0a, xx0b;
            vec_f2d(xxxx0, xx0a, xx0b);

            xx0a += yy0a;
            xx0b += yy0b;

            v4sf rrrr0 = vec_d2f(xx0a, xx0b);

            __builtin_ia32_storeups(r + i + 0, rrrr0);
        }
    }
#endif

    for (; i < n;  ++i) r[i] = x[i] + k[i] * y[i];
}

void vec_add(const float * x, const float * k, const double * y, float * r,
             size_t n)
{
    size_t i = 0;

        if (false)
        ;

#if MLDB_INTEL_ISA
    else {
        for (; i + 4 <= n;  i += 4) {
            v2df yy0a  = _mm_loadu_pd(y + i + 0);
            v2df yy0b  = _mm_loadu_pd(y + i + 2);
            v4sf kkkk0 = _mm_loadu_ps(k + i + 0);
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            
            v2df kk0a, kk0b;
            vec_f2d(kkkk0, kk0a, kk0b);

            yy0a *= kk0a;
            yy0b *= kk0b;

            v2df xx0a, xx0b;
            vec_f2d(xxxx0, xx0a, xx0b);

            xx0a += yy0a;
            xx0b += yy0b;

            v4sf rrrr0 = vec_d2f(xx0a, xx0b);

            __builtin_ia32_storeups(r + i + 0, rrrr0);
        }
    }
#endif

    for (; i < n;  ++i) r[i] = x[i] + k[i] * y[i];
}

void vec_add(const double * x, const float * k, const float * y, double * r,
             size_t n)
{
    size_t i = 0;

        if (false)
        ;

#if MLDB_INTEL_ISA
    else {
        for (; i + 4 <= n;  i += 4) {
            v4sf kkkk0 = _mm_loadu_ps(k + i + 0);
            v4sf yyyy0 = _mm_loadu_ps(y + i + 0);
            v2df xx0a  = _mm_loadu_pd(x + i + 0);
            v2df xx0b  = _mm_loadu_pd(x + i + 2);

            v4sf ykyk0 = yyyy0 * kkkk0;
            
            v2df yk0a, yk0b;
            vec_f2d(ykyk0, yk0a, yk0b);

            xx0a += yk0a;
            xx0b += yk0b;

            __builtin_ia32_storeupd(r + i + 0, xx0a);
            __builtin_ia32_storeupd(r + i + 2, xx0b);
        }
    }
#endif

    for (; i < n;  ++i) r[i] = x[i] + k[i] * y[i];
}

void vec_add(const double * x, const float * k, const double * y, double * r,
             size_t n)
{
    size_t i = 0;

        if (false)
        ;

#if MLDB_INTEL_ISA
    else {
        for (; i + 4 <= n;  i += 4) {
            v2df yy0a  = _mm_loadu_pd(y + i + 0);
            v2df yy0b  = _mm_loadu_pd(y + i + 2);
            v4sf kkkk0 = _mm_loadu_ps(k + i + 0);
            v2df xx0a  = _mm_loadu_pd(x + i + 0);
            v2df xx0b  = _mm_loadu_pd(x + i + 2);
            
            v2df kk0a, kk0b;
            vec_f2d(kkkk0, kk0a, kk0b);

            yy0a *= kk0a;
            yy0b *= kk0b;

            xx0a += yy0a;
            xx0b += yy0b;

            __builtin_ia32_storeupd(r + i + 0, xx0a);
            __builtin_ia32_storeupd(r + i + 2, xx0b);
        }
    }
#endif

    for (; i < n;  ++i) r[i] = x[i] + k[i] * y[i];
}

// See https://bugzilla.redhat.com/show_bug.cgi?id=521190
// for why we use double version of exp

void vec_exp(const float * x, float * r, size_t n)
{
    size_t i = 0;
    for (; i < n;  ++i) r[i] = exp((double)x[i]);
}

void vec_exp(const float * x, float k, float * r, size_t n)
{
    size_t i = 0;
    for (; i < n;  ++i) r[i] = exp((double)(k * x[i]));
}

void vec_exp(const float * x, double * r, size_t n)
{
    size_t i = 0;
    for (; i < n;  ++i) r[i] = exp((double)x[i]);
}

void vec_exp(const float * x, double k, double * r, size_t n)
{
    size_t i = 0;
    for (; i < n;  ++i) r[i] = exp((double)(k * x[i]));
}

void vec_exp(const double * x, double * r, size_t n)
{
    size_t i = 0;
    for (; i < n;  ++i) r[i] = exp((double)x[i]);
}

void vec_exp(const double * x, double k, double * r, size_t n)
{
    size_t i = 0;
    for (; i < n;  ++i) r[i] = exp((double)(k * x[i]));
}

float vec_twonorm_sqr(const float * x, size_t n)
{
    size_t i = 0;
    float result = 0.0;
    for (; i < n;  ++i) result += x[i] * x[i];
    return result;
}

double vec_twonorm_sqr_dp(const float * x, size_t n)
{
    size_t i = 0;
    double result = 0.0;
    for (; i < n;  ++i) {
        double xd = x[i];
        result += xd * xd;
    }
    return result;
}

double vec_twonorm_sqr(const double * x, size_t n)
{
    size_t i = 0;
    double result = 0.0;
    for (; i < n;  ++i) result += x[i] * x[i];
    return result;
}

double vec_kl(const float * p, const float * q, size_t n)
{
    size_t i = 0;

    double total = 0.0;

    for (; i < n;  ++i) total += p[i] * logf(p[i] / q[i]);

    return total;
}

void vec_min_max_el(const float * x, float * mins, float * maxs, size_t n)
{
    size_t i = 0;

        if (false)
        ;

#if MLDB_INTEL_ISA
    else {
        for (; i + 4 <= n;  i += 4) {
            v4sf xxxx0 = _mm_loadu_ps(x + i + 0);
            v4sf iiii0 = _mm_loadu_ps(mins + i + 0);
            v4sf aaaa0 = _mm_loadu_ps(maxs + i + 0);
            iiii0      = __builtin_ia32_minps(iiii0, xxxx0);
            aaaa0      = __builtin_ia32_maxps(aaaa0, xxxx0);
            __builtin_ia32_storeups(mins + i + 0, iiii0);
            __builtin_ia32_storeups(maxs + i + 0, aaaa0);
        }
    }
#endif

    for (; i < n;  ++i) {
        mins[i] = std::min(mins[i], x[i]);
        maxs[i] = std::max(maxs[i], x[i]);
    }
}

} // namespace Generic
} // namespace SIMD
} // namespace MLDB
