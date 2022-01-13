/** randomforest_kernels.cc                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#pragma once

#include "compute_kernel_cpu.h"

#include <cstdint>
#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <bit>
#include <span>
#include <experimental/coroutine>

namespace std {
using std::experimental::coroutine_traits;
} // namespace std

#define __constant
#define __global 
#define __kernel
#define __local

using std::min;
using std::max;
using std::sqrt;
using std::popcount;

using uint2 = std::array<uint32_t, 2>;
using atomic_int = std::atomic<int32_t>;
using atomic_uint = std::atomic<uint32_t>;

inline uint32_t __createMask32(uint32_t numBits)
{
    return numBits >= 32 ? -1 : (((uint32_t)1 << numBits) - 1);
}

static inline uint32_t extract_bits(uint32_t val, uint16_t offset, uint32_t bits)
{
    return (val >> offset) & __createMask32(bits);
}

static inline int32_t reinterpretFloatAsInt(float val)
{
    return *(int32_t *)(&val);
}

static inline float reinterpretIntAsFloat(int32_t val)
{
    return *(float *)(&val);
}

template<typename T, typename T2>
T atom_add(std::atomic<T> * v, T2 && arg)
{
    return v->fetch_add(arg);
}

template<typename T, typename T2>
T atom_sub(std::atomic<T> * v, T2 && arg)
{
    return v->fetch_sub(arg);
}

template<typename T>
T atom_inc(std::atomic<T> * v)
{
    return v->fetch_add(1);
}

template<typename T>
T atom_dec(std::atomic<T> * v)
{
    return v->fetch_sub(1);
}

template<typename T>
auto clz(T && arg)
{
    return std::countl_zero(arg);
}

template<typename T, typename T2>
T atom_or(std::atomic<T> * v, T2 && arg)
{
    return v->fetch_or(arg);
}

constexpr struct LocalBarrier {
} CLK_LOCAL_MEM_FENCE;

void barrier(LocalBarrier)
{
}

constexpr struct GlobalBarrier {
} CLK_GLOBAL_MEM_FENCE;

void barrier(GlobalBarrier)
{
}

static inline uint32_t simdgroup_ballot(bool val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    if (simd_lane == 0) {
        tmp[0] = 0;
    }

    if (val) {
        atom_or(tmp + 0, (uint32_t)1 << simd_lane);
    }

    return tmp[0];
}

static inline uint32_t simdgroup_sum(uint32_t val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    if (simd_lane == 0) {
        tmp[0] = 0;
    }

    if (val) {
        atom_add(tmp + 0, val);
    }

    return tmp[0];
}

static inline uint32_t prefix_exclusive_sum_bitmask(uint32_t bits, uint16_t n)
{
    return popcount(bits & ((1U << n)-1));
    //return n == 0 ? 0 : popcount(bits << (32 - n));
}

static inline uint32_t simdgroup_prefix_exclusive_sum_bools(bool val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    uint32_t ballots = simdgroup_ballot(val, simd_lane, tmp);
    return prefix_exclusive_sum_bitmask(ballots, simd_lane);
}

static inline uint32_t simdgroup_broadcast_first(uint32_t val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    if (simd_lane == 0) {
        tmp[0] = val;
    }

    return tmp[0];
}

#define FIFTY_FIRST_ARGUMENT(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, \
                             a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, \
                             a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, \
                             a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, \
                             a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, \
                             a50, ...) a50

#define COUNT_ARGS(...) FIFTY_FIRST_ARGUMENT(dummy, ## __VA_ARGS__, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

#define CALL(Fn, ...) \
    Fn(__VA_ARGS__)

#define CALL2(Name1, Name2, ...) \
    CALL(Name1 ## Name2, __VA_ARGS__)

#define FOREACH_PAIR_0(Fn) \

#define FOREACH_PAIR_2(Fn, T0, N0) \
    Fn(T0, N0)
#define FOREACH_PAIR_4(Fn, T0, N0, T1, N1) \
    Fn(T0, N0), Fn(T1, N1)
#define FOREACH_PAIR_6(Fn, T0, N0, T1, N1, T2, N2) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2)
#define FOREACH_PAIR_8(Fn, T0, N0, T1, N1, T2, N2, T3, N3) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3)
#define FOREACH_PAIR_10(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4)
#define FOREACH_PAIR_12(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5)
#define FOREACH_PAIR_14(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, T6, N6) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), Fn(T6, N6)
#define FOREACH_PAIR_16(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, T6, N6, T7, N7) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), Fn(T6, N6), Fn(T7, N7)
#define FOREACH_PAIR_18(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, T6, N6, T7, N7, T8, N8) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), Fn(T6, N6), Fn(T7, N7), Fn(T8, N8)
#define FOREACH_PAIR_20(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, T6, N6, T7, N7, T8, N8, T9, N9) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), Fn(T6, N6), Fn(T7, N7), Fn(T8, N8), Fn(T9, N9)
#define FOREACH_PAIR_22(Fn, T0, N0, ...) \
    Fn(T0, N0), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_24(Fn, T0, N0, T1, N1, ...) \
    Fn(T0, N0), Fn(T1, N1), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_26(Fn, T0, N0, T1, N1, T2, N2, ...) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_28(Fn, T0, N0, T1, N1, T2, N2, T3, N3, ...) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_30(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, ...) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_32(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, ...) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_34(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, T6, N6, ...) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), Fn(T6, N6), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_36(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, T6, N6, T7, N7, ...) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), Fn(T6, N6), Fn(T7, N7), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_38(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, T6, N6, T7, N7, T8, N8, ...) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), Fn(T6, N6), Fn(T7, N7), Fn(T8, N8), FOREACH_PAIR_20(Fn, __VA_ARGS__)
#define FOREACH_PAIR_40(Fn, T0, N0, T1, N1, T2, N2, T3, N3, T4, N4, T5, N5, T6, N6, T7, N7, T8, N8, T9, N9, ...) \
    Fn(T0, N0), Fn(T1, N1), Fn(T2, N2), Fn(T3, N3), Fn(T4, N4), Fn(T5, N5), Fn(T6, N6), Fn(T7, N7), Fn(T8, N8), Fn(T9, N9), FOREACH_PAIR_20(Fn, __VA_ARGS__)


#define FOREACH_PAIR_N(Name, Count, ...) \
    CALL2(FOREACH_PAIR_, Count, Name, __VA_ARGS__)

#define FOREACH_PAIR(Fn, ...) \
    FOREACH_PAIR_N(Fn, COUNT_ARGS(__VA_ARGS__), __VA_ARGS__)

#define DEFINE_KERNEL_N(Name, Count, ...) \
    CALL2(DEFINE_KERNEL_, Count, Name, __VA_ARGS__)

#define GEN_PARAMETER(Type, Name) Type Name
#define GEN_NAME_STRING(Type, Name) #Name

#define DEFINE_KERNEL(Library, Name, ...) \
    extern void Name( FOREACH_PAIR(GEN_PARAMETER, __VA_ARGS__) ); \
    static constexpr const char * Name ## _Args[COUNT_ARGS(__VA_ARGS__) / 2] = { FOREACH_PAIR(GEN_NAME_STRING, __VA_ARGS__) }; \
    namespace { \
        static struct InitKernel ## Library ## Name { \
            InitKernel ## Library ## Name () \
            { \
                registerCpuKernel(#Library, #Name, &Name, Name ## _Args); \
            } \
        } initKernel ## Library ## Name; \
    } \
    void Name( FOREACH_PAIR(GEN_PARAMETER, __VA_ARGS__) )


