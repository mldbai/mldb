/** randomforest_kernels.cc                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#pragma once

#include "compute_kernel_cpu.h"
#include "mldb/compiler/compiler.h"

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
using std::experimental::suspend_always;
using std::experimental::suspend_never;
using std::experimental::coroutine_handle;
} // namespace std

namespace MLDB {

template<typename T>
struct value_holder {
    CoroReturnKind kind_ = CRT_NONE;
    T value_;
    void return_value(T v)
    {
        value_ = std::move(v);
        kind_ = CRT_VALUE;
    }
};

template<>
struct value_holder<void> {
    CoroReturnKind kind_ = CRT_NONE;
    void return_void()
    {
        kind_ = CRT_VALUE;
    }
};

template<typename T>
struct coro_state_t {
    struct promise_type;
    using handle_type = std::coroutine_handle<promise_type>;

    // This will:
    // 1) Yield as many Barrier operations as there are synchronization points in the function
    // 2) Return the final value
    struct promise_type: public value_holder<T> {
        BarrierOp barrier_;
        std::exception_ptr exc_;
        using value_holder<T>::kind_;
        coro_state_t get_return_object() { return {handle_type::from_promise(*this)}; }
        std::suspend_always initial_suspend() { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void unhandled_exception() { kind_ = CRT_EXCEPTION; exc_ = std::current_exception(); }
        std::suspend_always yield_value(BarrierOp barrier) { kind_ = CRT_BARRIER; barrier_ = barrier; return {}; }
    };

    handle_type h_;

    coro_state_t() {}
    coro_state_t(handle_type h)
        : h_(std::move(h))
    {
    }

    coro_state_t(coro_state_t && other)
         : h_(std::move(other.h_))
    {
        other.h_ = {};
    }


    coro_state_t & operator = (coro_state_t && other)
    {
        h_ = std::move(other.h_);  other.h_ = {};
        return *this;
    }

    ~coro_state_t()
    {
        //using namespace std;
        //cerr << "destroying coroutine handle" << endl;
        if (h_) h_.destroy();
    }

    bool has_barrier()
    {
        fill();
        return !h_.done() && h_.promise().kind_ == CRT_BARRIER;
    }

    bool has_result()
    {
        fill();
        return h_.done() && h_.promise().kind_ == CRT_VALUE;
    }

    BarrierOp get_barrier()
    {
        fill();
        full_ = false;
        const auto & p = h_.promise();
        ExcAssertEqual(p.kind_, CRT_BARRIER);
        return std::move(p.barrier_);
    }

    T get_result()
    {
        fill();
        full_ = false;
        const auto & p = h_.promise();
        ExcAssertEqual(p.kind_, CRT_VALUE);
        return std::move(p.value_);
    }

private:
    bool full_ = false;

    void fill()
    {
        if (!full_) {
            h_();
            if (h_.promise().exc_)
                std::rethrow_exception(h_.promise().exc_);
            full_ = true;
        }
    }
};

template<typename T>
inline T get_result(coro_state_t<T> & state)
{
    return state.get_result();
}

inline void get_result(coro_state_t<void> & state)
{
    auto r = state.has_result();
    ExcAssert(r);
}

using kernel_coro = coro_state_t<int>;  // should be void, but that requires a specialization

} // namespace MLDB

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

template<typename T>
T atom_load(std::atomic<T> * v)
{
    return v->load();
}

template<typename T, typename T2>
void atom_store(std::atomic<T> * v, T2 && arg)
{
    return v->store(arg);
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

//void barrier(LocalBarrier)
//{
//}


#define ukl_simdgroup_barrier() co_yield MLDB::BarrierOp{MLDB::SIMD_GROUP_BARRIER, __FILE__, __LINE__}
#define ukl_threadgroup_barrier() co_yield MLDB::BarrierOp{MLDB::THREAD_GROUP_BARRIER, __FILE__, __LINE__}

#define SYNC_FUNCTION(Version, Return, Name) MLDB::coro_state_t<Return> MLDB_WARN_UNUSED_RESULT Name
#define SYNC_RETURN(Val) co_return Val;
#define SYNC_CALL(Fn, ...) \
    ({ auto coro = Fn(__VA_ARGS__);  while (coro.has_barrier()) { co_yield coro.get_barrier(); } get_result(coro); })


#define barrier(X) { co_yield BarrierOp{X, __FILE__, __LINE__}; }

SYNC_FUNCTION(v1,
uint32_t, simdgroup_ballot) (bool val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    ukl_simdgroup_barrier();

    if (simd_lane == 0) {
        tmp[0] = 0;
    }

    //using namespace std;
    //cerr << "thread " << simd_lane << " arriving at barrier A tmp = " << tmp << " *tmp = " << *tmp << endl;
    ukl_simdgroup_barrier();
    //cerr << "thread " << simd_lane << " after barrier A" << endl;

    atom_or(tmp + 0, (uint32_t)val << simd_lane);

    //cerr << "thread " << simd_lane << " arriving at barrier B" << " *tmp = " << *tmp << endl;
    ukl_simdgroup_barrier();
    //cerr << "thread " << simd_lane << " after barrier B returning " << tmp[0] << endl;

    SYNC_RETURN(tmp[0]);
}

SYNC_FUNCTION(v1,
uint32_t, simdgroup_sum) (uint32_t val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    ukl_simdgroup_barrier();

    if (simd_lane == 0) {
        tmp[0] = 0;
    }

    ukl_simdgroup_barrier();

    atom_add(tmp + 0, val);

    ukl_simdgroup_barrier();

    SYNC_RETURN(tmp[0]);
}

static inline uint32_t prefix_exclusive_sum_bitmask(uint32_t bits, uint16_t n)
{
    return popcount(bits & ((1U << n)-1));
    //return n == 0 ? 0 : popcount(bits << (32 - n));
}

SYNC_FUNCTION(v1,
uint32_t, simdgroup_prefix_exclusive_sum_bools) (bool val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    uint32_t ballots = SYNC_CALL(simdgroup_ballot, val, simd_lane, tmp);
    SYNC_RETURN(prefix_exclusive_sum_bitmask(ballots, simd_lane));
}

SYNC_FUNCTION(v1,
uint32_t, simdgroup_broadcast_first) (uint32_t val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    ukl_simdgroup_barrier();

    if (simd_lane == 0) {
        tmp[0] = val;
    }

    ukl_simdgroup_barrier();

    SYNC_RETURN(tmp[0]);
}

#define KERNEL_RETURN() co_return -1

// Returns the hundred and first argument in the list.  Used to implement COUNT_ARGS.
#define TWO_HUNDRED_AND_FIRST_ARGUMENT(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, \
                             a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, \
                             a20, a21, a22, a23, a24, a25, a26, a27, a28, a29, \
                             a30, a31, a32, a33, a34, a35, a36, a37, a38, a39, \
                             a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, \
                             a50, a51, a52, a53, a54, a55, a56, a57, a58, a59, \
                             a60, a61, a62, a63, a64, a65, a66, a67, a68, a69, \
                             a70, a71, a72, a73, a74, a75, a76, a77, a78, a79, \
                             a80, a81, a82, a83, a84, a85, a86, a87, a88, a89, \
                             a90, a91, a92, a93, a94, a95, a96, a97, a98, a99, \
                             a100, a101, a102, a103, a104, a105, a106, a107, a108, a109, \
                             a110, a111, a112, a113, a114, a115, a116, a117, a118, a119, \
                             a120, a121, a122, a123, a124, a125, a126, a127, a128, a129, \
                             a130, a131, a132, a133, a134, a135, a136, a137, a138, a139, \
                             a140, a141, a142, a143, a144, a145, a146, a147, a148, a149, \
                             a150, a151, a152, a153, a154, a155, a156, a157, a158, a159, \
                             a160, a161, a162, a163, a164, a165, a166, a167, a168, a169, \
                             a170, a171, a172, a173, a174, a175, a176, a177, a178, a179, \
                             a180, a181, a182, a183, a184, a185, a186, a187, a188, a189, \
                             a190, a191, a192, a193, a194, a195, a196, a197, a198, a199, \
                             a200, ...) a200

// Returns the number of arguments in its argument list
#define COUNT_ARGS(...) TWO_HUNDRED_AND_FIRST_ARGUMENT(dummy, ## __VA_ARGS__, 199, 198, 197, 196, 195, 194, 193, 192, 191, 190, 189, 188, 187, 186, 185, 184, 183, 182, 181, 180, 179, 178, 177, 176, 175, 174, 173, 172, 171, 170, 169, 168, 167, 166, 165, 164, 163, 162, 161, 160, 159, 158, 157, 156, 155, 154, 153, 152, 151, 150, 149, 148, 147, 146, 145, 144, 143, 142, 141, 140, 139, 138, 137, 136, 135, 134, 133, 132, 131, 130, 129, 128, 127, 126, 125, 124, 123, 122, 121, 120, 119, 118, 117, 116, 115, 114, 113, 112, 111, 110, 109, 108, 107, 106, 105, 104, 103, 102, 101, 100, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89, 88, 87, 86, 85, 84, 83, 82, 81, 80, 79, 78, 77, 76, 75, 74, 73, 72, 71, 70, 69, 68, 67, 66, 65, 64, 63, 62, 61, 60, 59, 58, 57, 56, 55, 54, 53, 52, 51, 50, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

// Call the given macro with the given arguments
#define CALL(Fn, ...) Fn(__VA_ARGS__)

// Call the macro given by concatenating Name1 and Name2 with the given args
#define CALL2(Name1, Name2, ...) CALL(Name1 ## Name2, __VA_ARGS__)

// Another version so we can apply call when evaluating call
#define CALL_A(Fn, ...) Fn(__VA_ARGS__)
#define CALL2_A(Name1, Name2, ...) CALL_A(Name1 ## Name2, __VA_ARGS__)

#define CALL_B(Fn, ...) Fn(__VA_ARGS__)
#define CALL2_B(Name1, Name2, ...) CALL_B(Name1 ## Name2, __VA_ARGS__)

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

#define COMMA_IF(...) __VA_OPT__(,)

#define COMMA_IF_CALL(Fn, ...) COMMA_IF(CALL_B(Fn, __VA_ARGS__))

#define MAYBE_COMMA(Fn, ...) COMMA_IF_CALL(Fn, __VA_ARGS__) CALL_B(Fn, __VA_ARGS__) 

#define FOREACH_QUAD_0(Fn)
#define FOREACH_QUAD_4(Fn, A0, A1, A2, A3)        MAYBE_COMMA(Fn, A0, A1, A2, A3)
#define FOREACH_QUAD_8(Fn, A0, A1, A2, A3, ...)   MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_4(Fn, __VA_ARGS__)
#define FOREACH_QUAD_12(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_8(Fn, __VA_ARGS__)
#define FOREACH_QUAD_16(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_12(Fn, __VA_ARGS__)
#define FOREACH_QUAD_20(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_16(Fn, __VA_ARGS__)
#define FOREACH_QUAD_24(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_20(Fn, __VA_ARGS__)
#define FOREACH_QUAD_28(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_24(Fn, __VA_ARGS__)
#define FOREACH_QUAD_32(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_28(Fn, __VA_ARGS__)
#define FOREACH_QUAD_36(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_32(Fn, __VA_ARGS__)
#define FOREACH_QUAD_40(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_36(Fn, __VA_ARGS__)
#define FOREACH_QUAD_44(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_40(Fn, __VA_ARGS__)
#define FOREACH_QUAD_48(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_44(Fn, __VA_ARGS__)
#define FOREACH_QUAD_52(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_48(Fn, __VA_ARGS__)
#define FOREACH_QUAD_56(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_52(Fn, __VA_ARGS__)
#define FOREACH_QUAD_60(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_56(Fn, __VA_ARGS__)
#define FOREACH_QUAD_64(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_60(Fn, __VA_ARGS__)
#define FOREACH_QUAD_68(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_64(Fn, __VA_ARGS__)
#define FOREACH_QUAD_72(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_68(Fn, __VA_ARGS__)
#define FOREACH_QUAD_76(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_72(Fn, __VA_ARGS__)
#define FOREACH_QUAD_80(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_76(Fn, __VA_ARGS__)
#define FOREACH_QUAD_84(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_80(Fn, __VA_ARGS__)
#define FOREACH_QUAD_88(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_84(Fn, __VA_ARGS__)
#define FOREACH_QUAD_92(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_88(Fn, __VA_ARGS__)
#define FOREACH_QUAD_96(Fn, A0, A1, A2, A3, ...)  MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_92(Fn, __VA_ARGS__)
#define FOREACH_QUAD_100(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_96(Fn, __VA_ARGS__)
#define FOREACH_QUAD_104(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_100(Fn, __VA_ARGS__)
#define FOREACH_QUAD_108(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_104(Fn, __VA_ARGS__)
#define FOREACH_QUAD_112(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_108(Fn, __VA_ARGS__)
#define FOREACH_QUAD_116(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_112(Fn, __VA_ARGS__)
#define FOREACH_QUAD_120(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_116(Fn, __VA_ARGS__)
#define FOREACH_QUAD_124(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_120(Fn, __VA_ARGS__)
#define FOREACH_QUAD_128(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_124(Fn, __VA_ARGS__)
#define FOREACH_QUAD_132(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_128(Fn, __VA_ARGS__)
#define FOREACH_QUAD_136(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_132(Fn, __VA_ARGS__)
#define FOREACH_QUAD_140(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_136(Fn, __VA_ARGS__)
#define FOREACH_QUAD_144(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_140(Fn, __VA_ARGS__)
#define FOREACH_QUAD_148(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_144(Fn, __VA_ARGS__)
#define FOREACH_QUAD_152(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_148(Fn, __VA_ARGS__)
#define FOREACH_QUAD_156(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_152(Fn, __VA_ARGS__)
#define FOREACH_QUAD_160(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_156(Fn, __VA_ARGS__)
#define FOREACH_QUAD_164(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_160(Fn, __VA_ARGS__)
#define FOREACH_QUAD_168(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_164(Fn, __VA_ARGS__)
#define FOREACH_QUAD_172(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_168(Fn, __VA_ARGS__)
#define FOREACH_QUAD_176(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_172(Fn, __VA_ARGS__)
#define FOREACH_QUAD_180(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_176(Fn, __VA_ARGS__)
#define FOREACH_QUAD_184(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_180(Fn, __VA_ARGS__)
#define FOREACH_QUAD_188(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_184(Fn, __VA_ARGS__)
#define FOREACH_QUAD_192(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_188(Fn, __VA_ARGS__)
#define FOREACH_QUAD_196(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_192(Fn, __VA_ARGS__)
#define FOREACH_QUAD_200(Fn, A0, A1, A2, A3, ...) MAYBE_COMMA(Fn, A0, A1, A2, A3) FOREACH_QUAD_196(Fn, __VA_ARGS__)

#define FOREACH_QUAD_N(Name, Count, ...) \
    CALL2(FOREACH_QUAD_, Count, Name, __VA_ARGS__)

#define FOREACH_QUAD(Fn, ...) \
    FOREACH_QUAD_N(Fn, COUNT_ARGS(__VA_ARGS__), __VA_ARGS__)


#define GEN_PARAMETER(Type, Name) Type Name
#define GEN_NAME_STRING(Type, Name) { #Name, "", "", "" }

#define DEFINE_KERNEL(Library, Name, ...) \
    extern void Name(const ThreadGroupExecutionState &, FOREACH_PAIR(GEN_PARAMETER, __VA_ARGS__) ); \
    static constexpr CPUGridKernelParameterInfo Name ## _ArgInfo[] = { FOREACH_PAIR(GEN_NAME_STRING, __VA_ARGS__) }; \
    namespace { \
        static struct InitKernel ## Library ## Name { \
            InitKernel ## Library ## Name () \
            { \
                registerCpuKernel(#Library, #Name, &Name, Name ## _ArgInfo); \
            } \
        } initKernel ## Library ## Name; \
    } \
    void Name(const ThreadGroupExecutionState &, FOREACH_PAIR(GEN_PARAMETER, __VA_ARGS__) )

#define GEN_TYPE_BUFFER(Type) std::span<Type>
#define GEN_TYPE_LITERAL(Type) Type
#define GEN_TYPE_LOCAL(Type) LocalArray<Type>
#define GEN_TYPE_TUNEABLE(Type) Type
#define GEN_TYPE_GID0(Type) GridQuery<0,0,0,Type>
#define GEN_TYPE_GID1(Type) GridQuery<0,0,1,Type>
#define GEN_TYPE_GID2(Type) GridQuery<0,0,2,Type>
#define GEN_TYPE_GSZ0(Type) GridQuery<0,1,0,Type>
#define GEN_TYPE_GSZ1(Type) GridQuery<0,1,1,Type>
#define GEN_TYPE_GSZ2(Type) GridQuery<0,1,2,Type>
#define GEN_TYPE_LID0(Type) GridQuery<1,0,0,Type>
#define GEN_TYPE_LID1(Type) GridQuery<1,0,1,Type>
#define GEN_TYPE_LID2(Type) GridQuery<1,0,2,Type>
#define GEN_TYPE_LSZ0(Type) GridQuery<1,1,0,Type>
#define GEN_TYPE_LSZ1(Type) GridQuery<1,1,1,Type>
#define GEN_TYPE_LSZ2(Type) GridQuery<1,1,2,Type>

#define GEN_PARAMETER2(Access, Type, Name, Bounds) CALL2_A(GEN_TYPE_, Access, Type) Name
//#define GEN_PARAMETER2(Access, Type, Name, Bounds) Type Name
#define GEN_NAME_STRING2(Access, Type, Name, Bounds) #Name

#define GEN_INNER_TYPE_BUFFER(Type) std::span<Type>
#define GEN_INNER_TYPE_LITERAL(Type) Type
#define GEN_INNER_TYPE_LOCAL(Type) Type *
#define GEN_INNER_TYPE_TUNEABLE(Type) Type
#define GEN_INNER_TYPE_GID0(Type) const Type
#define GEN_INNER_TYPE_GID1(Type) const Type
#define GEN_INNER_TYPE_GID2(Type) const Type
#define GEN_INNER_TYPE_GSZ0(Type) const Type
#define GEN_INNER_TYPE_GSZ1(Type) const Type
#define GEN_INNER_TYPE_GSZ2(Type) const Type
#define GEN_INNER_TYPE_LID0(Type) const Type
#define GEN_INNER_TYPE_LID1(Type) const Type
#define GEN_INNER_TYPE_LID2(Type) const Type
#define GEN_INNER_TYPE_LSZ0(Type) const Type
#define GEN_INNER_TYPE_LSZ1(Type) const Type
#define GEN_INNER_TYPE_LSZ2(Type) const Type

#define GEN_INNER_PARAMETER2(Access, Type, Name, Bounds) CALL2_A(GEN_INNER_TYPE_, Access, Type) Name

#define GEN_OUTER_ARG_BUFFER(Type, Name) std::span<Type> Name
#define GEN_OUTER_ARG_LITERAL(Type, Name) Type Name
#define GEN_OUTER_ARG_LOCAL(Type, Name) LocalArray<Type> Name
#define GEN_OUTER_ARG_TUNEABLE(Type, Name) Type Name
#define GEN_OUTER_ARG_GID0(Type, Name)
#define GEN_OUTER_ARG_GID1(Type, Name)
#define GEN_OUTER_ARG_GID2(Type, Name)
#define GEN_OUTER_ARG_GSZ0(Type, Name)
#define GEN_OUTER_ARG_GSZ1(Type, Name)
#define GEN_OUTER_ARG_GSZ2(Type, Name)
#define GEN_OUTER_ARG_LID0(Type, Name)
#define GEN_OUTER_ARG_LID1(Type, Name)
#define GEN_OUTER_ARG_LID2(Type, Name)
#define GEN_OUTER_ARG_LSZ0(Type, Name)
#define GEN_OUTER_ARG_LSZ1(Type, Name)
#define GEN_OUTER_ARG_LSZ2(Type, Name)

#define GEN_OUTER_PARAMETER2(Access, Type, Name, Bounds) CALL2_A(GEN_OUTER_ARG_, Access, Type, Name)

#define GEN_SIMDGROUP_PARAMETER_BUFFER(Type, Name) std::span<Type> Name
#define GEN_SIMDGROUP_PARAMETER_LITERAL(Type, Name) Type Name
#define GEN_SIMDGROUP_PARAMETER_LOCAL(Type, Name) Type * Name
#define GEN_SIMDGROUP_PARAMETER_TUNEABLE(Type, Name) Type Name
#define GEN_SIMDGROUP_PARAMETER_GID0(Type, Name)
#define GEN_SIMDGROUP_PARAMETER_GID1(Type, Name)
#define GEN_SIMDGROUP_PARAMETER_GID2(Type, Name)
#define GEN_SIMDGROUP_PARAMETER_GSZ0(Type, Name)
#define GEN_SIMDGROUP_PARAMETER_GSZ1(Type, Name)
#define GEN_SIMDGROUP_PARAMETER_GSZ2(Type, Name)
#define GEN_SIMDGROUP_PARAMETER_LID0(Type, Name)
#define GEN_SIMDGROUP_PARAMETER_LID1(Type, Name)
#define GEN_SIMDGROUP_PARAMETER_LID2(Type, Name)
#define GEN_SIMDGROUP_PARAMETER_LSZ0(Type, Name)
#define GEN_SIMDGROUP_PARAMETER_LSZ1(Type, Name)
#define GEN_SIMDGROUP_PARAMETER_LSZ2(Type, Name)

#define GEN_SIMDGROUP_PARAMETER2(Access, Type, Name, Bounds) CALL2_A(GEN_SIMDGROUP_PARAMETER_, Access, Type, Name)

#define PASS_INNER_ARG_BUFFER(Type, Name) Name
#define PASS_INNER_ARG_LITERAL(Type, Name) Name
#define PASS_INNER_ARG_LOCAL(Type, Name) Name
#define PASS_INNER_ARG_TUNEABLE(Type, Name) Name
#define PASS_INNER_ARG_GID0(Type, Name) __threadState.globalId(0)
#define PASS_INNER_ARG_GID1(Type, Name) __threadState.globalId(1)
#define PASS_INNER_ARG_GID2(Type, Name) __threadState.globalId(2)
#define PASS_INNER_ARG_GSZ0(Type, Name) __threadState.globalSize(0)
#define PASS_INNER_ARG_GSZ1(Type, Name) __threadState.globalSize(1)
#define PASS_INNER_ARG_GSZ2(Type, Name) __threadState.globalSize(2)
#define PASS_INNER_ARG_LID0(Type, Name) __threadState.localId(0)
#define PASS_INNER_ARG_LID1(Type, Name) __threadState.localId(1)
#define PASS_INNER_ARG_LID2(Type, Name) __threadState.localId(2)
#define PASS_INNER_ARG_LSZ0(Type, Name) __threadState.localSize(0)
#define PASS_INNER_ARG_LSZ1(Type, Name) __threadState.localSize(1)
#define PASS_INNER_ARG_LSZ2(Type, Name) __threadState.localSize(2)

#define PASS_INNER_ARG2(Access, Type, Name, Bounds) CALL2_A(PASS_INNER_ARG_, Access, Type, Name)

#define PASS_SIMDGROUP_ARG_BUFFER(Type, Name) Name
#define PASS_SIMDGROUP_ARG_LITERAL(Type, Name) Name
#define PASS_SIMDGROUP_ARG_LOCAL(Type, Name) __state.getLocal<Type>(#Name)
#define PASS_SIMDGROUP_ARG_TUNEABLE(Type, Name) Name
#define PASS_SIMDGROUP_ARG_GID0(Type, Name)
#define PASS_SIMDGROUP_ARG_GID1(Type, Name)
#define PASS_SIMDGROUP_ARG_GID2(Type, Name)
#define PASS_SIMDGROUP_ARG_GSZ0(Type, Name)
#define PASS_SIMDGROUP_ARG_GSZ1(Type, Name)
#define PASS_SIMDGROUP_ARG_GSZ2(Type, Name)
#define PASS_SIMDGROUP_ARG_LID0(Type, Name)
#define PASS_SIMDGROUP_ARG_LID1(Type, Name)
#define PASS_SIMDGROUP_ARG_LID2(Type, Name)
#define PASS_SIMDGROUP_ARG_LSZ0(Type, Name)
#define PASS_SIMDGROUP_ARG_LSZ1(Type, Name)
#define PASS_SIMDGROUP_ARG_LSZ2(Type, Name)

#define PASS_SIMDGROUP_ARG2(Access, Type, Name, Bounds) CALL2_A(PASS_SIMDGROUP_ARG_, Access, Type, Name)


#define GEN_ARG_INFO_STRUCT(Access, Type, Name, Bounds) { #Name, #Access, #Type, #Bounds } 
#define GEN_ARG_INFO_BUFFER(Access, Type, Name, Bounds) GEN_ARG_INFO_STRUCT(Access, Type, Name, Bounds)
#define GEN_ARG_INFO_LITERAL(Access, Type, Name, Bounds) GEN_ARG_INFO_STRUCT(Access, Type, Name, Bounds)
#define GEN_ARG_INFO_LOCAL(Access, Type, Name, Bounds) GEN_ARG_INFO_STRUCT(Access, Type, Name, Bounds)
#define GEN_ARG_INFO_TUNEABLE(Access, Type, Name, Bounds) GEN_ARG_INFO_STRUCT(Access, Type, Name, Bounds)
#define GEN_ARG_INFO_GID0(Access, Type, Name, Bounds) 
#define GEN_ARG_INFO_GID1(Access, Type, Name, Bounds) 
#define GEN_ARG_INFO_GID2(Access, Type, Name, Bounds) 
#define GEN_ARG_INFO_GSZ0(Access, Type, Name, Bounds) 
#define GEN_ARG_INFO_GSZ1(Access, Type, Name, Bounds) 
#define GEN_ARG_INFO_GSZ2(Access, Type, Name, Bounds) 
#define GEN_ARG_INFO_LID0(Access, Type, Name, Bounds) 
#define GEN_ARG_INFO_LID1(Access, Type, Name, Bounds) 
#define GEN_ARG_INFO_LID2(Access, Type, Name, Bounds) 
#define GEN_ARG_INFO_LSZ0(Access, Type, Name, Bounds) 
#define GEN_ARG_INFO_LSZ1(Access, Type, Name, Bounds) 
#define GEN_ARG_INFO_LSZ2(Access, Type, Name, Bounds) 

#define GEN_ARG_INFO2(Access, Type, Name, Bounds) CALL2_A(GEN_ARG_INFO_, Access, Access, Type, Name, Bounds)

#define DEFINE_KERNEL2(Library, Name, ...) \
    inline MLDB::kernel_coro Name ## _inner(const MLDB::ThreadExecutionState & __state FOREACH_QUAD(GEN_INNER_PARAMETER2, __VA_ARGS__) ); \
    inline MLDB::kernel_coro Name ## _simdgroup(const MLDB::SimdGroupExecutionState & __state FOREACH_QUAD(GEN_SIMDGROUP_PARAMETER2, __VA_ARGS__) ) \
    { \
        using namespace MLDB; \
        size_t __nthreads = __state.threadsPerSimdGroup; \
        std::vector<kernel_coro> __coros; __coros.reserve(__nthreads); \
        std::vector<ThreadExecutionState> __states;  __states.reserve(__nthreads); \
        for (auto && __threadState: __state.threads()) { \
            __states.emplace_back(__threadState); \
            __coros.emplace_back(Name ## _inner(__states.back() FOREACH_QUAD(PASS_INNER_ARG2, __VA_ARGS__))); \
        } \
        /*std::cerr << "done init" << std::endl;*/ \
        while (__coros[0].has_barrier()) { \
            auto __barrier = __coros[0].get_barrier(); \
            /*std::cerr << "got barrier from simd group 0" << std::endl;*/ \
            for (size_t __n = 1;  __n < __nthreads;  ++__n) { \
                /*std::cerr << "checking thread " << __n << std::endl;*/ \
                if (!__coros[__n].has_barrier()) { \
                    throw_barriers_out_of_sync(0, __barrier, __n); \
                } \
                auto __barrier2 = __coros[__n].get_barrier(); \
                verify_barriers_in_sync(__barrier, __barrier2); \
            } \
            if (__barrier.kind != SIMD_GROUP_BARRIER) { \
                co_yield __barrier; \
            } \
        } \
        for (size_t __n = 0;  __n < __nthreads;  ++__n) { \
            if (!MLDB_UNLIKELY(__coros[__n].has_result())) \
                throw_thread_has_no_result(__n); \
            /*std::cerr << "finishing thread " << __n << std::endl;*/ \
        } \
        /*std::cerr << "finished barriers" << std::endl;*/ \
        co_return -1; \
    } \
    inline void Name ## _threadgroup(const MLDB::ThreadGroupExecutionState & __state FOREACH_QUAD(GEN_OUTER_PARAMETER2, __VA_ARGS__) ) \
    { \
        using namespace MLDB; \
        auto __nsimdgroups = __state.numSimdGroups(); \
        kernel_coro __coros[__nsimdgroups]; \
        SimdGroupExecutionState __states[__nsimdgroups]; \
        size_t __n = 0; \
        for (auto & __simdGroupState: __state.simdGroups()) { \
            __states[__n] = __simdGroupState; \
            __coros[__n] = Name ## _simdgroup(__states[__n] FOREACH_QUAD(PASS_SIMDGROUP_ARG2, __VA_ARGS__)); \
            ++__n; \
        } \
        /*std::cerr << "done init" << std::endl;*/ \
        while (__coros[0].has_barrier()) { \
            auto __barrier = __coros[0].get_barrier(); \
            /*std::cerr << "got barrier from simd group 0" << std::endl;*/ \
            for (__n = 1;  __n < __nsimdgroups;  ++__n) { \
                /*std::cerr << "checking thread " << __n << std::endl;*/ \
                if (!__coros[__n].has_barrier()) { \
                    throw_barriers_out_of_sync(0, __barrier, __n); \
                } \
                auto __barrier2 = __coros[__n].get_barrier(); \
                verify_barriers_in_sync(__barrier, __barrier2); \
            } \
        } \
        for (__n = 0;  __n < __nsimdgroups;  ++__n) { \
            /*std::cerr << "finishing thread " << __n << std::endl;*/ \
            if (!MLDB_UNLIKELY(__coros[__n].has_result())) \
                throw_thread_has_no_result(__n); \
        } \
    } \
    static constexpr CPUGridKernelParameterInfo Name ## _ArgInfo[] = { {"","","",""} FOREACH_QUAD(GEN_ARG_INFO2, __VA_ARGS__) }; \
    namespace { \
        static struct InitKernel ## Library ## Name { \
            InitKernel ## Library ## Name () \
            { \
                registerCpuKernel(#Library, #Name, &Name ## _threadgroup, Name ## _ArgInfo + 1); \
            } \
        } initKernel ## Library ## Name; \
    } \
    inline MLDB::kernel_coro Name ## _inner(const MLDB::ThreadExecutionState & __state FOREACH_QUAD(GEN_INNER_PARAMETER2, __VA_ARGS__) )

