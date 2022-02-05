/** randomforest_kernels.cc                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#pragma once

#include "mldb/block/compute_kernel_cpu.h"
#include "mldb/compiler/compiler.h"
#include "mldb/block/ukl/ukl_macros.h"

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

template<typename T> struct LocalArray {};

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

#if 0
    struct await_reduction: public promise_type {
        std::coroutine_handle<promise_type> * hp_ = nullptr;

        await_reduction(StaticConstCharPtr file, int line, SimdGroupReductionKind kind, uint32_t arg)
        {
            using namespace std;
            cerr << "await_reduction at " << this << endl;
            this->yield_value(BarrierOp(SIMD_GROUP_REDUCTION, file, line, arg, kind));
        }

        bool await_ready()
        {
            return false;
        }

        void await_suspend(std::coroutine_handle<promise_type> h) { hp_ = &h; }

        constexpr uint32_t await_resume() const noexcept { return hp_->promise().value_; }
    };
#endif

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

    const BarrierOp & get_barrier()
    {
        fill();
        full_ = false;
        const auto & p = h_.promise();
        ExcAssertEqual(p.kind_, CRT_BARRIER);
        return p.barrier_;
    }

    T get_result()
    {
        fill();
        full_ = false;
        const auto & p = h_.promise();
        ExcAssertEqual(p.kind_, CRT_VALUE);
        return std::move(p.value_);
    }

    auto & get_promise() { return h_.promise(); }

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
    return v->load(std::memory_order_relaxed);
}

template<typename T, typename T2>
void atom_store(std::atomic<T> * v, T2 && arg)
{
    return v->store(arg, std::memory_order_relaxed);
}

template<typename T, typename T2>
T atom_add(std::atomic<T> * v, T2 && arg)
{
    return v->fetch_add(arg, std::memory_order_relaxed);
}

template<typename T, typename T2>
T atom_sub(std::atomic<T> * v, T2 && arg)
{
    return v->fetch_sub(arg, std::memory_order_relaxed);
}

template<typename T>
T atom_inc(std::atomic<T> * v)
{
    return v->fetch_add(1, std::memory_order_relaxed);
}

template<typename T>
T atom_dec(std::atomic<T> * v)
{
    return v->fetch_sub(1, std::memory_order_relaxed);
}

template<typename T, typename T2>
T atom_or(std::atomic<T> * v, T2 && arg)
{
    return v->fetch_or(arg, std::memory_order_relaxed);
}

template<typename T>
T atom_load_local(std::atomic<T> * av)
{
    T * v = reinterpret_cast<T *>(av);
    return *v;
}

template<typename T, typename T2>
void atom_store_local(std::atomic<T> * av, T2 && arg)
{
    T * v = reinterpret_cast<T *>(av);
    *v = arg;
}

template<typename T, typename T2>
T atom_add_local(std::atomic<T> * av, T2 && arg)
{
    T * v = reinterpret_cast<T *>(av);
    T result = *v;
    *v += arg;
    return result;
}

template<typename T, typename T2>
T atom_sub_local(std::atomic<T> * av, T2 && arg)
{
    T * v = reinterpret_cast<T *>(av);
    T result = *v;
    *v -= arg;
    return result;
}

template<typename T>
T atom_inc_local(std::atomic<T> * av)
{
    T * v = reinterpret_cast<T *>(av);
    T result = *v;
    *v += 1;
    return result;
}

template<typename T>
T atom_dec_local(std::atomic<T> * av)
{
    T * v = reinterpret_cast<T *>(av);
    T result = *v;
    *v -= 1;
    return result;
}

template<typename T, typename T2>
T atom_or_local(std::atomic<T> * av, T2 && arg)
{
    T * v = reinterpret_cast<T *>(av);
    T result = *v;
    *v |= arg;
    return result;
}

template<typename T>
auto ukl_clz(T && arg)
{
    return std::countl_zero(arg);
}

//void barrier(LocalBarrier)
//{
//}


#define ukl_simdgroup_barrier() co_yield MLDB::BarrierOp{MLDB::SIMD_GROUP_BARRIER, __FILE__, __LINE__}
#define ukl_threadgroup_barrier() co_yield MLDB::BarrierOp{MLDB::THREAD_GROUP_BARRIER, __FILE__, __LINE__}

#define UKL_FUNCTION(Version, Return, Name) inline Return Name
#define SYNC_FUNCTION(Version, Return, Name) MLDB_ALWAYS_INLINE MLDB::coro_state_t<Return> MLDB_WARN_UNUSED_RESULT Name
#define SYNC_RETURN(Val) co_return Val;
#define SYNC_CALL(Fn, ...) \
    ({ auto coro = Fn(__VA_ARGS__);  while (coro.has_barrier()) { co_yield coro.get_barrier(); } get_result(coro); })

#define ukl_cpu_vec_el_x() 0
#define ukl_cpu_vec_el_y() 1
#define ukl_cpu_vec_el_z() 2
#define ukl_cpu_vec_el_w() 3

#define ukl_vec_el(expr, el) ((expr)[CALL2_A(ukl_cpu_vec_el_, el)])

#define ukl_popcount(x) std::popcount(x)

#define SYNC_CALL_0(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_1(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_2(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_3(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_4(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_5(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_6(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_7(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_8(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_9(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_10(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_11(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_12(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_13(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_14(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_15(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_16(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_17(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_18(...) SYNC_CALL(__VA_ARGS__)
#define SYNC_CALL_19(...) SYNC_CALL(__VA_ARGS__)

#define UKL_CPU_BACKEND 1

SYNC_FUNCTION(v1,
uint32_t, ukl_simdgroup_ballot2) (bool val, uint16_t simd_lane, __local atomic_uint * tmp)
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

inline void ukl_simdgroup_sum_reducer(std::span<uint32_t> vals)
{
    uint32_t res = 0;
    for (auto v: vals)
        res += v;
    for (auto & v: vals)
        v = res;
}

#define ukl_simdgroup_sum(val, Lane, Tmp) ({ (void)(Lane); (void)(Tmp); uint32_t res;  co_yield MLDB::BarrierOp{MLDB::SIMD_GROUP_REDUCTION, __FILE__, __LINE__, val, &ukl_simdgroup_sum_reducer, &res};  res;})

inline void ukl_simdgroup_ballot_reducer(std::span<uint32_t> vals)
{
    uint32_t res = 0;
    for (size_t i = 0;  i < vals.size();  ++i)
        res |= (vals[i] ? 1 : 0) << i;
    for (auto & v: vals)
        v = res;
}

#define ukl_simdgroup_ballot(val, Lane, Tmp) ({ (void)(Lane); (void)(Tmp); uint32_t res;  co_yield MLDB::BarrierOp{MLDB::SIMD_GROUP_REDUCTION, __FILE__, __LINE__, val, &ukl_simdgroup_ballot_reducer, &res};  res;})

#if 0
SYNC_FUNCTION(v1,
uint32_t, simdgroup_ballot) (bool val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    uint32_t res;
    co_yield MLDB::BarrierOp{MLDB::SIMD_GROUP_REDUCTION, __FILE__, __LINE__, val, &simdgroup_sum_reducer, &res};
    co_return res;
}
uint32_t simdgroup_ballot(bool val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    return co_await MLDB::coro_state_t<uint32_t>::await_reduction{__FILE__, __LINE__, MLDB::SIMDGROUP_BALLOT, val};
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

#endif

static inline uint32_t ukl_prefix_exclusive_sum_bitmask(uint32_t bits, uint16_t n)
{
    return ukl_popcount(bits & ((1U << n)-1));
    //return n == 0 ? 0 : ukl_popcount(bits << (32 - n));
}

SYNC_FUNCTION(v1,
uint32_t, ukl_simdgroup_prefix_exclusive_sum_bools) (bool val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    uint32_t ballots = ukl_simdgroup_ballot(val, simd_lane, tmp);
    SYNC_RETURN(ukl_prefix_exclusive_sum_bitmask(ballots, simd_lane));
}

inline void ukl_simdgroup_broadcast_first_reducer(std::span<uint32_t> vals)
{
    for (auto & v: vals)
        v = vals[0];
}

#define ukl_simdgroup_broadcast_first(val, Lane, Tmp) ({ (void)(Lane); (void)(Tmp); uint32_t res;  co_yield MLDB::BarrierOp{MLDB::SIMD_GROUP_REDUCTION, __FILE__, __LINE__, val, &ukl_simdgroup_broadcast_first_reducer, &res};  res;})

#if 0
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
#endif

#define ukl_assert(x) ExcAssert(x)
#define ukl_assert_equal(x, y) ExcAssertEqual(x, y)
#define ukl_assert_not_equal(x, y) ExcAssertNotEqual(x, y)
#define ukl_assert_less(x, y) ExcAssertLess(x, y)
#define ukl_assert_less_equal(x, y) ExcAssertLessEqual(x, y)
#define ukl_assert_greater(x, y) ExcAssertGreater(x, y)
#define ukl_assert_greater_equal(x, y) ExcAssertGreaterEqual(x, y)

#define KERNEL_RETURN() co_return -1

#define ROBUFFER(Type) MLDB::Span<Type, true, false>
#define RWBUFFER(Type) MLDB::Span<Type, true, true>
#define WOBUFFER(Type) MLDB::Span<Type, false, true>

#define RWLOCAL(Type) MLDB::Span<Type, true, true>

#define CONSTBUFFER(Type) MLDB::Span<Type, true, false>

#define UKL_LOCAL_PTR(Type) Type *
#define UKL_LOCAL_PTR_CAST(Type) (Type *)
#define UKL_GLOBAL_PTR(Type) Type *
#define UKL_GLOBAL_PTR_CAST(Type) (Type *)

#define CAST_RWBUFFER(Buf, Type) (Buf).template cast<Type, true, true>()
#define CAST_ROBUFFER(Buf, Type) (Buf).template cast<Type, true, false>()

#define CAST_RWLOCAL(Buf, Type) (Buf).template cast<Type, true, true>()
#define CAST_ROLOCAL(Buf, Type) (Buf).template cast<Type, true, false>()

#define RWLOCAL_VALUE(Expr) MLDB::Span<decltype(Expr), true, true>(&(Expr))

#define GEN_PARAMETER(Type, Name) Type Name
#define GEN_NAME_STRING(Type, Name) { #Name, "", "", "" }

#define DEFINE_KERNEL(Library, Name, ...) \
    extern void Name(const ThreadGroupExecutionState &, FOREACH_PAIR(GEN_PARAMETER, __VA_ARGS__) ); \
    static constexpr GridKernelParameterInfo Name ## _ArgInfo[] = { FOREACH_PAIR(GEN_NAME_STRING, __VA_ARGS__) }; \
    namespace { \
        static struct InitKernel ## Library ## Name { \
            InitKernel ## Library ## Name () \
            { \
                registerCpuKernel(#Library, #Name, &Name, Name ## _ArgInfo); \
            } \
        } initKernel ## Library ## Name; \
    } \
    void Name(const ThreadGroupExecutionState &, FOREACH_PAIR(GEN_PARAMETER, __VA_ARGS__) )

#define GEN_TYPE_RWBUFFER(Type) MLDB::Span<Type, true, true>
#define GEN_TYPE_ROBUFFER(Type) MLDB::Span<Type, true, false>
#define GEN_TYPE_LITERAL(Type) Type
#define GEN_TYPE_RWLOCAL(Type) LocalArray<Type>
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

#define GEN_INNER_TYPE_RWBUFFER(Type) MLDB::Span<Type, true, true>
#define GEN_INNER_TYPE_ROBUFFER(Type) MLDB::Span<Type, true, false>
#define GEN_INNER_TYPE_LITERAL(Type) Type
#define GEN_INNER_TYPE_RWLOCAL(Type) MLDB::Span<Type, true, true>
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

#define GEN_OUTER_ARG_RWBUFFER(Type, Name) std::span<Type> Name
#define GEN_OUTER_ARG_ROBUFFER(Type, Name) std::span<const Type> Name
#define GEN_OUTER_ARG_LITERAL(Type, Name) Type Name
#define GEN_OUTER_ARG_RWLOCAL(Type, Name) LocalArray<Type> Name
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

#define GEN_SIMDGROUP_PARAMETER_RWBUFFER(Type, Name) MLDB::Span<Type, true, true> Name
#define GEN_SIMDGROUP_PARAMETER_ROBUFFER(Type, Name) MLDB::Span<Type, true, false> Name
#define GEN_SIMDGROUP_PARAMETER_LITERAL(Type, Name) Type Name
#define GEN_SIMDGROUP_PARAMETER_RWLOCAL(Type, Name) MLDB::Span<Type, true, true> Name
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

#define PASS_INNER_ARG_RWBUFFER(Type, Name) Name
#define PASS_INNER_ARG_ROBUFFER(Type, Name) Name
#define PASS_INNER_ARG_LITERAL(Type, Name) Name
#define PASS_INNER_ARG_RWLOCAL(Type, Name) Name
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

#define PASS_SIMDGROUP_ARG_RWBUFFER(Type, Name) Name
#define PASS_SIMDGROUP_ARG_ROBUFFER(Type, Name) Name
#define PASS_SIMDGROUP_ARG_LITERAL(Type, Name) Name
#define PASS_SIMDGROUP_ARG_RWLOCAL(Type, Name) __state.getLocal<Type>(#Name)
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
#define GEN_ARG_INFO_RWBUFFER(Access, Type, Name, Bounds) GEN_ARG_INFO_STRUCT(Access, Type, Name, Bounds)
#define GEN_ARG_INFO_ROBUFFER(Access, Type, Name, Bounds) GEN_ARG_INFO_STRUCT(Access, Type, Name, Bounds)
#define GEN_ARG_INFO_LITERAL(Access, Type, Name, Bounds) GEN_ARG_INFO_STRUCT(Access, Type, Name, Bounds)
#define GEN_ARG_INFO_RWLOCAL(Access, Type, Name, Bounds) GEN_ARG_INFO_STRUCT(Access, Type, Name, Bounds)
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
        for (const auto & __threadState: __state) { \
            __states.emplace_back(__threadState); \
            __coros.emplace_back(Name ## _inner(__states.back() FOREACH_QUAD(PASS_INNER_ARG2, __VA_ARGS__))); \
        } \
        /*std::cerr << "done init" << std::endl;*/ \
        while (__coros[0].has_barrier()) { \
            std::vector<BarrierOp> __barriers(__nthreads); \
            auto & __barrier = __barriers[0] = __coros[0].get_barrier(); \
            /*std::cerr << "got barrier from simd group 0" << std::endl;*/ \
            for (size_t __n = 1;  __n < __nthreads;  ++__n) { \
                /*std::cerr << "checking thread " << __n << std::endl;*/ \
                if (!__coros[__n].has_barrier()) { \
                    throw_barriers_out_of_sync(0, __barrier, __n); \
                } \
                auto & __barrier2 = __barriers[__n] = __coros[__n].get_barrier(); \
                verify_barriers_in_sync(__barrier, __barrier2); \
            } \
            switch (__barrier.kind) { \
            case THREAD_GROUP_BARRIER: \
                co_yield __barrier; \
                break; \
            case SIMD_GROUP_REDUCTION: { \
                /*std::cerr << "simd group reduction" << std::endl;*/ \
                std::vector<uint32_t> reductions_in(__nthreads); \
                for (size_t __n = 0;  __n < __nthreads;  ++__n) { \
                    uint32_t arg = __barriers[__n].arg; \
                    reductions_in[__n] = arg; \
                } \
                /*std::cerr << "running reducer" << std::endl; */ \
                /*std::cerr << jsonEncodeStr(__barrier) << std::endl;*/ \
                ExcAssert(__barrier.reducer); \
                __barrier.reducer(reductions_in); \
                /*std::cerr << "done running reducer" << std::endl; */ \
                for (size_t __n = 0;  __n < __nthreads;  ++__n) { \
                    ExcAssert(__barriers[__n].res); \
                    *__barriers[__n].res = reductions_in[__n]; \
                } \
                /*std::cerr << "done simd group reduction" << std::endl;*/ \
                break; \
            }\
            default:\
                break; \
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
        for (const auto & __simdGroupState: __state) { \
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
                auto & __barrier2 = __coros[__n].get_barrier(); \
                verify_barriers_in_sync(__barrier, __barrier2); \
            } \
        } \
        for (__n = 0;  __n < __nsimdgroups;  ++__n) { \
            /*std::cerr << "finishing thread " << __n << std::endl;*/ \
            if (!MLDB_UNLIKELY(__coros[__n].has_result())) \
                throw_thread_has_no_result(__n); \
        } \
    } \
    static constexpr GridKernelParameterInfo Name ## _ArgInfo[] = { {"","","",""} FOREACH_QUAD(GEN_ARG_INFO2, __VA_ARGS__) }; \
    namespace { \
        static struct InitKernel ## Library ## Name { \
            InitKernel ## Library ## Name () \
            { \
                registerCpuKernel(#Library, #Name, &Name ## _threadgroup, Name ## _ArgInfo + 1); \
            } \
        } initKernel ## Library ## Name; \
    } \
    inline MLDB::kernel_coro Name ## _inner(const MLDB::ThreadExecutionState & __state FOREACH_QUAD(GEN_INNER_PARAMETER2, __VA_ARGS__) )

