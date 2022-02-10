#pragma once
#include "ukl_auto_macros.h"
#include "ukl_macros.h"

#ifndef __UKL_METAL_PREPROCESSING
#  include <metal_atomic>
#  include <metal_stdlib>
#endif /* __UKL_METAL_PREPROCESSING */

#define __constant constant
#define __kernel kernel
#define __global device
#define __local threadgroup

#define ukl_assert(x)
#define ukl_assert_equal(x, y)
#define ukl_assert_not_equal(x, y)
#define ukl_assert_less(x, y)
#define ukl_assert_less_equal(x, y)
#define ukl_assert_greater(x, y)
#define ukl_assert_greater_equal(x, y)

#define ROBUFFER(Type) device const Type *
#define RWBUFFER(Type) device Type *
#define WOBUFFER(Type) device Type *

#define RWLOCAL(Type) threadgroup Type *

#define CONSTBUFFER(Type) constant const Type *

#define CAST_RWBUFFER(Buf, Type) reinterpret_cast<Type device *>(Buf)
#define CAST_ROBUFFER(Buf, Type) reinterpret_cast<const Type device *>(Buf)

#define CAST_RWLOCAL(Buf, Type) reinterpret_cast<Type threadgroup *>(Buf)
#define CAST_ROLOCAL(Buf, Type) reinterpret_cast<const Type threadgroup *>(Buf)

#define UKL_LOCAL_PTR(Type) threadgroup Type *
#define UKL_LOCAL_PTR_CAST(Type) (threadgroup Type *)
#define UKL_GLOBAL_PTR(Type) device Type *
#define UKL_GLOBAL_PTR_CAST(Type) (device Type *)


#define ukl_simdgroup_barrier()
#define ukl_threadgroup_barrier() threadgroup_barrier(mem_flags::mem_threadgroup)
#define printf(...) 
#define UKL_FUNCTION(Version, Return, Name) Return Name
#define SYNC_FUNCTION(Version, Return, Name) Return Name
#define SYNC_RETURN(...) return __VA_ARGS__
#define SYNC_CALL(Fn, ...) Fn(__VA_ARGS__)
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

#define atom_load(x) atomic_load_explicit(x, memory_order_relaxed)
#define atom_store(x, y) atomic_store_explicit(x, y, memory_order_relaxed)
#define atom_add(x,y) atomic_fetch_add_explicit(x, y, memory_order_relaxed)
#define atom_sub(x,y) atomic_fetch_sub_explicit(x, y, memory_order_relaxed)
#define atom_or(x,y) atomic_fetch_or_explicit(x, y, memory_order_relaxed)
#define atom_inc(x) atomic_fetch_add_explicit(x, 1, memory_order_relaxed)
#define atom_dec(x) atomic_fetch_sub_explicit(x, 1, memory_order_relaxed)
#define atom_load_local atom_load
#define atom_store_local atom_store
#define atom_add_local atom_add
#define atom_sub_local atom_sub
#define atom_or_local atom_or
#define atom_inc_local atom_inc
#define atom_dec_local atom_dec

#define get_global_id(n) global_id[n]
#define get_global_size(n) global_size[n]
#define get_local_id(n) local_id[n]
#define get_local_size(n) local_size[n]

#define KERNEL_RETURN() return

#define ukl_vec_el(expr,el) (expr).el

#define ukl_popcount popcount
#define ukl_clz clz

using namespace metal;


typedef unsigned uint32_t;
typedef signed short int16_t;
typedef int int32_t;
typedef unsigned long uint64_t;
typedef long int64_t;
typedef signed char int8_t;
typedef unsigned char uint8_t;
typedef unsigned short uint16_t;
typedef metal::atomic_uint atomic_uint;
typedef metal::atomic_int atomic_int;

static inline int32_t reinterpretFloatAsInt(float val)
{
    return as_type<int32_t>(val);
}

static inline float reinterpretIntAsFloat(int32_t val)
{
    return as_type<float>(val);
}

static inline uint32_t ukl_simdgroup_ballot(bool val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    using namespace metal;
    return (simd_vote::vote_t)simd_ballot(val);
}

static inline uint32_t ukl_simdgroup_sum(uint32_t val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    using namespace metal;
    return simd_sum(val);
}

static inline uint32_t ukl_simdgroup_prefix_exclusive_sum_bools(bool val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    using namespace metal;
    return simd_prefix_exclusive_sum((uint16_t)val);
}

static inline uint32_t ukl_simdgroup_broadcast_first(uint32_t val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    using namespace metal;
    return simd_broadcast_first(val);
}

static inline uint32_t ukl_prefix_exclusive_sum_bitmask(uint32_t bits, uint16_t n)
{
    using namespace metal;
    return popcount(bits & ((1U << n)-1));
}

#define GEN_INNER_PARAMETER_RWBUFFER(Type) device Type *
#define GEN_INNER_PARAMETER_ROBUFFER(Type) device const Type *
#define GEN_INNER_PARAMETER_LITERAL(Type) Type
#define GEN_INNER_PARAMETER_RWLOCAL(Type) threadgroup Type *
#define GEN_INNER_PARAMETER_TUNEABLE(Type) Type
#define GEN_INNER_PARAMETER_GID0(Type) const Type
#define GEN_INNER_PARAMETER_GID1(Type) const Type
#define GEN_INNER_PARAMETER_GID2(Type) const Type
#define GEN_INNER_PARAMETER_GSZ0(Type) const Type
#define GEN_INNER_PARAMETER_GSZ1(Type) const Type
#define GEN_INNER_PARAMETER_GSZ2(Type) const Type
#define GEN_INNER_PARAMETER_LID0(Type) const Type
#define GEN_INNER_PARAMETER_LID1(Type) const Type
#define GEN_INNER_PARAMETER_LID2(Type) const Type
#define GEN_INNER_PARAMETER_LSZ0(Type) const Type
#define GEN_INNER_PARAMETER_LSZ1(Type) const Type
#define GEN_INNER_PARAMETER_LSZ2(Type) const Type

#define GEN_INNER_PARAMETER2(Access, Type, Name, Bounds) CALL2_A(GEN_INNER_PARAMETER_, Access, Type) Name

#define PASS_INNER_ARG_RWBUFFER(Type, Name) Name
#define PASS_INNER_ARG_ROBUFFER(Type, Name) Name
#define PASS_INNER_ARG_LITERAL(Type, Name) Name
#define PASS_INNER_ARG_RWLOCAL(Type, Name) Name
#define PASS_INNER_ARG_TUNEABLE(Type, Name) Name
#define PASS_INNER_ARG_GID0(Type, Name) __globalid.x
#define PASS_INNER_ARG_GID1(Type, Name) __globalid.y
#define PASS_INNER_ARG_GID2(Type, Name) __globalid.z
#define PASS_INNER_ARG_GSZ0(Type, Name) __globalsz.x
#define PASS_INNER_ARG_GSZ1(Type, Name) __globalsz.y
#define PASS_INNER_ARG_GSZ2(Type, Name) __globalsz.z
#define PASS_INNER_ARG_LID0(Type, Name) __localid.x
#define PASS_INNER_ARG_LID1(Type, Name) __localid.y
#define PASS_INNER_ARG_LID2(Type, Name) __localid.z
#define PASS_INNER_ARG_LSZ0(Type, Name) __localsz.x
#define PASS_INNER_ARG_LSZ1(Type, Name) __localsz.y
#define PASS_INNER_ARG_LSZ2(Type, Name) __localsz.z

#define PASS_INNER_ARG2(Access, Type, Name, Bounds) CALL2_A(PASS_INNER_ARG_, Access, Type, Name)

#define GEN_OUTER_PARAMETER_RWBUFFER(Type, Name) device Type * Name
#define GEN_OUTER_PARAMETER_ROBUFFER(Type, Name) device const Type * Name
#define GEN_OUTER_PARAMETER_LITERAL(Type, Name) Type Name
#define GEN_OUTER_PARAMETER_RWLOCAL(Type, Name) threadgroup Type * Name
#define GEN_OUTER_PARAMETER_TUNEABLE(Type, Name) Type Name
#define GEN_OUTER_PARAMETER_GID0(Type, Name)
#define GEN_OUTER_PARAMETER_GID1(Type, Name)
#define GEN_OUTER_PARAMETER_GID2(Type, Name)
#define GEN_OUTER_PARAMETER_GSZ0(Type, Name)
#define GEN_OUTER_PARAMETER_GSZ1(Type, Name)
#define GEN_OUTER_PARAMETER_GSZ2(Type, Name)
#define GEN_OUTER_PARAMETER_LID0(Type, Name)
#define GEN_OUTER_PARAMETER_LID1(Type, Name)
#define GEN_OUTER_PARAMETER_LID2(Type, Name)
#define GEN_OUTER_PARAMETER_LSZ0(Type, Name)
#define GEN_OUTER_PARAMETER_LSZ1(Type, Name)
#define GEN_OUTER_PARAMETER_LSZ2(Type, Name)

#define GEN_OUTER_PARAMETER2(Access, Type, Name, Bounds) CALL2_A(GEN_OUTER_PARAMETER_, Access, Type, Name)

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

#define __UKL_METAL_LAUNCH_ARGS \
    uint3 __globalid [[thread_position_in_grid]], \
    uint3 __globalsz [[threads_per_grid]], \
    uint3 __localid [[thread_position_in_threadgroup]], \
    uint3 __localsz [[threads_per_threadgroup]], \
    uint16_t __laneid [[thread_index_in_simdgroup]], \
    uint16_t __lanesz [[threads_per_simdgroup]]

#define DEFINE_KERNEL2(Library, Name, ...) \
    inline void Name ## _inner( __UKL_FOREACH_QUAD(GEN_INNER_PARAMETER2, __VA_ARGS__) ); \
    kernel void Name ( __UKL_FOREACH_QUAD(GEN_OUTER_PARAMETER2, __VA_ARGS__), __UKL_METAL_LAUNCH_ARGS ) \
    { \
            Name ## _inner ( __UKL_FOREACH_QUAD(PASS_INNER_ARG2, __VA_ARGS__)); \
    } \
    inline void Name ## _inner( __UKL_FOREACH_QUAD(GEN_INNER_PARAMETER2, __VA_ARGS__) ) \

///#define __UKL_COMMA_BETWEEN(a1, a2) __UKL_COUNT_ARGS(a1) __UKL_COUNT_ARGS(a2)

//#define __UKL_CALL3_0(Fn, N1, N2, ...) 

///#define __UKL_CALL_2ARGS_0(Fn, a1, a2) Fn(__UKL_SINGLE_ARG(a1), __UKL_SINGLE_ARG(a2))
///#define __UKL_CALL_2ARGS_1(Fn, a1, a2) Fn(__UKL_SINGLE_ARG(a1), __UKL_SINGLE_ARG(a2))

///#define __UKL_INSERT_COMMAS_SWITCH_00(a1, a2) a1, a2
///#define __UKL_INSERT_COMMAS_SWITCH_01(a1, a2) a1
///#define __UKL_INSERT_COMMAS_SWITCH_10(a1, a2) a2
///#define __UKL_INSERT_COMMAS_SWITCH_11(a1, a2) 

///#define __UKL_CALL3_2ARGS_0(Name1, Name2, Name3, a1, a2) __UKL_CALL_2ARGS_0(Name1##Name2##Name3, __UKL_SINGLE_ARG(a1), __UKL_SINGLE_ARG(a2))
///#define __UKL_CALL3_2ARGS_1(Name1, Name2, Name3, a1, a2) __UKL_CALL_2ARGS_1(Name1##Name2##Name3, __UKL_SINGLE_ARG(a1), __UKL_SINGLE_ARG(a2))

///#define __UKL_SINGLE_ARG(...) __VA_ARGS__

//#define __UKL_INSERT_COMMAS_2_0(a1, a2) __UKL_CALL3_0(__UKL_INSERT_COMMAS_SWITCH_0_,__UKL_COUNT_ARGS(a1),__UKL_COUNT_ARGS(a2),a1,a2)
///#define __UKL_INSERT_COMMAS_2_0(e1, e2, a1, a2) __UKL_CALL3_2ARGS_0(__UKL_INSERT_COMMAS_SWITCH_,e1,e2,__UKL_SINGLE_ARG(a1),__UKL_SINGLE_ARG(a2))
///#define __UKL_INSERT_COMMAS_2_1(e1, e2, a1, a2) __UKL_CALL3_2ARGS_1(__UKL_INSERT_COMMAS_SWITCH_,e1,e2,__UKL_SINGLE_ARG(a1),__UKL_SINGLE_ARG(a2))

//#define __UKL_INSERT_COMMAS_3(a1, a2, a3) __UKL_INSERT_COMMAS_2_0(__UKL_INSERT_COMMAS_2_1(__UKL_SINGLE_ARG(a1), __UKL_SINGLE_ARG(a2)), __UKL_SINGLE_ARG(a3))
//#define __UKL_INSERT_COMMAS_4(a1, a2, a3, a4) __UKL_INSERT_COMMAS_2_0(__UKL_INSERT_COMMAS_2_1(a1, a2), __UKL_INSERT_COMMAS_2_1(a3, a4))

///#define __UKL_INSERT_COMMAS_2_0_A(a1, a2) __UKL_INSERT_COMMAS_2_0(__UKL_IS_EMPTY(a1), __UKL_IS_EMPTY(a2), __UKL_SINGLE_ARG(a1), __UKL_SINGLE_ARG(a2))
///#define __UKL_INSERT_COMMAS_2_1_A(a1, a2) __UKL_INSERT_COMMAS_2_1(__UKL_IS_EMPTY(a1), __UKL_IS_EMPTY(a2), __UKL_SINGLE_ARG(a1), __UKL_SINGLE_ARG(a2))

//#define __UKL_INSERT_COMMAS_4(a1, a2, a3, a4) __UKL_INSERT_COMMAS_2_1_A(a1, a2), __UKL_INSERT_COMMAS_2_1_A(a3, a4)
///#define __UKL_INSERT_COMMAS_4(a1, a2, a3, a4) __UKL_INSERT_COMMAS_2_0_A(__UKL_INSERT_COMMAS_2_1_A(__UKL_SINGLE_ARG(a1), __UKL_SINGLE_ARG(a2)), __UKL_INSERT_COMMAS_2_1_A(__UKL_SINGLE_ARG(a3), __UKL_SINGLE_ARG(a4)))
