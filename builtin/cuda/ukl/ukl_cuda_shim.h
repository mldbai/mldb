#pragma once
#include <assert.h>
#include "ukl_macros.h"
#include <string>
#include <tuple>
#include <functional>

#define __constant __constant__
#define __kernel __kernel__
#define __global __device__
#define __local __shared__
#define constexpr static __constant__

// TODO: should be possible to enable these on the device
// CUDA includes support for asserts
// Will need to make it switchable
#define ukl_assert(x)
#define ukl_assert_equal(x, y)
#define ukl_assert_not_equal(x, y)
#define ukl_assert_less(x, y)
#define ukl_assert_less_equal(x, y)
#define ukl_assert_greater(x, y)
#define ukl_assert_greater_equal(x, y)

#define ROBUFFER(Type) const Type *
#define RWBUFFER(Type) Type *
#define WOBUFFER(Type) Type *

#define RWLOCAL(Type) Type *

#define CONSTBUFFER(Type) const Type *

#define UKL_LOCAL_PTR(Type) Type *
#define UKL_LOCAL_PTR_CAST(Type) (Type *)
#define UKL_GLOBAL_PTR(Type) Type *
#define UKL_GLOBAL_PTR_CAST(Type) (Type *)

#define CAST_RWBUFFER(Buf, Type) reinterpret_cast<Type *>(Buf)
#define CAST_ROBUFFER(Buf, Type) reinterpret_cast<const Type *>(Buf)

#define CAST_RWLOCAL(Buf, Type) reinterpret_cast<Type __shared__ *>(Buf)
#define CAST_ROLOCAL(Buf, Type) reinterpret_cast<const Type __shared__ *>(Buf)

#define ukl_simdgroup_barrier()
#define ukl_threadgroup_barrier() __syncthreads()
#define printf(...)
#define UKL_FUNCTION(Version, Return, Name) __device__ Return Name
#define SYNC_FUNCTION(Version, Return, Name) __device__ Return Name
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

#define atom_load(x) (*x)
#define atom_store(x, y) ((*x) = (y))
#define atom_add(x,y) atomicAdd(x, y)
#define atom_sub(x,y) atomicSub(x, y)
#define atom_or(x,y) atomicOr(x, y)
#define atom_inc(x) atomicAdd(x,1)
#define atom_dec(x) atomicSub(x,1)
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

typedef unsigned uint32_t;
typedef signed short int16_t;
typedef int int32_t;
//typedef unsigned long uint64_t;
//typedef long int64_t;
typedef signed char int8_t;
typedef unsigned char uint8_t;
typedef unsigned short uint16_t;
typedef unsigned atomic_uint;
typedef int atomic_int;

#define ukl_vec_el(expr,el) (expr).el

#define ukl_popcount __popc
#define ukl_clz __clz

UKL_FUNCTION(v1,uint32_t,cudaCreateMask32)(uint32_t numBits)
{
    return numBits >= 32 ? -1 : (((uint32_t)1 << numBits) - 1);
}

UKL_FUNCTION(v1,uint32_t,extract_bits)(uint32_t val, uint16_t offset, uint32_t bits)
{
    return (val >> offset) & cudaCreateMask32(bits);
}

template<typename T, typename T2>
__device__ T as_type(const T2 & val)
{
    return *reinterpret_cast<const T *>(&val);
}

UKL_FUNCTION(v1,int32_t,reinterpretFloatAsInt)(float val)
{
    return as_type<int32_t>(val);
}

UKL_FUNCTION(v1,float,reinterpretIntAsFloat)(int32_t val)
{
    return as_type<float>(val);
}

UKL_FUNCTION(v1,uint32_t,ukl_simdgroup_ballot)(bool val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    return __ballot_sync(0xffffffff, val);
}

UKL_FUNCTION(v1,uint32_t,ukl_simdgroup_sum)(uint32_t val, uint16_t simd_lane, __local atomic_uint * tmp)
{
#if 0 // TODO SM80 and above
    return __reduce_add_sync(0xffffffff, val);
#else
    // TODO use warp shuffle to avoid need for temporary and atomics
    if (simd_lane == 0) {
        tmp[0] = 0;
    }

    if (val) {
        atom_add(tmp + 0, val);
    }

    return tmp[0];
#endif
}

UKL_FUNCTION(v1,uint32_t,ukl_prefix_exclusive_sum_bitmask)(uint32_t bits, uint16_t n)
{
    return ukl_popcount(bits & ((1U << n)-1));
}

UKL_FUNCTION(v1,uint32_t,ukl_simdgroup_prefix_exclusive_sum_bools)(bool val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    uint32_t ballots = ukl_simdgroup_ballot(val, simd_lane, tmp);
    return ukl_prefix_exclusive_sum_bitmask(ballots, simd_lane);
}

UKL_FUNCTION(v1,uint32_t,ukl_simdgroup_broadcast_first)(uint32_t val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    return __shfl_sync(0xffffffff, val, 0);
}

struct CudaGridKernelParameterInfo {
    const char * name;
    const char * kind;
    const char * type;
    const char * dims;
};

#define KERNEL_RETURN() return


#define GEN_INNER_PARAMETER_RWBUFFER(Type) Type *
#define GEN_INNER_PARAMETER_ROBUFFER(Type) const Type *
#define GEN_INNER_PARAMETER_LITERAL(Type) Type
#define GEN_INNER_PARAMETER_RWLOCAL(Type) Type *
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
#define PASS_INNER_ARG_GID0(Type, Name) (blockIdx.x * gridDim.x + threadIdx.x)
#define PASS_INNER_ARG_GID1(Type, Name) (blockIdx.y * gridDim.y + threadIdx.y)
#define PASS_INNER_ARG_GID2(Type, Name) (blockIdx.z * gridDim.z + threadIdx.z)
#define PASS_INNER_ARG_GSZ0(Type, Name) (blockDim.x * gridDim.x)
#define PASS_INNER_ARG_GSZ1(Type, Name) (blockDim.y * gridDim.y)
#define PASS_INNER_ARG_GSZ2(Type, Name) (blockDim.z * gridDim.z)
#define PASS_INNER_ARG_LID0(Type, Name) (threadIdx.x)
#define PASS_INNER_ARG_LID1(Type, Name) (threadIdx.y)
#define PASS_INNER_ARG_LID2(Type, Name) (threadIdx.z)
#define PASS_INNER_ARG_LSZ0(Type, Name) (blockDim.x)
#define PASS_INNER_ARG_LSZ1(Type, Name) (blockDim.y)
#define PASS_INNER_ARG_LSZ2(Type, Name) (blockDim.z)

#define PASS_INNER_ARG2(Access, Type, Name, Bounds) CALL2_A(PASS_INNER_ARG_, Access, Type, Name)

#define GEN_OUTER_PARAMETER_RWBUFFER(Type, Name) Type * Name
#define GEN_OUTER_PARAMETER_ROBUFFER(Type, Name) const Type * Name
#define GEN_OUTER_PARAMETER_LITERAL(Type, Name) Type Name
#define GEN_OUTER_PARAMETER_RWLOCAL(Type, Name) Type * Name
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

namespace MLDB {

struct CudaComputeFunction;

void registerCudaKernelImpl(const std::string & libraryName, const std::string & functionName,
                            std::function<std::shared_ptr<CudaComputeFunction> ()> generator);

template<typename... Args>
void registerCudaKernel(const std::string & libraryName, const std::string & functionName,
                        void (*launcher) (int, Args...),
                        const CudaGridKernelParameterInfo parameterInfo[sizeof...(Args)])
{
    auto initialize = [=] () -> std::shared_ptr<CPUComputeFunction>
    {
        auto argInfos = getArgumentInfos<0, std::tuple<Args...>, Args...>(functionName, parameterInfo);
        std::reverse(argInfos.begin(), argInfos.end());

        //HostComputeKernel kernel;
        //for (size_t i = 0;  i < argInfos.size();  ++i) {
        //    kernel.addParameter(argInfos[i].name, argInfos[i].type);
        //}
        //kernel.setGridComputeFunction(fn);

        //using namespace std;
        //cerr << jsonEncode(argInfos) << endl;

        auto createArgTuple = [] () -> std::any
        {
            return std::tuple<Args...>();
        };

        auto launch = [fn] (GridComputeQueue & queue, const std::string & opName, const std::any & argsAny,
                            const std::vector<size_t> & grid, const std::vector<size_t> & block,
                            size_t localMemBytesRequired, const std::map<std::string, std::tuple<size_t, size_t> > & localMemOffsets)
        {
            const auto & args = std::any_cast<const std::tuple<Args...> &>(argsAny);
            GridBounds gridBounds(grid);
            GridBounds blockBounds(block);

            HostComputeKernel::apply(launch, args, grid, block);
        };

        auto function = std::make_shared<CudaComputeFunction>();
        function->argumentInfo = std::move(argInfos);
        //function->kernel = std::move(kernel);

        function->createArgTuple = std::move(createArgTuple);
        function->launch = std::move(launch);

        return function;
    };

    registerCpuKernelImpl(libraryName, functionName, std::move(initialize));
}

} // namespace MLDB

#define DEFINE_KERNEL2(Library, Name, ...) \
    __device__ void Name ## _inner(int FOREACH_QUAD(GEN_INNER_PARAMETER2, __VA_ARGS__) ); \
    __global__ void Name ( int FOREACH_QUAD(GEN_OUTER_PARAMETER2, __VA_ARGS__) ) \
    { \
            Name ## _inner ( 0 FOREACH_QUAD(PASS_INNER_ARG2, __VA_ARGS__)); \
    } \
    void Name ## _launch() \
    { \
    \
    } \
    constexpr CudaGridKernelParameterInfo Name ## _ArgInfo[] = { {"","","",""} FOREACH_QUAD(GEN_ARG_INFO2, __VA_ARGS__) }; \
    namespace { \
        static struct InitKernel ## Library ## Name { \
            InitKernel ## Library ## Name () \
            { \
                registerCudaKernel(#Library, #Name, &Name ## _launch, Name ## _ArgInfo + 1); \
            } \
        } initKernel ## Library ## Name; \
    } \
    __device__ void Name ## _inner(int FOREACH_QUAD(GEN_INNER_PARAMETER2, __VA_ARGS__) )

