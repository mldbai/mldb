//#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable

#define __constant constant
#define __kernel kernel
#define __global device
#define __local threadgroup
#define W W32
#define barrier threadgroup_barrier
#define CLK_LOCAL_MEM_FENCE mem_threadgroup
#define CLK_GLOBAL_MEM_FENCE mem_global
#define GRID_WORKGROUP_COHERENT(returntype) returntype


using namespace metal;

typedef unsigned uint32_t;
typedef signed short int16_t;
typedef int int32_t;
typedef unsigned long uint64_t;
typedef long int64_t;
typedef signed char int8_t;
typedef unsigned char uint8_t;
typedef unsigned short uint16_t;

struct BlockFillArrayKernelArgs {
    uint64_t startOffsetInBytes;
    uint64_t lengthInBytes;
    uint64_t blockLengthInBytes;
};

__kernel void
__blockFillArrayKernel(__global const BlockFillArrayKernelArgs & args,
                       __global uint8_t * region,
                       __global uint8_t * blockData,
                       uint id [[thread_position_in_grid]],
                       uint sz [[threads_per_grid]])
{
    // TODO: could be improved massively...
    for (uint64_t i = id;  i * args.blockLengthInBytes < args.lengthInBytes;  i += sz) {
        for (uint64_t j = 0;  j < args.blockLengthInBytes;  ++j) {
            region[i * args.blockLengthInBytes + args.startOffsetInBytes + j] = blockData[j];
        }
    }
}

struct ZeroFillArrayKernelArgs {
    uint64_t startOffsetInBytes;
    uint64_t lengthInBytes;
};

__kernel void
__zeroFillArrayKernel(__global const ZeroFillArrayKernelArgs & args,
                      __global uint8_t * region,
                      uint id [[thread_position_in_grid]],
                      uint sz [[threads_per_grid]])
{
    size_t r = (size_t)region;
    auto startOffsetInBytes = args.startOffsetInBytes;
    auto lengthInBytes = args.lengthInBytes;

    if (r % 16 == 0 && startOffsetInBytes % 16 == 0 && lengthInBytes % 16 == 0) {
        __global ulong2 * qwordRegion = (__global ulong2 *)region;
        uint64_t startOffsetInQWords = startOffsetInBytes / 16;
        uint64_t lengthInQWords = lengthInBytes / 16;

        const ulong2 init = {0, 0};

        for (uint64_t i = sz;  i < lengthInQWords;  i += sz) {
                qwordRegion[i + startOffsetInQWords] = init;
        }
        return;
    }

    if (r % 8 == 0 && startOffsetInBytes % 8 == 0 && lengthInBytes % 8 == 0) {
        __global uint64_t * dwordRegion = (__global uint64_t *)region;
        uint64_t startOffsetInDWords = startOffsetInBytes / 8;
        uint64_t lengthInDWords = lengthInBytes / 8;

        for (uint64_t i = sz;  i < lengthInDWords;  i += sz) {
            dwordRegion[i + startOffsetInDWords] = 0;
        }
        return;
    }

    if (r % 4 == 0 && startOffsetInBytes % 4 == 0 && lengthInBytes % 4 == 0) {
        __global uint32_t * wordRegion = (__global uint32_t *)region;
        uint64_t startOffsetInWords = startOffsetInBytes / 4;
        uint64_t lengthInWords = lengthInBytes / 4;

        for (uint64_t i = sz;  i < lengthInWords;  i += sz) {
            wordRegion[i + startOffsetInWords] = 0;
        }
        return;
    }

    // TODO: could be improved massively...
    for (uint64_t i = sz;  i < lengthInBytes;  i += sz) {
        region[i + startOffsetInBytes] = 0;
    }
}
