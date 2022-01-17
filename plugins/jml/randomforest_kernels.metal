/** randomforest_kernels.metal                                 -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    OpenCL kernels for random forest algorithm.

    clang-6.0 -Xclang -finclude-default-header -cl-std=CL1.2 -Dcl_clang_storage_class_specifiers -target nvptx64-nvidia-nvcl -xcl plugins/jml/randomforest_kernels.cl -emit-llvm -cl-kernel-arg-info -S -o test.ll
*/

#include <metal_atomic>
#include <metal_stdlib>

#define __constant constant
#define __kernel kernel
#define __global device
#define __local threadgroup
#define W W32
#define ukl_simdgroup_barrier()
#define ukl_threadgroup_barrier() threadgroup_barrier(mem_flags::mem_threadgroup)
#define printf(...) 
#define SYNC_FUNCTION(Version, Return, Name) Return Name
#define SYNC_RETURN(...) return __VA_ARGS__
#define SYNC_CALL(Fn, ...) Fn(__VA_ARGS__)

#define atom_load(x) atomic_load_explicit(x, memory_order_relaxed)
#define atom_store(x, y) atomic_store_explicit(x, y, memory_order_relaxed)
#define atom_add(x,y) atomic_fetch_add_explicit(x, y, memory_order_relaxed)
#define atom_sub(x,y) atomic_fetch_sub_explicit(x, y, memory_order_relaxed)
#define atom_or(x,y) atomic_fetch_or_explicit(x, y, memory_order_relaxed)
#define atom_inc(x) atomic_fetch_add_explicit(x, 1, memory_order_relaxed)
#define atom_dec(x) atomic_fetch_sub_explicit(x, 1, memory_order_relaxed)

#define get_global_id(n) global_id[n]
#define get_global_size(n) global_size[n]
#define get_local_id(n) local_id[n]
#define get_local_size(n) local_size[n]

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

static inline uint32_t simdgroup_ballot(bool val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    using namespace metal;
    return (simd_vote::vote_t)simd_ballot(val);
}

static inline uint32_t simdgroup_sum(uint32_t val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    using namespace metal;
    return simd_sum(val);
}

static inline uint32_t simdgroup_prefix_exclusive_sum_bools(bool val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    using namespace metal;
    return simd_prefix_exclusive_sum((uint16_t)val);
}

static inline uint32_t simdgroup_broadcast_first(uint32_t val, uint16_t simd_lane, __local atomic_uint * tmp)
{
    using namespace metal;
    return simd_broadcast_first(val);
}

static inline uint32_t prefix_exclusive_sum_bitmask(uint32_t bits, uint16_t n)
{
    using namespace metal;
    return popcount(bits & ((1U << n)-1));
}

#include "mldb/plugins/jml/randomforest_kernels_common.h"

// Take a bit-compressed representation of rows, and turn it into a
// decompressed version with one float per row (the sign gives the
// label, and the magnitude gives the weight).
//
// This is a 1D kernel over the array of rows, not very complicated

struct DecompressRowsKernelArgs {
    uint32_t rowDataLength;
    uint16_t weightBits;
    uint16_t exampleNumBits;
    uint32_t numRows;
    WeightFormat weightFormat;
    float weightMultiplier;
};

__kernel void
decompressRowsKernel(__constant DecompressRowsKernelArgs & args,
                     __global const uint64_t * rowData,
                     __global const float * weightData,                     
                     __global float * decodedRowsOut,
                     uint32_t id [[thread_position_in_grid]],
                     uint32_t n [[threads_per_grid]])
{
    uint32_t rowDataLength = args.rowDataLength;
    uint16_t weightBits = args.weightBits;
    uint16_t exampleNumBits = args.exampleNumBits;
    uint32_t numRows = args.numRows;
    WeightFormat weightFormat = args.weightFormat;
    float weightMultiplier = args.weightMultiplier;

    decompressRowsImpl(rowData, rowDataLength, weightBits, exampleNumBits, numRows,
                             weightFormat, weightMultiplier, weightData, decodedRowsOut,
                             id, n);
}


struct TestFeatureArgs {
    uint32_t numRows;
    uint32_t maxLocalBuckets;
};

__kernel void
testFeatureKernel(__constant const TestFeatureArgs & args,
                  __global const float * decodedRows,
 
                  __global const uint32_t * bucketData,
                  __global const uint32_t * bucketDataOffsets,
                  __global const uint32_t * bucketNumbers,
                  __global const uint32_t * bucketEntryBits,
 
                  __global const uint32_t * activeFeatureList,
 
                  __local W * w,
                  __global W * partitionBuckets,
 
                  uint2 global_id [[thread_position_in_grid]],
                  uint2 global_size [[threads_per_grid]],
                  uint2 local_id [[thread_position_in_threadgroup]],
                  uint2 local_size [[threads_per_threadgroup]])
{
    testFeatureImpl(decodedRows, args.numRows, bucketData, bucketDataOffsets, bucketNumbers,
                          bucketEntryBits, activeFeatureList, w, args.maxLocalBuckets, partitionBuckets,
                          get_global_id(0),
                          get_local_id(1), get_local_size(1), get_global_id(1), get_global_size(1));
}

PartitionSplit
chooseSplit(__global const W * w,
            uint32_t numBuckets,
            W wAll,
            __local WIndexed * wLocal,
            uint32_t wLocalSize,
            __local WIndexed * wStartBest,  // length 2
            bool ordinal,
            uint16_t threadIdx, uint16_t numThreads,
            uint16_t f,
            uint16_t p);


struct GetPartitionSplitsArgs {
    uint32_t wLocalSize;
};

// this expects [bucket, feature, partition]
__kernel void
getPartitionSplitsKernel(__constant const TreeTrainingInfo & treeTrainingInfo,
                         __constant const GetPartitionSplitsArgs & args,
                         __global const uint32_t * bucketNumbers, // [nf]
                         
                         __global const uint32_t * activeFeatureList, // [naf]
                         __global const uint32_t * featureIsOrdinal, // [nf]
                         
                         __global const W * buckets, // [np x totalBuckets]

                         __global const W * wAll,  // one per partition
                         __global PartitionSplit * featurePartitionSplitsOut, // [np x nf]

                         __global const TreeDepthInfo & treeDepthInfo,

                         __local WIndexed * wLocal,  // [wLocalSize]

                         uint3 global_id [[thread_position_in_grid]],
                         uint3 global_size [[threads_per_grid]],
                         uint3 local_id [[thread_position_in_threadgroup]],
                         uint3 local_size [[threads_per_threadgroup]])
{
    __local WIndexed wStartBest[2];
    getPartitionSplitsImpl(&treeTrainingInfo, bucketNumbers, activeFeatureList, featureIsOrdinal,
                           buckets, wAll, featurePartitionSplitsOut,
                           &treeDepthInfo, wLocal, args.wLocalSize, wStartBest,
                           get_global_id(1), get_global_size(1),
                           get_local_id(0), get_local_size(0),
                           get_global_id(2), get_global_size(2));
}

// id 0: partition number
__kernel void
bestPartitionSplitKernel(__constant const TreeTrainingInfo & treeTrainingInfo,
                         __global const TreeDepthInfo & treeDepthInfo,
                         __global const uint32_t * activeFeatureList, // [numActiveFeatures]
                         __global const PartitionSplit * featurePartitionSplits,
                         __global const PartitionIndex * partitionIndexes,
                         __global IndexedPartitionSplit * allPartitionSplitsOut,
                         uint2 global_id [[thread_position_in_grid]],
                         uint2 global_size [[threads_per_grid]],
                         uint2 local_id [[thread_position_in_threadgroup]],
                         uint2 local_size [[threads_per_threadgroup]])
{
    bestPartitionSplitImpl(&treeTrainingInfo, &treeDepthInfo, activeFeatureList,
                           featurePartitionSplits, partitionIndexes, allPartitionSplitsOut,
                           get_global_id(0), get_global_size(0));
}

// one instance
// SmallSideIndex[numPartitionsOut]:
// Used to a) know when we need to clear partitions (those on the small side are cleared,
// those on the big side are not) and b) provide a local index for partition buckets for
// the UpdateBuckets kernel
// - 0 means it's not on the small side
// - 1-254 means we're partition number (n-1) on the small side
// - 255 means we're partition number 254 or greater on the small side
__kernel void
assignPartitionNumbersKernel(__constant const TreeTrainingInfo & treeTrainingInfo,
                             __global TreeDepthInfo & treeDepthInfo,
                             __global const IndexedPartitionSplit * allPartitionSplits,
                             __global PartitionIndex * partitionIndexesOut,
                             __global PartitionInfo * partitionInfoOut,
                             __global uint8_t * smallSideIndexesOut,
                             __global uint16_t * smallSideIndexToPartitionOut,
                             ushort lane_id [[thread_index_in_simdgroup]],
                             ushort num_lanes [[threads_per_simdgroup]])
{
    __local AssignPartitionNumbersLocalState localState;
    assignPartitionNumbersImpl(&treeTrainingInfo, &treeDepthInfo, allPartitionSplits,
                               partitionIndexesOut, partitionInfoOut,
                               smallSideIndexesOut, smallSideIndexToPartitionOut,
                               &localState,
                               lane_id, num_lanes);
}

// [partition, bucket]
__kernel void
clearBucketsKernel(__constant const TreeTrainingInfo & treeTrainingInfo,
                   __global const TreeDepthInfo & treeDepthInfo,
                   __global W * bucketsOut,
                   __global W * wAllOut,
                   __global uint32_t * numNonZeroDirectionIndices,
                   __constant const uint8_t * smallSideIndexes,
                   uint2 global_id [[thread_position_in_grid]],
                   uint2 global_size [[threads_per_grid]],
                   uint2 local_id [[thread_position_in_threadgroup]],
                   uint2 local_size [[threads_per_threadgroup]])
{
    clearBucketsImpl(&treeTrainingInfo, &treeDepthInfo, bucketsOut, wAllOut, numNonZeroDirectionIndices,
                     smallSideIndexes, get_global_id(0), get_global_size(0), get_global_id(1));
}

// Second part: per feature plus wAll, transfer examples
//[rowNumber]
__kernel void
updatePartitionNumbersKernel(__constant const TreeTrainingInfo & treeTrainingInfo,
                             __global const TreeDepthInfo & treeDepthInfo,

                             __global RowPartitionInfo * partitions,
                             __global uint32_t * directions,
                             __global uint32_t * numNonZeroDirectionIndices,
                             __global UpdateWorkEntry * nonZeroDirectionIndices,
                             __global const uint8_t * smallSideIndexes,

                             __global const IndexedPartitionSplit * allPartitionSplits,
                             __global const PartitionInfo * partitionInfo,

                             __global const uint32_t * bucketData,
                             __constant const uint32_t * bucketDataOffsets,
                             __constant const uint32_t * bucketNumbers,
                             __constant const uint32_t * bucketEntryBits,
                             __constant const uint32_t * featureIsOrdinal,

                             __global const float * decodedRows,

                             uint2 global_id [[thread_position_in_grid]],
                             uint2 global_size [[threads_per_grid]],
                             ushort2 local_id [[thread_position_in_threadgroup]],
                             ushort2 local_size [[threads_per_threadgroup]])
{
    __local UpdatePartitionNumbersLocalState localState;
    updatePartitionNumbersImpl(&treeTrainingInfo, &treeDepthInfo, partitions, directions,
                               numNonZeroDirectionIndices, nonZeroDirectionIndices, smallSideIndexes,
                               allPartitionSplits, partitionInfo,
                               bucketData, bucketDataOffsets, bucketNumbers, bucketEntryBits,
                               featureIsOrdinal, decodedRows,
                               &localState,
                               get_global_id(0), get_global_size(0),
                               get_local_id(0), get_local_size(0));
}

struct UpdateBucketsArgs {
    uint32_t maxLocalBuckets;  
};

// [row, feature]
__kernel void
updateBucketsKernel(__constant const UpdateBucketsArgs & args,
                    __constant const TreeTrainingInfo & treeTrainingInfo,
                    __global const TreeDepthInfo & treeDepthInfo,
                    __global const RowPartitionInfo * partitions,
                    __global uint32_t * directions,
                    __global const uint32_t * numNonZeroDirectionIndices,
                    __global UpdateWorkEntry * nonZeroDirectionIndices,
                    __global W * buckets,
                    __global W * wAll,
                    __constant const uint8_t * smallSideIndexes,
                    __constant const uint16_t * smallSideIndexToPartition,

                    // Row data
                    __global const float * decodedRows,
                    
                    // Feature data
                    __global const uint32_t * bucketData,
                    __global const uint32_t * bucketDataOffsets,
                    __global const uint32_t * bucketNumbers,
                    __global const uint32_t * bucketEntryBits,

                    __global const uint32_t * activeFeatureList,
                    __global const uint32_t * featureIsOrdinal,

                    __local W * wLocal,

                    uint2 global_id [[thread_position_in_grid]],
                    uint2 global_size [[threads_per_grid]],
                    ushort2 local_id [[thread_position_in_threadgroup]],
                    ushort2 local_size [[threads_per_threadgroup]],
                    ushort lane_id [[thread_index_in_simdgroup]])
{
    __local UpdateBucketsLocalState localState;
    updateBucketsImpl(&treeTrainingInfo, &treeDepthInfo, partitions, directions,
                      numNonZeroDirectionIndices, nonZeroDirectionIndices, buckets, wAll,
                      smallSideIndexes, smallSideIndexToPartition,
                      decodedRows, bucketData, bucketDataOffsets, bucketNumbers, bucketEntryBits,
                      activeFeatureList, featureIsOrdinal,
                      wLocal, args.maxLocalBuckets, &localState,
                      get_global_id(1),
                      get_local_id(0), get_local_size(0), get_global_id(0), get_global_size(0));
}

// For each partition and each bucket, we up to now accumulated just the
// weight that needs to be transferred in the right hand side of the
// partition splits.  We need to fix this up by:
// 1.  Subtracting this weight from the left hand side
// 2.  Flipping the buckets where the big amount of weight belongs on the
//     right not on the left.
//
// This is a 2 dimensional kernel:
// Dimension 0 = partition number (from 0 to the old number of partitions)
// Dimension 1 = bucket number (from 0 to the number of active buckets); may be padded

__kernel void
fixupBucketsKernel(__constant const TreeTrainingInfo & treeTrainingInfo,
                   __global const TreeDepthInfo & treeDepthInfo,
                   __global W * buckets,
                   __global W * wAll,
                   __global const PartitionInfo * partitionInfo,
                   __global const uint8_t * smallSideIndexes,
                   uint2 global_id [[thread_position_in_grid]],
                   uint2 global_size [[threads_per_grid]],
                   uint2 local_id [[thread_position_in_threadgroup]],
                   uint2 local_size [[threads_per_threadgroup]])
{
    fixupBucketsImpl(&treeTrainingInfo, &treeDepthInfo, buckets, wAll, partitionInfo, smallSideIndexes,
                     get_global_id(1), get_global_id(0), get_global_size(0));
}
