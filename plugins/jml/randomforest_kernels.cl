/** randomforest_kernels.cl                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    OpenCL kernels for random forest algorithm.

    clang-6.0 -Xclang -finclude-default-header -cl-std=CL1.2 -Dcl_clang_storage_class_specifiers -target nvptx64-nvidia-nvcl -xcl plugins/jml/randomforest_kernels.cl -emit-llvm -cl-kernel-arg-info -S -o test.ll
*/


#define WBITS 32
#define W W32

#define constexpr const

typedef unsigned uint32_t;
typedef signed short int16_t;
typedef int int32_t;
typedef unsigned long uint64_t;
typedef long int64_t;
typedef signed char int8_t;
typedef unsigned char uint8_t;
typedef unsigned short uint16_t;
typedef int32_t atomic_int;
typedef uint32_t atomic_uint;

#define ukl_simdgroup_barrier()
#define ukl_threadgroup_barrier() barrier(CLK_LOCAL_MEM_FENCE)
#define atom_load(addr) (*(addr))
#define atom_store(addr, val) ((*(addr)) = val)

#define SYNC_FUNCTION(Version, Return, Name) Return Name
#define SYNC_RETURN(Val) return Val;
#define SYNC_CALL_0(Fn) Fn()
#define SYNC_CALL_1(Fn, a1) Fn(a1)
#define SYNC_CALL_2(Fn, a1, a2) Fn(a1, a2)
#define SYNC_CALL_3(Fn, a1, a2, a3) Fn(a1, a2, a3)
#define SYNC_CALL_4(Fn, a1, a2, a3, a4) Fn(a1, a2, a3, a4)
#define SYNC_CALL_5(Fn, a1, a2, a3, a4, a5) Fn(a1, a2, a3, a4, a5)
#define SYNC_CALL_6(Fn, a1, a2, a3, a4, a5, a6) Fn(a1, a2, a3, a4, a5, a6)
#define SYNC_CALL_7(Fn, a1, a2, a3, a4, a5, a6, a7) Fn(a1, a2, a3, a4, a5, a6, a7)
#define SYNC_CALL_8(Fn, a1, a2, a3, a4, a5, a6, a7, a8) Fn(a1, a2, a3, a4, a5, a6, a7, a8)
#define SYNC_CALL_9(Fn, a1, a2, a3, a4, a5, a6, a7, a8, a9) Fn(a1, a2, a3, a4, a5, a6, a7, a8, a9)
#define SYNC_CALL_10(Fn, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10) Fn(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)
#define SYNC_CALL_11(Fn, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11) Fn(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11)
#define SYNC_CALL_12(Fn, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12) Fn(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12)
#define assert(x)

inline uint32_t createMask32(uint32_t numBits);

static inline uint32_t extract_bits(uint32_t val, uint16_t offset, uint32_t bits)
{
    return (val >> offset) & createMask32(bits);
}

static inline int32_t reinterpretFloatAsInt(float val)
{
    return *(int32_t *)(&val);
}

static inline float reinterpretIntAsFloat(int32_t val)
{
    return *(float *)(&val);
}

static inline uint32_t simdgroup_ballot(bool val, uint16_t simd_lane, __local uint32_t * tmp)
{
    if (simd_lane == 0) {
        tmp[0] = 0;
    }

    if (val) {
        atom_or(tmp + 0, (uint32_t)1 << simd_lane);
    }

    return tmp[0];
}

static inline uint32_t simdgroup_sum(uint32_t val, uint16_t simd_lane, __local uint32_t * tmp)
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

static inline uint32_t simdgroup_prefix_exclusive_sum_bools(bool val, uint16_t simd_lane, __local uint32_t * tmp)
{
    uint32_t ballots = simdgroup_ballot(val, simd_lane, tmp);
    return prefix_exclusive_sum_bitmask(ballots, simd_lane);
}

static inline uint32_t simdgroup_broadcast_first(uint32_t val, uint16_t simd_lane, __local uint32_t * tmp)
{
    if (simd_lane == 0) {
        tmp[0] = val;
    }

    return tmp[0];
}

#include "mldb/plugins/jml/randomforest_kernels_common.h"

__kernel void
decompressRowsKernel(__global const uint64_t * rowData,
                     uint32_t rowDataLength,
                     uint16_t weightBits,
                     uint16_t exampleNumBits,
                     uint32_t numRows,
                     
                     WeightFormat weightFormat,
                     float weightMultiplier,
                     __global const float * weightData,
                     
                     __global float * decodedRowsOut)
{
    decompressRowsImpl(rowData, rowDataLength, weightBits, exampleNumBits, numRows,
                       weightFormat, weightMultiplier, weightData, decodedRowsOut,
                       get_global_id(0), get_global_size(0));
}

__kernel void
testFeatureKernel(__global const float * decodedRows,
                 uint32_t numRows,

                 __global const uint32_t * bucketData,
                 __global const uint32_t * bucketDataOffsets,
                 __global const uint32_t * bucketNumbers,
                 __global const uint32_t * bucketEntryBits,

                 __global const uint32_t * activeFeatureList,
                      
                 __local W * w,
                 uint32_t maxLocalBuckets,
                 __global W * partitionBuckets)
{
    testFeatureImpl(decodedRows, numRows, bucketData, bucketDataOffsets, bucketNumbers,
                    bucketEntryBits, activeFeatureList, w, maxLocalBuckets, partitionBuckets,
                    get_global_id(0),
                    get_local_id(1), get_local_size(1), get_global_id(1), get_global_size(1));
}

// this expects [bucket, feature, partition]
__kernel void
getPartitionSplitsKernel(__constant const TreeTrainingInfo * treeTrainingInfo,
                         __global const uint32_t * bucketNumbers, // [nf]
                         
                         __global const uint32_t * activeFeatureList, // [naf]
                         __global const uint32_t * featureIsOrdinal, // [nf]
                         
                         __global const W * buckets, // [np x totalBuckets]

                         __global const W * wAll,  // one per partition
                         __global PartitionSplit * featurePartitionSplitsOut, // [np x nf]

                         __global const TreeDepthInfo * treeDepthInfo,

                         __local WIndexed * wLocal,
                         uint32_t wLocalSize)  // [wLocalSize]
{
    __local WIndexed wStartBest[2];
    getPartitionSplitsImpl(treeTrainingInfo, bucketNumbers, activeFeatureList, featureIsOrdinal,
                           buckets, wAll, featurePartitionSplitsOut,
                           treeDepthInfo, wLocal, wLocalSize, wStartBest,
                           get_global_id(1), get_global_size(1),
                           get_local_id(0), get_local_size(0),
                           get_global_id(2), get_global_size(2));
}

__kernel void
bestPartitionSplitKernel(__constant const TreeTrainingInfo * treeTrainingInfo,
                         __global const TreeDepthInfo * treeDepthInfo,
                         __global const uint32_t * activeFeatureList, // [numActiveFeatures]
                         __global const PartitionSplit * featurePartitionSplits,
                         __global const PartitionIndex * partitionIndexes,
                         __global IndexedPartitionSplit * allPartitionSplitsOut)
{
    bestPartitionSplitImpl(treeTrainingInfo, treeDepthInfo, activeFeatureList,
                           featurePartitionSplits, partitionIndexes, allPartitionSplitsOut,
                           get_global_id(0), get_global_size(0));
}

__kernel void
assignPartitionNumbersKernel(__constant const TreeTrainingInfo * treeTrainingInfo,
                             __global TreeDepthInfo * treeDepthInfo,
                             __global const IndexedPartitionSplit * allPartitionSplits,
                             __global PartitionIndex * partitionIndexesOut,
                             __global PartitionInfo * partitionInfoOut,
                             __global uint8_t * smallSideIndexesOut,
                             __global uint16_t * smallSideIndexToPartitionOut)
{
    __local AssignPartitionNumbersLocalState localState;
    assignPartitionNumbersImpl(treeTrainingInfo, treeDepthInfo, allPartitionSplits,
                               partitionIndexesOut, partitionInfoOut,
                               smallSideIndexesOut, smallSideIndexToPartitionOut,
                               &localState, get_local_id(0), get_local_size(0));
}

// [partition, bucket]
__kernel void
clearBucketsKernel(__constant const TreeTrainingInfo * treeTrainingInfo,
                   __global const TreeDepthInfo * treeDepthInfo,
                   __global W * bucketsOut,
                   __global W * wAllOut,
                   __global uint32_t * numNonZeroDirectionIndices,
                   __constant const uint8_t * smallSideIndexes)
{
    clearBucketsImpl(treeTrainingInfo, treeDepthInfo, bucketsOut, wAllOut, numNonZeroDirectionIndices,
                     smallSideIndexes, get_global_id(0), get_global_size(0), get_global_id(1));
}

// [row]
__kernel void
updatePartitionNumbersKernel(__constant const TreeTrainingInfo * treeTrainingInfo,
                             __global const TreeDepthInfo * treeDepthInfo,

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

                             __global const float * decodedRows)
{
    __local UpdatePartitionNumbersLocalState localState;
    updatePartitionNumbersImpl(treeTrainingInfo, treeDepthInfo, partitions, directions,
                               numNonZeroDirectionIndices, nonZeroDirectionIndices, smallSideIndexes,
                               allPartitionSplits, partitionInfo,
                               bucketData, bucketDataOffsets, bucketNumbers, bucketEntryBits,
                               featureIsOrdinal, decodedRows,
                               &localState,
                               get_global_id(0), get_global_size(0),
                               get_local_id(0), get_local_size(0));
}

// [row, feature]
__kernel void
updateBucketsKernel(__constant const TreeTrainingInfo * treeTrainingInfo,
                    __global const TreeDepthInfo * treeDepthInfo,
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
                    uint16_t maxLocalBuckets)
{
    __local UpdateBucketsLocalState localState;
    updateBucketsImpl(treeTrainingInfo, treeDepthInfo, partitions, directions,
                      numNonZeroDirectionIndices, nonZeroDirectionIndices, buckets, wAll,
                      smallSideIndexes, smallSideIndexToPartition,
                      decodedRows, bucketData, bucketDataOffsets, bucketNumbers, bucketEntryBits,
                      activeFeatureList, featureIsOrdinal,
                      wLocal, maxLocalBuckets, &localState,
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
fixupBucketsKernel(__constant const TreeTrainingInfo * treeTrainingInfo,
                   __global const TreeDepthInfo * treeDepthInfo,
                   __global W * buckets,
                   __global W * wAll,
                   __global const PartitionInfo * partitionInfo,
                   __global const uint8_t * smallSideIndexes)
{
    fixupBucketsImpl(treeTrainingInfo, treeDepthInfo, buckets, wAll, partitionInfo, smallSideIndexes,
                     get_global_id(1), get_global_id(0), get_global_size(0));
}
