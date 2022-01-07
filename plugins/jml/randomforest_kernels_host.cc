/** randomforest_kernels.cc                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

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

#include "mldb/block/compute_kernel_host.h"



namespace MLDB {
namespace RF {

#define __constant
#define __global 
#define __kernel
#define __local
#define WBITS 32
#define W W32

using std::min;
using std::max;
using std::sqrt;
using std::popcount;

using uint2 = std::array<uint32_t, 2>;
using atomic_int = std::atomic<int32_t>;
using atomic_uint = std::atomic<uint32_t>;

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

#include "randomforest_kernels_common.h"

extern void decodeRowsKernelCpu(ComputeContext & context,
                         ComputeKernelGridRange & rowRange,
                         FrozenMemoryRegionT<uint64_t> rowData,
                         uint32_t rowDataLength,
                         uint16_t weightBits,
                         uint16_t exampleNumBits,
                         uint32_t numRows,
                         WeightFormat weightFormat,
                         float weightMultiplier,
                         FrozenMemoryRegionT<float> weightData,
                         std::span<float> decodedRowsOut);

void decodeRowsKernelCpu(ComputeContext & context,
                         ComputeKernelGridRange & rowRange,
                         std::span<const uint64_t> rowData,
                         uint32_t rowDataLength,
                         uint16_t weightBits,
                         uint16_t exampleNumBits,
                         uint32_t numRows,
                         WeightFormat weightFormat,
                         float weightMultiplier,
                         FrozenMemoryRegionT<float> weightData,
                         std::span<float> decodedRowsOut)
{
    decompressRowsImpl(rowData.data(), rowDataLength, weightBits, exampleNumBits, numRows,
                       weightFormat, weightMultiplier, weightData.data(), decodedRowsOut.data(),
                       0 /* id */, 1 /* n */);
}

extern void
testFeatureKernel(ComputeContext & context,
                  uint32_t fidx, uint32_t naf,
                  ComputeKernelGridRange & rows,

                  std::span<const float> decodedRows,
                  uint32_t numRows,

                  std::span<const uint32_t> allBucketData,
                  std::span<const uint32_t> bucketDataOffsets,
                  std::span<const uint32_t> bucketNumbers,
                  std::span<const uint32_t> bucketEntryBits,

                  std::span<const uint32_t> activeFeatureList,

                  std::span<W> allWOut);

void
testFeatureKernel(ComputeContext & context,
                  uint32_t fidx, uint32_t naf,
                  ComputeKernelGridRange & rows,

                  std::span<const float> decodedRows,
                  uint32_t numRows,

                  std::span<const uint32_t> allBucketData,
                  std::span<const uint32_t> bucketDataOffsets,
                  std::span<const uint32_t> bucketNumbers,
                  std::span<const uint32_t> bucketEntryBits,

                  std::span<const uint32_t> activeFeatureList,

                  std::span<W> allWOut)
{
    uint32_t maxLocalBuckets = 0;
    testFeatureImpl(decodedRows.data(), numRows, allBucketData.data(), bucketDataOffsets.data(), bucketNumbers.data(),
                    bucketEntryBits.data(), activeFeatureList.data(), nullptr, maxLocalBuckets,
                    allWOut.data(), fidx,
                    0, 1, 0, numRows);
}

#if 0
void
getPartitionSplitsKernel(ComputeContext & context,

                         uint32_t fidx, uint32_t naf,
                         
                         std::span<const TreeTrainingInfo> treeTrainingInfo, // [1]
                         std::span<const uint32_t> bucketNumbers, // [nf]
                         
                         std::span<const uint32_t> activeFeatureList, // [naf]
                         std::span<const uint32_t> featureIsOrdinal, // [nf]
                         
                         std::span<const W> buckets, // [np x totalBuckets]

                         std::span<const W> wAll,  // [np] one per partition
                         std::span<PartitionSplit> splitsOut, // [np x nf]
                         std::span<const TreeDepthInfo> treeDepthInfo) //[1]
{
}

void
assignPartitionNumbersKernel(ComputeContext & context,

                             std::span<const TreeTrainingInfo> treeTrainingInfo, // [1]
                             std::span<TreeDepthInfo> treeDepthInfo, //[1]

                             std::span<const IndexedPartitionSplit> partitionSplits,
                             std::span<PartitionIndex> partitionIndexesOut,
                             std::span<PartitionInfo> partitionInfoOut,
                             std::span<uint8_t> smallSideIndexesOut,
                             std::span<uint16_t> smallSideIndexToPartitionOut)
{
}

void
clearBucketsKernel(ComputeContext & context,

                   ComputeKernelGridRange & bucketRange,

                   std::span<const TreeTrainingInfo> treeTrainingInfo, // [1]
                   std::span<TreeDepthInfo> treeDepthInfo, //[1]

                   std::span<W> allPartitionBuckets,
                   std::span<W> wAll,
                   std::span<uint32_t> numNonZeroDirectionIndices,
                   std::span<const uint8_t> smallSideIndexes)
{
}

void
updatePartitionNumbersKernel(ComputeContext & context,
                             ComputeKernelGridRange & rowRange,
          
                             std::span<const TreeTrainingInfo> treeTrainingInfo, // [1]
                             std::span<TreeDepthInfo> treeDepthInfo, //[1]
                             
                             std::span<RowPartitionInfo> partitions,
                             std::span<uint32_t> /* directions */,
                             std::span<uint32_t> numNonZeroDirectionIndices,
                             std::span<UpdateWorkEntry> nonZeroDirectionIndices,
                             std::span<uint8_t> smallSideIndexes,
          
                             std::span<const IndexedPartitionSplit> partitionSplits,
                             std::span<const PartitionInfo> partitionInfo,
                             
                             // Feature data
                             std::span<const uint32_t> allBucketData,
                             std::span<const uint32_t> bucketDataOffsets,
                             std::span<const uint32_t> bucketNumbers,
                             std::span<const uint32_t> bucketEntryBits,
          
                             std::span<const uint32_t> featureIsOrdinal,
                             std::span<const float> decodedRows)
{
}

void
updateBucketsKernel(ComputeContext & context,
                    ComputeKernelGridRange & rowRange,
                    uint32_t fidxp1, uint32_t nafp1,

                    std::span<const TreeTrainingInfo> treeTrainingInfo, // [1]
                    std::span<TreeDepthInfo> treeDepthInfo, //[1]

                    std::span<const RowPartitionInfo> partitions,
                    std::span<const uint32_t> /* directions */,
                    std::span<const uint32_t> numNonZeroDirectionIndices, //[1]
                    std::span<UpdateWorkEntry> nonZeroDirectionIndices,

                    std::span<W> partitionBuckets,
                    std::span<W> wAll,
                    std::span<uint8_t> smallSideIndexes,
                    std::span<uint16_t> smallSideIndexToPartition,

                    // Row data
                    std::span<const float> decodedRows,
                    
                    // Feature data
                    std::span<const uint32_t> allBucketData,
                    std::span<const uint32_t> bucketDataOffsets,
                    std::span<const uint32_t> bucketNumbers,
                    std::span<const uint32_t> bucketEntryBits,

                    std::span<const uint32_t> featureActive,
                    std::span<const uint32_t> featureIsOrdinal)
{
}

void
fixupBucketsKernel(ComputeContext & context,
                   ComputeKernelGridRange & bucketRange,

                   std::span<const TreeTrainingInfo> treeTrainingInfo, // [1]
                   std::span<TreeDepthInfo> treeDepthInfo, //[1]

                   std::span<W> allPartitionBuckets,
                   std::span<W> wAll,
                   std::span<const PartitionInfo> partitionInfo,
                   std::span<const uint8_t> smallSideIndexes)
{
}

#endif

} // namespace RF
} // namespace MLDB
