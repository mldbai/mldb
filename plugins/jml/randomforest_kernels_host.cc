/** randomforest_kernels.cc                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#include "mldb/block/compute_kernel_cpu_shims.h"

namespace MLDB {
namespace RF {

#define W W32

#include "randomforest_kernels_common.h"

DEFINE_KERNEL2(randomforest_kernels,
              decompressRowsKernel,
              BUFFER,   const uint64_t,                           rowData,        "[rowDataLength]",
              LITERAL,  uint32_t,                                 rowDataLength,,
              LITERAL,  uint16_t,                                 weightBits,,
              LITERAL,  uint16_t,                                 exampleNumBits,,
              LITERAL,  uint32_t,                                 numRows,,
              LITERAL,  WeightFormat,                             weightFormat,,
              LITERAL,  float,                                    weightMultiplier,,
              BUFFER,   const float,                              weightData,     "[weightDataLength]",
              BUFFER,   float,                                    decodedRowsOut, "[numRows]",
              GID0,     uint32_t,                                 id,,
              GSZ0,     uint32_t,                                 n,)
{
    using namespace std;
    //cerr << "id = " << id << " n = " << n << endl;
    decompressRowsImpl(rowData.data(), rowDataLength, weightBits, exampleNumBits, numRows,
                       weightFormat, weightMultiplier, weightData.data(), decodedRowsOut.data(),
                       id, n);
}

DEFINE_KERNEL2(randomforest_kernels,
              testFeatureKernel,
              LITERAL,  uint16_t,                                 numActiveFeatures,,
              BUFFER,   const float,                              decodedRows,          "[numRows]",
              LITERAL,  uint32_t,                                 numRows,,
              BUFFER,   const uint32_t,                           bucketData,           "[bucketDataLength]",
              BUFFER,   const uint32_t,                           bucketDataOffsets,    "[numFeatures + 1]",
              BUFFER,   const uint32_t,                           bucketNumbers,        "[numFeatures + 1]",
              BUFFER,   const uint32_t,                           bucketEntryBits,      "[numFeatures]",
              BUFFER,   const uint32_t,                           activeFeatureList,    "[numActiveFeatures]",
              BUFFER,   W,                                        partitionBuckets,     "[numBuckets]",
              LOCAL,    W,                                        w,                    "[maxLocalBuckets]",
              TUNEABLE, uint16_t,                                 maxLocalBuckets,      "getenv('RF_LOCAL_BUCKET_MEM',5500) / sizeof('W')",
              GID0,     uint16_t,                                 fidx,,
              LID1,     uint16_t,                                 threadGroupId,,
              LSZ1,     uint16_t,                                 threadGroupSize,,
              GID1,     uint16_t,                                 gridId,,
              GSZ1,     uint16_t,                                 gridSize,)
{
    testFeatureImpl(decodedRows.data(), numRows, bucketData.data(), bucketDataOffsets.data(), bucketNumbers.data(),
                    bucketEntryBits.data(), activeFeatureList.data(), w, maxLocalBuckets,
                    partitionBuckets.data(), fidx, threadGroupId, threadGroupSize, gridId, gridSize);
}

DEFINE_KERNEL2(randomforest_kernels,
               getPartitionSplitsKernel,
               BUFFER,      const TreeTrainingInfo,     treeTrainingInfo,           "[1]",
               BUFFER,      const uint32_t,             bucketNumbers,              "[numFeatures + 1]",
               BUFFER,      const uint32_t,             activeFeatureList,          "[numActiveFeatures]",
               BUFFER,      const uint32_t,             featureIsOrdinal,           "[numFeatures]",
               BUFFER,      const W,                    buckets,                    "[numActiveBuckets * numActivePartitions]",
               BUFFER,      const W,                    wAll,                       "[numActivePartitions]",
               BUFFER,      PartitionSplit,             featurePartitionSplitsOut,  "[numActivePartitions * numActiveFeatures]",
               BUFFER,      const TreeDepthInfo,        treeDepthInfo,              "[1]",
               LOCAL,       WIndexed,                   wLocal,                     "[wLocalSize]",
               TUNEABLE,    uint16_t,                   wLocalSize,                 "getenv('RF_LOCAL_BUCKET_MEM',5500) / sizeof('WIndexed')",

               GID1,        uint16_t,                   featureId,,
               GSZ1,        uint16_t,                   numActiveFeatures,,
               LID0,        uint16_t,                   workerId,,
               LSZ0,        uint16_t,                   workGroupSize,,
               GID2,        uint16_t,                   partitionWorkerId,,
               GSZ2,        uint16_t,                   partitionWorkerSize,)
{
    __local WIndexed wStartBest[2];
    getPartitionSplitsImpl(treeTrainingInfo.data(), bucketNumbers.data(),
                           activeFeatureList.data(), featureIsOrdinal.data(),
                           buckets.data(), wAll.data(), featurePartitionSplitsOut.data(),
                           treeDepthInfo.data(), wLocal, wLocalSize, wStartBest,
                           featureId, numActiveFeatures,
                           workerId, workGroupSize,
                           partitionWorkerId, partitionWorkerId);
}

DEFINE_KERNEL2(randomforest_kernels,
               bestPartitionSplitKernel,
               BUFFER,      const TreeTrainingInfo,         treeTrainingInfo,           "[1]",     
               BUFFER,      const TreeDepthInfo,            treeDepthInfo,              "[1]",     
               BUFFER,      const uint32_t,                 activeFeatureList,          "[numActiveFeatures]",     
               BUFFER,      const PartitionSplit,           featurePartitionSplits,     "[numActivePartitions * numActiveFeatures]",     
               BUFFER,      const PartitionIndex,           partitionIndexes,           "[numActivePartitions]",     
               BUFFER,      IndexedPartitionSplit,          allPartitionSplitsOut,      "[numActivePartitions]",
               GID0,        uint16_t,                       workerId,,
               GSZ0,        uint16_t,                       numWorkers,)
{
    bestPartitionSplitImpl(treeTrainingInfo.data(), treeDepthInfo.data(), activeFeatureList.data(),
                           featurePartitionSplits.data(), partitionIndexes.data(), allPartitionSplitsOut.data(),
                           workerId, numWorkers);
}

DEFINE_KERNEL(randomforest_kernels,
              assignPartitionNumbersKernel,
              std::span<const TreeTrainingInfo>,         treeTrainingInfo,
              std::span<TreeDepthInfo>,                  treeDepthInfo,
              std::span<const IndexedPartitionSplit>,    allPartitionSplits,
              std::span<PartitionIndex>,                 partitionIndexesOut,
              std::span<PartitionInfo>,                  partitionInfoOut,
              std::span<uint8_t>,                        smallSideIndexesOut,
              std::span<uint16_t>,                       smallSideIndexToPartitionOut)
{
    MLDB_THROW_UNIMPLEMENTED();
}

DEFINE_KERNEL(randomforest_kernels,
              clearBucketsKernel,
              std::span<const TreeTrainingInfo>,       treeTrainingInfo,
              std::span<TreeDepthInfo>,                treeDepthInfo,
              std::span<W>,                            bucketsOut,
              std::span<W>,                            wAllOut,
              std::span<uint32_t>,                     numNonZeroDirectionIndices,
              std::span<const uint8_t>,                smallSideIndexes)
{
    MLDB_THROW_UNIMPLEMENTED();
}

DEFINE_KERNEL(randomforest_kernels,
              updatePartitionNumbersKernel,
              std::span<const TreeTrainingInfo>,        treeTrainingInfo,
              std::span<TreeDepthInfo>,                 treeDepthInfo,
              std::span<RowPartitionInfo>,              partitions,
              std::span<uint32_t>,                      directions,
              std::span<uint32_t>,                      numNonZeroDirectionIndices,
              std::span<UpdateWorkEntry>,               nonZeroDirectionIndices,
              std::span<uint8_t>,                       smallSideIndexes,
              std::span<const IndexedPartitionSplit>,   allPartitionSplits,
              std::span<const PartitionInfo>,           partitionInfo,
              std::span<const uint32_t>,                bucketData,
              std::span<const uint32_t>,                bucketDataOffsets,
              std::span<const uint32_t>,                bucketNumbers,
              std::span<const uint32_t>,                bucketEntryBits,
              std::span<const uint32_t>,                featureIsOrdinal,
              std::span<const float>,                   decodedRows)
{
    MLDB_THROW_UNIMPLEMENTED();
}

DEFINE_KERNEL(randomforest_kernels,
              updateBucketsKernel,
              uint32_t,                                 fidxp1,
              uint32_t,                                 nafp1,
              std::span<const TreeTrainingInfo>,        treeTrainingInfo,
              std::span<TreeDepthInfo>,                 treeDepthInfo,
              std::span<const RowPartitionInfo>,        partitions,
              std::span<const uint32_t>,                directions,
              std::span<const uint32_t>,                numNonZeroDirectionIndices,
              std::span<UpdateWorkEntry>,               nonZeroDirectionIndices,
              std::span<W>,                             buckets,
              std::span<W>,                             wAll,
              std::span<uint8_t>,                       smallSideIndexes,
              std::span<uint16_t>,                      smallSideIndexToPartition,
              std::span<const float>,                   decodedRows,
              std::span<const uint32_t>,                bucketData,
              std::span<const uint32_t>,                bucketDataOffsets,
              std::span<const uint32_t>,                bucketNumbers,
              std::span<const uint32_t>,                bucketEntryBits,
              std::span<const uint32_t>,                activeFeatureList,
              std::span<const uint32_t>,                featureIsOrdinal)
{
    MLDB_THROW_UNIMPLEMENTED();
}

DEFINE_KERNEL(randomforest_kernels,
              fixupBucketsKernel,
              std::span<const TreeTrainingInfo>,        treeTrainingInfo,
              std::span<TreeDepthInfo>,                 treeDepthInfo,
              std::span<W>,                             buckets,
              std::span<W>,                             wAll,
              std::span<const PartitionInfo>,           partitionInfo,
              std::span<const uint8_t>,                 smallSideIndexes)
{
    MLDB_THROW_UNIMPLEMENTED();
}

} // namespace RF
} // namespace MLDB
