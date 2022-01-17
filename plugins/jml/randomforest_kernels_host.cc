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
    KERNEL_RETURN();
}

DEFINE_KERNEL2(randomforest_kernels,
              testFeatureKernel,
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
              GSZ0,     uint16_t,                                 numActiveFeatures,,
              LID1,     uint16_t,                                 threadGroupId,,
              LSZ1,     uint16_t,                                 threadGroupSize,,
              GID1,     uint16_t,                                 gridId,,
              GSZ1,     uint16_t,                                 gridSize,)
{
    SYNC_CALL(testFeatureImpl,
              decodedRows.data(), numRows, bucketData.data(), bucketDataOffsets.data(), bucketNumbers.data(),
              bucketEntryBits.data(), activeFeatureList.data(), w, maxLocalBuckets,
              partitionBuckets.data(), fidx, threadGroupId, threadGroupSize, gridId, gridSize);
    KERNEL_RETURN();
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
               LOCAL,       WIndexed,                   wStartBest,                 "[2]",

               GID1,        uint16_t,                   featureId,,
               GSZ1,        uint16_t,                   numActiveFeatures,,
               LID0,        uint16_t,                   workerId,,
               LSZ0,        uint16_t,                   workGroupSize,,
               GID2,        uint16_t,                   partitionWorkerId,,
               GSZ2,        uint16_t,                   partitionWorkerSize,)
{
    SYNC_CALL(getPartitionSplitsImpl,
              treeTrainingInfo.data(), bucketNumbers.data(),
              activeFeatureList.data(), featureIsOrdinal.data(),
              buckets.data(), wAll.data(), featurePartitionSplitsOut.data(),
              treeDepthInfo.data(), wLocal, wLocalSize, wStartBest,
              featureId, numActiveFeatures,
              workerId, workGroupSize,
              partitionWorkerId, partitionWorkerSize);
    KERNEL_RETURN();
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
    KERNEL_RETURN();
}

DEFINE_KERNEL2(randomforest_kernels,
               assignPartitionNumbersKernel,
               BUFFER,      const TreeTrainingInfo,         treeTrainingInfo,               "[1]",
               BUFFER,      TreeDepthInfo,                  treeDepthInfo,                  "[1]",
               BUFFER,      const IndexedPartitionSplit,    allPartitionSplits,             "[numActivePartitions]",
               BUFFER,      PartitionIndex,                 partitionIndexesOut,            "[maxActivePartitions]",
               BUFFER,      PartitionInfo,                  partitionInfoOut,               "[numActivePartitions]",
               BUFFER,      uint8_t,                        smallSideIndexesOut,            "[maxActivePartitions]",
               BUFFER,      uint16_t,                       smallSideIndexToPartitionOut,   "[256]",
               LOCAL,       AssignPartitionNumbersLocalState, localState,                   "[1]",
               GID0,        uint16_t,                       workerId,,
               GSZ0,        uint16_t,                       numWorkers,)
{
    SYNC_CALL(assignPartitionNumbersImpl,
              treeTrainingInfo.data(), treeDepthInfo.data(), allPartitionSplits.data(),
              partitionIndexesOut.data(), partitionInfoOut.data(),
              smallSideIndexesOut.data(), smallSideIndexToPartitionOut.data(),
              localState, workerId, numWorkers);
    KERNEL_RETURN();
}

DEFINE_KERNEL2(randomforest_kernels,
               clearBucketsKernel,
               BUFFER,      const TreeTrainingInfo,       treeTrainingInfo,                 "[1]",
               BUFFER,      TreeDepthInfo,                treeDepthInfo,                    "[1]",
               BUFFER,      W,                            bucketsOut,                       "[numActiveBuckets * numActivePartitions]",
               BUFFER,      W,                            wAllOut,                          "[numActivePartitions]",
               BUFFER,      uint32_t,                     numNonZeroDirectionIndices,       "[1]",
               BUFFER,      const uint8_t,                smallSideIndexes,                 "[numActivePartitions]",
               GID0,        uint16_t,                     partitionWorkerId,,
               GSZ0,        uint16_t,                     partitionWorkgroupSize,,
               GID1,        uint32_t,                     bucket,)
{
    clearBucketsImpl(
              treeTrainingInfo.data(), treeDepthInfo.data(), bucketsOut.data(), wAllOut.data(), numNonZeroDirectionIndices.data(),
              smallSideIndexes.data(),
              partitionWorkerId, partitionWorkgroupSize, bucket);
    KERNEL_RETURN();
}

DEFINE_KERNEL2(randomforest_kernels,
               updatePartitionNumbersKernel,
               BUFFER,      const TreeTrainingInfo,        treeTrainingInfo,                "[1]",
               BUFFER,      TreeDepthInfo,                 treeDepthInfo,                   "[1]",
               BUFFER,      RowPartitionInfo,              partitions,                      "",
               BUFFER,      uint32_t,                      directions,                      "",
               BUFFER,      uint32_t,                      numNonZeroDirectionIndices,      "",
               BUFFER,      UpdateWorkEntry,               nonZeroDirectionIndices,         "",
               BUFFER,      const uint8_t,                 smallSideIndexes,                "",
               BUFFER,      const IndexedPartitionSplit,   allPartitionSplits,              "",
               BUFFER,      const PartitionInfo,           partitionInfo,                   "",
               BUFFER,      const uint32_t,                bucketData,                      "",
               BUFFER,      const uint32_t,                bucketDataOffsets,               "",
               BUFFER,      const uint32_t,                bucketNumbers,                   "",
               BUFFER,      const uint32_t,                bucketEntryBits,                 "",
               BUFFER,      const uint32_t,                featureIsOrdinal,                "",
               BUFFER,      const float,                   decodedRows,                     "",
               LOCAL,       UpdatePartitionNumbersLocalState,localState,                    "[1]",
               GID0,        uint32_t,                      workerIdInGrid,,
               GSZ0,        uint32_t,                      numWorkersInGrid,,
               LID0,        uint16_t,                      workerIdInWorkgroup,,
               LSZ0,        uint16_t,                      numWorkersInWorkgroup,)
{
    SYNC_CALL(updatePartitionNumbersImpl,
              treeTrainingInfo.data(), treeDepthInfo.data(), partitions.data(), directions.data(),
              numNonZeroDirectionIndices.data(), nonZeroDirectionIndices.data(), smallSideIndexes.data(),
              allPartitionSplits.data(), partitionInfo.data(),
              bucketData.data(), bucketDataOffsets.data(), bucketNumbers.data(), bucketEntryBits.data(),
              featureIsOrdinal.data(), decodedRows.data(),
              localState,
              workerIdInGrid, numWorkersInGrid,
              workerIdInWorkgroup, numWorkersInWorkgroup);

    KERNEL_RETURN();
}

DEFINE_KERNEL2(randomforest_kernels,
               updateBucketsKernel,
               BUFFER,      const TreeTrainingInfo,        treeTrainingInfo,                "",
               BUFFER,      TreeDepthInfo,                 treeDepthInfo,                   "",
               BUFFER,      const RowPartitionInfo,        partitions,                      "",
               BUFFER,      const uint32_t,                directions,                      "",
               BUFFER,      const uint32_t,                numNonZeroDirectionIndices,      "",
               BUFFER,      UpdateWorkEntry,               nonZeroDirectionIndices,         "",
               BUFFER,      W,                             buckets,                         "",
               BUFFER,      W,                             wAll,                            "",
               BUFFER,      const uint8_t,                 smallSideIndexes,                "",
               BUFFER,      const uint16_t,                smallSideIndexToPartition,       "",
               BUFFER,      const float,                   decodedRows,                     "",
               BUFFER,      const uint32_t,                bucketData,                      "",
               BUFFER,      const uint32_t,                bucketDataOffsets,               "",
               BUFFER,      const uint32_t,                bucketNumbers,                   "",
               BUFFER,      const uint32_t,                bucketEntryBits,                 "",
               BUFFER,      const uint32_t,                activeFeatureList,               "",
               BUFFER,      const uint32_t,                featureIsOrdinal,                "",
               LOCAL,       W,                             wLocal,                          "[maxLocalBuckets]",
               LOCAL,       UpdateBucketsLocalState,       localState,                      "[1]",
               TUNEABLE,    uint16_t,                      maxLocalBuckets,                 "getenv('RF_LOCAL_BUCKET_MEM',5500) / sizeof('W')",
               GID1,        uint16_t,                      featureWorkerId,,
               GID0,        uint32_t,                      workerIdInGrid,,
               GSZ0,        uint32_t,                      numWorkersInGrid,,
               LID0,        uint16_t,                      workerIdInWorkgroup,,
               LSZ0,        uint16_t,                      numWorkersInWorkgroup,)
{
    SYNC_CALL(updateBucketsImpl,
              treeTrainingInfo.data(), treeDepthInfo.data(), partitions.data(), directions.data(),
              numNonZeroDirectionIndices.data(), nonZeroDirectionIndices.data(), buckets.data(), wAll.data(),
              smallSideIndexes.data(), smallSideIndexToPartition.data(),
              decodedRows.data(), bucketData.data(), bucketDataOffsets.data(), bucketNumbers.data(), bucketEntryBits.data(),
              activeFeatureList.data(), featureIsOrdinal.data(),
              wLocal, maxLocalBuckets, localState,
              featureWorkerId, workerIdInWorkgroup, numWorkersInWorkgroup,
              workerIdInGrid, numWorkersInGrid);

    KERNEL_RETURN();
}

DEFINE_KERNEL2(randomforest_kernels,
              fixupBucketsKernel,
               BUFFER,      const TreeTrainingInfo,        treeTrainingInfo,                "[1]",
               BUFFER,      TreeDepthInfo,                 treeDepthInfo,                   "[1]",
               BUFFER,      W,                             buckets,                         "",
               BUFFER,      W,                             wAll,                            "",
               BUFFER,      const PartitionInfo,           partitionInfo,                   "",
               BUFFER,      const uint8_t,                 smallSideIndexes,                "",
               GID1,        uint16_t,                      featureWorkerId,,
               GID0,        uint32_t,                      workerIdInGrid,,
               GSZ0,        uint32_t,                      numWorkersInGrid,,
               LID0,        uint16_t,                      workerIdInWorkgroup,,
               LSZ0,        uint16_t,                      numWorkersInWorkgroup,)

{
    fixupBucketsImpl(treeTrainingInfo.data(), treeDepthInfo.data(), buckets.data(), wAll.data(),
                     partitionInfo.data(), smallSideIndexes.data(),
                     featureWorkerId, workerIdInGrid, numWorkersInGrid);
    KERNEL_RETURN();
}

} // namespace RF
} // namespace MLDB
