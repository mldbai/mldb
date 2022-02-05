#include <ukl>

#include "randomforest_kernels_common.h"

DEFINE_KERNEL2(randomforest_kernels,
              decompressRowsKernel,
              ROBUFFER, uint64_t,                                 rowData,        "[rowDataLength]",
              LITERAL,  uint32_t,                                 rowDataLength,,
              LITERAL,  uint16_t,                                 weightBits,,
              LITERAL,  uint16_t,                                 exampleNumBits,,
              LITERAL,  uint32_t,                                 numRows,,
              LITERAL,  WeightFormat,                             weightFormat,,
              LITERAL,  float,                                    weightMultiplier,,
              ROBUFFER, float,                                    weightData,     "[weightDataLength]",
              RWBUFFER, float,                                    decodedRowsOut, "[numRows]",
              GID0,     uint32_t,                                 id,,
              GSZ0,     uint32_t,                                 n,)
{
    decompressRowsImpl(rowData, rowDataLength, weightBits, exampleNumBits, numRows,
                       weightFormat, weightMultiplier, weightData, decodedRowsOut,
                       id, n);
    KERNEL_RETURN();
}

DEFINE_KERNEL2(randomforest_kernels,
              testFeatureKernel,
              ROBUFFER, float,                                    decodedRows,          "[numRows]",
              LITERAL,  uint32_t,                                 numRows,,
              ROBUFFER, uint32_t,                                 bucketData,           "[bucketDataLength]",
              ROBUFFER, uint32_t,                                 bucketDataOffsets,    "[numFeatures + 1]",
              ROBUFFER, uint32_t,                                 bucketNumbers,        "[numFeatures + 1]",
              ROBUFFER, uint32_t,                                 bucketEntryBits,      "[numFeatures]",
              ROBUFFER, uint32_t,                                 activeFeatureList,    "[numActiveFeatures]",
              RWBUFFER, W,                                        partitionBuckets,     "[numBuckets]",
              RWLOCAL,  W,                                        w,                    "[maxLocalBuckets]",
              TUNEABLE, uint16_t,                                 maxLocalBuckets,      "getenv('RF_LOCAL_BUCKET_MEM',5500) / sizeof('W')",
              GID0,     uint16_t,                                 fidx,,
              GSZ0,     uint16_t,                                 numActiveFeatures,,
              LID1,     uint16_t,                                 threadGroupId,,
              LSZ1,     uint16_t,                                 threadGroupSize,,
              GID1,     uint16_t,                                 gridId,,
              GSZ1,     uint16_t,                                 gridSize,)
{
    SYNC_CALL(testFeatureImpl,
              decodedRows, numRows, bucketData, bucketDataOffsets, bucketNumbers,
              bucketEntryBits, activeFeatureList, w, maxLocalBuckets,
              partitionBuckets, fidx, threadGroupId, threadGroupSize, gridId, gridSize);
    KERNEL_RETURN();
}

DEFINE_KERNEL2(randomforest_kernels,
               getPartitionSplitsKernel,
               ROBUFFER,    TreeTrainingInfo,           treeTrainingInfo,           "[1]",
               ROBUFFER,    uint32_t,                   bucketNumbers,              "[numFeatures + 1]",
               ROBUFFER,    uint32_t,                   activeFeatureList,          "[numActiveFeatures]",
               ROBUFFER,    uint32_t,                   featureIsOrdinal,           "[numFeatures]",
               ROBUFFER,    W,                          buckets,                    "[numActiveBuckets * numActivePartitions]",
               ROBUFFER,    W,                          wAll,                       "[numActivePartitions]",
               RWBUFFER,    PartitionSplit,             featurePartitionSplitsOut,  "[numActivePartitions * numActiveFeatures]",
               ROBUFFER,    TreeDepthInfo,              treeDepthInfo,              "[1]",
               RWLOCAL,     WIndexed,                   wLocal,                     "[wLocalSize]",
               TUNEABLE,    uint16_t,                   wLocalSize,                 "getenv('RF_LOCAL_BUCKET_MEM',5500) / sizeof('WIndexed')",
               RWLOCAL,     WIndexed,                   wStartBest,                 "[2]",

               GID1,        uint16_t,                   featureId,,
               GSZ1,        uint16_t,                   numActiveFeatures,,
               LID0,        uint16_t,                   workerId,,
               LSZ0,        uint16_t,                   workGroupSize,,
               GID2,        uint16_t,                   partitionWorkerId,,
               GSZ2,        uint16_t,                   partitionWorkerSize,)
{
    SYNC_CALL(getPartitionSplitsImpl,
              treeTrainingInfo, bucketNumbers,
              activeFeatureList, featureIsOrdinal,
              buckets, wAll, featurePartitionSplitsOut,
              treeDepthInfo, wLocal, wLocalSize, wStartBest,
              featureId, numActiveFeatures,
              workerId, workGroupSize,
              partitionWorkerId, partitionWorkerSize);

    KERNEL_RETURN();
}

DEFINE_KERNEL2(randomforest_kernels,
               bestPartitionSplitKernel,
               ROBUFFER,    TreeTrainingInfo,               treeTrainingInfo,           "[1]",     
               ROBUFFER,    TreeDepthInfo,                  treeDepthInfo,              "[1]",     
               ROBUFFER,    uint32_t,                       activeFeatureList,          "[numActiveFeatures]",     
               ROBUFFER,    PartitionSplit,                 featurePartitionSplits,     "[numActivePartitions * numActiveFeatures]",     
               ROBUFFER,    PartitionIndex,                 partitionIndexes,           "[numActivePartitions]",     
               RWBUFFER,    IndexedPartitionSplit,          allPartitionSplitsOut,      "[numActivePartitions]",
               GID0,        uint16_t,                       workerId,,
               GSZ0,        uint16_t,                       numWorkers,)
{
    bestPartitionSplitImpl(treeTrainingInfo, treeDepthInfo, activeFeatureList,
                           featurePartitionSplits, partitionIndexes, allPartitionSplitsOut,
                           workerId, numWorkers);
    KERNEL_RETURN();
}

DEFINE_KERNEL2(randomforest_kernels,
               assignPartitionNumbersKernel,
               ROBUFFER,    TreeTrainingInfo,               treeTrainingInfo,               "[1]",
               RWBUFFER,    TreeDepthInfo,                  treeDepthInfo,                  "[1]",
               ROBUFFER,    IndexedPartitionSplit,          allPartitionSplits,             "[numActivePartitions]",
               RWBUFFER,    PartitionIndex,                 partitionIndexesOut,            "[maxActivePartitions]",
               RWBUFFER,    PartitionInfo,                  partitionInfoOut,               "[numActivePartitions]",
               RWBUFFER,    uint8_t,                        smallSideIndexesOut,            "[maxActivePartitions]",
               RWBUFFER,    uint16_t,                       smallSideIndexToPartitionOut,   "[256]",
               RWLOCAL,     AssignPartitionNumbersLocalState, localState,                   "[1]",
               GID0,        uint16_t,                       workerId,,
               GSZ0,        uint16_t,                       numWorkers,)
{
    SYNC_CALL(assignPartitionNumbersImpl,
              treeTrainingInfo, treeDepthInfo, allPartitionSplits,
              partitionIndexesOut, partitionInfoOut,
              smallSideIndexesOut, smallSideIndexToPartitionOut,
              localState, workerId, numWorkers);

    KERNEL_RETURN();
}

DEFINE_KERNEL2(randomforest_kernels,
               clearBucketsKernel,
               ROBUFFER,    TreeTrainingInfo,             treeTrainingInfo,                 "[1]",
               RWBUFFER,    TreeDepthInfo,                treeDepthInfo,                    "[1]",
               RWBUFFER,    W,                            bucketsOut,                       "[numActiveBuckets * numActivePartitions]",
               RWBUFFER,    W,                            wAllOut,                          "[numActivePartitions]",
               RWBUFFER,    uint32_t,                     numNonZeroDirectionIndices,       "[1]",
               ROBUFFER,    uint8_t,                      smallSideIndexes,                 "[numActivePartitions]",
               GID0,        uint16_t,                     partitionWorkerId,,
               GSZ0,        uint16_t,                     partitionWorkgroupSize,,
               GID1,        uint32_t,                     bucket,)
{
    clearBucketsImpl(
              treeTrainingInfo, treeDepthInfo, bucketsOut, wAllOut, numNonZeroDirectionIndices,
              smallSideIndexes,
              partitionWorkerId, partitionWorkgroupSize, bucket);
    KERNEL_RETURN();
}

DEFINE_KERNEL2(randomforest_kernels,
               updatePartitionNumbersKernel,
               ROBUFFER,    TreeTrainingInfo,              treeTrainingInfo,                "[1]",
               RWBUFFER,    TreeDepthInfo,                 treeDepthInfo,                   "[1]",
               RWBUFFER,    RowPartitionInfo,              partitions,                      "",
               RWBUFFER,    uint32_t,                      directions,                      "",
               RWBUFFER,    uint32_t,                      numNonZeroDirectionIndices,      "",
               RWBUFFER,    UpdateWorkEntry,               nonZeroDirectionIndices,         "",
               ROBUFFER,    uint8_t,                       smallSideIndexes,                "",
               ROBUFFER,    IndexedPartitionSplit,         allPartitionSplits,              "",
               ROBUFFER,    PartitionInfo,                 partitionInfo,                   "",
               ROBUFFER,    uint32_t,                      bucketData,                      "",
               ROBUFFER,    uint32_t,                      bucketDataOffsets,               "",
               ROBUFFER,    uint32_t,                      bucketNumbers,                   "",
               ROBUFFER,    uint32_t,                      bucketEntryBits,                 "",
               ROBUFFER,    uint32_t,                      featureIsOrdinal,                "",
               ROBUFFER,    float,                         decodedRows,                     "",
               RWLOCAL,     UpdatePartitionNumbersLocalState,localState,                    "[1]",
               GID0,        uint32_t,                      workerIdInGrid,,
               GSZ0,        uint32_t,                      numWorkersInGrid,,
               LID0,        uint16_t,                      workerIdInWorkgroup,,
               LSZ0,        uint16_t,                      numWorkersInWorkgroup,)
{
    SYNC_CALL(updatePartitionNumbersImpl,
              treeTrainingInfo, treeDepthInfo, partitions, directions,
              numNonZeroDirectionIndices, nonZeroDirectionIndices, smallSideIndexes,
              allPartitionSplits, partitionInfo,
              bucketData, bucketDataOffsets, bucketNumbers, bucketEntryBits,
              featureIsOrdinal, decodedRows,
              localState,
              workerIdInGrid, numWorkersInGrid,
              workerIdInWorkgroup, numWorkersInWorkgroup);

    KERNEL_RETURN();
}

DEFINE_KERNEL2(randomforest_kernels,
               updateBucketsKernel,
               ROBUFFER,    TreeTrainingInfo,              treeTrainingInfo,                "",
               RWBUFFER,    TreeDepthInfo,                 treeDepthInfo,                   "",
               ROBUFFER,    RowPartitionInfo,              partitions,                      "",
               ROBUFFER,    uint32_t,                      directions,                      "",
               ROBUFFER,    uint32_t,                      numNonZeroDirectionIndices,      "",
               RWBUFFER,    UpdateWorkEntry,               nonZeroDirectionIndices,         "",
               RWBUFFER,    W,                             buckets,                         "",
               RWBUFFER,    W,                             wAll,                            "",
               ROBUFFER,    uint8_t,                       smallSideIndexes,                "",
               ROBUFFER,    uint16_t,                      smallSideIndexToPartition,       "",
               ROBUFFER,    float,                         decodedRows,                     "",
               ROBUFFER,    uint32_t,                      bucketData,                      "",
               ROBUFFER,    uint32_t,                      bucketDataOffsets,               "",
               ROBUFFER,    uint32_t,                      bucketNumbers,                   "",
               ROBUFFER,    uint32_t,                      bucketEntryBits,                 "",
               ROBUFFER,    uint32_t,                      activeFeatureList,               "",
               ROBUFFER,    uint32_t,                      featureIsOrdinal,                "",
               RWLOCAL,     W,                             wLocal,                          "[maxLocalBuckets]",
               RWLOCAL,     UpdateBucketsLocalState,       localState,                      "[1]",
               TUNEABLE,    uint16_t,                      maxLocalBuckets,                 "getenv('RF_LOCAL_BUCKET_MEM',5500) / sizeof('W')",
               GID1,        uint16_t,                      featureWorkerId,,
               GID0,        uint32_t,                      workerIdInGrid,,
               GSZ0,        uint32_t,                      numWorkersInGrid,,
               LID0,        uint16_t,                      workerIdInWorkgroup,,
               LSZ0,        uint16_t,                      numWorkersInWorkgroup,)
{
    SYNC_CALL(updateBucketsImpl,
              treeTrainingInfo, treeDepthInfo, partitions, directions,
              numNonZeroDirectionIndices, nonZeroDirectionIndices, buckets, wAll,
              smallSideIndexes, smallSideIndexToPartition,
              decodedRows, bucketData, bucketDataOffsets, bucketNumbers, bucketEntryBits,
              activeFeatureList, featureIsOrdinal,
              wLocal, maxLocalBuckets, localState,
              featureWorkerId, workerIdInWorkgroup, numWorkersInWorkgroup,
              workerIdInGrid, numWorkersInGrid);

    KERNEL_RETURN();
}

DEFINE_KERNEL2(randomforest_kernels,
               fixupBucketsKernel,
               ROBUFFER,    TreeTrainingInfo,              treeTrainingInfo,                "[1]",
               RWBUFFER,    TreeDepthInfo,                 treeDepthInfo,                   "[1]",
               RWBUFFER,    W,                             buckets,                         "",
               RWBUFFER,    W,                             wAll,                            "",
               ROBUFFER,    PartitionInfo,                 partitionInfo,                   "",
               ROBUFFER,    uint8_t,                       smallSideIndexes,                "",
               GID1,        uint16_t,                      bucketWorkerId,,
               GSZ1,        uint16_t,                      numBucketsInGrid,,
               GID0,        uint32_t,                      workerIdInGrid,,
               GSZ0,        uint32_t,                      numWorkersInGrid,,
               LID0,        uint16_t,                      workerIdInWorkgroup,,
               LSZ0,        uint16_t,                      numWorkersInWorkgroup,)

{
    fixupBucketsImpl(treeTrainingInfo, treeDepthInfo, buckets, wAll,
                     partitionInfo, smallSideIndexes,
                     bucketWorkerId, workerIdInGrid, numWorkersInGrid);

    KERNEL_RETURN();
}
