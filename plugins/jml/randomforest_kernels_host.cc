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

DEFINE_KERNEL(randomforest_kernels,
              decompressRowsKernel,
              std::span<const uint64_t>,                rowData,
              uint32_t,                                 rowDataLength,
              uint16_t,                                 weightBits,
              uint16_t,                                 exampleNumBits,
              uint32_t,                                 numRows,
              WeightFormat,                             weightFormat,
              float,                                    weightMultiplier,
              std::span<const float>,                   weightData,
              std::span<float>,                         decodedRowsOut)
{
    using namespace std;
    cerr << "rowData = " << rowData.data() << " len " << rowData.size() << endl;
    cerr << "rowDataLength = " << rowDataLength << endl;
    decompressRowsImpl(rowData.data(), rowDataLength, weightBits, exampleNumBits, numRows,
                       weightFormat, weightMultiplier, weightData.data(), decodedRowsOut.data(),
                       0 /* id */, 1 /* n */);
}

DEFINE_KERNEL(randomforest_kernels,
              testFeatureKernel,
              uint16_t,                                 numActiveFeatures,
              std::span<const float>,                   decodedRows,
              uint32_t,                                 numRows,
              std::span<const uint32_t>,                bucketData,
              std::span<const uint32_t>,                bucketDataOffsets,
              std::span<const uint32_t>,                bucketNumbers,
              std::span<const uint32_t>,                bucketEntryBits,
              std::span<const uint32_t>,                activeFeatureList,
              std::span<W>,                             partitionBuckets)
{
    uint32_t maxLocalBuckets = 0;
    for (uint16_t fidx = 0;  fidx < numActiveFeatures;  ++fidx) {
        testFeatureImpl(decodedRows.data(), numRows, bucketData.data(), bucketDataOffsets.data(), bucketNumbers.data(),
                        bucketEntryBits.data(), activeFeatureList.data(), nullptr, maxLocalBuckets,
                        partitionBuckets.data(), fidx,
                        0, 1, 0, numRows);
    }
}

DEFINE_KERNEL(randomforest_kernels,
              getPartitionSplitsKernel,
              std::span<const TreeTrainingInfo>,         treeTrainingInfo,
              std::span<const uint32_t>,                 bucketNumbers,                      
              std::span<const uint32_t>,                 activeFeatureList,
              std::span<const uint32_t>,                 featureIsOrdinal,
              std::span<const W>,                        buckets,
              std::span<const W>,                        wAll,
              std::span<PartitionSplit>,                 featurePartitionSplitsOut,
              std::span<const TreeDepthInfo>,            treeDepthInfo)
{
    MLDB_THROW_UNIMPLEMENTED();
}

DEFINE_KERNEL(randomforest_kernels,
              bestPartitionSplitKernel,
              std::span<const TreeTrainingInfo>,         treeTrainingInfo,
              std::span<const TreeDepthInfo>,            treeDepthInfo,
              std::span<const uint32_t>,                 activeFeatureList,
              std::span<const PartitionSplit>,           featurePartitionSplits,
              std::span<const PartitionIndex>,           partitionIndexes,
              std::span<IndexedPartitionSplit>,          allPartitionSplitsOut)
{
    MLDB_THROW_UNIMPLEMENTED();
    //bestPartitionSplitImpl(&treeTrainingInfo, &treeDepthInfo, activeFeatureList,
    //                       featurePartitionSplits, partitionIndexes, allPartitionSplitsOut,
    //                       get_global_id(0), get_global_size(0));
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
