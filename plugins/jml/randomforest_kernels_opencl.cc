/** randomforest_kernels_opencl.cc                              -*- C++ -*-
    Jeremy Barnes, 8 September 2021
    Copyright (c) 2021 Jeremy Barnes.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#include "randomforest_kernels_opencl.h"
#include "randomforest_kernels.h"
#include "mldb/utils/environment.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/builtin/opencl/compute_kernel_opencl.h"
#include <future>
#include <array>


using namespace std;


namespace MLDB {
namespace RF {

EnvOption<bool> DEBUG_RF_OPENCL_KERNELS("DEBUG_RF_OPENCL_KERNELS", 0);
EnvOption<bool> RF_SEPARATE_FEATURE_UPDATES("RF_SEPARATE_FEATURE_UPDATES", 0);
EnvOption<bool> RF_EXPAND_FEATURE_BUCKETS("RF_EXPAND_FEATURE_BUCKETS", 0);
EnvOption<bool> RF_OPENCL_SYNCHRONOUS_LAUNCH("RF_OPENCL_SYNCHRONOUS_LAUNCH", 1);
EnvOption<size_t, true> RF_NUM_ROW_KERNELS("RF_NUM_ROW_KERNELS", 65536);
EnvOption<size_t, true> RF_ROW_KERNEL_WORKGROUP_SIZE("RF_ROW_KERNEL_WORKGROUP_SIZE", 256);

// Default of 5.5k allows 8 parallel workgroups for a 48k SM when accounting
// for 0.5k of local memory for the kernels.
// On Nvidia, with 32 registers/work item and 256 work items/workgroup
// (8 warps of 32 threads), we use 32 * 256 * 8 = 64k registers, which
// means full occupancy.
EnvOption<int, true> RF_LOCAL_BUCKET_MEM("RF_LOCAL_BUCKET_MEM", 5500);

namespace {

constexpr uint32_t maxWorkGroupSize = 256;  // TODO: device query


static struct RegisterKernels {

    RegisterKernels()
    {
        auto getProgram = [] (OpenCLComputeContext & context) -> OpenCLProgram
        {
            auto compileProgram = [&] () -> OpenCLProgram
            {
                std::string fileName = "mldb/plugins/jml/randomforest_kernels.cl";
                filter_istream stream(fileName);
                Utf8String source = "#line 1 \"" + fileName + "\"\n" + stream.readAll();

                OpenCLProgram program = context.clContext.createProgram(source);
                //string options = "-cl-kernel-arg-info -cl-fp32-correctly-rounded-divide-sqrt -DWBITS=32";
                string options = "-cl-kernel-arg-info -DWBITS=32";

                // Build for all devices
                auto buildInfo = program.build(context.clDevices, options);
                
                cerr << jsonEncode(buildInfo[0]) << endl;
                return program;
            };

            static const std::string cacheKey = "randomforest_kernels";
            OpenCLProgram program = context.getCacheEntry(cacheKey, compileProgram);
            return program;
        };
    
        //string options = "-cl-kernel-arg-info -cl-nv-maxrregcount=32 -cl-nv-verbose";// -cl-mad-enable -cl-fast-relaxed-math -cl-unsafe-math-optimizations -DFloat=" + type_name<Float>();

        auto createDecodeRowsKernel = [getProgram] (OpenCLComputeContext& context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "decodeRows";
            result->addDimension("r", "nr", 256);
            result->allowGridPadding();
            //result->device = context.devices[0]; // TODO deviceS
            result->addParameter("rowData", "r", "u64[rowDataLength]");
            result->addParameter("rowDataLength", "r", "u32");
            result->addParameter("weightBits", "r", "u16");
            result->addParameter("exampleNumBits", "r", "u16");
            result->addParameter("numRows", "r", "u32");
            result->addParameter("weightFormat", "r", "WeightFormat");
            result->addParameter("weightMultiplier", "r", "f32");
            result->addParameter("weightData", "r", "f32[weightDataLength]");
            result->addParameter("decodedRowsOut", "w", "f32[numRows]");
            result->addTuneable("threadsPerBlock", maxWorkGroupSize);
            result->addTuneable("blocksPerGrid", 16);
            result->setGridExpression("[blocksPerGrid]", "[threadsPerBlock]");
            result->setComputeFunction(program, "decompressRowsKernel");

            return result;
        };

        registerOpenCLComputeKernel("decodeRows", createDecodeRowsKernel);

        auto createTestFeatureKernel = [getProgram] (OpenCLComputeContext& context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "testFeature";
            //result->device = ComputeDevice::host();
            result->addDimension("fidx", "naf");
            result->addDimension("rowNum", "numRows");
            
            result->addParameter("decodedRows", "r", "f32[numRows]");
            result->addParameter("numRows", "r", "u32");
            result->addParameter("bucketData", "r", "u32[bucketDataLength]");
            result->addParameter("bucketDataOffsets", "r", "u32[nf + 1]");
            result->addParameter("bucketNumbers", "r", "u32[nf + 1]");
            result->addParameter("bucketEntryBits", "r", "u32[nf]");
            result->addParameter("activeFeatureList", "r", "u32[naf]");
            result->addParameter("partitionBuckets", "rw", "W32[numBuckets]");

            result->addTuneable("maxLocalBuckets", RF_LOCAL_BUCKET_MEM.get() / sizeof(W));
            result->addTuneable("threadsPerBlock", maxWorkGroupSize);
            result->addTuneable("blocksPerGrid", 32);

            result->addParameter("w", "w", "W[maxLocalBuckets]");
            result->addParameter("maxLocalBuckets", "r", "u32");

            result->setGridExpression("[naf,blocksPerGrid]", "[1,threadsPerBlock]");
            result->allowGridPadding();

            result->setComputeFunction(program, "testFeatureKernel");
            return result;
        };

        registerOpenCLComputeKernel("testFeature", createTestFeatureKernel);

        auto createGetPartitionSplitsKernel = [getProgram] (OpenCLComputeContext& context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "getPartitionSplits";
            //result->device = ComputeDevice::host();
            result->addDimension("fidx", "naf");

            result->addParameter("treeTrainingInfo", "r", "TreeTrainingInfo[1]");
            result->addParameter("bucketNumbers", "r", "u32[nf + 1]");
            result->addParameter("activeFeatureList", "r", "u32[naf]");
            result->addParameter("featureIsOrdinal", "r", "u32[nf]");
            result->addParameter("buckets", "r", "W32[numActiveBuckets * nap]");
            result->addParameter("wAll", "r", "W32[nap]");
            result->addParameter("featurePartitionSplitsOut", "w", "PartitionSplit[nap * naf]");
            result->addParameter("treeDepthInfo", "r", "TreeDepthInfo[1]");

            result->addTuneable("numPartitionsInParallel", maxWorkGroupSize);
            result->addTuneable("wLocalSize", RF_LOCAL_BUCKET_MEM.get() / sizeof(WIndexed));

            result->addParameter("wLocal", "w", "WIndexed[wLocalSize]");
            result->addParameter("wLocalSize", "r", "u32");

            result->setGridExpression("[1,naf,numPartitionsInParallel]", "[64,1,1]");

            result->setComputeFunction(program, "getPartitionSplitsKernel");

            return result;
        };

        registerOpenCLComputeKernel("getPartitionSplits", createGetPartitionSplitsKernel);

        auto createBestPartitionSplitKernel = [getProgram] (OpenCLComputeContext & context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "bestPartitionSplit";
            //result->device = ComputeDevice::host();

            result->addParameter("treeTrainingInfo", "r", "TreeTrainingInfo[1]");
            result->addParameter("treeDepthInfo", "r", "TreeDepthInfo[1]");

            result->addParameter("activeFeatureList", "r", "u32[numActiveFeatures]");
            result->addParameter("featurePartitionSplits", "r", "PartitionSplit[numActivePartitions * numActiveFeatures]");
            result->addParameter("partitionIndexes", "r", "PartitionIndex[npi]");
            result->addParameter("allPartitionSplitsOut", "w", "IndexedPartitionSplit[maxPartitions]");

            //result->addPreConstraint("numActivePartitions", "==", "readArrayElement(treeDepthInfo, 0).numActivePartitions");
            //result->addPostConstraint("numActivePartitions", "==", "readArrayElement(treeDepthInfo, 0).numActivePartitions");

            result->addTuneable("numPartitionsAtOnce", maxWorkGroupSize);
            result->setGridExpression("[numPartitionsAtOnce]", "[1]");
            result->setComputeFunction(program, "bestPartitionSplitKernel");
            return result;
        };

        registerOpenCLComputeKernel("bestPartitionSplit", createBestPartitionSplitKernel);

        auto createAssignPartitionNumbersKernel = [getProgram] (OpenCLComputeContext & context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();

            result->kernelName = "assignPartitionNumbers";
            //result->device = ComputeDevice::host();
            result->addParameter("treeTrainingInfo", "r", "TreeTrainingInfo[1]");
            result->addParameter("treeDepthInfo", "r", "TreeDepthInfo[1]");

            result->addParameter("allPartitionSplits", "r", "IndexedPartitionSplit[np]");
            result->addParameter("partitionIndexesOut", "w", "PartitionIndex[maxActivePartitions]");
            result->addParameter("partitionInfoOut", "w", "PartitionInfo[numActivePartitions]");
            result->addParameter("smallSideIndexesOut", "w", "u8[maxActivePartitions]");
            result->addParameter("smallSideIndexToPartitionOut", "w", "u16[256]");
            result->setGridExpression("[1]", "[32]");
            result->setComputeFunction(program, "assignPartitionNumbersKernel");
            return result;
        };

        registerOpenCLComputeKernel("assignPartitionNumbers", createAssignPartitionNumbersKernel);

        auto createClearBucketsKernel = [getProgram] (OpenCLComputeContext & context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "clearBuckets";
            //result->device = ComputeDevice::host();
            result->addDimension("bucket", "numActiveBuckets");
            result->addParameter("treeTrainingInfo", "r", "TreeTrainingInfo[1]");
            result->addParameter("treeDepthInfo", "r", "TreeDepthInfo[1]");
            result->addParameter("bucketsOut", "w", "W32[numActiveBuckets * numActivePartitions]");
            result->addParameter("wAllOut", "w", "W32[numActivePartitions]");
            result->addParameter("numNonZeroDirectionIndices", "w", "u32[1]");
            result->addParameter("smallSideIndexes", "r", "u8[numActivePartitions]");
            result->allowGridPadding();
            result->addTuneable("gridBlockSize", 64);
            result->addTuneable("numPartitionsAtOnce", maxWorkGroupSize);
            result->setGridExpression("[numPartitionsAtOnce,ceilDiv(numActiveBuckets,gridBlockSize)]", "[1,gridBlockSize]");
            result->setComputeFunction(program, "clearBucketsKernel");
            return result;
        };

        registerOpenCLComputeKernel("clearBuckets", createClearBucketsKernel);

        auto createUpdatePartitionNumbersKernel = [getProgram] (OpenCLComputeContext & context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "updatePartitionNumbers";
            //result->device = ComputeDevice::host();
            result->addDimension("r", "numRows");

            result->addParameter("treeTrainingInfo", "r", "TreeTrainingInfo[1]");
            result->addParameter("treeDepthInfo", "r", "TreeDepthInfo[1]");

            result->addParameter("partitions", "r", "RowPartitionInfo[numRows]");
            result->addParameter("directions", "w", "u32[(numRows+31)/32]");
            result->addParameter("numNonZeroDirectionIndices", "rw", "u32[1]");
            result->addParameter("nonZeroDirectionIndices", "w", "UpdateWorkEntry[numRows / 2 + 2]");
            result->addParameter("smallSideIndexes", "r", "u8[numActivePartitions]");
            result->addParameter("allPartitionSplits", "r", "IndexedPartitionSplit[naps]");
            result->addParameter("partitionInfo", "r", "PartitionInfo[np]");
            result->addParameter("bucketData", "r", "u32[bucketDataLength]");
            result->addParameter("bucketDataOffsets", "r", "u32[nf + 1]");
            result->addParameter("bucketNumbers", "r", "u32[nf + 1]");
            result->addParameter("bucketEntryBits", "r", "u32[nf]");
            result->addParameter("featureIsOrdinal", "r", "u32[nf]");
            result->addParameter("decodedRows", "r", "f32[numRows]");
            result->addTuneable("threadsPerBlock", maxWorkGroupSize);
            result->addTuneable("blocksPerGrid", 96);
            result->allowGridPadding();
            result->setGridExpression("[blocksPerGrid]", "[threadsPerBlock]");
            result->setComputeFunction(program, "updatePartitionNumbersKernel");
            return result;
        };

        registerOpenCLComputeKernel("updatePartitionNumbers", createUpdatePartitionNumbersKernel);

        auto createUpdateBucketsKernel = [getProgram] (OpenCLComputeContext & context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "updateBuckets";
            result->kernelName = "updateBuckets";
            result->device = ComputeDevice::host();
            result->addDimension("r", "numRows");
            result->addDimension("fidx_plus_1", "naf_plus_1");

            result->addParameter("treeTrainingInfo", "r", "TreeTrainingInfo[1]");
            result->addParameter("treeDepthInfo", "r", "TreeDepthInfo[1]");

            result->addParameter("partitions", "r", "RowPartitionInfo[numRows]");
            result->addParameter("directions", "r", "u32[(numRows + 31)/32]");
            result->addParameter("numNonZeroDirectionIndices", "r", "u32[1]");
            result->addParameter("nonZeroDirectionIndices", "r", "UpdateWorkEntry[numRows / 2 + 2]");
            result->addParameter("buckets", "w", "W32[numActiveBuckets * numActivePartitions]");
            result->addParameter("wAll", "w", "W32[numActivePartitions]");
            result->addParameter("smallSideIndexes", "r", "u8[numActivePartitions]");
            result->addParameter("smallSideIndexToPartition", "r", "u16[256]");
            result->addParameter("decodedRows", "r", "f32[nr]");
            result->addParameter("bucketData", "r", "u32[bucketDataLength]");
            result->addParameter("bucketDataOffsets", "r", "u32[nf + 1]");
            result->addParameter("bucketNumbers", "r", "u32[nf + 1]");
            result->addParameter("bucketEntryBits", "r", "u32[nf]");
            result->addParameter("activeFeatureList", "r", "u32[numActiveFeatures]");
            result->addParameter("featureIsOrdinal", "r", "u32[nf]");
            result->addTuneable("maxLocalBuckets", RF_LOCAL_BUCKET_MEM.get() / sizeof(W));
            result->addTuneable("threadsPerBlock", maxWorkGroupSize);
            result->addTuneable("blocksPerGrid", 32);
            result->addParameter("wLocal", "w", "W[maxLocalBuckets]");
            result->addParameter("maxLocalBuckets", "r", "u32");
            result->addConstraint("naf_plus_1", "==", "numActiveFeatures + 1", "help the solver");
            result->addConstraint("numActiveFeatures", "==", "naf_plus_1 - 1", "help the solver");
            result->setGridExpression("[blocksPerGrid,numActiveFeatures+1]", "[threadsPerBlock,1]");
            result->allowGridPadding();
            result->setComputeFunction(program, "updateBucketsKernel");
            return result;
        };

        registerOpenCLComputeKernel("updateBuckets", createUpdateBucketsKernel);

        auto createFixupBucketsKernel = [getProgram] (OpenCLComputeContext & context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "fixupBuckets";
            result->device = ComputeDevice::host();
            result->addDimension("bucket", "numActiveBuckets");

            result->addParameter("treeTrainingInfo", "r", "TreeTrainingInfo[=1]");
            result->addParameter("treeDepthInfo", "r", "TreeDepthInfo[=1]");

            result->addParameter("buckets", "rw", "W32[numActiveBuckets * newNumPartitions]");
            result->addParameter("wAll", "rw", "W32[newNumPartitions]");
            result->addParameter("partitionInfo", "r", "PartitionInfo[np]");
            result->addParameter("smallSideIndexes", "r", "u8[newNumPartitions]");
            result->addTuneable("gridBlockSize", 64);
            result->addTuneable("numPartitionsAtOnce", maxWorkGroupSize);
            result->allowGridPadding();
            result->setGridExpression("[numPartitionsAtOnce,ceilDiv(numActiveBuckets,gridBlockSize)]", "[1,gridBlockSize]");
            result->setComputeFunction(program, "fixupBucketsKernel");
            return result;
        };

        registerOpenCLComputeKernel("fixupBuckets", createFixupBucketsKernel);
    }

} registerKernels;
} // file scope

} // namespace RF
} // namespace MLDB
