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

        auto createDoNothingKernel = [getProgram] (OpenCLComputeContext& context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "doNothing";
            auto setTheRest = [=] (OpenCLKernel & kernel, OpenCLComputeContext & context)
            {
                kernel.bindArg("dummy", (uint32_t)42);
            };
            result->modifyGrid = [=] (std::vector<size_t> & grid)
            {
                ExcAssertEqual(grid.size(), 0);
                grid.push_back(1);
            };

            result->setParameters(setTheRest);
            result->setComputeFunction(program, "doNothingKernel", {});
            return result;
        };

        registerOpenCLComputeKernel("doNothing", createDoNothingKernel);

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
            result->modifyGrid = [=] (std::vector<size_t> & grid)
            {
                ExcAssertEqual(grid.size(), 1);
                grid[0] = 4096;  // don't do one launch per row, the kernel will iterate
            };

            result->setComputeFunction(program, "decompressRowsKernel", { 256 });

            return result;
        };

        registerOpenCLComputeKernel("decodeRows", createDecodeRowsKernel);

        auto createTestFeatureKernel = [getProgram] (OpenCLComputeContext& context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "testFeature";
            //result->device = ComputeDevice::host();
            result->addDimension("featureNum", "nf");
            result->addDimension("rowNum", "numRows");
            result->addParameter("decodedRows", "r", "f32[numRows]");
            result->addParameter("numRows", "r", "u32");
            result->addParameter("bucketData", "r", "u32[bucketDataLength]");
            result->addParameter("bucketDataOffsets", "r", "u32[nf + 1]");
            result->addParameter("bucketNumbers", "r", "u32[nf]");
            result->addParameter("bucketEntryBits", "r", "u32[nf]");
            result->addParameter("featuresActive", "r", "u32[nf]");
            result->addParameter("partitionBuckets", "rw", "W32[numBuckets]");
            result->allowGridPadding();
            auto setTheRest = [=] (OpenCLKernel & kernel, OpenCLComputeContext & context)
            {
                auto maxLocalBuckets = RF_LOCAL_BUCKET_MEM.get() / sizeof(W);
                //cerr << "maxLocalBuckets = " << maxLocalBuckets << endl;
                //auto maxLocalBuckets = context.getCacheEntry<uint32_t>("maxLocalBuckets");
                kernel.bindArg("w", LocalArray<W>(maxLocalBuckets));
                kernel.bindArg("maxLocalBuckets", maxLocalBuckets);
            };
            result->modifyGrid = [=] (std::vector<size_t> & grid)
            {
                ExcAssertEqual(grid.size(), 2);
                grid[1] = 4096;  // don't do one launch per row, the kernel will iterate
            };
            result->setParameters(setTheRest);
            result->setComputeFunction(program, "testFeatureKernel", { 1, 256 } );
            return result;
        };

        registerOpenCLComputeKernel("testFeature", createTestFeatureKernel);

        auto createGetPartitionSplitsKernel = [getProgram] (OpenCLComputeContext& context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "getPartitionSplits";
            //result->device = ComputeDevice::host();
            result->addDimension("f", "nf");
            result->addDimension("p", "numPartitions");
            result->addDimension("b", "maxNumBuckets");
            result->addParameter("totalBuckets", "r", "u32");
            result->addParameter("numPartitions", "r", "u32");
            result->addParameter("bucketNumbers", "r", "u32[nf]");
            result->addParameter("featuresActive", "r", "u32[nf]");
            result->addParameter("featureIsOrdinal", "r", "u32[nf]");
            result->addParameter("buckets", "r", "W32[totalBuckets * np]");
            result->addParameter("wAll", "r", "W32[np]");
            result->addParameter("featurePartitionSplitsOut", "w", "PartitionSplit[np * nf]");
            auto setTheRest = [=] (OpenCLKernel & kernel, OpenCLComputeContext & context)
            {
                auto maxLocalBuckets = RF_LOCAL_BUCKET_MEM.get() / sizeof(WIndexed);
                //cerr << "maxLocalBuckets WIndexed = " << maxLocalBuckets << endl;
                kernel.bindArg("wLocal", LocalArray<WIndexed>(maxLocalBuckets));
                kernel.bindArg("wLocalSize", maxLocalBuckets);
                kernel.bindArg("wStartBest", LocalArray<WIndexed>(2));
            };
            result->modifyGrid = [=] (std::vector<size_t> & grid)
            {
                ExcAssertEqual(grid.size(), 3);
            };
            result->setParameters(setTheRest);
            result->setComputeFunction(program, "getPartitionSplitsKernel", { 64, 1, 1 });
            return result;
        };

        registerOpenCLComputeKernel("getPartitionSplits", createGetPartitionSplitsKernel);

        auto createBestPartitionSplitKernel = [getProgram] (OpenCLComputeContext & context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "bestPartitionSplit";
            //result->device = ComputeDevice::host();
            result->addDimension("p", "np");
            result->addParameter("numFeatures", "r", "u32");
            result->addParameter("featuresActive", "r", "u32[numFeatures]");
            result->addParameter("featurePartitionSplits", "r", "PartitionSplit[np * nf]");
            result->addParameter("allPartitionSplitsOut", "w", "PartitionSplit[maxPartitions]");
            result->addParameter("partitionSplitsOffset", "r", "u32");
            auto setTheRest = [=] (OpenCLKernel & kernel, OpenCLComputeContext & context)
            {
            };
            result->setParameters(setTheRest);
            result->setComputeFunction(program, "bestPartitionSplitKernel", { 1 });
            return result;
        };

        registerOpenCLComputeKernel("bestPartitionSplit", createBestPartitionSplitKernel);

        auto createClearBucketsKernel = [getProgram] (OpenCLComputeContext & context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "clearBuckets";
            //result->device = ComputeDevice::host();
            result->addDimension("p", "partitionSplitsOffset");
            result->addDimension("b", "numActiveBuckets");
            result->addParameter("bucketsOut", "w", "W32[numActiveBuckets * np * 2]");
            result->addParameter("wAllOut", "w", "W32[np * 2]");
            result->addParameter("allPartitionSplits", "r", "MLDB::RF::PartitionSplit[np]");
            result->addParameter("numActiveBuckets", "r", "u32");
            result->addParameter("partitionSplitsOffset", "r", "u32");
            result->allowGridPadding();
            auto setTheRest = [=] (OpenCLKernel & kernel, OpenCLComputeContext & context)
            {
            };
            result->setParameters(setTheRest);
            result->setComputeFunction(program, "clearBucketsKernel", { 1, 64 });
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
            result->addParameter("partitionSplitsOffset", "r", "u32");
            result->addParameter("partitions", "r", "RowPartitionInfo[numRows]");
            result->addParameter("directions", "w", "u8[numRows]");
            result->addParameter("numRows", "r", "u32");
            result->addParameter("allPartitionSplits", "r", "PartitionSplit[np]");
            result->addParameter("bucketData", "r", "u32[bucketDataLength]");
            result->addParameter("bucketDataOffsets", "r", "u32[nf + 1]");
            result->addParameter("bucketNumbers", "r", "u32[nf]");
            result->addParameter("bucketEntryBits", "r", "u32[nf]");
            result->addParameter("featureIsOrdinal", "r", "u32[nf]");
            result->allowGridPadding();
            auto setTheRest = [=] (OpenCLKernel & kernel, OpenCLComputeContext & context)
            {
            };
            result->setParameters(setTheRest);
            result->setComputeFunction(program, "updatePartitionNumbersKernel", { 256 });
            return result;
        };

        registerOpenCLComputeKernel("updatePartitionNumbers", createUpdatePartitionNumbersKernel);

        auto createUpdateBucketsKernel = [getProgram] (OpenCLComputeContext & context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "updateBuckets";
            result->device = ComputeDevice::host();
            result->addDimension("r", "numRows");
            result->addDimension("f", "nf");
            result->addParameter("partitionSplitsOffset", "r", "u32");
            result->addParameter("numActiveBuckets", "r", "u32");
            result->addParameter("partitions", "r", "MLDB::RF::RowPartitionInfo[numRows]");
            result->addParameter("directions", "r", "u8[numRows]");
            result->addParameter("buckets", "w", "W32[numActiveBuckets * np * 2]");
            result->addParameter("wAll", "w", "W32[np * 2]");
            result->addParameter("decodedRows", "r", "f32[nr]");
            result->addParameter("numRows", "r", "u32");
            result->addParameter("bucketData", "r", "u32[bucketDataLength]");
            result->addParameter("bucketDataOffsets", "r", "u32[nf + 1]");
            result->addParameter("bucketNumbers", "r", "u32[nf]");
            result->addParameter("bucketEntryBits", "r", "u32[nf]");
            result->addParameter("featuresActive", "r", "u32[numFeatures]");
            result->addParameter("featureIsOrdinal", "r", "u32[nf]");
            result->allowGridPadding();
            auto setTheRest = [=] (OpenCLKernel & kernel, OpenCLComputeContext & context)
            {
                auto maxLocalBuckets = RF_LOCAL_BUCKET_MEM.get() / sizeof(W);
                //cerr << "maxLocalBuckets W = " << maxLocalBuckets << endl;
                kernel.bindArg("wLocal", LocalArray<W>(maxLocalBuckets));
                kernel.bindArg("maxLocalBuckets", maxLocalBuckets);
            };
            result->modifyGrid = [=] (std::vector<size_t> & grid)
            {
                ExcAssertEqual(grid.size(), 2);
                grid[0] = 2048;
            };
            result->setParameters(setTheRest);
            result->setComputeFunction(program, "updateBucketsKernel", { 256, 1 });
            return result;
        };

        registerOpenCLComputeKernel("updateBuckets", createUpdateBucketsKernel);

        auto createFixupBucketsKernel = [getProgram] (OpenCLComputeContext & context) -> std::shared_ptr<OpenCLComputeKernel>
        {
            auto program = getProgram(context);
            auto result = std::make_shared<OpenCLComputeKernel>();
            result->kernelName = "fixupBuckets";
            result->device = ComputeDevice::host();
            result->addDimension("partition", "np");
            result->addDimension("bucket", "numActiveBuckets");
            result->addParameter("buckets", "w", "W32[numActiveBuckets * np * 2]");
            result->addParameter("wAll", "w", "W32[np * 2]");
            result->addParameter("allPartitionSplits", "r", "MLDB::RF::PartitionSplit[np]");
            result->addParameter("numActiveBuckets", "r", "u32");
            result->addParameter("partitionSplitsOffset", "r", "u32");
            result->allowGridPadding();
            auto setTheRest = [=] (OpenCLKernel & kernel, OpenCLComputeContext & context)
            {
            };
            result->setParameters(setTheRest);
            result->setComputeFunction(program, "fixupBucketsKernel", { 1, 64 });
            return result;
        };

        registerOpenCLComputeKernel("fixupBuckets", createFixupBucketsKernel);
    }

} registerKernels;
} // file scope

} // namespace RF
} // namespace MLDB
