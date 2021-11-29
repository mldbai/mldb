/** randomforest_kernels_metal.cc                              -*- C++ -*-
    Jeremy Barnes, 8 September 2021
    Copyright (c) 2021 Jeremy Barnes.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#include "randomforest_kernels_metal.h"
#include "randomforest_kernels.h"
#include "mldb/utils/environment.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/builtin/metal/compute_kernel_metal.h"
#include <future>
#include <array>


using namespace std;
using namespace mtlpp;


namespace MLDB {
namespace RF {

EnvOption<bool> DEBUG_RF_METAL_KERNELS("DEBUG_RF_METAL_KERNELS", 0);

// Default of 5.5k allows 8 parallel workgroups for a 48k SM when accounting
// for 0.5k of local memory for the kernels.
// On Nvidia, with 32 registers/work item and 256 work items/workgroup
// (8 warps of 32 threads), we use 32 * 256 * 8 = 64k registers, which
// means full occupancy.
EnvOption<int, true> RF_METAL_LOCAL_BUCKET_MEM("RF_METAL_LOCAL_BUCKET_MEM", 5500);


namespace {

static struct RegisterKernels {

    RegisterKernels()
    {
        auto getLibrary = [] (MetalComputeContext & context) -> mtlpp::Library
        {
            auto compileLibrary = [&] () -> mtlpp::Library
            {
                std::string fileName = "mldb/plugins/jml/randomforest_kernels.metal";
                filter_istream stream(fileName);
                Utf8String source = /*"#line 1 \"" + fileName + "\"\n" +*/ stream.readAll();

                ns::Error error{ns::Handle()};
                CompileOptions compileOptions;
                //compileOptions.setPreprocessorMacro("WBITS","32");
                Library library = context.mtlDevice.NewLibrary(source.rawData(), compileOptions, &error);

                if (error) {
                    cerr << "Error compiling" << endl;
                    cerr << "domain: " << error.GetDomain().GetCStr() << endl;
                    cerr << "description: " << error.GetLocalizedDescription().GetCStr() << endl;
                    if (error.GetLocalizedFailureReason()) {
                        cerr << "reason: " << error.GetLocalizedFailureReason().GetCStr() << endl;
                    }
                }

                ExcAssert(library);

                return library;
            };

            static const std::string cacheKey = "randomforest_kernels";
            Library library = context.getCacheEntry(cacheKey, compileLibrary);
            return library;
        };
    
        //string options = "-cl-kernel-arg-info -cl-nv-maxrregcount=32 -cl-nv-verbose";// -cl-mad-enable -cl-fast-relaxed-math -cl-unsafe-math-optimizations -DFloat=" + type_name<Float>();

        auto createDoNothingKernel = [getLibrary] (MetalComputeContext& context) -> std::shared_ptr<MetalComputeKernel>
        {
            auto library = getLibrary(context);
            auto result = std::make_shared<MetalComputeKernel>(&context);
            result->kernelName = "doNothing";
            result->setComputeFunction(library, "doNothingKernel", {});
            return result;
        };

        registerMetalComputeKernel("doNothing", createDoNothingKernel);

        auto createDecodeRowsKernel = [getLibrary] (MetalComputeContext& context) -> std::shared_ptr<MetalComputeKernel>
        {
            auto library = getLibrary(context);
            auto result = std::make_shared<MetalComputeKernel>(&context);
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
            result->modifyGrid = [=] (std::vector<size_t> & grid, auto &)
            {
                ExcAssertEqual(grid.size(), 1);
                grid[0] = 4096;  // don't do one launch per row, the kernel will iterate
            };

            result->setComputeFunction(library, "decompressRowsKernel", { 256 });

            return result;
        };

        registerMetalComputeKernel("decodeRows", createDecodeRowsKernel);

        auto createTestFeatureKernel = [getLibrary] (MetalComputeContext& context) -> std::shared_ptr<MetalComputeKernel>
        {
            auto library = getLibrary(context);
            auto result = std::make_shared<MetalComputeKernel>(&context);
            result->kernelName = "testFeature";
            //result->device = ComputeDevice::host();
            result->addDimension("featureNum", "nf");
            result->addDimension("rowNum", "numRows");
            
            result->addParameter("decodedRows", "r", "f32[numRows]");
            result->addParameter("numRows", "r", "u32");
            result->addParameter("bucketData", "r", "u32[bucketDataLength]");
            result->addParameter("bucketDataOffsets", "r", "u32[nf + 1]");
            result->addParameter("bucketNumbers", "r", "u32[nf + 1]");
            result->addParameter("bucketEntryBits", "r", "u32[nf]");
            result->addParameter("featuresActive", "r", "u32[nf]");
            result->addParameter("partitionBuckets", "rw", "W32[numBuckets]");

            result->addTuneable("maxLocalBuckets", RF_METAL_LOCAL_BUCKET_MEM.get() / sizeof(W));
            result->addTuneable("threadsPerBlock", 256);
            result->addTuneable("blocksPerGrid", 64);

            result->addParameter("w", "w", "W[maxLocalBuckets]");
            result->addParameter("maxLocalBuckets", "r", "u32");

            result->setGridExpression("[nf,blocksPerGrid]", "[1,threadsPerBlock]");
            result->allowGridPadding();

            result->setComputeFunction(library, "testFeatureKernel", { 1, 256 } );
            return result;
        };

        registerMetalComputeKernel("testFeature", createTestFeatureKernel);

        auto createGetPartitionSplitsKernel = [getLibrary] (MetalComputeContext& context) -> std::shared_ptr<MetalComputeKernel>
        {
            auto library = getLibrary(context);
            auto result = std::make_shared<MetalComputeKernel>(&context);
            result->kernelName = "getPartitionSplits";
            //result->device = ComputeDevice::host();
            result->addDimension("f", "nf");
            result->addDimension("p", "np");

            result->addParameter("totalBuckets", "r", "u32");
            result->addParameter("numActivePartitions", "r", "u32");
            result->addParameter("bucketNumbers", "r", "u32[nf + 1]");
            result->addParameter("featuresActive", "r", "u32[nf]");
            result->addParameter("featureIsOrdinal", "r", "u32[nf]");
            result->addParameter("buckets", "r", "W32[totalBuckets * np]");
            result->addParameter("wAll", "r", "W32[np]");
            result->addParameter("featurePartitionSplitsOut", "w", "PartitionSplit[np * nf]");

            result->addTuneable("wLocalSize", RF_METAL_LOCAL_BUCKET_MEM.get() / sizeof(WIndexed));

            result->addParameter("wLocal", "w", "WIndexed[wLocalSize]");
            result->addParameter("wLocalSize", "r", "u32");

            result->setGridExpression("[1,nf,np]", "[64,1,1]");
            
            result->setComputeFunction(library, "getPartitionSplitsKernel", { 1, 1 });
            return result;
        };

        registerMetalComputeKernel("getPartitionSplits", createGetPartitionSplitsKernel);

        auto createBestPartitionSplitKernel = [getLibrary] (MetalComputeContext & context) -> std::shared_ptr<MetalComputeKernel>
        {
            auto library = getLibrary(context);
            auto result = std::make_shared<MetalComputeKernel>(&context);
            result->kernelName = "bestPartitionSplit";
            //result->device = ComputeDevice::host();
            result->addDimension("p", "np");
            result->addParameter("numFeatures", "r", "u32");
            result->addParameter("featuresActive", "r", "u32[numFeatures]");
            result->addParameter("featurePartitionSplits", "r", "PartitionSplit[np * numFeatures]");
            result->addParameter("partitionIndexes", "r", "PartitionIndex[npi]");
            result->addParameter("allPartitionSplitsOut", "w", "IndexedPartitionSplit[maxPartitions]");
            result->addParameter("partitionSplitsOffset", "r", "u32");
            result->addParameter("depth", "r", "u16");
            result->setGridExpression("[np]", "[1]");
            result->setComputeFunction(library, "bestPartitionSplitKernel", { 1 });
            return result;
        };

        registerMetalComputeKernel("bestPartitionSplit", createBestPartitionSplitKernel);

        auto createAssignPartitionNumbersKernel = [getLibrary] (MetalComputeContext & context) -> std::shared_ptr<MetalComputeKernel>
        {
            auto library = getLibrary(context);
            auto result = std::make_shared<MetalComputeKernel>(&context);
            result->kernelName = "assignPartitionNumbers";
            //result->device = ComputeDevice::host();
            result->addParameter("allPartitionSplits", "r", "IndexedPartitionSplit[np]");
            result->addParameter("partitionSplitsOffset", "r", "u32");
            result->addParameter("numActivePartitions", "r", "u32");
            result->addParameter("maxNumActivePartitions", "r", "u32");
            result->addParameter("partitionIndexesOut", "w", "PartitionIndex[maxActivePartitions]");
            result->addParameter("partitionInfoOut", "w", "PartitionInfo[numActivePartitions]");
            result->addParameter("smallSideIndexesOut", "w", "u8[maxActivePartitions]");
            result->addParameter("smallSideIndexToPartitionOut", "w", "u16[256]");
            result->addParameter("numActivePartitionsOut", "w", "u32[1]");
            result->setGridExpression("[1]", "[1]");
            result->setComputeFunction(library, "assignPartitionNumbersKernel", {});
            return result;
        };

        registerMetalComputeKernel("assignPartitionNumbers", createAssignPartitionNumbersKernel);

        auto createClearBucketsKernel = [getLibrary] (MetalComputeContext & context) -> std::shared_ptr<MetalComputeKernel>
        {
            auto library = getLibrary(context);
            auto result = std::make_shared<MetalComputeKernel>(&context);
            result->kernelName = "clearBuckets";
            //result->device = ComputeDevice::host();
            result->addDimension("p", "np");
            result->addDimension("b", "numActiveBuckets");
            result->addParameter("bucketsOut", "w", "W32[numActiveBuckets * np]");
            result->addParameter("wAllOut", "w", "W32[np]");
            result->addParameter("smallSideIndexes", "r", "u8[numActivePartitions]");
            result->addParameter("numActiveBuckets", "r", "u32");
            result->allowGridPadding();
            result->addTuneable("gridBlockSize", 64);
            result->setGridExpression("[np,divideToCover(numActiveBuckets,gridBlockSize)]", "[1,gridBlockSize]");
            result->setComputeFunction(library, "clearBucketsKernel", { 1, 64 });
            return result;
        };

        registerMetalComputeKernel("clearBuckets", createClearBucketsKernel);

        auto createUpdatePartitionNumbersKernel = [getLibrary] (MetalComputeContext & context) -> std::shared_ptr<MetalComputeKernel>
        {
            auto library = getLibrary(context);
            auto result = std::make_shared<MetalComputeKernel>(&context);
            result->kernelName = "updatePartitionNumbers";
            //result->device = ComputeDevice::host();
            result->addDimension("r", "numRows");

            result->addParameter("partitionSplitsOffset", "r", "u32");
            result->addParameter("partitions", "r", "RowPartitionInfo[numRows]");
            result->addParameter("directions", "w", "u8[numRows]");
            result->addParameter("numRows", "r", "u32");
            result->addParameter("allPartitionSplits", "r", "IndexedPartitionSplit[np + partitionSplitsOffset]");
            result->addParameter("partitionInfo", "r", "PartitionInfo[np]");
            result->addParameter("bucketData", "r", "u32[bucketDataLength]");
            result->addParameter("bucketDataOffsets", "r", "u32[nf + 1]");
            result->addParameter("bucketNumbers", "r", "u32[nf + 1]");
            result->addParameter("bucketEntryBits", "r", "u32[nf]");
            result->addParameter("featureIsOrdinal", "r", "u32[nf]");
            result->addParameter("depth", "r", "u16");
            result->addTuneable("threadsPerBlock", 256);
            result->addTuneable("blocksPerGrid", 128);
            result->allowGridPadding();
            result->setGridExpression("[blocksPerGrid]", "[threadsPerBlock]");
            result->setComputeFunction(library, "updatePartitionNumbersKernel", { 256 });
            return result;
        };

        registerMetalComputeKernel("updatePartitionNumbers", createUpdatePartitionNumbersKernel);

        auto createUpdateBucketsKernel = [getLibrary] (MetalComputeContext & context) -> std::shared_ptr<MetalComputeKernel>
        {
            auto library = getLibrary(context);
            auto result = std::make_shared<MetalComputeKernel>(&context);
            result->kernelName = "updateBuckets";
            result->device = ComputeDevice::host();
            result->addDimension("r", "numRows");
            result->addDimension("f_plus_1", "nf_plus_1");
            result->addParameter("numActiveBuckets", "r", "u32");
            result->addParameter("numActivePartitions", "r", "u32");
            result->addParameter("partitions", "r", "RowPartitionInfo[numRows]");
            result->addParameter("directions", "r", "u8[numRows]");
            result->addParameter("buckets", "w", "W32[numActiveBuckets * numActivePartitions]");
            result->addParameter("wAll", "w", "W32[numActivePartitions]");
            result->addParameter("smallSideIndexes", "r", "u8[numActivePartitions]");
            result->addParameter("smallSideIndexToPartition", "r", "u16[256]");
            result->addParameter("decodedRows", "r", "f32[nr]");
            result->addParameter("numRows", "r", "u32");
            result->addParameter("bucketData", "r", "u32[bucketDataLength]");
            result->addParameter("bucketDataOffsets", "r", "u32[nf + 1]");
            result->addParameter("bucketNumbers", "r", "u32[nf + 1]");
            result->addParameter("bucketEntryBits", "r", "u32[nf]");
            result->addParameter("featuresActive", "r", "u32[numFeatures]");
            result->addParameter("featureIsOrdinal", "r", "u32[nf]");
            result->addTuneable("maxLocalBuckets", RF_METAL_LOCAL_BUCKET_MEM.get() / sizeof(W));
            result->addTuneable("threadsPerBlock", 256);
            result->addTuneable("blocksPerGrid", 64);
            result->addParameter("wLocal", "w", "W[maxLocalBuckets]");
            result->addParameter("maxLocalBuckets", "r", "u32");
            result->addConstraint("nf_plus_1", "==", "nf + 1", "help the solver");
            result->addConstraint("nf", "==", "nf_plus_1 - 1", "help the solver");
            result->setGridExpression("[blocksPerGrid,nf]", "[threadsPerBlock,1]");
            result->allowGridPadding();
            result->setComputeFunction(library, "updateBucketsKernel", { 256, 1 });
            return result;
        };

        registerMetalComputeKernel("updateBuckets", createUpdateBucketsKernel);

        auto createFixupBucketsKernel = [getLibrary] (MetalComputeContext & context) -> std::shared_ptr<MetalComputeKernel>
        {
            auto library = getLibrary(context);
            auto result = std::make_shared<MetalComputeKernel>(&context);
            result->kernelName = "fixupBuckets";
            result->device = ComputeDevice::host();
            result->addDimension("partition", "np");
            result->addDimension("bucket", "numActiveBuckets");
            result->addParameter("buckets", "w", "W32[numActiveBuckets * newNumPartitions]");
            result->addParameter("wAll", "w", "W32[newNumPartitions]");
            result->addParameter("partitionInfo", "r", "PartitionInfo[np]");
            result->addParameter("numActiveBuckets", "r", "u32");
            result->addTuneable("gridBlockSize", 64);
            result->allowGridPadding();
            result->setGridExpression("[np,divideToCover(numActiveBuckets,gridBlockSize)]", "[1,gridBlockSize]");
            result->setComputeFunction(library, "fixupBucketsKernel", { 1, 64 });
            return result;
        };

        registerMetalComputeKernel("fixupBuckets", createFixupBucketsKernel);
    }

} registerKernels;
} // file scope

} // namespace RF
} // namespace MLDB
