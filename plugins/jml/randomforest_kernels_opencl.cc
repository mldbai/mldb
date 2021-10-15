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
#include "mldb/base/scope.h"
#include "mldb/arch/vm.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/tuple_description.h"
#include "mldb/types/pair_description.h"
#include "mldb/types/map_description.h"
#include "mldb/builtin/opencl/compute_kernel_opencl.h"
#include <future>
#include <array>


#if OPENCL_ENABLED
#  define CL_TARGET_OPENCL_VERSION 220
#  include "CL/cl.h"
#  include "mldb/builtin/opencl/opencl_types.h"


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

struct OpenCLMemoryManager {
    OpenCLMemoryManager(OpenCLContext context, OpenCLDevice defaultDevice)
        : context(std::move(context)), defaultDevice(std::move(defaultDevice))
    {
    }

    OpenCLContext context;
    OpenCLDevice defaultDevice;
    OpenCLCommandQueue queue;

    template<typename T>
    OpenCLArrayT<T> createArray(size_t size)
    {
        return context.createBuffer(CL_MEM_READ_WRITE, sizeof(T) * size);
    }

    template<typename T>
    OpenCLArrayT<T> createInitializedArray(const std::span<T> & cpuData)
    {
        return context.createBuffer(cpuData);
    }

    template<typename T, size_t N>
    OpenCLArrayT<T> createInitializedArray(const std::array<T, N> & cpuData)
    {
        return context.createBuffer(CL_MEM_READ_ONLY, cpuData.data(), cpuData.size() * sizeof(T));
    }

    template<typename T>
    OpenCLArrayT<T> mapArray(const FrozenMemoryRegionT<T> & region)
    {
        return context.createBuffer(CL_MEM_READ_ONLY, region.data(), region.memusage());
    }

    template<typename T>
    OpenCLArrayT<T> manageMemoryRegion(const std::vector<T> & mem)
    {
        return mapArray(std::span<const T>(mem));
    }

    template<typename T, size_t N>
    OpenCLArrayT<T> mapArray(const std::span<const T, N> & region)
    {
        return context.createBuffer(CL_MEM_READ_ONLY, region.data(), region.size() * sizeof(T));
    }

    template<typename T, size_t N>
    OpenCLArrayT<T> mapArray(const std::span<T, N> & region)
    {
        return context.createBuffer(CL_MEM_READ_ONLY, region.data(), region.size() * sizeof(T));
    }

    template<typename T, size_t N>
    OpenCLArrayT<T> mapArray(const std::array<T, N> & region)
    {
        return context.createBuffer(CL_MEM_READ_ONLY, region.data(), region.size() * sizeof(T));
    }

    template<typename T, size_t N>
    OpenCLArrayT<T> mapArray(std::array<T, N> & region)
    {
        return context.createBuffer(CL_MEM_READ_WRITE, region.data(), region.size() * sizeof(T));
    }
    
    template<typename T>
    std::future<std::span<T>> toCpu(OpenCLArrayT<T> & array);

    template<typename T>
    std::future<void> toCpu(OpenCLArrayT<T> & array, const std::span<T> & output);

    template<typename T>
    std::span<T> toCpuSync(OpenCLArrayT<T> & array)
    {
        return mapSpan<T>(array);
    }

    template<typename T>
    void toCpuSync(OpenCLArrayT<T> & array, const std::span<T> & output)
    {
        OpenCLEvent mapEvent;

        mapEvent = queue.enqueueReadBuffer
                (array, 0, /* offset in bytes */
                 output.size() * sizeof(T), /* length in bytes */
                 output.data(), {} /* dependencies */);
    
        mapEvent.waitUntilFinished();
    }

    template<typename T>
    void updateCpuSync(OpenCLArrayT<T> & array);

    template<typename T>
    std::vector<T> mapVector(const OpenCLMemObject & mem, ssize_t length = -1,
                             std::vector<OpenCLEvent> deps = {},
                             size_t offset = 0)
    {
        if (length == -1) {
            size_t sz = mem.size();
            ExcAssertEqual(sz % sizeof(T), 0);
            length = mem.size() / sizeof(T);
        }

        ExcAssertLessEqual(offset, length);
        length -= offset;

        OpenCLEvent mapEvent;
        std::shared_ptr<const void> mappedMemory;

        std::tie(mappedMemory, mapEvent)
            = queue.enqueueMapBuffer
                (mem, CL_MAP_READ,
                offset * sizeof(T),
                length * sizeof(T),
                deps);
    
        mapEvent.waitUntilFinished();

        const T * castMemory = reinterpret_cast<const T *>(mappedMemory.get());
        
        return std::vector<T>(castMemory, castMemory + length);
    }

    template<typename T>
    std::span<T> mapSpan(const OpenCLMemObject & mem, ssize_t length = -1,
                         std::vector<OpenCLEvent> deps = {},
                         size_t offset = 0)
    {
        if (length == -1) {
            size_t sz = mem.size();
            ExcAssertEqual(sz % sizeof(T), 0);
            length = mem.size() / sizeof(T);
        }

        ExcAssertLessEqual(offset, length);
        length -= offset;

        OpenCLEvent mapEvent;
        std::shared_ptr<const void> mappedMemory;

        std::tie(mappedMemory, mapEvent)
            = queue.enqueueMapBuffer
                (mem, CL_MAP_READ,
                offset * sizeof(T),
                length * sizeof(T),
                deps);
    
        mapEvent.waitUntilFinished();

        T * castMemory = const_cast<T *>(reinterpret_cast<const T *>(mappedMemory.get()));
        
        pinnedMemory.push_back(mappedMemory);

        return std::span<T>(castMemory, length);
    }

    std::vector<std::shared_ptr<const void>> pinnedMemory;
};

std::vector<OpenCLDevice>
getOpenCLDevices()
{
    std::vector<OpenCLPlatform> platforms
        = getOpenCLPlatforms();

    //cerr << platforms.size() << " OpenCL platforms available" << endl;

    for (size_t i = 0;  i < platforms.size();  ++i) {

        //cerr << "platform" << jsonEncode(platforms[i].getPlatformInfo())
        //     << endl;

        const OpenCLPlatform & platform
            = platforms[i];
        
        std::vector<OpenCLDevice> devices
            = platform.getDevices();

        //for (auto & d: platform.getDevices()) {
        //    cerr << jsonEncode(d.getDeviceInfo()) << endl;
        //}

        if (devices.empty())
            continue;

        devices.resize(1);

        return devices;
    }

    throw AnnotatedException(500, "No OpenCL platform found");
}


OpenCLProgram getTestFeatureProgramOpenCL(const OpenCLDevice & device)
{
    std::vector<OpenCLDevice> devices{device};
    OpenCLContext context(devices);
    
    Bitset<OpenCLCommandQueueProperties> queueProperties
        = { OpenCLCommandQueueProperties::PROFILING_ENABLE };
    if (!RF_OPENCL_SYNCHRONOUS_LAUNCH) {
        queueProperties.set
            (OpenCLCommandQueueProperties::OUT_OF_ORDER_EXEC_MODE_ENABLE);
    }
    
    auto queue = context.createCommandQueue(device, queueProperties);

    std::string fileName = "mldb/plugins/jml/randomforest_kernels.cl";
    filter_istream stream(fileName);
    Utf8String source = "#line 1 \"" + fileName + "\"\n" + stream.readAll();
    cerr << source << endl;

    
    OpenCLProgram program = context.createProgram(source);

    //string options = "-cl-kernel-arg-info -cl-nv-maxrregcount=32 -cl-nv-verbose";// -cl-mad-enable -cl-fast-relaxed-math -cl-unsafe-math-optimizations -DFloat=" + type_name<Float>();
    string options = "-cl-kernel-arg-info -DWBITS=32 -DW=W32";

    // Build for all devices
    auto buildInfo = program.build({device}, options);
    
    cerr << jsonEncode(buildInfo[0]) << endl;

    //std::string binary = program.getBinary();
    //filter_ostream stream2("randomforest_kernels.obj");
    //stream2 << binary;
    
    return program;
}

#if 0
struct Kernel {

    std::shared_ptr<void> registerKernel(const Utf8String & name);

    struct Register {

    };
};

struct TestFeatureKernel: public Kernel {
    TestFeatureKernel()
    {
        addParameter<Array<uint32_t>>("rowData");
        addLength("rowData", "rowDataLength");
        addParameter<uint16_t>("totalBits");
        addParameter<uint16_t>("weightBits");
        addParameter<uint16_t>("exampleNumBits");
        addParameter<uint32_t>("numRows");
        addParameter<Array<uint32_t>>("bucketData");
        addParameter<Array<uint32_t>>("bucketDataOffsets");
        addParameter<Array<uint32_t>>("bucketNumbers");
        addParameter<Array<uint16_t>>("bucketEntryBits");
        addParameter<Array<uint32_t>>("bucketEntryBits");
        addParameter<WeightFormat>("weightFormat");
        addParameter<float>("weightMultiplier");
        addParameter<Array<uint32_t>>("weightData");
        addParameter<Array<uint32_t>>("featuresActive");
        addParameter<uint32_t>("numBuckets");
        addOutput<Array<W>>("wOut");
        addOutput<Array<uint32_t>>("minMaxOut");
    }
};
#endif

std::pair<bool, int>
testFeatureKernelOpencl(Rows::RowIterator rowIterator,
                        size_t numRows,
                        const BucketList & buckets,
                        W * w /* buckets.numBuckets entries */)
{
    //numRows = 10;

    static std::mutex mutex;
    std::unique_lock<std::mutex> guard(mutex);
    
    static auto devices = getOpenCLDevices();
    if (devices.empty())
        throw AnnotatedException(500, "No OpenCL capable devices found");
    static auto device = devices[0];
    static OpenCLProgram program = getTestFeatureProgramOpenCL(device);

    OpenCLContext context = program.getContext();
    
    OpenCLKernel kernel
        = program.createKernel("testFeatureKernel");

    //cerr << "row data of " << rowIterator.owner->rowData.memusage()
    //     << " bytes and " << numRows << " rows" << endl;
    
    OpenCLMemoryManager mm(context, devices[0]);

    std::array<int, 2> minMax = { INT_MAX, INT_MIN };
    
    auto clRowData = mm.mapArray(rowIterator.owner->rowData);
    auto clBucketData = mm.mapArray(buckets.storage);
    auto clWeightData = mm.mapArray(rowIterator.owner->weightEncoder.weightFormatTable);
    auto clWOut = mm.createArray<W>(buckets.numBuckets);
    auto clMinMaxOut = mm.createInitializedArray(minMax);

#if 0
    OpenCLMemObject clRowData
        = context.createBuffer(0,
                               (const void *)rowIterator.owner->rowData.data(),
                               rowIterator.owner->rowData.memusage());

    OpenCLMemObject clBucketData
        = context.createBuffer(0,
                               (const void *)buckets.storage.data(),
                               buckets.storage.memusage());

    OpenCLMemObject clWeightData
        = context.createBuffer(CL_MEM_READ_ONLY, 4);

    if (rowIterator.owner->weightEncoder.weightFormat == WF_TABLE) {
        clWeightData = context.createBuffer
            (0,
             (const void *)rowIterator.owner->weightEncoder.weightFormatTable.data(),
             rowIterator.owner->weightEncoder.weightFormatTable.memusage());
    }

    OpenCLMemObject clWOut
        = context.createBuffer(CL_MEM_READ_WRITE,
                               w,
                               sizeof(W) * buckets.numBuckets);


    OpenCLMemObject clMinMaxOut
        = context.createBuffer(CL_MEM_READ_WRITE,
                               minMax,
                               sizeof(minMax[0]) * 2);
#endif

    size_t workGroupSize = 65536;
        
    uint32_t numRowsPerWorkItem
        = (numRows + workGroupSize - 1) / workGroupSize;

    const std::vector<int32_t> clBucketDataOffsets = { 0, (int32_t)buckets.storage.length() };
    const std::vector<int32_t> clBucketNumbers = { 0, buckets.numBuckets };
    const std::vector<int32_t> clBucketEntryBits = { buckets.entryBits };
    const std::vector<int32_t> clFeaturesActive = { true };
    
    kernel.bind(numRowsPerWorkItem,
                clRowData,
                (uint32_t)rowIterator.owner->rowData.length(),
                rowIterator.totalBits,
                rowIterator.owner->weightEncoder.weightBits,
                rowIterator.owner->exampleNumBits,
                (uint32_t)numRows,
                clBucketData,
                clBucketDataOffsets,
                clBucketNumbers,
                clBucketEntryBits,
                rowIterator.owner->weightEncoder.weightFormat,
                rowIterator.owner->weightEncoder.weightMultiplier,
                clWeightData,
                clFeaturesActive,
                LocalArray<W>(buckets.numBuckets),
                buckets.numBuckets,
                clWOut,
                clMinMaxOut);
    
    auto queue = context.createCommandQueue
        (device,
         OpenCLCommandQueueProperties::PROFILING_ENABLE);

    Date before = Date::now();
    
    OpenCLEvent runKernel
        = queue.launch(kernel,
                       { workGroupSize, 1 },
                       { 256, 1 });

    queue.flush();
    
    mm.toCpuSync(clWOut, std::span<W>(w, buckets.numBuckets));
    std::span<int> minMaxOut = mm.toCpuSync(clMinMaxOut);

#if 0    

    OpenCLEvent transfer
        = queue.enqueueReadBuffer(clWOut, 0 /* offset */,
                                  sizeof(W) * buckets.numBuckets /* length */,
                                  w,
                                  runKernel /* before */);

    OpenCLEvent minMaxTransfer
        = queue.enqueueReadBuffer(clMinMaxOut, 0 /* offset */,
                                  sizeof(minMax[0]) * 2 /* length */,
                                  minMax,
                                  runKernel);

    queue.flush();
    
    queue.wait({transfer, minMaxTransfer});
    transfer.assertSuccess();
    minMaxTransfer.assertSuccess();

#endif

    Date after = Date::now();

    cerr << "gpu took " << after.secondsSince(before) * 1000 << "ms" << endl;
    
#if 0    
    for (size_t i = 0;  i < 10 && i < buckets.numBuckets;  ++i) {
        cerr << "w[" << i << "] = (" << w[i].v[0] << " " << w[i].v[1] << ")"
             << endl;
    }
#endif

    bool active = (minMaxOut[0] != minMaxOut[1]);
    int maxBucket = minMaxOut[1];
    
    if (true) {

        std::vector<W> wCpu(buckets.numBuckets);

        bool cpuActive;
        int cpuMaxBucket;

        Date before = Date::now();
        
        std::tie(cpuActive, cpuMaxBucket)
            = testFeatureKernelCpu(rowIterator,
                                   numRows,
                                   buckets,
                                   wCpu.data());

        Date after = Date::now();
        
        cerr << "cpu took " << after.secondsSince(before) * 1000 << "ms" << endl;

        W totalGpu, totalCpu;
        for (size_t i = 0;  i < buckets.numBuckets;  ++i) {
            totalGpu += w[i];
            totalCpu += wCpu[i];
            if (w[i] != wCpu[i]) {
                cerr << "error on w " << i << ": gpu = ("
                     << w[i].v[0] << "," << w[i].v[1] << ") = "
                     << w[i].v[0] + w[i].v[1] << ", cpu = ("
                     << wCpu[i].v[0] << "," << wCpu[i].v[1] << ") = "
                     << wCpu[i].v[0] + wCpu[i].v[1] << endl;
            }
        }

        if (totalGpu != totalCpu) {
            cerr << "totals differ: "
                 << ": gpu = ("
                 << totalGpu.v[0] << "," << totalGpu.v[1] << ") = "
                 << totalGpu.v[0] + totalGpu.v[1] << ", cpu = ("
                 << totalCpu.v[0] << "," << totalCpu.v[1] << ") = "
                 << totalCpu.v[0] + totalCpu.v[1] << endl;                    
                    
        }
        
        if (cpuActive != active) {
            cerr << "error: active CPU = " << cpuActive << " != gpu " << active << endl;
        }
        if (cpuMaxBucket != maxBucket) {
            cerr << "error: max bucket CPU = " << cpuMaxBucket
                 << " != gpu " << maxBucket << endl;
        }
    }

#if 0    
    for (size_t i = 0;  i < 10 && i < buckets.numBuckets;  ++i) {
        cerr << "real w[" << i << "] = (" << w[i].v[0] << " " << w[i].v[1] << ")"
             << endl;
    }
#endif
    
#if 0    
    auto profile = runKernel.getProfilingInfo();
    
    cerr << "time to queue (ns): " << profile.submit - profile.queued << endl;
    cerr << "time to start (ns): " << profile.start - profile.submit << endl;
    cerr << "time to run (ns):   " << profile.end - profile.start << endl;
    
    cerr << jsonEncode(profile);
#endif
    
    return { active, maxBucket };
}
#endif

struct OpenCLKernelContext {
    OpenCLContext context;
    std::vector<OpenCLDevice> devices;
    OpenCLProgram program;
    OpenCLCommandQueue queue;
};

OpenCLKernelContext getKernelContext()
{
    static auto devices = getOpenCLDevices();
    if (devices.empty())
        throw AnnotatedException(500, "No OpenCL capable devices found");
    static auto device = devices[0];
    static const OpenCLProgram program = getTestFeatureProgramOpenCL(device);

    OpenCLContext context = program.getContext();

    auto queue = context.createCommandQueue
        (device,
         OpenCLCommandQueueProperties::PROFILING_ENABLE);

    
    OpenCLKernelContext result;
    result.program = program;
    result.context = program.getContext();
    result.devices = { device };
    result.queue = queue;
    return result;
}

std::tuple<double, int, int, W, W, std::vector<uint8_t> >
testAllOpenCL(int depth,
              const std::span<const Feature> & features,
              const Rows & rows,
              FrozenMemoryRegionT<uint32_t> bucketMemory_)
{
    // We have no impurity in our bucket.  Time to stop
    if (rows.wAll[0] == 0 || rows.wAll[1] == 0) {
        return std::make_tuple(1.0, -1, -1, rows.wAll, W(),
                               std::vector<uint8_t>(features.size(), false));
    }

    std::vector<uint8_t> newActive;
    for (auto & f: features) {
        newActive.push_back(f.active);
    }
    
#if 1
    static constexpr int PARALLELISM = 8;
    
    static std::atomic<int> numRunning(0);
    static std::mutex mutex;
    static std::atomic<int> numWaiting(0);

    ++numWaiting;
    static std::condition_variable cv;
    std::unique_lock<std::mutex> guard(mutex);

    
    cv.wait(guard, [&] () { return numRunning < PARALLELISM; });

    --numWaiting;
    numRunning += 1;

    cerr << "started; numRunning now " << numRunning << " numWaiting = "
         << numWaiting << endl;
    
    guard.unlock();

    auto onExit = ScopeExit([&] () noexcept
                            { guard.lock();
                              numRunning -= 1;
                              cv.notify_one();
                              //cerr << "finished; numRunning now "
                              //     << numRunning << " numWaiting = "
                              //       << numWaiting << endl;
                            });
#elif 1
    static std::mutex mutex;
    std::unique_lock<std::mutex> guard(mutex);
#endif
    
    unsigned nf = features.size();

    size_t activeFeatures = 0;
    size_t totalBuckets = 0;

    static constexpr size_t MAX_LOCAL_BUCKETS = 512;  // 8192 bytes
    
    // Maximum number of buckets
    size_t maxBuckets = 0;

    // Maximum number of buckets for all with less than MAX_LOCAL_BUCKETS
    size_t maxLocalBuckets = 0;
    
    uint32_t lastBucketDataOffset = 0;
    std::vector<uint32_t> bucketMemoryOffsets;
    std::vector<uint32_t> bucketEntryBits;
    std::vector<uint32_t> bucketNumbers(1, 0);
    std::vector<uint32_t> featuresActive;
    
    for (int i = 0;  i < nf;  ++i) {
        const BucketList & buckets = features[i].buckets;

        bucketEntryBits.push_back(buckets.entryBits);

        featuresActive.push_back(features[i].active);
        
        if (features[i].active) {
            ExcAssertGreaterEqual((void *)buckets.storage.data(),
                                  (void *)bucketMemory_.data());
        
            uint32_t offset
                = buckets.storage.data()
                - bucketMemory_.data();

            cerr << "feature = " << i << " offset = " << offset << endl;
            bucketMemoryOffsets.push_back(offset);
            lastBucketDataOffset = offset + bucketMemory_.length();
            
            ++activeFeatures;
            totalBuckets += features[i].buckets.numBuckets;
            maxBuckets = std::max<size_t>(maxBuckets,
                                          features[i].buckets.numBuckets);
            if (features[i].buckets.numBuckets <= MAX_LOCAL_BUCKETS) {
                maxLocalBuckets
                    = std::max<size_t>(maxLocalBuckets,
                                       features[i].buckets.numBuckets);
            }
        }
        else {
            bucketMemoryOffsets.push_back(lastBucketDataOffset);
        }

        bucketNumbers.push_back(totalBuckets);

        //cerr << "feature " << i << " buckets from " << bucketNumbers[i]
        //     << " to " << bucketNumbers[i + 1] << " numBuckets "
        //     << bucketNumbers[i + 1] - bucketNumbers[i] << endl;
    }

    bucketMemoryOffsets.push_back(lastBucketDataOffset);
    //cerr << "bucketNumbers are " << jsonEncode(bucketNumbers) << endl;
    
    //cerr << "doing " << rows.rowCount() << " rows with "
    //     << activeFeatures << " active features and "
    //     << totalBuckets << " total buckets" << endl;

    ExcAssertEqual(bucketMemoryOffsets.size(), nf + 1);
    ExcAssertEqual(bucketEntryBits.size(), nf);
    ExcAssertEqual(bucketNumbers.size(), nf + 1);
    ExcAssertEqual(featuresActive.size(), nf);

    
    //cerr << "bucketMemoryOffsets = " << jsonEncodeStr(bucketMemoryOffsets)
    //     << endl;
    //for (auto & b: bucketMemoryOffsets) {
    //    int b2 = b;
    //    if (b2 < 0) {
    //        cerr << "b = " << b << " b2 = " << b2 << endl;
    //    }
    //}
    
    // Shouldn't happen, except during development / testing
    if (totalBuckets == 0 || activeFeatures == 0) {
        return std::make_tuple(1.0, -1, -1, rows.wAll, W(),
                               std::vector<uint8_t>(features.size(), false));
    }
    
    OpenCLKernelContext kernelContext = getKernelContext();
    
    OpenCLContext & context = kernelContext.context;

    //cerr << "rows.rowData.data() = " << rows.rowData.data() << endl;
    //cerr << "bucketMemory_.data() = " << bucketMemory_.data() << endl;

    Date beforeTransfer = Date::now();
    
    // Transfer rows, weights on the GPU; these are shared across all features
    OpenCLMemObject clRowData
        = context.createBuffer(0,
                               (const void *)rows.rowData.data(),
                               roundUpToPageSize(rows.rowData.memusage()));

    OpenCLMemObject clBucketData
        = context.createBuffer(0,
                               (const void *)bucketMemory_.data(),
                               roundUpToPageSize(bucketMemory_.memusage()));

    OpenCLMemObject clBucketDataOffsets
        = context.createBuffer(bucketMemoryOffsets);

    
    std::vector<W> allW(totalBuckets);
    std::vector<std::array<int32_t, 2> > allMinMax(nf, { INT_MAX, INT_MIN });

    // TODO: fill don't transfer
    OpenCLMemObject clAllW
        = context.createBuffer(CL_MEM_READ_WRITE,
                               allW.data(),
                               sizeof(W) * allW.size());

    // TODO: fill don't transfer
    OpenCLMemObject clAllMinMax
        = context.createBuffer(CL_MEM_READ_WRITE,
                               allMinMax.data(),
                               sizeof(allMinMax[0]) * allMinMax.size());

    OpenCLMemObject clBucketNumbers
        = context.createBuffer(bucketNumbers);
    
    OpenCLMemObject clBucketEntryBits
        = context.createBuffer(bucketEntryBits);

    OpenCLMemObject clFeaturesActive
        = context.createBuffer(featuresActive);
    
    OpenCLMemObject clWeightData
        = context.createBuffer(CL_MEM_READ_ONLY, 4);
    
    if (rows.weightEncoder.weightFormat == WF_TABLE) {
        clWeightData = context.createBuffer
            (0,
             (const void *)rows.weightEncoder.weightFormatTable.data(),
             rows.weightEncoder.weightFormatTable.memusage());
    }

    double elapsedTransfer = Date::now().secondsSince(beforeTransfer);
    
    cerr << "sending over " << rows.rowData.memusage() / 1000000.0
         << "mb of rows and " << bucketMemory_.memusage() / 1000000.0
         << "mb of buckets in " << elapsedTransfer * 1000.0 << " ms at "
         << (rows.rowData.memusage() + bucketMemory_.memusage()) / 1000000.0 / elapsedTransfer
         << "mb/s" << endl;

    //uint64_t maxBit = rows.rowData.memusage() * 8;
    //cerr << "row bit maximum is " << log2(maxBit) << endl;
    
    Date start = Date::now();
    
    size_t numRows = rows.rowCount();

    //cerr << "total offset = " << bucketMemory_.memusage() / 4 << endl;

    Date beforeKernel = Date::now();
    
    OpenCLKernel kernel
        = kernelContext.program.createKernel("testFeatureKernel");

    Date afterKernel = Date::now();

    cerr << "kernel took " << afterKernel.secondsSince(beforeKernel) * 1000
         << "ms" << endl;
    
    //OpenCLKernelWorkgroupInfo info(kernel, context.getDevices()[0]);
    
    //cerr << jsonEncodeStr(info) << endl;
    
    size_t workGroupSize = 65536;
        
    size_t numRowsPerWorkItem
        = (numRows + workGroupSize - 1) / workGroupSize;

    kernel.bind((uint32_t)numRowsPerWorkItem,
                clRowData,
                (uint32_t)rows.rowData.length(),
                rows.totalBits,
                rows.weightEncoder.weightBits,
                rows.exampleNumBits,
                (uint32_t)numRows,
                clBucketData,
                clBucketDataOffsets,
                clBucketNumbers,
                clBucketEntryBits,
                rows.weightEncoder.weightFormat,
                rows.weightEncoder.weightMultiplier,
                clWeightData,
                clFeaturesActive,
                LocalArray<W>(maxLocalBuckets),
                (uint32_t)maxLocalBuckets,
                clAllW,
                clAllMinMax);

    cerr << "kernel bind took " << afterKernel.secondsUntil(Date::now()) * 1000
         << "ms" << endl;
    
    auto queue = kernelContext.context.createCommandQueue
        (kernelContext.devices[0],
         OpenCLCommandQueueProperties::PROFILING_ENABLE);

    //auto & queue = kernelContext.queue;

    Date beforeLaunch = Date::now();
    
    OpenCLEvent runKernel
        = queue.launch(kernel,
                       { workGroupSize, nf },
                       { 256, 1 });

    cerr << "launch took " << Date::now().secondsSince(beforeLaunch) * 1000
         << "ms" << endl;
    
    queue.flush();
    
    //queue.finish();

    OpenCLEvent wTransfer
        = queue.enqueueReadBuffer
            (clAllW, 0 /* offset */,
             sizeof(W) * allW.size() /* length */,
             allW.data(),
             runKernel /* before */);
    
    //queue.finish();

    OpenCLEvent minMaxTransfer
        = queue.enqueueReadBuffer
            (clAllMinMax, 0 /* offset */,
             sizeof(allMinMax[0]) * allMinMax.size() /* length */,
             allMinMax.data(),
             runKernel);
    
    //queue.finish();

    OpenCLEvent doneFeature = queue.enqueueMarker
        ({ runKernel, wTransfer, minMaxTransfer });
    
    //queue.finish();

    Date beforeWait = Date::now();
    
    doneFeature.waitUntilFinished();

    runKernel.assertSuccess();

    cerr << "wait wall time is " << Date::now().secondsSince(beforeWait) * 1000
         << "ms" << endl;
    
    cerr << "wall time is " << Date::now().secondsSince(afterKernel)* 1000
         << "ms" << endl;

    auto profile = runKernel.getProfilingInfo();
    
    uint64_t timeTakenNs = profile.end - profile.start;
    
    cerr << "kernel " << jsonEncodeStr(profile.relative()) << endl;
    cerr << "w " << jsonEncodeStr(wTransfer.getProfilingInfo().relative())
         << endl;
    cerr << "minmax " << jsonEncodeStr(minMaxTransfer.getProfilingInfo().relative())
         << endl;
    cerr << "did " << numRows << " rows in "
         << timeTakenNs / 1000000.0 << " ms at "
         << timeTakenNs * 1.0 / numRows / activeFeatures << " ns/row-feature"
         << endl;
    
    double bestScore = INFINITY;
    int bestFeature = -1;
    int bestSplit = -1;
        
    W bestLeft;
    W bestRight;

    // Reduction over the best split that comes in feature by feature;
    // we find the best global split score and store it.  This is done
    // in order to be sure that we deterministically pick the right
    // one.
    auto findBest = [&] (int feature,
                         const std::tuple<double, int, W, W> & val)
        {
            //cerr << "GPU: feature " << feature << " score "
            //     << std::get<0>(val) << " split " << std::get<1>(val)
            //     << " left " << jsonEncodeStr(std::get<2>(val))
            //     << " right " << jsonEncodeStr(std::get<3>(val))
            //     << endl;
                                       
            double score = std::get<0>(val);
            if (score < bestScore
                || (score == bestScore && feature < bestFeature)) {
                bestFeature = feature;
                std::tie(bestScore, bestSplit, bestLeft, bestRight) = val;
            }
        };
    


    for (int i = 0;  i < nf;  ++i) {

        if (!features[i].active)
            continue;

        const W * w = allW.data() + bucketNumbers[i];
        const std::array<int, 2> & minMax = allMinMax[i];

#if 0        
        auto profile = featureEvents[i].runKernel.getProfilingInfo();

        uint64_t timeTakenNs = profile.end - profile.start;
        
        cerr << jsonEncodeStr(profile.relative()) << endl;
        cerr << "did " << numRows << " rows on "
             << features[i].buckets.numBuckets << " buckets in "
             << timeTakenNs / 1000000.0 << " ms at "
             << timeTakenNs * 1.0 / numRows << " ns/row" << endl;
#endif

        //cerr << "feature " << i << " numBuckets "
        //     << features[i].buckets.numBuckets
        //     << " min " << minMax[0] << " max " << minMax[1] << endl;

        if (minMax[1] >= features[i].buckets.numBuckets) {
            cerr << "***** error minMax: " << minMax[1] << " numBuckets "
                 << features[i].buckets.numBuckets << endl;
            abort();
        }
        
        bool active = (minMax[0] != minMax[1]);
        int maxBucket = minMax[1];

        ExcAssertLess(minMax[1], features[i].buckets.numBuckets);
        ExcAssertGreaterEqual(minMax[1], 0);
        
        if (active) {
            findBest(i, chooseSplitKernel(w, maxBucket, features[i].ordinal,
                                          rows.wAll));
        }
        else {
            newActive[i] = false;
        }
    };

    Date finished = Date::now();

    double elapsed = finished.secondsSince(start);

    cerr << "did " << rows.rowCount() << " rows over " << activeFeatures
         << " features in " << elapsed * 1000.0 << " ms at " << elapsed * 1000000000.0 / rows.rowCount() / activeFeatures << " ns/row-feature" << endl;
    cerr << endl;
    
    //cerr << rows.rowCount() << " rows, " << nf << " features";

    //parallelMap(0, nf, doFeature);
    
    //for (int i = 0;  i < nf;  ++i) {
    //    doFeature(i);
        //cerr << " " << i;
    //}

    //cerr << endl;

    //cerr << "rows has " << rowData.referenceCount() << " references" << endl;

#if 0    
    guard.lock();


    numRunning -= 1;

    cerr << "finished; numRunning now " << numRunning << endl;

    cv.notify_one();
#endif
    
    return std::make_tuple(bestScore, bestFeature, bestSplit,
                           bestLeft, bestRight, newActive);
}

#if 0

ML::Tree::Ptr
trainPartitionedEndToEndOpenCL(int depth, int maxDepth,
                               ML::Tree & tree,
                               MappedSerializer & serializer,
                               const Rows & rows,
                               const std::span<const Feature> & features,
                               FrozenMemoryRegionT<uint32_t> bucketMemory,
                               const DatasetFeatureSpace & fs)
{
    const bool debugKernelOutput = DEBUG_RF_OPENCL_KERNELS;

    
    // First, figure out the memory requirements.  This means sizing all
    // kinds of things so that we can make our allocations statically.

    // How much total memory was allocated?
    uint64_t totalGpuAllocation = 0;

    // How many rows in this partition?
    size_t numRows = rows.rowCount();

    // How many features?
    size_t nf = features.size();
    
    // Which of our features do we need to consider?  This allows us to
    // avoid sizing things too large for the number of features that are
    // actually active.
    std::vector<int> activeFeatures;

    // How many iterations can we run for?  This may be reduced if the
    // memory is not available to run the full width
    int numIterations = std::min(16, maxDepth - depth);

    // How many partitions will we have at our widest?
    int maxPartitionCount = 1 << numIterations;
    
    size_t rowCount = rows.rowCount();
    
    // How many buckets do we keep in shared memory?  Features with
    // less than this number of buckets will be accumulated in shared
    // memory which is faster.  Features with more will be accumulated
    // in global memory.  Since we don't get to decide per feature
    // how much shared memory is used (for the moment), we need to decide
    // ahead of time.
    const size_t MAX_LOCAL_BUCKETS = RF_LOCAL_BUCKET_MEM.get() / sizeof(W);  // 16k bytes
    
    // Maximum number of buckets
    size_t maxBuckets = 0;

    // Maximum number of buckets for all with less than MAX_LOCAL_BUCKETS
    size_t maxLocalBuckets = 0;

    // Number of active features
    size_t numActiveFeatures = 0;
    
    // Now we figure out how to onboard a variable number of variable
    // lengthed data segments for the feature bucket information.
    uint32_t numActiveBuckets = 0;
    
    uint32_t lastBucketDataOffset = 0;
    std::vector<uint32_t> bucketMemoryOffsets;   ///< Offset in the buckets memory blob per feature [nf + 1]
    std::vector<uint32_t> bucketEntryBits;       ///< How many bits per bucket [nf]
    std::vector<uint32_t> bucketNumbers(1, 0);   ///< Range of bucket numbers for feature [nf + 1]
    std::vector<uint32_t> featuresActive;        ///< For each feature: which are active? [nf]
    std::vector<uint32_t> featureIsOrdinal;      ///< For each feature: is it ordinal (1) vs categorical(0) [nf]
    
    // For each feature, we set up a table of offsets which will allow our OpenCL kernel
    // to know where in a flat buffer of memory the data for that feature resides.
    for (int i = 0;  i < nf;  ++i) {
        const BucketList & buckets = features[i].buckets;

        bucketEntryBits.push_back(buckets.entryBits);

        featuresActive.push_back(features[i].active);
        featureIsOrdinal.push_back(features[i].ordinal);
        
        if (features[i].active) {
            //cerr << "feature " << i << " buckets " << features[i].buckets.numBuckets << endl;
            ExcAssertGreaterEqual((void *)buckets.storage.data(),
                                  (void *)bucketMemory.data());
        
            activeFeatures.push_back(i);

            uint32_t offset
                = buckets.storage.data()
                - bucketMemory.data();

            //cerr << "feature = " << i << " offset = " << offset << " numActiveBuckets = " << numActiveBuckets << endl;
            bucketMemoryOffsets.push_back(offset);
            lastBucketDataOffset = offset + bucketMemory.length();
            
            ++numActiveFeatures;
            numActiveBuckets += features[i].buckets.numBuckets;
            maxBuckets = std::max<size_t>(maxBuckets,
                                          features[i].buckets.numBuckets);
            if (features[i].buckets.numBuckets <= MAX_LOCAL_BUCKETS) {
                maxLocalBuckets
                    = std::max<size_t>(maxLocalBuckets,
                                       features[i].buckets.numBuckets);
            }
        }
        else {
            bucketMemoryOffsets.push_back(lastBucketDataOffset);
        }

        bucketNumbers.push_back(numActiveBuckets);
    }

    bucketMemoryOffsets.push_back(lastBucketDataOffset);

    ExcAssertEqual(bucketMemoryOffsets.size(), nf + 1);
    ExcAssertEqual(bucketEntryBits.size(), nf);
    ExcAssertEqual(bucketNumbers.size(), nf + 1);
    ExcAssertEqual(featuresActive.size(), nf);

    // How much memory to accumulate W over all features per partition?
    size_t bytesPerPartition = sizeof(W) * numActiveBuckets;

    // How much memory to accumulate W over all features over the maximum
    // number of partitions?
    size_t bytesForAllPartitions = bytesPerPartition * maxPartitionCount;

    cerr << "numActiveBuckets = " << numActiveBuckets << endl;
    cerr << "sizeof(W) = " << sizeof(W) << endl;
    cerr << "bytesForAllPartitions = " << bytesForAllPartitions * 0.000001
         << "mb" << endl;
    cerr << "numIterations = " << numIterations << endl;

    OpenCLKernelContext kernelContext = getKernelContext();
    OpenCLContext & context = kernelContext.context;

    Bitset<OpenCLCommandQueueProperties> queueProperties
        = { OpenCLCommandQueueProperties::PROFILING_ENABLE };
    if (!RF_OPENCL_SYNCHRONOUS_LAUNCH && false) {
        queueProperties.set
            (OpenCLCommandQueueProperties::OUT_OF_ORDER_EXEC_MODE_ENABLE);
    }
    
    auto queue = kernelContext.context.createCommandQueue
        (kernelContext.devices[0], queueProperties);

    auto memQueue = kernelContext.context.createCommandQueue
        (kernelContext.devices[0], queueProperties);
    
    auto memQueue2 = kernelContext.context.createCommandQueue
        (kernelContext.devices[0], queueProperties);
    
    OpenCLMemoryManager mm(context, kernelContext.devices[0]);

    Date before = Date::now();

    std::vector<std::pair<std::string, OpenCLEvent> > allEvents;

    auto transferToGpu = [&] (const auto & obj, const char * what)
        -> std::tuple<OpenCLMemObject, OpenCLEvent, size_t>
    {
        auto toAllocate = roundUpToPageSize(obj.memusage());

        OpenCLMemObject memObject
            = context.createBuffer(CL_MEM_READ_ONLY, toAllocate);

        totalGpuAllocation += toAllocate;

        // ... and send it over
        OpenCLEvent copyData
            = memQueue.enqueueWriteBuffer
                (memObject, 0 /* offset */, obj.memusage(),
                (const void *)obj.data());

        allEvents.emplace_back(what, copyData);

        return { memObject, copyData, toAllocate };
    };

    auto allocGpu = [&] (size_t bytesToAlloc)
    {
        OpenCLMemObject memObject
            = context.createBuffer(CL_MEM_READ_WRITE, bytesToAlloc);
        totalGpuAllocation += bytesToAlloc;
        return memObject;
    };

    // First, we need to send over the rows, as the very first thing to
    // be done is to expand them.
    auto [clRowData, copyRowData, rowMemorySizePageAligned]
        = transferToGpu(rows.rowData, "copyRowData");

    // Same for our weight data
    auto [clWeightData, copyWeightData, unused1]
        = transferToGpu(rows.weightEncoder.weightFormatTable, "copyWeightData");

#if 0
    OpenCLMemObject clWeightData
        = context.createBuffer(CL_MEM_READ_ONLY, 4);
    
    totalGpuAllocation += 4;
    
    if (rows.weightEncoder.weightFormat == WF_TABLE) {
        clWeightData = context.createBuffer
            (0,
             (const void *)rows.weightEncoder.weightFormatTable.data(),
             rows.weightEncoder.weightFormatTable.memusage());
        totalGpuAllocation += rows.weightEncoder.weightFormatTable.memusage();
    }
#endif

    // We transfer the bucket data as early as possible, as it's one of the
    // longest things to transfer
    auto [clBucketData, transferBucketData, bucketMemorySizePageAligned]
        = transferToGpu(bucketMemory, "transferBucketData");
    
    // This one contains an expanded version of the row data, with one float
    // per row rather than bit-compressed.  It's expanded on the GPU so that
    // the compressed version can be passed over the PCIe bus and not the
    // expanded version.
    OpenCLMemObject clExpandedRowData = allocGpu(sizeof(float) * rowCount);

    // Our first kernel expands the data.  It's pretty simple, as a warm
    // up for the rest.
    OpenCLKernel decompressRowsKernel
        = kernelContext.program.createKernel("decompressRowsKernel");
    
    decompressRowsKernel.bind(clRowData,
                          (uint32_t)rows.rowData.length(),
                          rows.totalBits,
                          rows.weightEncoder.weightBits,
                          rows.exampleNumBits,
                          (uint32_t)numRows,
                          rows.weightEncoder.weightFormat,
                          rows.weightEncoder.weightMultiplier,
                          clWeightData,
                          clExpandedRowData);

    OpenCLEvent runExpandRows
        = queue.launch(decompressRowsKernel, { 65536 }, { 256 },
                       { copyRowData });
    
    allEvents.emplace_back("runExpandRows", runExpandRows);

    std::vector<float> debugExpandedRowsCpu;
    
    if (debugKernelOutput) {
        auto expandedRowsGpu = mm.mapSpan<const float>(clExpandedRowData, rowCount);
        debugExpandedRowsCpu = decodeRows(rows);
        bool different = false;
        
        for (size_t i = 0;  i < rowCount;  ++i) {
            if (debugExpandedRowsCpu[i] != expandedRowsGpu[i]) {
                cerr << "row " << i << " CPU " << debugExpandedRowsCpu[i]
                     << " GPU " << expandedRowsGpu[i] << endl;
                different = true;
            }
        }

        ExcAssert(!different && "runExpandRows");
    }

    // Next we need to distribute the weignts into the first set of
    // buckets.  This is done with the testFeatureKernelExpanded.

    // Before that, we need to set up some memory objects to be used
    // by the kernel.

    OpenCLMemObject clBucketNumbers = mm.manageMemoryRegion(bucketNumbers);
    OpenCLMemObject clBucketEntryBits = mm.manageMemoryRegion(bucketEntryBits);
    OpenCLMemObject clFeaturesActive = mm.manageMemoryRegion(featuresActive);

    totalGpuAllocation
        += bucketMemorySizePageAligned
        + bucketNumbers.size() * sizeof(int)
        + bucketEntryBits.size() * sizeof(int)
        + featuresActive.size() * sizeof(int);
    
    OpenCLMemObject clBucketDataOffsets
        = context.createBuffer(bucketMemoryOffsets);

    totalGpuAllocation += bucketMemoryOffsets.size() * sizeof(bucketMemoryOffsets[0]);

    size_t decompressedBucketDataBytes = 4;

    if (RF_EXPAND_FEATURE_BUCKETS) {
        decompressedBucketDataBytes
            = numActiveFeatures * sizeof(uint16_t) * numRows;
    }
    
    // Decompress the bucket memory
    OpenCLMemObject clDecompressedBucketData
        = context.createBuffer(CL_MEM_READ_WRITE, decompressedBucketDataBytes);

    totalGpuAllocation += decompressedBucketDataBytes;

    std::vector<uint32_t> decompressedBucketDataOffsets;

    {
        uint32_t offset = 0;
        for (int f = 0;  f < nf;  ++f) {
            decompressedBucketDataOffsets.push_back(offset);
            if (!featuresActive[f] || !RF_EXPAND_FEATURE_BUCKETS)
                continue;
            offset += sizeof(uint16_t) * numRows;
        }
        decompressedBucketDataOffsets.push_back(offset);
    }

    OpenCLMemObject clDecompressedBucketDataOffsets
        = context.createBuffer(decompressedBucketDataOffsets);

    if (RF_EXPAND_FEATURE_BUCKETS) {
        OpenCLKernel decompressBucketsKernel
            = kernelContext.program.createKernel("decompressFeatureBucketsKernel");
        
        decompressBucketsKernel
            .bind((uint32_t)numRows,
                  clBucketData,
                  clBucketDataOffsets,
                  clBucketNumbers,
                  clBucketEntryBits,
                  clFeaturesActive,
                  clDecompressedBucketData,
                  clDecompressedBucketDataOffsets);

        OpenCLEvent runDecompressBuckets
            = queue.launch(decompressBucketsKernel, { 65536, nf }, { 256, 1 },
                           { transferBucketData });
        allEvents.emplace_back("runDecompressBuckets", runDecompressBuckets);

        if (debugKernelOutput) {
            OpenCLEvent mapDecompressedBucketData;
            std::shared_ptr<const void> mappedDecompressedBucketData;

            std::tie(mappedDecompressedBucketData, mapDecompressedBucketData)
                = memQueue.enqueueMapBuffer(clDecompressedBucketData, CL_MAP_READ,
                                         0 /* offset */, decompressedBucketDataBytes,
                                         { runDecompressBuckets });
        
            mapDecompressedBucketData.waitUntilFinished();

            const uint16_t * decompressedBucketDataGpu
                = reinterpret_cast<const uint16_t *>(mappedDecompressedBucketData.get());

            bool different = false;
        
            for (size_t f = 0;  f < nf;  ++f) {
                if (!featuresActive[f])
                    continue;

                cerr << "active feature " << f
                     << " at " << decompressedBucketDataOffsets[f] << endl;
            
                const uint16_t * featureBuckets
                    = decompressedBucketDataGpu
                    + (decompressedBucketDataOffsets[f] / sizeof(uint16_t));

                for (size_t i = 0;  i < numRows;  ++i) {
                    int bCpu = features[f].buckets[i];
                    int bGpu = featureBuckets[i];
                
                    if (bGpu != bCpu) {
                        cerr << "error in feature bucket: feature "
                             << f << " bucket " << i << ": CPU "
                             << bCpu << " GPU " << bGpu << endl;
                        different = true;
                    }
                }
            }

            ExcAssert(!different && "runDecompressBuckets");
        }
    }
    
    // Our wAll array contains the sum of all of the W buckets across
    // each partition.  We allocate a single array at the start and just
    // use more and more each iteration.

    size_t wAllBytes = maxPartitionCount * sizeof(W);

    OpenCLMemObject clWAll
        = context.createBuffer(CL_MEM_READ_WRITE, wAllBytes);

    totalGpuAllocation += wAllBytes;

        // The first one is initialized by the input wAll
    OpenCLEvent copyWAll
        = memQueue.enqueueWriteBuffer(clWAll, 0, sizeof(W),
                                   &rows.wAll);

    allEvents.emplace_back("copyWAll", copyWAll);

    
    // The rest are initialized to zero
    OpenCLEvent initializeWAll
        = queue.enqueueFillBuffer(clWAll, 0 /* pattern */,
                                  sizeof(W) /* offset */,
                                  wAllBytes - sizeof(W));

    allEvents.emplace_back("initializeWAll", initializeWAll);
    
    // Our W buckets live here, per partition.  We never need to see it on
    // the host, so we allow it to be initialized and live on the device.
    // Note that we only use the beginning 1/256th at the start, and we
    // double the amount of what we use until we're using the whole lot
    // on the last iteration
    OpenCLMemObject clPartitionBuckets
        = context.createBuffer(CL_MEM_READ_WRITE, bytesForAllPartitions);

    totalGpuAllocation += bytesForAllPartitions;

    // Before we use this, it needs to be zero-filled (only the first
    // set for a single partition)
    OpenCLEvent fillFirstBuckets
        = queue.enqueueFillBuffer(clPartitionBuckets, 0 /* pattern */,
                                  0 /* offset */, bytesPerPartition);

    allEvents.emplace_back("fillFirstBuckets", fillFirstBuckets);

    OpenCLKernel testFeatureKernel
        = kernelContext.program.createKernel("testFeatureKernelExpanded");

    testFeatureKernel.bind(clExpandedRowData,
                           (uint32_t)numRows,
                           clBucketData,
                           clBucketDataOffsets,
                           clBucketNumbers,
                           clBucketEntryBits,
                           clDecompressedBucketData,
                           clDecompressedBucketDataOffsets,
                           (uint32_t)RF_EXPAND_FEATURE_BUCKETS,
                           clFeaturesActive,
                           LocalArray<W>(MAX_LOCAL_BUCKETS),
                           (uint32_t)MAX_LOCAL_BUCKETS,
                           clPartitionBuckets);

    //cerr << jsonEncode(testFeatureKernel.getInfo()) << endl;
    //cerr << jsonEncode(OpenCLKernelWorkgroupInfo(testFeatureKernel, kernelContext.devices[0])) << endl;

    OpenCLEvent runTestFeatureKernel
        = queue.launch(testFeatureKernel,
                       { 65536, nf },
                       { 256, 1 },
                       { transferBucketData, fillFirstBuckets, runExpandRows });

    cerr << "runTestFeatureKernel refcount 1 = " << runTestFeatureKernel.referenceCount() << endl;

    allEvents.emplace_back("runTestFeatureKernel", runTestFeatureKernel);

    cerr << "runTestFeatureKernel refcount 2 = " << runTestFeatureKernel.referenceCount() << endl;

    if (debugKernelOutput) {
        // Get that data back (by mapping), and verify it against the
        // CPU-calcualted version.

        OpenCLEvent mapBuckets;
        std::shared_ptr<const void> mappedBuckets;

        std::tie(mappedBuckets, mapBuckets)
            = memQueue.enqueueMapBuffer(clPartitionBuckets, CL_MAP_READ,
                                     0 /* offset */, bytesPerPartition,
                                     { runTestFeatureKernel });

        cerr << "runTestFeatureKernel refcount 3 = " << runTestFeatureKernel.referenceCount() << endl;

        mapBuckets.waitUntilFinished();

        const W * allWGpu = reinterpret_cast<const W *>(mappedBuckets.get());

        std::vector<W> allWCpu(numActiveBuckets);
        
        bool different = false;
            
        // Print out the buckets that differ from CPU to GPU
        for (int i = 0;  i < nf;  ++i) {
            int start = bucketNumbers[i];
            int end = bucketNumbers[i + 1];
            int n = end - start;

            if (n == 0)
                continue;

            testFeatureKernelCpu(rows.getRowIterator(),
                                 rowCount,
                                 features[i].buckets,
                                 allWCpu.data() + start);

            const W * pGpu = allWGpu + start;
            const W * pCpu = allWCpu.data() + start;

            for (int j = 0;  j < n;  ++j) {
                if (pCpu[j] != pGpu[j]) {
                    cerr << "feat " << i << " bucket " << j << " w "
                         << jsonEncodeStr(pGpu[j]) << " != "
                         << jsonEncodeStr(pCpu[j]) << endl;
                    different = true;
                }
            }
        }

        ExcAssert(!different && "runTestFeatureKernel");
    }
    
    // Which partition is each row in?  Initially, everything
    // is in partition zero, but as we start to split, we end up
    // splitting them amongst many partitions.  Each partition has
    // its own set of buckets that we maintain.

    size_t partitionMemoryUsage = sizeof(RowPartitionInfo) * rowCount;
    
    OpenCLMemObject clPartitions
        = context.createBuffer(CL_MEM_READ_WRITE, partitionMemoryUsage);

    totalGpuAllocation += partitionMemoryUsage;

    // Before we use this, it needs to be zero-filled
    OpenCLEvent fillPartitions
        = queue.enqueueFillBuffer(clPartitions, (uint32_t)0 /* pattern */,
                                  0 /* offset */, partitionMemoryUsage);

    allEvents.emplace_back("fillPartitions", fillPartitions);

    // Record the split, per level, per partition
    std::vector<std::vector<PartitionSplit> > depthSplits;
    depthSplits.reserve(16);

    OpenCLMemObject clFeatureIsOrdinal
        = context.createBuffer(featureIsOrdinal);
    
    totalGpuAllocation += featureIsOrdinal.size() * sizeof(int);

    // How many partitions at the current depth?
    unsigned numPartitionsAtDepth = 1;

    cerr << "runTestFeatureKernel refcount 4 = " << runTestFeatureKernel.referenceCount() << endl;

    // Which event represents that the previous iteration of partitions
    // are available?
    OpenCLEvent previousIteration = runTestFeatureKernel;  // CRASH HERE ON DEBUG

    // Event list for all of the buckets
    std::vector<OpenCLEvent> clDepthSplitsEvents;

    // Each of the numIterations partitions has double the number of buckets,
    // so the total number is 2^(numIterations + 1) - 1.
    size_t allPartitionSplitBytes
        = sizeof(PartitionSplit) * (2 << numIterations);

    OpenCLMemObject clAllPartitionSplits
        = context.createBuffer(CL_MEM_READ_WRITE | CL_MEM_ALLOC_HOST_PTR,
                               allPartitionSplitBytes);
    
    totalGpuAllocation += allPartitionSplitBytes;

    // Pre-allocate partition buckets for the widest bucket
    // We need to store partition splits for each partition and each
    // feature.  Get the memory.  It doesn't need to be initialized.
    // Layout is partition-major.
        
    size_t featurePartitionSplitBytes
        = sizeof(PartitionSplit) * (1 << numIterations) * nf;

    OpenCLMemObject clFeaturePartitionSplits
        = context.createBuffer(CL_MEM_READ_WRITE, featurePartitionSplitBytes);

    totalGpuAllocation += featurePartitionSplitBytes;
    
    Date startDepth = Date::now();

    // We go down level by level
    for (int myDepth = 0;
         myDepth < 16 && depth < maxDepth;
         ++depth, ++myDepth, numPartitionsAtDepth *= 2) {

        cerr << "depth = " << depth << " myDepth = " << myDepth << " numPartitions " << numPartitionsAtDepth << endl;

        if (RF_OPENCL_SYNCHRONOUS_LAUNCH) {
            queue.flush();
            queue.finish();
        }
        
        if (debugKernelOutput) {
            // Verify that the input prerequisites are OK:
            // - The counts are all non-negative
            // - The wAll values are all non-negative
            // - The bucket totals all match the wAll values

            // To do this, we pull back the W and wAll wAll buckets
            OpenCLEvent mapBuckets;
            std::shared_ptr<const void> mappedBuckets;

            std::tie(mappedBuckets, mapBuckets)
                = memQueue.enqueueMapBuffer(clPartitionBuckets, CL_MAP_READ,
                                         0 /* offset */,
                                         bytesPerPartition * numPartitionsAtDepth,
                                         { previousIteration });
        
            mapBuckets.waitUntilFinished();

            const W * bucketsGpu
                = reinterpret_cast<const W *>(mappedBuckets.get());
            
            // Get back the CPU version of wAll
            OpenCLEvent mapWAll;
            std::shared_ptr<const void> mappedWAll;

            std::tie(mappedWAll, mapWAll)
                = memQueue.enqueueMapBuffer(clWAll, CL_MAP_READ,
                                         0 /* offset */,
                                         sizeof(W) * numPartitionsAtDepth,
                                         { previousIteration });
        
            mapWAll.waitUntilFinished();

            const W * wAllGpu
                = reinterpret_cast<const W *>(mappedWAll.get());

            bool problem = false;
            
            for (int p = 0;  p < numPartitionsAtDepth;  ++p) {
                
                //cerr << "partition " << p << " count " << wAllGpu[p].count() << endl;
                
                if (wAllGpu[p].count() < 0
                    || wAllGpu[p].v[0] < 0.0
                    || wAllGpu[p].v[1] < 0.0) {
                    cerr << "partition " << p << " wAll error: "
                         << jsonEncodeStr(wAllGpu[p]) << endl;
                    problem = true;
                }


                for (int f = 0;  f < nf;  ++f) {
                    int first = bucketNumbers[f];
                    int last = bucketNumbers[f + 1];
                    int nb = last - first;

                    if (nb == 0)
                        continue;
                    
                    W wAllPartition;
                    
                    for (int b = 0;  b < nb;  ++b) {
                        
                        const W * featureBuckets = bucketsGpu + (p * numActiveBuckets) + first;
                        
                        if (featureBuckets[b].count() < 0
                            || featureBuckets[b].v[0] < 0.0 || featureBuckets[b].v[1] < 0.0) {
                            cerr << "partition " << p << " feature " << f
                                 << " bucket " << b << " error: "
                                 << jsonEncodeStr(bucketsGpu[p * numActiveBuckets + b])
                                 << endl;
                            problem = true;
                        }

                        wAllPartition += featureBuckets[b];
                    }

                    if (wAllPartition != wAllGpu[p]) {
                        cerr << "partition " << p << " feature " << f
                             << " wAll consistency error: feature = "
                             << jsonEncodeStr(wAllPartition) << " wAll = "
                             << jsonEncodeStr(wAllGpu[p]) << endl;
                        problem = true;
                    }
                }
            }

            ExcAssert(!problem && "verifyPreconditions");
        }
    
        // Run a kernel to find the new split point for each partition,
        // best feature and kernel

        // Now we have initialized our data, we can get to running the
        // kernel.  This kernel is dimensioned on bucket number,
        // feature number and partition number (ie, a 3d invocation).
        
        OpenCLKernel getPartitionSplitsKernel
            = kernelContext.program.createKernel("getPartitionSplitsKernel");

        getPartitionSplitsKernel
            .bind((uint32_t)numActiveBuckets,
                  clBucketNumbers,
                  clFeaturesActive,
                  clFeatureIsOrdinal,
                  clPartitionBuckets,
                  clWAll,
                  clFeaturePartitionSplits,
                  LocalArray<W>(MAX_LOCAL_BUCKETS),
                  (uint32_t)MAX_LOCAL_BUCKETS,
                  LocalArray<W>(2) /* W start / W best */);

        //cerr << endl << endl << " depth " << depth << " numPartitions "
        //     << numPartitionsAtDepth << " buckets "
        //     << numPartitionsAtDepth * numActiveBuckets << endl;

        OpenCLEvent runPartitionSplitsKernel
            = queue.launch(getPartitionSplitsKernel,
                           { 64, nf, numPartitionsAtDepth },
                           { 64, 1, 1 },
                           { previousIteration, copyWAll, initializeWAll});

        allEvents.emplace_back("runPartitionSplitsKernel "
                               + std::to_string(myDepth),
                               runPartitionSplitsKernel);

        // Now we have the best split for each feature for each partition,
        // find the best one per partition and finally record it.
        OpenCLKernel bestPartitionSplitKernel
            = kernelContext.program.createKernel("bestPartitionSplitKernel");

        // What is our offset into partitionSplits?  In other words, where do
        // the partitions for this iteration start?  By skipping the first
        // bucket, this becomes trivially numPartitionsAtDepth: 1, 2, 4, 8, ...
        // It's not technically necessary to pass it since it's one of the
        // launch parameters, but this way is more clear.
        uint32_t partitionSplitsOffset = numPartitionsAtDepth;
        
        bestPartitionSplitKernel
            .bind((uint32_t)nf,
                  clFeaturesActive,
                  clFeaturePartitionSplits,
                  clAllPartitionSplits,
                  partitionSplitsOffset);

        OpenCLEvent runBestPartitionSplitKernel
            = queue.launch(bestPartitionSplitKernel,
                           { numPartitionsAtDepth },
                           { },
                           { runPartitionSplitsKernel });

        allEvents.emplace_back("runBestPartitionSplitKernel "
                               + std::to_string(myDepth),
                               runBestPartitionSplitKernel);

        // These are parallel CPU data structures for the on-GPU ones,
        // into which we copy the input data required to re-run the
        // computation on the CPU so we can verify the output of the GPU
        // algorithm.
        std::vector<PartitionSplit> debugPartitionSplitsCpu;
        std::span<W> debugBucketsCpu;
        std::vector<W> debugWAllCpu;
        std::vector<RowPartitionInfo> debugPartitionsCpu;

        if (depth == maxDepth - 1) {
            OpenCLEvent mapPartitionSplits;
            std::shared_ptr<void> mappedPartitionSplits;
            
            std::tie(mappedPartitionSplits, mapPartitionSplits)
                = memQueue.enqueueMapBuffer
                   (clAllPartitionSplits, CL_MAP_READ,
                    sizeof(PartitionSplit) * numPartitionsAtDepth, /* offset */
                    sizeof(PartitionSplit) * numPartitionsAtDepth, /* length */
                    { runPartitionSplitsKernel });
        
            mapPartitionSplits.waitUntilFinished();

            PartitionSplit * partitionSplitsGpu
                = reinterpret_cast<PartitionSplit *>
                (mappedPartitionSplits.get());

            std::vector<size_t> partitionCounts;
            
            for (int p = 0;  p < numPartitionsAtDepth;  ++p) {

                //new (&partitionSplitsGpu[p].activeFeatures) std::vector<int>;

                //cerr << "partition " << p << " split "
                //     << jsonEncodeStr(partitionSplitsGpu[p]) << endl;

                partitionCounts.push_back(partitionSplitsGpu[p].left.count());
                partitionCounts.push_back(partitionSplitsGpu[p].right.count());
            }

            std::sort(partitionCounts.begin(), partitionCounts.end());

            int firstNonZero = 0;
            for (int p = 0;  p < partitionCounts.size();  ++p, ++firstNonZero) {
                if (partitionCounts[p] > 0)
                    break;
            }
        
            int numNonZero = partitionCounts.size() - firstNonZero;
            cerr << "occupancy: " << firstNonZero << " zero, "
                 << numNonZero << " non-zero of "
                 << partitionCounts.size() << " partitions" << endl;
            cerr << "mean " << numRows / numNonZero << " median "
                 << partitionCounts[partitionCounts.size() / 2]
                 << " max " << partitionCounts.back() << endl;

            for (int i = partitionCounts.size() - 1;  i >= 0 && i >= partitionCounts.size() - 200;  --i) {
                if (partitionCounts[i] == 0)
                    break;
                cerr << "  " << partitionCounts[i] << endl;
            }
            
        }
        
        // Contains which partitions can be different because of known split differences caused by
        // rounding differences between the CPU and GPU implementations.
        std::set<int> okayDifferentPartitions;

        if (debugKernelOutput) {
            // Map back the GPU partition splits
            OpenCLEvent mapPartitionSplits;
            std::shared_ptr<void> mappedPartitionSplits;

            std::tie(mappedPartitionSplits, mapPartitionSplits)
                = memQueue.enqueueMapBuffer
                   (clAllPartitionSplits, CL_MAP_READ,
                    sizeof(PartitionSplit) * numPartitionsAtDepth /* offset */,
                    sizeof(PartitionSplit) * numPartitionsAtDepth /* length */,
                    { runPartitionSplitsKernel });
        
            mapPartitionSplits.waitUntilFinished();

            PartitionSplit * partitionSplitsGpu
                = reinterpret_cast<PartitionSplit *>
                    (mappedPartitionSplits.get());

            // Map back the GPU partition numbers
            OpenCLEvent mapPartitions;
            std::shared_ptr<const void> mappedPartitions;

            std::tie(mappedPartitions, mapPartitions)
                = memQueue.enqueueMapBuffer
                   (clPartitions, CL_MAP_READ,
                    0 /* offset */,
                    partitionMemoryUsage,
                    { runPartitionSplitsKernel, fillPartitions });
        
            mapPartitions.waitUntilFinished();

            const RowPartitionInfo * partitionsGpu
                = reinterpret_cast<const RowPartitionInfo *>
                    (mappedPartitions.get());

            debugPartitionsCpu = { partitionsGpu, partitionsGpu + rowCount };
            
            // Construct the CPU version of buckets
            OpenCLEvent mapBuckets;
            std::shared_ptr<const void> mappedBuckets;

            std::tie(mappedBuckets, mapBuckets)
                = memQueue.enqueueMapBuffer(clPartitionBuckets, CL_MAP_READ | CL_MAP_WRITE,
                                         0 /* offset */,
                                         bytesPerPartition * numPartitionsAtDepth,
                                         { mapPartitionSplits });
        
            mapBuckets.waitUntilFinished();

            W * bucketsGpu
                = const_cast<W *>(reinterpret_cast<const W *>(mappedBuckets.get()));

            debugBucketsCpu = { bucketsGpu, numActiveFeatures * numPartitionsAtDepth };
            
            // Get back the CPU version of wAll
            OpenCLEvent mapWAll;
            std::shared_ptr<const void> mappedWAll;

            std::tie(mappedWAll, mapWAll)
                = memQueue.enqueueMapBuffer(clWAll, CL_MAP_READ,
                                         0 /* offset */,
                                         sizeof(W) * numPartitionsAtDepth,
                                         { mapBuckets });
        
            mapWAll.waitUntilFinished();

            const W * wAllGpu
                = reinterpret_cast<const W *>(mappedWAll.get());

            debugWAllCpu = { wAllGpu, wAllGpu + numPartitionsAtDepth };

            std::vector<PartitionIndex> indexes;
            
            for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
                indexes.push_back(numPartitionsAtDepth + i);
            }

            // Run the CPU version... first getting the data in place
            debugPartitionSplitsCpu
                = getPartitionSplits(debugBucketsCpu, numActiveBuckets,
                                     activeFeatures, bucketNumbers,
                                     features, debugWAllCpu,
                                     indexes, 
                                     false /* parallel */);

            // Make sure we got the right thing back out
            ExcAssertEqual(debugPartitionSplitsCpu.size(),
                           numPartitionsAtDepth);

            bool different = false;
            
            for (int p = 0;  p < numPartitionsAtDepth;  ++p) {

                //cerr << "p = " << p << " of " << numPartitionsAtDepth << endl
                //     << " CPU " << jsonEncodeStr(debugPartitionSplitsCpu[p]) << endl
                //     << " GPU " << jsonEncodeStr(partitionSplitsGpu[p])
                //     << endl;

                if ((partitionSplitsGpu[p].left
                     != debugPartitionSplitsCpu[p].left)
                    || (partitionSplitsGpu[p].right
                        != debugPartitionSplitsCpu[p].right)
                    || (partitionSplitsGpu[p].feature
                        != debugPartitionSplitsCpu[p].feature)
                    || (partitionSplitsGpu[p].value
                        != debugPartitionSplitsCpu[p].value)
                    || (partitionSplitsGpu[p].index
                        != debugPartitionSplitsCpu[p].index)
                    /* || (partitionSplitsGpu[p].score
                        != debugPartitionSplitsCpu[p].score) */) {
                    float score1 = partitionSplitsGpu[p].score;
                    float score2 = debugPartitionSplitsCpu[p].score;

                    okayDifferentPartitions.insert(p);
                    okayDifferentPartitions.insert(p + numPartitionsAtDepth);

                    float relativeDifference = fabs(score1 - score2) / max(score1, score2);

                    different = different || relativeDifference >= 0.001;
                    cerr << "partition " << p << "\nGPU "
                         << jsonEncodeStr(partitionSplitsGpu[p])
                         << "\nCPU " << jsonEncodeStr(debugPartitionSplitsCpu[p])
                         << " score relative difference " << relativeDifference << endl;
                    if ((partitionSplitsGpu[p].score != debugPartitionSplitsCpu[p].score))
                        cerr << "score GPU: " << *(uint32_t *)(&partitionSplitsGpu[p].score)
                             << " score CPU: " << *(uint32_t *)(&debugPartitionSplitsCpu[p].score)
                             << endl;
                }
            }
            
            ExcAssert(!different);
        }
        
        // Double the number of partitions, create new W entries for the
        // new partitions, and transfer those examples that are in the
        // wrong partition to the right one

        // To update buckets, we first transfer the smallest number of
        // examples possible into the new partitions, without subtracting
        // them from the original partitions.  Afterwards, we subtract them
        // and swap any which are in the wrong order.  This means that we
        // only need to keep one set of partition buckets in shared memory
        // and saves lots of atomic operations.

        // First we clear everything on the right side, ready to accumulate
        // the new buckets there.

        OpenCLKernel clearBucketsKernel
            = kernelContext.program.createKernel("clearBucketsKernel");

        clearBucketsKernel
            .bind(clPartitionBuckets,
                  clWAll,
                  clAllPartitionSplits,
                  (uint32_t)numActiveBuckets,
                  (uint32_t)numPartitionsAtDepth);

        OpenCLEvent runClearBucketsKernel
            = queue.launch(clearBucketsKernel,
                           { numPartitionsAtDepth, (numActiveBuckets + 63) / 64 * 64 },
                           { 1, 64 },
                           { runPartitionSplitsKernel });
 
        allEvents.emplace_back("runClearBucketsKernel "
                               + std::to_string(myDepth),
                               runClearBucketsKernel);

        // Now the right side buckets are clear, we can transfer the weights
        // for the examples who have changed bucket from the left to the right.


        OpenCLKernel updateBucketsKernel
            = kernelContext.program.createKernel("updateBucketsKernel");

        updateBucketsKernel
            .bind((uint32_t)numPartitionsAtDepth,  // rightOffset
                  (uint32_t)numActiveBuckets,
                  clPartitions,
                  clPartitionBuckets,
                  clWAll,
                  clAllPartitionSplits,

                  clExpandedRowData,
                  (uint32_t)numRows,                  

                  clBucketData,
                  clBucketDataOffsets,
                  clBucketNumbers,
                  clBucketEntryBits,
                  clDecompressedBucketData,
                  clDecompressedBucketDataOffsets,
                  (uint32_t)RF_EXPAND_FEATURE_BUCKETS,
                  clFeaturesActive,
                  clFeatureIsOrdinal,
                  LocalArray<W>(MAX_LOCAL_BUCKETS),
                  (uint32_t)MAX_LOCAL_BUCKETS);

        //cerr << jsonEncodeStr(OpenCLKernelWorkgroupInfo(updateBucketsKernel,
        //                                                context.getDevices()[0]))
        //     << endl;
        
        OpenCLEvent runUpdateBucketsKernel;

        size_t numRowKernels = RF_NUM_ROW_KERNELS;
        
        if (RF_SEPARATE_FEATURE_UPDATES) {
        
            std::vector<OpenCLEvent> updateBucketsEvents;
            
            OpenCLEvent runUpdateWAllKernel
                = queue.launch(updateBucketsKernel,
                               { numRowKernels, 1 },
                               { RF_ROW_KERNEL_WORKGROUP_SIZE, 1 },
                               { runClearBucketsKernel },
                               { 0, 0 });

            allEvents.emplace_back("runUpdateWAllKernel "
                                   + std::to_string(myDepth),
                                   runUpdateWAllKernel);

            updateBucketsEvents.emplace_back(runUpdateWAllKernel);
        
            for (int f = 0;  f < nf;  ++f) {
                if (!featuresActive[f])
                    continue;

                OpenCLEvent runUpdateBucketsKernel
                    = queue.launch(updateBucketsKernel,
                                   { numRowKernels, 1 },
                                   { RF_ROW_KERNEL_WORKGROUP_SIZE, 1 },
                                   { runClearBucketsKernel },
                                   { 0, (unsigned)(f + 1) });

                allEvents.emplace_back("runUpdateBucketsKernel "
                                       + std::to_string(myDepth) + " feat "
                                       + std::to_string(f) + " nb "
                                       + std::to_string(bucketNumbers[f + 1] - bucketNumbers[f]),
                                       runUpdateBucketsKernel);
            
                updateBucketsEvents.emplace_back(runUpdateBucketsKernel);
            }

            runUpdateBucketsKernel
                = queue.enqueueMarker(updateBucketsEvents);
        }
        else {
            runUpdateBucketsKernel
                = queue.launch(updateBucketsKernel,
                               { numRowKernels, nf + 1 /* +1 is wAll */},
                               { RF_ROW_KERNEL_WORKGROUP_SIZE, 1 },
                               { runClearBucketsKernel });
        }
        
        allEvents.emplace_back("runUpdateBucketsKernel "
                               + std::to_string(myDepth),
                               runUpdateBucketsKernel);
        
        OpenCLKernel fixupBucketsKernel
            = kernelContext.program.createKernel("fixupBucketsKernel");

        fixupBucketsKernel
            .bind(clPartitionBuckets,
                  clWAll,
                  clAllPartitionSplits,
                  (uint32_t)numActiveBuckets,
                  (uint32_t)numPartitionsAtDepth);

        OpenCLEvent runFixupBucketsKernel
            = queue.launch(fixupBucketsKernel,
                           { numPartitionsAtDepth, (numActiveBuckets + 63) / 64 * 64 },
                           { 1, 64 },
                           { runUpdateBucketsKernel });
 
        allEvents.emplace_back("runFixupBucketsKernel "
                               + std::to_string(myDepth),
                               runFixupBucketsKernel);

        if (debugKernelOutput) {

            if (depth < 2) {
                cerr << "splits " << jsonEncode(debugPartitionSplitsCpu) << endl;
                cerr << "numPartitionsAtDepth = " << numPartitionsAtDepth << endl;
            }

            // These give the partition numbers for the left (.first) and right (.second) of each partition
            // or -1 if it's not active or < -2 if it's handled as a leaf
            std::vector<std::pair<int32_t, int32_t> > newPartitionNumbers(numPartitionsAtDepth, {-1,-1});
 
            ExcAssertEqual(numPartitionsAtDepth, debugPartitionSplitsCpu.size());

            for (int i = 0;  i < numPartitionsAtDepth;  ++i) {
                const PartitionSplit & split = debugPartitionSplitsCpu.at(i);
                if (!split.valid())
                    continue;
                int left = i;
                int right = i + numPartitionsAtDepth;
                //if (split.direction)
                //    std::swap(left, right);
                newPartitionNumbers[i] = { left, right };
            }
            
            //cerr << "newPartitionNumbers = " << jsonEncodeStr(newPartitionNumbers) << endl;

            // Run the CPU version of the computation
            updateBuckets(features,
                          debugPartitionsCpu,
                          debugBucketsCpu,
                          numActiveBuckets,
                          debugWAllCpu,
                          bucketNumbers,
                          debugPartitionSplitsCpu,
                          newPartitionNumbers,
                          numPartitionsAtDepth * 2,
                          debugExpandedRowsCpu,
                          activeFeatures);

            // There are three things that we modify (in-place):
            // 1) The per-partition, per-feature W buckets
            // 2) The per-partition wAll array
            // 3) The per-row partition number array
            //
            // Each of these three will be mapped back from the GPU and
            // its accuracy verified.

            bool different = false;

            // 1.  Map back the W buckets and compare against the CPU
            // version.

            // Construct the CPU version of buckets
            OpenCLEvent mapBuckets;
            std::shared_ptr<const void> mappedBuckets;

            std::tie(mappedBuckets, mapBuckets)
                = memQueue.enqueueMapBuffer(clPartitionBuckets, CL_MAP_READ,
                                         0 /* offset */,
                                         bytesPerPartition * numPartitionsAtDepth,
                                         { runUpdateBucketsKernel });
        
            mapBuckets.waitUntilFinished();

            const W * bucketsGpu
                = reinterpret_cast<const W *>(mappedBuckets.get());

            for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
                const W * partitionBuckets = bucketsGpu + numActiveBuckets * i;
                if (newPartitionNumbers[i].first == -1)
                    continue;  // dead partition, don't verify...
                if (okayDifferentPartitions.count(i))
                    continue;  // is different due to a different split caused by numerical errors

                std::span<W> debugBucketsCpuRow = debugBucketsCpu.subspan(i * numActiveFeatures, numActiveFeatures);

                for (size_t j = 0;  j < numActiveBuckets;  ++j) {
                    if (partitionBuckets[j] != debugBucketsCpuRow[j]) {
                        cerr << "part " << i << " bucket " << j
                                << " num " << numActiveBuckets * i + j
                                << " update error: CPU "
                                << jsonEncodeStr(debugBucketsCpuRow[j])
                                << " GPU "
                                << jsonEncodeStr(partitionBuckets[j])
                                << endl;
                        different = true;
                    }
                }
            }

            // 2.  Map back the wAll values and compare against the CPU
            // version
            OpenCLEvent mapWAll;
            std::shared_ptr<const void> mappedWAll;

            std::tie(mappedWAll, mapWAll)
                = memQueue.enqueueMapBuffer(clWAll, CL_MAP_READ,
                                         0 /* offset */,
                                         sizeof(W) * numPartitionsAtDepth,
                                         { runUpdateBucketsKernel });
        
            mapWAll.waitUntilFinished();

            const W * wAllGpu
                = reinterpret_cast<const W *>(mappedWAll.get());

            for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
                if (newPartitionNumbers[i].first == -1 || okayDifferentPartitions.count(i))
                    continue;  // dead partition, don't verify...
                if (wAllGpu[i] != debugWAllCpu[i]) {
                    cerr << "part " << i << " wAll update error: CPU "
                         << jsonEncodeStr(debugWAllCpu[i])
                         << " GPU " << jsonEncodeStr(wAllGpu[i])
                         << endl;
                    different = true;
                }
            }

            // 3.  Map back the GPU partition numbers and compare against
            // the GPU version
            OpenCLEvent mapPartitions;
            std::shared_ptr<const void> mappedPartitions;

            std::tie(mappedPartitions, mapPartitions)
                = memQueue.enqueueMapBuffer
                   (clPartitions, CL_MAP_READ,
                    0 /* offset */,
                    partitionMemoryUsage,
                    { runUpdateBucketsKernel });
        
            mapPartitions.waitUntilFinished();

            const RowPartitionInfo * partitionsGpu
                = reinterpret_cast<const RowPartitionInfo *>
                    (mappedPartitions.get());

            int numDifferences = 0;
            std::map<std::pair<int, int>, int> differenceStats;
            for (size_t i = 0;  i < rowCount;  ++i) {
                if (partitionsGpu[i] != debugPartitionsCpu[i] && debugPartitionsCpu[i] != RowPartitionInfo::max()) {
                    if (okayDifferentPartitions.count(debugPartitionsCpu[i]))
                        continue;  // caused by known numerical issues
                    different = true;
                    differenceStats[{debugPartitionsCpu[i], partitionsGpu[i]}] += 1;
                    if (++numDifferences < 10) {
                        cerr << "row " << i << " partition difference: CPU "
                            << (int)debugPartitionsCpu[i]
                            << " GPU " << (int)partitionsGpu[i]
                            << endl;
                    }
                    else if (numDifferences == 11) {
                        cerr << "..." << endl;
                    }
                }
            }

            if (numDifferences > 0) {
                cerr << "partition number error stats (total " << numDifferences << ")" << endl;
                for (auto & s: differenceStats) {
                    cerr << "  cpu: " << s.first.first << " gpu: " << s.first.second << " count: " << s.second << endl;
                }
            }

            ExcAssert(!different);
        }

        Date doneDepth = Date::now();

        cerr << "depth " << depth << " wall time is " << doneDepth.secondsSince(startDepth) * 1000 << endl;
        startDepth = doneDepth;

        // Ready for the next level
        previousIteration = runUpdateBucketsKernel;
    }

    OpenCLEvent mapAllPartitionSplits;
    std::shared_ptr<void> mappedAllPartitionSplits;
        
    std::tie(mappedAllPartitionSplits, mapAllPartitionSplits)
        = memQueue.enqueueMapBuffer
        (clAllPartitionSplits, CL_MAP_READ,
         0 /* offset */,
         sizeof(PartitionSplit) * numPartitionsAtDepth,
         { previousIteration });
        
    allEvents.emplace_back("mapAllPartitionSplits", mapAllPartitionSplits);

    queue.flush();
    memQueue.flush();
    queue.finish();
    memQueue.finish();
    
    cerr << "kernel wall time is " << Date::now().secondsSince(before) * 1000
         << "ms with " << totalGpuAllocation / 1000000.0 << "Mb allocated" << endl;
    cerr << "numPartitionsAtDepth = " << numPartitionsAtDepth << endl;

    cerr << "  submit    queue    start      end  elapsed name" << endl;
    
    auto first = allEvents.at(0).second.getProfilingInfo();
    for (auto & e: allEvents) {
        std::string name = e.first;
        const OpenCLEvent & ev = e.second;
        
        auto info = ev.getProfilingInfo() - first.queued;

        auto ms = [&] (int64_t ns) -> double
            {
                return ns / 1000000.0;
            };
        
        cerr << format("%8.3f %8.3f %8.3f %8.3f %8.3f ",
                       ms(info.queued), ms(info.submit), ms(info.start),
                       ms(info.end),
                       ms(info.end - info.start))
             << name << endl;
    }

    std::shared_ptr<const PartitionSplit> mappedAllPartitionSplitsCast
        = reinterpret_pointer_cast<const PartitionSplit>(mappedAllPartitionSplits);

    std::map<PartitionIndex, PartitionSplit> allSplits;

    for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
        cerr << "PARTITION " << i << " " << PartitionIndex(i) << endl;
        cerr << jsonEncode(mappedAllPartitionSplitsCast.get()[i]) << endl;
    }

    std::vector<PartitionIndex> indexes(numPartitionsAtDepth);
    std::map<PartitionIndex, ML::Tree::Ptr> leaves;

    cerr << "numRows = " << rowCount << endl;

    std::set<int> donePositions;

    std::function<void (PartitionIndex, int)> extractSplits = [&] (PartitionIndex index, int position)
    {
        donePositions.insert(position);
        PartitionIndex leftIndex = index.leftChild();
        PartitionIndex rightIndex = index.rightChild();

        cerr << "position = " << position << " numPartitionsAtDepth = " << numPartitionsAtDepth << endl;

        indexes[position] = index;

        auto & split = mappedAllPartitionSplitsCast.get()[position];

        cerr << "  split = " << jsonEncodeStr(split) << endl;

        int leftPosition = leftIndex.index;
        int rightPosition = rightIndex.index;

        allSplits[index] = split;

        if (split.left.count() > 0 && split.right.count() > 0) {

            cerr << "  testing left " << leftIndex << " with position " << leftPosition << " of " << numPartitionsAtDepth << endl;
            if (leftPosition < numPartitionsAtDepth) {
                auto & lsplit = mappedAllPartitionSplitsCast.get()[leftPosition];
                cerr << "  has split " << jsonEncodeStr(lsplit) << endl;
                ExcAssertEqual(lsplit.left.count() + lsplit.right.count(), split.left.count());
            }
            if (leftPosition >= numPartitionsAtDepth || !mappedAllPartitionSplitsCast.get()[leftPosition].valid()) {
                cerr << "    not valid; leaf" << endl;
                leaves[leftIndex] = getLeaf(tree, split.left);
            }
            else {
                cerr << "    valid; recurse" << endl;
                extractSplits(leftIndex, leftPosition);
            }

            cerr << "  testing right " << rightIndex << " with position " << rightPosition << " of " << numPartitionsAtDepth << endl;
            if (rightPosition < numPartitionsAtDepth) {
                auto & rsplit = mappedAllPartitionSplitsCast.get()[rightPosition];
                cerr << "  has split " << jsonEncodeStr(rsplit) << endl;
                ExcAssertEqual(rsplit.left.count() + rsplit.right.count(), split.right.count());
            }
            if (rightPosition >= numPartitionsAtDepth || !mappedAllPartitionSplitsCast.get()[rightPosition].valid()) {
                leaves[rightIndex] = getLeaf(tree, split.right);
            }
            else {
                extractSplits(rightIndex, rightPosition);
            }
        }
        else {
            leaves[index] = getLeaf(tree, split.left + split.right);
        }
    };

    extractSplits(PartitionIndex::root(), 1 /* index */);

    for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
        if (mappedAllPartitionSplitsCast.get()[i].valid() && !donePositions.count(i)) {
            cerr << "ERROR: valid split " << i << " was not extracted" << endl;
        }
    }

    //throw Exception("TODO allSplits");
    
    //for (int i = 0;  i < numIterations;  ++i) {
    //    depthSplits.emplace_back
    //        (mappedAllPartitionSplitsCast.get() + (1 << i),
    //         mappedAllPartitionSplitsCast.get() + (2 << i));
    //}
    
    cerr << "got " << allSplits.size() << " splits" << endl;


    // If we're not at the lowest level, partition our data and recurse
    // par partition to create our leaves.
    Date beforeMapping = Date::now();

    auto bucketsUnrolled = mm.mapVector<W>(clPartitionBuckets);
    auto partitions = mm.mapVector<RowPartitionInfo>(clPartitions);
    auto wAll = mm.mapVector<W>(clWAll);
    auto decodedRows = mm.mapVector<float>(clExpandedRowData);

    std::vector<std::vector<W>> buckets;
    for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
        const W * partitionBuckets = bucketsUnrolled.data() + numActiveBuckets * i;
        buckets.emplace_back(partitionBuckets, partitionBuckets + numActiveBuckets);
    }

    Date beforeSplitAndRecurse = Date::now();

    std::map<PartitionIndex, ML::Tree::Ptr> newLeaves
        = splitAndRecursePartitioned(depth, maxDepth, tree, serializer,
                                     bucketsUnrolled, numActiveBuckets,
                                     bucketNumbers,
                                     features, activeFeatures,
                                     decodedRows,
                                     partitions, wAll, indexes, fs,
                                     bucketMemory);

    leaves.merge(std::move(newLeaves));

    Date afterSplitAndRecurse = Date::now();

    // Finally, extract a tree from the splits we've been accumulating
    // and our leaves.
    auto result = extractTree(0, maxDepth,
                              tree, PartitionIndex::root(),
                              allSplits, leaves, features, fs);

    Date afterExtract = Date::now();

    cerr << "finished train: finishing tree took " << afterExtract.secondsSince(beforeMapping) * 1000
         << "ms ("
          << beforeSplitAndRecurse.secondsSince(beforeMapping) * 1000 << "ms in mapping and "
          << afterSplitAndRecurse.secondsSince(beforeSplitAndRecurse) * 1000 << "ms in split and recurse)" << endl;

    return result;
}

#endif

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
            result->addParameter("weightFormat", "r", "MLDB::RF::WeightFormat");
            result->addParameter("weightMultiplier", "r", "f32");
            result->addParameter("weightData", "r", "f32[weightDataLength]");
            result->addParameter("decodedRowsOut", "w", "f32[numRows]");

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
                cerr << "maxLocalBuckets = " << maxLocalBuckets << endl;
                //auto maxLocalBuckets = context.getCacheEntry<uint32_t>("maxLocalBuckets");
                kernel.bindArg("w", LocalArray<W>(maxLocalBuckets));
                kernel.bindArg("maxLocalBuckets", maxLocalBuckets);
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
                cerr << "maxLocalBuckets WIndexed = " << maxLocalBuckets << endl;
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
            result->addParameter("featurePartitionSplits", "r", "MLDB::RF::PartitionSplit[np * nf]");
            result->addParameter("allPartitionSplitsOut", "w", "MLDB::RF::PartitionSplit[maxPartitions]");
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
            result->addParameter("partitions", "r", "MLDB::RF::RowPartitionInfo[numRows]");
            result->addParameter("directions", "w", "u8[numRows]");
            result->addParameter("numRows", "r", "u32");
            result->addParameter("allPartitionSplits", "r", "MLDB::RF::PartitionSplit[np]");
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
                cerr << "maxLocalBuckets W = " << maxLocalBuckets << endl;
                kernel.bindArg("wLocal", LocalArray<W>(maxLocalBuckets));
                kernel.bindArg("maxLocalBuckets", maxLocalBuckets);
            };
            result->setParameters(setTheRest);
            result->setComputeFunction(program, "updateBucketsKernel", { 256,1 });
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
