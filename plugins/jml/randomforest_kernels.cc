/** randomforest_kernels.cc                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#include "randomforest_kernels.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/types/structure_description.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/base/map_reduce.h"
#include "mldb/base/scope.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/tuple_description.h"
#include "mldb/types/pair_description.h"
#include "mldb/types/map_description.h"
#include "mldb/utils/environment.h"
#include "mldb/arch/vm.h"
#include <condition_variable>
#include <sstream>

#define OPENCL_ENABLED 1

#if OPENCL_ENABLED
#  define CL_TARGET_OPENCL_VERSION 220
#  include "CL/cl.h"
#  include "mldb/builtin/opencl/opencl_types.h"
#endif

using namespace std;


namespace MLDB {
namespace RF {

std::ostream & operator << (std::ostream & stream, PartitionIndex idx)
{
    return stream << idx.path();
}

size_t roundUpToPageSize(size_t mem)
{
    return (mem + page_size - 1) / page_size * page_size;
}

std::pair<bool, int>
testFeatureKernelOpencl(Rows::RowIterator rowIterator,
                        size_t numRows,
                        const BucketList & buckets,
                        W * w /* buckets.numBuckets entries */);

DecodedRow decodeRow(Rows::RowIterator & rowIterator,
                     size_t rowNumber)
{
    return rowIterator.getDecodedRow();
}

DecodedRow decodeRow(const float * arr,
                     size_t rowNumber)
{
    float f = arr[rowNumber];
    DecodedRow result;
    result.label = f < 0;
    result.exampleNum = rowNumber;
    result.weight = fabs(f);
    return result;
}

// Core kernel of the decision tree search algorithm.  Transfer the
// example weight into the appropriate (bucket,label) accumulator.
// Returns whether or not the feature is still active
template<typename RowIterator, typename BucketList>
std::pair<bool, int>
testFeatureKernelCpu(RowIterator rowIterator,
                     size_t numRows,
                     const BucketList & buckets,
                     W * w /* buckets.numBuckets entries */)
{
    // Minimum bucket number we've seen
    int minBucket = INT_MAX;
    
    // Maximum bucket number we've seen.  Can significantly reduce the
    // work required to search the buckets later on, as those without
    // an example have no possible split point.
    int maxBucket = INT_MIN;

    for (size_t j = 0;  j < numRows;  ++j) {
        DecodedRow r = decodeRow(rowIterator, j);//rowIterator.getDecodedRow();
        int bucket = buckets[r.exampleNum];
        //ExcAssertLess(bucket, buckets.numBuckets);

        w[bucket].add(r.label, r.weight);
        maxBucket = std::max(maxBucket, bucket);
        minBucket = std::min(minBucket, bucket);
    }

    return { minBucket < maxBucket, maxBucket };
}

std::pair<bool, int>
testFeatureKernel(Rows::RowIterator rowIterator,
                  size_t numRows,
                  const BucketList & buckets,
                  W * w /* buckets.numBuckets entries */)
{
    return testFeatureKernelCpu(rowIterator, numRows, buckets, w);
    //return testFeatureKernelOpencl(rowIterator, numRows, buckets, w);
}

std::pair<bool, int>
testFeatureKernel(const float * decodedRows,
                  size_t numRows,
                  const BucketList & buckets,
                  W * w /* buckets.numBuckets entries */)
{
    return testFeatureKernelCpu(decodedRows, numRows, buckets, w);
    //return testFeatureKernelOpencl(rowIterator, numRows, buckets, w);
}

std::pair<bool, int>
testFeatureKernel(Rows::RowIterator rowIterator,
                  size_t numRows,
                  const uint32_t * buckets,
                  W * w /* buckets.numBuckets entries */)
{
    return testFeatureKernelCpu(rowIterator, numRows, buckets, w);
    //return testFeatureKernelOpencl(rowIterator, numRows, buckets, w);
}

std::pair<bool, int>
testFeatureKernel(Rows::RowIterator rowIterator,
                  size_t numRows,
                  const uint16_t * buckets,
                  W * w /* buckets.numBuckets entries */)
{
    return testFeatureKernelCpu(rowIterator, numRows, buckets, w);
    //return testFeatureKernelOpencl(rowIterator, numRows, buckets, w);
}

#if OPENCL_ENABLED

EnvOption<bool> DEBUG_RF_OPENCL_KERNELS("DEBUG_RF_OPENCL_KERNELS", 0);
EnvOption<bool> RF_SEPARATE_FEATURE_UPDATES("RF_SEPARATE_FEATURE_UPDATES", 0);
EnvOption<bool> RF_EXPAND_FEATURE_BUCKETS("RF_EXPAND_FEATURE_BUCKETS", 0);
EnvOption<bool> RF_OPENCL_SYNCHRONOUS_LAUNCH("RF_OPENCL_SYNCHRONOUS_LAUNCH", 1);
EnvOption<bool> RF_USE_OPENCL("RF_USE_OPENCL", 1);
EnvOption<size_t, true> RF_NUM_ROW_KERNELS("RF_NUM_ROW_KERNELS", 65536);
EnvOption<size_t, true> RF_ROW_KERNEL_WORKGROUP_SIZE("RF_ROW_KERNEL_WORKGROUP_SIZE", 256);

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


OpenCLProgram getTestFeatureProgramOpenCL()
{
    static auto devices = getOpenCLDevices();

    ExcAssertGreater(devices.size(), 0);
    
    OpenCLContext context(devices);
    
    Bitset<OpenCLCommandQueueProperties> queueProperties
        = { OpenCLCommandQueueProperties::PROFILING_ENABLE };
    if (!RF_OPENCL_SYNCHRONOUS_LAUNCH) {
        queueProperties.set
            (OpenCLCommandQueueProperties::OUT_OF_ORDER_EXEC_MODE_ENABLE);
    }
    
    auto queue = context.createCommandQueue
        (devices.at(0), queueProperties);

    filter_istream stream("mldb/plugins/jml/randomforest_kernels.cl");
    Utf8String source(stream.readAll());
    
    OpenCLProgram program = context.createProgram(source);

    //string options = "-cl-kernel-arg-info -cl-nv-maxrregcount=32 -cl-nv-verbose";// -cl-mad-enable -cl-fast-relaxed-math -cl-unsafe-math-optimizations -DFloat=" + type_name<Float>();
    string options = "-cl-kernel-arg-info -DW=W64";

    // Build for all devices
    auto buildInfo = program.build(devices, options);
    
    cerr << jsonEncode(buildInfo[0]) << endl;

    std::string binary = program.getBinary();

    filter_ostream stream2("randomforest_kernels.obj");
    stream2 << binary;
    
    return program;
}


std::pair<bool, int>
testFeatureKernelOpencl(Rows::RowIterator rowIterator,
                        size_t numRows,
                        const BucketList & buckets,
                        W * w /* buckets.numBuckets entries */)
{
    //numRows = 10;

    static std::mutex mutex;
    std::unique_lock<std::mutex> guard(mutex);
    
    static OpenCLProgram program = getTestFeatureProgramOpenCL();

    OpenCLContext context = program.getContext();
    
    OpenCLKernel kernel
        = program.createKernel("testFeatureKernel");

    //cerr << "row data of " << rowIterator.owner->rowData.memusage()
    //     << " bytes and " << numRows << " rows" << endl;
    
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


    int minMax[2] = { INT_MAX, INT_MIN };
    
    OpenCLMemObject clMinMaxOut
        = context.createBuffer(CL_MEM_READ_WRITE,
                               minMax,
                               sizeof(minMax[0]) * 2);

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
    
    auto devices = context.getDevices();
    
    auto queue = context.createCommandQueue
        (devices[0],
         OpenCLCommandQueueProperties::PROFILING_ENABLE);

    Date before = Date::now();
    
    OpenCLEvent runKernel
        = queue.launch(kernel,
                       { workGroupSize, 1 },
                       { 256, 1 });

    queue.flush();
    
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

    Date after = Date::now();

    cerr << "gpu took " << after.secondsSince(before) * 1000 << "ms" << endl;
    
#if 0    
    for (size_t i = 0;  i < 10 && i < buckets.numBuckets;  ++i) {
        cerr << "w[" << i << "] = (" << w[i].v[0] << " " << w[i].v[1] << ")"
             << endl;
    }
#endif

    bool active = (minMax[0] != minMax[1]);
    int maxBucket = minMax[1];
    
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

// Chooses which is the best split for a given feature.
std::tuple<double /* bestScore */,
           int /* bestSplit */,
           W /* bestLeft */,
           W /* bestRight */>
chooseSplitKernel(const W * w /* at least maxBucket + 1 entries */,
                  int maxBucket,
                  bool ordinal,
                  const W & wAll,
                  bool debug)
{
    double bestScore = INFINITY;
    int bestSplit = -1;
        
    W bestLeft;
    W bestRight;

    debug = debug || (wAll.count() == 2315 && maxBucket == 256 && false);
    if (debug) {
        cerr << "chooseSplitKernel maxBucket " << maxBucket << " wAll "
             << jsonEncodeStr(wAll) << " ordinal " << ordinal << endl;
    }

    
    if (ordinal) {
        // Calculate best split point for ordered values
        W wFalse = wAll, wTrue;

        // Now test split points one by one
        for (unsigned j = 0;  j <= maxBucket;  ++j) {

            //if (w[j].empty())
            //    continue;                   

            if (wFalse.count() > 0 && wTrue.count() > 0) {
            
                double s = scoreSplit(wFalse, wTrue);

                if (debug) {
                    std::cerr << "  ord split " << j << " "
                        //                              << features.info->bucketDescriptions.getValue(j)
                              << " had score " << s << std::endl;
                    cerr << "    false: " << jsonEncodeStr(wFalse) << endl;
                    cerr << "    true:  " << jsonEncodeStr(wTrue) << endl;
                }
                
                if (s < bestScore) {
                    bestScore = s;
                    bestSplit = j;
                    bestRight = wFalse;
                    bestLeft = wTrue;
                }
            }
            
            if (j < maxBucket && w[j].count() != 0) {
                wFalse -= w[j];
                wTrue += w[j];
            }
        }
    }
    else {
        // Calculate best split point for non-ordered values
        // Now test split points one by one

        for (unsigned j = 0;  j <= maxBucket;  ++j) {
                    
            if (w[j].empty())
                continue;
            
            W wFalse = wAll;
            wFalse -= w[j];                    

            if (wFalse.count() == 0 || w[j].count() == 0) {
                continue;
            }
            
            double s = scoreSplit(wFalse, w[j]);

            if (debug) {
                std::cerr << "  non ord split " << j << " "
                    //    << features.info->bucketDescriptions.getValue(j)
                          << " had score " << s << std::endl;
                    cerr << "    false: " << jsonEncodeStr(wFalse) << endl;
                    cerr << "    true:  " << jsonEncodeStr(w[j]) << endl;
            }
                    
            if (s < bestScore) {
                bestScore = s;
                bestSplit = j;
                bestRight = wFalse;
                bestLeft = w[j];
            }
        }

    }

    return { bestScore, bestSplit, bestLeft, bestRight };
}

std::tuple<double /* bestScore */,
           int /* bestSplit */,
           W /* bestLeft */,
           W /* bestRight */,
           bool /* feature is still active */ >
testFeatureNumber(int featureNum,
                  const std::vector<Feature> & features,
                  Rows::RowIterator rowIterator,
                  size_t numRows,
                  const W & wAll,
                  bool debug)
{
    const Feature & feature = features.at(featureNum);
    const BucketList & buckets = feature.buckets;
    int nb = buckets.numBuckets;

    std::vector<W> w(nb);
    int maxBucket = -1;

    double bestScore = INFINITY;
    int bestSplit = -1;
    W bestLeft;
    W bestRight;

    if (!feature.active)
        return std::make_tuple(bestScore, bestSplit, bestLeft, bestRight, false);

    // Is s feature still active?
    bool isActive;

    std::tie(isActive, maxBucket)
        = testFeatureKernel(rowIterator, numRows,
                            buckets, w.data());

    if (isActive) {
        std::tie(bestScore, bestSplit, bestLeft, bestRight)
            = chooseSplitKernel(w.data(), maxBucket, feature.ordinal,
                                wAll, debug);
    }

    return { bestScore, bestSplit, bestLeft, bestRight, isActive };
}

std::tuple<double, int, int, W, W, std::vector<uint8_t> >
testAllCpu(int depth,
           const std::vector<Feature> & features,
           const Rows & rows,
           FrozenMemoryRegionT<uint32_t> bucketMemory,
           bool trace)
{
    std::vector<uint8_t> newActive;
    for (auto & f: features) {
        newActive.push_back(f.active);
    }

    // We have no impurity in our bucket.  Time to stop
    if (rows.wAll[0] == 0 || rows.wAll[1] == 0) {
        return std::make_tuple(1.0, -1, -1, rows.wAll, W(),
                               std::vector<uint8_t>(features.size(), false));
    }

    bool debug = trace;

    int nf = features.size();

    size_t totalNumBuckets = 0;
    size_t activeFeatures = 0;

    for (unsigned i = 0;  i < nf;  ++i) {
        if (!features[i].active)
            continue;
        ++activeFeatures;
        totalNumBuckets += features[i].buckets.numBuckets;
    }

    if (debug) {
        std::cerr << "total of " << totalNumBuckets << " buckets" << std::endl;
        std::cerr << activeFeatures << " of " << nf << " features active"
                  << std::endl;
    }

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
            double score = std::get<0>(val);

#if 0
            cerr << "CPU: rows "
                 << rows.rowCount() << " wAll " << jsonEncodeStr(rows.wAll)
                 << " feature " << feature << " score "
                 << score << " split " << std::get<1>(val)
                 << " left " << jsonEncodeStr(std::get<2>(val))
                 << " right " << jsonEncodeStr(std::get<3>(val))
                 << endl;
#endif
            
            if (score < bestScore) {
                //cerr << "  *** best" << endl;
                bestFeature = feature;
                std::tie(bestScore, bestSplit, bestLeft, bestRight) = val;
            }
        };

    // Parallel map over all features
    auto doFeature = [&] (int i)
        {
            if (debug)
                cerr << "doing feature " << i << features[i].info->columnName << endl;
            double score;
            int split = -1;
            W bestLeft, bestRight;

            Rows::RowIterator rowIterator = rows.getRowIterator();

            bool stillActive;
            std::tie(score, split, bestLeft, bestRight, stillActive)
                = testFeatureNumber(i, features, rowIterator,
                                    rows.rowCount(), rows.wAll, debug);
            if (debug)
                cerr << "  score " << score << " split " << split << endl;
            newActive[i] = stillActive;

            return std::make_tuple(score, split, bestLeft, bestRight);
        };

#if 0
    for (unsigned i = 0;  i < nf;  ++i) {
        if (depth < 3) {
            if (!features[i].active)
                continue;
            Date beforeCpu = Date::now();
            findBest(i, doFeature(i));
            Date afterCpu = Date::now();

            std::ostringstream str;
            str << "    feature " << i << " buckets "
                << features[i].buckets.numBuckets
                << " CPU took "
                << afterCpu.secondsSince(beforeCpu) * 1000.0
                << "ms" << endl;
            cerr << str.str();
        }
        else {
            findBest(i, doFeature(i));
        }
    }
#else
    if (!debug && (depth < 4 || (uint64_t)rows.rowCount() * (uint64_t)nf > 20000)) {
        parallelMapInOrderReduce(0, nf, doFeature, findBest);
    }
    else {
        for (unsigned i = 0;  i < nf;  ++i)
            findBest(i, doFeature(i));
    }
#endif
    
    int bucketsEmpty = 0;
    int bucketsOne = 0;
    int bucketsBoth = 0;

#if 0        
    for (auto & wt: w[i]) {
        //wAll += wt;
        bucketsEmpty += wt[0] == 0 && wt[1] == 0;
        bucketsBoth += wt[0] != 0 && wt[1] != 0;
        bucketsOne += (wt[0] == 0) ^ (wt[1] == 0);
    }
#endif
            
    if (debug) {
        std::cerr << "buckets: empty " << bucketsEmpty << " one " << bucketsOne
                  << " both " << bucketsBoth << std::endl;
        std::cerr << "bestScore " << bestScore << std::endl;
        std::cerr << "bestFeature " << bestFeature << " "
                  << features[bestFeature].info->columnName << std::endl;
        std::cerr << "bestSplit " << bestSplit << " "
                  << features[bestFeature].info->bucketDescriptions.getSplit(bestSplit)
                  << std::endl;
    }

    return std::make_tuple(bestScore, bestFeature, bestSplit,
                           bestLeft, bestRight, newActive);
}

#if OPENCL_ENABLED

struct OpenCLKernelContext {
    OpenCLContext context;
    std::vector<OpenCLDevice> devices;
    OpenCLProgram program;
    OpenCLCommandQueue queue;
};

OpenCLKernelContext getKernelContext()
{
    static const OpenCLProgram program = getTestFeatureProgramOpenCL();

    OpenCLContext context = program.getContext();

    auto devices = context.getDevices();

    auto queue = context.createCommandQueue
        (devices[0],
         OpenCLCommandQueueProperties::PROFILING_ENABLE);

    
    OpenCLKernelContext result;
    result.program = program;
    result.context = program.getContext();
    result.devices = context.getDevices();
    result.queue = queue;
    return result;
}

std::tuple<double, int, int, W, W, std::vector<uint8_t> >
testAllOpenCL(int depth,
              const std::vector<Feature> & features,
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
#endif // OPENCL_ENABLED

std::tuple<double, int, int, W, W, std::vector<uint8_t> >
testAll(int depth,
        const std::vector<Feature> & features,
        const Rows & rows,
        FrozenMemoryRegionT<uint32_t> bucketMemory,
        bool trace)
{
    if (rows.rowCount() < 10000 && false) {
        if (depth < 3 && false) {
            //static std::mutex mutex;
            //std::unique_lock<std::mutex> guard(mutex);

            Date beforeCpu = Date::now();
            auto res = testAllCpu(depth, features, rows, bucketMemory, trace);
            Date afterCpu = Date::now();

            int activeFeatures = 0;
            int activeBuckets = 0;

            for (auto & f: features) {
                if (!f.active)
                    continue;
                ++activeFeatures;
                activeBuckets += f.buckets.numBuckets;
            }
            
            std::ostringstream str;
            str << "depth " << depth << " rows " << rows.rowCount()
                << " features " << activeFeatures
                << " buckets " << activeBuckets << " CPU took "
                << afterCpu.secondsSince(beforeCpu) * 1000.0
                << "ms" << endl;
            cerr << str.str();
            return res;
        }
        else {
            return testAllCpu(depth, features, rows, bucketMemory, trace);
        }
    }
    else {
#if OPENCL_ENABLED
        throw MLDB::Exception("OpenCL is disabled");
        static constexpr int MAX_GPU_JOBS = 4;
        static std::atomic<int> numGpuJobs = 0;
        

        if (numGpuJobs.fetch_add(1) >= MAX_GPU_JOBS) {
            --numGpuJobs;
            return testAllCpu(depth, features, rows, bucketMemory, false);
        }
        else {
            auto onExit = ScopeExit([&] () noexcept { --numGpuJobs; });
            try {
                return testAllOpenCL(depth, features, rows, bucketMemory);
            } MLDB_CATCH_ALL {
                return testAllCpu(depth, features, rows, bucketMemory, false);
            }
        }
        
        static std::mutex mutex;
        std::unique_lock<std::mutex> guard(mutex);

        Date beforeCpu = Date::now();
        auto cpuOutput = testAllCpu(depth, features, rows, bucketMemory, false);
        Date afterCpu = Date::now();
        auto gpuOutput = testAllOpenCL(depth, features, rows, bucketMemory);
        Date afterGpu = Date::now();

        ostringstream str;
        str << "CPU took " << afterCpu.secondsSince(beforeCpu) * 1000.0
            << "ms; GPU took " << afterGpu.secondsSince(afterCpu) * 1000.0
            << "ms" << endl;
        cerr << str.str();

        if (cpuOutput != gpuOutput) {
            cerr << "difference in outputs: " << endl;
            cerr << "rows.rowCount() = " << rows.rowCount() << endl;
            cerr << "depth = " << depth << endl;
            cerr << "cpu: " << jsonEncode(cpuOutput) << endl;
            cerr << "gpu: " << jsonEncode(gpuOutput) << endl;
            abort();
        }

        return cpuOutput;
#endif
    }
}


/*****************************************************************************/
/* RECURSIVE RANDOM FOREST KERNELS                                           */
/*****************************************************************************/

struct PartitionIndexDescription
    : public ValueDescriptionT<PartitionIndex> {
    
    virtual void parseJsonTyped(PartitionIndex * val,
                                JsonParsingContext & context) const
    {
        val->index = context.expectInt();
    }
    
    virtual void printJsonTyped(const PartitionIndex * val,
                                JsonPrintingContext & context) const
    {
        context.writeString(val->path());
    }

    virtual bool isDefaultTyped(const PartitionIndex * val) const
    {
        return *val == PartitionIndex::none();
    }
};

DEFINE_VALUE_DESCRIPTION_NS(PartitionIndex,
                            PartitionIndexDescription);


DEFINE_STRUCTURE_DESCRIPTION_INLINE(PartitionSplit)
{
    addField("score", &PartitionSplit::score, "");
    addField("feature", &PartitionSplit::feature, "");
    addField("value", &PartitionSplit::value, "");
    addField("left", &PartitionSplit::left, "");
    addField("right", &PartitionSplit::right, "");
    addField("direction", &PartitionSplit::direction, "");
    addField("index", &PartitionSplit::index, "");
}

void updateBuckets(const std::vector<Feature> & features,
                   std::vector<uint32_t> & partitions,
                   std::vector<std::vector<W> > & buckets,
                   std::vector<W> & wAll,
                   const std::vector<uint32_t> & bucketOffsets,
                   const std::vector<PartitionSplit> & partitionSplits,
                   const std::vector<std::pair<int32_t, int32_t> > & newPartitionNumbers,
                   int newNumPartitions,
                   const std::vector<float> & decodedRows,
                   const std::vector<int> & activeFeatures)
{
    int numPartitions = buckets.size();

    if (numPartitions == 0)
        return;
    
    ExcAssertEqual(numPartitions, buckets.size());
    ExcAssertEqual(numPartitions, wAll.size());
    ExcAssertEqual(numPartitions, partitionSplits.size());
    ExcAssertEqual(numPartitions, newPartitionNumbers.size());
                       
    int numActiveBuckets = buckets[0].size();

    buckets.resize(std::max(buckets.size(), (size_t)newNumPartitions));
    wAll.resize(std::max(buckets.size(), (size_t)newNumPartitions));

    // Keep track of which partition number each of our slots has in it
    std::vector<int> currentlyContains(buckets.size(), -1);

    // Keep track of which place each of our partitions currently is
    std::vector<int> currentLocations(buckets.size(), -1);

    //cerr << "newPartitionNumbers = " << jsonEncodeStr(newPartitionNumbers)
    //     << endl;

    {
        std::vector<std::vector<W> > newBuckets(newNumPartitions);
        std::vector<W> newWAll(newNumPartitions);
    
        // Distribute old ones to their new place
        for (size_t i = 0;  i < numPartitions;  ++i) {
            if (newPartitionNumbers[i].first == -1)
                continue;
            if (partitionSplits[i].left.count() == 0
                || partitionSplits[i].right.count() == 0)
                continue;

            int to = partitionSplits[i].direction
                ? newPartitionNumbers[i].second
                : newPartitionNumbers[i].first;

            newBuckets[to] = std::move(buckets[i]);
            newWAll[to] = wAll[i];
        }

        // Clear new buckets
        for (size_t i = 0;  i < newNumPartitions;  ++i) {
            if (newBuckets[i].empty()) {
                newBuckets[i].resize(numActiveBuckets);
            }
        }

        newBuckets.swap(buckets);
        newWAll.swap(wAll);
    }
        
    constexpr bool checkPartitionCounts = false;

    // Make sure that the number in the buckets is actually what we
    // expected when we calculated the split.
    if (checkPartitionCounts) {
        bool different = false;

        for (int i = 0;  i < partitionSplits.size();  ++i) {
            int leftPartition = newPartitionNumbers[i].first;
            int rightPartition = newPartitionNumbers[i].second;

            if (leftPartition == -1 || rightPartition == -1) {
                ExcAssertEqual(leftPartition, -1);
                ExcAssertEqual(rightPartition, -1);
                ExcAssertEqual(partitionSplits[i].feature, -1);
                continue;
            }

            ExcAssertEqual(wAll[leftPartition].count()
                           + wAll[rightPartition].count(),
                           partitionSplits[i].left.count()
                           + partitionSplits[i].right.count());

#if 0            
            for (auto & f: activeFeatures) {
                W wLeft, wRight;
                int startBucket = bucketOffsets[f];
                int endBucket = bucketOffsets[f + 1];

                for (int b = startBucket; b < endBucket;  ++b) {
                    wLeft += buckets[leftPartition][b];
                    wRight += buckets[rightPartition][b];
                }

                if (wLeft != partitionSplits[i].left) {
                    cerr << "error on partition " << i << " left "
                         << leftPartition << " right " << rightPartition
                         << " feature " << f << ": left "
                         << jsonEncodeStr(wLeft) << " != "
                         << jsonEncodeStr(partitionSplits[i].left)
                         << endl;
                    different = true;
                }
                
                if (wRight != partitionSplits[i].right) {
                    cerr << "error on partition " << i << " left "
                         << leftPartition << " right " << rightPartition
                         << " feature " << f << ": right "
                         << jsonEncodeStr(wRight) << " != "
                         << jsonEncodeStr(partitionSplits[i].right)
                         << endl;
                    different = true;
                }
            }
#endif
        }

        ExcAssert(!different);
    }

    uint32_t rowCount = decodedRows.size();
        
    std::vector<uint32_t> numInPartition(buckets.size());
        
    for (size_t i = 0;  i < rowCount;  ++i) {
        int partition = partitions[i];

        // Example is not in a partition
        if (partition == -1)
            continue;

        int splitFeature = partitionSplits[partition].feature;
                
        if (splitFeature == -1) {
            // reached a leaf here, nothing to split                    
            partitions[i] = -1;
            continue;
        }

        float weight = fabs(decodedRows[i]);
        bool label = decodedRows[i] < 0;
        int exampleNum = i;
            
        int leftPartition = newPartitionNumbers[partition].first;
        int rightPartition = newPartitionNumbers[partition].second;

        if (leftPartition == -1)
            continue;
        
        int splitValue = partitionSplits[partition].value;
        bool ordinal = features[splitFeature].ordinal;
        int bucket = features[splitFeature].buckets[exampleNum];
        //int bucket = featureBuckets[splitFeature][exampleNum];
        int side = ordinal ? bucket >= splitValue : bucket != splitValue;

        // Set the new partition number
        partitions[i] = side ? rightPartition : leftPartition;
            
        //cerr << "row " << i << " partition " << partition << " --> "
        //     << partitions[i] << " wt " << weight << " lbl "
        //     << label << " side " << side << endl;
        
        // Verify partition counts?
        if (checkPartitionCounts)
            ++numInPartition[partitions[i]];
            
        // 0 = left to right, 1 = right to left
        int direction = partitionSplits[partition].direction;

        // We only need to update features on the wrong side, as we
        // transfer the weight rather than sum it from the
        // beginning.  This means less work for unbalanced splits
        // (which are typically most of them, especially for discrete
        // buckets)

        if (direction != side) {
            int fromPartition = direction ? rightPartition : leftPartition;
            int toPartition = direction ? leftPartition : rightPartition;

            //cerr << "row " << i << " side " << side << " direction "
            //     << direction << " from " << fromPartition
            //     << " to " << toPartition << " l " << leftPartition
            //     << " r " << rightPartition
            //     << " wAll " << jsonEncodeStr(wAll[fromPartition])
            //     << " " << jsonEncodeStr(wAll[toPartition])
            //     << endl;

            if (checkPartitionCounts) {
                ExcAssertGreater(wAll[fromPartition].count(), 0);
            }
            
            // Update the wAll, transfering weight
            //wAll[fromPartition].sub(label, weight);
            wAll[toPartition  ].add(label, weight);
                    
            // Transfer the weight from each of the features
            for (auto & f: activeFeatures) {
                int startBucket = bucketOffsets[f];
                int bucket = features[f].buckets[exampleNum];
                
                //cerr << "  feature " << f << " bucket " << bucket
                //     << " offset " << startBucket + bucket << " lbl "
                //     << label << " weight " << weight << endl;

                if (checkPartitionCounts) {
                    if (buckets[fromPartition][startBucket + bucket].count() == 0) {
                        cerr << "  feature " << f << " from "
                             << jsonEncodeStr(buckets[fromPartition][startBucket + bucket])
                             << " to " << jsonEncodeStr(buckets[toPartition][startBucket + bucket])
                             << endl;
                    }
                    
                    ExcAssertGreater
                        (buckets[fromPartition][startBucket + bucket].count(),
                         0);
                }
                
                //buckets[fromPartition][startBucket + bucket]
                //    .sub(label, weight);
                buckets[toPartition  ][startBucket + bucket]
                    .add(label, weight);
            }
        }               
    }

    // Fix up all of the "from" buckets by subtracting the accumulated
    // counts from the "to" buckets.  This saves lots of work since we
    // only need to transfer the aggregate weight, not update row by row
    for (int i = 0;  i < partitionSplits.size();  ++i) {
        int leftPartition = newPartitionNumbers[i].first;
        int rightPartition = newPartitionNumbers[i].second;
        
        if (leftPartition == -1 || rightPartition == -1) {
            continue;
        }

        int fromPartition
            = partitionSplits[i].direction
            ? rightPartition : leftPartition;
        int toPartition
            = partitionSplits[i].direction
            ? leftPartition : rightPartition;

        wAll[fromPartition] -= wAll[toPartition];

        for (auto & f: activeFeatures) {
            int startBucket = bucketOffsets[f];
            int endBucket = bucketOffsets[f + 1];

            for (int b = startBucket; b < endBucket;  ++b) {
                if (buckets[toPartition][b].count() == 0)
                    continue;
                buckets[fromPartition][b] -= buckets[toPartition][b];
            }
        }
    }
    
    // Make sure that the number in the buckets is actually what we
    // expected when we calculated the split.
    if (checkPartitionCounts) {
        cerr << "numInPartition = " << jsonEncodeStr(numInPartition) << endl;
        
        bool different = false;

        for (int i = 0;  i < partitionSplits.size();  ++i) {
            int leftPartition = newPartitionNumbers[i].first;
            int rightPartition = newPartitionNumbers[i].second;

            if (leftPartition == -1 || rightPartition == -1) {
                ExcAssertEqual(leftPartition, -1);
                ExcAssertEqual(rightPartition, -1);
                ExcAssertEqual(partitionSplits[i].feature, -1);
                continue;
            }
            
            if (numInPartition[leftPartition]
                != partitionSplits[i].left.count()
                || numInPartition[rightPartition]
                != partitionSplits[i].right.count()) {
                using namespace std;
                    
                cerr << "PARTITION COUNT MISMATCH" << endl;
                cerr << "expected: left "
                     << partitionSplits[i].left.count()
                     << " right "
                     << partitionSplits[i].right.count() << endl;
                cerr << "got:      left " << numInPartition[leftPartition]
                     << " right " << numInPartition[rightPartition]
                     << endl;

                cerr << "feature " << partitionSplits[i].feature
                     << " " << features[partitionSplits[i].feature].info->columnName
                     << " bucket " << partitionSplits[i].value
                     << " " << features[partitionSplits[i].feature]
                    .info->bucketDescriptions.getSplit(partitionSplits[i].value)
                     << " ordinal " << features[partitionSplits[i].feature].ordinal
                     << endl;


            }
                
            ExcAssertEqual(numInPartition[leftPartition],
                           partitionSplits[i].left.count());
            ExcAssertEqual(numInPartition[rightPartition],
                           partitionSplits[i].right.count());

            for (auto & f: activeFeatures) {
                W wLeft, wRight;
                int startBucket = bucketOffsets[f];
                int endBucket = bucketOffsets[f + 1];

                for (int b = startBucket; b < endBucket;  ++b) {
                    wLeft += buckets[leftPartition][b];
                    wRight += buckets[rightPartition][b];
                }

                if (wLeft != partitionSplits[i].left) {
                    cerr << "error on partition " << i << " left "
                         << leftPartition << " right " << rightPartition
                         << " feature " << f << ": left "
                         << jsonEncodeStr(wLeft) << " != "
                         << jsonEncodeStr(partitionSplits[i].left)
                         << endl;
                    different = true;
                }
                
                if (wRight != partitionSplits[i].right) {
                    cerr << "error on partition " << i << " left "
                         << leftPartition << " right " << rightPartition
                         << " feature " << f << ": right "
                         << jsonEncodeStr(wRight) << " != "
                         << jsonEncodeStr(partitionSplits[i].right)
                         << endl;
                    different = true;
                }
            }
            
        }

        ExcAssert(!different);
    }
}

std::pair<std::vector<PartitionEntry>,
          FrozenMemoryRegionT<uint32_t> >
splitPartitions(const std::vector<Feature> features,
                const std::vector<int> & activeFeatures,
                const std::vector<float> & decodedRows,
                const std::vector<uint32_t> & partitions,
                const std::vector<W> & w,
                const std::vector<PartitionIndex> & indexes,
                MappedSerializer & serializer)
{
    using namespace std;

    int numPartitions = w.size();
    int numRows = decodedRows.size();

    std::vector<PartitionEntry> out(numPartitions);
        
    MutableMemoryRegionT<uint32_t> mutablePartitionMemory;
    std::vector<size_t> partitionMemoryOffsets = { 0 };
    size_t partitionMemoryOffset = 0;
    FrozenMemoryRegionT<uint32_t> partitionMemory;

#if 0
    std::vector<uint32_t> partitionRowCounts(numPartitions);

    for (auto & p: partitions)
        ++partitionRowCounts[p];

    size_t rows1 = 0, rows2 = 0;
        
    for (int i = 0;  i < numPartitions;  ++i) {
        //cerr << "part " << i << " count " << w[i].count() << " rows "
        //     << partitionRowCounts[i] << endl;

        ExcAssertEqual(partitionRowCounts[i], w[i].count());

        rows1 += w[i].count();
        rows2 += partitionRowCounts[i];
    }
#endif
        
    for (int i = 0;  i < numPartitions;  ++i) {
        int partitionRowCount = w[i].count();

        if (partitionRowCount == 0) {
            partitionMemoryOffsets.push_back(partitionMemoryOffset);
            continue;
        }

        //cerr << "part " << i << " count " << w[i].count() << " rows "
        //     << partitionRowCounts[i] << endl;

        //ExcAssertEqual(partitionRowCount, partitionRowCounts[i]);

        out[i].decodedRows.reserve(partitionRowCount);
        out[i].activeFeatures = activeFeatures;  // TODO: pass in real
        out[i].activeFeatureSet.insert
            (activeFeatures.begin(), activeFeatures.end());
        out[i].bucketMemoryOffsets = { 0 };
        out[i].index = indexes.at(i);
        
        size_t bucketMemoryRequired = 0;

        out[i].features.resize(features.size());
        for (int f = 0;  f < features.size();  ++f) {
            size_t bytesRequired = 0;
            if (out[i].activeFeatureSet.count(f)) {
                out[i].features[f].info = features[f].info;
                out[i].features[f].ordinal = features[f].ordinal;
                out[i].features[f].active = true;
                size_t wordsRequired
                    = WritableBucketList::wordsRequired
                    (partitionRowCount,
                     features[f].info->distinctValues);
                bytesRequired = wordsRequired * 4;
            }

            bucketMemoryRequired += bytesRequired;
            out[i].bucketMemoryOffsets
                .push_back(bucketMemoryRequired);
        }

        partitionMemoryOffset += bucketMemoryRequired;
        partitionMemoryOffsets.push_back(partitionMemoryOffset);
    }

    mutablePartitionMemory
        = serializer.allocateWritableT<uint32_t>
        (partitionMemoryOffset / 4, page_size);

    for (int i = 0;  i < numPartitions;  ++i) {
        out[i].mutableBucketMemory
            = mutablePartitionMemory
            .rangeBytes(partitionMemoryOffsets[i],
                        partitionMemoryOffsets[i + 1]);

        int partitionRowCount = w[i].count();
        if (partitionRowCount == 0) {
            partitionMemoryOffsets.push_back(partitionMemoryOffset);
            continue;
        }
    }
        
    for (size_t i = 0;  i < numRows;  ++i) {
        int partition = partitions[i];
        out[partition].decodedRows.push_back(decodedRows[i]);
        bool label = decodedRows[i] < 0;
        float weight = fabs(decodedRows[i]);
        out[partition].wAll.add(label, weight);
    }
    
    for (int f: activeFeatures) {
        std::vector<WritableBucketList> partitionFeatures(numPartitions);
        for (int i = 0;  i < numPartitions;  ++i) {
            int partitionRowCount = w[i].count();
            if (partitionRowCount == 0)
                continue;
#if 0
            cerr << "feature " << f << " partition " << i << " of "
                 << numPartitions << " rowCount "
                 << partitionRowCount
                 << " bytes " << out[i].bucketMemoryOffsets[f]
                 << " to " << out[i].bucketMemoryOffsets[f + 1]
                 << endl;
#endif
            partitionFeatures.at(i).init(partitionRowCount,
                                         features.at(f).buckets.numBuckets,
                                         out.at(i).mutableBucketMemory
                                         .rangeBytes(out.at(i).bucketMemoryOffsets.at(f),
                                                     out.at(i).bucketMemoryOffsets.at(f + 1)));
        }

        for (size_t j = 0;  j < numRows;  ++j) {
            int partition = partitions[j];
            partitionFeatures[partition].write(features[f].buckets[j]);
        }

        for (int i = 0;  i < numPartitions;  ++i) {
            int partitionRowCount = w[i].count();
            if (partitionRowCount == 0)
                continue;
            out[i].features[f].buckets
                = partitionFeatures[i].freeze(serializer);
        }
    }

    for (int i = 0;  i < numPartitions;  ++i) {
        int partitionRowCount = w[i].count();
        if (partitionRowCount == 0)
            continue;

        ExcAssertEqual(jsonEncodeStr(w[i]), jsonEncodeStr(out[i].wAll));

        out[i].bucketMemory
            = out[i].mutableBucketMemory.freeze();
    }

    partitionMemory = mutablePartitionMemory.freeze();

    return std::make_pair(std::move(out), std::move(partitionMemory));
}

/** This function takes the W values of each bucket of a number of
    partitions, and calculates the optimal split feature and value
    for each of the partitions.  It's the "search" part of the
    decision tree algorithm, but across a full set of rows split into
    multiple partitions.
*/
std::vector<PartitionSplit>
getPartitionSplits(const std::vector<std::vector<W> > & buckets,  // [np][nb] for each partition, feature buckets
                   const std::vector<int> & activeFeatures,       // [naf] list of feature numbers of active features only (< nf)
                   const std::vector<uint32_t> & bucketOffsets,   // [nf+1] offset in flat bucket list of start of feature
                   const std::vector<Feature> & features,         // [nf] feature info
                   const std::vector<W> & wAll,                   // [np] sum of buckets[0..nb-1] for each partition
                   const std::vector<PartitionIndex> & indexes,   // [np] index of each partition
                   bool parallel)
{
    size_t numPartitions = buckets.size();
    size_t numBuckets = bucketOffsets.back();  // Total num buckets over ALL features
    //cerr << "bucketOffsets = " << jsonEncodeStr(bucketOffsets) << endl;
    for (auto & b: buckets) {
        ExcAssertEqual(b.size(), numBuckets);
    }
    ExcAssertEqual(indexes.size(), numPartitions);
    ExcAssertEqual(wAll.size(), numPartitions);

    std::vector<PartitionSplit> partitionSplits(numPartitions);

    for (int partition = 0;  partition < numPartitions;  ++partition) {

        double bestScore = INFINITY;
        int bestFeature = -1;
        int bestSplit = -1;
                
        W bestLeft;
        W bestRight;
        
        // Reduction over the best split that comes in feature by
        // feature; we find the best global split score and store
        // it.  This is done in order to be sure that we
        // deterministically pick the right one.
        auto findBest = [&] (int af,
                             const std::tuple<int, double, int, W, W>
                             & val)
            {
                bool debug = false; //partition == 3 && buckets.size() == 8 && activeFeatures[af] == 4;

                double score = std::get<1>(val);

                if (score == INFINITY) return;

                if (debug) {
                    cerr << "part " << partition << " af " << af
                         << " f " << std::get<0>(val)
                         << " score " << std::get<1>(val) << " split "
                         << std::get<2>(val)
                         << endl;
                    cerr << "    score " << std::get<1>(val) << " "
                         << features[std::get<0>(val)].info->columnName
                         << " "
                         << features[std::get<0>(val)]
                        .info->bucketDescriptions.getSplit(std::get<2>(val))
                         << " l " << jsonEncodeStr(std::get<3>(val)) << " r "
                         << jsonEncodeStr(std::get<4>(val)) << endl;
                }
                
                if (score < bestScore) {
                    //cerr << "*** best" << endl;
                    std::tie(bestFeature, bestScore, bestSplit, bestLeft,
                             bestRight) = val;
                }
            };
            
        // Finally, we re-split
        auto doFeature = [&] (int af)
            {
                int f = activeFeatures.at(af);
                bool isActive = true;
                double bestScore = INFINITY;
                int bestSplit = -1;
                W bestLeft;
                W bestRight;

                bool debug = false; // partition == 3 && buckets.size() == 8 && activeFeatures[af] == 4;

                if (isActive && !buckets[partition].empty()
                    && wAll[partition].v[0] != 0.0 && wAll[partition].v[1] != 0.0) {
                    int startBucket = bucketOffsets[f];
                    int endBucket MLDB_UNUSED = bucketOffsets[f + 1];
                    int maxBucket = endBucket - startBucket - 1;
                    const W * wFeature
                        = buckets[partition].data() + startBucket;
                    std::tie(bestScore, bestSplit, bestLeft, bestRight)
                        = chooseSplitKernel(wFeature, maxBucket,
                                            features[f].ordinal,
                                            wAll[partition], debug);
                }

                //cerr << " score " << bestScore << " split "
                //     << bestSplit << endl;
                        
                return std::make_tuple(f, bestScore, bestSplit,
                                       bestLeft, bestRight);
            };
            

        if (parallel) {
            parallelMapInOrderReduce(0, activeFeatures.size(),
                                     doFeature, findBest);
        }
        else {
            for (size_t i = 0;  i < activeFeatures.size();  ++i) {
                findBest(i, doFeature(i));
            }
        }

        partitionSplits[partition] =
            { indexes.at(partition) /* index */,
              (float)bestScore, bestFeature, bestSplit,
              bestLeft, bestRight,
              bestFeature != -1 && bestLeft.count() <= bestRight.count()  // direction
            };

#if 0
        cerr << "partition " << partition << " of " << buckets.size()
             << " with " << wAll[partition].count()
             << " rows: " << bestScore << " " << bestFeature
             << " wAll " << jsonEncodeStr(wAll[partition]);
        if (bestFeature != -1) {
            cerr << " " << features[bestFeature].info->columnName
                 << " " << bestSplit
                 << " " << features[bestFeature].info->bucketDescriptions.getSplit(bestSplit);
        }
        cerr << " " << jsonEncodeStr(bestLeft) << " "
             << jsonEncodeStr(bestRight)
             << " dir " << partitionSplits[partition].direction
             << endl;
#endif
    }

    return partitionSplits;
}


// Check that the partition counts match the W counts.
void
verifyPartitionBuckets(const std::vector<uint32_t> & partitions,
                       const std::vector<W> & wAll)
{
    using namespace std;
        
    int numPartitions = wAll.size();
        
    // Check that our partition counts and W scores match
    std::vector<uint32_t> partitionRowCounts(numPartitions);

    for (auto & p: partitions) {
        if (p != -1)
            ++partitionRowCounts[p];
    }

    //for (int i = 0;  i < numPartitions;  ++i) {
    //    cerr << "part " << i << " count " << wAll[i].count() << " rows "
    //         << partitionRowCounts[i] << endl;
    //}

    bool different = false;
    for (int i = 0;  i < numPartitions;  ++i) {
        if (partitionRowCounts[i] != wAll[i].count()) {
            different = true;
            cerr << "error on partition " << i << ": row count "
                 << partitionRowCounts[i] << " wAll count "
                 << wAll[i].count() << endl;
        }
    }
    ExcAssert(!different);
}

// Split our dataset into a separate dataset for each leaf, and
// recurse to create a leaf node for each.  This is mutually
// recursive with trainPartitionedRecursive.
std::map<PartitionIndex, ML::Tree::Ptr>
splitAndRecursePartitioned(int depth, int maxDepth,
                           ML::Tree & tree,
                           MappedSerializer & serializer,
                           std::vector<std::vector<W> > buckets,
                           const std::vector<uint32_t> & bucketOffsets,
                           const std::vector<Feature> & features,
                           const std::vector<int> & activeFeatures,
                           const std::vector<float> & decodedRows,
                           const std::vector<uint32_t> & partitions,
                           const std::vector<W> & wAll,
                           const std::vector<PartitionIndex> & indexes,
                           const DatasetFeatureSpace & fs,
                           FrozenMemoryRegionT<uint32_t> bucketMemory)
{
    size_t numRows = decodedRows.size();
    ExcAssertEqual(partitions.size(), numRows);

    size_t numFeatures = features.size();
    ExcAssertEqual(bucketOffsets.size(), numFeatures + 1);

    size_t numPartitions = buckets.size();
    ExcAssertEqual(wAll.size(), numPartitions);
    ExcAssertEqual(indexes.size(), numPartitions);
    
    std::vector<std::pair<PartitionIndex, ML::Tree::Ptr> > leaves;

    if (depth == maxDepth)
        return { };
        
    leaves.resize(buckets.size()); // TODO: we double copy leaves into result

    // New partitions, per row
    std::vector<PartitionEntry> newData;
    FrozenMemoryRegionT<uint32_t> partitionMem;

    std::tie(newData, partitionMem)
        = splitPartitions(features, activeFeatures, decodedRows,
                          partitions, wAll, indexes, serializer);

    cerr << "splitAndRecursePartitioned: got " << newData.size() << " partitions" << endl;

    auto doEntry = [&] (int i)
        {
            if (newData[i].index == PartitionIndex::none())
                return;

            cerr << "training leaf for index " << newData[i].index << " with " << newData[i].decodedRows.size() << " rows" << endl;

            leaves[i].first = newData[i].index;
            leaves[i].second = trainPartitionedRecursive
                (depth, maxDepth,
                 tree, serializer,
                 bucketOffsets, newData[i].activeFeatures,
                 std::move(buckets[i]),
                 newData[i].decodedRows,
                 newData[i].wAll,
                 newData[i].index,
                 fs,
                 newData[i].features,
                 bucketMemory);
        };

    if (depth <= 8) {
        parallelMap(0, buckets.size(), doEntry);
    }
    else {
        for (size_t i = 0;  i < buckets.size();  ++i) {
            doEntry(i);
        }
    }

    std::map<PartitionIndex, ML::Tree::Ptr> result;
    for (auto index_branch: leaves) {
        PartitionIndex index = index_branch.first;
        ML::Tree::Ptr branch = index_branch.second;
        if (index == PartitionIndex::none())
            continue;
        result.emplace_hint(result.end(), index, branch);
    }

    return result;
}

struct PartitionExample {
    int ex;
    float row;
};

struct PartitionWorkEntry {
#if 0
    PartitionWorkEntry() = default;
    PartitionWorkEntry(PartitionIndex index,
                       PartitionExample * ex,
                       size_t numExamples,
                       W wAll,
                       std::vector<W> buckets)
        : index(index), ex(ex), numExamples(numExamples), wAll(wAll),
          buckets(std::move(buckets))
    {
    }
#endif
    
    PartitionIndex index;
    PartitionExample * ex;
    size_t numExamples;
    W wAll;
    std::vector<W> buckets;
};

// Go depth-first down a small partition until we reach the recursion limit,
// outputting as we go a set of splits and a set of work items to descend
// into.
//
// If buckets are passed in non-empty, they should represent the bucket
// weights of this partition.  Otherwise, they will be initialized at the
// beginning of the recursion.
//
// This function uses constant memory for the entire depth, and will
// create up to 2^d splits and O(d^2) extra work items where
// d = (index.depth() - maxDepth).  This means it is much more efficient in
// memory usage than a breath-first algorithm, which has temporary storage
// requirements in the order of O(2^d) not O(d^2).
//
// It is used as a counterpoint to the breadth-first algorithm for when
// the width is so high that the buckets are very sparse.

std::map<PartitionIndex, PartitionSplit>
descendSmallPartition(PartitionWorkEntry entry,
                      int maxDepth,
                      int numActiveBuckets,
                      const std::vector<Feature> & features,
                      const std::vector<int> & activeFeatures,
                      const std::vector<uint32_t> & bucketOffsets)
{
    PartitionIndex rootIndex = entry.index;
    std::map<PartitionIndex, PartitionSplit> splits;

    if (rootIndex.depth() >= maxDepth)
        return splits;
    
    // This is a stack of work we put off for later and need to come back to.
    // It should never be more than maxDepth - rootIndex.depth() entries, as
    // it traces a path back to the root of the tree.  Typically, work items
    // will get smaller as they are pushed on to this stack, so the last set
    // will be extremely small.
    
    std::vector<PartitionWorkEntry> work = { std::move(entry) };

    while (!work.empty()) {
        PartitionWorkEntry entry = std::move(work.back());
        work.pop_back();

        //cerr << "working on " << entry.index.path() << " with "
        //     << entry.numExamples << " examples and " << work.size()
        //     << " outstanding work" << endl;
        
        PartitionIndex index = entry.index;
        PartitionExample * ex = entry.ex;
        size_t numExamples = entry.numExamples;
        W & wAll = entry.wAll;
        std::vector<W> & buckets = entry.buckets;
        
        // If we weren't passed in our buckets, we re-initialize them here.
        // Passing them in requires much more memory, but saves some work in
        // initializing them.
        if (buckets.empty()) {
            buckets.resize(numActiveBuckets);
            
            for (int f: activeFeatures) {
                //cerr << "initializing feature " << f << " ofs "
                //     << bucketOffsets[f] << endl;
                W * featureBuckets = buckets.data() + bucketOffsets[f];
                for (int i = 0;  i < numExamples;  ++i) {
                    float weight = fabs(ex[i].row);
                    bool label = ex[i].row < 0;
                    int exampleNum = ex[i].ex;
                
                    int bucket = features[f].buckets[exampleNum];

                    //cerr << "ex " << i << " weight " << weight << " lbl "
                    //     << label << " exnum " << exampleNum << " bucket "
                    //     << bucket << endl;

                    featureBuckets[bucket].add(label, weight);
                }

                //for (int i = bucketOffsets[f];  i < bucketOffsets[f + 1];  ++i) {
                //    if (buckets[i].count() > 0) {
                //        cerr << "bucket " << i - bucketOffsets[f]
                //             << " value " << jsonEncodeStr(buckets[i]) << endl;
                //    }
                //}
            }
        }

        // Loop over all depths, keeping to the widest at each point
        for (;;) {
            //cerr << "looping with index " << index.path() << endl;

            static constexpr bool verifyBuckets = false;
            
            if (verifyBuckets) {
                for (int f: activeFeatures) {
                    int numBuckets = bucketOffsets[f + 1] - bucketOffsets[f];
                    std::vector<W> testFeatureBuckets(numBuckets);
                    for (int i = 0;  i < numExamples;  ++i) {
                        float weight = fabs(ex[i].row);
                        bool label = ex[i].row < 0;
                        int exampleNum = ex[i].ex;
                
                        int bucket = features[f].buckets[exampleNum];

                        //cerr << "ex " << i << " weight " << weight << " lbl "
                        //     << label << " exnum " << exampleNum << " bucket "
                        //     << bucket << endl;
                        
                        testFeatureBuckets[bucket].add(label, weight);
                    }

                    const W * featureBuckets = buckets.data() + bucketOffsets[f];

                    bool different = false;
                    
                    for (int i = 0;  i < numBuckets;  ++i) {
                        if (testFeatureBuckets[i] != featureBuckets[i]) {
                            cerr << "difference on feature " << f
                                 << " bucket " << i << ": is "
                                 << jsonEncodeStr(featureBuckets[i])
                                 << " should be "
                                 << jsonEncodeStr(testFeatureBuckets[i])
                                 << endl;
                            different = true;
                        }
                    }

                    ExcAssert(!different);
                }
            }

            
            PartitionSplit split;

            for (int f: activeFeatures) {
                W * featureBuckets = buckets.data() + bucketOffsets[f];
                int maxBucket = bucketOffsets[f + 1] - bucketOffsets[f] - 1;
            
                // Get the best split
                float featureScore;
                int featureSplit;
                W featureLeft, featureRight;
                    
                std::tie(featureScore, featureSplit, featureLeft, featureRight)
                    = chooseSplitKernel(featureBuckets, maxBucket,
                                        features[f].ordinal, wAll);

#if 0                
                cerr << "  feature " << f << " score " << featureScore
                     << " maxBucket " << maxBucket
                     << " feat " << features[f].info->columnName;
                if (featureSplit != -1) {
                    cerr << " "
                         << features[f].info->bucketDescriptions.getSplit(featureSplit);
                }
                cerr << endl;
#endif
                
                if (featureScore < split.score) {
                    //cerr << "    best" << endl;
                    split.score = featureScore;
                    split.feature = f;
                    split.value = featureSplit;
                    split.left = featureLeft;
                    split.right = featureRight;
                }
            }

            bool direction = split.direction
                = split.left.count() >= split.right.count();

            split.index = index;
            
            //cerr << "  split = " << jsonEncodeStr(split) << endl;
            
            if (split.feature == -1)
                break;

            //cerr << "produced split " << jsonEncodeStr(split) << endl;
            
            splits.emplace(index, std::move(split));

            if (index.depth() == maxDepth - 1
                || split.left.count() == 0 || split.right.count() == 0) {
                // We've finished
                break;
            }
                
            // Now we have our best split for the bucket.  We need to
            // split the partition, decide which side we recurse on,
            // push a new work item for the one we don't keep and fix
            // up our buckets.
                
            //cerr << "direction = " << direction << endl;

            const auto & featureBuckets = features[split.feature].buckets;
            bool splitOrdinal = features[split.feature].ordinal;
            
            // Works out which side of a split an element will be on
            auto getSide = [&] (const PartitionExample & example) -> bool
                {
                    int exampleNum = example.ex;
                    int bucket = featureBuckets[exampleNum];
                    int side = splitOrdinal
                        ? bucket >= split.value : bucket != split.value;
                    return side;
                };

            // Partition them into left and right
            PartitionExample * midpoint
                = std::partition(ex, ex + numExamples, getSide);

            PartitionExample * big, * small;
            size_t bigLen, smallLen;
            PartitionIndex bigIndex, smallIndex;
            W bigWAll, smallWAll;
            if (direction == 0) {
                // Biggest on right
                big = ex;
                bigLen = midpoint - ex;
                small = midpoint;
                smallLen = numExamples - bigLen;  

                bigIndex = index.rightChild();
                smallIndex = index.leftChild();
                bigWAll = split.right;
                smallWAll = split.left;
                
                //cerr << "smallLen = " << smallLen << " bigLen = " << bigLen
                //     << endl;

                ExcAssertEqual(bigLen, split.right.count());
                ExcAssertEqual(smallLen, split.left.count());
            }
            else {
                // Smallest on right
                small = ex;
                smallLen = midpoint - ex;
                big = midpoint;
                bigLen = numExamples - smallLen;

                smallIndex = index.rightChild();
                bigIndex = index.leftChild();
                smallWAll = split.right;
                bigWAll = split.left;

                //cerr << "smallLen = " << smallLen << " bigLen = " << bigLen
                //     << endl;

                ExcAssertEqual(smallLen, split.right.count());
                ExcAssertEqual(bigLen, split.left.count());
            }

            ExcAssertGreaterEqual(bigLen, smallLen);
            
            // Finally, update our W for the big one by subtracting all
            // of the values that were moved out for the small length
                
            for (int f: activeFeatures) {
                W * featureBuckets = buckets.data() + bucketOffsets[f];
                for (int i = 0;  i < smallLen;  ++i) {
                    float weight = fabs(small[i].row);
                    bool label = small[i].row < 0;
                    int exampleNum = small[i].ex;
                    
                    int bucket = features[f].buckets[exampleNum];
                    featureBuckets[bucket].sub(label, weight);
                }
            }

            // At this point, we can:
            // - recurse for the big bucket
            // - push a new work item for the small bucket
            work.push_back({smallIndex, small, smallLen, smallWAll,
                           {} /* buckets */});
            
            // Update for next iteration
            index = bigIndex;
            wAll = bigWAll;
            ex = big;
            numExamples = bigLen;
        }
    }

    return splits;
}            

std::map<PartitionIndex, ML::Tree::Ptr>
trainSmallPartitions(int depth, int maxDepth,
                     ML::Tree & tree,
                     const std::vector<Feature> & features,
                     std::vector<uint32_t> & partitions,
                     const std::vector<std::vector<W> > & buckets,
                     const std::vector<uint32_t> & bucketOffsets,
                     const std::vector<float> & decodedRows,
                     const std::vector<int> & activeFeatures,
                     const std::vector<std::pair<int, PartitionSplit> > & smallPartitions,
                     const DatasetFeatureSpace & fs)
{
    std::map<PartitionIndex, ML::Tree::Ptr> result;

    int numActiveBuckets = buckets.empty() ? 0 : buckets[0].size();
    
    // Mapping from partition number to small partition number
    std::vector<int> smallPartitionNumbers(buckets.size(), -1);

    size_t keptRows = 0;

    //for (auto & p: smallPartitions) {
    //    cerr << "small part " << p.first << " split " << jsonEncodeStr(p.second)
    //         << endl;
    //}
    
    // First, split into data structure for each partition    
    for (int i = 0;  i < smallPartitions.size();  ++i) {
        int partitionNumber = smallPartitions[i].first;
        const PartitionSplit & split = smallPartitions[i].second;
        keptRows += split.left.count() + split.right.count();
        smallPartitionNumbers.at(partitionNumber) = i;
    }

    // We now have twice as many partitions... one for the left and
    // one for the right of each
    int numPartitions = smallPartitions.size() * 2;

    // Structure containing the
    struct AugmentedPartitionExample: public PartitionExample {
        AugmentedPartitionExample() = default;
        AugmentedPartitionExample(int ex, float row, int part)
            : PartitionExample{ex,row}, part(part)
        {
        }
        
        int part;  // partition number

        bool operator < (const AugmentedPartitionExample & other) const
        {
            return part < other.part
               || (part == other.part && ex < other.ex);
        }
    };

    //cerr << "smallPartitionNumbers = " << jsonEncodeStr(smallPartitionNumbers)
    //     << endl;
    
    // Now extract for each partition.  This means applying the split
    std::vector<AugmentedPartitionExample> allExamples;
    allExamples.reserve(keptRows);
    
    int rowCount = decodedRows.size();

    for (int i = 0;  i < rowCount;  ++i) {

        int partition = partitions[i];

        // Example is not in a partition
        if (partition == -1)
            continue;

        int smallNum = smallPartitionNumbers.at(partition);

        // Example is not in a small partition we're dealing with
        if (smallNum == -1)
            continue;

        // Update the partition number to say that we're no longer
        // part of it.  TODO: don't require mutable references to be passed
        // in.
        partitions[i] = -1;
        
        int splitFeature = smallPartitions[smallNum].second.feature;

        if (splitFeature == -1) {
            // reached a leaf here, nothing to split                    
            continue;
        }

        int exampleNum = i;
            
        int splitValue = smallPartitions[smallNum].second.value;
        bool ordinal = features[splitFeature].ordinal;
        int bucket = features[splitFeature].buckets[exampleNum];
        int side = ordinal ? bucket >= splitValue : bucket != splitValue;

        int partitionNum = smallNum * 2 + side;

        //cerr << "example " << i << " partition " << partition
        //     << "  smallNum " << smallNum
        //     << "  splitFeature " << splitFeature
        //     << "  kept in partition " << partitionNum << endl;
        
        allExamples.emplace_back(exampleNum, decodedRows[i], partitionNum);
    }

    //cerr << "allExamples.size() = " << allExamples.size()
    //     << " keptRows = " << keptRows << endl;
    
    ExcAssertEqual(allExamples.size(), keptRows);
    
    // Now we have our entire set of examples extracted.  We go through and
    // sort them by partition number.
    std::sort(allExamples.begin(), allExamples.end());

    // Index the example number for the beginning of each partition.  This
    // enables us to know where to to for each one.
    std::vector<int> partitionStarts;
    int currentPartition = -1;

    for (int i = 0;  i < allExamples.size();  ++i) {
        int part = allExamples[i].part;
        //cerr << "row " << i << " example " << allExamples[i].ex
        //     << " in partition " << part << endl;
        while (currentPartition < part) {
            partitionStarts.push_back(i);
            ++currentPartition;
        }
    }

    while (currentPartition < numPartitions) {
        partitionStarts.push_back(allExamples.size());
        ++currentPartition;
    }

    //cerr << "partitionStarts = " << jsonEncodeStr(partitionStarts) << endl;
    
    std::vector<PartitionExample> examples
        { allExamples.begin(), allExamples.end() };
    
    for (int i = 0;  i < numPartitions;  ++i) {
        
        int smallPartitionNum = i / 2;
        int lr = i % 2;

        const PartitionSplit & split
            = smallPartitions.at(smallPartitionNum).second;
        
        PartitionIndex index
            = lr == 0 ? split.index.leftChild() : split.index.rightChild();

        const W & wAll = lr == 0 ? split.left : split.right;

        std::vector<W> buckets;  // TODO: pass this; they can be constructed
        
        PartitionWorkEntry entry {
            index,
            examples.data() + partitionStarts[i],
            static_cast<size_t>(partitionStarts[i + 1] - partitionStarts[i]),
            wAll,
            std::move(buckets)
        };
        
        std::map<PartitionIndex, PartitionSplit> splits
            = descendSmallPartition(entry, maxDepth, numActiveBuckets,
                                    features, activeFeatures, bucketOffsets);

        ML::Tree::Ptr leaf
            = extractTree(depth + 1, maxDepth, tree, index, splits,
                          {} /* leaves */, features, fs);
            
        result[index] = leaf;
    }

    return result;
}

// Maximum number of partitions to handle in parallel in the breadth
// first algorithm?  If it's wider than this, the smaller partitions
// will be handled depth first.
EnvOption<int>
MLDB_RF_CPU_PARTITION_MAX_WIDTH("MLDB_RF_CPU_PARTITION_MAX_WIDTH", 4096);

// Threshold to decide a partition is too small to handle in parallel,
// expressed as a multiple of the number of buckets.  If there are less
// than this number of rows in the bucket, then it will be handled depth
// first in a more memory efficient manner.  The intuition is that there
// is no point keeping large amounts of memory for buckets when there
// are more buckets than examples.  Set to zero to disable the breadth
// first algorithm.
EnvOption<float>
MLDB_RF_CPU_MIN_PARTITION_EXAMPLE_MULTIPLIER
    ("MLDB_RF_CPU_MIN_PARTITION_EXAMPLE_MULTIPLIER", 1.0);

ML::Tree::Ptr
trainPartitionedRecursiveCpu(int depth, int maxDepth,
                             ML::Tree & tree,
                             MappedSerializer & serializer,
                             const std::vector<uint32_t> & bucketOffsets,
                             const std::vector<int> & activeFeatures,
                             std::vector<W> bucketsIn,
                             const std::vector<float> & decodedRows,
                             const W & wAllInput,
                             PartitionIndex root,
                             const DatasetFeatureSpace & fs,
                             const std::vector<Feature> & features,
                             FrozenMemoryRegionT<uint32_t> bucketMemory)
{
    constexpr bool verifyBuckets = false;
    constexpr bool debug = false;

    using namespace std;

    //cerr << "trainPartitionedRecursiveCpu: root = " << root << " depth = " << depth << " maxDepth = " << maxDepth << endl;

    int rowCount = decodedRows.size();

    int maxDepthBeforeRecurse = std::min(maxDepth - depth, 20);
    
    // This is our total for each bucket across the whole lot
    // We keep track of it per-partition (there are up to 256
    // partitions, which corresponds to a depth of 8)
    std::vector<std::vector<W> > buckets;
    buckets.reserve(1 << maxDepthBeforeRecurse);
    buckets.emplace_back(std::move(bucketsIn));

    // Which partition is each row in?  Initially, everything
    // is in partition zero, but as we start to split, we end up
    // splitting them amongst many partitions.  Each partition has
    // its own set of buckets that we maintain.
    // Note that these are indexes into the partition list, to
    // enable sparseness in the tree.
    std::vector<uint32_t> partitions(rowCount, 0);

    // What is the index (position in the tree) for each partition?
    std::vector<PartitionIndex> indexes;
    indexes.reserve(1 << maxDepthBeforeRecurse);
    indexes.emplace_back(root);  // we're at the root of the tree
    
    // What is our weight total for each of our partitions?
    std::vector<W> wAll = { wAllInput };
    wAll.reserve(1 << maxDepthBeforeRecurse);
        
    // Record the split, per level, per partition
    std::map<PartitionIndex, PartitionSplit> allSplits;

    // Maximum width to process.  If we exceed this width, we divide and
    // conquer before preceeding.  This stops us from having a set of
    // buckets so wide that we trash the cache.
    int maxWidth MLDB_UNUSED = MLDB_RF_CPU_PARTITION_MAX_WIDTH;

    // How many buckets are required?  This tells us how much memory we
    // need to allocate to determine a split.
    int numActiveBuckets MLDB_UNUSED = buckets.empty() ? 0 : buckets[0].size();

    // Minimum number of examples in a bucket to use the parallel algorithm.
    // Once a part of the tree gets below this threshold, it's processed
    // using an iterative algorithm.
    int minExamplesPerBucket
        = MLDB_RF_CPU_MIN_PARTITION_EXAMPLE_MULTIPLIER * numActiveBuckets;

    Date start = Date::now();

    int originalDepth = depth;

    std::map<PartitionIndex, ML::Tree::Ptr> leaves;
    
    // We go down level by level
    for (int myDepth = 0; myDepth < maxDepthBeforeRecurse && depth < maxDepth;
         ++depth, ++myDepth) {

        // Check the preconditions if we're in testing mode
        if (verifyBuckets) {
            verifyPartitionBuckets(partitions, wAll);
        }

        // Run a kernel to find the new split point for each partition,
        // best feature and kernel.
        std::vector<PartitionSplit> splits
            = getPartitionSplits(buckets, activeFeatures, bucketOffsets,
                                 features, wAll, indexes,
                                 depth < 4 /* parallel */); 


#if 0        
        if (verifyBuckets || true) {
            // At this point, the partitions are already sorted by their
            // row count.  Verify it's true.

            int lastCount = -1;
            for (auto & s: splits) {
                int count = s.left.count() + s.right.count();
                if (count == 0)
                    continue;
                if (s.feature == -1)
                    continue;
                if (lastCount == -1)
                    lastCount = count;
                ExcAssertLessEqual(count, lastCount);
                lastCount = count;
            }
        }
#endif
        
        // Identify new left and right buckets for each of the partitions.
        // A -1 number means that the partition is not kept.  A -2 or below
        // number means that the partition is processed recursively as a
        // leaf.
        std::vector<std::pair<int32_t, int32_t> >
            newPartitionNumbers(splits.size(), { -1, -1 });
        
        int numActivePartitions = 0;
        uint32_t numRowsInActivePartition = 0;

        // first = count, second = index * 2 + (right ? 1 : 0)
        std::vector<std::pair<uint32_t, uint32_t> > partitionCounts;

        // These are the partitions that are too small to be handled by the
        // main loop or that result in too many active partitions for our
        // configured maximum width.  They are handled in a separate algorithm
        // in parallel with running the main algorithm.
        // first = partition number, second = split
        std::vector<std::pair<int, PartitionSplit> > smallPartitions;

        int smallPartitionRows = 0;
        
        for (size_t i = 0;  i < splits.size();  ++i) {
            const PartitionSplit & p = splits[i];

            //cerr << "split " << i << " feat " << p.feature
            //     << " all " << wAll[i].count() << " left "
            //     << p.left.count() << " right " << p.right.count()
            //     << " dir " << p.direction << endl;

            if (p.left.count() > 0 && p.right.count() > 0) {
                int count = p.left.count() + p.right.count();
                if (count >= minExamplesPerBucket) {
                    ++numActivePartitions;
                    partitionCounts.emplace_back(p.left.count(), i * 2);
                    partitionCounts.emplace_back(p.right.count(), i * 2 + 1);
                    numRowsInActivePartition += count;
                }
                else {
                    smallPartitionRows += count;
                    smallPartitions.emplace_back(i, p);
                }
            }
        }

        // Any partitions that don't make the width get handled
        // separately
        if (partitionCounts.size() > maxWidth) {
            int splitCount = partitionCounts[maxWidth].first
                + partitionCounts[maxWidth + 1].first;
            if (debug) {
                cerr << "splitting off " << partitionCounts.size() - maxWidth
                    << " partitions with " << splitCount
                    << " max count due to maximum width" << endl;
            }

            for (int i = maxWidth;  i < partitionCounts.size();  ++i) {
                uint32_t idx = partitionCounts[i].second;
                uint32_t part = idx >> 1;
                int lr = idx & 1;
                smallPartitionRows += partitionCounts[i].first;
                if (lr) continue;
                smallPartitions.emplace_back(part, splits[part]);
            }

            partitionCounts.resize(maxWidth);
        }

        if (debug) {
            cerr << smallPartitions.size() << " small partitions with "
                << smallPartitionRows << " rows" << endl;
        }

        // Do the small partitions
        std::map<PartitionIndex, ML::Tree::Ptr> smallPartitionsOut
            = trainSmallPartitions(depth, maxDepth, tree, features,
                                   partitions, buckets, bucketOffsets,
                                   decodedRows, activeFeatures,
                                   smallPartitions, fs);

#if 0        
        cerr << "small partitions returned " << smallPartitionsOut.size()
             << " trees" << endl;

        for (auto & out: smallPartitionsOut) {
            cerr << "idx " << out.first << " --> "
                 << (out.second.isNode() ? "node" : "leaf")
                 << " ex " << out.second.examples()
                 << " pr " << out.second.pred() << endl;
        }
#endif
        
        leaves.merge(std::move(smallPartitionsOut));
        
        // Sort our larger partitions
        std::sort(partitionCounts.begin(), partitionCounts.end(),
                  [] (std::pair<uint32_t, uint32_t> p1,
                      std::pair<uint32_t, uint32_t> p2)
                  {
                      return p1.first > p2.first
                          || (p1.first == p2.first && p1.second < p2.second);
                  });

        std::vector<PartitionIndex> newIndexes(partitionCounts.size());

        for (int i = 0;  i < partitionCounts.size();  ++i) {
            uint32_t idx = partitionCounts[i].second;
            uint32_t part = idx >> 1;
            int lr = idx & 1;

            // Count for the whole partition, since we need to either have both
            // or none of the partition values in order for this to work.
            uint32_t count
                = splits[part].left.count()
                + splits[part].right.count();
            
            if (count >= minExamplesPerBucket) {
                if (lr) { // right
                    newPartitionNumbers[part].second = i; 
                    newIndexes[i] = splits[part].index.rightChild();
                }
                else { // left
                    newPartitionNumbers[part].first = i;
                    newIndexes[i] = splits[part].index.leftChild();
                }
            }
            
            if (i < 10 && debug) {
                cerr << "partition " << i << " index " << part << " path "
                     << splits[part].index.path()
                     << " lr " << lr << " count " << partitionCounts[i].first
                     << endl;
            }
        }

        indexes = std::move(newIndexes);
        
        if (debug) {
            std::vector<size_t> partitionCountsCum;
            size_t cum = 0;
            for (auto & c: partitionCounts) {
                cum += c.first;
                partitionCountsCum.push_back(cum);
            }

            std::vector<Path> activeFeatureNames;
            for (auto & f: activeFeatures) {
                activeFeatureNames.emplace_back(features[f].info->columnName);
            }
            
            cerr << "depth " << depth << " active partitions "
                << numActivePartitions
                << " of " << splits.size()
                << " rows " << numRowsInActivePartition
                << " of " << decodedRows.size()
                << " buckets "
                << (buckets.empty() ? 0 : buckets[0].size())
                << " features " << jsonEncodeStr(activeFeatures)
                << " " << jsonEncodeStr(activeFeatureNames)
                << endl;

            cerr << "dist: ";
            for (int i = 1;  i < partitionCounts.size();  i *= 2) {
                cerr << i << ": " << partitionCounts[i - 1].first << "->"
                    << partitionCountsCum[i - 1]
                    << "("
                    << 100.0 * partitionCountsCum[i - 1] / numRowsInActivePartition
                    << "%)" << endl;
            }
            cerr << endl;
        }

        //cerr << "newPartitionNumbers = " << jsonEncodeStr(newPartitionNumbers) << endl;


        // Double the number of partitions, create new W entries for the
        // new partitions, and transfer those examples that are in the
        // wrong partition to the right one
        updateBuckets(features,
                      partitions, buckets, wAll,
                      bucketOffsets, splits,
                      newPartitionNumbers, partitionCounts.size(),
                      decodedRows, activeFeatures);

        if (debug) {
            cerr << "depth " << depth << " time "
                << 1000.0 * Date::now().secondsSince(start)
                << "ms" << endl;
        }
        start = Date::now();
        
        for (auto & s: splits) {
            allSplits.emplace(s.index, s);
        }

        // Ready for the next level
    }

    // If we're not at the lowest level, partition our data and recurse
    // par partition to create our leaves.
    std::map<PartitionIndex, ML::Tree::Ptr> newLeaves
        = splitAndRecursePartitioned(depth, maxDepth, tree, serializer,
                                     std::move(buckets), bucketOffsets,
                                     features, activeFeatures,
                                     decodedRows,
                                     partitions, wAll, indexes, fs,
                                     bucketMemory);

    // Merge everything together
    leaves.merge(std::move(newLeaves));
    
    // Finally, extract a tree from the splits we've been accumulating
    // and our leaves.
    return extractTree(originalDepth, maxDepth,
                       tree, root,
                       allSplits, leaves, features, fs);
}

// Recursively go through and extract our tree.  There is no
// calculating going on here, just creation of the data structure.
ML::Tree::Ptr
extractTree(int depth, int maxDepth,
            ML::Tree & tree,
            PartitionIndex root,
            const std::map<PartitionIndex, PartitionSplit> & splits,
            const std::map<PartitionIndex, ML::Tree::Ptr> & leaves,
            const std::vector<Feature> & features,
            const DatasetFeatureSpace & fs)
{
    ExcAssertEqual(root.depth(), depth);
    
    cerr << "extractTree: depth " << depth << " root " << root << endl;
    if (depth == 0) {
        cerr << " with " << splits.size() << " splits and " << leaves.size() << " leaves" << endl;
        for (auto & s: splits) {
            cerr << "  split " << s.first << " --> " << s.second.feature << " " << s.second.index
                 << " " << s.second.left.count() << ":" << s.second.right.count() << " d: " << s.second.direction << endl;
        }
        for (auto & l: leaves) {
            cerr << "  leaf " << l.first << " --> " << l.second.pred() << " " << l.second.examples() << endl;
        }
    }

    // First look for a leaf
    {
        auto it = leaves.find(root);
        if (it != leaves.end()) {
            return it->second;
        }
        cerr << "  not found in leaves" << endl;
    }

    // Secondly look for a split
    auto it = splits.find(root);
    if (it == splits.end()) {
        cerr << "  split not found" << endl;
        return ML::Tree::Ptr();
    }
    auto & s = it->second;

    //cerr << std::string(relativeDepth * 2, ' ')
    //     << relativeDepth << " " << partition << " "
    //     << jsonEncodeStr(s.left) << " " << jsonEncodeStr(s.right)
    //     << " " << s.feature << endl;

    if (s.left.count() + s.right.count() == 0) 
        return ML::Tree::Ptr();
    else if (s.feature == -1)
        return getLeaf(tree, s.left + s.right);
                
    W total = s.left + s.right;

    // No impurity
    if (total.v[false] == 0 || total.v[true] == 0)
        return ML::Tree::Ptr();
                
    ML::Tree::Ptr left
        = extractTree(depth + 1, maxDepth,
                      tree, root.leftChild(),
                      splits, leaves, features, fs);
    ML::Tree::Ptr right
        = extractTree(depth + 1, maxDepth,
                      tree, root.rightChild(),
                      splits, leaves, features, fs);
    
    // Fill in from the information in the split if we can't recurse
    if (!left)
        left = getLeaf(tree, s.left);
    if (!right)
        right = getLeaf(tree, s.right);
    
    return getNode(tree, s.score, s.feature, s.value,
                   left, right, s.left, s.right, features, fs);
}

static std::vector<float> decodeRows(const Rows & rows)
{
    std::vector<float> decodedRows(rows.rowCount());
        
    auto it = rows.getRowIterator();
        
    for (size_t i = 0;  i < rows.rowCount();  ++i) {
        DecodedRow row = it.getDecodedRow();
        ExcAssertEqual(i, row.exampleNum);
        decodedRows[i] = row.weight * (1-2*row.label);
    }

    return decodedRows;
}

ML::Tree::Ptr
trainPartitionedEndToEndCpu(int depth, int maxDepth,
                            ML::Tree & tree,
                            MappedSerializer & serializer,
                            const Rows & rows,
                            const std::vector<Feature> & features,
                            FrozenMemoryRegionT<uint32_t> bucketMemory,
                            const DatasetFeatureSpace & fs)
{
    // First, grab the bucket totals for each bucket across the whole
    // lot.
    size_t numActiveBuckets = 0;
    std::vector<uint32_t> bucketOffsets = { 0 };
    std::vector<int> activeFeatures;
    for (auto & f: features) {
        if (f.active) {
            activeFeatures.push_back(bucketOffsets.size() - 1);
            numActiveBuckets += f.buckets.numBuckets;
        }
        bucketOffsets.push_back(numActiveBuckets);
    }

    std::vector<W> buckets(numActiveBuckets);

    std::vector<std::vector<uint16_t> > featureBuckets(features.size());

    // Keep a cache of our decoded weights, with the sign
    // being the label
    std::vector<float> decodedRows = decodeRows(rows);
        
    //std::atomic<uint64_t> featureBucketMem(0);
    //std::atomic<uint64_t> compressedFeatureBucketMem(0);

    // Start by initializing the weights for each feature, if
    // this isn't passed in already
    auto initFeature = [&] (int f)
        {
#if 0
            const auto & feature = features[f];
            auto & fb = featureBuckets[f];
            size_t ne = feature.buckets.numEntries;
            fb.resize(ne);
            featureBucketMem += sizeof(featureBuckets[0][0]) * ne;
            compressedFeatureBucketMem += feature.buckets.storage.memusage();
            for (size_t i = 0;  i < ne;  ++i) {
                fb[i] = feature.buckets[i];
            }
#endif

            // Distribute weights into buckets for the first iteration
            int startBucket = bucketOffsets[f];
            bool active;
            int maxBucket;
            std::tie(active, maxBucket)
            = testFeatureKernel(decodedRows.data(),
                                decodedRows.size(),
                                features[f].buckets,
                                buckets.data() + startBucket);
                
#if 0 // debug
            W wAll1;
            auto it = rows.getRowIterator();
            for (size_t i = 0;  i < rows.rowCount();  ++i) {
                auto row = it.getDecodedRow();
                wAll1[row.label] += row.weight;
            }

            W wAll2;
            for (size_t i = startBucket;  i < bucketOffsets[f + 1];  ++i) {
                wAll2 += buckets[i];
            }

            ExcAssertEqual(jsonEncodeStr(rows.wAll), jsonEncodeStr(wAll1));
            ExcAssertEqual(jsonEncodeStr(rows.wAll), jsonEncodeStr(wAll2));
#endif // debug
        };

    parallelForEach(activeFeatures, initFeature);

    using namespace std;
    //cerr << "feature bucket mem = " << featureBucketMem.load() / 1000000.0
    //     << "mb; compressed = "
    //     << compressedFeatureBucketMem.load() / 1000000.0 << "mb" << endl;
        
    return trainPartitionedRecursive(depth, maxDepth, tree, serializer,
                                     bucketOffsets, activeFeatures,
                                     std::move(buckets),
                                     decodedRows,
                                     rows.wAll,
                                     PartitionIndex::root(),
                                     fs, features, bucketMemory);
}

// Default of 5.5k allows 8 parallel workgroups for a 48k SM when accounting
// for 0.5k of local memory for the kernels.
// On Nvidia, with 32 registers/work item and 256 work items/workgroup
// (8 warps of 32 threads), we use 32 * 256 * 8 = 64k registers, which
// means full occupancy.
EnvOption<int, true> RF_LOCAL_BUCKET_MEM("RF_LOCAL_BUCKET_MEM", 5500);

struct MapBack {
    MapBack(OpenCLCommandQueue & queue)
        : queue(queue)
    {
    }

    OpenCLCommandQueue & queue;

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
};

#if OPENCL_ENABLED
ML::Tree::Ptr
trainPartitionedEndToEndOpenCL(int depth, int maxDepth,
                               ML::Tree & tree,
                               MappedSerializer & serializer,
                               const Rows & rows,
                               const std::vector<Feature> & features,
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
    std::vector<uint32_t> bucketMemoryOffsets;
    std::vector<uint32_t> bucketEntryBits;
    std::vector<uint32_t> bucketNumbers(1, 0);
    std::vector<uint32_t> featuresActive;
    std::vector<uint32_t> featureIsOrdinal;
    
    for (int i = 0;  i < nf;  ++i) {
        const BucketList & buckets = features[i].buckets;

        bucketEntryBits.push_back(buckets.entryBits);

        featuresActive.push_back(features[i].active);
        featureIsOrdinal.push_back(features[i].ordinal);
        
        if (features[i].active) {
            cerr << "feature " << i << " buckets " << features[i].buckets.numBuckets << endl;
            ExcAssertGreaterEqual((void *)buckets.storage.data(),
                                  (void *)bucketMemory.data());
        
            activeFeatures.push_back(i);

            uint32_t offset
                = buckets.storage.data()
                - bucketMemory.data();

            cerr << "feature = " << i << " offset = " << offset << " numActiveBuckets = " << numActiveBuckets << endl;
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
    
    size_t rowMemorySizePageAligned = roundUpToPageSize(rows.rowData.memusage());
    
    Date before = Date::now();

    std::vector<std::pair<std::string, OpenCLEvent> > allEvents;

    // First, we need to send over the rows, as the very first thing to
    // be done is to expand them.
    //
    // Create the buffer...
    OpenCLMemObject clRowData
        = context.createBuffer(CL_MEM_READ_ONLY,
                               rowMemorySizePageAligned);

    totalGpuAllocation += rowMemorySizePageAligned;

    // ... and send it over
    OpenCLEvent copyRowData
        = memQueue.enqueueWriteBuffer
            (clRowData, 0 /* offset */, rowMemorySizePageAligned,
             (const void *)rows.rowData.data());

    allEvents.emplace_back("copyRowData", copyRowData);

    // Same for our weight data
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

    // We transfer the bucket data as early as possible, as it's one of the
    // longest things to transfer
    
    size_t bucketMemorySizePageAligned = roundUpToPageSize(bucketMemory.memusage());
    
    OpenCLMemObject clBucketData
        = context.createBuffer(CL_MEM_READ_ONLY,
                               bucketMemorySizePageAligned);

    OpenCLEvent transferBucketData
        = memQueue2.enqueueWriteBuffer
            (clBucketData, 0 /* offset */, bucketMemory.memusage(),
             (const void *)bucketMemory.data());
    
    allEvents.emplace_back("transferBucketData", transferBucketData);

    // This one contains an expanded version of the row data, with one float
    // per row rather than bit-compressed.  It's expanded on the GPU so that
    // the compressed version can be passed over the PCIe bus and not the
    // expanded version.
    OpenCLMemObject clExpandedRowData
        = context.createBuffer(CL_MEM_READ_WRITE,
                               sizeof(float) * rowCount);

    totalGpuAllocation += sizeof(float) * rowCount;

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
        OpenCLEvent mapExpandedRows;
        std::shared_ptr<const void> mappedExpandedRows;

        std::tie(mappedExpandedRows, mapExpandedRows)
            = memQueue.enqueueMapBuffer(clExpandedRowData, CL_MAP_READ,
                                     0 /* offset */, sizeof(float) * rowCount,
                                     { runExpandRows });
        
        mapExpandedRows.waitUntilFinished();

        debugExpandedRowsCpu = decodeRows(rows);

        const float * expandedRowsGpu
            = reinterpret_cast<const float *>(mappedExpandedRows.get());

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

    OpenCLMemObject clBucketNumbers
        = context.createBuffer(bucketNumbers);
    
    OpenCLMemObject clBucketEntryBits
        = context.createBuffer(bucketEntryBits);

    OpenCLMemObject clFeaturesActive
        = context.createBuffer(featuresActive);

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

    size_t partitionMemoryUsage = sizeof(uint32_t) * rowCount;
    
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
        std::vector<std::vector<W> > debugBucketsCpu;
        std::vector<W> debugWAllCpu;
        std::vector<uint32_t> debugPartitionsCpu;

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

            const uint32_t * partitionsGpu
                = reinterpret_cast<const uint32_t *>
                    (mappedPartitions.get());

            debugPartitionsCpu = { partitionsGpu, partitionsGpu + rowCount };
            
            // Construct the CPU version of buckets
            OpenCLEvent mapBuckets;
            std::shared_ptr<const void> mappedBuckets;

            std::tie(mappedBuckets, mapBuckets)
                = memQueue.enqueueMapBuffer(clPartitionBuckets, CL_MAP_READ,
                                         0 /* offset */,
                                         bytesPerPartition * numPartitionsAtDepth,
                                         { mapPartitionSplits });
        
            mapBuckets.waitUntilFinished();

            const W * bucketsGpu
                = reinterpret_cast<const W *>(mappedBuckets.get());

            for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
                const W * partitionBuckets = bucketsGpu + numActiveBuckets * i;
                debugBucketsCpu.emplace_back(partitionBuckets,
                                             partitionBuckets + numActiveBuckets);
            }
            
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
                = getPartitionSplits(debugBucketsCpu,
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

                if (!debugBucketsCpu[i].empty()) {
                    for (size_t j = 0;  j < numActiveBuckets;  ++j) {
                        if (partitionBuckets[j] != debugBucketsCpu[i].at(j)) {
                            cerr << "part " << i << " bucket " << j
                                 << " num " << numActiveBuckets * i + j
                                 << " update error: CPU "
                                 << jsonEncodeStr(debugBucketsCpu[i][j])
                                 << " GPU "
                                 << jsonEncodeStr(partitionBuckets[j])
                                 << endl;
                            different = true;
                        }
                    }
                } else {
                    for (size_t j = 0;  j < numActiveBuckets;  ++j) {
                        if (partitionBuckets[j].count() != 0) {
                            cerr << "part " << i << " bucket " << j
                                 << " update error: CPU empty "
                                 << " GPU "
                                 << jsonEncodeStr(partitionBuckets[j])
                                 << endl;
                            different = true;
                        }
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

            const uint32_t * partitionsGpu
                = reinterpret_cast<const uint32_t *>
                    (mappedPartitions.get());

            int numDifferences = 0;
            std::map<std::pair<int, int>, int> differenceStats;
            for (size_t i = 0;  i < rowCount;  ++i) {
                if (partitionsGpu[i] != debugPartitionsCpu[i] && debugPartitionsCpu[i] != -1) {
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

        int leftPosition = split.direction ? leftIndex.index : rightIndex.index;
        int rightPosition = split.direction ? rightIndex.index : leftIndex.index;
        //int leftPosition = leftIndex.index;
        //int rightPosition = rightIndex.index;

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

    MapBack mapper(memQueue);
    auto bucketsUnrolled = mapper.mapVector<W>(clPartitionBuckets);
    auto partitions = mapper.mapVector<uint32_t>(clPartitions);
    auto wAll = mapper.mapVector<W>(clWAll);
    auto decodedRows = mapper.mapVector<float>(clExpandedRowData);

    std::vector<std::vector<W>> buckets;
    for (size_t i = 0;  i < numPartitionsAtDepth;  ++i) {
        const W * partitionBuckets = bucketsUnrolled.data() + numActiveBuckets * i;
        buckets.emplace_back(partitionBuckets, partitionBuckets + numActiveBuckets);
    }

    Date beforeSplitAndRecurse = Date::now();

    std::map<PartitionIndex, ML::Tree::Ptr> newLeaves
        = splitAndRecursePartitioned(depth, maxDepth, tree, serializer,
                                     std::move(buckets), bucketNumbers,
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
#endif // OPENCL_ENABLED

ML::Tree::Ptr
trainPartitionedRecursive(int depth, int maxDepth,
                          ML::Tree & tree,
                          MappedSerializer & serializer,
                          const std::vector<uint32_t> & bucketOffsets,
                          const std::vector<int> & activeFeatures,
                          std::vector<W> bucketsIn,
                          const std::vector<float> & decodedRows,
                          const W & wAllInput,
                          PartitionIndex root,
                          const DatasetFeatureSpace & fs,
                          const std::vector<Feature> & features,
                          FrozenMemoryRegionT<uint32_t> bucketMemory)
{
    return trainPartitionedRecursiveCpu
        (depth, maxDepth, tree, serializer, bucketOffsets, activeFeatures,
         std::move(bucketsIn), decodedRows, wAllInput, root, fs, features,
         std::move(bucketMemory));
                                           
}

ML::Tree::Ptr
trainPartitionedEndToEnd(int depth, int maxDepth,
                         ML::Tree & tree,
                         MappedSerializer & serializer,
                         const Rows & rows,
                         const std::vector<Feature> & features,
                         FrozenMemoryRegionT<uint32_t> bucketMemory,
                         const DatasetFeatureSpace & fs)
{
#if OPENCL_ENABLED
    if (RF_USE_OPENCL) {
        return trainPartitionedEndToEndOpenCL(depth, maxDepth, tree, serializer,
                                              rows, features, bucketMemory, fs);
    }
#endif

    return trainPartitionedEndToEndCpu(depth, maxDepth, tree, serializer,
                                       rows, features, bucketMemory, fs);

}

} // namespace RF
} // namespace MLDB
