/** randomforest_kernels.h                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#include "randomforest_kernels.h"
#include "mldb/builtin/opencl/opencl_types.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/types/structure_description.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/base/map_reduce.h"
#include "mldb/base/scope.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/tuple_description.h"
#include "mldb/utils/environment.h"
#include <condition_variable>
#include <sstream>

using namespace std;


namespace MLDB {
namespace RF {

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
    
    OpenCLCommandQueue queue
        = context.createCommandQueue
        (devices.at(0),
         { OpenCLCommandQueueProperties::PROFILING_ENABLE,
           OpenCLCommandQueueProperties::OUT_OF_ORDER_EXEC_MODE_ENABLE} );

    filter_istream stream("mldb/plugins/jml/randomforest_kernels.cl");
    Utf8String source(stream.readAll());
    
    OpenCLProgram program = context.createProgram(source);

    string options = "-cl-kernel-arg-info -cl-nv-maxrregcount=32 -cl-nv-verbose";// -cl-mad-enable -cl-fast-relaxed-math -cl-unsafe-math-optimizations -DFloat=" + type_name<Float>();

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
                    std::cerr << "    false: " << wFalse[0] << " " << wFalse[1] << std::endl;
                    std::cerr << "    true:  " << wTrue[0] << " " << wTrue[1] << std::endl;
                }
                
                if (s < bestScore) {
                    bestScore = s;
                    bestSplit = j;
                    bestRight = wFalse;
                    bestLeft = wTrue;
                }
            }
            
            if (j < maxBucket) {
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
                std::cerr << "    false: " << wFalse[0] << " " << wFalse[1] << std::endl;
                std::cerr << "    true:  " << w[j][0] << " " << w[j][1] << std::endl;
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
                  const W & wAll)
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
                                wAll);
    }

    return { bestScore, bestSplit, bestLeft, bestRight, isActive };
}

std::tuple<double, int, int, W, W, std::vector<uint8_t> >
testAllCpu(int depth,
           const std::vector<Feature> & features,
           const Rows & rows,
           FrozenMemoryRegionT<uint32_t> bucketMemory)
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

    bool debug = false;

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
            double score;
            int split = -1;
            W bestLeft, bestRight;

            Rows::RowIterator rowIterator = rows.getRowIterator();

            bool stillActive;
            std::tie(score, split, bestLeft, bestRight, stillActive)
                = testFeatureNumber(i, features, rowIterator,
                                    rows.rowCount(), rows.wAll);
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
    if (depth < 4 || (uint64_t)rows.rowCount() * (uint64_t)nf > 20000) {
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
                  << features[bestFeature].info->bucketDescriptions.getValue(bestSplit)
                  << std::endl;
    }

    return std::make_tuple(bestScore, bestFeature, bestSplit,
                           bestLeft, bestRight, newActive);
}

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
    static constexpr int PARALLELISM = 4;
    
    static std::atomic<int> numRunning(0);
    static std::mutex mutex;
    static std::atomic<int> numWaiting(0);

    ++numWaiting;
    static std::condition_variable cv;
    std::unique_lock<std::mutex> guard(mutex);

    
    cv.wait(guard, [&] () { return numRunning < PARALLELISM; });

    --numWaiting;
    numRunning += 1;

    //cerr << "started; numRunning now " << numRunning << " numWaiting = "
    //     << numWaiting << endl;
    
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

            //cerr << "i = " << i << " offset = " << offset << endl;
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
                               (rows.rowData.memusage() + 4095) / 4096 * 4096);

    OpenCLMemObject clBucketData
        = context.createBuffer(0,
                               (const void *)bucketMemory_.data(),
                               (bucketMemory_.memusage() + 4095) / 4096 * 4096);

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

std::tuple<double, int, int, W, W, std::vector<uint8_t> >
testAll(int depth,
        const std::vector<Feature> & features,
        const Rows & rows,
        FrozenMemoryRegionT<uint32_t> bucketMemory)
{
    if (rows.rowCount() < 1000000 || true) {
        if (depth < 3 && false) {
            //static std::mutex mutex;
            //std::unique_lock<std::mutex> guard(mutex);

            Date beforeCpu = Date::now();
            auto res = testAllCpu(depth, features, rows, bucketMemory);
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
            return testAllCpu(depth, features, rows, bucketMemory);
        }
    }
    else {
        static constexpr int MAX_GPU_JOBS = 4;
        static std::atomic<int> numGpuJobs = 0;
        

        if (numGpuJobs.fetch_add(1) >= MAX_GPU_JOBS) {
            --numGpuJobs;
            return testAllCpu(depth, features, rows, bucketMemory);
        }
        else {
            auto onExit = ScopeExit([&] () noexcept { --numGpuJobs; });
            try {
                return testAllOpenCL(depth, features, rows, bucketMemory);
            } MLDB_CATCH_ALL {
                return testAllCpu(depth, features, rows, bucketMemory);
            }
        }
        
        //static std::mutex mutex;
        //std::unique_lock<std::mutex> guard(mutex);

        Date beforeCpu = Date::now();
        auto cpuOutput = testAllCpu(depth, features, rows, bucketMemory);
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
        context.writeInt(val->index);
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
                   std::vector<uint16_t> & partitions,
                   std::vector<std::vector<W> > & buckets,
                   std::vector<W> & wAll,
                   const std::vector<uint32_t> & bucketOffsets,
                   const std::vector<PartitionSplit> & partitionSplits,
                   int rightOffset,
                   const std::vector<float> & decodedRows)
{
    ExcAssertEqual(rightOffset, buckets.size());
    ExcAssertEqual(rightOffset, wAll.size());
    ExcAssertEqual(rightOffset, partitionSplits.size());
                       
    int numActiveBuckets = buckets[0].size();

    // Firstly, we double the number of partitions.  The left all
    // go with the lower partition numbers, the right have the higher
    // partition numbers.
            
    buckets.resize(buckets.size() * 2);
    wAll.resize(wAll.size() * 2);

    for (size_t i = 0;  i < rightOffset;  ++i) {
        if (partitionSplits[i].right.count() == 0)
            continue;
            
        buckets[i + rightOffset].resize(numActiveBuckets);

        // Those buckets which are transfering right to left should
        // start with the weight on the right
        if (partitionSplits[i].direction) {
            std::swap(buckets[i], buckets[i + rightOffset]);
            std::swap(wAll[i], wAll[i + rightOffset]);
        }
    }

    constexpr bool checkPartitionCounts = false;

    int rowCount = decodedRows.size();
        
    std::vector<uint32_t> numInPartition(buckets.size());
        
    for (size_t i = 0;  i < rowCount;  ++i) {
        // TODO: for each feature, choose right to left vs left to
        // right based upon processing the smallest number of
        // shifts

        int partition = partitions[i];
        int splitFeature = partitionSplits[partition].feature;
                
        if (splitFeature == -1) {
            // reached a leaf here, nothing to split                    
            //rowIterator.skipRow();
            continue;
        }
                
        //DecodedRow row = rowIterator.getDecodedRow();

        float weight = fabs(decodedRows[i]);
        bool label = decodedRows[i] < 0;
        int exampleNum = i;
            
        int leftPartition = partition;
        int rightPartition = partition + rightOffset;

        int splitValue = partitionSplits[partition].value;
        bool ordinal = features[splitFeature].ordinal;
        int bucket = features[splitFeature].buckets[exampleNum];
        //int bucket = featureBuckets[splitFeature][exampleNum];
        int side = ordinal ? bucket >= splitValue : bucket != splitValue;

        // Set the new partition number
        partitions[i] = partition + side * rightOffset;
            
        //ExcAssert(partitions[i] == leftPartition
        //          || partitions[i] == rightPartition);
            
        // Verify partition counts?
        if (checkPartitionCounts)
            ++numInPartition[partition + side * rightOffset];
            
        // 0 = left to right, 1 = right to left
        int direction = partitionSplits[partition].direction;

        // We only need to update features on the wrong side, as we
        // transfer the weight rather than sum it from the
        // beginning.  This means less work for unbalanced splits
        // (which are typically most of them, especially for discrete
        // buckets)

        if (direction != side) {
            int fromPartition, toPartition;
            if (direction == 0 && side == 1) {
                // Transfer from left to right
                fromPartition = leftPartition;
                toPartition   = rightPartition;
            }
            else {
                // Transfer from right to left
                fromPartition = rightPartition;
                toPartition   = leftPartition;
            }

            //cerr << "row " << i << " side " << side << " direction "
            //     << direction << " from " << fromPartition
            //     << " to " << toPartition << endl;
            
            // Update the wAll, transfering weight
            wAll[fromPartition].sub(label, weight);
            wAll[toPartition  ].add(label, weight);
                    
            // Transfer the weight from each of the features
            for (auto & f: partitionSplits[partition].activeFeatures) {
                int startBucket = bucketOffsets[f];
                int bucket = features[f].buckets[exampleNum];
                
                //cerr << "  feature " << f << " bucket " << bucket
                //     << " offset " << startBucket + bucket << " lbl "
                //     << label << " weight " << weight << endl;

                buckets[fromPartition][startBucket + bucket]
                    .sub(label, weight);
                buckets[toPartition  ][startBucket + bucket]
                    .add(label, weight);
            }
        }               
    }

    // Make sure that the number in the buckets is actually what we
    // expected when we calculated the split.
    if (checkPartitionCounts) {
        for (int i = 0;  i < partitionSplits.size();  ++i) {
            int leftPartition = i;
            int rightPartition = i + rightOffset;

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
        }
    }
}

std::pair<std::vector<PartitionEntry>,
          FrozenMemoryRegionT<uint32_t> >
splitPartitions(const std::vector<Feature> features,
                const std::vector<int> & activeFeatures,
                const std::vector<float> & decodedRows,
                const std::vector<uint16_t> & partitions,
                const std::vector<W> & w,
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
        (partitionMemoryOffset / 4, 4096 /* page aligned */);

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
getPartitionSplits(const std::vector<std::vector<W> > & buckets,
                   const std::vector<int> & activeFeatures,
                   const std::vector<uint32_t> & bucketOffsets,
                   const std::vector<Feature> & features,
                   const std::vector<W> & wAll,
                   bool parallel)
{
    std::vector<PartitionSplit> partitionSplits(buckets.size());
    
    for (int partition = 0;  partition < buckets.size();  ++partition) {

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
            { (float)bestScore, bestFeature, bestSplit, bestLeft, bestRight,
              bestFeature != -1 && bestLeft.count() <= bestRight.count()
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
verifyPartitionBuckets(const std::vector<uint16_t> & partitions,
                       const std::vector<W> & wAll)
{
    using namespace std;
        
    int numPartitions = wAll.size();
        
    // Check that our partition counts and W scores match
    std::vector<uint32_t> partitionRowCounts(numPartitions);

    for (auto & p: partitions)
        ++partitionRowCounts[p];

    for (int i = 0;  i < numPartitions;  ++i) {
        cerr << "part " << i << " count " << wAll[i].count() << " rows "
             << partitionRowCounts[i] << endl;
    }

    for (int i = 0;  i < numPartitions;  ++i) {
        ExcAssertEqual(partitionRowCounts[i], wAll[i].count());
    }
}

// Split our dataset into a separate dataset for each leaf, and
// recurse to create a leaf node for each.  This is mutually
// recursive with trainPartitionedRecursive.
std::vector<ML::Tree::Ptr>
splitAndRecursePartitioned(int depth, int maxDepth,
                           ML::Tree & tree,
                           MappedSerializer & serializer,
                           std::vector<std::vector<W> > buckets,
                           const std::vector<uint32_t> & bucketOffsets,
                           const std::vector<Feature> & features,
                           const std::vector<int> & activeFeatures,
                           const std::vector<float> & decodedRows,
                           const std::vector<uint16_t> & partitions,
                           const std::vector<W> & wAll,
                           const DatasetFeatureSpace & fs,
                           FrozenMemoryRegionT<uint32_t> bucketMemory)
{
    std::vector<ML::Tree::Ptr> leaves;

    if (depth == maxDepth)
        return leaves;
        
    leaves.resize(buckets.size());
            
    // New partitions, per row
    std::vector<PartitionEntry> newData;
    FrozenMemoryRegionT<uint32_t> partitionMem;

    std::tie(newData, partitionMem)
        = splitPartitions(features, activeFeatures, decodedRows,
                          partitions, wAll, serializer);

    auto doEntry = [&] (int i)
        {
            leaves[i] = trainPartitionedRecursive
                (depth, maxDepth,
                 tree, serializer,
                 bucketOffsets, newData[i].activeFeatures,
                 std::move(buckets[i]),
                 newData[i].decodedRows,
                 newData[i].wAll,
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

    return leaves;
}
    
ML::Tree::Ptr
trainPartitionedRecursiveCpu(int depth, int maxDepth,
                             ML::Tree & tree,
                             MappedSerializer & serializer,
                             const std::vector<uint32_t> & bucketOffsets,
                             const std::vector<int> & activeFeatures,
                             std::vector<W> bucketsIn,
                             const std::vector<float> & decodedRows,
                             const W & wAllInput,
                             const DatasetFeatureSpace & fs,
                             const std::vector<Feature> & features,
                             FrozenMemoryRegionT<uint32_t> bucketMemory)
{
    constexpr bool verifyBuckets = false;
        
    using namespace std;

    int rowCount = decodedRows.size();

    int maxDepthBeforeRecurse = std::min(maxDepth - depth, 10);
    
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
    std::vector<uint16_t> partitions(rowCount, 0);

    // What is our weight total for each of our partitions?
    std::vector<W> wAll = { wAllInput };
    wAll.reserve(1 << maxDepthBeforeRecurse);
        
    // Record the split, per level, per partition
    std::vector<std::vector<PartitionSplit> > depthSplits;
    depthSplits.reserve(8);
        
    // We go down level by level
    for (int myDepth = 0; myDepth < maxDepthBeforeRecurse && depth < maxDepth;
         ++depth, ++myDepth) {

        // Check the preconditions if we're in testing mode
        if (verifyBuckets) {
            verifyPartitionBuckets(partitions, wAll);
        }

        // Run a kernel to find the new split point for each partition,
        // best feature and kernel
        depthSplits.emplace_back
            (getPartitionSplits(buckets, activeFeatures, bucketOffsets,
                                features, wAll, depth < 4));
            
        // Double the number of partitions, create new W entries for the
        // new partitions, and transfer those examples that are in the
        // wrong partition to the right one
        updateBuckets(features,
                      partitions, buckets, wAll,
                      bucketOffsets, depthSplits.back(), buckets.size(),
                      decodedRows);

        // Ready for the next level
    }

    // If we're not at the lowest level, partition our data and recurse
    // par partition to create our leaves.
    std::vector<ML::Tree::Ptr> leaves
        = splitAndRecursePartitioned(depth, maxDepth, tree, serializer,
                                     std::move(buckets), bucketOffsets,
                                     features, activeFeatures,
                                     decodedRows,
                                     partitions, wAll, fs,
                                     bucketMemory);
        
    // Finally, extract a tree from the splits we've been accumulating
    // and our leaves.
    return extractTree(0 /*relative depth */, depth, maxDepth,
                       0 /* partition */,
                       tree, depthSplits, leaves, features, fs);
}
    
// Recursively go through and extract our tree.  There is no
// calculating going on here, just creation of the data structure.
ML::Tree::Ptr extractTree(int relativeDepth, int depth, int maxDepth,
                          int partition,
                          ML::Tree & tree,
                          const std::vector<std::vector<PartitionSplit> > & depthSplits,
                          const std::vector<ML::Tree::Ptr> & leaves,
                          const std::vector<Feature> & features,
                          const DatasetFeatureSpace & fs)
{
    auto & s = depthSplits.at(relativeDepth).at(partition);

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
                
    ML::Tree::Ptr left, right;

    if (relativeDepth == depthSplits.size() - 1) {
        // asking for the last level.  Get it from what
        // was calculated earlier.
        if (depth < maxDepth) {
            left = leaves.at(partition);
            right = leaves.at(partition + (1 << relativeDepth));
        }
        else {
            left = getLeaf(tree, s.left);
            right = getLeaf(tree, s.right);
        }
    }
    else {
        // Not the last level
        left = extractTree(relativeDepth + 1, depth, maxDepth, partition,
                           tree, depthSplits, leaves, features, fs);
        right = extractTree(relativeDepth + 1, depth, maxDepth,
                            partition + (1 << relativeDepth),
                            tree, depthSplits, leaves, features, fs);
    }

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
                                     fs, features, bucketMemory);
}

EnvOption<bool> DEBUG_RF_OPENCL_KERNELS("DEBUG_RF_OPENCL_KERNELS", 0);
EnvOption<bool> RF_SEPARATE_FEATURE_UPDATES("RF_SEPARATE_FEATURE_UPDATES", 0);
EnvOption<bool> RF_EXPAND_FEATURE_BUCKETS("RF_EXPAND_FEATURE_BUCKETS", 0);
EnvOption<bool> RF_OPENCL_SYNCHRONOUS_LAUNCH("RF_OPENCL_SYNCHRONOUS_LAUNCH", 0);
EnvOption<size_t, true> RF_NUM_ROW_KERNELS("RF_NUM_ROW_KERNELS", 65536);
EnvOption<size_t, true> RF_ROW_KERNEL_WORKGROUP_SIZE("RF_ROW_KERNEL_WORKGROUP_SIZE", 256);

// Default of 5.5k allows 8 parallel workgroups for a 48k SM when accounting
// for 0.5k of local memory for the kernels.
// On Nvidia, with 32 registers/work item and 256 work items/workgroup
// (8 warps of 32 threads), we use 32 * 256 * 8 = 64k registers, which
// means full occupancy.
EnvOption<int, true> RF_LOCAL_BUCKET_MEM("RF_LOCAL_BUCKET_MEM", 5500);


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

            //cerr << "i = " << i << " offset = " << offset << endl;
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
    if (!RF_OPENCL_SYNCHRONOUS_LAUNCH) {
        queueProperties.set
            (OpenCLCommandQueueProperties::OUT_OF_ORDER_EXEC_MODE_ENABLE);
    }
    
    auto queue = kernelContext.context.createCommandQueue
        (kernelContext.devices[0], queueProperties);

    auto memQueue = kernelContext.context.createCommandQueue
        (kernelContext.devices[0], queueProperties);
    
    auto memQueue2 = kernelContext.context.createCommandQueue
        (kernelContext.devices[0], queueProperties);
    
    size_t rowMemorySizePageAligned
        = (rows.rowData.memusage() + 4095) / 4096 * 4096;
    
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
    
    size_t bucketMemorySizePageAligned
        = (bucketMemory.memusage() + 4095) / 4096 * 4096;
    
    OpenCLMemObject clBucketData
        = context.createBuffer(CL_MEM_READ_ONLY,
                               bucketMemorySizePageAligned);

    OpenCLEvent transferBucketData
        = memQueue2.enqueueWriteBuffer
            (clBucketData, 0 /* offset */, bucketMemorySizePageAligned,
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

        ExcAssert(!different);
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

            ExcAssert(!different);
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
                           
    OpenCLEvent runTestFeatureKernel
        = queue.launch(testFeatureKernel,
                       { 65536, nf },
                       { 256, 1 },
                       { transferBucketData, fillFirstBuckets, runExpandRows });

    allEvents.emplace_back("runTestFeatureKernel", runTestFeatureKernel);

    if (debugKernelOutput) {
        // Get that data back (by mapping), and verify it against the
        // CPU-calcualted version.

        OpenCLEvent mapBuckets;
        std::shared_ptr<const void> mappedBuckets;

        std::tie(mappedBuckets, mapBuckets)
            = memQueue.enqueueMapBuffer(clPartitionBuckets, CL_MAP_READ,
                                     0 /* offset */, bytesPerPartition,
                                     { runTestFeatureKernel });
        
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

        ExcAssert(!different);
    }
    
    // Which partition is each row in?  Initially, everything
    // is in partition zero, but as we start to split, we end up
    // splitting them amongst many partitions.  Each partition has
    // its own set of buckets that we maintain.

    size_t partitionMemoryUsage = sizeof(uint16_t) * rowCount;
    
    OpenCLMemObject clPartitions
        = context.createBuffer(CL_MEM_READ_WRITE, partitionMemoryUsage);

    totalGpuAllocation += partitionMemoryUsage;

    // Before we use this, it needs to be zero-filled
    OpenCLEvent fillPartitions
        = queue.enqueueFillBuffer(clPartitions, (uint16_t)0 /* pattern */,
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

    // Which event represents that the previous iteration of partitions
    // are available?
    OpenCLEvent previousIteration = runTestFeatureKernel;

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
    
    
    // We go down level by level
    for (int myDepth = 0;
         myDepth < 16 && depth < maxDepth;
         ++depth, ++myDepth, numPartitionsAtDepth *= 2) {

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
                
                cerr << "partition " << p << " count " << wAllGpu[p].count() << endl;
                
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

            ExcAssert(!problem);
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
                  (uint32_t)MAX_LOCAL_BUCKETS);

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
        std::vector<uint16_t> debugPartitionsCpu;

        if (depth == maxDepth - 1 && false) {
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

            const uint16_t * partitionsGpu
                = reinterpret_cast<const uint16_t *>
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
            
            // Run the CPU version... first getting the data in place
            debugPartitionSplitsCpu
                = getPartitionSplits(debugBucketsCpu,
                                     activeFeatures, bucketNumbers,
                                     features, debugWAllCpu,
                                     false /* parallel */);

            // Make sure we got the right thing back out
            ExcAssertEqual(debugPartitionSplitsCpu.size(),
                           numPartitionsAtDepth);

            bool different = false;
            
            for (int p = 0;  p < numPartitionsAtDepth;  ++p) {

                // We don't compare the score as the result is different
                // due to numerical differences in the sqrt function.
                if ((partitionSplitsGpu[p].left
                     != debugPartitionSplitsCpu[p].left)
                    || (partitionSplitsGpu[p].right
                        != debugPartitionSplitsCpu[p].right)
                    || (partitionSplitsGpu[p].feature
                        != debugPartitionSplitsCpu[p].feature)
                    || (partitionSplitsGpu[p].value
                        != debugPartitionSplitsCpu[p].value)) {
                    different = true;
                    cerr << "partition " << p << "\nGPU "
                         << jsonEncodeStr(partitionSplitsGpu[p])
                         << "\nCPU " << jsonEncodeStr(debugPartitionSplitsCpu[p])
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
            .bind((uint32_t)numPartitionsAtDepth,
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

            // Run the CPU version of the computation
            updateBuckets(features,
                          debugPartitionsCpu,
                          debugBucketsCpu,
                          debugWAllCpu,
                          bucketNumbers,
                          debugPartitionSplitsCpu,
                          numPartitionsAtDepth,
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

            const uint16_t * partitionsGpu
                = reinterpret_cast<const uint16_t *>
                    (mappedPartitions.get());

            for (size_t i = 0;  i < rowCount;  ++i) {
                if (partitionsGpu[i] != debugPartitionsCpu[i]) {
                    different = true;
                    cerr << "row " << i << " partition difference: CPU "
                         << (int)debugPartitionsCpu[i]
                         << " GPU " << (int)partitionsGpu[i]
                         << endl;
                }
            }

            ExcAssert(!different);
        }
                
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

    std::shared_ptr<const PartitionSplit> mappedAllPartitionSplitsCast
        = reinterpret_pointer_cast<const PartitionSplit>(mappedAllPartitionSplits);
    
    for (int i = 0;  i < numIterations;  ++i) {
        depthSplits.emplace_back
            (mappedAllPartitionSplitsCast.get() + (1 << i),
             mappedAllPartitionSplitsCast.get() + (2 << i));
    }
    
    auto first = allEvents.at(0).second.getProfilingInfo();

    cerr << "  submit    queue    start      end  elapsed name" << endl;
    
    for (auto & e: allEvents) {
        std::string name = e.first;
        const OpenCLEvent & ev = e.second;
        
        auto info = ev.getProfilingInfo() - first.queued;

        auto ms = [&] (int64_t ns) -> double
            {
                return ns / 1000000.0;
            };
        
        cerr << format("%8.2f %8.2f %8.2f %8.2f %8.2f ",
                       ms(info.queued), ms(info.submit), ms(info.start),
                       ms(info.end),
                       ms(info.end - info.start))
             << name << endl;
    }
    
    std::vector<ML::Tree::Ptr> leaves;

    // TODO: fill this in by recursing...
    
    // Finally, extract a tree from the splits we've been accumulating
    // and our leaves.
    return extractTree(0 /*relative depth */, depth, maxDepth,
                       0 /* partition */,
                       tree, depthSplits, leaves, features, fs);
}

ML::Tree::Ptr
trainPartitionedRecursive(int depth, int maxDepth,
                          ML::Tree & tree,
                          MappedSerializer & serializer,
                          const std::vector<uint32_t> & bucketOffsets,
                          const std::vector<int> & activeFeatures,
                          std::vector<W> bucketsIn,
                          const std::vector<float> & decodedRows,
                          const W & wAllInput,
                          const DatasetFeatureSpace & fs,
                          const std::vector<Feature> & features,
                          FrozenMemoryRegionT<uint32_t> bucketMemory)
{
    return trainPartitionedRecursiveCpu
        (depth, maxDepth, tree, serializer, bucketOffsets, activeFeatures,
         std::move(bucketsIn), decodedRows, wAllInput, fs, features,
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
    //return trainPartitionedEndToEndCpu(depth, maxDepth, tree, serializer,
    //                                   rows, features, bucketMemory, fs);
#if 0
    for (size_t i = 0;  i < 19;  ++i) {
        trainPartitionedEndToEndOpenCL(depth, maxDepth, tree, serializer,
                                       rows, features, bucketMemory, fs);
    }
#endif
    
    return trainPartitionedEndToEndOpenCL(depth, maxDepth, tree, serializer,
                                          rows, features, bucketMemory, fs);
}

} // namespace RF
} // namespace MLDB
