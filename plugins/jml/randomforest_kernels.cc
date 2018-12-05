/** randomforest_kernels.h                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Kernels for random forest algorithm.
*/

#include "randomforest_kernels.h"
#include "mldb/builtin/opencl/opencl_types.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/base/map_reduce.h"
#include "mldb/base/scope.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/tuple_description.h"
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

// Core kernel of the decision tree search algorithm.  Transfer the
// example weight into the appropriate (bucket,label) accumulator.
// Returns whether
template<typename BucketList>
std::pair<bool, int>
testFeatureKernelCpu(Rows::RowIterator rowIterator,
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
        DecodedRow r = rowIterator.getDecodedRow();
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
         OpenCLCommandQueueProperties::PROFILING_ENABLE);

    filter_istream stream("mldb/plugins/jml/randomforest_kernels.cl");
    Utf8String source(stream.readAll());
    
    OpenCLProgram program = context.createProgram(source);

    string options = "-cl-kernel-arg-info";// -cl-mad-enable -cl-fast-relaxed-math -cl-unsafe-math-optimizations -DFloat=" + type_name<Float>();

    // Build for all devices
    auto buildInfo = program.build(devices, options);
    
    cerr << jsonEncode(buildInfo[0]) << endl;

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
                  const W & wAll)
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

#if 0                
                if (debug) {
                    std::cerr << "  ord split " << j << " "
                              << features.info->bucketDescriptions.getValue(j)
                              << " had score " << s << std::endl;
                    std::cerr << "    false: " << wFalse[0] << " " << wFalse[1] << std::endl;
                    std::cerr << "    true:  " << wTrue[0] << " " << wTrue[1] << std::endl;
                }
#endif
                
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

#if 0                    
            if (debug) {
                std::cerr << "  non ord split " << j << " "
                          << features.info->bucketDescriptions.getValue(j)
                          << " had score " << s << std::endl;
                std::cerr << "    false: " << wFalse[0] << " " << wFalse[1] << std::endl;
                std::cerr << "    true:  " << w[j][0] << " " << w[j][1] << std::endl;
            }
#endif
                    
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
           FrozenMemoryRegionT<uint32_t> bucketData)
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
              FrozenMemoryRegionT<uint32_t> bucketData_)
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
    std::vector<uint32_t> bucketDataOffsets;
    std::vector<uint32_t> bucketEntryBits;
    std::vector<uint32_t> bucketNumbers(1, 0);
    std::vector<uint32_t> featuresActive;
    
    for (int i = 0;  i < nf;  ++i) {
        const BucketList & buckets = features[i].buckets;

        bucketEntryBits.push_back(buckets.entryBits);

        featuresActive.push_back(features[i].active);
        
        if (features[i].active) {
            ExcAssertGreaterEqual((void *)buckets.storage.data(),
                                  (void *)bucketData_.data());
        
            uint32_t offset
                = buckets.storage.data()
                - bucketData_.data();

            //cerr << "i = " << i << " offset = " << offset << endl;
            bucketDataOffsets.push_back(offset);
            lastBucketDataOffset = offset + bucketData_.length();
            
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
            bucketDataOffsets.push_back(lastBucketDataOffset);
        }

        bucketNumbers.push_back(totalBuckets);

        //cerr << "feature " << i << " buckets from " << bucketNumbers[i]
        //     << " to " << bucketNumbers[i + 1] << " numBuckets "
        //     << bucketNumbers[i + 1] - bucketNumbers[i] << endl;
    }

    bucketDataOffsets.push_back(lastBucketDataOffset);
    //cerr << "bucketNumbers are " << jsonEncode(bucketNumbers) << endl;
    
    //cerr << "doing " << rows.rowCount() << " rows with "
    //     << activeFeatures << " active features and "
    //     << totalBuckets << " total buckets" << endl;

    ExcAssertEqual(bucketDataOffsets.size(), nf + 1);
    ExcAssertEqual(bucketEntryBits.size(), nf);
    ExcAssertEqual(bucketNumbers.size(), nf + 1);
    ExcAssertEqual(featuresActive.size(), nf);

    
    //cerr << "bucketDataOffsets = " << jsonEncodeStr(bucketDataOffsets)
    //     << endl;
    //for (auto & b: bucketDataOffsets) {
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
    //cerr << "bucketData_.data() = " << bucketData_.data() << endl;

    Date beforeTransfer = Date::now();
    
    // Transfer rows, weights on the GPU; these are shared across all features
    OpenCLMemObject clRowData
        = context.createBuffer(0,
                               (const void *)rows.rowData.data(),
                               (rows.rowData.memusage() + 4095) / 4096 * 4096);

    OpenCLMemObject clBucketData
        = context.createBuffer(0,
                               (const void *)bucketData_.data(),
                               (bucketData_.memusage() + 4095) / 4096 * 4096);

    OpenCLMemObject clBucketDataOffsets
        = context.createBuffer(bucketDataOffsets);

    
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
         << "mb of rows and " << bucketData_.memusage() / 1000000.0
         << "mb of buckets in " << elapsedTransfer * 1000.0 << " ms at "
         << (rows.rowData.memusage() + bucketData_.memusage()) / 1000000.0 / elapsedTransfer
         << "mb/s" << endl;

    //uint64_t maxBit = rows.rowData.memusage() * 8;
    //cerr << "row bit maximum is " << log2(maxBit) << endl;
    
    Date start = Date::now();
    
    size_t numRows = rows.rowCount();

    //cerr << "total offset = " << bucketData_.memusage() / 4 << endl;

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
        FrozenMemoryRegionT<uint32_t> bucketData)
{
    if (rows.rowCount() < 1000000 || true) {
        if (depth < 3 && false) {
            //static std::mutex mutex;
            //std::unique_lock<std::mutex> guard(mutex);

            Date beforeCpu = Date::now();
            auto res = testAllCpu(depth, features, rows, bucketData);
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
            return testAllCpu(depth, features, rows, bucketData);
        }
    }
    else {
        static constexpr int MAX_GPU_JOBS = 4;
        static std::atomic<int> numGpuJobs = 0;
        

        if (numGpuJobs.fetch_add(1) >= MAX_GPU_JOBS) {
            --numGpuJobs;
            return testAllCpu(depth, features, rows, bucketData);
        }
        else {
            auto onExit = ScopeExit([&] () noexcept { --numGpuJobs; });
            try {
                return testAllOpenCL(depth, features, rows, bucketData);
            } MLDB_CATCH_ALL {
                return testAllCpu(depth, features, rows, bucketData);
            }
        }
        
        //static std::mutex mutex;
        //std::unique_lock<std::mutex> guard(mutex);

        Date beforeCpu = Date::now();
        auto cpuOutput = testAllCpu(depth, features, rows, bucketData);
        Date afterCpu = Date::now();
        auto gpuOutput = testAllOpenCL(depth, features, rows, bucketData);
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

void updateBuckets(const std::vector<Feature> & features,
                   std::vector<uint8_t> & partitions,
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
    std::vector<int8_t> transfers(rowCount);
        
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

            // Update the wAll, transfering weight
            wAll[fromPartition].sub(label, weight);
            wAll[toPartition  ].add(label, weight);
                    
            // Transfer the weight from each of the features
            for (auto & f: partitionSplits[partition].activeFeatures) {
                int startBucket = bucketOffsets[f];
                int bucket = features[f].buckets[exampleNum];
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
                const std::vector<uint8_t> & partitions,
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
                double score = std::get<1>(val);

                if (score == INFINITY) return;

#if 0                        
                cerr << "af " << af << " f " << std::get<0>(val)
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
#endif
                        
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
                int startBucket = bucketOffsets[f];
                int endBucket MLDB_UNUSED = bucketOffsets[f + 1];
                int maxBucket = endBucket - startBucket - 1;
                bool isActive = true;
                double bestScore = INFINITY;
                int bestSplit = -1;
                W bestLeft;
                W bestRight;

                if (isActive && !buckets[partition].empty()) {
                    const W * wFeature
                        = buckets[partition].data() + startBucket;
                    std::tie(bestScore, bestSplit, bestLeft, bestRight)
                        = chooseSplitKernel(wFeature, maxBucket,
                                            features[f].ordinal,
                                            wAll[partition]);
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
            { bestScore, bestFeature, bestSplit, bestLeft, bestRight,
              std::move(activeFeatures),
              bestFeature != -1 && bestLeft.count() <= bestRight.count() };

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
verifyPartitionBuckets(const std::vector<uint8_t> & partitions,
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
                           const std::vector<uint8_t> & partitions,
                           const std::vector<W> & wAll,
                           const DatasetFeatureSpace & fs)
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
                 newData[i].features);
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
trainPartitionedRecursive(int depth, int maxDepth,
                          ML::Tree & tree,
                          MappedSerializer & serializer,
                          const std::vector<uint32_t> & bucketOffsets,
                          const std::vector<int> & activeFeatures,
                          std::vector<W> bucketsIn,
                          const std::vector<float> & decodedRows,
                          const W & wAllInput,
                          const DatasetFeatureSpace & fs,
                          const std::vector<Feature> & features)
{
    constexpr bool verifyBuckets = false;
        
    using namespace std;

    int rowCount = decodedRows.size();
        
    // This is our total for each bucket across the whole lot
    // We keep track of it per-partition (there are up to 256
    // partitions, which corresponds to a depth of 8)
    std::vector<std::vector<W> > buckets;
    buckets.reserve(256);
    buckets.emplace_back(std::move(bucketsIn));

    // Which partition is each row in?  Initially, everything
    // is in partition zero, but as we start to split, we end up
    // splitting them amongst many partitions.  Each partition has
    // its own set of buckets that we maintain.
    std::vector<uint8_t> partitions(rowCount, 0);

    // What is our weight total for each of our partitions?
    std::vector<W> wAll = { wAllInput };
    wAll.reserve(256);
        
    // Record the split, per level, per partition
    std::vector<std::vector<PartitionSplit> > depthSplits;
    depthSplits.reserve(8);
        
    // We go down level by level
    for (int myDepth = 0; myDepth < 8 && depth < maxDepth;
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
                                     partitions, wAll, fs);
        
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

} // namespace RF
} // namespace MLDB
