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
#include <condition_variable>


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
std::pair<bool, int>
testFeatureKernelCpu(Rows::RowIterator rowIterator,
                     size_t numRows,
                     const BucketList & buckets,
                     W * w /* buckets.numBuckets entries */)
{
    // Number of the last bucket we saw.  Enables us to determine if
    // we change buckets at any point.
    int lastBucket = -1;

    // Number of times we've changed bucket numbers.  Since lastBucket
    // starts off at -1, this will be incremented to 0 on the first loop
    // iteration.
    int bucketTransitions = -1;

    // Maximum bucket number we've seen.  Can significantly reduce the
    // work required to search the buckets later on, as those without
    // an example have no possible split point.
    int maxBucket = -1;

    for (size_t j = 0;  j < numRows;  ++j) {
        DecodedRow r = rowIterator.getDecodedRow();
        int bucket = buckets[r.exampleNum];
        //ExcAssertLess(bucket, buckets.numBuckets);
        bucketTransitions += (bucket != lastBucket ? 1 : 0);
        lastBucket = bucket;

        w[bucket][r.label] += r.weight;
        maxBucket = std::max(maxBucket, bucket);
    }

    return { bucketTransitions > 0, maxBucket };
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

    cerr << "row data of " << rowIterator.owner->rowData.memusage()
         << " bytes and " << numRows << " rows" << endl;
    
    OpenCLMemObject rowData
        = context.createBuffer(0,
                               (const void *)rowIterator.owner->rowData.data(),
                               rowIterator.owner->rowData.memusage());

    OpenCLMemObject bucketData
        = context.createBuffer(0,
                               (const void *)buckets.storage.data(),
                               buckets.storage.memusage());

    OpenCLMemObject weightData
        = context.createBuffer(CL_MEM_READ_ONLY, 4);

    if (rowIterator.owner->weightEncoder.weightFormat == WF_TABLE) {
        weightData = context.createBuffer
            (0,
             (const void *)rowIterator.owner->weightEncoder.weightFormatTable.data(),
             rowIterator.owner->weightEncoder.weightFormatTable.memusage());
    }

    OpenCLMemObject wOut
        = context.createBuffer(CL_MEM_READ_WRITE,
                               w,
                               sizeof(W) * buckets.numBuckets);


    int minMax[2] = { INT_MAX, INT_MIN };
    
    OpenCLMemObject minMaxOut
        = context.createBuffer(CL_MEM_READ_WRITE,
                               minMax,
                               sizeof(minMax[0]) * 2);

    int numRowsPerWorkItem = (numRows + 1023) / 1024;
    
    kernel.bind(numRowsPerWorkItem,
                rowData,
                rowIterator.totalBits,
                rowIterator.owner->weightEncoder.weightBits,
                rowIterator.owner->exampleNumBits,
                (uint32_t)numRows,
                bucketData,
                buckets.entryBits,
                buckets.numBuckets,
                rowIterator.owner->weightEncoder.weightFormat,
                rowIterator.owner->weightEncoder.weightMultiplier,
                weightData,
                LocalArray<W>(buckets.numBuckets),
                wOut,
                minMaxOut);
    
    auto devices = context.getDevices();
    
    auto queue = context.createCommandQueue
        (devices[0],
         OpenCLCommandQueueProperties::PROFILING_ENABLE);

    Date before = Date::now();
    
    OpenCLEvent runKernel
        = queue.launch(kernel,
                       { 1024 },//(rowIterator.owner->rowCount() + 63numRowsPerWorkItem - 1) / numRowsPerWorkItem },
                       { 256 });

    queue.flush();
    
    OpenCLEvent transfer
        = queue.enqueueReadBuffer(wOut, 0 /* offset */,
                                  sizeof(W) * buckets.numBuckets /* length */,
                                  w,
                                  runKernel /* before */);

    OpenCLEvent minMaxTransfer
        = queue.enqueueReadBuffer(minMaxOut, 0 /* offset */,
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

        for (size_t i = 0;  i < buckets.numBuckets;  ++i) {
            if (w[i] != wCpu[i]) {
                cerr << "error on w " << i << ": gpu = ("
                     << w[i].v[0] << "," << w[i].v[1] << "), cpu = ("
                     << wCpu[i].v[0] << "," << wCpu[i].v[1] << ")" << endl;
            }
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
        for (unsigned j = 0;  j < maxBucket;  ++j) {
            if (w[j].empty())
                continue;                   

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
                
            wFalse -= w[j];
            wTrue += w[j];
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
            if (score < bestScore) {
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

    if (depth < 4 || rows.rowCount() * nf > 20000) {
        parallelMapInOrderReduce(0, nf, doFeature, findBest);
    }
    else {
        for (unsigned i = 0;  i < nf;  ++i)
            findBest(i, doFeature(i));
    }

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
    static constexpr int PARALLELISM = 2;
    
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
#elif 0
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
    
    std::vector<uint32_t> bucketDataOffsets;
    std::vector<uint32_t> bucketEntryBits;
    std::vector<uint32_t> bucketNumbers(1, 0);
    std::vector<uint32_t> featuresActive;
    
    for (int i = 0;  i < nf;  ++i) {
        const BucketList & buckets = features[i].buckets;

        bucketEntryBits.push_back(buckets.entryBits);
        
        uint32_t offset
            = buckets.storage.data()
            - bucketData_.data();
        cerr << "i = " << i << " offset = " << offset << endl;
        bucketDataOffsets.push_back(offset);

        featuresActive.push_back(features[i].active);
        
        if (features[i].active) {
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

        bucketNumbers.push_back(totalBuckets);

        cerr << "feature " << i << " buckets from " << bucketNumbers[i]
             << " to " << bucketNumbers[i + 1] << " numBuckets "
             << bucketNumbers[i + 1] - bucketNumbers[i] << endl;
    }

    cerr << "doing " << rows.rowCount() << " rows with "
         << activeFeatures << " active features and "
         << totalBuckets << " total buckets" << endl;

    ExcAssertEqual(bucketDataOffsets.size(), nf);
    ExcAssertEqual(bucketEntryBits.size(), nf);
    ExcAssertEqual(bucketNumbers.size(), nf + 1);
    ExcAssertEqual(featuresActive.size(), nf);

    // Shouldn't happen, except during development / testing
    if (totalBuckets == 0 || activeFeatures == 0) {
        return std::make_tuple(1.0, -1, -1, rows.wAll, W(),
                               std::vector<uint8_t>(features.size(), false));
    }
    
    OpenCLKernelContext kernelContext = getKernelContext();
    
    OpenCLContext & context = kernelContext.context;

    cerr << "rows.rowData.data() = " << rows.rowData.data() << endl;
    cerr << "bucketData_.data() = " << bucketData_.data() << endl;

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

    cerr << "total offset = " << bucketData_.memusage() / 4 << endl;

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

        cerr << "feature " << i << " numBuckets "
             << features[i].buckets.numBuckets
             << " min " << minMax[0] << " max " << minMax[1] << endl;

        if (minMax[1] >= features[i].buckets.numBuckets) {
            cerr << "***** error" << endl;
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
    if (rows.rowCount() < 10000) {
        return testAllCpu(depth, features, rows, bucketData);
    }
    else {
        return testAllOpenCL(depth, features, rows, bucketData);
    }
}

} // namespace RF
} // namespace MLDB
