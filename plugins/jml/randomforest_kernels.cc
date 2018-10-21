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
    //return testFeatureKernelCpu(rowIterator, numRows, buckets, w);
    return testFeatureKernelOpencl(rowIterator, numRows, buckets, w);
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

std::tuple<double, int, int, W, W>
testAllCpu(int depth,
           std::vector<Feature> & features,
           const Rows & rows)
{
    // We have no impurity in our bucket.  Time to stop
    if (rows.wAll[0] == 0 || rows.wAll[1] == 0) {
        return std::make_tuple(1.0, -1, -1, rows.wAll, W());
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
                
            std::tie(score, split, bestLeft, bestRight, features[i].active)
            = testFeatureNumber(i, features, rowIterator,
                                rows.rowCount(), rows.wAll);
            return std::make_tuple(score, split, bestLeft, bestRight);
        };

    if (depth < 4 || rows.rowCount() * nf > 20000) {
        parallelMapInOrderReduce(0, nf, doFeature, findBest);
    }
    else {
        for (unsigned i = 0;  i < nf;  ++i)
            doFeature(i);
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

    return std::make_tuple(bestScore, bestFeature, bestSplit, bestLeft, bestRight);
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

std::tuple<double, int, int, W, W>
testAllOpenCL(int depth,
              std::vector<Feature> & features,
              const Rows & rows)
{
    // We have no impurity in our bucket.  Time to stop
    if (rows.wAll[0] == 0 || rows.wAll[1] == 0) {
        return std::make_tuple(1.0, -1, -1, rows.wAll, W());
    }

    static std::mutex mutex;
    std::unique_lock<std::mutex> guard(mutex);
    
    int nf = features.size();

    size_t activeFeatures = 0;

    for (unsigned i = 0;  i < nf;  ++i) {
        if (!features[i].active)
            continue;
        ++activeFeatures;
    }

    //cerr << "doing " << rows.rowCount() << " rows with "
    //     << activeFeatures << " active features" << endl;
    
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
    
    static OpenCLKernelContext kernelContext = getKernelContext();
    
    OpenCLContext & context = kernelContext.context;
    
    // Transfer rows, weights on the GPU; these are shared across all features
    OpenCLMemObject rowData
        = context.createBuffer(0,
                               (const void *)rows.rowData.data(),
                               rows.rowData.memusage());
    
    OpenCLMemObject weightData
        = context.createBuffer(CL_MEM_READ_ONLY, 4);
    
    if (rows.weightEncoder.weightFormat == WF_TABLE) {
        weightData = context.createBuffer
            (0,
             (const void *)rows.weightEncoder.weightFormatTable.data(),
             rows.weightEncoder.weightFormatTable.memusage());
    }

    OpenCLCommandQueue & queue = kernelContext.queue;
    
    OpenCLEventList featureEvents;
    

    auto doFeature = [&] (int i) {

        if (!features[i].active)
            return;

        const BucketList & buckets = features[i].buckets;

        std::vector<W> w(buckets.numBuckets);
        
        OpenCLKernel kernel
            = kernelContext.program.createKernel("testFeatureKernel");

        OpenCLMemObject bucketData
            = context.createBuffer(0,
                                   (const void *)buckets.storage.data(),
                                   buckets.storage.memusage());

        OpenCLMemObject wOut
            = context.createBuffer(CL_MEM_READ_WRITE,
                                   w.data(),
                                   sizeof(W) * w.size());

        int minMax[2] = { INT_MAX, INT_MIN };
        
        OpenCLMemObject minMaxOut
            = context.createBuffer(CL_MEM_READ_WRITE,
                                   minMax,
                                   sizeof(minMax[0]) * 2);

        size_t numRows = rows.rowCount();
        
        int numRowsPerWorkItem = (numRows + 1023) / 1024;
    
        kernel.bind(numRowsPerWorkItem,
                    rowData,
                    rows.totalBits,
                    rows.weightEncoder.weightBits,
                    rows.exampleNumBits,
                    (uint32_t)numRows,
                    bucketData,
                    buckets.entryBits,
                    buckets.numBuckets,
                    rows.weightEncoder.weightFormat,
                    rows.weightEncoder.weightMultiplier,
                    weightData,
                    LocalArray<W>(buckets.numBuckets),
                    wOut,
                    minMaxOut);
    
        OpenCLEvent runKernel
            = queue.launch(kernel,
                           { 1024 },
                           { 256 });

        OpenCLEvent wTransfer
            = queue.enqueueReadBuffer(wOut, 0 /* offset */,
                                      sizeof(W) * buckets.numBuckets /* length */,
                                      w.data(),
                                      runKernel /* before */);

        OpenCLEvent minMaxTransfer
            = queue.enqueueReadBuffer(minMaxOut, 0 /* offset */,
                                      sizeof(minMax[0]) * 2 /* length */,
                                      minMax,
                                      runKernel);
    
        OpenCLEvent doneFeature = queue.enqueueMarker
            ({ runKernel, wTransfer, minMaxTransfer });

        doneFeature.waitUntilFinished();
        runKernel.assertSuccess();
        wTransfer.assertSuccess();
        minMaxTransfer.assertSuccess();
        doneFeature.assertSuccess();
        
        bool active = (minMax[0] != minMax[1]);
        int maxBucket = minMax[1];

        ExcAssertLessEqual(minMax[1], buckets.numBuckets);
        ExcAssertGreaterEqual(minMax[1], 0);
        
        if (active) {
            findBest(i, chooseSplitKernel(w.data(), maxBucket, features[i].ordinal,
                                          rows.wAll));
        }
        else {
            features[i].active = false;
        }
    };

    cerr << rows.rowCount() << " rows, " << nf << " features";
    
    for (int i = 0;  i < nf;  ++i) {
        doFeature(i);
        cerr << " " << i;
    }

    cerr << endl;
    
    return std::make_tuple(bestScore, bestFeature, bestSplit, bestLeft, bestRight);
}

std::tuple<double, int, int, W, W>
testAll(int depth,
        std::vector<Feature> & features,
        const Rows & rows)
{
    return testAllOpenCL(depth, features, rows);
}

} // namespace RF
} // namespace MLDB
