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

struct AtInit {


} atInit;

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

#if 0
    = context.createBuffer(0,
                               (const void *)rowIterator.owner->weightEncoder.weightFormatTable.data(),
                               rowIterator.owner->weightEncoder.weightFormatTable.memusage());
#endif

    OpenCLMemObject wOut
        = context.createBuffer(CL_MEM_READ_WRITE,
                               w,
                               sizeof(W) * buckets.numBuckets);

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
                wOut);
    
    auto devices = context.getDevices();
    
    auto queue = context.createCommandQueue
        (devices[0],
         OpenCLCommandQueueProperties::PROFILING_ENABLE);
    
    OpenCLEvent runKernel
        = queue.launch(kernel,
                       { 1024 },//(rowIterator.owner->rowCount() + 63numRowsPerWorkItem - 1) / numRowsPerWorkItem },
                       { 256 });

    OpenCLEvent transfer
        = queue.enqueueReadBuffer(wOut, 0 /* offset */,
                                  sizeof(W) * buckets.numBuckets /* length */,
                                  w,
                                  runKernel /* before */);
    
    transfer.waitUntilFinished();

    transfer.assertSuccess();

#if 0    
    for (size_t i = 0;  i < 10 && i < buckets.numBuckets;  ++i) {
        cerr << "w[" << i << "] = (" << w[i].v[0] << " " << w[i].v[1] << ")"
             << endl;
    }
#endif
    
    if (true) {

        std::vector<W> wCpu(buckets.numBuckets);
        
        testFeatureKernelCpu(rowIterator,
                             numRows,
                             buckets,
                             wCpu.data());

        for (size_t i = 0;  i < buckets.numBuckets;  ++i) {
            if (w[i] != wCpu[i]) {
                cerr << "error on w " << i << ": gpu = ("
                     << w[i].v[0] << "," << w[i].v[1] << "), cpu = ("
                     << wCpu[i].v[0] << "," << wCpu[i].v[1] << ")" << endl;
            }
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
    
    return { false, -1 };
}

// Chooses which is the best split for a given feature.
MLDB_NEVER_INLINE
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
    
} // namespace RF
} // namespace MLDB
