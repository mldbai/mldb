/** randomforest_kernels.h                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    OpenCL kernels for random forest algorithm.

    clang-6.0 -Xclang -finclude-default-header -cl-std=CL1.2 -Dcl_clang_storage_class_specifiers -target nvptx64-nvidia-nvcl -xcl plugins/jml/randomforest_kernels.cl -emit-llvm -cl-kernel-arg-info -S -o test.ll
*/

#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable

typedef unsigned uint32_t;
typedef int int32_t;
typedef unsigned long uint64_t;
typedef long int64_t;
typedef signed char int8_t;
typedef unsigned char uint8_t;
typedef unsigned short uint16_t;

static const __constant float VAL_2_HL = 1.0f * (1UL << 63);
static const __constant float HL_2_VAL = 1.0f / (1UL << 63);
static const __constant float VAL_2_H = (1UL << 31);
static const __constant float H_2_VAL = 1.0f / (1UL << 31);
static const __constant float ADD_TO_ROUND = 0.5f / (1UL << 63);

float decodeWeight(uint32_t bits, int floatEncoding, float baseMultiplier,
                   __global const float * table)
{
    if (floatEncoding == 0) {
        return bits * baseMultiplier;
    }
    else if (floatEncoding == 1) {
        return as_float(bits);
    }
    else if (floatEncoding == 2) {
        return table[bits];
    }
    else return INFINITY;
}

uint32_t createMask32(int numBits)
{
    return numBits >= 32 ? -1 : (((uint32_t)1 << numBits) - 1);
}

uint64_t createMask64(int numBits)
{
    return numBits >= 64 ? -1 : (((uint64_t)1 << numBits) - 1);
}

inline uint64_t extractBitRange64(__global const uint64_t * data,
                                  int numBits,
                                  int entryNumber,
                                  uint64_t mask,
                                  uint32_t dataLength)
{
    long bitNumber = numBits * (long)entryNumber;
    if (bitNumber >= INT_MAX) {
        printf("bit number requires > 32 bits: %ld\n", bitNumber); 
    }
    int wordNumber = bitNumber / 64;
    int wordOffset = bitNumber % 64;

    if (wordNumber >= dataLength) {
        printf("OUT OF RANGE WORD %d vs %d\n", wordNumber, dataLength);
        return 0;
    }
    
    //return data[wordNumber];
    
    //printf("reading word number %d in worker %ld\n",
    //       wordNumber, get_global_id(0));
    
    //printf("wordNumber = %d, bitNumber = %d\n", wordNumber, wordOffset);
    
    int bottomBits = min(numBits, 64 - wordOffset);
    int topBits = numBits - bottomBits;

    //printf("numBits = %d, bottomBits = %d, topBits = %d\n",
    //       numBits, bottomBits, topBits);
    
    //uint64_t mask = createMask64(numBits);

    //printf("mask = %016lx\n", mask);
    
    uint64_t val = data[wordNumber];

    //printf("val = %016lx\n", val);

    val >>= wordOffset;

    if (topBits > 0) {
        uint64_t val2 = data[wordNumber + 1];
        val = val | val2 << bottomBits;
    }
    val = val & mask;
    //printf("val out = %016lx\n", val);
    return val;
}

inline uint32_t extractBitRange32(__global const uint32_t * data,
                                  int numBits,
                                  int entryNumber)
{
    int bitNumber = numBits * entryNumber;
    int wordNumber = bitNumber / 32;
    int wordOffset = bitNumber % 32;

    //if (wordNumber >= dataLength) {
    //    printf("OUT OF RANGE WORD 32 %d vs %d\n", wordNumber, dataLength);
    //    return 0;
    //}
    //printf("wordNumber = %d, bitNumber = %d\n", wordNumber, wordOffset);
    
    int bottomBits = min(numBits, 32 - wordOffset);
    int topBits = numBits - bottomBits;

    //printf("numBits = %d, bottomBits = %d, topBits = %d\n",
    //       numBits, bottomBits, topBits);
    
    uint32_t mask = createMask32(numBits);

    //printf("mask = %08x\n", mask);
    
    uint32_t val = data[wordNumber];

    //printf("val = %08x\n", val);

    val >>= wordOffset;

    if (topBits > 0) {
        uint32_t val2 = data[wordNumber + 1];
        val = val | val2 << bottomBits;
    }
    val = val & mask;
    //printf("val out = %08lx\n", val);
    return val;
}

void getDecodedRow(uint32_t rowNumber,

                   __global const uint64_t * rowData,
                   uint32_t rowDataLength,
                   uint32_t totalBits,
                   uint32_t weightBits,
                   uint32_t exampleBits,
                   uint32_t numRows,

                   int weightEncoding,
                   float weightMultiplier,
                   __global const float * weightTable,
                   
                   uint32_t * example,
                   float * weight,
                   bool * label,

                   uint64_t mask,
                   uint32_t exampleMask,
                   uint32_t weightMask,
                   uint32_t labelMask)
{
    //*example = rowNumber;
    //*weight = weightMultiplier;
    //*label = (rowNumber % 4 == 0);
    //return;
    uint64_t bits = extractBitRange64(rowData, totalBits, rowNumber, mask,
                                      rowDataLength);
    //printf("rowNum %d bits = %016lx weightBits = %d exampleBits = %d\n",
    //       rowNumber, bits, weightBits, exampleBits);
    //printf("exampleMask = %016lx example = %016lx\n",
    //       createMask64(exampleBits),
    //       bits & createMask64(exampleBits));
    *example = exampleBits == 0 ? rowNumber : bits & exampleMask;
    *weight = decodeWeight((bits >> exampleBits) & weightMask,
                           weightEncoding, weightMultiplier, weightTable);
    *label = (bits & labelMask) != 0;
}

inline uint32_t getBucket(uint32_t exampleNum,
                          __global const uint32_t * bucketData,
                          uint32_t bucketDataLength,
                          uint32_t bucketBits,
                          uint32_t numBuckets)
{
    return extractBitRange32(bucketData, bucketBits, exampleNum);
}

typedef struct W {
    int64_t vals[2];
    int32_t count;
    int32_t unused;
} W;// __attribute__((packed)) __attribute__((aligned(4)));

void zeroW(__local W * w)
{
    w->vals[0] = 0;
    w->vals[1] = 0;
    w->count = 0;
}

int64_t encodeW(float f)
{
    int64_t result = convert_long((f + ADD_TO_ROUND) * VAL_2_HL);
    //printf("encoding %g (%08x) to %g (%08x) to %ld, ADD_TO_ROUND = %g, VAL_2_HL = %g\n",
    //       f, as_int(f),
    //       (f + ADD_TO_ROUND) * VAL_2_HL, as_int((f + ADD_TO_ROUND) * VAL_2_HL),
    //       result, ADD_TO_ROUND, VAL_2_HL);
    return result;
}

float decodeW(int64_t v)
{
    return v * HL_2_VAL;
}

void incrementWLocal(__local W * w, bool label, float weight)
{
    int64_t inc = encodeW(weight);
    atom_add(&w->vals[label ? 1 : 0], inc);
    atom_inc(&w->count);
}

void decrementWLocal(__local W * w, bool label, float weight)
{
    int64_t inc = encodeW(weight);
    atom_sub(&w->vals[label ? 1 : 0], inc);
    atom_dec(&w->count);
}

void incrementWGlobal(__global W * w, bool label, float weight)
{
    //if (weight != 0.0)
    //    return;
    
    int64_t inc = encodeW(weight);
#if 1
    atom_add(&w->vals[label ? 1 : 0], inc);
    atom_inc(&w->count);
#else
    w->vals[label ? 1 : 0] += inc;
    ++w->count;
#endif
}

void incrementWOut(__global W * wOut, __local const W * wIn)
{
    atom_add(&wOut->vals[0], wIn->vals[0]);
    atom_add(&wOut->vals[1], wIn->vals[1]);
    int oldCount = atom_add(&wOut->count,   wIn->count);
    //if (oldCount < 0 || oldCount + wIn->count < 0) {
    //    printf("W COUNT < 0: %d + %d = %d\n", oldCount, wIn->count, oldCount + wIn->count);
    //}
}

uint32_t testRow(uint32_t rowId,

                 __global const uint64_t * rowData,
                 uint32_t rowDataLength,
                 uint32_t totalBits,
                 uint32_t weightBits,
                 uint32_t exampleBits,
                 uint32_t numRows,
                   
                 __global const uint32_t * bucketData,
                 uint32_t bucketDataLength,
                 uint32_t bucketBits,
                 uint32_t numBuckets,
                   
                 int weightEncoding,
                 float weightMultiplier,
                 __global const float * weightTable,
                   
                 __local W * w,
                 __global W * wOut,
                 uint32_t maxLocalBuckets,
                 
                 uint64_t mask,
                 uint32_t exampleMask,
                 uint32_t weightMask,
                 uint32_t labelMask)
{
    uint32_t exampleNum = 0;
    float weight = 1.0;
    bool label = false;

    uint32_t bucket;// = rowId % numBuckets;

#if 1
    getDecodedRow(rowId, rowData, rowDataLength,
                  totalBits, weightBits, exampleBits, numRows,
                  weightEncoding, weightMultiplier, weightTable,
                  &exampleNum, &weight, &label,
                  mask, exampleMask, weightMask, labelMask);

    //if (exampleNum >= numRows) {
    //    printf("ERROR EXAMPLE NUM: got %d numRows %d row %d feature %d\n",
    //           exampleNum, numRows, rowId, get_global_id(0));
    //    return 0;
    //}
    
    bucket = getBucket(exampleNum, bucketData, bucketDataLength,
                       bucketBits, numBuckets);
    if (bucket >= numBuckets) {
        printf("ERROR BUCKET NUMBER: got %d numBuckets %d row %d feature %d\n",
               bucket, numBuckets, rowId, get_global_id(0));
        return 0;
    }
    //bucket = min(bucket, numBuckets - 1);
#else
    bucket = rowId % numBuckets;
#endif
    
    //if (rowId < 10)
    //    printf("rowId %d exampleNum %d bucket %d of %d weight %g label %d\n",
    //           rowId, exampleNum, bucket, numBuckets, weight, label);

    if (bucket < maxLocalBuckets) {
        incrementWLocal(w + bucket, label, weight);
    }
    else {
        incrementWGlobal(wOut + bucket, label, weight);
    }

    return bucket;
}

__kernel void testFeatureKernel(uint32_t numRowsPerWorkgroup,

                                __global const uint64_t * rowData,
                                uint32_t rowDataLength,
                                uint32_t totalBits,
                                uint32_t weightBits,
                                uint32_t exampleBits,
                                uint32_t numRows,

                                __global const uint32_t * allBucketData,
                                __global const uint32_t * bucketDataOffsets,
                                __global const uint32_t * bucketNumbers,
                                __global const uint32_t * bucketEntryBits,

                                int weightEncoding,
                                float weightMultiplier,
                                __global const float * weightTable,

                                __global const uint32_t * featureActive,
                                
                                __local W * w,
                                uint32_t maxLocalBuckets,
                                __global W * allWOut,
                                __global int * allMinMaxOut)
{
    const uint32_t workGroupId = get_global_id (0);
    const uint32_t workerId = get_local_id(0);
    const uint32_t f = get_global_id(1);
    
    uint32_t bucketDataOffset = bucketDataOffsets[f];
    uint32_t bucketDataLength = bucketDataOffsets[f + 1] - bucketDataOffset;
    uint32_t numBuckets = bucketNumbers[f + 1] - bucketNumbers[f];
    __global const uint32_t * bucketData = allBucketData + bucketDataOffset;
    uint32_t bucketBits = bucketEntryBits[f];

    __global W * wOut = allWOut + bucketNumbers[f];
    __global int * minMaxOut = allMinMaxOut + 2 * f;

    if (!featureActive[f])
        return;
    
    __local int minWorkgroupBucket, maxWorkgroupBucket;

    if (workGroupId == 0 && false) {
        printf("feat %d global size %ld, num groups %ld, local size %ld, numRows %d, per wg %d, numBuckets %d, buckets %d-%d, offset %d\n",
               get_global_id(1),
               get_global_size(0),
               get_num_groups(0),
               get_local_size(0),
               numRows,
               numRowsPerWorkgroup,
               numBuckets,
               bucketNumbers[f],
               bucketNumbers[f + 1],
               bucketDataOffset);
    }
    
    if (workerId == 0) {
        minWorkgroupBucket = INT_MAX;
        maxWorkgroupBucket = INT_MIN;
    }

    //printf("workGroupId = %d workerId = %d\n",
    //       workGroupId, workerId);
    
    //if (workGroupId == 0) {
    //    printf("Num work groups = %d size = %d\n",
    //           get_num_groups(0), get_local_size(0));
    //}

    //printf("workerId = %d, localsize[0] = %d\n",
    //       workerId, get_local_size(0));
    
    for (int i = workerId;  i < numBuckets && i < maxLocalBuckets;
         i += get_local_size(0)) {
        //printf("zeroing %d of %d for feature %d\n", i, numBuckets, f);
        zeroW(w + i);
    }

    barrier(CLK_LOCAL_MEM_FENCE);
    //if (rowId % 10000 == 0)
    //    printf("afer barrier %d\n", rowId);

    //if (rowId > 4)
    //    return;
    
    int minBucket = INT_MAX;
    int maxBucket = INT_MIN;

    int i = 0;
    
    uint64_t mask = createMask64(totalBits);
    uint32_t exampleMask = createMask32(exampleBits);
    uint32_t weightMask = createMask32(weightBits);
    uint32_t labelMask = (1 << (weightBits + exampleBits));

    // global id 0 does 0, 1024, 2048, ...
    
    for (int rowId = get_global_id(0);  rowId < numRows;  rowId += get_global_size(0)) {
        //int rowId = workGroupId * numRowsPerWorkgroup + i;
        //int rowId = workGroupId + i * get_local_size(0);
        //if (workGroupId == 0)
        //    printf("i = %d getting row %d in worker %d with %ld groups\n",
        //           i, rowId, workGroupId, get_local_size(0));
        //printf("rowId = %d, numRows = %d\n", rowId, numRows);
        if (rowId < numRows) {
            int bucket
                = testRow(rowId, rowData, rowDataLength,
                          totalBits, weightBits, exampleBits,
                          numRows,
                          bucketData, bucketDataLength, bucketBits, numBuckets,
                          weightEncoding, weightMultiplier, weightTable, w,
                          wOut, maxLocalBuckets,
                          mask, exampleMask, weightMask, labelMask);
            minBucket = min(minBucket, bucket);
            maxBucket = max(maxBucket, bucket);
        }
    }

    atomic_min(&minWorkgroupBucket, minBucket);
    atomic_max(&maxWorkgroupBucket, maxBucket);
    
    //minBucket = work_group_reduce_min(minBucket);
    //maxBucket = work_group_reduce_max(maxBucket);
    
    barrier(CLK_LOCAL_MEM_FENCE);


    for (int i = workerId;  i < numBuckets && i < maxLocalBuckets;
         i += get_local_size(0)) {
        //printf("copying %d group %ld\n", i, get_group_id(0));
        //wOut[i].vals[0] = w[i].vals[0];
        //wOut[i].vals[1] = w[i].vals[1];
        incrementWOut(wOut + i, w + i);
    }

    if (workerId == 0) {
        //printf("feat %d index %d minBucket %d maxBucket %d\n",
        //       f, workGroupId, minWorkgroupBucket, maxWorkgroupBucket);
        atomic_min(minMaxOut + 0, minWorkgroupBucket);
        atomic_max(minMaxOut + 1, maxWorkgroupBucket);
    }
    
    barrier(CLK_GLOBAL_MEM_FENCE);

    if (workGroupId == 0 && false) {
        printf("feat %d global size %ld, num groups %ld, local size %ld, numRows %d, per wg %d, numBuckets %d, min %d, max %d\n",
               get_global_id(1),
               get_global_size(0),
               get_num_groups(0),
               get_local_size(0),
               numRows,
               numRowsPerWorkgroup,
               numBuckets,
               minMaxOut[0],
               minMaxOut[1]);
    }
}

// Take a bit-compressed representation of rows, and turn it into a
// decompressed version with one float per row (the sign gives the
// label, and the magnitude gives the weight).
//
// This is a 1D kernel over the array of rows, not very complicated

__kernel void
decompressRowsKernel(__global const uint64_t * rowData,
                     uint32_t rowDataLength,
                     uint32_t totalBits,
                     uint32_t weightBits,
                     uint32_t exampleBits,
                     uint32_t numRows,
                     
                     int weightEncoding,
                     float weightMultiplier,
                     __global const float * weightTable,
                     
                     __global float * decompressedWeightsOut)
{
    //if (get_global_id(0) == 0) {
    //    printf("sizeof(W) = %d\n", sizeof(W));
    //}

    uint32_t exampleNum = 0;
    float weight = 1.0;
    bool label = false;

    uint32_t bucket;// = rowId % numBuckets;

    uint64_t mask = createMask64(totalBits);
    uint32_t exampleMask = createMask32(exampleBits);
    uint32_t weightMask = createMask32(weightBits);
    uint32_t labelMask = (1 << (weightBits + exampleBits));

    for (int rowId = get_global_id(0);  rowId < numRows;
         rowId += get_global_size(0)) {
        getDecodedRow(rowId, rowData, rowDataLength,
                      totalBits, weightBits, exampleBits, numRows,
                      weightEncoding, weightMultiplier, weightTable,
                      &exampleNum, &weight, &label,
                      mask, exampleMask, weightMask, labelMask);
        
        if (exampleNum != rowId) {
            printf("non-consecutive example numbers");
        }

        float encoded = label ? -weight : weight;
        
        if (false && rowId < 128) {
            printf("row %d ex %d wt %f lb %d encoded %f\n",
                   rowId, exampleNum, weight, label, encoded);
        }
        
        decompressedWeightsOut[rowId] = encoded;
    }
}

// Decompress the data for the features, to save on access time
__kernel void
decompressFeatureBucketsKernel(uint32_t numRows,
                               __global const uint32_t * allBucketData,
                               __global const uint32_t * bucketDataOffsets,
                               __global const uint32_t * bucketNumbers,
                               __global const uint32_t * bucketEntryBits,
                               
                               __global const uint32_t * featureActive,
                               
                               __global uint16_t * featuresOut,
                               __global const uint32_t * featureDataOffsets)
{
    int f = get_global_id(1);

    if (!featureActive[f])
        return;

    uint32_t bucketDataOffset = bucketDataOffsets[f];
    uint32_t bucketDataLength = bucketDataOffsets[f + 1] - bucketDataOffset;
    uint32_t numBuckets = bucketNumbers[f + 1] - bucketNumbers[f];
    __global const uint32_t * bucketData = allBucketData + bucketDataOffset;
    uint32_t bucketBits = bucketEntryBits[f];
    
    __global uint16_t * out = featuresOut + featureDataOffsets[f] / sizeof(*out);

    for (int rowId = get_global_id(0);  rowId < numRows; rowId += get_global_size(0)) {
        uint32_t bucket = getBucket(rowId, bucketData, bucketDataLength, bucketBits, numBuckets);
        out[rowId] = bucket;
    }
}


uint32_t testRowExpanded(uint32_t rowId,

                         __global const float * rowData,
                         uint32_t numRows,
                   
                         __global const uint32_t * bucketData,
                         uint32_t bucketDataLength,
                         uint32_t bucketBits,
                         uint32_t numBuckets,

                         __global const uint16_t * expandedBuckets,
                         bool useExpandedBuckets,
                         
                         __local W * w,
                         __global W * wOut,
                         uint32_t maxLocalBuckets)
{
    uint32_t exampleNum = rowId;
    float val = rowData[rowId];
    float weight = fabs(val);
    bool label = val < 0;
    uint32_t bucket;
    if (!useExpandedBuckets) {
        bucket = getBucket(exampleNum, bucketData, bucketDataLength,
                           bucketBits, numBuckets);
    }
    else {
        bucket = expandedBuckets[exampleNum];
    }
    if (bucket >= numBuckets) {
        printf("ERROR BUCKET NUMBER: got %d numBuckets %d row %d feature %d\n",
               bucket, numBuckets, rowId, get_global_id(0));
        return 0;
    }

    if (bucket < maxLocalBuckets) {
        incrementWLocal(w + bucket, label, weight);
    }
    else {
        incrementWGlobal(wOut + bucket, label, weight);
    }

    return bucket;
}

__kernel void
testFeatureKernelExpanded(__global const float * expandedRows,
                          uint32_t numRows,

                          __global const uint32_t * allBucketData,
                          __global const uint32_t * bucketDataOffsets,
                          __global const uint32_t * bucketNumbers,
                          __global const uint32_t * bucketEntryBits,

                          __global const uint16_t * expandedBuckets,
                          __global const uint32_t * expandedBucketOffsets,
                          uint32_t useExpandedBuckets,
                          
                          __global const uint32_t * featureActive,
                                
                          __local W * w,
                          uint32_t maxLocalBuckets,
                          __global W * allWOut)
{
    const uint32_t workGroupId = get_global_id (0);
    const uint32_t workerId = get_local_id(0);
    const uint32_t f = get_global_id(1);
    
    uint32_t bucketDataOffset = bucketDataOffsets[f];
    uint32_t bucketDataLength = bucketDataOffsets[f + 1] - bucketDataOffset;
    uint32_t numBuckets = bucketNumbers[f + 1] - bucketNumbers[f];
    __global const uint32_t * bucketData = allBucketData + bucketDataOffset;
    uint32_t bucketBits = bucketEntryBits[f];

    if (useExpandedBuckets) {
        expandedBuckets += expandedBucketOffsets[f] / sizeof(expandedBuckets[0]);
    }
    
    __global W * wOut = allWOut + bucketNumbers[f];

    if (!featureActive[f])
        return;
    
    if (workGroupId == 0 && false) {
        printf("feat %d global size %ld, num groups %ld, local size %ld, numRows %d, numBuckets %d, buckets %d-%d, offset %d\n",
               get_global_id(1),
               get_global_size(0),
               get_num_groups(0),
               get_local_size(0),
               numRows,
               numBuckets,
               bucketNumbers[f],
               bucketNumbers[f + 1],
               bucketDataOffset);
    }
    
    for (int i = workerId;  i < numBuckets && i < maxLocalBuckets;
         i += get_local_size(0)) {
        zeroW(w + i);
    }

    barrier(CLK_LOCAL_MEM_FENCE);
    
    int i = 0;
    
    for (int rowId = get_global_id(0);  rowId < numRows;  rowId += get_global_size(0)) {
        testRowExpanded(rowId,
                        expandedRows, numRows,
                        bucketData, bucketDataLength, bucketBits, numBuckets,
                        expandedBuckets, useExpandedBuckets,
                        w, wOut, maxLocalBuckets);
    }
    
    barrier(CLK_LOCAL_MEM_FENCE);
    
    for (int i = workerId;  i < numBuckets && i < maxLocalBuckets;
         i += get_local_size(0)) {
        incrementWOut(wOut + i, w + i);
    }
}

typedef struct {
    float score;
    int feature;
    int value;
    W left;
    W right;
    int direction;
    int activeFeatures[6];
} PartitionSplit;

__kernel void
fillPartitionSplitsKernel(__global PartitionSplit * splits)
{
    int n = get_global_id(0);
    //printf("filling partition %d at %ld\n",
    //       n, (long)(((__global char *)(splits + n)) - (__global char *)splits));
    PartitionSplit spl = { INFINITY, -1, -1, { { 0, 0 }, 0 }, { { 0, 0}, 0},
                           0, { 0, 0, 0, 0, 0, 0 } };
    splits[n] = spl;
}

inline double sqrt2(float x)
{
    return sqrt((double)x);
}

inline float scoreSplit(W wFalse, W wTrue)
{
    double score
        = 2.0 * (    sqrt2(decodeW(wFalse.vals[0]) * decodeW(wFalse.vals[1]))
                   + sqrt2(decodeW(wTrue.vals[0]) * decodeW(wTrue.vals[1])));
    return score;
};

PartitionSplit
chooseSplitKernelOrdinal(__global const W * w,
                         int numBuckets,
                         W wAll)
{
    PartitionSplit result;

    int bucket = get_global_id(0);

    // For now, we serialize on a single thread
    if (bucket != 0)
        return result;

    int f = get_global_id(1);

    int p = get_global_id(2);
    
    //if (bucket == 0) {
    //   printf("feature %d is ordinal\n", f);
    //}

    // Calculate best split point for ordered values
    W wFalse = wAll, wTrue = { {0, 0}, 0 };

    double bestScore = INFINITY;
    int bestSplit = -1;
    W bestLeft, bestRight;
    
    // Now test split points one by one
    for (unsigned j = 0;  j < numBuckets;  ++j) {

        if (wFalse.count < 0 || wTrue.count < 0)
            printf("p %d f %d split %d false = (%.4f,%.4f,%d) true = (%.4f,%.4f,%d) cur = (%.4f,%.4f,%d) all=(%.4f,%.4f,%d)\n", p, f, j,
                   decodeW(wFalse.vals[0]), decodeW(wFalse.vals[1]), wFalse.count,
                   decodeW(wTrue.vals[0]), decodeW(wTrue.vals[1]), wTrue.count,
                   decodeW(w[j].vals[0]), decodeW(w[j].vals[1]), w[j].count,
                   decodeW(wAll.vals[0]), decodeW(wAll.vals[1]), wAll.count);
        //printf("split %d false = (%d) true = (%d)\n", j,
        //       wFalse.count,
        //       wTrue.count);
        
        if (wFalse.count > 0 && wTrue.count > 0) {
            
            float s = scoreSplit(wFalse, wTrue);

            //if (f == 4 && get_global_id(2) == 61) {
            //    printf("p %d f %d bucket %d score %.10f\n",
            //           (uint32_t)get_global_id(2), f, j, s);
            //}
            
            //printf("f %d bucket %d score %f\n",
            //       f, j, s);
            
            if (s < bestScore) {
                bestScore = s;
                bestSplit = j;
                bestRight = wFalse;
                bestLeft = wTrue;
            }
        }
            
        if (j < numBuckets - 1) {
            wFalse.vals[0] -= w[j].vals[0];
            wTrue.vals[0]  += w[j].vals[0];
            wFalse.vals[1] -= w[j].vals[1];
            wTrue.vals[1]  += w[j].vals[1];
            wFalse.count   -= w[j].count;
            wTrue.count    += w[j].count;
        }
    }

    result.score = bestScore;

    result.feature = f;
    result.value = bestSplit;
    result.left = bestLeft;
    result.right = bestRight;

    return result;
}

PartitionSplit
chooseSplitKernelCategorical(__global const W * w,
                             int numBuckets,
                             W wAll)
{
    PartitionSplit result;

    int bucket = get_global_id(0);

    // For now, we serialize on a single thread
    if (bucket != 0)
        return result;

    int f = get_global_id(1);

    //if (bucket == 0) {
    //    printf("feature %d is categorical\n", f);
    //}

    double bestScore = INFINITY;
    int bestSplit = -1;
    W bestLeft, bestRight;
    
    // Now test split points one by one
    for (unsigned j = 0;  j < numBuckets;  ++j) {


        if (w[j].count == 0)
            continue;

        W wFalse = wAll;

        wFalse.vals[0] -= w[j].vals[0];
        wFalse.vals[1] -= w[j].vals[1];
        wFalse.count   -= w[j].count;

        if (wFalse.count == 0) {
            continue;
        }
            
        double s = scoreSplit(wFalse, w[j]);

        //if (f == 3 && get_global_id(2) == 61) {
        //    printf("p %d f %d bucket %d score %.10f\n",
        //           get_global_id(2), f, j, s);
        //}
            
        if (s < bestScore) {
            bestScore = s;
            bestSplit = j;
            bestRight = wFalse;
            bestLeft = w[j];
        }
    }

    result.score = bestScore;

    result.feature = f;
    result.value = bestSplit;
    result.left = bestLeft;
    result.right = bestRight;

    return result;
}

inline void atomic_store_int(__global int * p, int val)
{
    atomic_xchg(p, val);
}

inline void atomic_store_long(__global long * p, long val)
{
    atom_xchg(p, val);
}

inline void atomic_store_W(__global W * w, const W * val)
{
    printf("atomic store W (%ld,%ld,%d)\n", val->vals[0], val->vals[1], val->count);
    atomic_store_long(&w->vals[0], val->vals[0]);
    atomic_store_long(&w->vals[1], val->vals[1]);
    atomic_store_int(&w->count,    val->count);
    printf("atomic store W wrote (%ld,%ld,%d)\n", w->vals[0], w->vals[1], w->count);
}

__kernel void
getPartitionSplitsKernel(uint32_t totalBuckets,
                         __global const uint32_t * bucketNumbers,
                         
                         __global const uint32_t * featureActive,
                         __global const uint32_t * featureIsOrdinal,
                         
                         __global const W * allW,

                         __global const W * wAll,  // one per partition
                         __global PartitionSplit * splitsOut,
                         __global volatile int * partitionLocks)
{
    int f = get_global_id(1);

    // Don't do inactive features
    if (!featureActive[f])
        return;
    
    int bucket = get_global_id(0);
    int partition = get_global_id(2);
    int numPartitions = get_global_size(2);

    if (wAll[partition].count == 0)
        return;
    
    //if (partition == 0 && bucket == 0) {
    //    printf("sizeof(PartitionSplit) = %d\n", sizeof(PartitionSplit));
    //}
    
    uint32_t bucketStart = bucketNumbers[f];
    uint32_t bucketEnd = bucketNumbers[f + 1];
    uint32_t numBuckets = bucketEnd - bucketStart;

    // We dimension as the feature with the highest number of buckets, so
    // for most of them we have to many and can stop
    if (bucket >= numBuckets)
        return;

    // Find where our bucket data starts
    __global const W * myW
        = allW
        + (totalBuckets * partition)
        + bucketStart;
    
    bool ordinal = featureIsOrdinal[f];
    PartitionSplit best;

    // TODO: use prefix sums to enable better parallelization (this will mean
    // multiple kernel launches, though, so not sure) or accumulate locally
    // per set of buckets and then accumulate globally in a second operation.
    if (bucket != 0)
        return;

    if (ordinal) {
        // We have to perform a prefix sum to have the data over which
        // we can calculate the split points.  This is more complicated.
        best = chooseSplitKernelOrdinal(myW, numBuckets, wAll[partition]);
    }
    else {
        // We have a simple search which is independent per-bucket.
        best = chooseSplitKernelCategorical(myW, numBuckets, wAll[partition]);
    }

    best.direction
        = best.feature != -1 && best.left.count <= best.right.count;
    
    __global PartitionSplit * splitOut = splitsOut + partition;

    //printf("part %d feature %d bucket %d score %f\n",
    //       partition, f, best.value, best.score);
    
    // Finally, we have each feature update the lowest split for the
    // partition.  Spin until we've acquired the lock for the partition.
    // We won't hold it for long, so the spinlock is not _too_ inefficient.
    // NOTE: this only works because we make all workgroups apart from one
    // return above.  In general, you can't do a simple spin lock like this
    // in a kernel without causing a deadlock.
    while (atomic_inc(&partitionLocks[partition]) != 0) {
        atomic_dec(&partitionLocks[partition]);
    }

    // We can only access the splitOut values via atomic operations, as the
    // non-atomic global memory operations are only eventually consistent
    // over workgroups, and on AMD GPUs the writes may not be visible to
    // other workgroups until the kernel has finished.
    
    float currentScore = atomic_xchg(&splitOut->score, best.score);
    int currentFeature = atomic_xchg(&splitOut->feature, best.feature);
    
    // If we have the best score or an equal score and a lower feature number
    // (for determinism), then we update the output
    if (best.score < currentScore
        || (best.score == currentScore
            && best.feature < currentFeature)) {

        //printf("BEST, %.10f < %.10f\n", best.score, currentScore);
        
        // Copy the rest in, atomically
        atomic_store_int(&splitOut->value, best.value);

        atomic_store_long(&splitOut->left.vals[0], best.left.vals[0]);
        atomic_store_long(&splitOut->left.vals[1], best.left.vals[1]);
        atomic_store_int(&splitOut->left.count, best.left.count);

        atomic_store_long(&splitOut->right.vals[0], best.right.vals[0]);
        atomic_store_long(&splitOut->right.vals[1], best.right.vals[1]);
        atomic_store_int(&splitOut->right.count, best.right.count);

        atomic_store_int(&splitOut->direction, best.direction);
        
        // Shouldn't be needed, but just in case...
        mem_fence(CLK_GLOBAL_MEM_FENCE);
    }
    else {
        // Not the best, exchange them back
        atomic_xchg(&splitOut->score, currentScore);
        atomic_xchg(&splitOut->feature, currentFeature);
    }
    
    // Finally, release the lock to let another feature in
    atomic_dec(&partitionLocks[partition]);
}

// For each partition and each bucket, start with all weight in the
// bucket with the largest count.  That way we minimize the number of
// rows that are in the wrong bucket.
//
// This is a 2 dimensional kernel:
// Dimension 0 = partition number (from 0 to the old number of partitions)
// Dimension 1 = bucket number (from 0 to the number of active buckets)
__kernel void
transferBucketsKernel(__global W * allPartitionBuckets,
                      __global W * wAll,
                      __global const PartitionSplit * partitionSplits,
                      uint32_t numActiveBuckets)

{
    uint32_t numPartitions = get_global_size(0);

    int i = get_global_id(0);

    // Commented out because it causes spurious differences in the debugging code between
    // the two sides.  It doesn' thave any effect on the output, though, and saves some
    // work.
    //if (partitionSplits[i].right.count == 0)
    //    return;
            
    int j = get_global_id(1);

    if (j >= numActiveBuckets)
        return;
    
    __global W * bucketsLeft
        = allPartitionBuckets + i * numActiveBuckets;
    __global W * bucketsRight
        = allPartitionBuckets + (i + numPartitions) * numActiveBuckets;
    
    // We double the number of partitions.  The left all
    // go with the lower partition numbers, the right have the higher
    // partition numbers.

    const W empty = { { 0, 0 }, 0 };
    
    if (partitionSplits[i].direction) {
        // Those buckets which are transfering right to left should
        // start with the weight on the right
        bucketsRight[j] = bucketsLeft[j];
        bucketsLeft[j] = empty;

        if (j == 0) {
            wAll[i + numPartitions] = wAll[i];
            wAll[i] = empty;
        }
    }
    else {
        // Otherwise, we start with zero on the right
        bucketsRight[j] = empty;
        if (j == 0) {
            wAll[i + numPartitions] = empty;
        }
    }
}

inline void incrementWAtomic(__global W * w,
                             bool label,
                             float weight)
{
    int64_t incr = encodeW(weight);
    atom_add(&w->vals[label], incr);
    atom_inc(&w->count);
}

inline void decrementWAtomic(__global W * w,
                             bool label,
                             float weight)
{
    int64_t incr = encodeW(weight);
    atom_sub(&w->vals[label], incr);
    atom_dec(&w->count);
}

#if 0
inline void incrementWLocal(__local W * w,
                            bool label,
                            float weight)
{
    int64_t incr = encodeW(weight);
    atom_add(&w->vals[label], incr);
    atom_inc(&w->count);
}

inline void decrementWLocal(__local W * w,
                            bool label,
                            float weight)
{
    int64_t incr = encodeW(weight);
    atom_sub(&w->vals[label], incr);
    atom_dec(&w->count);
}
#endif

// First part: per row, accumulate
// - weight
// - label
// - from
// - to

// Second part: per feature plus wAll, transfer examples

__kernel void
updateBucketsKernel(uint32_t rightOffset,
                    uint32_t numActiveBuckets,
                    
                    __global uint8_t * partitions,
                    __global W * partitionBuckets,
                    __global W * wAll,

                    __global const PartitionSplit * partitionSplits,
                    
                    // Row data
                    __global const float * decodedRows,
                    uint32_t rowCount,
                    
                    // Feature data
                    __global const uint32_t * allBucketData,
                    __global const uint32_t * bucketDataOffsets,
                    __global const uint32_t * bucketNumbers,
                    __global const uint32_t * bucketEntryBits,

                    __global const uint16_t * expandedBuckets,
                    __global const uint32_t * expandedBucketOffsets,
                    uint32_t useExpandedBuckets,

                    __global const uint32_t * featureActive,
                    __global const uint32_t * featureIsOrdinal,

                    __local W * wLocal,
                    uint32_t maxLocalBuckets)
{
    int f = get_global_id(1) - 1;

    if (f != -1 && !featureActive[f])
        return;
    
    // We have to set up to access two different features:
    // 1) The feature we're splitting on for this example (splitFeature)
    // 2) The feature we're updating for the split (f)
    // Thus, we need to perform the same work twice, once for each of the
    // features.
    
    uint32_t bucketDataOffset;
    uint32_t bucketDataLength;
    uint32_t numBuckets;
    __global const uint32_t * bucketData;
    uint32_t bucketBits;
    uint32_t numLocalBuckets = 0;
    __global const uint16_t * featureExpandedBuckets;
    int startBucket;
            

    // Pointer to the global array we eventually want to update
    __global W * wGlobal;

    __local int numLocalUpdates;
    __local int numGlobalUpdates;
    __local int numGlobalUpdatesDirect;
    __local int numRows;
    
    if (get_local_id(0) == 0) {
        numLocalUpdates = numGlobalUpdates = numGlobalUpdatesDirect = numRows = 0;
    }
    
    if (f == -1) {
        numLocalBuckets = rightOffset * 2;
        wGlobal = wAll;
        numBuckets = numLocalBuckets;
        startBucket = 0;
    }
    else {
        bucketDataOffset = bucketDataOffsets[f];
        bucketDataLength = bucketDataOffsets[f + 1] - bucketDataOffset;
        numBuckets = bucketNumbers[f + 1] - bucketNumbers[f];
        bucketData = allBucketData + bucketDataOffset;
        bucketBits = bucketEntryBits[f];
        numLocalBuckets = min(numBuckets * rightOffset * 2, maxLocalBuckets);
        //printf("f %d nlb = %d nb = %d ro = %d mlb = %d\n",
        //       f, numLocalBuckets, numBuckets, rightOffset, maxLocalBuckets);
        wGlobal = partitionBuckets;
        startBucket = bucketNumbers[f];

        if (useExpandedBuckets) {
            featureExpandedBuckets
                = expandedBuckets + expandedBucketOffsets[f] / sizeof(expandedBuckets[0]);
        }
    }

    //numLocalBuckets = 0;
    
    // Clear our local accumulation buckets
    for (int b = get_local_id(0);  b < numLocalBuckets; b += get_local_size(0)) {
        zeroW(wLocal + b);
    }

    // Wait for all buckets to be clear
    barrier(CLK_LOCAL_MEM_FENCE);
    
    for (int i = get_global_id(0);  i < rowCount;  i += get_global_size(0)) {

        // We may have updated partition already, so here we mask out any
        // possible offset.
        int partition = partitions[i] & (rightOffset - 1);
        int splitFeature = partitionSplits[partition].feature;
                
        if (splitFeature == -1) {
            // reached a leaf here, nothing to split                    
            continue;
        }

        // Split feature values go here
        uint32_t splitBucketDataOffset = bucketDataOffsets[splitFeature];
        uint32_t splitBucketDataLength
            = bucketDataOffsets[splitFeature + 1]
            - splitBucketDataOffset;
        uint32_t splitNumBuckets = bucketNumbers[splitFeature + 1]
            - bucketNumbers[splitFeature];
        __global const uint32_t * splitBucketData
            = allBucketData + splitBucketDataOffset;
        uint32_t splitBucketBits = bucketEntryBits[splitFeature];

        float weight = fabs(decodedRows[i]);
        bool label = decodedRows[i] < 0;
        int exampleNum = i;
            
        int leftPartition = partition;
        int rightPartition = partition + rightOffset;

        if (leftPartition >= rightOffset
            || rightPartition >= rightOffset * 2) {
            printf("PARTITION SCALING ERROR\n");
        }
    
        int splitValue = partitionSplits[partition].value;
        bool ordinal = featureIsOrdinal[splitFeature];
        int bucket;

        if (!useExpandedBuckets) {
            bucket = getBucket(exampleNum,
                               splitBucketData, splitBucketDataLength,
                               splitBucketBits, splitNumBuckets);
        }
        else {
            bucket = expandedBuckets[expandedBucketOffsets[splitFeature] / sizeof(expandedBuckets[0])
                                     + exampleNum];
        }
        
        int side = ordinal ? bucket >= splitValue : bucket != splitValue;
        
        // Set the new partition number from the wAll job
        if (f == -1) {
            partitions[i] = partition + side * rightOffset;
        }

        // 0 = left to right, 1 = right to left
        int direction = partitionSplits[partition].direction;

        // We only need to update features on the wrong side, as we
        // transfer the weight rather than sum it from the
        // beginning.  This means less work for unbalanced splits
        // (which are typically most of them, especially for discrete
        // buckets)

        if (direction == side) {
            continue;
        }
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

        int fromBucket, toBucket;
        int fromBucketLocal, toBucketLocal;
        
        if (f == -1) {
            fromBucketLocal = fromBucket = fromPartition;
            toBucketLocal = toBucket = toPartition;
        }
        else {
            if (!useExpandedBuckets) {
                bucket = getBucket(exampleNum,
                                   bucketData, bucketDataLength,
                                   bucketBits, numBuckets);
            }
            else {
                bucket = featureExpandedBuckets[exampleNum];
            }

            
            fromBucket = fromPartition * numActiveBuckets + startBucket + bucket;
            toBucket = toPartition * numActiveBuckets + startBucket + bucket;
            fromBucketLocal = fromPartition * numBuckets + bucket;
            toBucketLocal = toPartition * numBuckets + bucket;
        }

        if (fromBucketLocal < numLocalBuckets) {
            decrementWLocal(wLocal + fromBucketLocal, label, weight);
            //atomic_inc(&numLocalUpdates);
        }
        else {
            decrementWAtomic(wGlobal + fromBucket, label, weight);
            //atomic_inc(&numGlobalUpdatesDirect);
        }

        if (toBucketLocal < numLocalBuckets) {
            incrementWLocal(wLocal + toBucketLocal, label, weight);
            //atomic_inc(&numLocalUpdates);
        }
        else {
            incrementWAtomic(wGlobal + toBucket, label, weight);
            //atomic_inc(&numGlobalUpdatesDirect);

            if (f != -1 && false)
                printf("row %d feature %d side %d direction %d from %d to %d lbl %d wt %f bkt %d -> %d\n",
                       i, f, side, direction, fromPartition, toPartition,
                       label, weight, fromBucket, toBucket);
        

        }

        //if ((int)wAll[fromPartition].count < 0) {
        //    printf("NEGATIVE WALL row %d feature %d side %d direction %d from %d to %d lbl %d wt %f\n",
        //           i, f, side, direction, fromPartition, toPartition,
        //           label, weight);
        //}

        //atomic_inc(&numRows);
    }

    // Wait for all buckets to be updated, before we write them back to the global mem
    barrier(CLK_LOCAL_MEM_FENCE);
    
    for (int b = get_local_id(0);  b < numLocalBuckets;  b += get_local_size(0)) {
        if (wLocal[b].count != 0) {
            if (f == -1) {
                incrementWOut(wGlobal + b, wLocal + b);
                //atomic_inc(&numGlobalUpdates);
                continue;
            }

            // TODO: avoid div/mod if possible in these calculations
            int partition = b / numBuckets;
            int bucket = b % numBuckets;
            int bucketNumberGlobal = partition * numActiveBuckets + startBucket + bucket;

            //printf("f %d local bucket %d part %d buck %d nb %d nab %d global bucket %d\n",
            //       f, b, partition, bucket, numBuckets, numActiveBuckets, bucketNumberGlobal);

            incrementWOut(wGlobal + bucketNumberGlobal, wLocal + b);
            //atomic_inc(&numGlobalUpdates);
        }
    }

    barrier(CLK_LOCAL_MEM_FENCE);

    if (get_local_id(0) == 0 && false) {
        printf("f %d nb %d nbp %d nlb %d id %d rows %d of %d loc %d dir %d global %d\n",
               (int)f, (int)numBuckets, (int)(numBuckets*rightOffset*2), (int)numLocalBuckets, (int)get_global_id(0), (int)numRows, (int)(rowCount / get_local_size(0)),
               numLocalUpdates, numGlobalUpdatesDirect, numGlobalUpdates);
    }
}

