/** randomforest_kernels.h                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    OpenCL kernels for random forest algorithm.

    clang-6.0 -Xclang -finclude-default-header -cl-std=CL1.2 -Dcl_clang_storage_class_specifiers -target nvptx64-nvidia-nvcl -xcl plugins/jml/randomforest_kernels.cl -emit-llvm -cl-kernel-arg-info -S -o test.ll
*/

#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable

typedef unsigned uint32_t;
typedef unsigned long uint64_t;
typedef long int64_t;

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

uint32_t extractBitRange32(__global const uint32_t * data,
                           int numBits,
                           int entryNumber,
                           uint32_t dataLength)
{
    int bitNumber = numBits * entryNumber;
    int wordNumber = bitNumber / 32;
    int wordOffset = bitNumber % 32;

    if (wordNumber >= dataLength) {
        printf("OUT OF RANGE WORD 32 %d vs %d\n", wordNumber, dataLength);
        return 0;
    }
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

uint32_t getBucket(uint32_t exampleNum,
                   __global const uint32_t * bucketData,
                   uint32_t bucketDataLength,
                   uint32_t bucketBits,
                   uint32_t numBuckets)
{
    return extractBitRange32(bucketData, bucketBits, exampleNum, bucketDataLength);
}

typedef struct W {
    int64_t vals[2];
} W;

void zeroW(__local W * w)
{
    w->vals[0] = 0;
    w->vals[1] = 0;
}

void zeroWPrivate(__private W * w)
{
    w->vals[0] = 0;
    w->vals[1] = 0;
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

double decodeW(int64_t v)
{
    return v * HL_2_VAL;
}

void incrementWLocal(__local W * w, bool label, float weight)
{
    //if (weight != 0.0)
    //    return;
    
    int64_t inc = encodeW(weight);
#if 1
    atom_add(&w->vals[label ? 1 : 0], inc);
#else
    w->vals[label ? 1 : 0] += inc;
#endif
}

void incrementWGlobal(__global W * w, bool label, float weight)
{
    //if (weight != 0.0)
    //    return;
    
    int64_t inc = encodeW(weight);
#if 1
    atom_add(&w->vals[label ? 1 : 0], inc);
#else
    w->vals[label ? 1 : 0] += inc;
#endif
}

void incrementWOut(__global W * wOut, __local const W * wIn)
{
    atom_add(&wOut->vals[0], wIn->vals[0]);
    atom_add(&wOut->vals[1], wIn->vals[1]);
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

    if (false) {
        bucket = exampleNum % numBuckets;
        incrementW(w + bucket, label, weight);
        return bucket;
    }
    
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
    
#if 0    
    W myW[512];

    for (int i = 0;  i < numBuckets;  ++i) {
        zeroWPrivate(myW + i);
        //myW[i].vals[0] = 0;
        //myW[i].vals[1] = 0;
    }
#endif
    
    __local int minWorkgroupBucket, maxWorkgroupBucket;

    if (workGroupId == 0) {
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

    for (i = 0;  i < numRowsPerWorkgroup;  ++i) {
        //int rowId = workGroupId * numRowsPerWorkgroup + i;
        int rowId = workGroupId + i * get_local_size(0);
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


    for (int i = workerId;  i < numBuckets;  i += get_local_size(0)) {
        //printf("copying %d\n", i);
        //wOut[i].vals[0] = w[i].vals[0];
        //wOut[i].vals[1] = w[i].vals[1];
        incrementWOut(wOut + i, w + i);
    }

    if (workerId == 0) {
        //printf("index %d minBucket %d maxBucket %d\n",
        //       workGroupId, minWorkgroupBucket, maxWorkgroupBucket);
        atomic_min(minMaxOut + 0, minWorkgroupBucket);
        atomic_max(minMaxOut + 1, maxWorkgroupBucket);
    }
    
    barrier(CLK_GLOBAL_MEM_FENCE);

    if (workGroupId == 0) {
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

    
    if (workGroupId == 0 && false) {
        for (int i = 256;  i < 266 && i < numBuckets;  ++i) {
            printf("local bucket %d W = (%g, %g)\n",
                   i, decodeW(w[i].vals[0]), decodeW(w[i].vals[1]));
            printf("global bucket %d W = (%g, %g)\n",
                   i, decodeW(wOut[i].vals[0]), decodeW(wOut[i].vals[1]));
        }
    }
}

#if 0

double scoreSplit(W wFalse, W wTrue)
{
    double score
        = 2.0 * (  sqrt(wFalse[0] * wFalse[1])
                   + sqrt(wTrue[0] * wTrue[1]));
    return score;
}

typedef struct SplitOut {
    double bestScore;
    int bestSplit;
    W bestLeft;
    W bestRight;
};

__kernel void
scoreSplitKernel(int maxuint32_t numRowsPerWorkgroup,
                  
                  __global const uint64_t * rowData,
                  uint32_t totalBits,
                  uint32_t weightBits,
                  uint32_t exampleBits,
                  uint32_t numRows,
                  
                  __global const uint32_t * bucketData,
                  uint32_t bucketBits,
                  uint32_t numBuckets,
                  
                  int weightEncoding,
                  float weightMultiplier,
                  __global const float * weightTable,
                  
                  __global const W * w,
                  bool ordinal,
                  __global const W * wAll,
                  __global SplitOut * splitOut)
{
    // First, we calculate the W values
    


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


__kernel void testAndScoreFeature(uint32_t numRowsPerWorkgroup,

                                  __global const uint64_t * rowData,
                                  uint32_t totalBits,
                                  uint32_t weightBits,
                                  uint32_t exampleBits,
                                  uint32_t numRows,

                                  __global const uint32_t * bucketData,
                                  uint32_t bucketBits,
                                  uint32_t numBuckets,
                                  
                                  int weightEncoding,
                                  float weightMultiplier,
                                  __global const float * weightTable,

                                  __local W * w)
{
    int maxBucket = -1;

    double bestScore = INFINITY;
    int bestSplit = -1;
    W bestLeft;
    W bestRight;

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

#endif
