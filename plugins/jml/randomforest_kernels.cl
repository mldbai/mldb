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

static const __constant float VAL_2_HL = 1.0f * (1ULL << 63);
static const __constant float HL_2_VAL = 1.0f / (1ULL << 63);
static const __constant float VAL_2_H = (1ULL << 31);
static const __constant float H_2_VAL = 1.0f / (1ULL << 31);
static const __constant float ADD_TO_ROUND = 0.5f / (1ULL << 63);

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

uint32_t extractBitRange64(__global const uint64_t * data,
                           int numBits,
                           int entryNumber)
{
    int bitNumber = numBits * entryNumber;
    int wordNumber = bitNumber / 64;
    int wordOffset = bitNumber % 64;

    //printf("wordNumber = %d, bitNumber = %d\n", wordNumber, wordOffset);
    
    int bottomBits = min(numBits, 64 - wordOffset);
    int topBits = numBits - bottomBits;

    //printf("numBits = %d, bottomBits = %d, topBits = %d\n",
    //       numBits, bottomBits, topBits);
    
    uint64_t mask = createMask64(numBits);

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
                           int entryNumber)
{
    int bitNumber = numBits * entryNumber;
    int wordNumber = bitNumber / 32;
    int wordOffset = bitNumber % 32;

    //printf("wordNumber = %d, bitNumber = %d\n", wordNumber, wordOffset);
    
    int bottomBits = min(numBits, 32 - wordOffset);
    int topBits = numBits - bottomBits;

    //printf("numBits = %d, bottomBits = %d, topBits = %d\n",
    //       numBits, bottomBits, topBits);
    
    uint64_t mask = createMask64(numBits);

    //printf("mask = %08x\n", mask);
    
    uint64_t val = data[wordNumber];

    //printf("val = %08x\n", val);

    val >>= wordOffset;

    if (topBits > 0) {
        uint64_t val2 = data[wordNumber + 1];
        val = val | val2 << bottomBits;
    }
    val = val & mask;
    //printf("val out = %08lx\n", val);
    return val;
}

void getDecodedRow(uint32_t rowNumber,

                   __global const uint64_t * rowData,
                   uint32_t totalBits,
                   uint32_t weightBits,
                   uint32_t exampleBits,
                   uint32_t numRows,

                   int weightEncoding,
                   float weightMultiplier,
                   __global const float * weightTable,
                   
                   uint32_t * example,
                   float * weight,
                   bool * label)
{
    uint64_t bits = extractBitRange64(rowData, totalBits, rowNumber);
    //printf("rowNum %d bits = %016lx weightBits = %d exampleBits = %d\n",
    //       rowNumber, bits, weightBits, exampleBits);
    //printf("exampleMask = %016lx example = %016lx\n",
    //       createMask64(exampleBits),
    //       bits & createMask64(exampleBits));
    *example = exampleBits == 0 ? rowNumber : bits & createMask64(exampleBits);
    *weight = decodeWeight((bits >> exampleBits) & createMask64(weightBits),
                           weightEncoding, weightMultiplier, weightTable);
    *label = (bits & (1 << (weightBits + exampleBits))) != 0;
}

uint32_t getBucket(uint32_t exampleNum,
                   __global const uint32_t * bucketData,
                   uint32_t bucketBits,
                   uint32_t numBuckets)
{
    return extractBitRange32(bucketData, bucketBits, exampleNum);
}

typedef struct W {
    int64_t vals[2];
} W;

void zeroW(__local W * w)
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

void incrementW(__local W * w, bool label, float weight)
{
    int64_t inc = encodeW(weight);
    atom_add(&w->vals[label ? 1 : 0], inc);
}

void incrementWOut(__global W * wOut, __local const W * wIn)
{
    atom_add(&wOut->vals[0], wIn->vals[0]);
    atom_add(&wOut->vals[1], wIn->vals[1]);
}

void testRow(uint32_t rowId,

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
    uint32_t exampleNum;
    float weight;
    bool label;

    uint32_t bucket;

    getDecodedRow(rowId, rowData, totalBits, weightBits, exampleBits, numRows,
                  weightEncoding, weightMultiplier, weightTable,
                  &exampleNum, &weight, &label);
    
    bucket = getBucket(exampleNum, bucketData, bucketBits, numBuckets);

    //if (rowId < 10)
    //    printf("rowId %d exampleNum %d bucket %d of %d weight %g label %d\n",
    //           rowId, exampleNum, bucket, numBuckets, weight, label);

    incrementW(w + bucket, label, weight);
}

__kernel void testFeatureKernel(uint32_t numRowsPerWorkgroup,

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

                                __local W * w,
                                __global W * wOut)
{
    const uint32_t workGroupId = get_global_id (0);
    const uint32_t workerId = get_local_id(0);

    //printf("workGroupId = %d workerId = %d\n",
    //       workGroupId, workerId);
    
    //if (workGroupId == 0) {
    //    printf("Num work groups = %d size = %d\n",
    //           get_num_groups(0), get_local_size(0));
    //}

    //printf("workerId = %d, localsize[0] = %d\n",
    //       workerId, get_local_size(0));
    
    for (int i = workerId;  i < numBuckets;  i += get_local_size(0)) {
        //printf("zeroing %d\n", i);
        zeroW(w + workerId);
    }
    
    barrier(CLK_LOCAL_MEM_FENCE);
    //if (rowId % 10000 == 0)
    //    printf("afer barrier %d\n", rowId);

    //if (rowId > 4)
    //    return;

    for (int i = 0;  i < numRowsPerWorkgroup;  ++i) {
        int rowId = workGroupId * numRowsPerWorkgroup + i;
        //printf("rowId = %d, numRows = %d\n", rowId, numRows);
        if (rowId < numRows) {
            testRow(rowId, rowData, totalBits, weightBits, exampleBits, numRows,
                    bucketData, bucketBits, numBuckets,
                    weightEncoding, weightMultiplier, weightTable, w);
        }
    }
        
    barrier(CLK_LOCAL_MEM_FENCE);
    
    for (int i = workerId;  i < numBuckets;  i += get_local_size(0)) {
        //printf("copying %d\n", i);
        incrementWOut(wOut + i, w + i);
    }

    barrier(CLK_GLOBAL_MEM_FENCE);

    if (workGroupId == 0 && false) {
        for (int i = 256;  i < 266 && i < numBuckets;  ++i) {
            printf("local bucket %d W = (%g, %g)\n",
                   i, decodeW(w[i].vals[0]), decodeW(w[i].vals[1]));
            printf("global bucket %d W = (%g, %g)\n",
                   i, decodeW(wOut[i].vals[0]), decodeW(wOut[i].vals[1]));
        }
    }
}
