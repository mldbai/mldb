/** randomforest_kernels.h                                     -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    OpenCL kernels for random forest algorithm.

    clang-6.0 -Xclang -finclude-default-header -cl-std=CL1.2 -Dcl_clang_storage_class_specifiers -target nvptx64-nvidia-nvcl -xcl plugins/jml/randomforest_kernels.cl -emit-llvm -cl-kernel-arg-info -S -o test.ll
*/
#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable

#ifndef WBITS
#  define WBITS 64
#endif

#if WBITS == 64
#  define W W64
#endif

#if WBITS == 32
#  define W W32
#endif

typedef unsigned uint32_t;
typedef signed short int16_t;
typedef int int32_t;
typedef unsigned long uint64_t;
typedef long int64_t;
typedef signed char int8_t;
typedef unsigned char uint8_t;
typedef unsigned short uint16_t;

#if WBITS == 64
static const __constant float VAL_2_HL = 1.0f * (1UL << 60);
static const __constant float HL_2_VAL = 1.0f / (1UL << 60);
static const __constant float VAL_2_H = (1UL << 28);
static const __constant float H_2_VAL = 1.0f / (1UL << 28);
static const __constant float ADD_TO_ROUND = 0.5f / (1UL << 60);
#elif WBITS == 32
static const __constant float VAL_2_HL = 1.0f * (1UL << 30);
static const __constant float HL_2_VAL = 1.0f / (1UL << 30);
static const __constant float ADD_TO_ROUND = 0.5f / (1UL << 30);
#endif

// Used to measure the startup latency
__kernel void doNothingKernel(uint32_t dummy)
{
    //printf("did nothing ");
    //printf("%d\n", dummy);
}

typedef struct WeightFormat {
    uint16_t f;
} WeightFormat;

inline float decodeWeight(uint32_t bits, WeightFormat floatEncoding, float baseMultiplier,
                   __global const float * table)
{
    if (floatEncoding.f == 0 || true) {
        return bits * baseMultiplier;
    }
    else if (floatEncoding.f == 1) {
        return as_float(bits);
    }
    else if (floatEncoding.f == 2) {
        return table[bits];
    }
    else return INFINITY;
}

inline uint32_t createMask32(uint32_t numBits)
{
    return numBits >= 32 ? -1 : (((uint32_t)1 << numBits) - 1);
}

inline uint64_t createMask64(uint32_t numBits)
{
    return numBits >= 64 ? -1 : (((uint64_t)1 << numBits) - 1);
}

inline uint64_t extractBitRange64(__global const uint64_t * data,
                                  uint32_t numBits,
                                  uint32_t entryNumber,
                                  uint64_t mask,
                                  uint32_t dataLength)
{
    long bitNumber = numBits * (long)entryNumber;
    //if (bitNumber >= INT_MAX) {
    //    printf("bit number requires > 32 bits: %ld\n", bitNumber); 
    //}
    uint32_t wordNumber = bitNumber / 64;
    uint32_t wordOffset = bitNumber % 64;

    if (wordNumber >= dataLength) {
        //printf("OUT OF RANGE WORD %d vs %d\n", wordNumber, dataLength);
        return 0;
    }
    
    //return data[wordNumber];
    
    //printf("reading word number %d in worker %ld\n",
    //       wordNumber, get_global_id(0));
    
    //printf("wordNumber = %d, bitNumber = %d\n", wordNumber, wordOffset);
    
    uint32_t bottomBits = min(numBits, 64 - wordOffset);
    uint32_t topBits = numBits - bottomBits;

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
                                  uint32_t numBits,
                                  uint32_t entryNumber)
{
    uint32_t bitNumber = numBits * entryNumber;
    uint32_t wordNumber = bitNumber / 32;
    uint32_t wordOffset = bitNumber % 32;

    //if (wordNumber >= dataLength) {
    //    printf("OUT OF RANGE WORD 32 %d vs %d\n", wordNumber, dataLength);
    //    return 0;
    //}
    //printf("wordNumber = %d, bitNumber = %d\n", wordNumber, wordOffset);
    
    uint32_t bottomBits = min(numBits, 32 - wordOffset);
    uint32_t topBits = numBits - bottomBits;

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

typedef struct DecodedRow {
    uint32_t example;
    float weight;
    bool label;
} DecodedRow;

DecodedRow getDecodedRow(uint32_t rowNumber,

                   __global const uint64_t * rowData,
                   uint32_t rowDataLength,
                   uint32_t totalBits,
                   uint32_t weightBits,
                   uint32_t exampleBits,
                   uint32_t numRows,

                   WeightFormat weightEncoding,
                   float weightMultiplier,
                   __global const float * weightTable,
                   
                   uint64_t mask,
                   uint32_t exampleMask,
                   uint32_t weightMask,
                   uint32_t labelMask);

DecodedRow getDecodedRow(uint32_t rowNumber,

                   __global const uint64_t * rowData,
                   uint32_t rowDataLength,
                   uint32_t totalBits,
                   uint32_t weightBits,
                   uint32_t exampleBits,
                   uint32_t numRows,

                   WeightFormat weightEncoding,
                   float weightMultiplier,
                   __global const float * weightTable,
                   
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
    DecodedRow result;
    result.example = exampleBits == 0 ? rowNumber : bits & exampleMask;
    result.weight = decodeWeight((bits >> exampleBits) & weightMask,
                                 weightEncoding, weightMultiplier, weightTable);
    result.label = (bits & labelMask) != 0;
    return result;
}

inline uint32_t getBucket(uint32_t exampleNum,
                          __global const uint32_t * bucketData,
                          uint32_t bucketDataLength,
                          uint32_t bucketBits,
                          uint32_t numBuckets)
{
    return extractBitRange32(bucketData, bucketBits, exampleNum);
}

typedef struct W64 {
    int64_t vals[2];
    int32_t count;
} W64;

typedef struct W32 {
    int32_t vals[2];
    int32_t count;
} W32;

// Struct with an extra index field used for sorting (local only)
typedef struct WIndexed {
    W w;
    int32_t index;
} WIndexed;

#define W_INIT { { 0, 0}, 0 }
#define W_INDEXED_INIT { W_INIT, 0 }

inline void zeroW(__local W * w)
{
    w->vals[0] = 0;
    w->vals[1] = 0;
    w->count = 0;
}

#if WBITS == 64
inline int64_t encodeW(float f)
{
    int64_t result = convert_long((f + ADD_TO_ROUND) * VAL_2_HL);
    //printf("encoding %g (%08x) to %g (%08x) to %ld, ADD_TO_ROUND = %g, VAL_2_HL = %g\n",
    //       f, as_int(f),
    //       (f + ADD_TO_ROUND) * VAL_2_HL, as_int((f + ADD_TO_ROUND) * VAL_2_HL),
    //       result, ADD_TO_ROUND, VAL_2_HL);
    return result;
}

inline float decodeW(int64_t v)
{
    return v * HL_2_VAL;
}

inline float decodeWf(int64_t v)
{
    return v * HL_2_VAL;
}
#elif WBITS == 32
inline int32_t encodeW(float f)
{
    int32_t result = convert_int((f + ADD_TO_ROUND) * VAL_2_HL);
    //printf("encoding %g (%08x) to %g (%08x) to %ld, ADD_TO_ROUND = %g, VAL_2_HL = %g\n",
    //       f, as_int(f),
    //       (f + ADD_TO_ROUND) * VAL_2_HL, as_int((f + ADD_TO_ROUND) * VAL_2_HL),
    //       result, ADD_TO_ROUND, VAL_2_HL);
    return result;
}

inline float decodeW(int32_t v)
{
    return v * HL_2_VAL;
}

inline float decodeWf(int32_t v)
{
    return v * HL_2_VAL;
}
#endif

#ifdef cl_khr_int64_base_atomics

#define atom_add_64_local atom_add
#define atom_sub_64_local atom_sub
#define atom_add_64_global atom_add
#define atom_sub_64_global atom_sub

#else

#ifdef __ENDIAN_LITTLE__
#define LOW_INDEX 0
#define HIGH_INDEX 1
#else
#define LOW_INDEX 1
#define HIGH_INDEX 0
#endif

#if WBITS==64
// Synthesize 64 bit atomic additions/subtractions by two 32 bit atomic
// additions/subtractions.  This works because we only use the targets to
// accumulate; we don't read them until all accumulations are done.
inline void atom_add_64_local(__local int64_t * val, int64_t arg)
{
    __local uint32_t * lo = ((__local uint32_t *)val) + LOW_INDEX;
    __local int32_t * hi = ((__local int32_t *)val) + HIGH_INDEX;
    uint32_t argLo = arg;
    int32_t argHi = arg >> 32;

    if (argLo != 0) {
        uint32_t oldLo = atomic_add(lo, argLo);
        // Overflow if oldLo + argLo > INT_MAX or oldLo + argLo < INT_MIN
        // So if oldLo > INT_MAX - argLo or oldLo < INT_MIN - argLo
        //if (oldLo > INT_MAX - argLo || oldLo < INT_MIN - argLo) {
        if (__builtin_add_overflow(oldLo, argLo, &oldLo)) {
            argHi += 1;
        }
    }
    if (argHi != 0) {
        atomic_add(hi, argHi);
    }
}

inline void atom_sub_64_local(__local int64_t * val, int64_t arg)
{
    atom_add_64_local(val, -arg);  // TODO: doesn't work properly for all values of arg..
}

inline void atom_add_64_global(__global int64_t * val, int64_t arg)
{
    __global uint32_t * lo = ((__global uint32_t *)val) + LOW_INDEX;
    __global int32_t * hi = ((__global int32_t *)val) + HIGH_INDEX;
    uint32_t argLo = arg;
    int32_t argHi = arg >> 32;

    if (argLo != 0) {
        uint32_t oldLo = atomic_add(lo, argLo);
        // Overflow if oldLo + argLo > INT_MAX or oldLo + argLo < INT_MIN
        // So if oldLo > INT_MAX - argLo or oldLo < INT_MIN - argLo
        //if (oldLo > INT_MAX - argLo || oldLo < INT_MIN - argLo) {
        if (__builtin_add_overflow(oldLo, argLo, &oldLo)) {
            argHi += 1;
        }
    }
    if (argHi != 0) {
        atomic_add(hi, argHi);
    }
}

inline void atom_sub_64_global(__global int64_t * val, int64_t arg)
{
    atom_add_64_global(val, -arg);
}
#endif // WBITS==64

#endif // 64 bit base atomics

#if WBITS == 64

inline void incrementWLocal(__local W * w, bool label, float weight)
{
    int64_t inc = encodeW(weight);
    atom_add_64_local(&w->vals[label ? 1 : 0], inc);
    atom_inc(&w->count);
}

inline void decrementWLocal(__local W * w, bool label, float weight)
{
    int64_t inc = encodeW(weight);
    atom_sub_64_local(&w->vals[label ? 1 : 0], inc);
    atom_dec(&w->count);
}

inline void incrementWGlobal(__global W * w, bool label, float weight)
{
    int64_t inc = encodeW(weight);
    atom_add_64_global(&w->vals[label ? 1 : 0], inc);
    atom_inc(&w->count);
}

inline void incrementWOut(__global W * wOut, __local const W * wIn)
{
    if (wIn->count == 0)
        return;
    if (wIn->vals[0] != 0)
        atom_add_64_global(&wOut->vals[0], wIn->vals[0]);
    if (wIn->vals[1] != 0)
        atom_add_64_global(&wOut->vals[1], wIn->vals[1]);
    atom_add(&wOut->count,   wIn->count);
}

inline void decrementWOutGlobal(__global W * wOut, __global const W * wIn)
{
    if (wIn->count == 0)
        return;
    if (wIn->vals[0] != 0)
        atom_sub_64_global(&wOut->vals[0], wIn->vals[0]);
    if (wIn->vals[1] != 0)
        atom_sub_64_global(&wOut->vals[1], wIn->vals[1]);
    atom_sub(&wOut->count,   wIn->count);
}
#elif WBITS == 32

inline void incrementWLocal(__local W * w, bool label, float weight)
{
    int32_t inc = encodeW(weight);
    atom_add(&w->vals[label ? 1 : 0], inc);
    atom_inc(&w->count);
}

inline void decrementWLocal(__local W * w, bool label, float weight)
{
    int32_t inc = encodeW(weight);
    atom_sub(&w->vals[label ? 1 : 0], inc);
    atom_dec(&w->count);
}

inline void incrementWGlobal(__global W * w, bool label, float weight)
{
    int32_t inc = encodeW(weight);
    atom_add(&w->vals[label ? 1 : 0], inc);
    atom_inc(&w->count);
}

inline void incrementWOut(__global W * wOut, __local const W * wIn)
{
    if (wIn->count == 0)
        return;
    if (wIn->vals[0] != 0)
        atom_add(&wOut->vals[0], wIn->vals[0]);
    if (wIn->vals[1] != 0)
        atom_add(&wOut->vals[1], wIn->vals[1]);
    atom_add(&wOut->count,   wIn->count);
}

inline void decrementWOutGlobal(__global W * wOut, __global const W * wIn)
{
    if (wIn->count == 0)
        return;
    if (wIn->vals[0] != 0)
        atom_sub(&wOut->vals[0], wIn->vals[0]);
    if (wIn->vals[1] != 0)
        atom_sub(&wOut->vals[1], wIn->vals[1]);
    atom_sub(&wOut->count,   wIn->count);
}

#endif

// Take a bit-compressed representation of rows, and turn it into a
// decompressed version with one float per row (the sign gives the
// label, and the magnitude gives the weight).
//
// This is a 1D kernel over the array of rows, not very complicated

__kernel void
decompressRowsKernel(__global const uint64_t * rowData,
                     uint32_t rowDataLength,
                     uint16_t weightBits,
                     uint16_t exampleNumBits,
                     uint32_t numRows,
                     
                     WeightFormat weightFormat,
                     float weightMultiplier,
                     __global const float * weightData,
                     
                     __global float * decodedRowsOut)
{
    uint32_t totalBits = weightBits + exampleNumBits + 1;  // 1 is for the label
    //if (get_global_id(0) == 0) {
    //    printf("weightBits = %d exampleNumBits = %d totalBits = %d\n",
    //           weightBits, exampleNumBits, totalBits);
    //}

    uint64_t mask = createMask64(totalBits);
    uint32_t exampleMask = createMask32(exampleNumBits);
    uint32_t weightMask = createMask32(weightBits);
    uint32_t labelMask = (1 << (weightBits + exampleNumBits));

    for (uint32_t rowId = get_global_id(0);  rowId < numRows;
         rowId += get_global_size(0)) {

        DecodedRow row = getDecodedRow(rowId, rowData, rowDataLength,
                      totalBits, weightBits, exampleNumBits, numRows,
                      weightFormat, weightMultiplier, weightData,
                      mask, exampleMask, weightMask, labelMask);
        
        if (row.example != rowId) {
            printf("non-consecutive example numbers: %d vs %d, exampleNumBits=%d\n", row.example, rowId, exampleNumBits);
        }

        float encoded = row.label ? -row.weight : row.weight;
        
        if (rowId < 128 && false) {
            printf("row %d ex %d wt %f lb %d encoded %f\n",
                   rowId, rowId, row.weight, row.label, encoded);
        }
        
        decodedRowsOut[rowId] = encoded;
    }
}

#if 0
// Decompress the data for the features, to save on access time
__kernel void
decompressFeatureBucketsKernel(uint32_t numRows,
                               __global const uint32_t * bucketData,
                               __global const uint32_t * bucketDataOffsets,
                               __global const uint32_t * bucketNumbers,
                               __global const uint32_t * bucketEntryBits,
                               
                               __global const uint32_t * featuresActive,
                               
                               __global uint16_t * featuresOut,
                               __global const uint32_t * featureDataOffsets)
{
    uint32_t f = get_global_id(1);

    if (!featuresActive[f])
        return;

    uint32_t bucketDataOffset = bucketDataOffsets[f];
    uint32_t bucketDataLength = bucketDataOffsets[f + 1] - bucketDataOffset;
    uint32_t numBuckets = bucketNumbers[f + 1] - bucketNumbers[f];
    __global const uint32_t * bucketData = bucketData + bucketDataOffset;
    uint32_t bucketBits = bucketEntryBits[f];
    
    __global uint16_t * out = featuresOut + featureDataOffsets[f] / sizeof(*out);

    for (uint32_t rowId = get_global_id(0);  rowId < numRows; rowId += get_global_size(0)) {
        uint32_t bucket = getBucket(rowId, bucketData, bucketDataLength, bucketBits, numBuckets);
        out[rowId] = bucket;
    }
}
#endif

uint32_t testRow(uint32_t rowId,
                 __global const float * rowData,
                 uint32_t numRows,
                 
                 __global const uint32_t * bucketData,
                 uint32_t bucketDataLength,
                 uint32_t bucketBits,
                 uint32_t numBuckets,

                 __local W * w,
                 __global W * wOut,
                 uint32_t maxLocalBuckets);

inline void printW(__global W * w)
{
    //printf("{\"v\":[%f,%f],\"c\":%d}", decodeWf(w->vals[0]), decodeWf(w->vals[1]), w->count);
}

uint32_t testRow(uint32_t rowId,
                 __global const float * rowData,
                 uint32_t numRows,
                 
                 __global const uint32_t * bucketData,
                 uint32_t bucketDataLength,
                 uint32_t bucketBits,
                 uint32_t numBuckets,

                 __local W * w,
                 __global W * wOut,
                 uint32_t maxLocalBuckets)
{
    uint32_t exampleNum = rowId;
    float val = rowData[rowId];
    float weight = fabs(val);
    bool label = val < 0;
    //int f = get_global_id(0);
    uint32_t bucket
        = getBucket(exampleNum, bucketData, bucketDataLength,
                    bucketBits, numBuckets);

    if (bucket >= numBuckets) {
        printf("ERROR BUCKET NUMBER: got %d numBuckets %d row %d feature %d\n",
               bucket, numBuckets, rowId, (int)get_global_id(0));
        return 0;
    }

    //if (f == 0 && bucket == 1) {
    //    printf("feat %d row %d bucket %d of %d weight %f label %d\n",
    //            f, rowId, bucket, numBuckets, weight, label);
    //}

    if (bucket < maxLocalBuckets) {
        incrementWLocal(w + bucket, label, weight);
    }
    else {
        incrementWGlobal(wOut + bucket, label, weight);
    }

    return bucket;
}

__kernel void
testFeatureKernel(__global const float * decodedRows,
                 uint32_t numRows,

                 __global const uint32_t * bucketData,
                 __global const uint32_t * bucketDataOffsets,
                 __global const uint32_t * bucketNumbers,
                 __global const uint32_t * bucketEntryBits,

                 __global const uint32_t * featuresActive,
                      
                 __local W * w,
                 uint32_t maxLocalBuckets,
                 __global W * partitionBuckets)
{
    const uint32_t workGroupId = get_global_id (1);
    const uint32_t workerId = get_local_id(1);
    const uint32_t f = get_global_id(0);
    
    uint32_t bucketDataOffset = bucketDataOffsets[f];
    uint32_t bucketDataLength = bucketDataOffsets[f + 1] - bucketDataOffset;
    uint32_t numBuckets = bucketNumbers[f + 1] - bucketNumbers[f];
    bucketData += bucketDataOffset;
    uint32_t bucketBits = bucketEntryBits[f];

    __global W * wOut = partitionBuckets + bucketNumbers[f];

    if (!featuresActive[f])
        return;
    
    if (workGroupId == 0 && false) {
        printf("feat %d global size %ld, num groups %ld, local size %ld, numRows %d, numBuckets %d, buckets %d-%d, offset %d\n",
               (int)f,
               get_global_size(1),
               get_num_groups(1),
               get_local_size(1),
               numRows,
               numBuckets,
               bucketNumbers[f],
               bucketNumbers[f + 1],
               bucketDataOffset);
    }

    //maxLocalBuckets = 0;  // TODO DEBUG ONLY

    for (uint32_t i = workerId;  i < numBuckets && i < maxLocalBuckets;
         i += get_local_size(1)) {
        zeroW(w + i);
    }

    barrier(CLK_LOCAL_MEM_FENCE);
    
    for (uint32_t rowId = get_global_id(1);  rowId < numRows;  rowId += get_global_size(1)) {
        //if (f == 0 && workGroupId == 0) {
        //    printf("testing row %d\n", rowId);
        //}
        testRow(rowId,
                decodedRows, numRows,
                bucketData, bucketDataLength, bucketBits, numBuckets,
                w, wOut, maxLocalBuckets);
    }
    
    barrier(CLK_LOCAL_MEM_FENCE);
    
    for (uint32_t i = workerId;  i < numBuckets && i < maxLocalBuckets;
         i += get_local_size(1)) {
        incrementWOut(wOut + i, w + i);
    }
}

typedef struct {
    float score;
    int16_t feature;
    int16_t value;
    W left;
    W right;
} PartitionSplit;

#define PARTITION_SPLIT_INIT { INFINITY, -1, -1, W_INIT, W_INIT }

typedef struct {
    float score;
    int16_t feature;
    int16_t value;
    W left;
    W right;
    uint32_t index;
} IndexedPartitionSplit;


inline bool partitionSplitDirectionGlobal(__global const PartitionSplit * split)
{
    return split->feature != -1 && split->left.count < split->right.count;
}

__kernel void
fillPartitionSplitsKernel(__global PartitionSplit * splits)
{
    uint32_t n = get_global_id(0);
    //printf("filling partition %d at %ld\n",
    //       n, (long)(((__global char *)(splits + n)) - (__global char *)splits));
    PartitionSplit spl = PARTITION_SPLIT_INIT;
    splits[n] = spl;
}

inline float nextFloat(float val, int32_t n)
{
    int32_t ival = *(int32_t *)(&val);
    ival += n;
    return *(float *)(&ival);
}

inline int32_t floatAsInt(float val)
{
    return *(int32_t *)(&val);
}

inline float sqrt2(float val, int * adj)
{
    int low = -3;
    int high = +3;
    float approx = sqrt(val);
    float bestErr = INFINITY;
    float bestApprox = approx;

    for (int i = low;  i <= high;  ++i) {
        float guess = nextFloat(approx, i);
        float err = fabs(val - guess * guess);
        if (err < bestErr) {
            bestErr = err;
            *adj = i;
            bestApprox = guess;
        }
    }
    
    return bestApprox;
}

// Stable SQRT: take the largest value which when squared is less than or equal to the desired value
inline float sqrt3(float val)
{
    float approx = nextFloat(sqrt(val), 1);
    while (approx * approx > val) {
        approx = nextFloat(approx, -1);
    }
    return approx;
}

float scoreSplit(W wFalse, W wTrue);

#if 1
float scoreSplit(W wFalse, W wTrue)
{
    float arg1 = decodeWf(wFalse.vals[0]) * decodeWf(wFalse.vals[1]);
    float arg2 = decodeWf(wTrue.vals[0])  * decodeWf(wTrue.vals[1]);
    float sr1 = sqrt3(arg1);
    float sr2 = sqrt3(arg2);
    float score = 2.0f * (sr1 + sr2);
    return score;
};
#endif

void incrementW(__local W * out, __local const W * in);

void incrementW(__local W * out, __local const W * in)
{
    if (in->count == 0)
        return;
    out->vals[0] += in->vals[0];
    out->vals[1] += in->vals[1];
    out->count += in->count;
}

float scoreSplitWAll(W wTrue, W wAll);

float scoreSplitWAll(W wTrue, W wAll)
{
    W wFalse = { { wAll.vals[0] - wTrue.vals[0],
                   wAll.vals[1] - wTrue.vals[1] },
                 wAll.count - wTrue.count };
    if (wFalse.count == 0 || wTrue.count == 0)
        return INFINITY;
    return scoreSplit(wFalse, wTrue);
}

#if 1
void minW(__local WIndexed * out, __local const WIndexed * in, W wAll);

/* In this function, we identify empty buckets (that should not be scored)
   by marking their index with -1.
*/
void minW(__local WIndexed * out, __local const WIndexed * in, W wAll)
{
    if (in->w.count == 0 || in->index < 0)
        return;
    if (out->w.count == 0 || out->index < 0) {
        *out = *in;
        return;
    }

    float scoreIn = scoreSplitWAll(in->w, wAll);
    float scoreOut = scoreSplitWAll(out->w, wAll);

#if 0
    if (debug) {
        printf("score %d vs %d: %f(%08lx) vs %f(%08lx): (%f,%f,%d) vs (%f,%f,%d): %d\n",
               in->index, out->index, scoreIn, scoreIn, scoreOut, scoreOut,
               decodeW(in->vals[0]), decodeW(in->vals[1]), in->count,
               decodeW(out->vals[0]), decodeW(out->vals[1]), out->count,
               scoreIn >= scoreOut);
    }
#endif

    // No need to check the indexes if less; the way it's structured out will
    // always be on the higher index.
    if (scoreIn <= scoreOut)
        *out = *in;
}
#endif

void prefixSumW(__local WIndexed * w, uint32_t n);

#if 1
// Post: start[0] <- start[0] + init
//       start[n] <- sum(start[0...n]) + init
// This is a low latency prefix sum, not work-minimizing
void prefixSumW(__local WIndexed * w, uint32_t n)
{
    // iter 0
    // a b c d e f g h i j k l m n o p

    // iter 1
    // 0 0+1 2 2+3 4 4+5 6 6+7 8 8+9 10 10+11 12 12+13 14 14+15
    // a a+b c c+d e e+f g g+h i i+j  k   k+l   m  m+n  o   o+p
    //     0     1     2     3     4        5        6        7
    
    // j = 2, blocks of 2, one thread per block, read from 0
    // n odd: n-1 + n
    // n even:      n
    
    // iter 2
    // 0   1   1+2     1+3 4   5   5+6     5+7    
    // a a+b a+b+c a+b+c+d e e+f e+f+g e+f+g+h ...
    //           0       1           2       3
    // 8   9  9+10    9+11 12    13 13+14   13+15
    // i i+j i+j+k i+j+k+l  m   m+n m+n+o m+n+o+p
    //           4       5              6       7
    
    // j = 4, blocks of 4, two threads per block, read from 1
    // n % 4 == 0,1:         n
    // n % 4 == 2,3: n%4+1 + n
    
    // iter 3
    // 0   1     2       3        3+4           3+5           3+6             3+7
    // a a+b a+b+c a+b+c+d  a+b+c+d+e   a+b+c+d+e+f a+b+c+d+e+f+g a+b+c+d+e+f+g+h
    //                              0             1             2               3
    // 8   9    10      11      11+12         11+13         11+14           11+15
    // i i+j i+j+k i+j+k+l  i+j+k+l+m   i+j+k+l+m+n i+j+k+l+m+n+o i+j+k+l+m+n+o+p
    //                              4             5             6               7
    
    // j = 8, blocks of 8, four threads per block, read from 3
    // n % 8 == 0,1,2,3:         n
    // n % 8 == 4,5,6,7: n%8+3 + n
    
    // iter 4, blocks of 16, 8 threads per block, read from 7
    // n % 16 < 8: n
    // n % 16 >= 8: n%16+7+n

    // iter i, j = 2^i, blocks of j, j/2 threads per block, read from j/2-1

    for (uint32_t iter = 0, blockSize = 2;  blockSize < n * 2;  blockSize *= 2, iter += 1) {
        // Each thread has one increment to do.  Here we calculate the index of
        // the result and the index of the argument.

        // If we need more than one round from our warp to process all elements,
        // then do  it
        for (uint32_t i = get_local_id(0);  i < n / 2;  i += get_local_size(0)) {
            // j is the number of elements in the block.
            uint32_t blockNum = i * 2 / blockSize;
            uint32_t threadNumInBlock = i % (blockSize / 2);
            uint32_t blockStart = blockNum * blockSize;
            uint32_t halfBlockSize = blockSize/2;

            // All read from this same one
            uint32_t argIndex = blockStart + halfBlockSize - 1;

            // We add to the second half of the block
            uint32_t resultIndex = blockStart + halfBlockSize + threadNumInBlock;

            if (resultIndex < n) {
#if 0
                if (debug) {
                    printf("b %d = %d + %d = (%f,%f,%d,i%d) + (%f,%f,%d,i%d) = (%f,%f,%d)\n",
                           resultIndex, resultIndex, argIndex,
                           decodeW(w[resultIndex].vals[0]),
                           decodeW(w[resultIndex].vals[1]),
                           w[resultIndex].count, w[resultIndex].index,
                           decodeW(w[argIndex].vals[0]),
                           decodeW(w[argIndex].vals[1]),
                           w[argIndex].count, w[argIndex].index,
                           decodeW(w[resultIndex].vals[0] + w[argIndex].vals[0]),
                           decodeW(w[resultIndex].vals[1] + w[argIndex].vals[1]),
                           w[resultIndex].count + w[argIndex].count
                           );
                }
#endif

                // All threads read the same value (argIndex) and write a different
                // element of the block (resultIndex)
                incrementW(&w[resultIndex].w, &w[argIndex].w);
            }
        }

        // Make sure everything has finished on this iteration before we do the
        // next one
        barrier(CLK_LOCAL_MEM_FENCE);
    }
}
#endif

uint32_t argMinScanW(__local WIndexed * w, uint32_t n, W wAll);

#if 1
// Return the lowest index of the W array with a score equal to the minimum
// score.  Only get_local_id(0) knows the actual correct result; the result
// from the others should be ignored.
uint32_t argMinScanW(__local WIndexed * w, uint32_t n, W wAll)
{
    for (uint32_t iter = 0, blockSize = 2;  blockSize < n * 2;  blockSize *= 2, iter += 1) {
        // If we need more than one round from our warp to process all elements,
        // then do  it
        for (uint32_t i = get_local_id(0);  i < n / 2;  i += get_local_size(0)) {
            // j is the number of elements in the block.
            uint32_t blockNum = i * 2 / blockSize;
            uint32_t threadNumInBlock = i % (blockSize / 2);
            uint32_t blockStart = blockNum * blockSize;
            uint32_t halfBlockSize = blockSize/2;

            // All read from this same one
            uint32_t argIndex = blockStart + halfBlockSize - 1;

            // We add to the second half of the block
            uint32_t resultIndex = blockStart + halfBlockSize + threadNumInBlock;

            if (resultIndex <  n) {
                // Compare them, taking the minimum and pushing it right
                minW(w + resultIndex, w + argIndex, wAll);
            }
        }

        // Make sure everything has finished on this iteration before we do the
        // next one
        barrier(CLK_LOCAL_MEM_FENCE);
    }

    return w[n - 1].index;
}
#elif 0
uint32_t argMinScanW(__local WIndexed * w, uint32_t n, W wAll)
{
    if (get_local_id(0) == 0) {
        for (uint32_t i = 1;  i < n;  ++i) {
            minW(w, w + i, wAll);
        }
        return w[0].index;
    }
    else {
        return 0;
    }
}
#endif

#if 1

PartitionSplit
chooseSplitKernelOrdinal(__global const W * w,
                         uint32_t numBuckets,
                         W wAll,
                         __local WIndexed * wLocal,
                         uint32_t wLocalSize);

PartitionSplit
chooseSplitKernelOrdinal(__global const W * w,
                         uint32_t numBuckets,
                         W wAll,
                         __local WIndexed * wLocal,
                         uint32_t wLocalSize)
{
    PartitionSplit result = PARTITION_SPLIT_INIT;

    uint32_t bucket = get_global_id(0);

    // For now, we serialize on a single thread
    if (bucket != 0)
        return result;

    // Now check our result
    
    uint32_t f = get_global_id(1);
    uint32_t p = get_global_id(2);

    //if (bucket == 0) {
    //   printf("feature %d is ordinal\n", f);
    //}

    // Calculate best split point for ordered values
    W wFalse = wAll, wTrue = { {0, 0}, 0 };

    float bestScore = INFINITY;
    int bestSplit = -1;
    W bestLeft, bestRight;
    
    // Now test split points one by one
    for (unsigned j = 0;  j < numBuckets;  ++j) {

        //if (wFalse.count < 0 || wTrue.count < 0)
        //    printf("p %d f %d split %d false = (%.4f,%.4f,%d) true = (%.4f,%.4f,%d) cur = (%.4f,%.4f,%d) all=(%.4f,%.4f,%d)\n", p, f, j,
        //           decodeW(wFalse.vals[0]), decodeW(wFalse.vals[1]), wFalse.count,
        //           decodeW(wTrue.vals[0]), decodeW(wTrue.vals[1]), wTrue.count,
        //           decodeW(w[j].vals[0]), decodeW(w[j].vals[1]), w[j].count,
        //           decodeW(wAll.vals[0]), decodeW(wAll.vals[1]), wAll.count);
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

    if (p == 0 && false) {
        printf("real wBest %d (%f,%f,%d) %f\n",
               bestSplit,
               decodeW(bestLeft.vals[0]),
               decodeW(bestLeft.vals[1]),
               bestLeft.count,
               bestScore);
    }
    
    result.score = bestScore;

    result.feature = f;
    result.value = bestSplit;
    result.left = bestLeft;
    result.right = bestRight;

    return result;
}    

inline PartitionSplit
chooseSplitKernelCategorical(__global const W * w,
                             uint32_t numBuckets,
                             W wAll)
{
    PartitionSplit result = PARTITION_SPLIT_INIT;

    uint32_t bucket = get_global_id(0);

    // For now, we serialize on a single thread
    if (bucket != 0)
        return result;

    uint32_t f = get_global_id(1);

    //if (bucket == 0) {
    //    printf("feature %d is categorical\n", f);
    //}

    float bestScore = INFINITY;
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
            
        float s = scoreSplit(wFalse, w[j]);

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
#endif

#if 1
PartitionSplit
chooseSplit(__global const W * w,
            uint32_t numBuckets,
            W wAll,
            __local WIndexed * wLocal,
            uint32_t wLocalSize,
            __local WIndexed * wStartBest,  // length 2
            bool ordinal,
            uint32_t partitionIndex);

// Function that uses a single workgroup to scan all of the buckets in a split
PartitionSplit
chooseSplit(__global const W * w,
            uint32_t numBuckets,
            W wAll,
            __local WIndexed * wLocal,
            uint32_t wLocalSize,
            __local WIndexed * wStartBest,  // length 2
            bool ordinal,
            uint32_t partitionIndex)  // 
{
    PartitionSplit result = PARTITION_SPLIT_INIT;

    uint32_t f = get_global_id(1);
    //uint32_t p = get_global_id(2);

    // We need some weight in both true and false for a split to make sense
    if (wAll.vals[0] == 0 || wAll.vals[1] == 0)
        return result;
    
    // Read our W array into shared memory
    
    // If we have more W values than local buckets, we need to
    // do the prefix sum multiple times to fill it up completely

    // Contains the prefix sum from the previous iteration
    __local WIndexed * wStart = wStartBest;

    // Contains the best split from the previous iteration
    __local WIndexed * wBest = wStartBest + 1;
    

    if (get_local_id(0) == 0) {
        // Initialize wBest
        if (ordinal) {
            wStart->w.vals[0] = wStart->w.vals[1] = wStart->w.count = 0;
            wStart->index = -1;
        }
        wBest->w.vals[0] = wBest->w.vals[1] = wBest->w.count = 0;
        wBest->index = -1;
    }

    // Shouldn't be necessary; just in case
    barrier(CLK_LOCAL_MEM_FENCE);

    //bool debug = false;//(f == 0 && p == 2 && get_global_size(2) == 8);
 
    for (uint32_t startBucket = 0;  startBucket < numBuckets;
        startBucket += wLocalSize) {

        //if (startBucket != 0) {
        //    printf("f %d p %d startBucket %d\n",
        //           f, p, startBucket);
        //}
        
        uint32_t endBucket = min(startBucket + wLocalSize, numBuckets);
        uint32_t numBucketsInIteration = endBucket - startBucket;
        
        for (uint32_t i = get_local_id(0);  i < numBucketsInIteration;
             i += get_local_size(0)) {
            wLocal[i].w = w[startBucket + i];

            // We don't want buckets with zero count to participate in the
            // scoring, so those get an index of -1 to mark them as empty.
            // They still accumulate their weight, however.
            if (wLocal[i].w.count > 0)
                wLocal[i].index = startBucket + i;
            else wLocal[i].index = -1;

            //if (debug) {
                //printf("bucket %d has count %d index %d\n",
                //       startBucket + i, wLocal[i].count, wLocal[i].index);
            //}
            
            if (i == 0 && startBucket != 0) {
                // Add the previous prefix to the first element
                if (ordinal) {
                    incrementW(&wLocal[0].w, &wStart[0].w);

                    // if (debug) {
                    //     printf("f %d p %d sb %d new iter (%f,%f,%d) min (%f,%f,%d,i%d) first (%f,%f,%d,i%d) in (%f,%f,%d)\n",
                    //            f, p, startBucket,
                    //            decodeW(wStart->vals[0]), decodeW(wStart->vals[1]),
                    //            wStart->count,
                    //            decodeW(wBest->vals[0]), decodeW(wBest->vals[1]),
                    //            wBest->count, wBest->index,
                    //            decodeW(wLocal->vals[0]), decodeW(wLocal->vals[1]),
                    //            wLocal->count, wLocal->index,
                    //            decodeW(w[startBucket].vals[0]),
                    //            decodeW(w[startBucket].vals[1]),
                    //            w[startBucket].count
                    //            );
                    // }
                }
            }
        }
        barrier(CLK_LOCAL_MEM_FENCE);

        if (ordinal) {
            // Now we've read it in, we can do our prefix sum
            prefixSumW(wLocal, numBucketsInIteration);

            // Seed for the next one
            *wStart = wLocal[numBucketsInIteration - 1];
        }

        // And then scan that to find the index of the best score
        argMinScanW(wLocal, numBucketsInIteration, wAll);

        // Find if our new bucket is better than the last champion
        minW(wBest, wLocal + numBucketsInIteration - 1, wAll);

        barrier(CLK_LOCAL_MEM_FENCE);
    }

#if 0
    if (get_local_id(0) == 0 && ordinal) {
        if (wStart->vals[0] != wAll.vals[0]
            || wStart->vals[1] != wAll.vals[1]
            || wStart->count != wAll.count) {
            //printf("Accumulation error: wAll != wStart: (%f,%f,%d) != (%f,%f,%d)\n",
            //       decodeW(wAll.vals[0]), decodeW(wAll.vals[1]), wAll.count,
            //       decodeW(wStart->vals[0]), decodeW(wStart->vals[1]), wStart->count);
        }
    }
#endif

#if 0
    if (p < 256 && get_local_id(0) == 0 && false) {
        //printf("f %d p %d numBuckets = %d\n",
        //       f, p, numBuckets);

#if 0        
        printf("wAll (%f,%f,%d)\n",
               decodeW(wAll.vals[0]), decodeW(wAll.vals[1]), wAll.count);

        for (uint32_t i = 0;  i < numBuckets;  ++i) {
            printf("  b%d in (%f,%f,%d) cum (%f,%f,%d)\n",
                   i, decodeW(w[i].vals[0]), decodeW(w[i].vals[1]),
                   w[i].count,
                   decodeW(wLocal[i].vals[0]), decodeW(wLocal[i].vals[1]),
                   wLocal[i].count);
        }
#endif
        float score = scoreSplitWAll(*wBest, wAll);

        
        printf("f %d p %d nb %5d wBest %4d ord %d (%f,%f,%7d) %f\n",
               f, p, numBuckets, wBest->index, ordinal,
               decodeW(wBest->vals[0]), decodeW(wBest->vals[1]), wBest->count,
               score);
    }
#endif

    result.score = scoreSplitWAll(wBest->w, wAll);
    result.feature = f;
    result.value = wBest->index + ordinal;  // ordinal indexes are offset by 1
    result.left = wBest->w;
    result.right = wAll;

    result.right.vals[0] -= wBest->w.vals[0];
    result.right.vals[1] -= wBest->w.vals[1];
    result.right.count   -= wBest->w.count;

    return result;
}
#endif

#if 1
// this expects [bucket, feature, partition]
__kernel void
getPartitionSplitsKernel(uint32_t totalBuckets,
                         uint32_t numPartitions,
                         __global const uint32_t * bucketNumbers, // [nf]
                         
                         __global const uint32_t * featuresActive, // [nf]
                         __global const uint32_t * featureIsOrdinal, // [nf]
                         
                         __global const W * buckets, // [np x totalBuckets]

                         __global const W * wAll,  // one per partition
                         __global PartitionSplit * featurePartitionSplitsOut, // [np x nf]

                         __local WIndexed * wLocal,  // [wLocalSize]
                         uint32_t wLocalSize,
                         __local WIndexed * wStartBest) // [2]
{
    uint32_t f = get_global_id(1);
    uint32_t nf = get_global_size(1);
    uint32_t partition = get_global_id(2);
    
    uint32_t bucket = get_global_id(0);
    uint32_t partitionIndex = partition + numPartitions;

    if (f >= nf || partition >= numPartitions)
        return;

    const PartitionSplit NONE = PARTITION_SPLIT_INIT;
    PartitionSplit best = NONE;

    // Don't do inactive features
    if (!featuresActive[f] || wAll[partition].count == 0) {
        if (bucket == 0) {
            //printf("writing out inactive PartitionSplit part %d f %d idx %x\n", partition, f, partition * nf + f);
            featurePartitionSplitsOut[partition * nf + f] = best;            
        }
        return;
    }
    
    uint32_t bucketStart = bucketNumbers[f];
    uint32_t bucketEnd = bucketNumbers[f + 1];
    uint32_t numBuckets = bucketEnd - bucketStart;

    if (numBuckets == 0) {
        printf("ERROR: zero buckets for part %d feat %d\n", partition, f);
    }

    // We dimension as the feature with the highest number of buckets, so
    // for most of them we have to many and can stop
    if (bucket >= numBuckets)
        return;

    // SPEED DEBUG
    //if (bucket == 0) {
    //    printf("getPartitionSplits f=%d p=%d numPartitions=%d\n", f, partition, numPartitions);
    //}
    //if (numPartitions == 65536)
    //    return;

    // Find where our bucket data starts
    __global const W * myW
        = buckets
        + (totalBuckets * partition)
        + bucketStart;
    
    bool ordinal = featureIsOrdinal[f];

#if 1
    best = chooseSplit(myW, numBuckets, wAll[partition],
                       wLocal, wLocalSize, wStartBest, ordinal, partitionIndex);
#elif 1
    // TODO: use prefix sums to enable better parallelization (this will mean
    // multiple kernel launches, though, so not sure) or accumulate locally
    // per set of buckets and then accumulate globally in a second operation.

    if (wAll[partition].vals[0] == 0 || wAll[partition].vals[1] == 0) {
        // Nothing to do; we have either a uniformly true or a uniformly false
        // label across the partition
    }
    else if (ordinal) {
        // We have to perform a prefix sum to have the data over which
        // we can calculate the split points.  This is more complicated.
        best = chooseSplitKernelOrdinal(myW, numBuckets, wAll[partition],
                                        wLocal, wLocalSize);
    }
    else {
        // We have a simple search which is independent per-bucket.
        best = chooseSplitKernelCategorical(myW, numBuckets, wAll[partition]);
    }
    if (bucket == 0) {
        best.index = partitionIndex;
    }
#endif
    
    if (bucket == 0) {
        if (best.score == INFINITY) {
            best = NONE;
        }
        //printf("writing out active PartitionSplit part %d f %d idx %x score %f\n", partition, f, partition * nf + f, best.score);
        featurePartitionSplitsOut[partition * nf + f] = best;
    }

    //printf("part %d feature %d bucket %d score %f\n",
    //       partition, f, best.value, best.score);


#if 0    
    // Finally, we have each feature update the lowest split for the
    // partition.  Spin until we've acquired the lock for the partition.
    // We won't hold it for long, so the spinlock is not _too_ inefficient.
    // NOTE: this only works because we make all workgroups apart from one
    // return above.  In general, you can't do a simple spin lock like this
    // in a kernel without causing a deadlock.
    if (bucket == 0) {

        printf("waiting feature %d partition %d\n", f, partition);

        while (atomic_inc(&partitionLocks[partition]) != 0) {
            atomic_dec(&partitionLocks[partition]);
        }

        printf("starting feature %d partition %d\n", f, partition);
    
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

        printf("finishing feature %d partition %d\n", f, partition);
    }
#endif
}
#endif

// id 0: partition number
__kernel void
bestPartitionSplitKernel(uint32_t numFeatures,
                         __global const uint32_t * featuresActive, // [numFeatures]
                         __global const PartitionSplit * featurePartitionSplits,
                         __global PartitionSplit * allPartitionSplitsOut,
                         uint32_t partitionSplitsOffset)
{
    allPartitionSplitsOut += partitionSplitsOffset;

    uint32_t p = get_global_id(0);
    uint32_t partitionIndex = p + get_global_size(0);

    PartitionSplit best = PARTITION_SPLIT_INIT;

    featurePartitionSplits += p * numFeatures;
    allPartitionSplitsOut += p;
    
    //printf("bestPartitionSplitKernel for partition %d\n", p);

    // TODO: with lots of features this will become slow; we can have a workgroup cooperate
    for (uint32_t f = 0;  f < numFeatures;  ++f) {
        if (!featuresActive[f])
            continue;
        if (featurePartitionSplits[f].value < 0)
            continue;
        if (featurePartitionSplits[f].score < best.score) {
            best = featurePartitionSplits[f];
        }
    }

    *allPartitionSplitsOut = best;
}

// [partition, bucket]
__kernel void
clearBucketsKernel(__global W * bucketsOut,
                   __global W * wAllOut,
                   __global const PartitionSplit * allPartitionSplits,
                   uint32_t numActiveBuckets,
                   uint32_t partitionSplitsOffset)

{
    allPartitionSplits += partitionSplitsOffset;
    uint32_t numPartitions = get_global_size(0);
    
    uint32_t i = get_global_id(0);

    // Commented out because it causes spurious differences in the debugging code between
    // the two sides.  It doesn' thave any effect on the output, though, and saves some
    // work.
    //if (allPartitionSplits[i].right.count == 0)
    //    return;
            
    uint32_t j = get_global_id(1);

    if (j >= numActiveBuckets)
        return;
    
    __global W * bucketsRight
        = bucketsOut + (i + numPartitions) * numActiveBuckets;
    
    // We double the number of partitions.  The left all
    // go with the lower partition numbers, the right have the higher
    // partition numbers.

    const W empty = { { 0, 0 }, 0 };
    
    bucketsRight[j] = empty;
    if (j == 0) {
        wAllOut[i + numPartitions] = empty;
    }
}

#if WBITS==64
inline void incrementWAtomic(__global W * w,
                             bool label,
                             float weight)
{
    int64_t incr = encodeW(weight);
    atom_add_64_global(&w->vals[label], incr);
    atom_inc(&w->count);
}

inline void decrementWAtomic(__global W * w,
                             bool label,
                             float weight)
{
    int64_t incr = encodeW(weight);
    atom_sub_64_global(&w->vals[label], incr);
    atom_dec(&w->count);
}
#elif WBITS==32
inline void incrementWAtomic(__global W * w,
                             bool label,
                             float weight)
{
    int32_t incr = encodeW(weight);
    atom_add(&w->vals[label], incr);
    atom_inc(&w->count);
}

inline void decrementWAtomic(__global W * w,
                             bool label,
                             float weight)
{
    int32_t incr = encodeW(weight);
    atom_sub(&w->vals[label], incr);
    atom_dec(&w->count);
}
#endif

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

// Structure for a partition number
typedef struct {
    int16_t num;  // partition number
} RowPartitionInfo;

// Second part: per feature plus wAll, transfer examples
//[rowNumber]
__kernel void
__attribute__((reqd_work_group_size(256,1,1)))
updatePartitionNumbersKernel(uint32_t partitionSplitsOffset,
                    
                            __global RowPartitionInfo * partitions,
                            __global uint8_t * directions,
                            uint32_t numRows,
                    
                            __global const PartitionSplit * allPartitionSplits,

                            __global const uint32_t * bucketData,
                            __global const uint32_t * bucketDataOffsets,
                            __global const uint32_t * bucketNumbers,
                            __global const uint32_t * bucketEntryBits,
                            __global const uint32_t * featureIsOrdinal)
{
    uint32_t r = get_global_id(0);
    if (r >= numRows)
        return;

    bool debug = false; //(r == 845 || r == 3006 || r == 3758);

    allPartitionSplits += partitionSplitsOffset;
    uint16_t partition = partitions[r].num;
    int16_t splitFeature = allPartitionSplits[partition].feature;

    if (debug)
        printf("r = %d partition = %d splitFeature = %d\n", r, partition, splitFeature);

    if (splitFeature == -1) { // reached a leaf, nothing to split
        directions[r] = 0;  // for debug, not needed otherwise
        return;
    }

    uint16_t splitValue = allPartitionSplits[partition].value;
    bool ordinal = featureIsOrdinal[splitFeature];

    // Split feature values go here
    uint32_t splitBucketDataOffset = bucketDataOffsets[splitFeature];
    uint32_t splitBucketDataLength = bucketDataOffsets[splitFeature + 1] - splitBucketDataOffset;
    uint32_t splitNumBuckets = bucketNumbers[splitFeature + 1] - bucketNumbers[splitFeature];
    __global const uint32_t * splitBucketData = bucketData + splitBucketDataOffset;
    uint32_t splitBucketBits = bucketEntryBits[splitFeature];

    uint32_t bucket = getBucket(r,
                                splitBucketData, splitBucketDataLength,
                                splitBucketBits, splitNumBuckets);
    
    uint32_t side = ordinal ? bucket >= splitValue : bucket != splitValue;
    if (side)
        partitions[r].num = partition + side * partitionSplitsOffset;
    uint8_t direction = partitionSplitDirectionGlobal(&allPartitionSplits[partition]);

    if (debug)
        printf("splitValue = %d bucket=%d ordinal=%d side=%d direction=%d lcount %d rcount %d\n",
               splitValue, bucket, ordinal, side, direction, allPartitionSplits[partition].left.count, allPartitionSplits[partition].right.count);

    directions[r] = direction;
}

__kernel void
//__attribute__((reqd_work_group_size(256,1,1)))
updateBucketsKernel(uint32_t partitionSplitsOffset,
                    uint32_t numActiveBuckets,
                    
                    __global const RowPartitionInfo * partitions,
                    __global const uint8_t * directions,
                    __global W * buckets,
                    __global W * wAll,

                    // Row data
                    __global const float * decodedRows,
                    uint32_t numRows,
                    
                    // Feature data
                    __global const uint32_t * bucketData,
                    __global const uint32_t * bucketDataOffsets,
                    __global const uint32_t * bucketNumbers,
                    __global const uint32_t * bucketEntryBits,

                    __global const uint32_t * featuresActive,
                    __global const uint32_t * featureIsOrdinal,

                    __local W * wLocal,
                    uint32_t maxLocalBuckets)
{
    int f = get_global_id(1) - 1;  // -1 means wAll, otherwise it's the feature number

    if (f != -1 && !featuresActive[f])
        return;

    // We have to set up to access two different features:
    // 1) The feature we're splitting on for this example (splitFeature)
    // 2) The feature we're updating for the split (f)
    // Thus, we need to perform the same work twice, once for each of the
    // features.
    
    uint32_t bucketDataOffset;
    uint32_t bucketDataLength;
    uint32_t numBuckets;
    uint32_t bucketBits;
    uint32_t numLocalBuckets = 0;
    uint32_t startBucket;
            
    // Pointer to the global array we eventually want to update
    __global W * wGlobal;

    // We always transfer weights from the left to the right, and then
    // in a later kernel swap them
    if (f == -1) {
        numBuckets = partitionSplitsOffset * 2;
        wGlobal = wAll;
        numLocalBuckets = min(partitionSplitsOffset, maxLocalBuckets);
        startBucket = 0;
    }
    else {
        bucketDataOffset = bucketDataOffsets[f];
        bucketDataLength = bucketDataOffsets[f + 1] - bucketDataOffset;
        numBuckets = bucketNumbers[f + 1] - bucketNumbers[f];
        bucketData = bucketData + bucketDataOffset;
        bucketBits = bucketEntryBits[f];
        numLocalBuckets = min(numBuckets * partitionSplitsOffset, maxLocalBuckets);
        //printf("f %d nlb = %d nb = %d ro = %d mlb = %d\n",
        //       f, numLocalBuckets, numBuckets, partitionSplitsOffset, maxLocalBuckets);
        wGlobal = buckets;
        startBucket = bucketNumbers[f];
    }

    // Clear our local accumulation buckets to get started
    for (uint32_t b = get_local_id(0);  b < numLocalBuckets; b += get_local_size(0)) {
        zeroW(wLocal + b);
    }

    // Wait for all buckets to be clear
    barrier(CLK_LOCAL_MEM_FENCE);
    
    //numLocalBuckets = 0;  // DEBUG DO NOT COMMIT
    
    for (uint32_t i = get_global_id(0);  i < numRows;  i += get_global_size(0)) {

        // Partition was updated already, so here we mask out the offset to know where the
        // partition came from (ensure that this partition number is parent partition number)
        uint16_t partition = partitions[i].num & (partitionSplitsOffset - 1);
        bool side = partition != partitions[i].num;  // it is on the right if the partition number changed
        bool direction = directions[i];

        // We only need to update features on the wrong side, as we
        // transfer the weight rather than sum it from the
        // beginning.  This means less work for unbalanced splits
        // (which are typically most of them, especially for discrete
        // buckets)
        if (direction == side)
            continue;

        uint32_t leftPartition = partition;
        uint32_t rightPartition = partition + partitionSplitsOffset;

        float weight = fabs(decodedRows[i]);
        bool label = decodedRows[i] < 0;
        uint32_t exampleNum = i;

        uint32_t toBucketLocal, toBucketGlobal;
        
        if (f == -1) {
            // Since we don't touch the buckets on the left side of the
            // partition, we don't need to have local accumulators for them.
            // Hence, the partition number for local is the left partition.
            toBucketLocal = leftPartition;
            toBucketGlobal = rightPartition;
        }
        else {
            uint32_t bucket = getBucket(exampleNum,
                                        bucketData, bucketDataLength,
                                        bucketBits, numBuckets);
            toBucketLocal = leftPartition * numBuckets + bucket;
            toBucketGlobal = rightPartition * numActiveBuckets + startBucket + bucket;
        }

        if (f == 0 && false)
            printf("updating row %d partition %d side %d direction %d weight %f label %d toBucket %d\n",
                   i, (uint32_t)partition, side, direction, weight, label,
                   toBucketGlobal - (rightPartition * numActiveBuckets + startBucket));


        if (toBucketLocal < numLocalBuckets) {
            incrementWLocal(wLocal + toBucketLocal, label, weight);
        }
        else {
            incrementWAtomic(wGlobal + toBucketGlobal, label, weight);
        }
    }

    // Wait for all buckets to be updated, before we write them back to the global mem
    barrier(CLK_LOCAL_MEM_FENCE);
    
    // Now write our local changes to the global
    for (uint32_t b = get_local_id(0);  b < numLocalBuckets;  b += get_local_size(0)) {
        if (wLocal[b].count != 0) {
            if (f == -1) {
                incrementWOut(wGlobal + b + partitionSplitsOffset, wLocal + b);
                continue;
            }

            // TODO: avoid div/mod if possible in these calculations
            uint32_t partition = b / numBuckets + partitionSplitsOffset;
            uint32_t bucket = b % numBuckets;
            uint32_t bucketNumberGlobal = partition * numActiveBuckets + startBucket + bucket;

            if (f == 0 && false) {
                printf("f %d local bucket %d part %d buck %d nb %d nab %d global bucket %d cnt %d\n",
                    f, b, partition, bucket, numBuckets, numActiveBuckets, bucketNumberGlobal,
                    wLocal[b].count);
            }

            incrementWOut(wGlobal + bucketNumberGlobal, wLocal + b);
            //atomic_inc(&numGlobalUpdates);
        }
    }
}

// For each partition and each bucket, we up to now accumulated just the
// weight that needs to be transferred in the right hand side of the
// partition splits.  We need to fix this up by:
// 1.  Subtracting this weight from the left hand side
// 2.  Flipping the buckets where the big amount of weight belongs on the
//     right not on the left.
//
// This is a 2 dimensional kernel:
// Dimension 0 = partition number (from 0 to the old number of partitions)
// Dimension 1 = bucket number (from 0 to the number of active buckets)
__kernel void
fixupBucketsKernel(__global W * buckets,
                   __global W * wAll,
                   __global const PartitionSplit * allPartitionSplits,
                   uint32_t numActiveBuckets,
                   uint32_t partitionSplitsOffset)

{
    allPartitionSplits += partitionSplitsOffset;
    uint32_t numPartitions = get_global_size(0);

    uint32_t i = get_global_id(0);  // partition number

    // Commented out because it causes spurious differences in the debugging code between
    // the two sides.  It doesn' thave any effect on the output, though, and saves some
    // work.
    //if (partitionSplits[i].right.count == 0)
    //    return;
            
    uint32_t j = get_global_id(1);  // bucket number in partition

    if (j >= numActiveBuckets)
        return;

    __global W * bucketsLeft = buckets + i * numActiveBuckets;
    __global W * bucketsRight = buckets + (i + numPartitions) * numActiveBuckets;
    
    // We double the number of partitions.  The left all
    // go with the lower partition numbers, the right have the higher
    // partition numbers.

    if (bucketsRight[j].count > 0 && false) {
        printf("part %d decrementing bucket %d by count %d (from %d to %d)\n",
                i, j, bucketsRight[j].count, bucketsLeft[j].count,
                bucketsLeft[j].count - bucketsRight[j].count);
    }

    decrementWOutGlobal(bucketsLeft + j, bucketsRight + j);
    if (j == 0) {
        decrementWOutGlobal(wAll + i, wAll + i + numPartitions);
    }
    
    if (partitionSplitDirectionGlobal(&allPartitionSplits[i])) {
        // We need to swap the buckets
        if (j == 0 && false) {
            printf("swapping buckets part %d\n", i);
        }
        W tmp = bucketsRight[j];
        bucketsRight[j] = bucketsLeft[j];
        bucketsLeft[j] = tmp;

        if (j == 0) {
            tmp = wAll[i + numPartitions];
            wAll[i + numPartitions] = wAll[i];
            wAll[i] = tmp;
        }
    }
}

#if 0 // Later... recursively do the small partitions, one per workgroup

typedef struct {
} WorkItem;

typedef struct {
    __global WorkItem * workItems;
    uint32_t len;
    uint32_t read;
    uint32_t write;
} WorkItemData;

WorkItem popWorkItem(__global WorkItemData * work)
{
    // Will return the same for every thread in the workgroup

    
}

void pushWorkItem(__global WorkItemData * work)
{
    WorkItem allWorkItems[65536];
}

// Once we get down to the lower levels of the tree, there are too many
// branches to process them all in parallel due to the amount of memory
// required for accumulation buckets.  Instead, we change strategy.  We
// split the problem into two:
//
// - One part where we still have a lot of rows per partition (we filter
//   down to a small number of partitions that still contain most of the
//   examples).  These can still be processed in parallel the same as
//   before.  Typically, this will be 1/16th to 1/256th of the number of
//   partitions, which enables another 4 to 8 levels to be done before
//   the buckets fill up again.  This keeps the data and expanded
//   buckets from the previous round.
//
//   The extra information per partition is simply the partition index.
//
//   This will be handled by the normal invocations on the next run
//   through the kernel.
//
// - A second part with a lot of partitions, each of which has only a
//   small number of rows per partition.  The information kept to
//   specify one of these is:
//   - a wAll value
//   - a list of example numbers within the bucket
//   - partition index
//
//   Each of these partitions is pushed onto a work-queue, where each
//   partition is handled by a single work group.  The work group will
//   accumulate the buckets in shared memory, calculate the best split,
//   output a partition split as a result, and if recursion is required
//   then push the right-hand bucket onto the work queue whilst
//   continuing to process the left-hand bucket recursively.

// Input: PartitionSplits
// Output: Same, but sorted by number of examples within the partition

typedef struct PartitionSortEntry {
    uint32_t count;
    uint32_t index;
};

// Single WG function
// n must be a power of two
// NOTE http://www.bealto.com/gpu-sorting_parallel-merge-local.html
void sortPartitionBlockLocal(__local PartitionSplit * aux,
                             uint32_t n)
{
    uint32_t i = get_local_id(0); // index in workgroup

    // We merge sub-sequences of length 1,2,...,WG/2
    for (uint32_t length = 1;  length < n;  length *= 2) {
        struct PartitionSortEntry entry = aux[i];
        uint32_t ikey = entry.count;
        uint32_t ii = i & (length-1);  // index in our sequence in 0..length-1
        uint32_t sibling = (i - ii) ^ length; // beginning of the sibling sequence
        uint32_t pos = 0;

        // increment for dichotomic search
        for (uint32_t inc = length;  inc > 0;  inc >>= 1) {
            uint32_t j = sibling + pos + inc - 1;
            uint jKey = aux[j].count;
            bool smaller = (jKey > iKey) || ( jKey == iKey && j < i );
            pos += (smaller) ? inc : 0;
            pos = min(pos,length);
        }
        uint32_t bits = 2 * length - 1; // mask for destination
        uint32_t dest
            = ((ii + pos) & bits)
            | (i & ~bits); // destination index in merged sequence

        barrier(CLK_LOCAL_MEM_FENCE);

        aux[dest] = iData;

        barrier(CLK_LOCAL_MEM_FENCE);
    }
}

void sortPartitionBlock(__global const PartitionSplit * in,
                        __global PartitionSplit * out,
                        uint32_t n,
                        __local PartitionSplit * aux)
{
    uint32_t i = get_local_id(0);

    // Copy into local memory
    aux[i] = in[i];

    barrier(CLK_LOCAL_MEM_FENCE);

    // Sort the local block
    sortPartitionBlockLocal(aux, n);
    
    barrier(CLK_LOCAL_MEM_FENCE);

    // Copy the sorted block back out
    out[i] = aux[i];
    
    barrier(CLK_LOCAL_MEM_FENCE);
}

void mergeSortRanges(__global const PartitionSplit * in,
                     __global PartitionSplit * out,
                     uint32_t n)
{
    
}

#define SORT_PARTITIONS_NUM_AUX_ENTRIES 512

__kernel void sortPartitionsKernel(__global PartitionSplit * splits,
                                   uint32_t numSplits)
{
    __local struct PartitionSortEntry aux[SORT_PARTITIONS_NUM_AUX_ENTRIES];
}

// Dimensions:
// 0 = worker index
__kernel void
splitKernel()
{
    // First, 
}
#endif

#if 0
// Split a dataset.  It compacts into two parts:
// - An active part, with full buckets and a reduced set of partitions.
//   typically, this will contain most of the rows and very few of the
//   partitions.  They can be processed on the GPU efficiently.
// - A passive part, with no buckets and a large set of partitions.
//   This will normally contain few rows but lots of partition, and is
//   hard to efficiently process on the GPU due to the number of kernel
//   launches required.  As a result, it's usually better to do these
//   on the CPU.

// Split a dataset per partition.  It generates a set of offsets for
// - expanded rows, per partition (in-place)
// - 
// - 

// NOTE http://www.bealto.com/gpu-sorting_parallel-merge-local.html
#define PartitionSplit data_t
#define float key_t

__kernel void
ParallelMerge_Local(__global const data_t * in,__global data_t * out,__local data_t * aux)
{
  uint32_t i = get_local_id(0); // index in workgroup
  uint32_t wg = get_local_size(0); // workgroup size = block size, power of 2

  // Move IN, OUT to block start
  uint32_t offset = get_group_id(0) * wg;
  in += offset; out += offset;

  // Load block in AUX[WG]
  aux[i] = in[i];
  barrier(CLK_LOCAL_MEM_FENCE); // make sure AUX is entirely up to date

  // Now we will merge sub-sequences of length 1,2,...,WG/2
  for (uint32_t length=1;length<wg;length<<=1)
  {
    data_t iData = aux[i];
    uint iKey = getKey(iData);
    uint32_t ii = i & (length-1);  // index in our sequence in 0..length-1
    uint32_t sibling = (i - ii) ^ length; // beginning of the sibling sequence
    uint32_t pos = 0;
    for (uint32_t inc=length;inc>0;inc>>=1) // increment for dichotomic search
    {
      uint32_t j = sibling+pos+inc-1;
      uint jKey = getKey(aux[j]);
      bool smaller = (jKey < iKey) || ( jKey == iKey && j < i );
      pos += (smaller)?inc:0;
      pos = min(pos,length);
    }
    uint32_t bits = 2*length-1; // mask for destination
    uint32_t dest = ((ii + pos) & bits) | (i & ~bits); // destination index in merged sequence
    barrier(CLK_LOCAL_MEM_FENCE);
    aux[dest] = iData;
    barrier(CLK_LOCAL_MEM_FENCE);
  }

  // Write output
  out[i] = aux[i];
}

__kernel void
compactPartitionsKernel()
{
    // 1.  Sort the partitions in order from maximum to minimum number
    //     of examples.

    sortPartitions();


    // 2.  Split into:
    // - partitions we continue to handle breadth first
    // - partitions we work on depth first

    uint32_t maxPartition = ...;
}




PartitionSplit
trainSplitSingleWarp(uint32_t numRows,
                     uint32_t numBucketsPerPartition,
                     __global float * examples,
                     __global uint32_t * exampleNumbers,
                     __local W * wLocal,
                     uint32_t numLocalBuckets)
{
    

    // Basic algorithm:
    // - this is entered by a whole single warp, so we can assume
    //   local memory is coherent across the entire set of threads
    // - we don't have global memory for a scratchpad, forcing us
    //   to keep state in registers and local memory
    // - We have a limit of 12k of local memory, which may force us
    //   to make multiple iterations through the data

    __local PartitionSplit bestSplit;

    // If we're in an ordinal feature, this is the accumulated wTrue
    // from the previous splits.
    __local W wAccum;
    
    if (get_local_id(0) == 0) {
        clearPartitionSplit(bestSplit);
        wAccum = { { 0, 0 }, 0 };
    }

    barrier(CLK_LOCAL_MEM_FENCE);
    
    // This loop makes as many passes through the data as necessary to
    // accumulate entirely within local buckets.  We put all of the
    // units to work, though.

    // Which features are we testing on this iteration?  It will be the
    // same set of features for each thread in the warp.
    uint32_t fMin = 0, fMax = 0;
        
    for (uint32_t minBucket = 0;  minBucket < numLocalBuckets;
         minBucket += numLocalBuckets) {
        uint32_t maxBucket = min(numLocalBuckets, minBucket + numLocalBuckets);
        uint32_t numBuckets = maxBucket - minBucket;

        // Clear the buckets
        for (uint32_t i = 0;  i < numBuckets;  i += get_local_dim(0)) {
            zeroW(wLocal + i);
        }
        
        barrier(CLK_LOCAL_MEM_FENCE);
        
        for (uint32_t i = 0;  i < numRows;  i += get_local_dim(0)) {
            float weight = ...;
            bool label = ...;
            uint32_t example = ...;

            for (uint32_t f = fMin;  f <= fMax;  ++f) {
                uint32_t bucket
                    = expandedBuckets[f][example]
                    + bucketStart[f]
                    - minBucket;
                if (bucket < 0)
                    continue;
                if (bucket >= numBuckets)
                    break;

                incrementWLocal(wLocal + bucket, weight, label);
            }
        }

        barrier(CLK_LOCAL_MEM_FENCE);

        // Now that all rows are done, we can score the splits in our
        // buckets.

        // Categorical features are easy; they can be independently
        // tested.  Ordinal features require a prefix sum operation
        // to properly test.

        // First feature
        for (uint32_t f = fMin;  f < fMax;  ++f) {
            if (featureIsOrdinal[f]) {
                // We have to perform a prefix sum of the values in
                // our W buckets

                

            }
            else {
            }
        }
        
        // ...
    }

    barrier(CLK_LOCAL_MEM_FENCE);

    // Finally, we can separate our examples onto each side of the two
    // buckets
    
    return bestSplit;
}
#endif
