/** randomforest_kernels.metal                                 -*- C++ -*-
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    OpenCL kernels for random forest algorithm.

    clang-6.0 -Xclang -finclude-default-header -cl-std=CL1.2 -Dcl_clang_storage_class_specifiers -target nvptx64-nvidia-nvcl -xcl plugins/jml/randomforest_kernels.cl -emit-llvm -cl-kernel-arg-info -S -o test.ll
*/

#include <metal_atomic>
#include <metal_stdlib>

#define __constant constant
#define __kernel kernel
#define __global device
#define __local threadgroup
#define W W32
#define barrier threadgroup_barrier
#define CLK_LOCAL_MEM_FENCE mem_flags::mem_threadgroup
#define CLK_GLOBAL_MEM_FENCE mem_flags::mem_device
#define printf(...) 

#define atom_add(x,y) atomic_fetch_add_explicit(x, y, memory_order_relaxed)
#define atom_sub(x,y) atomic_fetch_sub_explicit(x, y, memory_order_relaxed)
#define atom_or(x,y) atomic_fetch_or_explicit(x, y, memory_order_relaxed)
#define atom_inc(x) atomic_fetch_add_explicit(x, 1, memory_order_relaxed)
#define atom_dec(x) atomic_fetch_sub_explicit(x, 1, memory_order_relaxed)

#define get_global_id(n) global_id[n]
#define get_global_size(n) global_size[n]
#define get_local_id(n) local_id[n]
#define get_local_size(n) local_size[n]

using namespace metal;


typedef unsigned uint32_t;
typedef signed short int16_t;
typedef int int32_t;
typedef unsigned long uint64_t;
typedef long int64_t;
typedef signed char int8_t;
typedef unsigned char uint8_t;
typedef unsigned short uint16_t;

static const __constant float VAL_2_HL = 1.0f * (1UL << 30);
static const __constant float HL_2_VAL = 1.0f / (1UL << 30);
static const __constant float ADD_TO_ROUND = 0.5f / (1UL << 30);

// Used to measure the startup latency
__kernel void doNothingKernel(uint32_t x [[thread_position_in_grid]])
{
    //printf("did nothing ");
    //printf("%d\n", dummy);
}

enum WeightFormat : uint16_t {
    WF_INT_MULTIPLE,  ///< Stored as an int multiplied by a constant
    WF_TABLE,         ///< Stored as an index into a fixed table
    WF_FLOAT          ///< Stored directly as a 32 bit float
};

inline float decodeWeight(uint32_t bits, WeightFormat floatEncoding, float baseMultiplier,
                   __global const float * table)
{
    if (floatEncoding == WF_INT_MULTIPLE || true) {
        return bits * baseMultiplier;
    }
    else if (floatEncoding == WF_FLOAT) {
        return as_type<float>(bits);
    }
    else if (floatEncoding == WF_TABLE) {
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
    uint32_t wordNumber = bitNumber >> 6;
    uint32_t wordOffset = bitNumber - (wordNumber << 6);

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

#if 1
inline uint16_t extract16BitRange32(__global const uint32_t * data,
                                    uint16_t numBits,
                                    uint32_t entryNumber)
{
    uint32_t bitNumber = numBits * entryNumber;
    uint32_t wordNumber = bitNumber / 32;
    uint16_t wordOffset = bitNumber % 32;

    uint16_t bottomBits = min((uint16_t)numBits, uint16_t(32 - wordOffset));
    uint16_t topBits = numBits - bottomBits;

    if (topBits == 0) {
        return extract_bits(data[wordNumber], wordOffset, bottomBits);
    }
    else {
        uint2 words = *(const __global uint2 *)(data + wordNumber);
        uint16_t bottom = extract_bits(words[0], wordOffset, bottomBits);
        return bottom | extract_bits(words[1], 0, topBits) << bottomBits;
    }
}
#elif 0
inline uint32_t extract16BitRange32(__global const uint32_t * data,
                                  uint32_t numBits,
                                  uint32_t entryNumber)
{
    uint32_t bitNumber = numBits * entryNumber;
    uint32_t wordNumber = bitNumber / 32;
    uint32_t wordOffset = bitNumber % 32;

    uint32_t bottomBits = min(numBits, 32 - wordOffset);
    uint32_t topBits = numBits - bottomBits;

    uint32_t mask = createMask32(numBits);

    uint32_t val = data[wordNumber];

    val >>= wordOffset;

    if (topBits > 0) {
        uint32_t val2 = data[wordNumber + 1];
        val = val | val2 << bottomBits;
    }
    val = val & mask;
    return val;
}
#else
inline uint32_t extract16BitRange32(__global const uint32_t * data,
                                  uint32_t numBits,
                                  uint32_t entryNumber)
{
    uint32_t bitNumber = numBits * entryNumber;
    uint32_t wordNumber = bitNumber / 32;
    uint8_t wordOffset = bitNumber % 32;

    uint8_t bottomBits = min((uint8_t)numBits, uint8_t(32 - wordOffset));
    uint8_t topBits = numBits - bottomBits;

    if (topBits == 0) {
        return extract_bits(data[wordNumber], wordOffset, bottomBits);
    }

    uint32_t mask = createMask32(numBits);

    uint32_t val = data[wordNumber];

    val >>= wordOffset;

    if (topBits > 0) {
        uint32_t val2 = data[wordNumber + 1];
        val = val | val2 << bottomBits;
    }
    val = val & mask;
    return val;
}
#endif

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

inline uint16_t getBucket(uint32_t exampleNum,
                          __global const uint32_t * bucketData,
                          uint32_t bucketDataLength,
                          uint32_t bucketBits,
                          uint32_t numBuckets)
{
    return extract16BitRange32(bucketData, bucketBits, exampleNum);
}

typedef struct W32 {
    int32_t vals[2];
    int32_t count;
} W32;

typedef struct W32A {
    atomic_int vals[2];
    atomic_int count;
} W32A;

using WAtomic = W32A;

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

inline int32_t encodeW(float f)
{
    int32_t result = (f + ADD_TO_ROUND) * VAL_2_HL;
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

inline void incrementWLocal(__local W * wIn, bool label, float weight)
{
    auto w = (__local WAtomic *)wIn;
    int32_t inc = encodeW(weight);
    atom_add(&w->vals[label ? 1 : 0], inc);
    atom_inc(&w->count);
}

inline void decrementWLocal(__local W * wIn, bool label, float weight)
{
    auto w = (__local WAtomic *)wIn;
    int32_t inc = encodeW(weight);
    atom_sub(&w->vals[label ? 1 : 0], inc);
    atom_dec(&w->count);
}

inline void incrementWGlobal(__global W * wIn, bool label, float weight)
{
    auto w = (__global WAtomic *)wIn;
    int32_t inc = encodeW(weight);
    atom_add(&w->vals[label ? 1 : 0], inc);
    atom_inc(&w->count);
}

inline void incrementWOut(__global W * wOut_, __local const W * wIn)
{
    auto wOut = (__global WAtomic *)wOut_;
    if (wIn->count == 0)
        return;
    if (wIn->vals[0] != 0)
        atom_add(&wOut->vals[0], wIn->vals[0]);
    if (wIn->vals[1] != 0)
        atom_add(&wOut->vals[1], wIn->vals[1]);
    atom_add(&wOut->count,   wIn->count);
}

inline void decrementWOutGlobal(__global W * wOut_, __global const W * wIn)
{
    auto wOut = (__global WAtomic *)wOut_;
    if (wIn->count == 0)
        return;
    if (wIn->vals[0] != 0)
        atom_sub(&wOut->vals[0], wIn->vals[0]);
    if (wIn->vals[1] != 0)
        atom_sub(&wOut->vals[1], wIn->vals[1]);
    atom_sub(&wOut->count,   wIn->count);
}

// Take a bit-compressed representation of rows, and turn it into a
// decompressed version with one float per row (the sign gives the
// label, and the magnitude gives the weight).
//
// This is a 1D kernel over the array of rows, not very complicated

struct DecompressRowsKernelArgs {
    uint32_t rowDataLength;
    uint16_t weightBits;
    uint16_t exampleNumBits;
    uint32_t numRows;
    WeightFormat weightFormat;
    float weightMultiplier;
};

__kernel void
decompressRowsKernel(__constant DecompressRowsKernelArgs & args [[buffer(0)]],
                     __global const uint64_t * rowData [[buffer(1)]],
                     __global const float * weightData [[buffer(2)]],                     
                     __global float * decodedRowsOut [[buffer(3)]],
                     uint32_t id [[thread_position_in_grid]],
                     uint32_t n [[threads_per_grid]])
{
    uint32_t rowDataLength = args.rowDataLength;
    uint16_t weightBits = args.weightBits;
    uint16_t exampleNumBits = args.exampleNumBits;
    uint32_t numRows = args.numRows;
    WeightFormat weightFormat = args.weightFormat;
    float weightMultiplier = args.weightMultiplier;

    uint32_t totalBits = weightBits + exampleNumBits + 1;  // 1 is for the label
    //if (get_global_id(0) == 0) {
    //    printf("weightBits = %d exampleNumBits = %d totalBits = %d\n",
    //           weightBits, exampleNumBits, totalBits);
    //}

    uint64_t mask = createMask64(totalBits);
    uint32_t exampleMask = createMask32(exampleNumBits);
    uint32_t weightMask = createMask32(weightBits);
    uint32_t labelMask = (1 << (weightBits + exampleNumBits));

    for (uint32_t rowId = id;  rowId < numRows; rowId += n) {

        DecodedRow row = getDecodedRow(rowId, rowData, rowDataLength,
                      totalBits, weightBits, exampleNumBits, numRows,
                      weightFormat, weightMultiplier, weightData,
                      mask, exampleMask, weightMask, labelMask);
        
        //if (row.example != rowId) {
        //    printf("non-consecutive example numbers: %d vs %d, exampleNumBits=%d\n", row.example, rowId, exampleNumBits);
        //}

        float encoded = row.label ? -row.weight : row.weight;
        
        //if (rowId < 128 && false) {
        //    printf("row %d ex %d wt %f lb %d encoded %f\n",
        //           rowId, rowId, row.weight, row.label, encoded);
        //}
        
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
                               
                               __global const uint32_t * activeFeatureList,
                               
                               __global uint16_t * featuresOut,
                               __global const uint32_t * featureDataOffsets)
{
    uint32_t fidx = get_global_id(1);
    uint32_t f = activeFeatureList[fidx];

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
    uint16_t bucket
        = getBucket(exampleNum, bucketData, bucketDataLength,
                    bucketBits, numBuckets);

#if 0
    if (bucket >= numBuckets) {
        printf("ERROR BUCKET NUMBER: got %d numBuckets %d row %d feature %d\n",
               bucket, numBuckets, rowId, (int)get_global_id(0));
        return 0;
    }
#endif

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

struct TestFeatureArgs {
    uint32_t numRows;
    uint32_t maxLocalBuckets;
};

__kernel void
testFeatureKernel(__constant const TestFeatureArgs & args,
                  __global const float * decodedRows,
 
                  __global const uint32_t * bucketData,
                  __global const uint32_t * bucketDataOffsets,
                  __global const uint32_t * bucketNumbers,
                  __global const uint32_t * bucketEntryBits,
 
                  __global const uint32_t * activeFeatureList,
 
                  __local W * w,
                  __global W * partitionBuckets,
 
                  uint2 global_id [[thread_position_in_grid]],
                  uint2 global_size [[threads_per_grid]],
                  uint2 local_id [[thread_position_in_threadgroup]],
                  uint2 local_size [[threads_per_threadgroup]])
{
    const uint32_t numRows = args.numRows;
    uint16_t maxLocalBuckets = args.maxLocalBuckets;
    const uint32_t workerId = get_local_id(1);
    const uint32_t fidx = get_global_id(0);
    const uint32_t f = activeFeatureList[fidx];

    uint32_t bucketDataOffset = bucketDataOffsets[f];
    uint32_t bucketDataLength = bucketDataOffsets[f + 1] - bucketDataOffset;
    uint32_t numBuckets = bucketNumbers[f + 1] - bucketNumbers[f];
    bucketData += bucketDataOffset;
    uint32_t bucketBits = bucketEntryBits[f];

    __global W * wOut = partitionBuckets + bucketNumbers[f];

    //maxLocalBuckets = 0;  // TODO DEBUG ONLY

    for (uint32_t i = workerId;  i < numBuckets && i < maxLocalBuckets;  i += get_local_size(1)) {
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

#define INDEXED_PARTITION_SPLIT_INIT { INFINITY, -1, -1, W_INIT, W_INIT, 0 }

inline bool partitionSplitDirection(PartitionSplit split)
{
    return split.feature != -1 && split.left.count < split.right.count;
}

inline bool indexedPartitionSplitDirection(IndexedPartitionSplit split)
{
    return split.feature != -1 && split.left.count < split.right.count;
}

inline bool partitionSplitDirectionGlobal(__global const PartitionSplit * split)
{
    return partitionSplitDirection(*split);
}

#if 0
__kernel void
fillPartitionSplitsKernel(__global PartitionSplit * splits)
{
    uint32_t n = get_global_id(0);
    //printf("filling partition %d at %ld\n",
    //       n, (long)(((__global char *)(splits + n)) - (__global char *)splits));
    PartitionSplit spl = PARTITION_SPLIT_INIT;
    splits[n] = spl;
}
#endif

inline float nextFloat(float val, int32_t n)
{
    int32_t ival = as_type<int32_t>(val);
    ival += n;
    return as_type<float>(ival);
}

inline int32_t floatAsInt(float val)
{
    return as_type<int32_t>(val);
}

template<typename AdjPtr>
inline float sqrt2(float val, AdjPtr adj)
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
    constexpr float SQRT3_MULT_BEFORE = 65536.0f;
    constexpr float SQRT3_MULT_AFTER  = 1.0f / 256.0f;

    val *= SQRT3_MULT_BEFORE;
    float approx = nextFloat(sqrt(val), 3);
    while (approx * approx > val) {
        approx = nextFloat(approx, -1);
    }
    return approx * SQRT3_MULT_AFTER;
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
void prefixSumW(__local WIndexed * w, uint32_t n, uint16_t threadIdx, uint16_t numThreads)
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
        for (uint32_t i = threadIdx;  i < n / 2;  i += numThreads) {
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
uint32_t argMinScanW(__local WIndexed * w, uint32_t n, W wAll, uint16_t threadIdx, uint16_t numThreads)
{
    for (uint32_t iter = 0, blockSize = 2;  blockSize < n * 2;  blockSize *= 2, iter += 1) {
        // If we need more than one round from our warp to process all elements,
        // then do  it
        for (uint32_t i = threadIdx;  i < n / 2;  i += numThreads) {
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
                         uint32_t wLocalSize,
                         uint32_t bucket,
                         uint16_t f,
                         uint16_t p)
{
    PartitionSplit result = PARTITION_SPLIT_INIT;

    //uint32_t bucket = get_global_id(0);

    // For now, we serialize on a single thread
    if (bucket != 0)
        return result;

    // Now check our result
    
    //uint32_t f = get_global_id(1);
    //uint32_t p = get_global_id(2);

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

#if 0
    if (p == 0 && false) {
        printf("real wBest %d (%f,%f,%d) %f\n",
               bestSplit,
               decodeW(bestLeft.vals[0]),
               decodeW(bestLeft.vals[1]),
               bestLeft.count,
               bestScore);
    }
#endif

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
                             W wAll,
                             uint32_t bucket,
                             uint16_t f)
{
    PartitionSplit result = PARTITION_SPLIT_INIT;

    //uint32_t bucket = get_global_id(0);

    // For now, we serialize on a single thread
    if (bucket != 0)
        return result;

    //uint32_t f = get_global_id(1);

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
            uint16_t threadIdx, uint16_t numThreads,
            uint16_t f,
            uint16_t p);

// Function that uses a single workgroup to scan all of the buckets in a split
PartitionSplit
chooseSplit(__global const W * w,
            uint32_t numBuckets,
            W wAll,
            __local WIndexed * wLocal,
            uint32_t wLocalSize,
            __local WIndexed * wStartBest,  // length 2
            bool ordinal,
            uint16_t threadIdx, uint16_t numThreads,
            uint16_t f,
            uint16_t p)
{
    PartitionSplit result = PARTITION_SPLIT_INIT;

    //uint32_t f = get_global_id(1);
    //uint32_t p = get_global_id(2);

    // We need some weight in both true and false for a split to make sense
    if (wAll.vals[0] == 0 || wAll.vals[1] == 0)
        return result;
    
    // Read our W array into shared memory
    
    // If we have more W values than local buckets, we need to
    // do the prefix sum multiple times to fill it up completely

    // Contains the prefix sum from the previous iteration
    auto wStart = wStartBest + 0;

    // Contains the best split from the previous iteration
    auto wBest = wStartBest + 1;
    
    if (threadIdx == 0) {
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
 
    for (uint32_t startBucket = 0;  startBucket < numBuckets;  startBucket += wLocalSize) {

        //if (startBucket != 0) {
        //    printf("f %d p %d startBucket %d\n",
        //           f, p, startBucket);
        //}
        
        uint32_t endBucket = min(startBucket + wLocalSize, numBuckets);
        uint32_t numBucketsInIteration = endBucket - startBucket;
        
        for (uint32_t i = threadIdx;  i < numBucketsInIteration;  i += numThreads) {
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
            prefixSumW(wLocal, numBucketsInIteration, threadIdx, numThreads);

            // Seed for the next one
            *wStart = wLocal[numBucketsInIteration - 1];
        }

        // And then scan that to find the index of the best score
        argMinScanW(wLocal, numBucketsInIteration, wAll, threadIdx, numThreads);

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

struct GetPartitionSplitsArgs {
    uint32_t totalBuckets;
    uint32_t numActivePartitions;
    uint32_t wLocalSize;
};

#if 1
// this expects [bucket, feature, partition]
__kernel void
getPartitionSplitsKernel(__constant const GetPartitionSplitsArgs & args,
                         __global const uint32_t * bucketNumbers, // [nf]
                         
                         __global const uint32_t * activeFeatureList, // [naf]
                         __global const uint32_t * featureIsOrdinal, // [nf]
                         
                         __global const W * buckets, // [np x totalBuckets]

                         __global const W * wAll,  // one per partition
                         __global PartitionSplit * featurePartitionSplitsOut, // [np x nf]

                         __local WIndexed * wLocal,  // [wLocalSize]
                         //__local WIndexed * wStartBest, //[2]

                         uint3 global_id [[thread_position_in_grid]],
                         uint3 global_size [[threads_per_grid]],
                         uint3 local_id [[thread_position_in_threadgroup]],
                         uint3 local_size [[threads_per_threadgroup]])
{
    const uint32_t totalBuckets = args.totalBuckets;
    const uint32_t numActivePartitions = args.numActivePartitions;
    const uint32_t wLocalSize = args.wLocalSize;

    const uint32_t bucket = get_global_id(0);
    const uint16_t fidx = get_global_id(1);
    const uint16_t naf = get_global_size(1);
    const uint16_t partition = get_global_id(2);
    
    if (partition >= numActivePartitions)
        return;

    const PartitionSplit NONE = PARTITION_SPLIT_INIT;
    PartitionSplit best = NONE;

    // Don't do inactive features
    if (wAll[partition].count == 0) {
        if (bucket == 0) {
            //printf("writing out inactive PartitionSplit part %d f %d idx %x\n", partition, f, partition * nf + f);
            featurePartitionSplitsOut[partition * naf + fidx] = best;            
        }
        return;
    }
    
    const uint16_t f = activeFeatureList[fidx];

    uint32_t bucketStart = bucketNumbers[f];
    uint32_t bucketEnd = bucketNumbers[f + 1];
    uint32_t numBuckets = bucketEnd - bucketStart;

#if 0
    if (numBuckets == 0) {
        printf("ERROR: zero buckets for part %d feat %d\n", partition, f);
    }
#endif

    // We dimension as the feature with the highest number of buckets, so
    // for most of them we have to many and can stop
    if (bucket >= numBuckets)
        return;


    // Find where our bucket data starts
    __global const W * myW
        = buckets
        + (totalBuckets * partition)
        + bucketStart;
    
    bool ordinal = featureIsOrdinal[f];

#if 1
    __local WIndexed wStartBest[2];
    best = chooseSplit(myW, numBuckets, wAll[partition],
                       wLocal, wLocalSize, wStartBest, ordinal,
                       get_local_id(0), get_local_size(0), f, partition);
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
                                        wLocal, wLocalSize, bucket, f, partition);
    }
    else {
        // We have a simple search which is independent per-bucket.
        best = chooseSplitKernelCategorical(myW, numBuckets, wAll[partition], bucket, f);
    }
#endif
    
    if (bucket == 0) {
        if (best.score == INFINITY) {
            best = NONE;
        }
        //printf("writing out active PartitionSplit part %d f %d idx %x score %f\n", partition, f, partition * nf + f, best.score);
        featurePartitionSplitsOut[partition * naf + fidx] = best;
    }

}
#endif

typedef struct PartitionIndex {
    uint32_t index;
} PartitionIndex;

struct BestPartitionSplitArgs {
    uint32_t numActiveFeatures;
    uint32_t partitionSplitsOffset;
    uint16_t depth;
};

// id 0: partition number
__kernel void
bestPartitionSplitKernel(__constant const BestPartitionSplitArgs & args,
                         __global const uint32_t * activeFeatureList, // [numActiveFeatures]
                         __global const PartitionSplit * featurePartitionSplits,
                         __global const PartitionIndex * partitionIndexes,
                         __global IndexedPartitionSplit * allPartitionSplitsOut,
                         uint2 global_id [[thread_position_in_grid]],
                         uint2 global_size [[threads_per_grid]],
                         uint2 local_id [[thread_position_in_threadgroup]],
                         uint2 local_size [[threads_per_threadgroup]])
{
    const uint16_t numActiveFeatures = args.numActiveFeatures;
    const uint16_t depth = args.depth;

    allPartitionSplitsOut += args.partitionSplitsOffset;

    uint32_t p = get_global_id(0);

    featurePartitionSplits += p * numActiveFeatures;
    allPartitionSplitsOut += p;

    IndexedPartitionSplit best = INDEXED_PARTITION_SPLIT_INIT;
    best.index = (depth == 0 ? 1 : partitionIndexes[p].index);
    //printf("partition %d has index %d\n", p, best.index);
    if (best.index == 0) {
        *allPartitionSplitsOut = best;
        return;
    }
    
    //printf("bestPartitionSplitKernel for partition %d\n", p);

    // TODO: with lots of features this will become slow; we can have a workgroup cooperate
    for (uint32_t fidx = 0;  fidx < numActiveFeatures;  ++fidx) {
        //printf("feature %d active %d split %d score %f\n", f, activeFeatureList[f], featurePartitionSplits[f].value, featurePartitionSplits[f].score);
        if (featurePartitionSplits[fidx].value < 0)
            continue;
        //printf("doing feature %d\n", f);
        if (featurePartitionSplits[fidx].score < best.score) {
            best.score = featurePartitionSplits[fidx].score;
            best.feature = featurePartitionSplits[fidx].feature;
            best.value = featurePartitionSplits[fidx].value;
            best.left = featurePartitionSplits[fidx].left;
            best.right = featurePartitionSplits[fidx].right;
        }
    }

    //printf("partition %d offset %d feature %d value %d score %f\n", p, best.feature, best.value, best.score);

    *allPartitionSplitsOut = best;
}

typedef struct PartitionInfo {
    int32_t left;
    int32_t right;
} PartitionInfo;

inline bool discardSplit(IndexedPartitionSplit split)
{
    return split.feature == -1
        || ((split.left.vals[0] == 0 || split.left.vals[1] == 0)
             && (split.right.vals[0] == 0 || split.right.vals[1] == 0));
}

inline int32_t indexDepth(uint32_t index)
{
    return index == 0 ? -1 : 31 - clz(index);
}

inline uint32_t leftChildIndex(uint32_t parent)
{
    return parent + (1 << indexDepth(parent));
}

inline uint32_t rightChildIndex(uint32_t parent)
{
    return parent + (2 << indexDepth(parent));
}

// one instance
// SmallSideIndex[numPartitionsOut]:
// Used to a) know when we need to clear partitions (those on the small side are cleared,
// those on the big side are not) and b) provide a local index for partition buckets for
// the UpdateBuckets kernel
// - 0 means it's not on the small side
// - 1-254 means we're partition number (n-1) on the small side
// - 255 means we're partition number 254 or greater on the small side
struct AssignPartitionNumbersArgs {
    uint32_t partitionSplitsOffset;
    uint32_t numActivePartitions;
    uint32_t maxNumActivePartitions;
};

__kernel void
assignPartitionNumbersKernel(__constant const AssignPartitionNumbersArgs & args,
                             __global const IndexedPartitionSplit * allPartitionSplits,
                             __global PartitionIndex * partitionIndexesOut,
                             __global PartitionInfo * partitionInfoOut,
                             __global uint8_t * smallSideIndexesOut,
                             __global uint16_t * smallSideIndexToPartitionOut,
                             __global uint32_t * numActivePartitionsOut,
                             ushort lane_id [[thread_index_in_simdgroup]],
                             ushort num_lanes [[threads_per_simdgroup]])
{
    const uint16_t numActivePartitions = args.numActivePartitions;
    const uint16_t maxNumActivePartitions = args.maxNumActivePartitions;

    constexpr uint16_t inactivePartitionsLength = 16300;
    __local uint16_t inactivePartitions[inactivePartitionsLength];

    allPartitionSplits += args.partitionSplitsOffset;

#if 1
    __local uint32_t numInactivePartitions;
    __local uint32_t numSmallSideRows;

    if (lane_id == 0) {
        numInactivePartitions = 0;
        numSmallSideRows = 0;
    }

    barrier(CLK_LOCAL_MEM_FENCE);

    for (uint16_t i = 0;  i < numActivePartitions;  i += num_lanes) {
        uint16_t p = i + lane_id;

        bool discard = false;

        if (p < numActivePartitions) {
            IndexedPartitionSplit split = allPartitionSplits[p];
            discard = discardSplit(split);
        }

        uint32_t discardWhich = (simd_vote::vote_t)simd_ballot(discard);
        uint32_t numDiscarded = popcount(discardWhich);

        if (numDiscarded + numInactivePartitions >= inactivePartitionsLength) {
            // OVERFLOW
            numActivePartitionsOut[0] = -1;
            return;
        }

        uint32_t base = numInactivePartitions;
        uint32_t index = base + simd_prefix_exclusive_sum((uint16_t)discard);

        if (p >= numActivePartitions)
            break;

        if (discard) {
            smallSideIndexesOut[p] = 255;
            inactivePartitions[index] = p;
        }
        else {
            smallSideIndexesOut[p] = 0;
        }

        numInactivePartitions += numDiscarded;
    }

    barrier(CLK_LOCAL_MEM_FENCE);  // for when we use more than one SIMD group

    __local uint32_t n, n2, skippedRows, skippedPartitions, ssi;

    if (lane_id == 0) {
        n = skippedRows = skippedPartitions = ssi = 0;
        n2 = numActivePartitions;
    }

    for (uint16_t i = 0;  i < numActivePartitions;  i += num_lanes) {
        uint16_t p = i + lane_id;
        __global PartitionInfo * info = partitionInfoOut + p;

        bool discard = true;
        bool direction = false;
        uint32_t partitionSmallSideRows = 0;
        IndexedPartitionSplit split;

        if (p < numActivePartitions) {
            split = allPartitionSplits[p];
            discard = discardSplit(split);
            if (discard) {
                info->left = info->right = -1;
            }
            else {
                partitionSmallSideRows = min(split.left.count, split.right.count);
                direction = indexedPartitionSplitDirection(split);
            }
        }

        uint32_t smallSideRowsIncrement = simd_sum(partitionSmallSideRows);

        if (simd_is_first()) {
            atom_add((__local atomic_uint *)&numSmallSideRows, smallSideRowsIncrement);
        }

        if (!discard) {
            bool skipped = false;
            uint32_t minorPartitionIndex = atom_add((__local atomic_uint *)&n, 1);
            uint32_t minorPartitionNumber;
            if (minorPartitionIndex < numInactivePartitions) {
                minorPartitionNumber = inactivePartitions[minorPartitionIndex];
            }
            else {
                minorPartitionNumber = atom_add((__local atomic_uint *)&n2, 1);
            }
            if (minorPartitionNumber >= maxNumActivePartitions) {
                skipped = true;
            }
            else {
                // Attempt to allocate a small side number, and if it's possible record the
                // mapping.

                uint32_t idx = 1 + atom_add((__local atomic_uint *)&ssi, 1);
                if (idx < 255) {
                    smallSideIndexesOut[minorPartitionNumber] = idx;
                    smallSideIndexToPartitionOut[idx] = minorPartitionNumber;
                }
                else {
                    smallSideIndexesOut[minorPartitionNumber] = 255;
                }
            }

            if (skipped) {
                info->left = -1;
                info->right = -1;
                atom_add((__local atomic_uint *)&skippedRows, split.left.count + split.right.count);
                atom_add((__local atomic_uint *)&skippedPartitions, 1);
            }
            else {
                if (direction == 0) {
                    info->left = p;
                    info->right = minorPartitionNumber;
                }
                else {
                    info->left = minorPartitionNumber;
                    info->right = p;
                }

                //printf("partition %d direction %d minor %d n %d n2 %d\n", p, direction, minorPartitionNumber, n, n2);

                partitionIndexesOut[info->left].index = leftChildIndex(split.index);
                partitionIndexesOut[info->right].index = rightChildIndex(split.index);
            }
        }
    }

    if (simd_is_first()) {
        uint32_t nap = min(n2, (uint32_t)maxNumActivePartitions);
        numActivePartitionsOut[0] = nap;
        numActivePartitionsOut[1] = numSmallSideRows;

        // Clear partition indexes for gap partitions
        for (; n < numInactivePartitions;  ++n) {
            uint32_t part = inactivePartitions[n];
            smallSideIndexesOut[part] = 0;
            partitionIndexesOut[part].index = 0;
        }
        for (; ssi < 255 && ssi < nap/2;  ++ssi) {
            smallSideIndexToPartitionOut[ssi+1] = 0;
        }
    }
#else
    if (lane_id != 0)
        return;

    uint32_t numInactivePartitions = 0;
    uint32_t numSmallSideRows = 0;

    for (uint32_t p = 0;  p < numActivePartitions;  ++p) {
        IndexedPartitionSplit split = allPartitionSplits[p];

        if (discardSplit(split)) {

            if (numInactivePartitions == inactivePartitionsLength) {
                // OVERFLOW
                numActivePartitionsOut[0] = -1;
                return;
            }
            smallSideIndexesOut[p] = 255;
            inactivePartitions[numInactivePartitions++] = p;
        }
        else {
            smallSideIndexesOut[p] = 0;
        }
    }

    //printf("numActivePartitions=%d, numInactivePartitions=%d\n", numActivePartitions, numInactivePartitions);

    uint32_t n = 0, n2 = numActivePartitions;
    uint32_t skippedRows = 0, skippedPartitions = 0;
    uint16_t ssi = 0;

    for (uint32_t p = 0;  p < numActivePartitions;  ++p) {
        IndexedPartitionSplit split = allPartitionSplits[p];
        __global PartitionInfo * info = partitionInfoOut + p;


        if (discardSplit(split)) {
            info->left = info->right = -1;
            continue;
        }

        numSmallSideRows += min(split.left.count, split.right.count);

        uint32_t direction = indexedPartitionSplitDirection(split);
        // both still valid.  One needs a new partition number
        uint32_t minorPartitionNumber;

        if (n < numInactivePartitions) {
            minorPartitionNumber = inactivePartitions[n++];
        }
        else if (n2 < maxNumActivePartitions) {
            minorPartitionNumber = n2++;
        }
        else {
            // Max width reached
            skippedRows += split.left.count + split.right.count;
            skippedPartitions += 1;
            info->left = -1;
            info->right = -1;
            continue;
        }

        // Attempt to allocate a small side number, and if it's possible record the
        // mapping.
        if (ssi < 254) {
            uint16_t idx = ++ssi;
            smallSideIndexesOut[minorPartitionNumber] = idx;
            smallSideIndexToPartitionOut[idx] = minorPartitionNumber;
        }
        else {
            smallSideIndexesOut[minorPartitionNumber] = 255;
        }

        if (direction == 0) {
            info->left = p;
            info->right = minorPartitionNumber;
        }
        else {
            info->left = minorPartitionNumber;
            info->right = p;
        }

        //printf("partition %d direction %d minor %d n %d n2 %d\n", p, direction, minorPartitionNumber, n, n2);

        partitionIndexesOut[info->left].index = leftChildIndex(split.index);
        partitionIndexesOut[info->right].index = rightChildIndex(split.index);
    }


    numActivePartitionsOut[0] = n2;
    numActivePartitionsOut[1] = numSmallSideRows;

    // Clear partition indexes for gap partitions
    for (; n < numInactivePartitions;  ++n) {
        uint32_t part = inactivePartitions[n];
        smallSideIndexesOut[part] = 0;
        partitionIndexesOut[part].index = 0;
    }
#endif

}

struct UpdateWorkEntry {
    uint32_t row;
    uint16_t partition;
    uint16_t smallSideIndex;
    float decodedRow;
};

struct ClearBucketsArgs {
    uint32_t numActiveBuckets;
};

// [partition, bucket]
__kernel void
clearBucketsKernel(__constant const ClearBucketsArgs & args,
                   __global W * bucketsOut,
                   __global W * wAllOut,
                   __global uint32_t * numNonZeroDirectionIndices,
                   __constant const uint8_t * smallSideIndexes,
                   uint2 global_id [[thread_position_in_grid]],
                   uint2 global_size [[threads_per_grid]],
                   uint2 local_id [[thread_position_in_threadgroup]],
                   uint2 local_size [[threads_per_threadgroup]])
{
    //uint32_t numPartitions = get_global_size(0);
    
    // Clear the number of non zero directions
    if (get_global_id(0) == 0 && get_global_id(1) == 0) {
        numNonZeroDirectionIndices[0] = 0;
    }

    uint32_t partition = get_global_id(0);
    if (!smallSideIndexes[partition])
        return;

    uint32_t bucket = get_global_id(1);

    if (bucket >= args.numActiveBuckets)
        return;
    
    __global W * buckets
        = bucketsOut + partition * args.numActiveBuckets;
    
    const W empty = W_INIT;
    
    buckets[bucket] = empty;
    if (bucket == 0) {
        wAllOut[partition] = empty;
    }
}


// First part: per row, accumulate
// - weight
// - label
// - from
// - to

// Structure for a partition number
typedef struct {
    int16_t num;  // partition number
} RowPartitionInfo;



struct UpdatePartitionNumbersArgs {
    uint32_t partitionSplitsOffset;
    uint32_t numRows;
    uint16_t numActivePartitions;
    uint16_t depth;
};

// Second part: per feature plus wAll, transfer examples
//[rowNumber]
__kernel void
//__attribute__((reqd_work_group_size(256,1,1)))
updatePartitionNumbersKernel(__constant const UpdatePartitionNumbersArgs & args,
                             __global RowPartitionInfo * partitions,
                             __global uint32_t * directions,
                             __global uint32_t * numNonZeroDirectionIndices,
                             __global UpdateWorkEntry * nonZeroDirectionIndices,
                             __constant const uint8_t * smallSideIndexes,

                             __global const IndexedPartitionSplit * allPartitionSplits,
                             __global const PartitionInfo * partitionInfo,

                             __global const uint32_t * bucketData,
                             __constant const uint32_t * bucketDataOffsets,
                             __constant const uint32_t * bucketNumbers,
                             __constant const uint32_t * bucketEntryBits,
                             __constant const uint32_t * featureIsOrdinal,

                             __global const float * decodedRows,

                             uint2 global_id [[thread_position_in_grid]],
                             uint2 global_size [[threads_per_grid]],
                             ushort2 local_id [[thread_position_in_threadgroup]],
                             ushort2 local_size [[threads_per_threadgroup]])
{
    uint16_t depth = args.depth;
    uint32_t numRows = args.numRows;
    uint16_t numActivePartitions = args.numActivePartitions;

    // Most rows live in partitions with a number < 256; avoid main memory accesses for these
    // by caching in local memory
    constexpr uint16_t SSI_CACHE_SIZE=1024;
    __local uint16_t smallSideIndexesCache[SSI_CACHE_SIZE];

    // Populate the cache of smallSideIndexes
    for (uint32_t b = get_local_id(0);  b < SSI_CACHE_SIZE && b < numActivePartitions; b += get_local_size(0)) {
        smallSideIndexesCache[b] = smallSideIndexes[b];
    }

    // Wait for all buckets to be clear
    barrier(CLK_LOCAL_MEM_FENCE);

    allPartitionSplits += args.partitionSplitsOffset;

    //uint16_t numSimdGroups = get_local_size(0) / 32;
    //uint16_t simdGroupNum = get_local_id(0) / 32;
    //uint16_t simdLaneNum = get_local_id(0) % 32;

    //__local uint32_t work_group_directions[32];  // 32 * 32 = 1024 bits at once, which is the SIMD width

    __global atomic_uint * nonZeroDirectionCount = (__global atomic_uint *)numNonZeroDirectionIndices;
    __global packed_uint3 * nonZeroDirections = (__global packed_uint3 *)nonZeroDirectionIndices;

    for (uint32_t i = 0;  i < numRows;  i += get_global_size(0)) {
        uint32_t r = i + get_global_id(0);

        uint16_t storedPartition = r < numRows ? partitions[r].num : 0;
        uint16_t oldPartition = depth == 0 ? 0 : storedPartition;
        uint16_t partition = oldPartition;

        if (r < numRows && partition != (uint16_t)-1) {
            PartitionInfo info = partitionInfo[partition];

            if (info.left == -1 || info.right == -1) {
                partition = -1;
            }
            else {
                int16_t splitFeature = allPartitionSplits[partition].feature;
                uint16_t splitValue = allPartitionSplits[partition].value;
                uint16_t ordinal = featureIsOrdinal[splitFeature];

                // Split feature values go here
                uint32_t splitBucketDataOffset = bucketDataOffsets[splitFeature];
                //uint32_t splitBucketDataLength = bucketDataOffsets[splitFeature + 1] - splitBucketDataOffset;
                //uint32_t splitNumBuckets = bucketNumbers[splitFeature + 1] - bucketNumbers[splitFeature];
                __global const uint32_t * splitBucketData = bucketData + splitBucketDataOffset;
                uint16_t splitBucketBits = bucketEntryBits[splitFeature];

                uint16_t bucket = getBucket(r,
                                            splitBucketData, 0 /*splitBucketDataLength*/,
                                            splitBucketBits, 0 /*splitNumBuckets*/);
                
                uint16_t side = ordinal ? bucket >= splitValue : bucket != splitValue;

                // Set the new partition number
                uint16_t newPartitionNumber = side ? info.right : info.left;
                partition = newPartitionNumber;
            }
        }

        bool direction = r < numRows && partition != oldPartition && partition != (uint16_t)-1;
        if (r < numRows && partition != storedPartition)
            partitions[r].num = partition;

        //if (r + 32 >= numRows)
        //    break;

        uint32_t simdDirections = (simd_vote::vote_t)simd_ballot(direction);

        //if (simd_is_first() && (r / 32) * 32 < numRows) {
        //    directions[r/32] = simdDirections;
        //}

        uint16_t numDirections = popcount(simdDirections);
        if (numDirections == 0)
            continue;

        uint32_t nonZeroDirectionBase = 0;
        if (simd_is_first()) {
            nonZeroDirectionBase = atomic_fetch_add_explicit(nonZeroDirectionCount, numDirections, memory_order_relaxed);
        }

        nonZeroDirectionBase = simd_broadcast_first(nonZeroDirectionBase);

        uint16_t n = simd_prefix_exclusive_sum((uint16_t)(direction != 0));

        if (direction) {
            uint16_t smallPartitionIndex = partition < SSI_CACHE_SIZE ? smallSideIndexesCache[partition] : smallSideIndexes[partition];
            nonZeroDirections[nonZeroDirectionBase + n] = packed_uint3{ r, partition | (((uint32_t)smallPartitionIndex) << 16), as_type<uint32_t>(decodedRows[r]) };
        }
    }

    barrier(CLK_GLOBAL_MEM_FENCE);
}

struct UpdateBucketsArgs {
    uint32_t numActiveBuckets;
    uint32_t numActivePartitions;
    uint32_t numRows;
    uint32_t maxLocalBuckets;  
};

__kernel void // [row, feature]
//__attribute__((reqd_work_group_size(256,1,1)))
updateBucketsKernel(__constant const UpdateBucketsArgs & args,
                    __global const RowPartitionInfo * partitions,
                    __global uint32_t * directions,
                    __global const uint32_t * numNonZeroDirectionIndices,
                    __global UpdateWorkEntry * nonZeroDirectionIndices,
                    __global W * buckets,
                    __global W * wAll,
                    __constant const uint8_t * smallSideIndexes,
                    __constant const uint16_t * smallSideIndexToPartition,

                    // Row data
                    __global const float * decodedRows,
                    
                    // Feature data
                    __global const uint32_t * bucketData,
                    __global const uint32_t * bucketDataOffsets,
                    __global const uint32_t * bucketNumbers,
                    __global const uint32_t * bucketEntryBits,

                    __global const uint32_t * activeFeatureList,
                    __global const uint32_t * featureIsOrdinal,

                    __local W * wLocal,

                    uint2 global_id [[thread_position_in_grid]],
                    uint2 global_size [[threads_per_grid]],
                    ushort2 local_id [[thread_position_in_threadgroup]],
                    ushort2 local_size [[threads_per_threadgroup]],
                    ushort lane_id [[thread_index_in_simdgroup]])
{
    int16_t fidx = get_global_id(1) - 1;  // -1 means wAll, otherwise it's the feature number
    const uint16_t numActivePartitions = args.numActivePartitions;
    const uint32_t numRows = args.numRows;
    const uint16_t maxLocalBuckets = args.maxLocalBuckets;
    const uint32_t numActiveBuckets = args.numActiveBuckets;

    int16_t f = fidx == -1 ? -1 : activeFeatureList[fidx];

    // We have to set up to access two different features:
    // 1) The feature we're splitting on for this example (splitFeature)
    // 2) The feature we're updating for the split (f)
    // Thus, we need to perform the same work twice, once for each of the
    // features.
    
    uint16_t numBucketsPerPartition;        // How many buckets for this feature?
    uint16_t bucketBits;
    uint16_t numLocalBuckets;
            
    // Pointer to the global array we eventually want to update
    __global W * wGlobal;

    if (f == -1) {
        numBucketsPerPartition = 1;
        wGlobal = wAll;
        numLocalBuckets = min(uint16_t(numActivePartitions / 2), maxLocalBuckets);
        numLocalBuckets = min(numLocalBuckets, (uint16_t)254);
    }
    else {
        uint32_t bucketDataOffset = bucketDataOffsets[f];
        numBucketsPerPartition = bucketNumbers[f + 1] - bucketNumbers[f];
        bucketData = bucketData + bucketDataOffset;
        bucketBits = bucketEntryBits[f];
        numLocalBuckets = min((uint32_t)numBucketsPerPartition * numActivePartitions / 2, (uint32_t)maxLocalBuckets);
        wGlobal = buckets + bucketNumbers[f];
    }

    // We call getBucket, but since our stride is the workgroup width (which is always a multiple of 32),
    // we always use the same bit number and mask.  This code allows us to avoid most of the logic by
    // pre-computing the constants.

    // Most rows live in partitions with a number < 256; avoid main memory accesses for these
    // by caching in local memory
    constexpr uint16_t SSI_CACHE_SIZE=256;
    __local uint16_t smallSideIndexesCache[SSI_CACHE_SIZE];

    // Clear our local accumulation buckets to get started
    for (uint32_t b = get_local_id(0);  b < numLocalBuckets; b += get_local_size(0)) {
        zeroW(wLocal + b);
    }

    // Populate the cache of smallSideIndexes
    for (uint32_t b = get_local_id(0);  b < SSI_CACHE_SIZE && b < numActivePartitions; b += get_local_size(0)) {
        smallSideIndexesCache[b] = smallSideIndexes[b];
    }

    // Wait for all buckets to be clear
    barrier(CLK_LOCAL_MEM_FENCE);
    
    //numLocalBuckets = 0;  // DEBUG DO NOT COMMIT

    uint32_t numNonZero = numNonZeroDirectionIndices[0];
    __global const packed_uint3 * nonZeroDirections = (__global const packed_uint3 *)(nonZeroDirectionIndices);

    for (uint32_t i = get_global_id(0);  i < numNonZero;  i += get_global_size(0)) {

        packed_uint3 d = nonZeroDirections[i];
        uint32_t r = d[0];
        uint16_t partition = d[1] & 0xffff;
        uint16_t smallPartitionIndex = d[1] >> 16;
        float decodedRow = as_type<float>(d[2]);

        float weight = fabs(decodedRow);
        bool label = decodedRow < 0;

        uint32_t toBucketLocal;
        uint32_t toBucketGlobal;
        //uint8_t smallPartitionIndex = partition < 256 ? smallSideIndexesCache[partition] : smallSideIndexes[partition];
        
        if (f == -1) {
            // Since we don't touch the buckets on the left side of the
            // partition, we don't need to have local accumulators for them.
            // Hence, the partition number for local is the left partition.
            if (smallPartitionIndex == 255)
                toBucketLocal = -1;
            else
                toBucketLocal = smallPartitionIndex - 1;
            toBucketGlobal = partition;
        }
        else {
            uint16_t bucket = getBucket(r /*exampleNum*/,
                                        bucketData, 0 /* bucketDataLength */,
                                        bucketBits, 0 /* numBucketsPerPartition */);
            toBucketGlobal = partition * numActiveBuckets + bucket;
            if (smallPartitionIndex == 255)
                toBucketLocal = -1;
            else
                toBucketLocal = (smallPartitionIndex - 1) * numBucketsPerPartition + bucket;
        }

        if (toBucketLocal < numLocalBuckets) {
            //atom_inc(&numLocalUpdates);
            incrementWLocal(wLocal + toBucketLocal, label, weight);
        }
        else {
            //atom_inc(&numGlobalUpdates);
            incrementWGlobal(wGlobal + toBucketGlobal, label, weight);
        }
    }

    // Wait for all buckets to be updated, before we write them back to the global mem
    barrier(CLK_LOCAL_MEM_FENCE);
    
    // Now write our local changes to the global
    for (uint32_t b = get_local_id(0);  b < numLocalBuckets;  b += get_local_size(0)) {
        if (wLocal[b].count != 0) {
            //++numCopyLocalToGlobal;
            if (f == -1) {
                uint32_t bucketNumberGlobal = smallSideIndexToPartition[b + 1];
                incrementWOut(wGlobal + bucketNumberGlobal, wLocal + b);
                continue;
            }

            // TODO: avoid div/mod if possible in these calculations
            uint32_t partition = b / numBucketsPerPartition;
            uint32_t bucket = b % numBucketsPerPartition;

            uint32_t bucketNumberGlobal = smallSideIndexToPartition[partition + 1] * numActiveBuckets + bucket;

            if (f == 0 && false) {
                printf("f %d local bucket %d part %d buck %d nb %d nab %d global bucket %d cnt %d\n",
                    f, b, partition, bucket, numBucketsPerPartition, numActiveBuckets, bucketNumberGlobal,
                    wLocal[b].count);
            }

            incrementWOut(wGlobal + bucketNumberGlobal, wLocal + b);
            //atomic_inc(&numGlobalUpdates);
        }
    }

#if 0
    if (get_global_id(0) == 0 && false) {
        printf("f %d nlb=%d nb=%d mlb=%d nbp=%d nlu=%d ngu=%d ncg2l=%d\n",
                f, numLocalBuckets, numBucketsPerPartition, maxLocalBuckets, numBucketsPerPartition * numActivePartitions / 2,
                numLocalUpdates, numGlobalUpdates, numCopyLocalToGlobal);
    }
#endif

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
// Dimension 1 = bucket number (from 0 to the number of active buckets); may be padded

struct FixupBucketsArgs {
    uint32_t numActiveBuckets;
};

__kernel void
fixupBucketsKernel(__constant const FixupBucketsArgs & args,
                   __global W * buckets,
                   __global W * wAll,
                   __global const PartitionInfo * partitionInfo,
                   __global const uint8_t * smallSideIndexes,
                   uint2 global_id [[thread_position_in_grid]],
                   uint2 global_size [[threads_per_grid]],
                   uint2 local_id [[thread_position_in_threadgroup]],
                   uint2 local_size [[threads_per_threadgroup]])
{
    uint32_t numActiveBuckets = args.numActiveBuckets;
    uint32_t j = get_global_id(1);  // bucket number in partition
    if (j >= args.numActiveBuckets)
        return;

    //uint32_t numPartitions = get_global_size(0);
    uint32_t partition = get_global_id(0);  // partition number

    PartitionInfo info = partitionInfo[partition];
    if (info.left == -1 || info.right == -1)
        return;

    // The small side partition always gets subtracted from the large size partition
    int32_t to, from;
    if (smallSideIndexes[info.left]) {
        to = info.right;
        from = info.left;
    }
    else {
        to = info.left;
        from = info.right;
    }

    __global W * bucketsFrom = buckets + from * numActiveBuckets;
    __global W * bucketsTo = buckets + to * numActiveBuckets;
    
    decrementWOutGlobal(bucketsTo + j, bucketsFrom + j);
    if (j == 0) {
        decrementWOutGlobal(wAll + to, wAll + from);
    }
}
