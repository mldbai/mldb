/** svd_utils.h                                                    -*- C++ -*-
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Utilities for SVD calculations.
*/

#pragma once

#include "mldb/utils/compact_vector.h"
#include "mldb/types/hash_wrapper.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/base/exc_assert.h"

namespace MLDB {

/** Select which inner product space we use */
enum SvdSpace {
    HAMMING,    ///< Vectors are binary; overlap is number in common
    PYTHAGOREAN ///< Vectors are real in number of counts; overlap is dp
};

DECLARE_ENUM_DESCRIPTION(SvdSpace);


int
intersectionCountBasic(const uint16_t * it1, const uint16_t * end1,
                       const uint16_t * it2, const uint16_t * end2);

int
intersectionCountOptimized(const uint16_t * it1, const uint16_t * end1,
                           const uint16_t * it2, const uint16_t * end2);

int
intersectionCount(const uint16_t * it1, const uint16_t * end1,
                  const uint16_t * it2, const uint16_t * end2);


int
intersectionCountBasic(const uint32_t * it1, const uint32_t * end1,
                       const uint32_t * it2, const uint32_t * end2);


int
intersectionCountOptimized(const uint32_t * it1, const uint32_t * end1,
                           const uint32_t * it2, const uint32_t * end2);

int
intersectionCount(const uint32_t * it1, const uint32_t * end1,
                  const uint32_t * it2, const uint32_t * end2);


/** Controls whether an optimized or basic version of the intersection is
    used.  Mostly for testing purposes.

    0 = use no optimization for set intersections, ie do direct
        intersection calculations on the uint32_t elements.
    1 = use the assembly optimized version of the uint32_t intersection
        algorithm,
    2 = use the 16 bit hierarchical representation, but perform intersections
        of 16 bit integers using an unoptimized algorithm.
    3 = use the 16 bit hierarchical representation, and perform intersections
        using the optimized algorithm.  This is the fastest and the default.
*/
extern int useOptimizedIntersection;  // = 3



/*****************************************************************************/
/* SVD COLUMN ENTRY                                                          */
/*****************************************************************************/

/** This represents a bucket of increasing subject IDs, that can be
    effectively intersected with another.
*/
struct SvdColumnEntry {
    SvdColumnEntry()
        : totalRows(0), eligibleRows(0), bitmap(0)
    {
    }

    struct Bucket {
        Bucket()
            : bitmap(0)
        {
        }

        compact_vector<uint32_t, 3> rows;
        compact_vector<uint32_t, 3> counts;  // May be empty if they are all         
        // This representaton has:
        // 1 entry: high bits
        // 1 entry: number of values with those bits
        // n entries: low bits for those values
        compact_vector<uint16_t, 4, uint32_t, false> compressedRows;

        uint64_t bitmap;

        void add(int rowIndex, uint64_t rowHash);
        void add(int rowIndex, uint64_t rowHash, int count);

        // Sort the entries that have been added
        void sort();

        // Calculate the compressed versions
        void compress();

        // Intersect with another bucket in the given space
        double calcOverlap(const Bucket & other, SvdSpace space) const;

        template<typename Fn>
        void forEach(Fn && fn) const
        {
            if (!compressedRows.empty()) {
                int i = 0;
                while (i < compressedRows.size()) {
                    uint32_t high = compressedRows[i];
                    uint16_t n = compressedRows[i + 1];
                    i += 2;
                    int end = i + n;
                    for (; i < end;  ++i) {
                        uint32_t low = compressedRows[i];
                        uint32_t s = high << 16 | low;
                        fn(s);
                    }
                    ExcAssert(i <= compressedRows.size());
                }
            }
            else {
                for (auto & s: rows) {
                    fn(s);
                }
            }
        }
    };

    void add(int subject, uint64_t rowHash);
    void add(int subject, uint64_t rowHash, int count);

    // Sort all buckets
    void sort();

    // Compress all buckets
    void compress();

    template<typename Fn>
    void forEach(Fn && fn) const
    {
        for (auto & b: buckets) {
            b.forEach(std::forward<Fn>(fn));
        }
    }

    // Calculate the overlap with another entry
    double calcOverlap(const SvdColumnEntry & other,
                       SvdSpace space,
                       bool shortCircuit) const;

    // 64 buckets to allow for work to be split up
    Bucket buckets[64];

    int totalRows;     ///< Total number of rows in all buckets
    int eligibleRows;  ///< Total number of rows we are drawn from

    uint64_t bitmap;  // Used for rapid non-intersection calculation
};


} // namespace MLDB
