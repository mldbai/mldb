/** svd_utils.cc
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "svd_utils.h"
#include "mldb/arch/arch.h"
#include "mldb/jml/utils/environment.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/vfs/filter_streams.h"
#include <mutex>

#if MLDB_INTEL_ISA
#include <smmintrin.h>
#endif

using namespace std;

namespace MLDB {

DEFINE_ENUM_DESCRIPTION(SvdSpace);

SvdSpaceDescription::
SvdSpaceDescription()
{
    addValue("hamming", HAMMING,
             "Hamming space encoding");
    addValue("pythagorean", PYTHAGOREAN,
             "Pythagorean space encoding");
}


static inline int rowToBucket(uint64_t rowHash)
{
    // Extract 6 bits for a bitmap index
    // The last 16 are for the tranche number, so don't use them
    uint64_t shifted = rowHash >> 16;
    shifted = shifted & 63;
    return shifted;
}

static inline int rowToBucketRowhash(uint64_t rowHash)
{
    // Extract 6 bits for a bitmap index within a bucket
    // The last 16 are for the tranche number, so don't use them
    // The next 6 are for the bucket number, so don't use them
    uint64_t shifted = rowHash >> 22;
    shifted = shifted & 63;
    return shifted;
}

void
SvdColumnEntry::Bucket::
add(int rowIndex, uint64_t rowHash)
{
    int subhash = rowToBucketRowhash(rowHash);
    bitmap |= (1ULL << subhash);
    rows.push_back(rowIndex);
}

void
SvdColumnEntry::Bucket::
add(int rowIndex, uint64_t rowHash, int count)
{
    int subhash = rowToBucketRowhash(rowHash);
    bitmap |= (1ULL << subhash);
    rows.push_back(rowIndex);
    counts.push_back(count);
}

void
SvdColumnEntry::Bucket::
sort()
{
    if (counts.empty()) {
        std::sort(rows.begin(), rows.end());
        return;
    }

    ExcAssertEqual(rows.size(), counts.size());

    vector<pair<int, int> > sorted;
    sorted.reserve(rows.size());
    for (unsigned i = 0;  i < rows.size();  ++i) {
        sorted.emplace_back(rows[i], counts[i]);
    }

    std::sort(sorted.begin(), sorted.end());

    rows.clear();  rows.reserve(sorted.size());
    counts.clear();    counts.reserve(sorted.size());

    for (unsigned i = 0;  i < sorted.size();  ++i) {
        rows.push_back(sorted[i].first);
        counts.push_back(sorted[i].second);
    }
}

void
SvdColumnEntry::Bucket::
compress()
{
    //ExcAssert(std::is_sorted(rows.begin(), rows.end()));

    for (unsigned i = 0;  i < rows.size();  /* no inc */) {
        uint32_t current = rows[i];
        uint16_t activeHigh = current >> 16;

        compressedRows.push_back(activeHigh);
        int countEntry = compressedRows.size();
        compressedRows.push_back(0);  // count

        // Find the end of this run of entries with the same count
        unsigned j = i;
        unsigned n = 0;

        for (;  j < rows.size();  ++j, ++n) {
            uint32_t current = rows[j];
            uint16_t high = current >> 16;
            
            if (high != activeHigh)
                break;

            uint16_t low = current & 65535;
            compressedRows.push_back(low);
            ++compressedRows[countEntry];
        }

        i = j;

        ExcAssertGreater(n, 0);

        ExcAssertEqual(compressedRows[countEntry], n);
    }

    // Add 8 empty entries on the end for padding so that we can run
    // parallel algorithms on a 128 bit width without danger
    for (unsigned i = 0;  i < 8;  ++i)
        compressedRows.push_back(0);
}

void
SvdColumnEntry::
add(int rowIndex, uint64_t rowHash)
{
    int bucket = rowToBucket(rowHash);
    bitmap |= (1ULL << bucket);
    buckets[bucket].add(rowIndex, rowHash);
    totalRows += 1;
}

void
SvdColumnEntry::
add(int rowIndex, uint64_t rowHash, int count)
{
    int bucket = rowToBucket(rowHash);
    bitmap |= (1ULL << bucket);
    buckets[bucket].add(rowIndex, rowHash, count);
    totalRows += 1;
}

void
SvdColumnEntry::
sort()
{
    for (auto & b: buckets)
        b.sort();
}

void
SvdColumnEntry::
compress()
{
    for (auto & b: buckets)
        b.compress();
}

double
SvdColumnEntry::
calcOverlap(const SvdColumnEntry & other,
            SvdSpace space,
            bool shortCircuit) const
{
    if (space == HAMMING && totalRows == eligibleRows) {
        // If all rows have this column, the overlap is automatically
        // the count of the smaller one
        return other.totalRows;
    }

    // If no overlap in their bitmaps, then they can't overlap
    int64_t overlap = bitmap & other.bitmap;

    if (overlap == 0)
        return 0.0;

    double result = 0.0;
    double estimatedResult = 0.0;
    int estimatedResultIterations = -1;
    for (unsigned b = 0;  b < 64;  ++b) {

        // If no overlap in the bucket, then nothing to do
        if ((overlap & (1ULL << b)) == 0)
            continue;

        result += buckets[b].calcOverlap(other.buckets[b], space);

        if (shortCircuit && result > 1000.0 && estimatedResultIterations == -1) {
            estimatedResult = result * 64.0 / (b + 1);
            estimatedResultIterations = b;
            return estimatedResult;
        }
    }

    if (estimatedResultIterations != -1 && false) {
        cerr << "result: real " << result << " estimated "
             << estimatedResult << " difference "
             << 100.0 * (estimatedResult - result) / result
             << "% diff after " << estimatedResultIterations
             << " iterations" << endl;
    }

    return result;

}

int
intersectionCountBasic(const uint16_t * it1, const uint16_t * end1,
                       const uint16_t * it2, const uint16_t * end2)
{
#if 1
    ExcAssert(std::is_sorted(it1, end1));
    ExcAssert(std::is_sorted(it2, end2));

    vector<int16_t> test;
    std::set_intersection(it1, end1, it2, end2,
                          std::back_inserter(test));
    return test.size();
#else

    int result = 0;
    
    while (it1 != end1 && it2 != end2) {
#if MLDB_INTEL_ISA
        int eq = 0, le = 0, ge = 0;
        
        __asm__
            ("cmp     %[val2], %[val1]    \n\t"
             "cmovz   %[one], %[eq] \n\t"
             "cmovle  %[one], %[le] \n\t"
             "cmovge  %[one], %[ge] \n\t"
             : [eq] "+r" (eq), [le] "+r" (le), [ge] "+r" (ge)
             : [val1] "r" (*it1), [val2] "r" (*it2), [one] "r" (1)
             : "cc"
             );
#else
        int val1 = *it1;
        int val2 = *it2;
        int eq = val1 == val2;
        int le = val1 <= val2;
        int ge = val1 >= val2;
#endif
        
        result += eq;
        it1 += le;
        it2 += ge;
    }

    //ExcAssertEqual(result, test.size());

    return result;
#endif
}

int
intersectionCountOptimized(const uint16_t * it1, const uint16_t * end1,
                           const uint16_t * it2, const uint16_t * end2)
{
    int result = 0;

#if MLDB_INTEL_ISA
    typedef char v16qi __attribute__((__vector_size__(16)));
                                              
    constexpr int8_t mode
        = 0x01 /* SIDD_UWORD_OPS */
        | 0x00 /* SIDD_CMP_EQUAL_ANY */
        | 0x00 /* SIDD_BIT_MASK */;

    //static std::mutex lock;
    //std::unique_lock<std::mutex> guard(lock);

    //cerr << "vector 1 " << end1 - it1 << " vector 2 " << end2 - it2 << endl;

    // see here: http://www.adms-conf.org/p1-SCHLEGEL.pdf

    //auto start1 = it1, start2 = it2;

    while (it1 < end1 && it2 < end2) {
        auto v1 = (v16qi)_mm_loadu_ps((float *)it1);
        auto v2 = (v16qi)_mm_loadu_ps((float *)it2);
        
        int len1 = end1 - it1;
        int len2 = end2 - it2;


#if 0
        union {
            v16qi vec;
            uint16_t u16[8];
        } v1e, v2e;

        v1e.vec = v1;
        v2e.vec = v2;


        int realMask = 0;

        {
            for (unsigned i = 0;  i < 8 && i < len1;  ++i) {
                for (unsigned j = 0;  j < 8 && j < len2;  ++j) {
                    if (v1e.u16[i] == v2e.u16[j]) {
                        realMask = realMask | (1 << j);
                    }
                }
            }
        }
#endif

        auto v3 = __builtin_ia32_pcmpestrm128(v1, len1, v2, len2,
                                              mode);

        int mask = _mm_extract_epi32((__m128i)v3, 0);

        int lastOffset1 = std::min<int>(7, len1 - 1);
        int lastOffset2 = std::min<int>(7, len2 - 1);

        uint16_t last1 = it1[lastOffset1];
        uint16_t last2 = it2[lastOffset2];

#if 0
        if (mask != realMask) {
            cerr << "total1 = " << (end1 - it1)
                 << " total2 = " << (end2 - it2)
                 << endl;
            cerr << "len1 = " << len1 << " len2 = " << len2 << endl;

            cerr << "vec1: ";
            for (unsigned i = 0;  i < 8 && i < len1;  ++i)
                cerr << v1e.u16[i] << " ";
            cerr << endl << "vec2: ";
            for (unsigned j = 0;  j < 8 && j < len2;  ++j)
                cerr << v2e.u16[j] << " ";
            
            //cerr << endl << "vec1: ";
            //for (unsigned i = 0;  i < 8 && i < len1;  ++i)
            //    cerr << it1[i] << " ";
            //cerr << endl << "vec2: ";
            //for (unsigned j = 0;  j < 8 && j < len2;  ++j)
            //    cerr << it2[j] << " ";
            cerr << endl;

            cerr << "mask = " << (int)mask << " realMask = " << (int)realMask << endl;
        }
#endif

        //cerr << "mask = " << mask << " last1 = " << last1 << " last2 = "
        //     << last2 << endl;

        it1 += 8 * (last1 <= last2);
        it2 += 8 * (last2 <= last1);

        result += __builtin_popcount(mask);
    }

#else  // MLDB_INTEL_ISA
    while (it1 < end1 && it2 < end2) {

        int val1 = *it1;
        int val2 = *it2;
        int eq = val1 == val2;
        int le = val1 <= val2;
        int ge = val1 >= val2;

        result += eq;
        it1 += le;
        it2 += ge;
    }
#endif // MLDB_INTEL_ISA

    return result;
}

int
intersectionCountBasic(const uint32_t * it1, const uint32_t * end1,
                       const uint32_t * it2, const uint32_t * end2)
{
    vector<uint32_t> output;

    std::set_intersection(it1, end1, it2, end2,
                          std::back_inserter(output));
    return output.size();
}

int
intersectionCountOptimized(const uint32_t * it1, const uint32_t * end1,
                           const uint32_t * it2, const uint32_t * end2)
{
    int result = 0;
    
    while (it1 != end1 && it2 != end2) {
        
#if MLDB_INTEL_ISA
        int eq = 0, le = 0, ge = 0;

        __asm__
            ("cmp     %[val2], %[val1]    \n\t"
             "cmovz   %[one], %[eq] \n\t"
             "cmovle  %[one], %[le] \n\t"
             "cmovge  %[one], %[ge] \n\t"
             : [eq] "+r" (eq), [le] "+r" (le), [ge] "+r" (ge)
             : [val1] "r" (*it1), [val2] "r" (*it2), [one] "r" (1)
             : "cc"
             );
#else // MLDB_INTEL_ISA
        int val1 = *it1;
        int val2 = *it2;
        int eq = val1 == val2;
        int le = val1 <= val2;
        int ge = val1 >= val2;
#endif // MLDB_INTEL_ISA
        
        result += eq;
        it1 += le;
        it2 += ge;
    }

    return result;
}

int
intersectionCount(const uint32_t * it1, const uint32_t * end1,
                  const uint32_t * it2, const uint32_t * end2)
{
    return (useOptimizedIntersection & 1)
        ? intersectionCountOptimized(it1, end1, it2, end2)
        : intersectionCountBasic(it1, end1, it2, end2);
}

int
intersectionCount(const uint16_t * it1, const uint16_t * end1,
                  const uint16_t * it2, const uint16_t * end2)
{
    return (useOptimizedIntersection & 1)
        ? intersectionCountOptimized(it1, end1, it2, end2)
        : intersectionCountBasic(it1, end1, it2, end2);
}

EnvOption<int> SVD_OPTIMIZED_INTERSECTION("SVD_OPTIMIZED_INTERSECTION", 3);

int useOptimizedIntersection = SVD_OPTIMIZED_INTERSECTION;

double
SvdColumnEntry::Bucket::
calcOverlap(const Bucket & other, SvdSpace space) const
{
    const Bucket & ei = *this;
    const Bucket & ej = other;

    if (ei.rows.size() < ej.rows.size())
        return other.calcOverlap(*this, space);

    // We can't get an iterator if empty
    if (ej.rows.empty())
        return 0.0;

    // ei has the most rows
    const unsigned * begin1 = &ei.rows[0];
    const unsigned * it1 = begin1;
    const unsigned * end1 = begin1 + ei.rows.size();
    const unsigned * begin2 = &ej.rows[0];
    const unsigned * it2 = begin2;
    const unsigned * end2 = begin2 + ej.rows.size();

    int64_t result = 0;

    // If no overlap in their bitmaps, then they can't overlap
    if (ei.bitmap && ej.bitmap && ((ei.bitmap & ej.bitmap) == 0))
        return result;

    if (ej.rows.size() * 100 < ei.rows.size()) {
        // Sizes are unbalanced; perform a lookup of each of them in turn
        for (; it2 != end2;  ++it2) {
            it1 = std::lower_bound(it1, end1, *it2);
            if (it1 == end1) break;
            if (*it1 == *it2) {
                switch (space) {
                case HAMMING:
                    result += 1;
                    break;
                case PYTHAGOREAN:
                    result += ei.counts.at(it1 - begin1) * ej.counts.at(it2 - begin2);
                    break;
                default:
                    throw MLDB::Exception("unknown space");
                }

                ++it1;
            }
        }
        
        return result;
    }
    
    // They are roughly the same size... co-iterate through them
    switch (space) {
    case HAMMING: {

        if (!compressedRows.empty() && (useOptimizedIntersection & 2)) {
            // Do it with an outer comparison on high order, inner on
            // low order
            
            // Note: skip the 8 padding entries that were added
            int i1 = 0, e1 = compressedRows.size() - 8,
                i2 = 0, e2 = other.compressedRows.size() - 8;

#if 0
            vector<uint32_t> uncomp;

            int lastHigh = -1;

            for (int i = 0;  i < compressedRows.size() - 8;  /* no inc */) {
                int32_t high = compressedRows[i];

                ExcAssertGreater(high, lastHigh);

                uint16_t n = compressedRows[i + 1];

                i += 2;

                int lastLow = -1;

                for (unsigned j = 0;  j < n;  ++j, ++i) {

                    int32_t low = compressedRows[i];

                    ExcAssertGreater(low, lastLow);
                    
                    uncomp.push_back(high << 16 | low);

                    lastLow = low;
                }

                lastHigh = high;
            }

            ExcAssertEqual(uncomp.size(), rows.size());

            for (unsigned i = 0;  i < uncomp.size();  ++i)
                ExcAssertEqual(uncomp[i], rows[i]);
#endif

            while (i1 != e1 && i2 != e2) {
                int16_t high1 = compressedRows[i1];
                int16_t n1 = compressedRows[i1 + 1];

                int16_t high2 = other.compressedRows[i2];
                int16_t n2 = other.compressedRows[i2 + 1];

                if (high1 == high2) {
                    result += intersectionCount(&compressedRows[i1 + 2],
                                                &compressedRows[i1 + 2] + n1,
                                                &other.compressedRows[i2 + 2],
                                                &other.compressedRows[i2 + 2] + n2);
                }
                if (high1 <= high2)
                    i1 += 2 + n1;
                if (high2 <= high1)
                    i2 += 2 + n2;
            }

            ExcAssert(i1 == e1 || i2 == e2);

            if (false /* debugging of problems */) {
                int realResult = intersectionCount(it1, end1, it2, end2);

                if (realResult != result) {
                    static std::mutex lock;
                    std::unique_lock<std::mutex> guard(lock);

                    cerr << "difference in result: " << realResult << " vs "
                         << result << endl;

                    filter_ostream test("test.txt");

                    test << jsonEncode(vector<uint32_t>(it1, end1)) << endl;
                    test << jsonEncode(vector<uint32_t>(it2, end2)) << endl;

                    vector<uint32_t> cmp1, cmp2;
                    this->forEach([&] (uint32_t index) { cmp1.push_back(index); });
                    other.forEach([&] (uint32_t index) { cmp2.push_back(index); });

                    test << jsonEncode(cmp1) << endl;
                    test << jsonEncode(cmp2) << endl;

                    cerr << "it1 size " << end1 - it1 << endl;
                    cerr << "cmp1 size " << cmp1.size() << endl;
                    cerr << "it2 size " << end2 - it2 << endl;
                    cerr << "cmp2 size " << cmp2.size() << endl;

                    cerr << "is sorted cmp1 = " << std::is_sorted(cmp1.begin(), cmp1.end()) << endl;
                    cerr << "is sorted cmp2 = " << std::is_sorted(cmp2.begin(), cmp2.end()) << endl;

                    // Note: skip the 8 padding entries that were added
                    int i1 = 0, e1 = compressedRows.size() - 8,
                        i2 = 0, e2 = other.compressedRows.size() - 8;

                    vector<uint32_t> intersections;

                    while (i1 != e1 && i2 != e2) {
                        int16_t high1 = compressedRows[i1];
                        int16_t n1 = compressedRows[i1 + 1];
                        
                        int16_t high2 = other.compressedRows[i2];
                        int16_t n2 = other.compressedRows[i2 + 1];

                        if (high1 == high2) {
                            vector<int16_t> subInt;

                            std::set_intersection(&compressedRows[i1 + 2],
                                                  &compressedRows[i1 + 2] + n1,
                                                  &other.compressedRows[i2 + 2],
                                                  &other.compressedRows[i2 + 2] + n2,
                                                  back_inserter(subInt));

                            int cnt = intersectionCount(&compressedRows[i1 + 2],
                                                        &compressedRows[i1 + 2] + n1,
                                                        &other.compressedRows[i2 + 2],
                                                        &other.compressedRows[i2 + 2] + n2);

                            if (cnt != subInt.size())
                                cerr << "error in count for bucket " << high1 << ": "
                                     << cnt << " should be "
                                     << subInt.size() << endl;

                            for (auto & v: subInt) {
                                uint32_t val = v;
                                uint32_t high = high1 << 16;
                                val |= high;
                                intersections.push_back(val);
                            }
                        }
                        if (high1 <= high2)
                            i1 += 2 + n1;
                        if (high2 <= high1)
                            i2 += 2 + n2;
                    }

                    cerr << "intersections.size() = " << intersections.size()
                         << endl;

                    vector<uint32_t> intersections2;

                    std::set_intersection(it1, end1, it2, end2,
                                          back_inserter(intersections2));

                    cerr << "intersections2.size() = " << intersections2.size()
                         << endl;

                    vector<uint32_t> diff;

                    std::set_difference(intersections2.begin(),
                                        intersections2.end(),
                                        intersections.begin(),
                                        intersections.end(),
                                        back_inserter(diff));

                    cerr << "diff = " << jsonEncode(diff) << endl;

                    test << jsonEncode(intersections) << endl;
                    test << jsonEncode(intersections2) << endl;
                    test << jsonEncode(diff) << endl;


                    abort();
                }
            }

            return result;
        }

        return intersectionCount(it1, end1, it2, end2);

        while (it1 != end1 && it2 != end2) {
#if 0 // inexplicably slower... need to look at machine code
            int s1 = *it1, s2 = *it2;
            result += s1 == s2;
            it1 += s1 <= s2;
            it2 += s2 <= s1;
#elif 1
            int eq = 0, le = 0, ge = 0;

            __asm__
                ("cmp    %[val2], %[val1]    \n\t"
                 "cmovz   %[one], %[eq] \n\t"
                 "cmovle  %[one], %[le] \n\t"
                 "cmovge  %[one], %[ge] \n\t"
                 : [eq] "+r" (eq), [le] "+r" (le), [ge] "+r" (ge)
                 : [val1] "r" (*it1), [val2] "r" (*it2), [one] "r" (1)
                 : "cc"
                 );

#if 0            
            ExcAssertEqual(eq, *it1 == *it2);
            ExcAssertEqual(le, *it1 <= *it2);
            ExcAssertEqual(ge, *it1 >= *it2);
#endif

            result += eq;
            it1 += le;
            it2 += ge;


#else
            if (*it1 < *it2) {
                ++it1;
            }
            else if (*it1 == *it2) {
                result += 1;
                ++it1;
                ++it2;
            }
            else ++it2;
#endif
        }

        break;
    }
    case PYTHAGOREAN:
        while (it1 != end1 && it2 != end2) {
            if (*it1 < *it2) {
                ++it1;
            }
            else if (*it1 == *it2) {
                result
                    += ei.counts.at(it1 - begin1)
                    * ej.counts.at(it2 - begin2); 
                ++it1;
                ++it2;
            }
            else ++it2;
        }

        break;
    }

    return result;
}



} // namespace MLDB

