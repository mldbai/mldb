// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** merge_hash_entries.h                                           -*- C++ -*-
    Jeremy Barnes, 1 March 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Data structure to allow merging of buckets of hash entries.
*/

#pragma once

#include "mldb/base/parallel.h"

namespace MLDB {

struct MergeHashEntry {
    MergeHashEntry(uint64_t hash = SH(), uint32_t bitmap = 0)
        : hash(hash), bitmap(bitmap)
    {
    }
        
    uint64_t hash;
    uint32_t bitmap;
} MLDB_PACKED;

struct MergeHashEntryBucket {
    MergeHashEntry * data;
    size_t size_;
    size_t capacity_;

    MergeHashEntryBucket()
        : data(0), size_(0), capacity_(0)
    {
    }
        
    ~MergeHashEntryBucket()
    {
        clear();
    }
        
    void clear()
    {
        free(data);
        size_ = 0;
        capacity_ = 0;
    }

    size_t size() const
    {
        return size_;
    }

    void operator = (MergeHashEntryBucket && other)
    {
        swap(other);
    }

    void swap(MergeHashEntryBucket & other)
    {
        using std::swap;
        swap(data, other.data);
        swap(size_, other.size_);
        swap(capacity_, other.capacity_);
    }

    void merge(const MergeHashEntryBucket & left,
               const MergeHashEntryBucket & right)
    {
        if (left.size() > right.size()) {
            merge(right, left);
            return;
        }

        clear();

        // Merge the two together.  Requires that they be sorted.
        auto it1 = left.data, end1 = it1 + left.size();
        auto it2 = right.data, end2 = it2 + right.size();

        // Get at least enough space
        reserve(left.size() + right.size());
        auto out = data;

        for (;it1 != end1 && it2 != end2;) {
            if (MLDB_LIKELY(it2->hash < it1->hash)) {
                *out++ = *it2++;
                __builtin_prefetch(it2 + 16, 0, 3);
            }
            else if (it1->hash == it2->hash) {
                *out++ = MergeHashEntry(it1->hash, it1->bitmap | it2->bitmap);
                //emplace_back(it1->hash, it1->bitmap | it2->bitmap);
                ++it1;
                ++it2;
                __builtin_prefetch(it1 + 16, 0, 3);
                __builtin_prefetch(it2 + 16, 0, 3);
            }
            else {
                *out++ = *it1++;
                __builtin_prefetch(it1 + 16, 0, 3);
            }

            __builtin_prefetch(out + 16, 1, 3);
        }

        while (it1 != end1)
            *out++ = *it1++;

        while (it2 != end2)
            *out++ = *it2++;
            
        size_ = out - data;
    }

    void reserve(size_t newCap)
    {
        if (newCap <= capacity_)
            return;

        auto newData = (MergeHashEntry *)malloc(newCap * sizeof(MergeHashEntry));
        if (!newData) {
            throw MLDB::Exception("not enough memory to allocate %d",
                                (int) (newCap * sizeof(MergeHashEntry)));
        }
        std::copy(data, data + size_, newData);
        capacity_ = newCap;
        free(data);
        data = newData;
    }

    void emplace_back(uint64_t hash, uint32_t bitmap)
    {
        if (size_ == capacity_)
            reserve(std::max<size_t>(128, capacity_) * 2);
        new (data + size_) MergeHashEntry(hash, bitmap);
        ++size_;
    }

    MergeHashEntry * begin() const
    {
        return data;
    }

    MergeHashEntry * end() const
    {
        return data + size_;
    }

private:
    MergeHashEntryBucket(const MergeHashEntryBucket & other);
    void operator = (const MergeHashEntryBucket & other);
};

struct MergeHashEntries {
    enum {
        BUCKETBITS = IdHashes::BUCKETBITS,
        NBUCKETS = 1 << BUCKETBITS
    };

    MergeHashEntries()
    {
    }

    MergeHashEntries(MergeHashEntries && other)
    {
        for (unsigned i = 0;  i < NBUCKETS;  ++i)
            buckets[i] = std::move(other.buckets[i]);
    }

    void operator = (MergeHashEntries && other)
    {
        for (unsigned i = 0;  i < NBUCKETS;  ++i)
            buckets[i] = std::move(other.buckets[i]);
    }

    MergeHashEntryBucket buckets[NBUCKETS];

    void reserve(size_t size)
    {
        for (unsigned i = 0;  i < NBUCKETS;  ++i)
            buckets[i].reserve(size / NBUCKETS * 2);
    }

    void merge(const MergeHashEntries & left,
               const MergeHashEntries & right)
    {
        if (left.size() + right.size() > 1000000) {
            auto onBucket = [&] (int i)
                {
                    buckets[i].merge(left.buckets[i], right.buckets[i]);
                };
                
            parallelMap(0, NBUCKETS, onBucket);
        }
        else {
            for (unsigned i = 0;  i < NBUCKETS;  ++i) {
                buckets[i].merge(left.buckets[i], right.buckets[i]);
            }
        }
    }

    void add(uint64_t hash, uint32_t bitmap)
    {
        int bucket = hash >> (64 - BUCKETBITS);
        buckets[bucket].emplace_back(hash, bitmap);
    }

    size_t size() const
    {
        size_t result = 0;
        for (unsigned i = 0;  i < NBUCKETS;  ++i)
            result += buckets[i].size();
        return result;
    }

private:
    MergeHashEntries(const MergeHashEntries &);
    void operator = (const MergeHashEntries &);
};

/** Extract and merge an index from a set of elements to merge.

    There is a set of numElementsToMerge items, each of which have a set of
    hashes that need to be merged into an index of (hash key, bitmap index).

    Parameters:
    - numElementsToMerge: the length of the list
    - getEntries: for element n, returns the set of entries to be merged
    - finishBucket: called once each bucket has finished being merged
 */
inline MergeHashEntries
extractAndMerge(size_t numElementsToMerge,
                std::function<MergeHashEntries (int)> getEntries,
                std::function<void (int, MergeHashEntryBucket &)> finishBucket)
{
    using namespace std;

    vector<MergeHashEntries> allEntries(numElementsToMerge);
            
    // Phase 1: extract each into a lot of buckets
    auto onEntry = [&] (int i)
        {
            allEntries[i] = getEntries(i);
        };

    parallelMap(0, numElementsToMerge, onEntry);

    // Phase 2: merge each of the buckets

    MergeHashEntries result;
    
    std::function<void (int, int, int, MergeHashEntryBucket &)> mergeBucketsRecursive
        = [&] (int bucket, int first, int last,
               MergeHashEntryBucket & result)
        {
            if (first == 0 && last == 0) {
                return;
            }
            else if (first >= last) {
                throw MLDB::Exception("first should come before last");
            }
            else if (last == first + 1) {
                // One of them
                result = std::move(allEntries[first].buckets[bucket]);
            }
            else {
                // Recursive
                int mid = (first + last) / 2;
                MergeHashEntryBucket left, right;
                mergeBucketsRecursive(bucket, first, mid, left);
                mergeBucketsRecursive(bucket, mid, last, right);
                result.merge(left, right);
            }
        };
        
    auto mergeBucket = [&] (int i)
        {
            mergeBucketsRecursive(i, 0, numElementsToMerge, result.buckets[i]);

            finishBucket(i, result.buckets[i]);
        };

    parallelMap(0, MergeHashEntries::NBUCKETS, mergeBucket);

    return result;
}


} // namespace MLDB

