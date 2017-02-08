/** id_hash.h                                                      -*- C++ -*-
    Jeremy Barnes, 1 March 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Data structure for storage of efficiencly mergeable hash values with a bitmap
    associated with each one.
*/

#pragma once

#include "mldb/jml/utils/lightweight_hash.h"

namespace MLDB {

enum {
    RESERVEDBUCKETBITS = 6
};

struct IdHashBucket {
    IdHashBucket(uint64_t first = 0, uint32_t second = 0)
        : first(first), second(second)
    {
    }

    template<typename T1, typename T2>
    IdHashBucket(std::pair<T1, T2> p)
        : first(p.first), second(p.second)
    {
    }

    uint64_t first;
    uint32_t second;
} MLDB_PACKED;

inline std::ostream &
operator << (std::ostream & stream, const IdHashBucket & bucket)
{
    return stream << std::make_pair(bucket.first, bucket.second);
}

struct IdHashFn {
    size_t operator () (uint64_t val) MLDB_CONST_FN
    {
        return val;
    }
};

struct IdHashOps : public PairOps<uint64_t, uint32_t, IdHashFn, IdHashBucket> {

    template<typename Storage>
    static size_t hashKey(IdHashBucket bucket, int capacity,
                          const Storage & storage)
    {
        return hashKey(getKey(bucket), capacity, storage);
    }

    template<typename Storage>
    static size_t hashKey(uint64_t key, int capacity, const Storage & storage)
    {
#if 0
        // Take the middle bits
        key >>= 32;
        return key % capacity;
        //uint64_t mask = (1ULL << ((storage.bits_ - 1))) - 1;
        //return key & mask;
#elif 0
        //key >>= 32;
        uint64_t mask = (1ULL << ((storage.bits_ - 1))) - 1;
        return key & mask;

#elif 1
        /* This hashing scheme is designed to put elements in buckets roughly
           in the order that they would occur when sorted in an array.

           If we look at the hash, we get:

           64     64-RBB                       n          0
           V      V                            v          v
           +------+---------+------------------+-----------+
           | bct  | hash    |                  | partition |
           +------+---------+------------------+-----------+
              1        2             3              4
             
           1.  The top RBB (RESERVEDBUCKETBITS) are used to divide the
               ID space into a lot of "buckets", each of which is hashed
               individually.  The goal of this is to squeeze all accesses
               of nearby hashes into the same segment of memory, and to
               allow initialization of each bucket to happen in paralle1.
           2.  The next bits (storage.bits_) are used as the result of the
               hash function.  This hash function has the interesting
               property of keeping the order of buckets very similar to
               the order of the sorted keys, which helps with locality of
               reference when accessing keys in sorted order.
           3.  These bits have no special use.
           4.  The lower bits of the hash are used to partition users
               when sharding.  This means that you cannot rely on these
               bits having any entropy; often the first n bits will all
               have the same value.  Common values of n are 1-6.  This
               is one reason why we hash on the higher bits rather than the
               lower bits.
        */

        // 1.  Clear out the top bits that are reserved for the bucket
        uint64_t key2 = (key << RESERVEDBUCKETBITS) >> RESERVEDBUCKETBITS;

        // 2.  Shift out everything except enough high order bits to cover
        //     the entire bucket range.
        uint64_t shift = 65ULL - RESERVEDBUCKETBITS - storage.bits_;
        uint64_t res = key2 >> shift;
        return res;

        using namespace std;
        cerr << MLDB::format("key = %016llx res = %016llx",
                           (unsigned long long)key,
                           (unsigned long long)res)
             << endl;
#elif 0
        // Replace a div with a shift
        uint64_t mask = (1ULL << ((storage.bits_ - 1))) - 1;
        return key & mask;

#elif 0
        // Non-improved version
        return key % capacity;
#endif
    }
    
};

#if 1
typedef Lightweight_Hash<uint64_t,
                             uint32_t,
                             IdHashBucket,
                             IdHashBucket,
                             IdHashOps,
                             LogMemStorage<IdHashBucket> > 
IdHash;
#else
typedef Lightweight_Hash<uint64_t,
                             uint32_t,
                             IdHashBucket,
                             IdHashBucket,
                             IdHashOps>
IdHash;
#endif

struct IdHashes {
    enum {
        BUCKETBITS = RESERVEDBUCKETBITS,
        NBUCKETS = 1 << BUCKETBITS
    };
    
    IdHash buckets[NBUCKETS];

    int bucketNum(uint64_t hash) const
    {
        return hash >> (64 - BUCKETBITS);
    }

    void clear()
    {
        for (auto & b: buckets)
            b.clear();
    }

    void insert(std::pair<uint64_t, uint32_t> value)
    {
        buckets[bucketNum(value.first)].insert(value);
    }

    size_t size() const
    {
        size_t result = 0;
        for (auto & b: buckets)
            result += b.size();
        return result;
    }

    bool count(uint64_t key) const
    {
        return buckets[bucketNum(key)].count(key);
    }

    uint32_t getDefault(uint64_t key, uint32_t def = (uint32_t)-1) const
    {
        auto & b = buckets[bucketNum(key)];
        auto it = b.find(key);
        if (it == b.end())
            return def;
        return it->second;
    }

    template<typename Fn>
    bool forEach(const Fn & fn) const
    {
        for (unsigned i = 0;  i < NBUCKETS;  ++i) {
            for (auto & e: buckets[i]) {
                if (!fn(e.first, e.second))
                    return false;
            }
        }
        return true;
    }

    struct const_iterator
    {
        const_iterator() : source(nullptr), bucket(0)
        {

        }

        const_iterator(const IdHashes* source) : source(source), bucket(0)
        {
            bucketIter = source->buckets[bucket].begin();
            while (bucketIter == source->buckets[bucket].end())
            {
                bucket++;
                if (bucket == NBUCKETS)
                    break;
                else
                    bucketIter = source->buckets[bucket].begin();
            }
        }
        IdHashBucket operator *() const
        {
            return bucketIter.dereference();
        }
        void operator ++()
        {
            ++bucketIter;
            while (bucketIter == source->buckets[bucket].end())
            {
                bucket++;
                if (bucket == NBUCKETS)
                    break;
                else
                    bucketIter = source->buckets[bucket].begin();
            }
        }
        private:

        const IdHashes * source;
        size_t bucket;
        IdHash::const_iterator bucketIter;
    };

    const_iterator begin() const
    {
        return const_iterator(this);
    }
};

} // namespace MLDB

