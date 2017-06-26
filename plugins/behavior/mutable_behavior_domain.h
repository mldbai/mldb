/* mutable_behavior_domain.h                                      -*- C++ -*-
   Jeremy Barnes, 4 April 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "behavior_domain.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/base/exc_assert.h"
#include "mldb/arch/spinlock.h"
#include "mldb/arch/gc_lock.h"
#include "mldb/arch/rcu_protected.h"
#include "mldb/arch/thread_specific.h"
#include "mldb/arch/bit_range_ops.h"  // TODO: put in .h file
#include <mutex>
#include <thread>
#include <atomic>


namespace MLDB {

/*****************************************************************************/
/* MUTABLE BEHAVIOR DOMAIN                                                  */
/*****************************************************************************/

struct MutableBehaviorDomain : public BehaviorDomain {
    MutableBehaviorDomain(int minSubjects = 1);

    MutableBehaviorDomain(const MutableBehaviorDomain & other);

    ~MutableBehaviorDomain();

    /** Make a copy of the data structure.  */
    virtual MutableBehaviorDomain * makeShallowCopy() const;

    /** Make a deep of the data structure. */
    virtual MutableBehaviorDomain * makeDeepCopy() const;

    /**
     * This is thread safe
     */
    virtual void
    record(const Id & subject, const Id & behavior, Date ts,
           uint32_t count = 1, const Id & verb = Id());

    virtual void
    recordOnce(const Id & subject,
               const Id & behavior, Date ts, const Id & verb = Id());

    struct ManyEntryInt {
        ManyEntryInt()
            : behavior(0), count(1)
        {
        }

        ManyEntryInt(int behavior, Date timestamp, uint32_t count = 1,
                     Id verb = Id())
            : behavior(behavior), timestamp(timestamp),
              count(count), verb(verb)
        {
        }

        int behavior;
        Date timestamp;
        uint32_t count;
        Id verb;
    };

    struct ManyEntryId {
        ManyEntryId()
            : count(1)
        {
        }

        ManyEntryId(Id behavior, Date timestamp, uint32_t count = 1,
                    Id verb = Id())
            : behavior(std::move(behavior)), timestamp(timestamp),
              count(count), verb(std::move(verb))
        {
        }

        Id behavior;
        Date timestamp;
        uint32_t count;
        Id verb;
    };

    virtual void recordMany(const Id & subject,
                            const ManyEntryInt * first,
                            size_t n);

    virtual void recordMany(const Id & subject,
                            const ManyEntryId * first,
                            size_t n);

    struct ManyEntryIndex {
        ManyEntryIndex()
            : behIndex(-1), subjIndex(-1), count(1)
        {
        }

        int behIndex;
        int subjIndex;
        Date timestamp;
        uint32_t count;
    };

    virtual void recordMany(const Id * behIds,
                            size_t numBehIds,
                            const Id * subjectIds,
                            size_t numSubjectIds,
                            const ManyEntryIndex * first,
                            size_t n);

    struct ManySubjectId {
        ManySubjectId()
            : count(1)
        {
        }

        ManySubjectId(Id subject, Date timestamp, uint32_t count = 1,
                      Id verb = Id())
            : subject(std::move(subject)), timestamp(timestamp),
              count(count), verb(std::move(verb))
        {
        }

        Id subject;
        Date timestamp;
        uint32_t count;
        Id verb;
    };

    virtual void recordManySubjects(const Id & beh,
                                    const ManySubjectId * first,
                                    size_t n);

    virtual void finish();

    /** How many distinct subjects are known? */
    virtual size_t subjectCount() const;

    /** How many distinct behaviors are known? */
    virtual size_t behaviorCount() const;

    /** Return a list of behaviors. */
    virtual std::vector<BH> allBehaviorHashes(bool sorted = false) const;

    /** Return a list of subjects. */
    virtual std::vector<SH>
    allSubjectHashes(SH maxSubject = SH(-1), bool sorted = false) const;

    virtual std::unique_ptr<BehaviorStream>
    getBehaviorStream(size_t start) const;

    virtual std::unique_ptr<SubjectStream>
    getSubjectStream(size_t start) const;

    virtual bool
    forEachSubject(const OnSubject & onSubject,
                   const SubjectFilter & filter = SubjectFilter()) const;

    virtual bool knownSubject(SH subjectHash) const;

    virtual bool knownBehavior(BH behHash) const;

    virtual SubjectStats
    getSubjectStats(SH subjectHash,
                    bool needDistinctBehaviors = true,
                    bool needDistinctTimestamps = false) const;

    virtual std::pair<Date, Date>
    getSubjectTimestampRange(SH subjectHash) const;

    virtual int
    numDistinctTimestamps(SH subjectHash, uint32_t maxValue = -1) const;

    virtual std::vector<SH>
    getSubjectHashes(BI beh, SH maxSubject = SH(-1), bool sorted = false)
        const;

    virtual std::vector<SH>
    getSubjectHashes(BH beh, SH maxSubject = SH(-1), bool sorted = false)
        const;

    virtual std::vector<std::pair<SH, Date> >
    getSubjectHashesAndTimestamps(BI beh, SH maxSubject = SH(-1),
                                  bool sorted = false) const;

    virtual std::vector<std::pair<SH, Date> >
    getSubjectHashesAndTimestamps(BH beh, SH maxSubject = SH(-1),
                                  bool sorted = false) const;

    virtual std::vector<std::pair<SH, Date> >
    getSubjectHashesAndAllTimestamps(BI beh, SH maxSubject = SH(-1),
                                     bool sorted = false) const;
    
    virtual std::vector<std::pair<SH, Date> >
    getSubjectHashesAndAllTimestamps(BH beh, SH maxSubject = SH(-1),
                                     bool sorted = false) const;

    virtual bool
    forEachBehaviorSubject(BH beh,
                            const OnBehaviorSubject & onSubject,
                            bool withTimestamps = false,
                            Order order = INORDER,
                            SH maxSubject = SH::max()) const;

    virtual Id getSubjectId(SH subjectHash) const;

    virtual Id getBehaviorId(BI beh) const;

    virtual BH getBehaviorHash(BI beh) const;

    virtual BI getBehaviorIndex(BH beh) const;

    virtual Id getBehaviorId(BH beh) const;

    virtual BehaviorStats
    getBehaviorStats(BI index, int fields) const;

    virtual BehaviorStats
    getBehaviorStats(BH behavior, int fields) const;

    virtual std::pair<Date, Date>
    getBehaviorTimeRange(BH beh) const;

    virtual std::pair<Date, Date>
    getBehaviorTimeRange(BI beh) const;

    virtual std::vector<std::pair<BH, uint32_t> >
    getSubjectBehaviorCounts(SH subjectHash, Order order = INORDER) const;

    virtual bool forEachSubjectBehaviorHash
        (SH subject,
         const OnSubjectBehaviorHash & onBeh,
         SubjectBehaviorFilter filter = SubjectBehaviorFilter(),
         Order order = INORDER) const;

    using BehaviorDomain::getSubjectBehaviorCounts;

    virtual Date earliestTime() const;

    virtual Date latestTime() const;

    virtual Date nominalStart() const
    {
        return nominalStart_;
    }

    virtual Date nominalEnd() const
    {
        return nominalEnd_;
    }

    virtual void setNominalTimeRange(Date start, Date end)
    {
        ExcAssertGreater(end, start);

        nominalStart_ = start;
        nominalEnd_ = end;
    }

    virtual bool fileMetadataExists(const std::string & key) const;

    virtual Json::Value getFileMetadata(const std::string & key) const;

    virtual void setFileMetadata(const std::string & key,
                                 Json::Value && value);

    virtual Json::Value getAllFileMetadata() const;

    /** Make the data structure immutable; that is to say there can be no further changes
        to it.  This will optimize read accesses.
    */
    virtual void makeImmutable();

    /** Return an approximation to the size of the data in the file. */
    uint64_t getApproximateFileSize() const;

    virtual int64_t totalEventsRecorded() const
    {
        return totalEventsRecorded_;
    }

    static bool profile;
    static std::atomic<uint64_t> records;
    static std::atomic<uint64_t> recordTries;
    static std::atomic<uint64_t> recordSpins;
    static std::atomic<uint64_t> recordsImmediate;
    static std::atomic<uint64_t> recordsWrongWidth;
    static std::atomic<uint64_t> recordsEmpty;
    static std::atomic<uint64_t> recordsNoSpace;
    static std::atomic<uint64_t> recordsEarlierTimestamp;

    
private:
    void recordImpl(const Id & subject,
                    const Id & behavior,
                    Date ts,
                    uint32_t count,
                    const Id & verb,
                    bool once);

    // Independent lock for behaviorIndex
    typedef Spinlock IndexLock;
    //typedef std::mutex IndexLock;
    mutable IndexLock behaviorIndexLock;
    Lightweight_Hash<uint64_t, uint32_t> behaviorIndex;

    // Independent lock for writing to subjectIndex
    std::atomic<double> earliest_, latest_;
    Date nominalStart_, nominalEnd_;
    std::atomic<uint64_t> totalEventsRecorded_;

    std::mutex immutableLock;   ///< Lock to set immutable
    bool immutable_;   ///< If true, no modify operations can be performed
    
public:  // for testing
    struct SubjectEntry {
        Id id;
        SH hash;
        mutable Spinlock lock;

        SubjectEntry()
            : firstSeen(-1), lastSeen(0), behaviorCount(0),
              tsBits(0), cntBits(0), behBits(0), typeBits(0),
              sorted(false), data(4)
        {
        }

        uint64_t firstSeen;  /// Absolute offset of first entry
        uint64_t lastSeen;   /// Same for last entry
        uint32_t behaviorCount;  ///< Number of behaviors

        uint8_t tsBits;     ///< Bits needed to hold the timestamps
        uint8_t cntBits;    ///< Bits needed to hold the counts
        uint8_t behBits;    ///< Bits needed to hold a behavior
        uint8_t typeBits:7; ///< Bits needed to hold a behavior type
        uint8_t sorted:1;   ///< Is it sorted?

        compact_vector<uint32_t, 4> data;  ///< Internal data

        // Iterate over all behaviors
        template<typename F>
        bool forEachBehavior(const F & onBehavior, bool immutable) const;

        void record(BI beh, uint64_t ts, uint32_t count, VI verb);

        bool recordOnce(BI behavior, uint64_t ts, uint32_t count,
                        VI verb);

        SubjectStats getSubjectStats(bool needDistinctBehaviors,
                                     bool needDistinctTimestamps,
                                     const MutableBehaviorDomain * owner) const;

        uint32_t calcNewCapacity(size_t newBitsRequired) const;

        uint64_t
        expand(int newBehBits, int newTsBits, int newCntBits,
               int newBehaviorCount, bool copyOldData,
               int64_t tsOffset);
        
        void sort(bool immutable, const MutableBehaviorDomain *owner)
        {
            std::unique_lock<Spinlock> guard(lock, std::defer_lock);
            if (!immutable)
                guard.lock();

            sortUnlocked(immutable, owner);
        }

        void sortUnlocked(bool immutable, const MutableBehaviorDomain *owner);

        size_t memUsage() const
        {
            return sizeof(*this) + (data.size() > 4 ? 4 * data.size() : 0);
        }
    };

    struct SI2 {
        SI2()
            : bits(-1)
        {
        }

        SI2(uint32_t root, uint32_t idx)
        {
            this->root = root;
            this->idx  = idx;
        }

        union {
            struct {
                uint32_t root:5;
                uint32_t idx:27;
            };
            uint32_t bits;
        };

        bool operator == (const SI2 & other) const
        {
            return bits == other.bits;
        }

        bool operator != (const SI2 & other) const
        {
            return ! operator == (other);
        }

        bool operator < (const SI2 & other) const
        {
            return bits < other.bits;
        }

        bool operator > (const SI2 & other) const
        {
            return bits > other.bits;
        }

    } MLDB_PACKED;

    // Records information for each subject
    // std::vector<SubjectEntry *> subjects;

    struct BehaviorEntry {

        BehaviorEntry()
            : earliest(std::numeric_limits<uint64_t>::max()),
              latest(0),
              sorted(nullptr),
              unsorted(nullptr)
        {
        }

        ~BehaviorEntry()
        {
            if (sorted)
                deleteDataNode(sorted);
            if (unsorted)
                deleteDataNode(unsorted);
        }

        BehaviorEntry(const BehaviorEntry &) = delete;
        void operator = (const BehaviorEntry &) = delete;

        std::atomic<uint64_t> earliest;  /// Absolute offset of first entry
        std::atomic<uint64_t> latest;    /// Same for last entry

        BH hash;
        Id id;
        BI index;

        //void record(uint64_t sub, Date ts, uint32_t count, VI verb);

        void clear(GcLock & rootLock);
        void record(SI2 subject, uint64_t ts, uint32_t count, VI verb,
                    GcLock & rootLock);
        void sort(GcLock & rootLock, bool immutable) const;
        void makeImmutable(GcLock & rootLock);

        template<typename F>
        bool forEachSubject(const F & onSubject, bool immutable,
                            GcLock & rootLock) const;

        typedef Spinlock EntryLock;
        typedef std::unique_lock<EntryLock> EntryGuard;

        mutable EntryLock fullLock;
        mutable EntryLock lock;

        struct SubjectEntry {
            SI2 subj;
            uint32_t tsOfs; //uint64_t ts;

            bool operator < (const SubjectEntry & other) const
            {
                return (subj < other.subj)
                    || (subj == other.subj && tsOfs < other.tsOfs);
            }
        } MLDB_PACKED;

        /** This class holds a collection of SubjectEntry values.  It
            has a few special features over a vector<SubjectEntry>,
            though:

            1.  It bit-compresses its entries, and so uses up a lot
                less memory than a vector.
            2.  It stores a base offset for timestamps so that they
                don't take up more bits than necessary.
            3.  It supports lock-free insertion at the end of the
                list (when a resize is not required), and the ability
                to wait until all elements up to a point are ready.
            4.  It includes a couple of fields for the BehaviorEntry
                to store its work in.
            5.  It is possible to atomically block off the rest of
                the entries even if it is not full and force all writers
                to wait for a reallocation.  This is used to pause
                writing during a resize.

            Note that entries each have one status bit which is used
            to allow speculative writes and to know when entries are
            available.  This is:

            - present: the entry is committed, and will never change
                       again;

            The class arranges for present to always be set after all
            other writes have been committed to memory.
        */

        struct DataNode {
            DataNode(uint32_t capacity, uint64_t timeBase)
                : timeBase(timeBase), mergedUpTo(0), size_(0),
                  capacity_(capacity), firstNotDone_(0)
            {
            }

            uint32_t size() const { return size_ & ~SizeLockBit; }
            uint32_t capacity() const { return capacity_; }
            uint32_t firstNotDone() const { return firstNotDone_; }

            size_t memUsage() const
            {
                return sizeof(*this)
                    + sizeof(mem[0]) * memWordsAllocated();
            }

            SubjectEntry operator [] (unsigned index) const
            {
                size_t bit = index * bitsPerEntry() + 1;
                const std::atomic<uint64_t> * word = mem + (bit / 64);
                uint64_t allBits
                    = ML::extract_bit_range(word,
                                            bit % 64,
                                            subjectBits + timestampBits);

                SubjectEntry result;
                result.subj.bits = allBits & ((1 << subjectBits) - 1);
                result.tsOfs     = allBits >> subjectBits;
                return result;
            }

            // Must be called with RCU lock held.  Returns 0 if the record
            // succeeded, or a non-zero code if not based upon the reason:
            // 0 = success; new entry is present and valid
            // 1 = timestamp was earlier than the base timestamp
            // 2 = didn't fit; entry was marked as present but invalid
            // 3 = it was full
            int atomicRecord(SI2 sub, uint64_t ts);

            // Spin until all values are in place.  Once this is true, current
            // is immutable and nothing will modify it.
            void waitForWriters()
            {
                while (this->firstNotDone_ < capacity_) ;
            }

            // Spin until all values are in place.  Once this is true, current
            // is immutable and nothing will modify it.
            void waitForWritersUpTo(uint32_t maxsz)
            {
                while (this->firstNotDone_ < maxsz) ;
            }

            // Return a size where we are guaranteed that everything is
            // committed up to, and wait for all of those entries to
            // appear.
            size_t getCommittedSize() const
            {
                uint32_t sz = this->size_;
                if (sz >= this->capacity_) {
                    sz = this->capacity_;
                    // we have exclusive access to this memory... noone else
                    // can be touching it
                }

                // Spin until all writes are committed
                while (this->firstNotDone_ < sz);
                
                return sz;
            }

            // Lock the node so that nothing else can write to it, then
            // wait for outstanding writes to complete and return the
            // number of committed writes.  This must be done with a lock
            // held.
            size_t lockWaitAndReturnSize();

            // Is the given entry present?
            bool isPresentEntry(int pos) const
            {
                size_t bit = pos * bitsPerEntry();
                const std::atomic<uint64_t> * word
                    = mem + (bit / 64);
                return word->load(std::memory_order_acquire)
                    & ((uint64_t)1 << (bit % 64));
            }

            // Set the given entry.  Can be called with other readers and
            // writers simultaneously active.
            void setEntryAtomic(size_t pos, const SubjectEntry & entry);

            // The maximum subject recorded
            SI2 maxSub() const
            {
                if (size_ == 0)
                    return SI2();
                return operator [] (size_ - 1).subj;
            }

            void dump(std::ostream & stream = std::cerr);

            /// Offset which should be added to times
            uint64_t timeBase;

            /// Space for the caller to say where the merging has got to
            int mergedUpTo;

            /// How many words of memory are allocated?
            uint32_t memWordsAllocated() const
            {
                return numWordsForBits(bitsPerEntry() * capacity_);
            }

            /// How many bits for each entry?
            uint32_t bitsPerEntry() const
            {
                return 1 + subjectBits + timestampBits;
            }

            /// How many words to allocate for the given number of bits?
            static size_t numWordsForBits(size_t bits)
            {
                return (bits + 63) / 64;
            }

            static DataNode *
            allocate(uint32_t capacity,
                     uint64_t timeBase,
                     uint64_t maxTime,
                     SI2 maxSubject,
                     const SubjectEntry * first = nullptr,
                     const SubjectEntry * last = nullptr);
            static void deallocate(DataNode *);

        private:
            static constexpr uint32_t SizeLockBit = uint32_t(1) << 31;

            // WARNING: The msb of this field is set during resize. Because of
            // this, this field should not be accessed directly in most
            // cases. Use the size() function instead which strips the lock bit.
            std::atomic<uint32_t> size_;

            uint32_t capacity_;
            std::atomic<uint32_t> firstNotDone_;
            uint32_t subjectBits:8;   // Bits required to hold a subject
            uint32_t timestampBits:8; // Bits required to hold a time offset
            uint32_t unused:16 MLDB_UNUSED_PRIVATE_FIELD;
            uint32_t unused2 MLDB_UNUSED_PRIVATE_FIELD;

            // Layout of entries:
            // 1 bit: present (0 = no, 1 = yes)
            // i bits: subject (root and index)
            // t bits: timestamp ofs
            std::atomic<uint64_t> mem[0];
        };

        mutable DataNode * sorted;
        std::atomic<DataNode *> unsorted;

        static DataNode * newDataNode(size_t capacity, uint64_t timeBase,
                                      const SubjectEntry * first = nullptr,
                                      const SubjectEntry * last = nullptr);
        static void deleteDataNode(DataNode * node);

        void sortUnlocked(GcLock & rootLock, bool immutable,
                          DataNode * unsorted = nullptr,
                          ssize_t actualSize = -1) const;

        // NOTE: could cause segfaults since we're not saving unsorted
        size_t memUsage() const
        {
            auto us = unsorted.load();
            return sizeof(*this)
                + (sorted ? sorted->memUsage() : 0)
                + (us ? us->memUsage() : 0);
        }
    };

    template<typename T>
    struct Root {
        Root()
            : entries(0), size(0), capacity(0)
        {
        }

        ~Root()
        {
            delete[] entries;
        }

        std::atomic<T *> * entries;
        std::atomic<uint64_t> size;
        uint64_t capacity;

        T * at(uint32_t index) const
        {
            if (index >= size)
                throw MLDB::Exception("invalid index");
            ExcAssert(entries[index]);
            return entries[index];
        }

        T * add(std::unique_ptr<T> & newEntry, size_t knownSize)
        {
            if (knownSize >= capacity)
                throw MLDB::Exception("inserting at end of capacity");

            T * val = newEntry.get();
            T * current = entries[knownSize];
            using namespace std;
            if (current != 0) {
                cerr << "current != 0" << endl;
                return 0;  // something already doing it
            }
            if (size != knownSize) {
                cerr << "known size wrong" << endl;
                return 0;  // someone already did it
            }
            if (!entries[knownSize].compare_exchange_strong(current, val)) {
                cerr << "cmp_xchg failed" << endl;
                return 0;  // someone raced us to it
            }
            std::atomic_thread_fence(std::memory_order_release);
            size += 1;
            
            newEntry.release();
            return val;
        }

        std::unique_ptr<Root> expand() const
        {
            std::unique_ptr<Root> newRoot(new Root());
            size_t newCapacity = std::max<size_t>(capacity, 1024) * 2;
            newRoot->entries = new std::atomic<T *> [newCapacity];
            for (size_t i = 0;  i < size;  ++i)
                newRoot->entries[i].store(entries[i].load());
            for (size_t i = size;  i < newCapacity;  ++i)
                newRoot->entries[i].store(nullptr);
            newRoot->capacity = newCapacity;
            newRoot->size.store(size);
            return newRoot;
        }

        size_t memUsage() const
        {
            size_t result = 24;
            for (unsigned i = 0;  i < size;  ++i)
                result += entries[i].load()->memUsage();
            result += capacity * sizeof(T *);

            return result;
        }
    };

    Json::Value getMemoryStats() const;

    virtual int64_t approximateMemoryUsage() const;

private:

    friend struct MutableBehaviorDomainSubjectStream;
    friend struct MutableBehaviorDomainBehaviorStream;

    enum {
        NUM_SUBJECT_ROOTS = 32
    };

    struct SubjectRoot {
        SubjectRoot(GcLock & lock)
            : subjectIndexPtr(lock),
              subjectEntryPtr(lock)
        {
        }

        mutable IndexLock subjectIndexWriteLock;
        RcuProtected<Lightweight_Hash<uint64_t, uint32_t> > subjectIndexPtr;
        RcuProtected<Root<SubjectEntry> > subjectEntryPtr;
    };

    // We get behaviors from here
    RcuProtected<Root<BehaviorEntry> > behaviorRoot;

    // This is a cache of the mapping of behaviors to their corresponding
    // entry.  It's shared amongst all threads.
    RcuProtected<Lightweight_Hash<BH, BehaviorEntry *> > behaviorPartialCache;
    std::vector<std::shared_ptr<SubjectRoot> > subjectRoots;

    // GC lock for the behavior and subject roots
    mutable GcLock rootLock;

    struct ThreadInfo {
        Lightweight_Hash<uint64_t, SubjectEntry *> subjectCache;
        Lightweight_Hash<uint64_t, BehaviorEntry *> behaviorCache;
    };

    ML::ThreadSpecificInstanceInfo<ThreadInfo, MutableBehaviorDomain>
        threadInfo;


    BehaviorEntry * obtainBehaviorEntry(const Id & behavior);
    SubjectEntry * obtainSubjectEntry(const Id & subject);
    std::pair<MutableBehaviorDomain::SubjectEntry *, SI2>
    obtainSubjectEntryAndIndex(const Id & subject);

    const BehaviorEntry * getBehaviorEntry(BH behavior) const;
    const BehaviorEntry * getBehaviorEntry(BI behavior) const;
    const SubjectEntry * getSubjectEntry(SH subject) const;

    SH getSubjectHash(SI2 index) const;

    /** Signal that we've added enough entries to the cache that it
        might be worth updating.
    */
    void signalPartialCacheNeedsUpdate();

    /** Perform an update of the cache, by reading in the new entries,
        and publishing a new cache.  This happens from a worker thread.
    */
    void updatePartialCache();

    // This is essentially a spinlock so that we only update the
    // partial behavior cache once at a time.  When it's > 0, there
    // may be an enqueued job to update it.
    std::atomic<int> updatingCache;

    /** All of our metadata is stored here. */
    mutable std::mutex fileMetadataLock;
    Json::Value fileMetadata_;
};

std::ostream &
operator << (std::ostream & stream, MutableBehaviorDomain::SI2 si2);

} // namespace MLDB

