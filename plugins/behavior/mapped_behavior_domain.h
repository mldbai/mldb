/* mapped_behavior_domain.h                                      -*- C++ -*-
   Jeremy Barnes, 4 April 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "behavior_domain.h"
#include "mapped_value.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/arch/endian.h"


namespace MLDB {

// Frustrating, but clang doesn't ever like taking addresses of
// packed fields (even if they are certainly aligned) whereas
// gcc doesn't pack things that contain non-packed structures.
// No way I found to cleanly deal with it, and it's dealing with
// on-disk values so it's important that we maintain the layout.
#if !defined(__clang__) && defined(__GNUC__)
#  define MLDB_PACKED_IF_GCC MLDB_PACKED
#else
#  define MLDB_PACKED_IF_GCC 
#endif

using BH_le = LittleEndian<BH>;
using SH_le = LittleEndian<SH>;

template<>
struct LittleEndian<Date> {

    LittleEndian(Date d = Date())
        : val(d.secondsSinceEpoch())
    {
    }

    LittleEndian & operator = (const Date & date)
    {
        val = date.secondsSinceEpoch();
        return *this;
    }

    operator Date () const { return Date::fromSecondsSinceEpoch(val); }

    LittleEndian<double> val;
} MLDB_PACKED;

using Date_le = LittleEndian<Date>;

inline BH host_to_le(BH h)
{
    return BH(host_to_le(h.index()));
}

inline BH le_to_host(BH h)
{
    return BH(le_to_host(h.index()));
}

inline SH host_to_le(SH h)
{
    return SH(host_to_le(h.index()));
}

inline SH le_to_host(SH h)
{
    return SH(le_to_host(h.index()));
}

/*****************************************************************************/
/* MAPPED BEHAVIOR DOMAIN                                                   */
/*****************************************************************************/

struct MappedBehaviorDomain: public BehaviorDomain {
    MappedBehaviorDomain();
    MappedBehaviorDomain(const std::string & filename);
    MappedBehaviorDomain(const MLDB::File_Read_Buffer & file);

    void load(const std::string & filename);
    void load(const MLDB::File_Read_Buffer & file);

    void init(const MLDB::File_Read_Buffer & file, uint64_t md_offset);

    /** Behaviors for a given subject are structured as follows:
        1.  Behavior index: list of (behavior id, count) bit compressed
        2.  Time index: list of (time offset, behavior index, count)
            bit compressed

        Behavior IDs are indexed linearly from 0 (the most frequent) onwards.
        Behavior indexes are an index into the Behavior Index list, so they
        take very few bits.
        Times are stored as an offset to the earliestTime.
        Counts are stored as (count - 1) bit compressed.

        NOTE: this version of the structure is for reading of legacy files.
    */
    struct SubjectIndexEntry1 {
        struct {
            uint32_t offsetLow;        ///< Offset in file

            uint32_t earliestTime:27;  ///< Offset of earliest timestamp
            uint32_t behBits:5;        ///< Number of bits in beh number

            uint32_t numBehaviors:27; ///< Total number of beh events
            uint32_t indexCountBits:5; ///< 

            uint32_t numDistinctBehaviors:19;
            uint32_t version:3;
            uint32_t timeBits:5; ///< Num bits used for time offset
            uint32_t countBits:5;
        };

        /** Number of bits in the index part (with the behavior IDs and their
            total counts.
        */
        uint64_t numIndexBits() const;

        /** Number of bits in the total structure. */
        uint64_t numBits() const;
    
        /** Number of 32 bit words required to hold it all. */
        uint32_t numWords() const;

        /** Get the data for the given word. */
        std::pair<const uint32_le *, uint32_t>
        getData(const uint32_le * data) const;

        uint64_t offset() const
        {
            return offsetLow;
        }

        uint64_t numBehaviorTableBits() const
        {
            return (uint64_t)(behBits + indexCountBits)
                * (uint64_t)numDistinctBehaviors;
        }

        uint64_t numTableIndexBits() const
        {
            return MLDB::highest_bit(numDistinctBehaviors - 1, -1) + 1;
        }
    } MLDB_PACKED;

    struct SubjectIndexEntry2 {
        SubjectIndexEntry2()
            : bits1(0), bits2(0), bits3(0)
        {
        }

        union {
            struct {
                uint32_t offsetLow;                 // 4TB of offsets possible (42 bits = 40 + 2)
                uint32_t offsetHigh:8;
                uint32_t numDistinctBehaviors:24;  // 16M distinct behaviors
                uint64_t earliestTime:40;           // ~30 years at 1ms timestamps
                uint64_t numDistinctTimestamps:24;  // 16M total timestamps
                uint32_t numBehaviors:24;          // 16M total behaviors
                uint32_t behTableSplitValue:8;      // Actually a fixed point 0 to 1
                uint32_t version:3;
                uint32_t behBits:5;
                uint32_t timeBits:5;
                uint32_t indexCountBits:5;
                uint32_t countBits:5;
                uint32_t behTable2Bits:5;
                uint32_t hasBehaviorTable:1;
                uint32_t hasTimestampTable:1;
                uint32_t splitBehaviorTable:1;
                uint32_t unused1:1;
            };
            struct {
                uint64_t bits1;
                uint64_t bits2;
                uint64_t bits3;
            };
        };

        uint64_t offset() const
        {
            return (uint64_t)offsetHigh << 32
                | offsetLow;
        }

        void setOffset(uint64_t newOffset)
        {
            offsetLow = newOffset;
            offsetHigh = newOffset >> 32;

            if (offset() != newOffset)
                throw MLDB::Exception("error setting offset");
        }

        /** Number of bits in the index part (with the behavior IDs and their
            total counts.
        */
        uint64_t numIndexBits() const;

        /** Number of bits in the total structure. */
        uint64_t numBits() const;
    
        /** Number of 32 bit words required to hold it all. */
        uint32_t numWords() const;

        /** Get the data for the given word. */
        std::pair<const uint32_le *, uint32_t>
        getData(const uint32_le * data) const;

        uint64_t numBehaviorTableBits() const;

        uint64_t numTimestampTableBits() const
        {
            if (!hasTimestampTable)
                return 0;
            return (uint64_t)(timeBits) * numDistinctTimestamps;
        }

        uint64_t numTableIndexBits() const
        {
            return MLDB::highest_bit(numDistinctBehaviors - 1, -1) + 1;
        }

        int numEventTableTimestampBits() const
        {
            if (hasTimestampTable)
                return MLDB::highest_bit(numDistinctTimestamps - 1, -1) + 1;
            else return timeBits;
        }
    }  MLDB_PACKED;

    typedef SubjectIndexEntry2 SubjectIndexEntry;

    struct BehaviorStatsFormat;

    /** Make a copy of the data structure.  */
    virtual MappedBehaviorDomain * makeShallowCopy() const;

    /** Make a deep of the data structure. */
    virtual MappedBehaviorDomain * makeDeepCopy() const;

    /** Return the number of distinct behaviors for the subject. */
    uint32_t getSubjectNumDistinctBehaviors(SI index) const;

    /** Do we have the old format of subject index? */
    bool indexV1() const
    {
        return MLDB_UNLIKELY(md->version < 4);
    }

    SH getSubjectHash(SI index) const
    {
        if (indexV1())
            return SH(subjectIndex1[index.index()].key);
        else return SH(subjectIndex2[index.index()].key);
    }

    SI getSubjectIndex(SH hash) const
    {
        return SI(getSubjectIndexImpl(hash));
    }

    virtual bool knownSubject(SH subjectHash) const
    {
        if (indexV1())
            return subjectIndex1.indexOf(subjectHash) != -1;
        else return subjectIndex2.indexOf(subjectHash) != -1;
    }

    /** Do we know about this behavior? */
    virtual bool knownBehavior(BH behHash) const
    {
        return behaviorIndex.get(behHash, -1) != -1;
    }

    virtual bool
    forEachSubject(const OnSubject & onSubject,
                   const SubjectFilter & filter = SubjectFilter()) const;

    std::vector<std::pair<BH, uint32_t> >
    getSubjectBehaviorCounts(SH subjectHash, Order order = INORDER) const;

    using BehaviorDomain::getSubjectBehaviorCounts;

    virtual Id getSubjectId(SH subjectHash) const;

    virtual Id getBehaviorId(BI index) const;
    
    virtual Id getBehaviorId(BH behavior) const;

    virtual BehaviorStats getBehaviorStats(BI index, int fields) const;

    virtual BehaviorStats getBehaviorStats(BH behavior, int fields) const;

    virtual size_t
    getBehaviorSubjectCount(BI beh, SH maxSubject = SH::max(),
                             Precision p = EXACT) const;
    virtual size_t
    getBehaviorSubjectCount(BH beh, SH maxSubject = SH::max(),
                             Precision p = EXACT) const;

    virtual bool forEachSubjectBehaviorHash
        (SH subject,
         const OnSubjectBehaviorHash & onBeh,
         SubjectBehaviorFilter filter = SubjectBehaviorFilter(),
         Order order = INORDER) const;

    /** Return if the subject has at least n distinct behaviors. */
    virtual bool subjectHasNDistinctBehaviors(SH subject, int N) const;

    /** How many distinct subjects are known? */
    virtual size_t subjectCount() const
    {
        return md->numSubjects;
    }
    
    /** How many distinct behaviors are known? */
    virtual size_t behaviorCount() const
    {
        return md->numBehaviors;
    }

    /** Return a list of behaviors. */
    virtual std::vector<BH> allBehaviorHashes(bool sorted = false) const;

    /** Return a list of subjects. */
    virtual std::vector<SH>
    allSubjectHashes(SH maxSubject = SH(-1), bool sorted = false) const;

    virtual std::unique_ptr<BehaviorStream>
    getBehaviorStream(size_t start) const;

    virtual std::unique_ptr<SubjectStream>
    getSubjectStream(size_t start) const;

    virtual int coIterateBehaviors(BI beh1, BI beh2, 
                                    SH maxSubject = (SH)-1,
                                    const OnBehaviors & onBehaviors
                                        = OnBehaviors()) const;

    virtual int coIterateBehaviors(BH beh1, BH beh2, 
                                    SH maxSubject = (SH)-1,
                                    const OnBehaviors & onBehaviors
                                    = OnBehaviors()) const;

    virtual SubjectStats
    getSubjectStats(SI subjectIndex,
                    bool needDistinctBehaviors = true,
                    bool needDistinctTimestamps = false) const;

    virtual SubjectStats
    getSubjectStats(SH subjectHash,
                    bool needDistinctBehaviors = true,
                    bool needDistinctTimestamps = false) const;

    virtual std::pair<Date, Date>
    getSubjectTimestampRange(SI index) const;

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
    getSubjectHashesAndAllTimestamps(BH beh, SH maxSubject = SH(-1),
                                     bool sorted = false) const;

    virtual bool
    forEachBehaviorSubject(BH beh,
                            const OnBehaviorSubject & onSubject,
                            bool withTimestamps = false,
                            Order order = INORDER,
                            SH maxSubject = SH::max()) const ;

    virtual Date earliestTime() const
    {
        return unQuantizeTime(md->earliest);
    }

    virtual Date latestTime() const
    {
        return unQuantizeTime(md->latest);
    }

    /** Return the nominal start time in the file. */
    virtual Date nominalStart() const
    {
        return md->nominalStart;
    }

    virtual Date nominalEnd() const
    {
        return md->nominalEnd;
    }

    virtual int64_t totalEventsRecorded() const
    {
        return md->totalEventsRecorded;
    }

    virtual bool fileMetadataExists(const std::string & key) const;

    virtual Json::Value getFileMetadata(const std::string & key) const;

    virtual Json::Value getAllFileMetadata() const;

    virtual int64_t approximateMemoryUsage() const
    {
        return file.size();
    }

    Date unQuantizeTime(uint64_t tm) const
    {
        return Date::fromSecondsSinceEpoch(tm * md->timeQuantum);
    }

    int64_t getSubjectIndexImpl(SH subjectHash) const;

    /** Implement behavior co-iteration via a lookup when behi1 contains
        a lot more entries than behi2.
    */
    int coIterateBehaviorsLookup(BI behi1, BI behi2, 
                                  const BehaviorStatsFormat & e1,
                                  const BehaviorStatsFormat & e2,
                                  SH maxSubject,
                                  const OnBehaviors & onBehaviors) const;

    /** Implement behavior co-iteration via a scan when behi1 contains
        roughly as many entries as behi2.
    */
    int coIterateBehaviorsScan(BI behi1, BI behi2, 
                                const BehaviorStatsFormat & e1,
                                const BehaviorStatsFormat & e2,
                                SH maxSubject,
                                const OnBehaviors & onBehaviors) const;

    struct Metadata {
        uint64_le magic = 0;
        uint64_le version = 0;
        uint64_le behaviorIndexOffset = 0;
        uint64_le behaviorIdOffset = 0;
        uint64_le behaviorIdIndexOffset = 0;
        uint64_le behaviorInfoOffset = 0;
        uint64_le behaviorToSubjectsIndexOffset = 0;
        uint64_le behaviorSubjectsOffset = 0;
        uint64_le subjectDataOffset = 0;
        uint64_le subjectIndexOffset = 0;
        uint64_le earliest = 0, latest = 0;
        Date_le   nominalStart;  ///< Nominal start time (seconds since UTC)
        Date_le   nominalEnd;    ///< Nominal end time (seconds since UTC)
        uint32_le numBehaviors = 0;
        uint32_le numSubjects = 0;
        uint32_le minSubjects = 0;
        uint32_le unused1 = 0;
        double_le timeQuantum = 0;
        uint64_le subjectIdDataOffset = 0;
        uint64_le subjectIdIndexOffset = 0;
        uint64_le behaviorToSubjectTimestampsIndexOffset = 0;
        uint64_le behaviorToSubjectTimestampsOffset = 0;
        uint64_le idSpaceDeprecated = 0;
        uint64_le fileMetadataOffset = 0;
        uint64_le totalEventsRecorded = 0;
        uint64_le forExpansion[504] = {0};
    };

    /** This is the format into which behavior stats are mapped. */
    struct BehaviorStatsFormat {
        BH_le     hash = BH(0);     // 64 bit hash of behavior number
        uint64_le unused = 0;       // Used to be count
        uint32_le subjectCount = 0; // Number of subjects with this behavior
        uint32_le unused2 = 0;      // Gap in structure
        Date_le earliest = Date::positiveInfinity();
        Date_le latest   = Date::negativeInfinity();
    
        std::pair<Date, Date> timeRange() const
        {
            return std::make_pair(earliest, latest);
        }
    };

    MLDB::File_Read_Buffer file;

    MappedValue<Metadata> md;  // unaligned
    MappedSortedKeyValueArray<BH_le, uint32_le> behaviorIndex; /// beh -> behindex
    MappedValueArray<uint32_le> behaviorIdIndex;     /// behindex -> idoffset
    const char * behaviorIdStore;
    size_t behaviorIdStoreSize;
    MappedArray<BehaviorStatsFormat> behaviorStats; /// behindex -> info
    const uint32_le * subjectDataStore;
    MappedSortedKeyValueArray<SH_le, SubjectIndexEntry1> subjectIndex1; /// subjectid -> info, dataoffset
    MappedSortedKeyValueArray<SH_le, SubjectIndexEntry2> subjectIndex2; /// subjectid -> info, dataoffset
    std::vector<uint64_t> subjectMarks; // range starts for the subject index
    uint64_t lastSubjectHash;
    size_t numSHPerMark;
    const uint32_le * behaviorToSubjects;
    MappedValueArray<uint32_le> behaviorToSubjectsIndex;

    const uint32_le * behaviorToSubjectTimestamps;
    MappedValueArray<uint32_le> behaviorToSubjectTimestampsIndex;

    MappedValueArray<uint32_le> subjectIdIndex;
    const char * subjectIdStore;
    size_t subjectIdStoreSize;
    std::vector<size_t> subjectIdAdjustOffsets;

    Json::Value fileMetadata_;

    /// Internal structure used to access the behavior table
    struct BehaviorTable;

    /// Internal structure used to access the event table
    struct EventTable;

    /// Internal structure used to access the timestamp table
    struct TimestampTable;

    BehaviorTable
    getBehaviorTable(SH subj) const;
    
    BehaviorTable
    getBehaviorTable(SI subj) const;
    
    size_t extractBehaviorTable(SI subj, BH * output) const;

    EventTable
    getEventTable(SH subj) const;
    
    EventTable
    getEventTable(SI subj) const;

    TimestampTable
    getTimestampTable(SH subj) const;
    
    TimestampTable
    getTimestampTable(SI subj) const;

    template<typename SubjectIndex>
    int64_t
    getSubjectIndexImplTmpl(SH subjectHash, const SubjectIndex & subjectIndex) const;

};

static_assert(sizeof(MappedBehaviorDomain::SubjectIndexEntry2) == 24,
              "wrong mapped behavior domain size");


} // namespace MLDB
