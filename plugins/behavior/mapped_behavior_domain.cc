// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* mapped_behavior_domain.cc
   Jeremy Barnes, 4 April 2012
   Copyright (c) 2012 mldb.ai inc.

*/

#include "mapped_behavior_domain.h"
#include "mldb/arch/bitops.h"
#include "mldb/base/exc_assert.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/jml/db/persistent.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/types/jml_serialization.h"
#include "id_serialization.h"
#include <boost/iterator/iterator_facade.hpp>


using namespace std;
using namespace ML;


namespace {

constexpr size_t MAX_SUBJECT_MARKS = 32;

}


namespace MLDB {


/*****************************************************************************/
/* TABLES                                                                    */
/*****************************************************************************/

struct MappedBehaviorDomain::BehaviorTable {

    BehaviorTable()
        : data(0), behBits(0), behBits2(0), cntBits(0), size_(0),
          splitPos(0), owner(0)
    {
    }

    BehaviorTable(const uint32_t * data,
                   int behBits,
                   int cntBits,
                   int size,
                   int splitPos,
                   int behBits2,
                   const MappedBehaviorDomain * owner)
        : data(data), behBits(behBits), behBits2(behBits2), cntBits(cntBits),
          size_(size), splitPos(splitPos), owner(owner)
    {
    }

    const uint32_t * const data;
    const int behBits;
    const int behBits2;
    const int cntBits;
    const int size_;
    const int splitPos;
    const MappedBehaviorDomain * const owner;

    std::pair<BH, uint32_t> operator [] (int i) const
    {
        ML::Bit_Extractor<uint32_t> extractor(data);
        uint32_t beh, count;

        if (i < splitPos) {
            extractor.advance(i * (behBits + cntBits));
            extractor.extract(beh, behBits, count, cntBits);
        }
        else {
            extractor.advance(splitPos * (behBits + cntBits)
                              + (i - splitPos) * (behBits2 + cntBits));
            
            extractor.extract(beh, behBits2, count, cntBits);
        }
        return { owner->behaviorStats[beh].hash, count + 1 };
    }

    BH beh(int i) const
    {
        return owner->behaviorStats[behIndex(i)].hash;
    }

    size_t behIndex(int i) const
    {
        ML::Bit_Extractor<uint32_t> extractor(data);

        if (i < splitPos) {
            extractor.advance(i * (behBits + cntBits));
            return extractor.extract<uint32_t>(behBits);
        }
        else {
            ML::Bit_Extractor<uint32_t> extractor(data);
            extractor.advance(splitPos * (behBits + cntBits)
                              + (i - splitPos) * (behBits2 + cntBits));
            return extractor.extract<uint32_t>(behBits2);
        }
    }

    template<typename Fn>
    bool forEach(const Fn & onEntry) const
    {
        ML::Bit_Extractor<uint32_t> extractor(data);
        uint32_t beh, count;
        
        for (unsigned i = 0;  i < splitPos;  ++i) {
            extractor.extract(beh, behBits, count, cntBits);
            if (!onEntry(owner->behaviorStats[beh].hash, count + 1))
                return false;
        }

        for (unsigned i = splitPos;  i < size_;  ++i) {
            uint32_t beh, count;
            extractor.extract(beh, behBits2, count, cntBits);
            if (!onEntry(owner->behaviorStats[beh].hash, count + 1))
                return false;
        }
        
        return true;
    }

    template<typename Fn>
    bool forEachBeh(const Fn & onEntry) const
    {
        ML::Bit_Extractor<uint32_t> extractor(data);
        
        for (unsigned i = 0;  i < splitPos;  ++i) {
            uint32_t behIndex = extractor.extract<uint32_t>(behBits);
            BH beh = owner->behaviorStats[behIndex].hash;
            if (!onEntry(beh))
                return false;
            extractor.advance(cntBits);
        }

        for (unsigned i = splitPos;  i < size_;  ++i) {
            uint32_t behIndex = extractor.extract<uint32_t>(behBits2);
            BH beh = owner->behaviorStats[behIndex].hash;
            if (!onEntry(beh))
                return false;
            extractor.advance(cntBits);
        }
        
        return true;
    }

    bool empty() const { return size_ == 0; }
    size_t size() const { return size_; }
};


struct MappedBehaviorDomain::TimestampTable {

    TimestampTable(uint64_t timeOffset,
                   const MappedBehaviorDomain * owner)
        : extractor(0), hasTable(false), timeOffset(timeOffset),
          tsOffsetBits(0), size_(0), owner(owner)
    {
    }

    TimestampTable(ML::Bit_Extractor<uint32_t> extractor,
                   bool hasTable,
                   uint64_t timeOffset,
                   int tsOffsetBits,
                   int size,
                   const MappedBehaviorDomain * owner)
        : extractor(extractor),
          hasTable(hasTable),
          timeOffset(timeOffset),
          tsOffsetBits(tsOffsetBits),
          size_(size),
          owner(owner)
    {
    }

    ML::Bit_Extractor<uint32_t> extractor;
    bool hasTable;
    uint64_t timeOffset;
    const int tsOffsetBits;
    const int size_;
    const MappedBehaviorDomain * const owner;

    uint32_t lookup(uint32_t offset) const
    {
        ExcAssertLess(offset, size_);
        ML::Bit_Extractor<uint32_t> extractor(this->extractor);
        extractor.advance(offset * tsOffsetBits);
        return extractor.extract<uint32_t>(tsOffsetBits);
    }

    Date decode(uint32_t time) const
    {
        if (hasTable)
            time = lookup(time);
        return owner->unQuantizeTime(owner->md->earliest + timeOffset + time);
    }

    bool empty() const { return size_ == 0; }
    size_t size() const { return size_; }
};


struct MappedBehaviorDomain::EventTable {
    EventTable()
        : owner(0), subj(-1), extractor(0), size_(0), timeBits(0), behBits(0), cntBits(0), timeOffset(0)
    {
    }

    EventTable(const MappedBehaviorDomain * owner,
               SI subj,
               const ML::Bit_Extractor<uint32_t> & extractor,
               size_t size,
               int timeBits,
               int behBits,
               int cntBits,
               uint64_t timeOffset)
        : owner(owner), subj(subj), extractor(extractor), size_(size), timeBits(timeBits),
          behBits(behBits), cntBits(cntBits), timeOffset(timeOffset)
    {
    }

    const MappedBehaviorDomain * const owner;
    SI subj;
    ML::Bit_Extractor<uint32_t> extractor;
    size_t size_;
    int timeBits;
    int behBits;
    int cntBits;
    uint64_t timeOffset;

    template<typename Fn>
    bool forEachRaw(const Fn & onEntry, unsigned startAt = 0) const
    {
        ML::Bit_Extractor<uint32_t> extractor = this->extractor;

        if (startAt)
            extractor.advance(startAt * (timeBits + behBits + cntBits));

        for (unsigned i = startAt;  i < size_;  ++i) {
            uint32_t time, behIndex, count;
            extractor.extract(time, timeBits,
                              behIndex, behBits,
                              count, cntBits);

            if (!onEntry(behIndex, time, count + 1))
                return false;
        }

        return true;
    }

    uint32_t lowerBoundTimestamp(Date ts, const TimestampTable & tst) const
    {
        uint32_t lower = 0, upper = size_;

        for (;;) {
            uint32_t range = upper - lower;

            // Linear search if there is a small range
            if (range <= 32) {
                ML::Bit_Extractor<uint32_t> extractor = this->extractor;
                extractor.advance(lower * (behBits + cntBits + timeBits));

                for (unsigned i = lower;  i < upper;  ++i) {
                    uint32_t time;
                    extractor.extract(time, timeBits);

                    Date decoded = tst.decode(time);

                    //cerr << "i = " << i << " decoded = " << decoded
                    //     << " looking for " << ts << endl;

                    if (decoded >= ts)
                        return i;
                    
                    extractor.advance(behBits + cntBits);
                }
             
                return upper;
            }
        
            // Otherwise, binary search
            uint32_t middle = (upper + lower) / 2;

            ML::Bit_Extractor<uint32_t> extractor = this->extractor;
            extractor.advance(middle * (behBits + cntBits + timeBits));
            
            uint32_t time;
            extractor.extract(time, timeBits);

            Date decoded = tst.decode(time);

            //cerr << "lower = " << lower << " upper = " << upper
            //     << " middle = " << middle << " decoded = " << decoded
            //     << " looking for " << ts << endl;

            if (decoded >= ts) {
                upper = middle;
            }
            else {
                lower = middle;
            }
        }


        ML::Bit_Extractor<uint32_t> extractor = this->extractor;

        for (unsigned i = 0;  i < size_;  ++i) {
            uint32_t time;
            extractor.extract(time, timeBits);

            if (tst.decode(time) >= ts)
                return i;

            extractor.advance(behBits + cntBits);
        }

        return size_;
    }

    template<typename Fn>
    bool forEach(const Fn & onEntry, Date earliest, Date latest) const
    {
        //cerr << "forEachBehavior" << endl;

        //cerr << "size_ = " << size_ << endl;

        //cerr << "timeBits " << timeBits << " behBits " << behBits << " cntBits "
        //     << cntBits << endl;

        ML::Bit_Extractor<uint32_t> extractor = this->extractor;

        BehaviorTable behs = owner->getBehaviorTable(subj);
        TimestampTable tst  = owner->getTimestampTable(subj);
     
        //cerr << "behs.size() = " << behs.size() << endl;
        //cerr << "tst.size() = " << tst.size() << endl;

        bool stopped = false;

        uint32_t startAt = 0;
        if (earliest.isADate()) {
            startAt = lowerBoundTimestamp(earliest, tst);
        }

        //cerr << "startAt = " << startAt << " earliest = " << earliest
        //     << " size_ = " << size_ << endl;

        auto onEntry2 = [&] (uint32_t behIndex, uint32_t time, uint32_t count) -> bool
            {
                //cerr << "raw: " << behIndex << " " << time << " " << count << endl;
                
                Date ts = tst.decode(time);
                //cerr << "ts = " << ts << " earliest " << earliest << " latest " << latest << endl;

                if (ts < earliest)  // skip forward since not at right time
                    return true;
                if (ts > latest)    // stop since we're after the required time
                    return false;
                ExcAssertLess(behIndex, behs.size());
                BH beh = behs.beh(behIndex);
            
                //cerr << "is beh " << beh << " ts " << ts << " count " << count
                //     << owner->getBehaviorId(beh) << endl;
                
                bool res = onEntry(beh, ts, count);
                if (!res) {
                    stopped = true;
                }
                return res;
            };

        forEachRaw(onEntry2, startAt);

        return !stopped;
    }
    
    template<typename Fn>
    bool forEachTimestamp(const Fn & onEntry) const
    {
        ML::Bit_Extractor<uint32_t> extractor = this->extractor;
        TimestampTable tst  = owner->getTimestampTable(subj);

        for (unsigned i = 0;  i < size_;  ++i) {
            Date ts = tst.decode(extractor.extract<uint32_t>(timeBits));
            extractor.advance(behBits + cntBits);

            if (!onEntry(ts))
                return false;
        }

        return true;
    }

    size_t size() const
    {
        return size_;
    }

    bool empty() const
    {
        return size_ == 0;
    }

    Date timestamp(int index) const
    {
        ML::Bit_Extractor<uint32_t> extractor = this->extractor;
        extractor.advance(index * (timeBits + behBits + cntBits));
        TimestampTable tst  = owner->getTimestampTable(subj);
        return tst.decode(extractor.extract<uint32_t>(timeBits));
    }
    
};



/*****************************************************************************/
/* INDEX ENTRIES                                                             */
/*****************************************************************************/


uint64_t
MappedBehaviorDomain::SubjectIndexEntry1::
numIndexBits() const
{
    int numBehIndexBits
        = (indexCountBits + behBits) * numDistinctBehaviors;
    return numBehIndexBits;
}

uint64_t
MappedBehaviorDomain::SubjectIndexEntry1::
numBits() const
{
    if (numBehaviors == 0) return 0;
    int numBehIndexBits = numIndexBits();
    int behNumBits = numTableIndexBits();
    int numTimeIndexBits
        = (timeBits + behNumBits + countBits) * numBehaviors;
    int numBits = numBehIndexBits + numTimeIndexBits;
    return numBits;
}
    
uint32_t
MappedBehaviorDomain::SubjectIndexEntry1::
numWords() const
{
    uint32_t numWords = (numBits() + 31) / 32;
    return numWords;
}

std::pair<const uint32_t *, uint32_t>
MappedBehaviorDomain::SubjectIndexEntry1::
getData(const uint32_t * data) const
{
    int nw = numWords();
    if (nw <= 1)
        data = &offsetLow;
    else {
        //cerr << "entry.offset = " << entry.offset << endl;
        data = data + offsetLow;
    }
    
    return make_pair(data, nw);
}




uint64_t
MappedBehaviorDomain::SubjectIndexEntry2::
numIndexBits() const
{
    return numBehaviorTableBits() + numTimestampTableBits();
}

uint64_t
MappedBehaviorDomain::SubjectIndexEntry2::
numBits() const
{
    if (numBehaviors == 0)
        return 0;

    uint64_t behNumBits = numTableIndexBits();
    uint64_t numEventBits
        = (numEventTableTimestampBits() + behNumBits + countBits)
        * numBehaviors;
    uint64_t numBits = numIndexBits() + numEventBits;
    return numBits;
}
    
uint32_t
MappedBehaviorDomain::SubjectIndexEntry2::
numWords() const
{
    uint32_t numWords = (numBits() + 31) / 32;
    return numWords;
}

std::pair<const uint32_t *, uint32_t>
MappedBehaviorDomain::SubjectIndexEntry2::
getData(const uint32_t * data) const
{
    int nw = numWords();
    if (nw <= 1)
        data = &offsetLow;
    else {
        //cerr << "entry.offset = " << entry.offset << endl;
        data = data + offset();
    }
    
    return make_pair(data, nw);
}


uint64_t
MappedBehaviorDomain::SubjectIndexEntry2::
numBehaviorTableBits() const
{
    if (!hasBehaviorTable)
        return 0;
    if (!splitBehaviorTable)
        return (uint64_t)(behBits + indexCountBits)
            * (uint64_t)numDistinctBehaviors;
    
    // It's split into two tables, one with behBits for the behavior number
    // and one with behBits2 for the behavior number

    int splitPoint = behTableSplitValue * numDistinctBehaviors / 256;

    uint64_t bits
        = (uint64_t)indexCountBits * numDistinctBehaviors
        + (uint64_t)behBits * splitPoint
        + (uint64_t)behTable2Bits * (numDistinctBehaviors - splitPoint);

    return bits;
}


/*****************************************************************************/
/* MAPPED BEHAVIOR DOMAIN                                                   */
/*****************************************************************************/

MappedBehaviorDomain::
MappedBehaviorDomain()
{
}

MappedBehaviorDomain::
MappedBehaviorDomain(const std::string & filename)
{
    load(filename);
}

MappedBehaviorDomain::
MappedBehaviorDomain(const ML::File_Read_Buffer & file)
{
    load(file);
}

void
MappedBehaviorDomain::
load(const std::string & filename)
{
    file.open(filename);
    init(file, readTrailingOffset(file));
}

void
MappedBehaviorDomain::
load(const ML::File_Read_Buffer & file)
{
    this->file = file;
    init(file, readTrailingOffset(file));
}

void
MappedBehaviorDomain::
init(const File_Read_Buffer & file, uint64_t md_offset)
{
    this->file = file;
    md.init(file, md_offset);

    if (md->magic != magicStr("BehsHour"))
        throw MLDB::Exception("invalid behavior magic");

    if (md->version > 5)
        throw MLDB::Exception("invalid behavior version");
    if (md->idSpaceDeprecated != 0)
        throw MLDB::Exception("id space must be equal to 0");

    timeQuantum = md->timeQuantum;
    minSubjects = md->minSubjects;

    //ExcAssertGreater(md->nominalStart, Date());
    //ExcAssertGreater(md->nominalEnd, md->nominalStart);
    //cerr << "nominalStart = " << md->nominalStart << " nominalEnd = "
    //     << md->nominalEnd << endl;

    //cerr << "behavior index at " << md->behaviorIndexOffset << endl;

    behaviorIndex.init(file, md->behaviorIndexOffset, md->numBehaviors);
    behaviorIdIndex.init(file, md->behaviorIdIndexOffset,
                          md->numBehaviors);
    behaviorIdStore = file.start() + md->behaviorIdOffset;
    behaviorIdStoreSize = md->behaviorInfoOffset - md->behaviorIdOffset;
    behaviorStats.init(file, md->behaviorInfoOffset, md->numBehaviors);
    subjectDataStore
        = (const uint32_t *)(file.start() + md->subjectDataOffset);

    behaviorToSubjects
        = (const uint32_t *)(file.start() + md->behaviorSubjectsOffset);
    behaviorToSubjectsIndex.init(file, md->behaviorToSubjectsIndexOffset,
                                  md->numBehaviors);

    // Version 1 added optional subject IDs
    if (md->version >= 1
        && (md->subjectIdDataOffset > 0 || md->numSubjects == 0)) {
        subjectIdStore = file.start() + md->subjectIdDataOffset;
        subjectIdStoreSize = md->subjectIdIndexOffset - md->subjectIdDataOffset;
        subjectIdIndex.init(file, md->subjectIdIndexOffset, md->numSubjects);
        hasSubjectIds = true;
    }
    else {
        hasSubjectIds = false;
    }

    // In version 2, we add extra data about the earliest timestamp for
    // each subject in the behavior index
    if (md->version >= 2) {
        behaviorToSubjectTimestamps
            = (const uint32_t *)
                (file.start()
                 + md->behaviorToSubjectTimestampsOffset);
        behaviorToSubjectTimestampsIndex
            .init(file,
                  md->behaviorToSubjectTimestampsIndexOffset,
                  md->numBehaviors);
    }
    else {
        behaviorToSubjectTimestamps = 0;
    }

    // Version 3 adds metadata
    if (md->version >= 3) {
        DB::Store_Reader store(file);
        store.skip(md->fileMetadataOffset);
        unsigned char c;
        store >> c;
        if (c != 0)
            throw MLDB::Exception("unknown file metadata name");
        std::string s;
        store >> s;
        fileMetadata_ = Json::parse(s);
    }
    else fileMetadata_ = Json::Value();

    /* Slicing the subject index in ranges of equal sizes enables us to
       alleviate the loss of performance (in the form of cache misses)
       occurring during lookups in "getSubjectIndex" in the case where the
       distribution of keys in "subjectIndexX" is not balanced enough. By
       reducing the size of the ranges where the lookups are performed, any
       imabalance becomes more constrained in "space". */
    subjectMarks.reserve(MAX_SUBJECT_MARKS);
    numSHPerMark = md->numSubjects / MAX_SUBJECT_MARKS;

    // Version 4 has version 2 subject entries
    if (md->version >= 4) {
        subjectIndex2.init(file, md->subjectIndexOffset, md->numSubjects);
        size_t max = std::min(MAX_SUBJECT_MARKS, (size_t) md->numSubjects);
        for (size_t i = 0; i < max; i++) {
            size_t idx = i * numSHPerMark;
            subjectMarks.push_back(subjectIndex2[idx].key.hash());
        }
        if (md->numSubjects > 0) {
            lastSubjectHash = subjectIndex2[md->numSubjects-1].key.hash();
        }
    }
    else {
        subjectIndex1.init(file, md->subjectIndexOffset, md->numSubjects);
        size_t max = std::min(MAX_SUBJECT_MARKS, (size_t) md->numSubjects);
        for (size_t i = 0; i < max; i++) {
            size_t idx = i * numSHPerMark;
            subjectMarks.push_back(subjectIndex1[idx].key.hash());
        }
        if (md->numSubjects > 0) {
            lastSubjectHash = subjectIndex1[md->numSubjects-1].key.hash();
        }
    }

    // Version 5 has split behavior tables

    const Metadata & md2 = *md;

    vector<pair<string, uint64_t> > offsets = {
        { "behaviorIndex", md2.behaviorIndexOffset },
        { "behaviorIdIndex", md2.behaviorIdIndexOffset },
        { "behaviorIdStore", md2.behaviorIdOffset },
        { "behaviorStats", md2.behaviorInfoOffset },
        { "subjectData", md2.subjectDataOffset },
        { "behaviorToSubjects", md2.behaviorSubjectsOffset },
        { "behaviorToSubjectIndex", md2.behaviorToSubjectsIndexOffset },
        { "subjectIds", md2.subjectIdDataOffset },
        { "subjectIdIndex", md2.subjectIdIndexOffset },
        { "behaviorToSubjectTimestamps", md2.behaviorToSubjectTimestampsOffset },
        { "behaviorToSubjectTimestampsIndex", md2.behaviorToSubjectTimestampsIndexOffset },
        { "fileMetadata", md2.fileMetadataOffset },
        { "subjectIndex", md2.subjectIndexOffset },
        { "eof", file.size() } };

    ML::sort_on_second_ascending(offsets);

    cerr << "size map: " << endl;
    for (unsigned i = 0;  i < offsets.size() - 1;  ++i) {
        cerr << MLDB::format("%35s %12.3fMB %8.4f%%\n",
                           offsets[i].first.c_str(),
                           (offsets[i + 1].second - offsets[i].second) / 1000000.0,
                           100.0 * (offsets[i + 1].second - offsets[i].second) / file.size());
    }
    
    // Subject ID offsets are stored with 32 bits so if that's not enough then
    // we need to find all the places where the offsets overflow in
    // subjectIdIndex and build a table to adjust the offset Id when we load
    // them up.
    if (md->subjectIdIndexOffset - md->subjectIdDataOffset > uint32_t(-1)) {
        for (size_t i = 1; i < md->numSubjects; ++i) {
            if (subjectIdIndex[i-1] > subjectIdIndex[i])
                subjectIdAdjustOffsets.push_back(i);
        }
    }

    //cerr << "earliest = " << md->earliest << endl;
    //cerr << "latest = " << md->latest << endl;

#if 0
    for (auto & s: subjectIndex2) {
        SubjectIndexEntry2 entry = s.value;

        cerr << "hash " << s.key
             <<  "numDistinctBehaviors " << entry.numDistinctBehaviors
             << " earliestTime " << entry.earliestTime
             << " numDistinctTimestamps " << entry.numDistinctTimestamps
             << " numBehaviors " << entry.numBehaviors
             << " behBits " << entry.behBits
             << " timeBits " << entry.timeBits
             << " indexCountBits " << entry.indexCountBits
             << " countBits " << entry.countBits
             << " verbBits " << entry.verbBits
             << " version " << entry.version
             << " hasBehTable " << entry.hasBehaviorTable
             << " hasTsTable " << entry.hasTimestampTable
             << " offset "<< entry.offset()
             << " indexBytes " << entry.numIndexBits() / 8
             << " behTableBytes " << entry.numBehaviorTableBits() / 8
             << " tableIndexBytes " << entry.numTableIndexBits() / 8
             << " bytes " << entry.numWords() * 4
             << endl;

        auto etbl = getBehaviorTable(s.key);

        for (unsigned i = 0;  i < etbl.size();  ++i) {
            cerr << "  entry " << i << " index " << etbl.behIndex(i) << endl;
        }
    }
#endif
}

MappedBehaviorDomain *
MappedBehaviorDomain::
makeShallowCopy() const
{
    return new MappedBehaviorDomain(*this);
}

MappedBehaviorDomain *
MappedBehaviorDomain::
makeDeepCopy() const
{
    return new MappedBehaviorDomain(*this);
}

bool
MappedBehaviorDomain::
forEachSubject(const OnSubject & onSubject,
               const SubjectFilter & filter) const
{
    for (unsigned i = 0;  i < subjectCount();  ++i) {
        SubjectIterInfo info;
        if (!onSubject(getSubjectHash(SI(i)), info))
            return false;
    }
    return true;
}

uint32_t
MappedBehaviorDomain::
getSubjectNumDistinctBehaviors(SI index) const
{
    if (indexV1()) {
        const SubjectIndexEntry1 & entry = subjectIndex1[index].value;
        return entry.numDistinctBehaviors;
    }
    else {
        const SubjectIndexEntry2 & entry = subjectIndex2[index].value;
        return entry.numDistinctBehaviors;
    }
}

bool
MappedBehaviorDomain::
subjectHasNDistinctBehaviors(SH subj, int n) const
{
    int index = getSubjectIndexImpl(subj);
    if (index == -1) return n == 0;

    return getSubjectNumDistinctBehaviors(SI(index)) >= n;
}

std::vector<std::pair<BH, uint32_t> >
MappedBehaviorDomain::
getSubjectBehaviorCounts(SH subj, Order order) const
{
    //cerr << "getSubjectBehaviorCounts(" << subj << ")" << endl;

    vector<pair<BH, uint32_t> > result;

    getBehaviorTable(subj).forEach([&] (BH beh, uint32_t count)
                                    {
                                        result.push_back(make_pair(beh, count));
                                        return true;
                                    });
    
    if (order == INORDER)
        std::sort(result.begin(), result.end());
    
    return result;
}

MappedBehaviorDomain::BehaviorTable
MappedBehaviorDomain::
getBehaviorTable(SH subj) const
{
    int index = getSubjectIndexImpl(subj);

    if (index == -1) return BehaviorTable();
    return getBehaviorTable(SI(index));
    
}

MappedBehaviorDomain::BehaviorTable
MappedBehaviorDomain::
getBehaviorTable(SI subj) const
{
    if (indexV1()) {
        const SubjectIndexEntry1 & entry = subjectIndex1[subj].value;

        const uint32_t * data;
        int nw;
        std::tie(data, nw) = entry.getData(subjectDataStore);
        
        //cerr << "getBehaviorTable for " << subj << " with "
        //     << nw << " words and " << entry.numDistinctBehaviors
        //     << " distinct behaviors" << endl;
            
        return BehaviorTable(data, entry.behBits,
                              entry.indexCountBits,
                              entry.numDistinctBehaviors,
                              entry.numDistinctBehaviors /* split pos */,
                              -1 /* beh bits 2 */,
                              this);
    }
    else {
        const SubjectIndexEntry2 & entry = subjectIndex2[subj].value;

        const uint32_t * data;
        int nw;
        std::tie(data, nw) = entry.getData(subjectDataStore);

        if (entry.splitBehaviorTable) {
            // Behavior table is split into two entries
            int splitPos = entry.behTableSplitValue * entry.numDistinctBehaviors / 256;

            return BehaviorTable(data,
                                  entry.behBits,
                                  entry.indexCountBits,
                                  entry.numDistinctBehaviors,
                                  splitPos,
                                  entry.behTable2Bits,
                                  this);
        }
        else {
            return BehaviorTable(data,
                                  entry.behBits,
                                  entry.indexCountBits,
                                  entry.numDistinctBehaviors,
                                  entry.numDistinctBehaviors /* split pos */,
                                  -1 /* beh bits 2 */,
                                  this);
        }
    }
}

MappedBehaviorDomain::TimestampTable
MappedBehaviorDomain::
getTimestampTable(SH subj) const
{
    int index = getSubjectIndexImpl(subj);
    if (index == -1) return TimestampTable(0, 0);
    return getTimestampTable(SI(index));
}
    
MappedBehaviorDomain::TimestampTable
MappedBehaviorDomain::
getTimestampTable(SI subj) const
{
    if (indexV1()) {
        const SubjectIndexEntry1 & entry = subjectIndex1[subj].value;
        return TimestampTable(entry.earliestTime, this);
    }
    else {
        const SubjectIndexEntry2 & entry = subjectIndex2[subj].value;

        const uint32_t * data;
        int nw;
        std::tie(data, nw) = entry.getData(subjectDataStore);

        ML::Bit_Extractor<uint32_t> extractor(data);

        // Skip the behavior table to get to the timestamp table
        extractor.advance(entry.numBehaviorTableBits());

        return TimestampTable(extractor, entry.hasTimestampTable,
                              entry.earliestTime, entry.timeBits,
                              entry.numDistinctTimestamps,
                              this);
    }
}

MappedBehaviorDomain::EventTable
MappedBehaviorDomain::
getEventTable(SH subj) const
{
    int index = getSubjectIndexImpl(subj);
    if (index == -1) return EventTable();
    return getEventTable(SI(index));
}

MappedBehaviorDomain::EventTable
MappedBehaviorDomain::
getEventTable(SI subj) const
{
    if (indexV1()) {
        const SubjectIndexEntry1 & entry = subjectIndex1[subj].value;

#if 0
        cerr << "entry.numDistinctBehaviors = " << entry.numDistinctBehaviors << endl;
        cerr << "entry.numWords = " << entry.numWords() << endl;
        cerr << "entry.behBits = " << entry.behBits << endl;
        cerr << "entry.offsetLow = " << entry.offsetLow << endl;
        cerr << "entry.indexCountBits = " << entry.indexCountBits << endl;
        cerr << "entry.timeBits = " << entry.timeBits << endl;
        cerr << "entry.countBits = " << entry.countBits << endl;
#endif

        const uint32_t * data;
        int nw;
        std::tie(data, nw) = entry.getData(subjectDataStore);

        ML::Bit_Extractor<uint32_t> extractor(data);

        // Skip the behavior table
        extractor.advance(entry.numBehaviorTableBits());

        int behIndexBits = entry.numTableIndexBits();

        return EventTable(this, subj, extractor, entry.numBehaviors,
                          entry.timeBits, behIndexBits, entry.countBits,
                          md->earliest + entry.earliestTime);
    }
    else {
        const SubjectIndexEntry2 & entry = subjectIndex2[subj].value;

        const uint32_t * data;
        int nw;
        std::tie(data, nw) = entry.getData(subjectDataStore);

        ML::Bit_Extractor<uint32_t> extractor(data);

        // Skip the behavior table and the timestamp table
        extractor.advance(entry.numBehaviorTableBits()
                          + entry.numTimestampTableBits());

        int behIndexBits = entry.numTableIndexBits();

        return EventTable(this, subj, extractor, entry.numBehaviors,
                          entry.numEventTableTimestampBits(),
                          behIndexBits, entry.countBits,
                          md->earliest + entry.earliestTime);

    }

    throw MLDB::Exception("getEventTable");
}


size_t
MappedBehaviorDomain::
extractBehaviorTable(SI subj, BH * output) const
{
    auto table = getBehaviorTable(subj);
    int i = 0;
    table.forEachBeh([&] (BH beh) { output[i++] = beh;  return true; });
    return i;
}

bool
MappedBehaviorDomain::
forEachSubjectBehaviorHash(SH subj,
                            const OnSubjectBehaviorHash & onBeh,
                            SubjectBehaviorFilter filter,
                            Order inOrder) const
{
    int index = getSubjectIndexImpl(subj);
    if (index == -1) return true;

    // Access the event table
    EventTable events = getEventTable(SI(index));

    // We need to reorder those with the same timestamp into behavior
    // order since they are sorted in behavior index order.
    Date currentTs = Date::negativeInfinity();

    // Try to do small ones on the stack
    char stackCurrentStorage[4096 * sizeof(std::pair<BH, uint32_t>)];

    auto stackCurrent = (std::pair<BH, uint32_t> *)stackCurrentStorage;
    std::pair<BH, uint32_t> * ptr = stackCurrent;
    std::unique_ptr<std::pair<BH, uint32_t>[]> allocCurrent;
    size_t n = 0;
    size_t capacity = 4096;

    auto addCurrent = [&] (BH beh, uint32_t count)
        {
            if (n == capacity) {
                capacity *= 2;
                std::unique_ptr<std::pair<BH, uint32_t>[] > newCurrent
                    (new std::pair<BH, uint32_t>[capacity]);
                std::copy(ptr, ptr + n, newCurrent.get());
                allocCurrent = std::move(newCurrent);
                ptr = allocCurrent.get();
            }
            ExcAssertLess(n, capacity);
            ptr[n++] = { beh, count };
        };


    auto drainCurrent = [&] ()
        {
            std::sort(ptr, ptr + n);
            for (auto it = ptr, end = ptr + n;
                 it != end;  ++it) {
                if (!onBeh(it->first, currentTs, it->second))
                    return false;
            }
            n = 0;
            return true;
        };

    auto doBeh = [&] (BH beh, Date ts, uint32_t count)
        {
            ExcAssertGreaterEqual(ts, filter.earliest);
            ExcAssertLessEqual(ts, filter.latest);

            if (!filter.pass(beh, ts, count))
                return true;

            if (inOrder == ANYORDER) {
                return onBeh(beh, ts, count);
            }

            if (ts != currentTs) {
                if (!drainCurrent())
                    return false;
            }
            addCurrent(beh, count);
            currentTs = ts;
            return true;
        };

    if (!events.forEach(doBeh, filter.earliest, filter.latest))
        return false;
    
    return drainCurrent();
}

int
MappedBehaviorDomain::
numDistinctTimestamps(SH subjectHash, uint32_t maxValue) const
{
    int index = getSubjectIndexImpl(subjectHash);
    if (index == -1) return 0;
    
    if (indexV1()) {
        // Need to calculate it
        auto events = getEventTable(SI(index));
        if (events.empty())
            return 0;

        Date prev = Date();
        int result = 0;

        events.forEachTimestamp([&] (Date ts)
                                {
                                    if (ts != prev) {
                                        ++result;
                                        prev = ts;
                                    }
                                    return result < maxValue;
                                });

        return result;
    }
    else {
        // Index v2 records it
        const SubjectIndexEntry2 & entry = subjectIndex2[index].value;
        return entry.numDistinctTimestamps;
    }
}

std::pair<Date, Date>
MappedBehaviorDomain::
getSubjectTimestampRange(SI index) const
{
    EventTable events = getEventTable(index);
    if (events.empty())
        return make_pair(Date::notADate(), Date::notADate());

    return make_pair(events.timestamp(0), events.timestamp(events.size() - 1));
}

std::pair<Date, Date>
MappedBehaviorDomain::
getSubjectTimestampRange(SH subjectHash) const
{
    int index = getSubjectIndexImpl(subjectHash);
    if (index == -1)
        return make_pair(Date::notADate(), Date::notADate());
    return getSubjectTimestampRange(SI(index));
}

BehaviorDomain::SubjectStats
MappedBehaviorDomain::
getSubjectStats(SI index,
                bool needDistinctBehaviors,
                bool needDistinctTimestamps) const
{
    SubjectStats result;

    if (index == -1) return result;
    
    if (indexV1()) {
        const SubjectIndexEntry1 & entry = subjectIndex1[index].value;

        std::tie(result.earliest, result.latest)
            = getSubjectTimestampRange(index);

        result.numBehaviors = entry.numBehaviors;
        result.numDistinctBehaviors = entry.numDistinctBehaviors;
        result.numDistinctTimestamps = 0;
    }
    else {
        const SubjectIndexEntry2 & entry = subjectIndex2[index].value;

        std::tie(result.earliest, result.latest)
            = getSubjectTimestampRange(index);
        
        result.numBehaviors = entry.numBehaviors;
        result.numDistinctBehaviors = entry.numDistinctBehaviors;
        result.numDistinctTimestamps = entry.numDistinctTimestamps;
    }

    return result;
}

BehaviorDomain::SubjectStats
MappedBehaviorDomain::
getSubjectStats(SH subjectHash,
                bool needDistinctBehaviors,
                bool needDistinctTimestamps) const
{
    int index = getSubjectIndexImpl(subjectHash);
    if (index == -1)
        return SubjectStats();
    return getSubjectStats(SI(index), 
                           needDistinctBehaviors,
                           needDistinctTimestamps);
}

template<typename SubjectIndex>
int64_t
MappedBehaviorDomain::
getSubjectIndexImplTmpl(SH subjectHash, const SubjectIndex & subjectIndex) const
{
    size_t sz = subjectIndex.size();
    if (sz == 0
        || subjectHash.hash() < subjectMarks[0]
        || subjectHash.hash() > lastSubjectHash) {
        return -1;
    }

    /* Lookup of subregion in "subjectMarks".

       We perform a binary search of the highest subject value less than or
       equal to "subjectHash" in "subjectMarks". Along with "numSHPerMark",
       this enable us to deduce the range of indexes to which "subjectIndex"
       belongs. */
    size_t rangeStart;

    auto it = std::lower_bound(subjectMarks.begin(), subjectMarks.end(),
                               subjectHash.hash());
    if (it == subjectMarks.end()) {
        rangeStart = subjectMarks.size() - 1;
    }
    else {
        rangeStart = distance(subjectMarks.begin(), it);
        if (subjectMarks[rangeStart] > subjectHash.hash()) {
            rangeStart--;
        }
    }

    size_t indexOffset = rangeStart * numSHPerMark;
    uint64_t firstSH = subjectIndex[indexOffset].key.hash();

    uint64_t lastSH;
    if (rangeStart < subjectMarks.size() - 1) {
        size_t lastIdx = (rangeStart + 1) * numSHPerMark;
        lastSH = subjectIndex[lastIdx].key.hash();
    }
    else {
        lastSH = lastSubjectHash;
    }

    /* We compute the likely location of "subjectHash" by rescaling the range
       of indexes deduced above to the range of corresponding SH values. When
       the hashes are perfectly distributed, we should be able to hit the
       right entry nearly straight on with a direct lookup. */
    double factor = double(numSHPerMark-1) / (lastSH - firstSH);
    //double propn = subjectHash * factor;
    auto subjectDelta = subjectHash.hash() - firstSH;
    int64_t testIndex
        = std::min<int64_t>(sz - 1, indexOffset + (subjectDelta * factor));

    /* We refine the result in case of a miss by another interpolation based
     * on the difference with the actual result. */
    SH atIndex = subjectIndex[testIndex].key;
    int64_t diff = (int64_t)subjectHash - (int64_t)atIndex;
    int offBy = diff * factor;
    testIndex = testIndex + offBy;
    if (testIndex < 0) {
        testIndex = 0;
    }
    else if (testIndex > sz - 1) {
        testIndex = sz - 1;
    }

    /* We find the final position by adjusting "testIndex" one step at a time. */
    while (testIndex < (sz-1) && subjectIndex[testIndex].key < subjectHash)
        ++testIndex;
    while (testIndex > 0 && subjectIndex[testIndex].key > subjectHash)
        --testIndex;
    
    if (subjectIndex[testIndex].key != subjectHash)
        testIndex = -1;

    return testIndex;
}

int64_t
MappedBehaviorDomain::
getSubjectIndexImpl(SH subj) const
{
    if (indexV1())
        return getSubjectIndexImplTmpl(subj, subjectIndex1);
    else
        return getSubjectIndexImplTmpl(subj, subjectIndex2);
}

Id
MappedBehaviorDomain::
getSubjectId(SH subjectHash) const
{
    if (!hasSubjectIds)
        throw MLDB::Exception("attempt to get subject ID with none present");
    int index = getSubjectIndex(subjectHash);
    if (index == -1)
        throw MLDB::Exception("getSubjectId for unknown subject");

    uint64_t offset = subjectIdIndex[index];

    // Adjustments for offset overflows.
    for (size_t i : subjectIdAdjustOffsets) {
        if (index < i) break;
        offset += 1ULL << 32;
    }

    DB::Store_Reader store(subjectIdStore, subjectIdStoreSize);
    store.skip(offset);
    Id result;
    store >> result;

    // NOTE: if the IDs are encoded, then result.hash() will not be equal
    // to the subject hash.  Hence this check is disabled.
    //ExcAssertEqual(result.hash(), subjectHash);

    return result;
}

Id
MappedBehaviorDomain::
getBehaviorId(BI index) const
{
    uint32_t offset = behaviorIdIndex[index];
    DB::Store_Reader store(behaviorIdStore, behaviorIdStoreSize);
    store.skip(offset);
    Id result;
    store >> result;
    return result;
}

Id
MappedBehaviorDomain::
getBehaviorId(BH behavior) const
{
    int index = behaviorIndex.get(behavior, -1);
    if (index == -1)
        return Id();
    return getBehaviorId(BI(index));
}

BehaviorDomain::BehaviorStats
MappedBehaviorDomain::
getBehaviorStats(BI index, int fields) const
{
    auto stats = behaviorStats[index];
    
    BehaviorStats result;
    result.subjectCount = stats.subjectCount;
    result.earliest = stats.earliest;
    result.latest = stats.latest;

    if (fields & BS_ID)
        result.id = getBehaviorId(index);
    
    return result;
}

BehaviorDomain::BehaviorStats
MappedBehaviorDomain::
getBehaviorStats(BH behavior, int fields) const
{
    int index = behaviorIndex.get(behavior, -1);
    if (index == -1)
        return BehaviorStats();
    if (fields == BS_ID) {
        BehaviorStats result;
        result.id = getBehaviorId(behavior);
        return result;
    }
    return getBehaviorStats(BI(index), fields);
}

size_t
MappedBehaviorDomain::
getBehaviorSubjectCount(BI beh, SH maxSubject, Precision p) const
{
    if (maxSubject.isMax())
        return behaviorStats[beh].subjectCount;

    const uint32_t * data = behaviorToSubjects + behaviorToSubjectsIndex[beh];

    int len = behaviorStats[beh].subjectCount;
    int numSubjectBits = ML::highest_bit(md->numSubjects - 1, -1) + 1;

    typedef ML::BitArrayIterator<uint32_t> BitArrayIterator;

    BitArrayIterator
        start(data, numSubjectBits, 0),
        end(data, numSubjectBits, len),
        it = std::upper_bound(start, end, maxSubject);

    return it.index;
}

size_t
MappedBehaviorDomain::
getBehaviorSubjectCount(BH beh, SH maxSubject, Precision p) const
{
    int index = behaviorIndex.get(beh, -1);
    if (index == -1) return 0;
    return getBehaviorSubjectCount(BI(index), maxSubject, p);
}

std::vector<SH>
MappedBehaviorDomain::
getSubjectHashes(BI beh, SH maxSubject, bool sorted) const
{
    const uint32_t * data = behaviorToSubjects + behaviorToSubjectsIndex[beh];

    int len = getBehaviorSubjectCount(beh);
    int numSubjectBits = ML::highest_bit(md->numSubjects - 1, -1) + 1;

    ML::Bit_Extractor<uint32_t> extractor(data);

    vector<SH> result;

    // These are always sorted by default

    for (unsigned i = 0;  i < len;  ++i) {
        uint32_t sub;
        extractor.extract(sub, numSubjectBits);
        SH hash = getSubjectHash(SI(sub));
        if (hash > maxSubject) break;
        //cerr << "sub " << i << " = " << sub << endl;
        result.push_back(hash);
    }

    // They are already sorted

    return result;
}

std::vector<SH>
MappedBehaviorDomain::
getSubjectHashes(BH beh, SH maxSubject, bool sorted) const
{
    int index = behaviorIndex.get(beh, -1);
    if (index == -1)
        return vector<SH>();
    return getSubjectHashes(BI(index), maxSubject, sorted);
}

std::vector<std::pair<SH, Date> >
MappedBehaviorDomain::
getSubjectHashesAndTimestamps(BH beh, SH maxSubject, bool sorted) const
{
    int index = behaviorIndex.get(beh, -1);
    if (index == -1)
        return vector<pair<SH, Date> >();
    return getSubjectHashesAndTimestamps(BI(index), maxSubject, sorted);
}

std::vector<std::pair<SH, Date> >
MappedBehaviorDomain::
getSubjectHashesAndTimestamps(BI index, SH maxSubject, bool sorted) const
{
    vector<pair<SH, Date> > result;

    auto stats = getBehaviorStats(index, BS_EARLIEST | BS_SUBJECT_COUNT);

    const uint32_t * subjectData
        = behaviorToSubjects + behaviorToSubjectsIndex[index];

    bool hasTimestamps = behaviorToSubjectTimestamps != 0;

    uint32_t offset = 0;
    if (hasTimestamps)
        offset = behaviorToSubjectTimestampsIndex[index];

    const uint32_t * subjectTimestampData
        = behaviorToSubjectTimestamps + offset;

    //cerr << "index = " << index << " offset = " << offset
    //     << " subjectCount = " << stats.subjectCount << endl;
    //cerr << "*subjectTimestampData = " << *subjectTimestampData << endl;

    uint64_t timestampBase = 0;
    int numTimestampBits = 0;
    ML::Bit_Extractor<uint32_t> timeExtractor(subjectTimestampData);

    if (hasTimestamps) {
        Date earliest = stats.earliest;
        earliest.quantize(md->timeQuantum);
        timestampBase = earliest.secondsSinceEpoch() / md->timeQuantum;
        //cerr << "timestampBase = " << timestampBase << endl;
        numTimestampBits = timeExtractor.extract<uint32_t>(6);
        //cerr << "numTimestampBits = " << numTimestampBits << endl;
    }

    int numSubjectBits = ML::highest_bit(md->numSubjects - 1, -1) + 1;

    ML::Bit_Extractor<uint32_t> extractor(subjectData);

    for (unsigned i = 0;  i < stats.subjectCount;  ++i) {
        uint32_t sub;
        extractor.extract(sub, numSubjectBits);
        SH subject = getSubjectHash(SI(sub));
        if (subject > maxSubject)
            break;

        //cerr << "sub " << i << " = " << sub << endl;
        Date ts;
        if (hasTimestamps) {
            uint32_t ofs;
            timeExtractor.extract(ofs, numTimestampBits);
            ts = unQuantizeTime(ofs + timestampBase);
        }
        result.push_back(make_pair(subject, ts));
    }
    
    return result;
}

std::vector<std::pair<SH, Date> >
MappedBehaviorDomain::
getSubjectHashesAndAllTimestamps(BH beh, SH maxSubject,
                                 bool sorted) const
{
    return BehaviorDomain
        ::getSubjectHashesAndAllTimestamps(beh, maxSubject, sorted);
}

bool
MappedBehaviorDomain::
forEachBehaviorSubject(BH beh,
                        const OnBehaviorSubject & onSubject,
                        bool withTimestamps,
                        Order order,
                        SH maxSubject) const
{
    int index = behaviorIndex.get(beh, -1);
    if (index == -1)
        return true;

    auto stats = getBehaviorStats(BI(index), BS_EARLIEST | BS_SUBJECT_COUNT);

    const uint32_t * subjectData
        = behaviorToSubjects + behaviorToSubjectsIndex[index];

    bool hasTimestamps = behaviorToSubjectTimestamps != 0;
    if (withTimestamps && !hasTimestamps)
        throw MLDB::Exception("asked for timestamps with none present");

    uint32_t offset = 0;
    if (hasTimestamps)
        offset = behaviorToSubjectTimestampsIndex[index];

    const uint32_t * subjectTimestampData
        = behaviorToSubjectTimestamps + offset;

    //cerr << "index = " << index << " offset = " << offset
    //     << " subjectCount = " << stats.subjectCount << endl;
    //cerr << "*subjectTimestampData = " << *subjectTimestampData << endl;

    uint64_t timestampBase = 0;
    int numTimestampBits = 0;
    ML::Bit_Extractor<uint32_t> timeExtractor(subjectTimestampData);

    if (hasTimestamps) {
        Date earliest = stats.earliest;
        earliest.quantize(md->timeQuantum);
        timestampBase = earliest.secondsSinceEpoch() / md->timeQuantum;
        //cerr << "timestampBase = " << timestampBase << endl;
        numTimestampBits = timeExtractor.extract<uint32_t>(6);
        //cerr << "numTimestampBits = " << numTimestampBits << endl;
    }

    int numSubjectBits = ML::highest_bit(md->numSubjects - 1, -1) + 1;

    ML::Bit_Extractor<uint32_t> extractor(subjectData);

    for (unsigned i = 0;  i < stats.subjectCount;  ++i) {
        uint32_t sub;
        extractor.extract(sub, numSubjectBits);
        SH subject = getSubjectHash(SI(sub));
        if (subject > maxSubject)
            break;

        //cerr << "sub " << i << " = " << sub << endl;
        Date ts;
        if (hasTimestamps) {
            uint32_t ofs;
            timeExtractor.extract(ofs, numTimestampBits);
            ts = unQuantizeTime(ofs + timestampBase);
        }
        
        if (!onSubject(subject, ts))
            return false;
    }
    
    return true;
}

std::vector<BH>
MappedBehaviorDomain::
allBehaviorHashes(bool sorted) const
{
    std::vector<BH> result;
    result.reserve(md->numBehaviors);
    for (unsigned i = 0;  i < md->numBehaviors;  ++i)
        result.push_back(behaviorIndex[i].key);

    // already sorted

    return result;
}

std::vector<SH>
MappedBehaviorDomain::
allSubjectHashes(SH maxSubject, bool sorted) const
{
    std::vector<SH> result;
    if (maxSubject == SH::max())
        result.reserve(md->numSubjects);

    if (indexV1()) {
        // They are already sorted, so we can be more efficient about it
        for (unsigned i = 0;  i < md->numSubjects;  ++i) {
            SH sh = subjectIndex1[i].key;
            if (sh > maxSubject) break;
            result.push_back(sh);
        }
    }
    else {
        // They are already sorted, so we can be more efficient about it
        for (unsigned i = 0;  i < md->numSubjects;  ++i) {
            SH sh = subjectIndex2[i].key;
            if (sh > maxSubject) break;
            result.push_back(sh);
        }
    }
    
    /* Result is already sorted */
    
    return result;
}

struct MappedBehaviorDomainBehaviorStream : BehaviorDomain::BehaviorStream
{ 
    MappedBehaviorDomainBehaviorStream(const MappedBehaviorDomain * source,
                                         size_t start)
        : index(start), source(source)
    {
    }

    virtual Id current() const
    {
        return source->getBehaviorId(source->behaviorIndex[index].key);
    }

    virtual void advance()
    {
        ++index;
    }

    virtual void advanceBy(size_t n)
    {
        index += n;
    }

    virtual Id next()
    {
        Id result = current();
        advance();
        return result;
    }

    size_t index;
    const MappedBehaviorDomain* source;
};

std::unique_ptr<BehaviorDomain::BehaviorStream>
MappedBehaviorDomain::
getBehaviorStream(size_t start) const
{
    std::unique_ptr<BehaviorDomain::BehaviorStream> result
        (new MappedBehaviorDomainBehaviorStream(this, start));
    return std::move(result);
}

struct MappedBehaviorDomainSubjectStream : BehaviorDomain::SubjectStream
{ 
    MappedBehaviorDomainSubjectStream(const MappedBehaviorDomain* source,
                                       size_t start)
        : index(start), source(source), isV1(true)
    {
        isV1 = source->indexV1();
    }
    
    virtual Id current() const
    {
        if (isV1) {
            if (index >= source->subjectIndex1.size())
                return Id();
            return source->getSubjectId(source->subjectIndex1[index].key);
        }
        else {
            if (index >= source->subjectIndex2.size())
                return Id();
            return source->getSubjectId(source->subjectIndex2[index].key);
        }
    }

    virtual void advance()
    {
        ++index;
    }

    virtual void advanceBy(size_t n)
    {
        index += n;
    }

    virtual Id next()
    {
        Id result = current();
        advance();
        return result;
    }

    size_t index;
    const MappedBehaviorDomain* source;
    bool isV1;
};

std::unique_ptr<BehaviorDomain::SubjectStream>
MappedBehaviorDomain::
getSubjectStream(size_t start) const
{
    std::unique_ptr<BehaviorDomain::SubjectStream> result
        (new MappedBehaviorDomainSubjectStream(this, start));
    return std::move(result);
}

int
MappedBehaviorDomain::
coIterateBehaviors(BI behi1, BI behi2, 
                    SH maxSubject,
                    const OnBehaviors & onBehaviors) const
{
    if (behi1 == BI(-1) || behi2 == BI(-1))
        return 0;

    const BehaviorStatsFormat & e1 = behaviorStats[behi1.index()];
    const BehaviorStatsFormat & e2 = behaviorStats[behi2.index()];
    
    if (e1.subjectCount == 0 || e2.subjectCount == 0)
        return 0;

    bool enableLookup = true;

    if (e1.subjectCount > 10 * e2.subjectCount && enableLookup) {
        int lookupResult = coIterateBehaviorsLookup(behi1, behi2, e1, e2, maxSubject,
                                                     onBehaviors);
        return lookupResult;
        int scanResult = coIterateBehaviorsScan(behi1, behi2, e1, e2,
                                                 maxSubject, OnBehaviors());

        if (scanResult != lookupResult) {
            cerr << "bi1 = " << behi1 << endl;
            cerr << "bi2 = " << behi2 << endl;
            cerr << "maxSubject = " << maxSubject << endl;
            cerr << "e1.subjectCount = " << e1.subjectCount << endl;
            cerr << "e2.subjectCount = " << e2.subjectCount << endl;
            cerr << "scanResult = " << scanResult << endl;
            cerr << "lookupResult = " << lookupResult << endl;
        }
        ExcAssertEqual(scanResult, lookupResult);
        return lookupResult;
    }
    else if (e2.subjectCount > 10 * e1.subjectCount && enableLookup) {
        int lookupResult = coIterateBehaviorsLookup(behi1, behi2, e1, e2,
                                                     maxSubject,
                                                     onBehaviors);
        return lookupResult;
        int scanResult = coIterateBehaviorsLookup(behi2, behi1, e2, e1,
                                                   maxSubject,
                                         onBehaviors);
        if (scanResult != lookupResult) {
            cerr << "bi1 = " << behi1 << endl;
            cerr << "bi2 = " << behi2 << endl;
            cerr << "maxSubject = " << maxSubject << endl;
            cerr << "e1.subjectCount = " << e1.subjectCount << endl;
            cerr << "e2.subjectCount = " << e2.subjectCount << endl;
            cerr << "scanResult = " << scanResult << endl;
            cerr << "lookupResult = " << lookupResult << endl;
        }
        ExcAssertEqual(scanResult, lookupResult);
        return lookupResult;
    }
    else return coIterateBehaviorsScan(behi1, behi2, e1, e2, maxSubject,
                                        onBehaviors);
}

int
MappedBehaviorDomain::
coIterateBehaviorsScan(BI behi1, BI behi2, 
                        const BehaviorStatsFormat & e1,
                        const BehaviorStatsFormat & e2,
                        SH maxSubject,
                        const OnBehaviors & onBehaviors) const
{
    //cerr << "mapped coiterate scan" << endl;

    //cerr << "beh1 = " << beh1 << " beh2 = " << beh2 << endl;
    
    int numSubjectBits = ML::highest_bit(md->numSubjects - 1, -1) + 1;

    auto getExtractor = [&] (BI beh)
        {
            const uint32_t * data
            = behaviorToSubjects
            + behaviorToSubjectsIndex[beh];

            return ML::Bit_Extractor<uint32_t>(data);
        };

    ML::Bit_Extractor<uint32_t> extractor1 = getExtractor(behi1);
    ML::Bit_Extractor<uint32_t> extractor2 = getExtractor(behi2);

    int idx1 = 0, len1 = e1.subjectCount;
    uint32_t sub1, sub2;
    int idx2 = 0, len2 = e2.subjectCount;
    extractor1.extract(sub1, numSubjectBits);
    extractor2.extract(sub2, numSubjectBits);

    size_t result = 0;

    int unmatched = 0;

    //cerr << "len1 = " << len1 << " len2 = " << len2 << endl;

    while (idx1 < len1 && idx2 < len2) {
        uint32_t cur1 = sub1, cur2 = sub2;

#if 0
        cerr << "c1 = " << cur1 << " c2 = " << cur2
             << " i1 = " << idx1 << " l1 = " << len1
             << " i2 = " << idx2 << " l2 = " << len2
             << " h1 = " << getSubjectHash(SI(cur1))
             << " h2 = " << getSubjectHash(SI(cur2))
             << " max = " << maxSubject << endl;
#endif

        if (cur1 == cur2) {
            if (maxSubject.isMax() && !onBehaviors) {
                ++result;
            }
            else {
                SH sh = getSubjectHash(SI(cur1));
                if (sh > maxSubject) break;
                //cerr << "   *** got subject " << sh << endl;
                if (onBehaviors)
                    onBehaviors(sh);
                ++result;
                unmatched = 0;
            }
        }
        else {
            if (!maxSubject.isMax()) {
                if (unmatched > 10) {
                    SH sh = getSubjectHash(SI(std::min(cur1, cur2)));
                    if (sh > maxSubject) {
                        //cerr << " --> bailing for SH " << sh << " too high"
                        //     << endl;
                        break;
                    }
                    unmatched = 0;
                }
                else ++unmatched;
            }
        }
        if (cur1 <= cur2) {
            ++idx1;
            if (idx1 == len1) break;
            extractor1.extract(sub1, numSubjectBits);
        }
        if (cur2 <= cur1) {
            ++idx2;
            if (idx2 == len2) break;
            extractor2.extract(sub2, numSubjectBits);
        }
    }

    return result;
}

int
MappedBehaviorDomain::
coIterateBehaviorsLookup(BI behi1, BI behi2, 
                          const BehaviorStatsFormat & e1,
                          const BehaviorStatsFormat & e2,
                          SH maxSubject,
                          const OnBehaviors & onBehaviors) const
{
    BH beh1 = e1.hash;
    BH beh2 = e2.hash;

    int numSubjectBits = ML::highest_bit(md->numSubjects - 1, -1) + 1;

    auto getExtractor = [&] (BI beh)
        {
            const uint32_t * data
            = behaviorToSubjects
            + behaviorToSubjectsIndex[beh];

            return ML::Bit_Extractor<uint32_t>(data);
        };

    ML::Bit_Extractor<uint32_t> extractor2 = getExtractor(behi2);
    int len2 = e2.subjectCount;

    typedef ML::BitArrayIterator<uint32_t> BitArrayIterator;

    int len1 = e1.subjectCount;
    const uint32_t * data1
        = behaviorToSubjects
        + behaviorToSubjectsIndex[behi1];

    BitArrayIterator
        start(data1, numSubjectBits, 0),
        end(data1, numSubjectBits, len1);

    struct CompareSubj {
        CompareSubj(const MappedBehaviorDomain * dom)
            : dom(dom)
        {
        }

        const MappedBehaviorDomain * dom;
        bool operator () (uint32_t s1, SH s2) const
        {
            return dom->getSubjectHash(SI(s1)) < s2;
        }

        bool operator () (SH s1, uint32_t s2) const
        {
            return s1 < dom->getSubjectHash(SI(s2));
        }
    };
    
    if (!maxSubject.isMax())
        end = std::upper_bound(start, end, maxSubject, CompareSubj(this));
    
    size_t result = 0;
    
    for (unsigned i = 0;  i < len2;  ++i) {
        uint32_t sub = extractor2.extract<uint32_t>(numSubjectBits);
        SH sh = getSubjectHash(SI(sub));
        if (sh > maxSubject) break;
        
 #if 0
        cerr << "scanning " << sh << " sub " << sub << " at "
             << i << " of " << len2 << " between "
             << start.index << " (" << *start << ")  and " << end.index
             << " (" << *end << ")" << endl;
#endif

        // Perform a binary search
        BitArrayIterator it = std::lower_bound(start, end, sub);

        //cerr << "  found " << it.index << " (" << *it << ")" << endl;

        if (it != end && *it == sub) {
            if (onBehaviors)
                onBehaviors(sh);
            //cerr << "   *** got one" << endl;
            ++result;
        }

        // Don't start from any earlier than before
        start = it;
    }
    
    return result;
}

int
MappedBehaviorDomain::
coIterateBehaviors(BH beh1, BH beh2, 
                    SH maxSubject,
                    const OnBehaviors & onBehaviors) const
{
    auto getIndex = [&] (BH beh) -> int
        {
            return behaviorIndex.get(beh, -1);
        };

    int i1 = getIndex(beh1), i2 = getIndex(beh2);
    if (i1 == -1 || i2 == -1)
        return 0;

    return coIterateBehaviors(BI(i1), BI(i2), maxSubject,
                               onBehaviors);
}

bool
MappedBehaviorDomain::
fileMetadataExists(const std::string & key) const
{
    return fileMetadata_.isMember(key);
}

Json::Value
MappedBehaviorDomain::
getFileMetadata(const std::string & key) const
{
    return fileMetadata_[key];
}

Json::Value
MappedBehaviorDomain::
getAllFileMetadata() const
{
    return fileMetadata_;
}


} // namespace MLDB
