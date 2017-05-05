/* mutable_behavior_domain.cc
   Jeremy Barnes, 4 April 2012
   Copyright (c) 2012 mldb.ai inc.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include <limits>
#include "mutable_behavior_domain.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/base/parallel.h"
#include "mldb/base/thread_pool.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/value_description.h"
#include "mldb/server/parallel_merge_sort.h"
#include "mldb/utils/possibly_dynamic_buffer.h"

using namespace std;
using namespace ML;

namespace MLDB {

template<typename T>
void atomic_min(std::atomic<T> & val, T other)
{
    T current = val.load(std::memory_order_relaxed);
    for (;;) {
        if (other >= current)
            return;
        if (val.compare_exchange_weak(current, other, std::memory_order_relaxed))
            return;
    }
}

template<typename T>
void atomic_max(std::atomic<T> & val, T other)
{
    T current = val.load(std::memory_order_relaxed);
    for (;;) {
        if (other <= current)
            return;
        if (val.compare_exchange_weak(current, other, std::memory_order_relaxed))
            return;
    }
}

void atomic_min(std::atomic<double> & val, Date other)
{
    atomic_min(val, other.secondsSinceEpoch());
}

void atomic_max(std::atomic<double> & val, Date other)
{
    atomic_max(val, other.secondsSinceEpoch());
}


/*****************************************************************************/
/* MUTABLE BEHAVIOR DOMAIN                                                  */
/*****************************************************************************/

MutableBehaviorDomain::
MutableBehaviorDomain(int minSubjects)
    : BehaviorDomain(minSubjects),
      earliest_(INFINITY),
      latest_(-INFINITY),
      nominalStart_(Date::positiveInfinity()),
      nominalEnd_(Date::negativeInfinity()),
      totalEventsRecorded_(0),
      immutable_(false),
      behaviorRoot(rootLock),
      behaviorPartialCache(rootLock),
      subjectRoots(NUM_SUBJECT_ROOTS),
      updatingCache(0)
{
    for (unsigned i = 0;  i < NUM_SUBJECT_ROOTS;  ++i) {
        subjectRoots[i] = std::make_shared<SubjectRoot>(rootLock);
    }
}

MutableBehaviorDomain::
MutableBehaviorDomain(const MutableBehaviorDomain & other)
    : behaviorIndex(other.behaviorIndex),
      earliest_(other.earliest_.load()),
      latest_(other.latest_.load()),
      nominalStart_(other.nominalStart_),
      nominalEnd_(other.nominalEnd_),
      totalEventsRecorded_(other.totalEventsRecorded_.load()),
      immutable_(false),
      behaviorRoot(rootLock),
      behaviorPartialCache(rootLock),
      updatingCache(0)
{
    throw MLDB::Exception("MutableBehaviorDomain copying not done");
}

MutableBehaviorDomain::
~MutableBehaviorDomain()
{
    while (updatingCache) {
        // Do some work, so we can continue even if all other threads are
        // busy.
        ThreadPool::instance().work();
        if (updatingCache)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    auto behRoot = behaviorRoot();
    for (unsigned i = 0;  i < behRoot->size;  ++i)
        delete behRoot->entries[i].load();

    for (unsigned s = 0;  s < NUM_SUBJECT_ROOTS;  ++s) {
        auto subRoot = subjectRoots[s]->subjectEntryPtr();
        for (unsigned i = 0;  i < subRoot->size;  ++i)
            delete subRoot->entries[i].load();
    }
}

MutableBehaviorDomain *
MutableBehaviorDomain::
makeShallowCopy() const
{
    // TODO: thread safe... not easy
    return new MutableBehaviorDomain(*this);
}

MutableBehaviorDomain *
MutableBehaviorDomain::
makeDeepCopy() const
{
    // TODO: thread safe... not easy
    return new MutableBehaviorDomain(*this);
}

void
MutableBehaviorDomain::
recordImpl(const Id & subject,
           const Id & behavior,
           Date ts_,
           uint32_t count,
           const Id & verb,
           bool once)
{ 
    ExcAssert(!immutable_);

    if (count == 0)
        throw MLDB::Exception("can't record a zero count for a behavior");

    GcLock::SharedGuard guard(rootLock);

    BehaviorEntry * behaviorEntry = obtainBehaviorEntry(behavior);
    SubjectEntry * subjectEntry;
    SI2 subjectIndex;

    std::tie(subjectEntry, subjectIndex)
        = obtainSubjectEntryAndIndex(subject);
    
    BI behaviorIndex(behaviorEntry->index);

    if (!ts_.isADate()) {
        ts_ = Date();
        //throw HttpReturnException(400,
        //                          "Cannot record non-finite timestamp into behavior dataset",
        //                          "timestamp", ts_,
        //                          "subject", subject,
        //                          "behavior", behavior);
    }

    ts_.quantize(timeQuantum);

    atomic_min(earliest_, ts_);
    atomic_max(latest_, ts_);
    totalEventsRecorded_ += 1;
    
    uint64_t ts = quantizeTime(ts_);

    VI verbi(0);
    if (verb != Id())
        throw MLDB::Exception("types not supported yet");

    if (once) {
        if (!subjectEntry->recordOnce(behaviorIndex, ts, 1, verbi)) return;
    }
    else subjectEntry->record(behaviorIndex, ts, count, verbi);
    
    behaviorEntry->record(subjectIndex, ts, count, verbi, rootLock);
}

void
MutableBehaviorDomain::
finish()
{
    // TODO: thread safe... not easy

    for (unsigned s = 0;  s < NUM_SUBJECT_ROOTS;  ++s) {
        auto subs = subjectRoots[s]->subjectEntryPtr();
        for (unsigned i = 0;  i < subs->size;  ++i) {
            subs->entries[i].load()->sort(immutable_, this);
        }
    }
    
    auto behs = behaviorRoot();
    for (unsigned i = 0;  i < behs->size;  ++i) {
        behs->entries[i].load()->sort(rootLock, immutable_);
    }

    if (!nominalStart_.isADate())
        nominalStart_ = Date::fromSecondsSinceEpoch(earliest_);
    if (!nominalEnd_.isADate())
        nominalEnd_ = Date::fromSecondsSinceEpoch(latest_);
}

void
MutableBehaviorDomain::
record(const Id & subject,
       const Id & behavior,
       Date ts,
       uint32_t count,
       const Id & verb)
{ 
    recordImpl(subject, behavior, ts, count, verb,
               false /* once */);
}

void
MutableBehaviorDomain::
recordOnce(const Id & subject,
           const Id & behavior,
           Date ts,
           const Id & verb)
{ 
    recordImpl(subject, behavior, ts, 1, verb, true /* once */);
}

void
MutableBehaviorDomain::
recordMany(const Id & subject,
           const ManyEntryInt * first,
           size_t n)
{
    GcLock::SharedGuard guard(rootLock);

    ExcAssert(!immutable_);

    SubjectEntry * subjectEntry;
    SI2 subjectIndex;

    std::tie(subjectEntry, subjectIndex)
        = obtainSubjectEntryAndIndex(subject);

    if (n == 0)
        return;

    Date entryMin = Date::positiveInfinity();
    Date entryMax = Date::negativeInfinity();

    for (unsigned i = 0;  i < n;  ++i) {
        ManyEntryInt entry = first[i];

        VI verb(0);
        if (entry.verb != Id())
            throw MLDB::Exception("types not supported yet");

        BehaviorEntry * behaviorEntry
            = obtainBehaviorEntry(Id(entry.behavior));
        BI behaviorIndex = behaviorEntry->index;

        if (!entry.timestamp.isADate()) {
            entry.timestamp = Date();
            //throw HttpReturnException(400,
            //                          "Cannot record non-finite timestamp into behavior dataset",
            //                          "timestamp", entry.timestamp,
            //                          "subject", subject,
            //                          "behavior", entry.behavior);
        }


        entry.timestamp.quantize(timeQuantum);
        uint64_t ts = quantizeTime(entry.timestamp);

        entryMin.setMin(entry.timestamp);
        entryMax.setMax(entry.timestamp);

        subjectEntry->record(behaviorIndex, ts, entry.count, verb);
        behaviorEntry->record(subjectIndex, ts, entry.count,
                               verb, rootLock);
    }

    atomic_min(earliest_, entryMin);
    atomic_max(latest_, entryMax);

    totalEventsRecorded_ += n;
}

void
MutableBehaviorDomain::
recordMany(const Id & subject,
           const ManyEntryId * first,
           size_t n)
{
    GcLock::SharedGuard guard(rootLock);

    ExcAssert(!immutable_);
    SubjectEntry * subjectEntry;
    SI2 subjectIndex;

    std::tie(subjectEntry, subjectIndex)
        = obtainSubjectEntryAndIndex(subject);

    if (n == 0)
        return;

    Date entryMin = Date::positiveInfinity();
    Date entryMax = Date::negativeInfinity();

    std::unordered_map<Id, BehaviorEntry *> behEntries;
    
    for (unsigned i = 0;  i < n;  ++i) {
        ManyEntryId entry = first[i];

        VI verb(0);
        if (entry.verb != Id())
            throw MLDB::Exception("types not supported yet");

#if 0
        BehaviorEntry * behaviorEntry;
        auto it = behEntries.find(entry.behavior);
        if (it == behEntries.end()) {
            behEntries[entry.behavior]
                = behaviorEntry
                = obtainBehaviorEntry(entry.behavior);
        } else behaviorEntry = it->second;
#else
        BehaviorEntry * behaviorEntry
            = obtainBehaviorEntry(entry.behavior);
#endif
        
        BI behaviorIndex = behaviorEntry->index;

        if (!entry.timestamp.isADate()) {
            entry.timestamp = Date();
            //throw HttpReturnException(400,
            //                          "Cannot record non-finite timestamp into behavior dataset",
            //                          "timestamp", entry.timestamp,
            //                          "subject", subject,
            //                          "behavior", entry.behavior);
        }

        entry.timestamp.quantize(timeQuantum);
        uint64_t ts = (entry.timestamp.secondsSinceEpoch() / timeQuantum);

        entryMin.setMin(entry.timestamp);
        entryMax.setMax(entry.timestamp);
        
        subjectEntry->record(behaviorIndex, ts, entry.count, verb);
        behaviorEntry->record(subjectIndex, ts, entry.count,
                               verb, rootLock);
    }

    atomic_min(earliest_, entryMin);
    atomic_max(latest_, entryMax);

    totalEventsRecorded_ += n;
}

void
MutableBehaviorDomain::
recordManySubjects(const Id & beh,
                   const ManySubjectId * first,
                   size_t n)
{
    GcLock::SharedGuard guard(rootLock);

    ExcAssert(!immutable_);
    BehaviorEntry * behaviorEntry = obtainBehaviorEntry(beh);

    if (n == 0)
        return;

    Date entryMin = Date::positiveInfinity();
    Date entryMax = Date::negativeInfinity();

    for (unsigned i = 0;  i < n;  ++i) {
        ManySubjectId entry = first[i];

        VI verb(0);
        if (entry.verb != Id())
            throw MLDB::Exception("types not supported yet");

        SubjectEntry * subjectEntry;
        SI2 subjectIndex;
        std::tie(subjectEntry, subjectIndex)
            = obtainSubjectEntryAndIndex(entry.subject);
        
        BI behaviorIndex = behaviorEntry->index;

        if (!entry.timestamp.isADate()) {
            entry.timestamp = Date();
            //throw HttpReturnException(400,
            //                          "Cannot record non-finite timestamp into behavior dataset",
            //                          "timestamp", entry.timestamp,
            //                          "subject", entry.subject,
            //                          "behavior", beh);
        }

        entry.timestamp.quantize(timeQuantum);
        uint64_t ts = quantizeTime(entry.timestamp);

        entryMin.setMin(entry.timestamp);
        entryMax.setMax(entry.timestamp);
        
        subjectEntry->record(behaviorIndex, ts, entry.count, verb);
        behaviorEntry->record(subjectIndex, ts, entry.count, verb, rootLock);
    }

    atomic_min(earliest_, entryMin);
    atomic_max(latest_, entryMax);

    totalEventsRecorded_ += n;
}

void
MutableBehaviorDomain::
recordMany(const Id * behIds,
           size_t numBehIds,
           const Id * subjectIds,
           size_t numSubjectIds,
           const ManyEntryIndex * first,
           size_t n)
{
    //cerr << "recording " << n << " events with " << numBehIds << " behs and "
    //     << numSubjectIds << " subjects" << endl;

    GcLock::SharedGuard guard(rootLock);

    ExcAssert(!immutable_);

    std::vector<std::pair<SubjectEntry *, SI2> > subjectEntries(numSubjectIds);
    for (unsigned i = 0;  i < numSubjectIds;  ++i) {
        subjectEntries[i] = obtainSubjectEntryAndIndex(subjectIds[i]);
    }
    
    std::vector<BehaviorEntry *> behaviorEntries(numBehIds);
    for (unsigned i = 0;  i < numBehIds;  ++i) {
        behaviorEntries[i] = obtainBehaviorEntry(behIds[i]);
    }
    
    if (n == 0)
        return;

    Date entryMin = Date::positiveInfinity();
    Date entryMax = Date::negativeInfinity();

    for (unsigned i = 0;  i < n;  ++i) {
        ManyEntryIndex entry = first[i];

        BehaviorEntry * behaviorEntry = behaviorEntries.at(entry.behIndex);
        BI behaviorIndex = behaviorEntry->index;
        SubjectEntry * subjectEntry = subjectEntries.at(entry.subjIndex).first;
        SI2 subjectIndex = subjectEntries.at(entry.subjIndex).second;

        if (!entry.timestamp.isADate()) {
            entry.timestamp = Date();
            //throw HttpReturnException(400,
            //                          "Cannot record non-finite timestamp into behavior dataset",
            //                          "timestamp", entry.timestamp,
            //                          "subject", subjectIds[entry.subjIndex],
            //                          "behavior", behIds[entry.behIndex]);
        }

        entry.timestamp.quantize(timeQuantum);
        uint64_t ts = quantizeTime(entry.timestamp);

        entryMin.setMin(entry.timestamp);
        entryMax.setMax(entry.timestamp);

        subjectEntry->record(behaviorIndex, ts, entry.count, VI());
        behaviorEntry->record(subjectIndex, ts, entry.count,
                               VI(), rootLock);
    }

    atomic_min(earliest_, entryMin);
    atomic_max(latest_, entryMax);

    totalEventsRecorded_ += n;
}

size_t
MutableBehaviorDomain::
subjectCount() const
{
    size_t result = 0;
    for (unsigned s = 0;  s < NUM_SUBJECT_ROOTS;  ++s) {
        auto subRoot = subjectRoots[s]->subjectEntryPtr();
        result += subRoot->size;
    }
    return result;
}

size_t
MutableBehaviorDomain::
behaviorCount() const
{
    return behaviorRoot()->size;
}

std::vector<BH>
MutableBehaviorDomain::
allBehaviorHashes(bool sorted) const
{
    vector<BH> result;
    {
        auto behRoot = behaviorRoot();
        
        for (unsigned i = 0;  i < behRoot->size;  ++i)
            result.push_back(behRoot->entries[i].load()->hash);
    }
    if (sorted)
        std::sort(result.begin(), result.end());

    return result;
}

struct MutableBehaviorDomainBehaviorStream
    : public BehaviorDomain::BehaviorStream
{
    MutableBehaviorDomainBehaviorStream(const MutableBehaviorDomain* source, size_t start)
        : index(start), source(source)
    {
    }  

    virtual Id next()
    { 
        Id result = current();
        advance();
        return result;
    }

    virtual Id current() const
    {
        auto behRoot = source->behaviorRoot();
        if (index >= behRoot->size)
            return Id();
        return behRoot->entries[index].load()->id;
    }
   
    virtual void advance()
    {
        ++index;
    }

    virtual void advanceBy(size_t n)
    {
        index += n;
    } 

    size_t index;
    const MutableBehaviorDomain* source;
};

std::unique_ptr<BehaviorDomain::BehaviorStream>
MutableBehaviorDomain::
getBehaviorStream(size_t start) const
{
    std::unique_ptr<BehaviorDomain::BehaviorStream> result
        (new MutableBehaviorDomainBehaviorStream(this, start));
    return result;
}

struct MutableBehaviorDomainSubjectStream
    : public BehaviorDomain::SubjectStream
{
    MutableBehaviorDomainSubjectStream(const MutableBehaviorDomain* source,
                                        size_t start)
        : s(0), i(0), source(source)
    {
        root = source->subjectRoots[0]->subjectEntryPtr();

        // Skip over any roots with no subjects
        while (s < MutableBehaviorDomain::NUM_SUBJECT_ROOTS
               && root->size == 0) {
            ++s;
            root.unlock();
            if (s < MutableBehaviorDomain::NUM_SUBJECT_ROOTS)
                root = source->subjectRoots[s]->subjectEntryPtr();
        }

        advanceBy(start);
    }

    virtual Id next()
    { 
        Id result = current();
        advance();
        return result;
    }

    virtual Id current() const
    {
        ExcAssert(root);
        ExcAssertLess(i, root->size);
        return root->entries[i].load()->id;
    }
   
    virtual void advance()
    {
        ExcAssert(root);

        for (;  s < MutableBehaviorDomain::NUM_SUBJECT_ROOTS;) {
           
            bool found = false;
            if (i < root->size) {
                found = true;
                ++i;
            }
            if (i >= root->size) {
                i = 0;
                root.unlock();
                ++s;
                if (s < MutableBehaviorDomain::NUM_SUBJECT_ROOTS) {
                    root = source->subjectRoots[s]->subjectEntryPtr();
                }
            }
            if (found)
                return;
        }

        throw MLDB::Exception("off the end access to mutable beh subject stream");
    }

    virtual void advanceBy(size_t n)
    {
        // TODO (no issue number because minor): advance by blocks of
        // subjects instead of one at a time
        while (n--)
            advance();
    } 

    size_t s;  ///< Root number
    size_t i;  ///< Index within root
    const MutableBehaviorDomain* source;
    RcuLocked<MutableBehaviorDomain::Root<MLDB::MutableBehaviorDomain::SubjectEntry> > root;  ///< RCU reference for the values we're iterating through
};

std::unique_ptr<BehaviorDomain::SubjectStream>
MutableBehaviorDomain::
getSubjectStream(size_t start) const
{
    std::unique_ptr<BehaviorDomain::SubjectStream> result
        (new MutableBehaviorDomainSubjectStream(this, start));
    return result;
}

std::vector<SH>
MutableBehaviorDomain::
allSubjectHashes(SH maxSubject, bool sorted) const
{
    vector<SH> result;

    for (unsigned s = 0;  s < NUM_SUBJECT_ROOTS;  ++s) {
        auto root = subjectRoots[s]->subjectEntryPtr();
        
        for (unsigned i = 0;  i < root->size;  ++i) {
            SH sh = root->entries[i].load()->hash;
            if (sh <= maxSubject)
                result.push_back(sh);
        }
    }

    if (sorted)
        MLDB::parallelQuickSortRecursive<SH>(result);

    return result;
}

bool
MutableBehaviorDomain::
forEachSubject(const OnSubject & onSubject,
               const SubjectFilter & filter) const
{

    for (unsigned s = 0;  s < NUM_SUBJECT_ROOTS;  ++s) {
        auto root = subjectRoots[s]->subjectEntryPtr();
        
        for (unsigned i = 0;  i < root->size;  ++i) {
            SH sh = root->entries[i].load()->hash;

            SubjectIterInfo info;
            if (!onSubject(sh, info))
                return false;
        }
    }

    return true;
}

bool
MutableBehaviorDomain::
knownSubject(SH subjectHash) const
{
    return getSubjectEntry(subjectHash);
}

bool
MutableBehaviorDomain::
knownBehavior(BH behHash) const
{
    return getBehaviorEntry(behHash);
}

int
MutableBehaviorDomain::
numDistinctTimestamps(SH subjectHash, uint32_t maxValue) const
{
    auto si = getSubjectEntry(subjectHash);
    if (!si)
        return 0;

    std::unique_lock<Spinlock> guard(si->lock);

    // Assume that it's sorted until we prove otherwise

    int result = 0;
    bool isSorted = true;
    uint64_t prev = numeric_limits<uint64_t>::max();

    auto onBeh = [&] (BI beh, uint64_t ts, int cnt)
        {
            if (prev != numeric_limits<uint64_t>::max()
                && ts < prev) {
                isSorted = false;
                return false;
            }
            result += (ts != prev);
            prev = ts;

            return (result < maxValue);
        };

    si->forEachBehavior(onBeh, immutable_);
    if (isSorted)
        return result;

    // Not sorted; we ened to collect them in a set
    std::set<uint64_t> dates;

    auto onBeh2 = [&] (BI beh, uint64_t ts, int cnt)
        {
            dates.insert(ts);
            return dates.size() < maxValue;
        };

    si->forEachBehavior(onBeh2, immutable_);

    return dates.size();
}

BehaviorDomain::SubjectStats
MutableBehaviorDomain::
getSubjectStats(SH subjectHash,
                bool needDistinctBehaviors,
                bool needDistinctTimestamps) const
{
    auto si = getSubjectEntry(subjectHash);
    if (!si) return SubjectStats();
    return si->getSubjectStats(needDistinctBehaviors,
                               needDistinctTimestamps,
                               this);
}

std::pair<Date, Date>
MutableBehaviorDomain::
getSubjectTimestampRange(SH subjectHash) const
{
    auto si = getSubjectEntry(subjectHash);
    if (!si)
        return make_pair(Date::positiveInfinity(),
                         Date::negativeInfinity());
    return make_pair(unQuantizeTime(si->firstSeen),
                     unQuantizeTime(si->lastSeen));
}

std::vector<SH>
MutableBehaviorDomain::
getSubjectHashes(BH beh, SH maxSubject, bool sorted) const
{
    std::vector<SH> result;
    auto bi = getBehaviorEntry(beh);
    if (!bi) return result;

    auto onSubject = [&] (SI2 si, uint64_t ts)
        {
            SH subject = getSubjectHash(si);
            
            if (subject > maxSubject) return false;
            result.push_back(subject);
            return true;
        };

    bi->forEachSubject(onSubject, immutable_, rootLock);

    if (sorted)
        std::sort(result.begin(), result.end());

    return result;
}

std::vector<SH>
MutableBehaviorDomain::
getSubjectHashes(BI beh, SH maxSubject, bool sorted) const
{
    std::vector<SH> result;
    auto bi = getBehaviorEntry(beh);
    if (!bi) return result;

    auto onSubject = [&] (SI2 si, uint64_t ts)
        {
            SH subject = getSubjectHash(si);
            if (subject > maxSubject) return true;
            result.push_back(subject);
            return true;
        };

    bi->forEachSubject(onSubject, immutable_, rootLock);

    if (sorted)
        std::sort(result.begin(), result.end());

    return result;
}

std::vector<std::pair<SH, Date> >
MutableBehaviorDomain::
getSubjectHashesAndTimestamps(BH beh, SH maxSubject, bool sorted) const
{
    GcLock::SharedGuard pin(rootLock, GcLock::RD_YES,
                            immutable_ ? GcLock::DONT_LOCK : GcLock::DO_LOCK);

    std::vector<pair<SH, Date> > result;
    auto bi = getBehaviorEntry(beh);
    if (!bi) return result;

    auto onSubject = [&] (SI2 si, uint64_t ts)
        {
            SH subject = getSubjectHash(si);
            if (subject > maxSubject) return true;
            result.push_back(make_pair(subject, unQuantizeTime(ts)));
            return true;
        };

    bi->forEachSubject(onSubject, immutable_, rootLock);

    if (sorted)
        std::sort(result.begin(), result.end());

    return result;
}

std::vector<std::pair<SH, Date> >
MutableBehaviorDomain::
getSubjectHashesAndTimestamps(BI beh, SH maxSubject, bool sorted) const
{
    GcLock::SharedGuard pin(rootLock, GcLock::RD_YES,
                            immutable_ ? GcLock::DONT_LOCK : GcLock::DO_LOCK);

    std::vector<pair<SH, Date> > result;
    auto bi = getBehaviorEntry(beh);
    if (!bi) return result;

    auto onSubject = [&] (SI2 si, uint64_t ts)
        {
            SH subject = getSubjectHash(si);
            if (subject > maxSubject) return true;
            result.push_back(make_pair(subject, unQuantizeTime(ts)));
            return true;
        };

    bi->forEachSubject(onSubject, immutable_, rootLock);

    if (sorted)
        std::sort(result.begin(), result.end());

    return result;
}

std::vector<std::pair<SH, Date> >
MutableBehaviorDomain::
getSubjectHashesAndAllTimestamps(BI beh, SH maxSubject,
                                 bool sorted) const
{
    auto bi = getBehaviorEntry(beh);
    if (!bi) return {};

    return BehaviorDomain
        ::getSubjectHashesAndAllTimestamps(bi->hash, maxSubject, sorted);
}
    
std::vector<std::pair<SH, Date> >
MutableBehaviorDomain::
getSubjectHashesAndAllTimestamps(BH beh, SH maxSubject,
                                 bool sorted) const
{
    return BehaviorDomain
        ::getSubjectHashesAndAllTimestamps(beh, maxSubject, sorted);
}

bool
MutableBehaviorDomain::
forEachBehaviorSubject(BH beh,
                        const OnBehaviorSubject & onSubject,
                        bool withTimestamps,
                        Order order,
                        SH maxSubject) const
{
    GcLock::SharedGuard pin(rootLock, GcLock::RD_YES,
                            immutable_ ? GcLock::DONT_LOCK : GcLock::DO_LOCK);

    if (order == INORDER) {
        for (auto & s_ts: getSubjectHashesAndTimestamps(beh, maxSubject, true))
            if (!onSubject(s_ts.first, s_ts.second))
                return false;
        return true;
    }
    else {
        auto bi = getBehaviorEntry(beh);
        if (!bi) return true;

        auto onSubject2 = [&] (SI2 si, uint64_t ts)
            {
                SH subject = getSubjectHash(si);
                if (subject > maxSubject) return false;
                return onSubject(subject, unQuantizeTime(ts));
            };

        return bi->forEachSubject(onSubject2, immutable_, rootLock);
    }
}

Id
MutableBehaviorDomain::
getSubjectId(SH subjectHash) const
{
    auto si = getSubjectEntry(subjectHash);
    if (!si) {
        throw MLDB::Exception("%s unknown subject hash: %s", __FUNCTION__,
                            to_string(subjectHash).c_str());
    }
    return si->id;
}

Id
MutableBehaviorDomain::
getBehaviorId(BI beh) const
{
    auto bi = getBehaviorEntry(beh);
    if (!bi) {
        throw MLDB::Exception("%s unknown behavior index: %s", __FUNCTION__,
                            to_string(beh).c_str());
    }
    return bi->id;
}

Id 
MutableBehaviorDomain::
getBehaviorId(BH beh) const
{
    auto bi = getBehaviorEntry(beh);
    if (!bi) {
        throw MLDB::Exception("%s unknown behavior hash: %s", __FUNCTION__,
                            to_string(beh).c_str());
    }
    return bi->id;
}

BH
MutableBehaviorDomain::
getBehaviorHash(BI beh) const
{
    auto bi = getBehaviorEntry(beh);
    if (!bi) {
        throw MLDB::Exception("%s unknown behavior index: %s", __FUNCTION__,
                            to_string(beh).c_str());
    }
    return bi->hash;
}

BI
MutableBehaviorDomain::
getBehaviorIndex(BH beh) const
{
    auto bi = getBehaviorEntry(beh);
    if (!bi) {
        throw MLDB::Exception("%s unknown behavior hash: %s", __FUNCTION__,
                            to_string(beh).c_str());
    }
    return bi->index;
}

BehaviorDomain::BehaviorStats
MutableBehaviorDomain::
getBehaviorStats(BI beh, int fields) const
{
    BehaviorStats stats;
    auto bi = getBehaviorEntry(beh);
    if (!bi) return stats;

    BehaviorEntry::EntryGuard guard(bi->lock, std::defer_lock);
    if (!immutable_)
        guard.lock();
    
    bi->sortUnlocked(rootLock, immutable_);

    stats.subjectCount = bi->sorted ? bi->sorted->size() : 0;
    stats.earliest = unQuantizeTime(bi->earliest);
    stats.latest = unQuantizeTime(bi->latest);
    stats.id = bi->id;

    return stats;
}

BehaviorDomain::BehaviorStats
MutableBehaviorDomain::
getBehaviorStats(BH beh, int fields) const
{
    BehaviorStats stats;

    auto bi = getBehaviorEntry(beh);
    if (!bi) return stats;

    BehaviorEntry::EntryGuard guard(bi->lock, std::defer_lock);
    if (!immutable_)
        guard.lock();

    bi->sortUnlocked(rootLock, immutable_);

    stats.subjectCount = bi->sorted ? bi->sorted->size() : 0;

    stats.earliest = unQuantizeTime(bi->earliest);
    stats.latest = unQuantizeTime(bi->latest);
    stats.id = bi->id;

    return stats;
}

std::pair<Date, Date>
MutableBehaviorDomain::
getBehaviorTimeRange(BH beh) const
{
    auto bi = getBehaviorEntry(beh);
    if (!bi)
        return std::make_pair(Date::positiveInfinity(),
                              Date::negativeInfinity());
    return make_pair(unQuantizeTime(bi->earliest),
                     unQuantizeTime(bi->latest));
}

std::pair<Date, Date>
MutableBehaviorDomain::
getBehaviorTimeRange(BI beh) const
{
    auto bi = getBehaviorEntry(beh);
    if (!bi)
        return std::make_pair(Date::positiveInfinity(),
                              Date::negativeInfinity());
    return make_pair(unQuantizeTime(bi->earliest),
                     unQuantizeTime(bi->latest));
}

std::vector<std::pair<BH, uint32_t> >
MutableBehaviorDomain::
getSubjectBehaviorCounts(SH subject, Order order) const
{
    vector<pair<BH, uint32_t> > result;

    auto si = getSubjectEntry(subject);
    if (!si) return result;

    std::unique_lock<Spinlock> guard(si->lock);

    std::map<uint32_t, uint32_t> counts;
    
    auto onBeh = [&] (int beh, uint64_t ts, int cnt)
        {
            counts[beh] += cnt;
            return true;
        };

    si->forEachBehavior(onBeh, immutable_);

    for (auto it = counts.begin(), end = counts.end();
         it != end;  ++it) {
        result.push_back(make_pair(getBehaviorHash(BI(it->first)),
                                   it->second));
    }

    if (order == INORDER)
        std::sort(result.begin(), result.end());

    return result;
}

bool
MutableBehaviorDomain::
forEachSubjectBehaviorHash(SH subject,
                            const OnSubjectBehaviorHash & onBeh,
                            SubjectBehaviorFilter filter,
                            Order inOrder) const
{
    // Pin the behaviors so we don't lock and unlock on each one
    GcLock::SharedGuard pin(rootLock, GcLock::RD_YES, immutable_ ? GcLock::DONT_LOCK : GcLock::DO_LOCK);

    auto si = getSubjectEntry(subject);

    if (!si) {
        //cerr << "subject not found" << endl;
        return true;
    }

    std::unique_lock<Spinlock> guard(si->lock, std::defer_lock);
    if (!immutable_)
        guard.lock();

    if (inOrder == INORDER) {
        // Call SubjectInfo version which doesn't take the lock
        const_cast<SubjectEntry *>(si)->sortUnlocked(immutable_, this);
    }

    // We need to reorder those with the same timestamp into behavior
    // order since they are sorted in behavior index order.
    Date currentTs = Date::negativeInfinity();
    std::vector<std::pair<BH, uint32_t> > current;

    auto drainCurrent = [&] ()
        {
            std::sort(current.begin(), current.end());
            for (auto it = current.begin(), end = current.end();
                 it != end;  ++it) {
                if (!onBeh(it->first, currentTs, it->second))
                    return false;
            }
            current.resize(0);
            return true;
        };

    auto doBeh = [&] (BI behi, uint64_t tsofs, uint32_t count)
        {
            BH beh = getBehaviorHash(behi);
            Date ts = unQuantizeTime(tsofs + si->firstSeen);

            if (!filter.pass(beh, ts, count)) {
                return true;
            }

            if (inOrder == ANYORDER) {
                return onBeh(beh, ts, count);
            }

            if (ts != currentTs) {
                if (!drainCurrent())
                    return false;
            }
            current.push_back(make_pair(beh, count));
            currentTs = ts;
            return true;
        };

    si->forEachBehavior(doBeh, immutable_);

    return drainCurrent();
}

Date
MutableBehaviorDomain::
earliestTime() const
{
    return Date::fromSecondsSinceEpoch(earliest_);
}

Date
MutableBehaviorDomain::
latestTime() const
{
    return Date::fromSecondsSinceEpoch(latest_);
}

bool
MutableBehaviorDomain::
fileMetadataExists(const std::string & key) const
{
    std::unique_lock<std::mutex> guard(fileMetadataLock);
    return fileMetadata_.isMember(key);
}

Json::Value
MutableBehaviorDomain::
getFileMetadata(const std::string & key) const
{
    std::unique_lock<std::mutex> guard(fileMetadataLock);
    return fileMetadata_[key];
}

void
MutableBehaviorDomain::
setFileMetadata(const std::string & key,
                Json::Value && value)
{
    std::unique_lock<std::mutex> guard(fileMetadataLock);
    fileMetadata_[key] = std::move(value);
}

Json::Value
MutableBehaviorDomain::
getAllFileMetadata() const
{
    std::unique_lock<std::mutex> guard(fileMetadataLock);
    return fileMetadata_;
}

void
MutableBehaviorDomain::
signalPartialCacheNeedsUpdate()
{
    if (updatingCache.fetch_add(1) != 0) {
        --updatingCache;
        return;
    }

    auto updateCacheJob = [=] ()
        {
            try {
                this->updatePartialCache();
            } catch (...) {
                --updatingCache;
                throw;
            }
            --updatingCache;
        };

    try {
        ThreadPool::instance().add(updateCacheJob);
    } catch (...) {
        --updatingCache;
        throw;
    }
}

void
MutableBehaviorDomain::
updatePartialCache()
{
    auto root = behaviorRoot();

    std::unique_ptr<Lightweight_Hash<BH, BehaviorEntry *> >
        newCache(new Lightweight_Hash<BH, BehaviorEntry *>
                 (*behaviorPartialCache()));
            
    for (unsigned i = newCache->size();  i < root->size;  ++i)
        (*newCache)[root->at(i)->hash] = root->at(i);
            
    behaviorPartialCache.replace(newCache.release());
}

MutableBehaviorDomain::BehaviorEntry *
MutableBehaviorDomain::
obtainBehaviorEntry(const Id & behavior)
{
    // Fast path: we already know the behavior

    MutableBehaviorDomain::BehaviorEntry * result = nullptr;

    BH behHash(behavior);

    // Look for locally cached version
    ThreadInfo * thrInfo = threadInfo.get();
    if (thrInfo) {
        auto it = thrInfo->behaviorCache.find(behHash);
        if (it != thrInfo->behaviorCache.end())
            return it->second;
    }

    size_t cacheSize = 0;

    // Look in the shared partial cache
    {
        auto c = behaviorPartialCache();
        cacheSize = c->size();
        auto it = c->find(behHash);
        if (it != c->end()) {
            result = it->second;
        }
    }

    if (!result) {
        int idx = -1;

        std::unique_lock<IndexLock> guard(behaviorIndexLock);

        auto it = behaviorIndex.find(behHash);
        if (it == behaviorIndex.end()) {
            // Slow path: we need to insert it.  We keep the lock which is
            // unfortunate but can be dealt with later
            auto root = behaviorRoot();
            
            // need to expand root
            if (root->size == root->capacity) {
                auto expanded = root->expand();
                if (!behaviorRoot.cmp_xchg(root, expanded))
                    throw MLDB::Exception("cmp_xchg should not fail");

                // TODO: should be done by cmp_xchg
                root.ptr = behaviorRoot.val;
            }

            BehaviorEntry * result = 0;
            
            for (;;) {
                unique_ptr<BehaviorEntry> newEntry(new BehaviorEntry());
                newEntry->id = behavior;
                newEntry->hash = behHash;
                idx = newEntry->index = BI(root->size);

                if ((result = root->add(newEntry, idx)))
                    break;
                cerr << "shouldn't happen: race adding beh result" << endl;
                // if we got here, something had changed under us and we
                // need to retry it
            }

            if (root->size >= cacheSize * 3 / 2 || root->size % 10000 == 0) {
                signalPartialCacheNeedsUpdate();
            }
            
            behaviorIndex[behHash] = idx;
            return result;
        }
        else {
            idx = it->second;
        }

        result = behaviorRoot()->at(idx);
    }

    if (thrInfo) {
        if (thrInfo->behaviorCache.size() >= 10000)
            thrInfo->behaviorCache.clear();
        thrInfo->behaviorCache[behHash] = result;
    }

    return result;
}

MutableBehaviorDomain::SubjectEntry *
MutableBehaviorDomain::
obtainSubjectEntry(const Id & subject)
{
    return obtainSubjectEntryAndIndex(subject).first;
}

std::pair<MutableBehaviorDomain::SubjectEntry *,
          MutableBehaviorDomain::SI2>
MutableBehaviorDomain::
obtainSubjectEntryAndIndex(const Id & subject)
{
    SH subjectHash(subject);
    int rootNumber = (subjectHash >> 59);
    ExcAssertLess(rootNumber, NUM_SUBJECT_ROOTS);

    // Fast path: we already know the subject

    auto & subjectRoot = *subjectRoots[rootNumber];

    int idx = -1;

    // TODO: why not move this lock down...
    std::unique_lock<IndexLock> guard(subjectRoot.subjectIndexWriteLock);

    auto subjectIndex = subjectRoot.subjectIndexPtr();

    auto it = subjectIndex->find(subjectHash);
    if (it != subjectIndex->end())
        return make_pair(subjectRoot.subjectEntryPtr()->at(it->second), SI2(rootNumber, it->second));
    
    // ... to here?
    // Answer... it leads to race conditions I haven't fully worked out
    // yet

    {
        
        auto it = subjectIndex->find(subjectHash);
        if (it == subjectIndex->end()) {
            // Slow path: we need to insert it.  We keep the lock which is
            // unfortunate but can be dealt with later
            auto root = subjectRoot.subjectEntryPtr();
            
            // need to expand root
            if (root->size == root->capacity) {
                auto expanded = root->expand();
                if (!subjectRoot.subjectEntryPtr.cmp_xchg(root, expanded))
                    throw MLDB::Exception("cmp_xchg should not fail");

                // TODO: should be done by cmp_xchg
                root.ptr = subjectRoot.subjectEntryPtr.val;
            }

            SubjectEntry * result = 0;
            
            for (;;) {
                unique_ptr<SubjectEntry> newEntry(new SubjectEntry());
                newEntry->id = subject;
                newEntry->hash = subjectHash;
                idx = root->size;

                if ((result = root->add(newEntry, idx)))
                    break;
                cerr << "shouldn't happen: race adding sub result" << endl;

                // if we got here, something had changed under us and we
                // need to retry it
            }
            
            if (subjectIndex->needs_expansion()) {
                //cerr << "expanding from " << subjectIndex->size()
                //     << " with ptr "
                //     << &(*subjectIndex) << endl;

                auto oldPtr = &*subjectIndex;
                if (subjectRoot.subjectIndexPtr.val != oldPtr) {
                    //cerr << "oldPtr = " << oldPtr << " newPtr = "
                    //     << subjectIndexPtr.val << endl;
                    throw MLDB::Exception("pointer changed under us");
                }

                // Expand it
                unique_ptr<Lightweight_Hash<uint64_t, uint32_t> >
                    newEntries(new Lightweight_Hash<uint64_t, uint32_t>(*subjectIndex));
                newEntries->insert(make_pair(subjectHash, idx));
                
                if (!subjectRoot.subjectIndexPtr.cmp_xchg(subjectIndex, newEntries))
                    throw MLDB::Exception("cmp_xchg didn't work: old size %zd new size %zd", subjectIndex->size(), newEntries->size());
                //cerr << "expansion succeeded: from " << oldPtr << " to "
                //     << subjectIndexPtr.val
                //     << endl;
            }
            else {
                // Simply insert it.  The way this works it won't affect
                // readers.
                (*subjectIndex)[subjectHash] = idx;
            }
            return make_pair(result, SI2(rootNumber, idx));
        }
        else {
            idx = it->second;
        }
    }

    return make_pair(subjectRoot.subjectEntryPtr()->at(idx), SI2(rootNumber, idx));
}

const MutableBehaviorDomain::BehaviorEntry *
MutableBehaviorDomain::
getBehaviorEntry(BH behavior) const
{
    if (immutable_) {
        auto it = behaviorIndex.find(behavior);
        if (it == behaviorIndex.end())
            return 0;
        return behaviorRoot.val.load()->entries[it->second];
    }

    // Look for locally cached version
    ThreadInfo * thrInfo = threadInfo.get();
    if (thrInfo) {
        auto it = thrInfo->behaviorCache.find(behavior);
        if (it != thrInfo->behaviorCache.end())
            return it->second;
    }

    int idx = -1;

    {
        std::unique_lock<IndexLock> guard(behaviorIndexLock);
        auto it = behaviorIndex.find(behavior);
        if (it == behaviorIndex.end())
            return 0;
        idx = it->second;
    }

    auto root = behaviorRoot();
    ExcAssertLess(idx, root->size);

    if (thrInfo) {
        if (thrInfo->behaviorCache.size() >= 10000)
            thrInfo->behaviorCache.clear();
        thrInfo->behaviorCache[behavior] = root->entries[idx];
    }

    return root->entries[idx];
}

const MutableBehaviorDomain::BehaviorEntry *
MutableBehaviorDomain::
getBehaviorEntry(BI behavior) const
{
    if (immutable_) {
        auto root = behaviorRoot.unsafePtr();
    
        if (behavior >= root->size)
            throw MLDB::Exception("getting unknown behavior index");
        return root->entries[behavior];
    }
    else {
        auto root = behaviorRoot();
    
        if (behavior >= root->size)
            throw MLDB::Exception("getting unknown behavior index");
        return root->entries[behavior];
    }
}

const MutableBehaviorDomain::SubjectEntry *
MutableBehaviorDomain::
getSubjectEntry(SH subject) const
{
    int rootNumber = (subject >> 59);
    ExcAssertLess(rootNumber, NUM_SUBJECT_ROOTS);

    auto & subjectRoot = *subjectRoots[rootNumber];

    int idx = -1;

    if (immutable_) {
        {
            auto subjectIndex = subjectRoot.subjectIndexPtr.unsafePtr();

            auto it = subjectIndex->find(subject);
            if (it == subjectIndex->end())
                return nullptr;
            idx = it->second;
        }

        auto root = subjectRoot.subjectEntryPtr.unsafePtr();
        ExcAssertLess(idx, root->size);
        return root->entries[idx];
    }
    else {
        {
            auto subjectIndex = subjectRoot.subjectIndexPtr();

            auto it = subjectIndex->find(subject);
            if (it == subjectIndex->end())
                return nullptr;
            idx = it->second;
        }

        auto root = subjectRoot.subjectEntryPtr();
        ExcAssertLess(idx, root->size);
        return root->entries[idx];
    }
}


/*****************************************************************************/
/* MUTABLE BEHAVIOR DOMAIN SUBJECT ENTRY                                    */
/*****************************************************************************/

BehaviorDomain::SubjectStats
MutableBehaviorDomain::SubjectEntry::
getSubjectStats(bool needDistinctBehaviors,
                bool needDistinctTimestamps,
                const MutableBehaviorDomain * owner) const
{
    std::unique_lock<Spinlock> guard(lock, std::defer_lock);

    if (!owner->immutable_)
        guard.lock();

    SubjectStats result;
    result.earliest = owner->unQuantizeTime(firstSeen);
    result.latest   = owner->unQuantizeTime(lastSeen);
    result.numBehaviors = behaviorCount;

    // Get the distinct timestamps
    if (needDistinctTimestamps) {

        set<uint64_t> distinctTimestamps;

        auto onBeh = [&] (BI beh, uint64_t ts, int)
            {
                distinctTimestamps.insert(ts);
                return true;
            };
        forEachBehavior(onBeh, owner->immutable_);
        result.numDistinctTimestamps = distinctTimestamps.size();
    }
    else {
        result.numDistinctTimestamps = -1;
    }
   
    // Get the distinct behaviors
    if (needDistinctBehaviors) {

        Lightweight_Hash_Set<int> distinctBehaviors;

        auto onBeh = [&] (BI beh, uint64_t ts, int)
            {
                distinctBehaviors.insert(beh.index());
                return true;
            };
        forEachBehavior(onBeh, owner->immutable_);
        result.numDistinctBehaviors = distinctBehaviors.size();
    }
    else {
        result.numDistinctBehaviors = -1;
    }
    return result;
}

// If you are using ValGrind, uncomment this code.  It will use an exact
// algorithm that doesn't trigger Valgrind but is slower.

// extractFast requires that there always be an extra word allocated, and can in some
// cases segfault.  Until we can arrange for this extra word, we'll just use extract.
#define extractFast extract

template<typename F>
bool
MutableBehaviorDomain::SubjectEntry::
forEachBehavior(const F & onBehavior, bool immutable) const
{
    ML::Bit_Extractor<uint32_t> extractor(data.unsafe_raw_data());

    for (unsigned i = 0;  i < behaviorCount;  ++i) {
        uint32_t beh, tsofs, count;
        extractor.extractFast(beh, behBits);
        extractor.extractFast(tsofs, tsBits);
        extractor.extractFast(count, cntBits);
        count += 1;

        // Good check but slows down the code significantly.  Enable if you
        // are debugging errors.
        //ExcAssertLessEqual(extractor.current_offset(data.unsafe_raw_data()),
        //                   data.capacity() * 32);

        if (!onBehavior(BI(beh), tsofs, count))
            return false;
    }

    ExcAssertLessEqual(extractor.current_offset(data.unsafe_raw_data()),
                       data.capacity() * 32);

    ExcAssertEqual(extractor.current_offset(data.unsafe_raw_data()),
                   (behBits + tsBits + cntBits) * behaviorCount);

    return true;
}

uint32_t
MutableBehaviorDomain::SubjectEntry::
calcNewCapacity(size_t newBitsRequired) const
{
    uint32_t currentCapacity = data.capacity();
    size_t currentBitCapacity = currentCapacity * 32;

    //cerr << "calculating new capacity from " << currentBitCapacity
    //<< " to " << newBitsRequired << endl;

    while (currentBitCapacity < newBitsRequired) {
        uint32_t newCapacity;
        if (currentCapacity < 8)
            newCapacity = currentCapacity + (currentCapacity % 2) + 4;
        else if (currentCapacity < 1024)
            newCapacity = currentCapacity * 2;
        else newCapacity = currentCapacity + 1024;
        //cerr << "currentCapacity = " << currentCapacity
        //     << " newCapacity = " << newCapacity << endl;

        currentBitCapacity = 32 * newCapacity;
        currentCapacity = newCapacity;
    }

    ExcAssertGreaterEqual(currentBitCapacity, newBitsRequired);

    // +1 is for the extractFast function that assumes it can always read
    // an extra word at the end
    return currentCapacity;
}

uint64_t
MutableBehaviorDomain::SubjectEntry::
expand(int newBehBits, int newTsBits, int newCntBits,
       int newBehaviorCount, bool copyOldData,
       int64_t tsOffset)
{
    ExcAssertGreaterEqual(newBehBits, behBits);
    ExcAssertGreaterEqual(newTsBits,  tsBits);
    ExcAssertGreaterEqual(newCntBits, cntBits);

#if 0
    cerr << "expand from " << (int)behBits << " -> " << (int)newBehBits
         << " behBits, " << (int)tsBits << " -> " << (int)newTsBits
         << " tsBits, " << (int)cntBits << " -> " << (int)newCntBits
         << " cntBits, " << behaviorCount << " -> "
         << newBehaviorCount << " count" << endl;
#endif

    int currentWidth = behBits + tsBits + cntBits;
    int newWidth = newBehBits + newTsBits + newCntBits;

    size_t newBitsRequired = newWidth * newBehaviorCount;

    uint64_t currentBitsUsed = currentWidth * behaviorCount;

    if (currentWidth == newWidth
        && newBehBits == behBits
        && newTsBits == tsBits
        && newCntBits == cntBits
        && tsOffset == 0) {

        //cerr << "   *** in-place" << endl;

        if (data.capacity() * 32 >= newBitsRequired)
            return currentBitsUsed;  // current capacity is OK

        //cerr << "expansion" << endl;
        // Need to expand, but our current encoding is still valid so
        // we can copy our data a uint32_t at a time
        uint32_t newCapacity = calcNewCapacity(newBitsRequired);

        compact_vector<uint32_t, 4> newData(newCapacity);  // +1 for extractFast
        if (copyOldData)
            std::copy(data.begin(), data.end(), newData.begin());
        data.swap(newData);
        return currentBitsUsed;
    }

    // Need to re-record everything and resize the new elements
    uint32_t newCapacity = calcNewCapacity(newBitsRequired);

#if 0
    cerr << "capacity from " << data.capacity() * 32 << " to "
         << newCapacity * 32 << " bits" << endl;

    cerr << "currentBitsUsed = " << currentBitsUsed << endl;
    cerr << "newBitsRequired = " << newBitsRequired << endl;
#endif

    compact_vector<uint32_t, 4> newData(newCapacity);  // +1 for extractFast

    if (copyOldData) {
        ssize_t bitsAvailable = newCapacity * 32;
        
        ML::Bit_Writer<uint32_t> writer(newData.unsafe_raw_data());
        
        auto recordBeh = [&] (BI beh, uint64_t ts, uint32_t cnt)
            {
                ExcAssertGreaterEqual(bitsAvailable, newWidth);

                writer.write(beh.index(), newBehBits);
                writer.write(ts + tsOffset, newTsBits);
                writer.write(cnt - 1, newCntBits);

                bitsAvailable -= newWidth;

                return true;
            };

        forEachBehavior(recordBeh, true);

        ExcAssertEqual(writer.current_offset(newData.unsafe_raw_data()),
                       newWidth * behaviorCount);
        ExcAssertGreaterEqual(bitsAvailable, 0);

        ExcAssertLessEqual(writer.current_offset(newData.unsafe_raw_data()),
                           newData.capacity() * 32);

    }

    data.swap(newData);
    behBits = newBehBits;
    tsBits = newTsBits;
    cntBits = newCntBits;

    return newWidth * behaviorCount;
}

void
MutableBehaviorDomain::SubjectEntry::
record(BI beh, uint64_t ts, uint32_t count, VI verb)
{
    std::unique_lock<Spinlock> guard(lock);

    bool check = false;

    std::vector<std::tuple<BI, uint64_t, uint32_t> > correct;

    if (check) {
        auto onBeh = [&] (BI beh, uint64_t ts, uint32_t cnt)
            {
                correct.push_back(make_tuple(beh, ts + firstSeen, cnt));
                return true;
            };

        forEachBehavior(onBeh, false /* immutable */);
        correct.push_back(make_tuple(beh, ts, count));
    }

    //cerr << endl;
    //cerr << "called record() for subject " << id << endl;
    //cerr << "behaviorCount = " << behaviorCount << endl;

    // If possible, we fit into the space we have available.  There are
    // three circumstances in which it would not be possible:
    // 1.  The behavior, timestamp or count have values which don't fit
    //     into the current bucket sizes;
    // 2.  The timestamp is earlier than the previous earliest timestamp,
    //     which means we need to re-offset all of the existing timestamps
    // 3.  There is simply no space available
    
    int currentWidth = behBits + tsBits + cntBits;
    uint64_t currentBitsUsed = behaviorCount * currentWidth;
    uint64_t currentBitCapacity = 32 * data.capacity();
    int64_t bitsLeft = currentBitCapacity - currentBitsUsed;

    uint8_t behBitsRequired = ML::highest_bit(beh.index(), -1) + 1;
    uint8_t cntBitsRequired = ML::highest_bit(count - 1, -1) + 1;

    uint64_t savedFirstSeen = firstSeen;
    firstSeen = std::min(firstSeen, ts);
    lastSeen = std::max(lastSeen, ts);

    uint8_t tsBitsRequired = ML::highest_bit((lastSeen - firstSeen), -1) + 1;

    ExcAssertLessEqual(firstSeen, savedFirstSeen);

    bool didTsAdjustment = false;
    bool didWidthAdjustment = false;
    bool didExpansion = false;

    if (firstSeen < savedFirstSeen && tsBitsRequired == tsBits) {
        didTsAdjustment = true;
        //cerr << "timestamp adjustment" << endl;
        // Go through and adjust timestamps in place for the new offset

        ML::Bit_Extractor<uint32_t> reader(data.unsafe_raw_data());
        ML::Bit_Writer<uint32_t> writer(data.unsafe_raw_data());
        
        for (unsigned i = 0;  i < behaviorCount;  ++i) {
            reader.advance(behBits);  writer.skip(behBits);
            uint64_t ts;
            reader.extractFast(ts, tsBits);
            ts = ts + savedFirstSeen - firstSeen;
            writer.write(ts, tsBits);
            reader.advance(cntBits);  writer.skip(cntBits);
        }

        savedFirstSeen = firstSeen;
    }
    uint8_t newBehBits = std::max(behBits, behBitsRequired);
    uint8_t newTsBits = std::max(tsBits, tsBitsRequired);
    uint8_t newCntBits = std::max(cntBits, cntBitsRequired);

    int newWidth = newBehBits + newTsBits + newCntBits;
    int64_t newBitsUsed = behaviorCount * newWidth;
    int64_t newBitsRequired = newBitsUsed + newWidth;
    //int64_t newBitCapacity = currentBitCapacity;

    //cerr << "currentWidth = " << currentWidth << " newWidth = " << newWidth
    //     << endl;
    //cerr << "currentBitCapacity = " << currentBitCapacity
    //     << " newBitCapacity = " << newBitCapacity << endl;

    if (newWidth != currentWidth) {
        didWidthAdjustment = true;
        //cerr << "width adjustment" << endl;

        currentBitsUsed
            = expand(newBehBits, newTsBits, newCntBits, behaviorCount + 1,
                     true /* copyOldData */, savedFirstSeen - firstSeen);
    }
    else if (bitsLeft < currentWidth) {
        didExpansion = true;

        currentBitsUsed
            =  expand(newBehBits, newTsBits, newCntBits, behaviorCount + 1,
                      true /* copyOldData */, savedFirstSeen - firstSeen);
    }

    ExcAssertGreaterEqual(behBits, behBitsRequired);
    ExcAssertGreaterEqual(cntBits, cntBitsRequired);
    ExcAssertGreaterEqual(tsBits,  tsBitsRequired);

    //cerr << "currentBitsUsed = " << currentBitsUsed << endl;
    
    ML::Bit_Writer<uint32_t> writer(data.unsafe_raw_data());
    writer.skip(currentBitsUsed);
    writer.write(beh.index(), behBits);
    writer.write(ts - firstSeen, tsBits);
    writer.write(count - 1, cntBits);

    ExcAssertEqual(writer.current_offset(data.unsafe_raw_data()),
                   newBitsRequired);

    ++behaviorCount;

    if (check) {
        int i = 0;
        auto onCheckBeh = [&] (BI beh, uint64_t ts, uint32_t cnt)
            {
                ExcAssertLess(i, correct.size());
                if (beh != std::get<0>(correct[i])
                    || ts + firstSeen != std::get<1>(correct[i])
                    || cnt != std::get<2>(correct[i])) {
                    cerr << "i = " << i << " of " << correct.size() << "/"
                         << behaviorCount << endl;
                    cerr << "didTsAdjustment = " << didTsAdjustment << endl;
                    cerr << "didWidthAdjustment = " << didWidthAdjustment << endl;
                    cerr << "didExpansion = " << didExpansion << endl;
                    ExcAssertEqual(beh, std::get<0>(correct[i]));
                    ExcAssertEqual(ts + firstSeen, std::get<1>(correct[i]));
                    ExcAssertEqual(cnt, std::get<2>(correct[i]));
                }
                ++i;
                return true;
            };

        forEachBehavior(onCheckBeh, false);
        ExcAssertEqual(i, behaviorCount);
    }
}

bool
MutableBehaviorDomain::SubjectEntry::
recordOnce(BI beh, uint64_t ts, uint32_t count, VI verb)
{
    throw MLDB::Exception("recordOnce not updated");
#if 0
    std::unique_lock<Spinlock> guard(lock);
    int nb = behaviors.size();
    for (int i = 0;  i < nb;  ++i) {
        if (behaviors[i].behavior == beh) {
            behaviors[i].timestamp
                = std::min(behaviors[i].timestamp, ts);
            return false;  // already recorded
        }
    }
        
    record(beh, ts, count);

    return true;
#endif
}

void
MutableBehaviorDomain::SubjectEntry::
sortUnlocked(bool immutable, const MutableBehaviorDomain *owner)
{
    if (immutable)
        return;

    // TODO: sort in place
    if (behaviorCount < 2) return;

    struct Entry {
        Entry(BI behavior = BI(), uint64_t timestamp = 0,
              uint32_t count = 0)
            : behavior(behavior), timestamp(timestamp), count(count)
        {
        }

        BI behavior;
        uint64_t timestamp;
        uint32_t count;

        bool operator < (const Entry & other) const
        {
            return ML::less_all(timestamp, other.timestamp,
                                behavior, other.behavior,
                                count, other.count);
        }
    };

    std::vector<Entry> behaviors;
    behaviors.reserve(behaviorCount);

    uint64_t totalCount = 0;

    auto onBeh = [&] (BI beh, uint64_t ts, uint32_t cnt)
        {
            behaviors.push_back(Entry(beh, ts, cnt));
            totalCount += cnt;
            return true;
        };

    forEachBehavior(onBeh, immutable);

    ExcAssertEqual(behaviors.size(), behaviorCount);

    std::sort(behaviors.begin(), behaviors.end());
    
    uint32_t maxCount = 0;

    // Write everything, returning the new behavior count
    auto doWrite = [&] () -> int
        {
            ML::Bit_Writer<uint32_t> writer(data.unsafe_raw_data());

            uint64_t writtenCount = 0;

            auto recordBeh = [&] (const Entry & entry)
            {
                //cerr << "writing " << entry.behavior << " " << entry.timestamp
                //<< " with count " << entry.count << endl;
                ExcAssertGreater(entry.count, 0);
                writer.write(entry.behavior.index(), behBits);
                writer.write(entry.timestamp, tsBits);
                writer.write(entry.count - 1, cntBits);
                writtenCount += entry.count;
                maxCount = std::max(maxCount, entry.count);
            };

            // Look for where two adjacent can be merged because they have the
            // same timestamp.
            int in = 1, out = 0;

            Entry curr = behaviors[0];

            for (;in < behaviors.size(); ++in) {
                //cerr << "in = " << in << " out = " << out << endl;
                //cerr << "in = " << in << " out = " << out << " curr = " << curr
                //     << " bin = " << bin << endl;

                auto & bin = behaviors[in];

                if (bin.timestamp == curr.timestamp
                    && bin.behavior == curr.behavior) {
                    //cerr << "adding " << bin.count << " to current count of "
                    //     << curr.count << " for beh " << bin.behavior << endl;
                    uint64_t sum64(curr.count);
                    sum64 += bin.count;
                    if (sum64 > UINT_MAX) {
                        throw MLDB::Exception("adding %u to %u would cause an"
                                            " uint32_t overflow: n_bits(%lu) on behavior id (%s)"
                                            " > 32",
                                            curr.count, bin.count, sum64, 
                                            owner->getBehaviorId(bin.behavior).toString().c_str());
                        
                    }
                    curr.count = sum64;
                }
                else {
                    recordBeh(curr);
                    ++out;
                    curr = bin;//behaviors[in + 1];
                }
            }
    
            ++out;
            recordBeh(curr);

            ExcAssertEqual(totalCount, writtenCount);

            //if (behaviorCount != out)
            //    cerr << "went from " << behaviorCount << " to " << out << " entries"
            //<< endl;

            return out;
        };

    int newBehaviorCount = doWrite();
    int countBitsRequired = ML::highest_bit(maxCount -1, -1) + 1;

    if (countBitsRequired > cntBits) {
        // Counts increased and can no longer fit in.  Expand so that we can
        // fit them.

        //cerr << "merged counts required expansion" << endl;

        expand(behBits, tsBits, countBitsRequired, newBehaviorCount,
               false /* copyOldData */, 0 /* tsOffset */);

        doWrite();
    }
    
    behaviorCount = newBehaviorCount;

    return;
}

/*****************************************************************************/
/* MUTABLE BEHAVIOR DOMAIN BEHAVIOR ENTRY                                  */
/*****************************************************************************/

bool     MutableBehaviorDomain::profile = false;
std::atomic<uint64_t> MutableBehaviorDomain::records(0);
std::atomic<uint64_t> MutableBehaviorDomain::recordsImmediate(0);
std::atomic<uint64_t> MutableBehaviorDomain::recordsWrongWidth(0);
std::atomic<uint64_t> MutableBehaviorDomain::recordsEmpty(0);
std::atomic<uint64_t> MutableBehaviorDomain::recordsNoSpace(0);
std::atomic<uint64_t> MutableBehaviorDomain::recordsEarlierTimestamp(0);
std::atomic<uint64_t> MutableBehaviorDomain::recordTries(0);
std::atomic<uint64_t> MutableBehaviorDomain::recordSpins(0);

void
MutableBehaviorDomain::BehaviorEntry::
record(SI2 sub, uint64_t ts, uint32_t count, VI verb,
       GcLock & rootLock)
{
    bool debug = false; //(sub.toString() == "9c9bd0458210ea8b");

    if (profile)
        ++records;

    atomic_min(earliest, ts);
    atomic_max(latest, ts);

    ExcAssert(earliest <= ts);
    ExcAssert(latest >= ts);

    for (;;) {
        if (profile)
            ++recordTries;

        // Try to insert... fast, speculative path with no locks
        DataNode * current = unsorted;

#if 1
        // First insert?  Do a compare and swap to avoid taking a lock and
        // slowing things down
        if (!current) {
            if (profile)
                ++recordsEmpty;
            DataNode * newNode
                = DataNode::allocate(4, earliest, latest, sub);
            if (!unsorted.compare_exchange_strong(current, newNode))
                // Someone raced us... delete the old one (current is now
                // pointing to the winning node)
                delete newNode;
        }
#endif

        if (debug && current) {
            current->dump();
        }

        //ML::memory_barrier();  // TODO: not necessary?

        // If there is an unsorted table, try to insert our entry in it
        // in an atomic manner without taking the lock.  It could fail
        // for one of three reasons:
        // 
        // 1.  The time offset for our timestamp is lower than the offset
        //     of the table.  We need to reallocate a table and re-offset
        //     the elements.
        // 2.  One of the elements we are trying to record is too wide
        //     for the number of bits that have been reserved for it.
        //     We need to reallocate a wider table.
        // 3.  The current table is full.  We need to clear the table
        //     by merging it with the sorted range.

        if (current) {
            int insertRes = current->atomicRecord(sub, ts);
            //cerr << "insertRes returned " << insertRes << endl;
            //cerr << "current = " << endl;
            //current->dump();
            if (insertRes == 0)
                return;  // succeeded
            if (debug)
                cerr << "couldn't insert for reason " << insertRes
                     << endl;
        }

        // We couldn't reserve it.  The rest of the work
        if (debug)
            cerr << "couldn't get a place" << endl;

        // If we got here but unsorted has changed, then someone else got
        // the lock and did the work.  We cab restart our insert.
        if (current != unsorted)
            continue;  // someone raced us to update it

        //EntryGuard fullGuard(fullLock);

        // If we got here but unsorted has changed, then someone else got
        // the lock and did the work.  We cab restart our insert.
        if (current != unsorted)
            continue;  // someone raced us to update it


        EntryGuard guard2(lock, std::defer_lock);

#if 1
        // Spin trying to acquire the lock, or waiting for the current
        // published node to change.
        int spins = 0;
        for (;; ++spins) {
            if (current != unsorted)
                break;
            else if (guard2.try_lock())
                break;
            std::this_thread::yield();
        }

        if (profile)
            recordSpins += spins;
#endif

        // If we got here but unsorted has changed, then someone else got
        // the lock and did the work.  We cab restart our insert.
        if (current != unsorted)
            continue;  // someone raced us to update it
        
        // If we got here, it is our job to create more space.  We do this
        // by merging together the full or too narrow unsorted table with
        // the current sorted entries, and creating a new unsorted table
        // that is wide enough.


        size_t actualSize = 0;

        if (current)
            actualSize = current->lockWaitAndReturnSize();

        // At this point

        // Create a new node
        uint32_t newSize = sorted ? sorted->size() : 4;

        SI2 maxSub = sub;
        if (sorted && sorted->maxSub() > sub)
            maxSub = sorted->maxSub();

        DataNode * newNode
            = DataNode::allocate(newSize, earliest, latest, maxSub);

        // Insert the new value
        int val = newNode->atomicRecord(sub, ts);
        ExcAssertEqual(val, 0);

        // Publish the new node
        unsorted = newNode;

        // Allow everything waiting on it being full to wake up
        //fullGuard.unlock();

        // It's time for a consolidation, do it
        sortUnlocked(rootLock, false, current, actualSize);

        std::atomic_thread_fence(std::memory_order_release);

        // The old node can only be removed via RCU
        if (current)
            rootLock.defer([=] () { deleteDataNode(current); });

        return;
    }
}

void
MutableBehaviorDomain::BehaviorEntry::
makeImmutable(GcLock & rootLock)
{
    EntryGuard guard(lock);
    sortUnlocked(rootLock, false);

    auto us = unsorted.load();
    ExcAssert(us);
    ExcAssertEqual(us->mergedUpTo, us->size());
    
    // The old node can only be removed via RCU
    rootLock.defer([=] () { deleteDataNode(us); });
    
    // Now make sure nothing else touches it
    unsorted = nullptr;
}

void
MutableBehaviorDomain::BehaviorEntry::
sort(GcLock & rootLock, bool immutable) const
{
    if (!unsorted)
        return;

    EntryGuard guard(lock, std::defer_lock);
    if (!immutable)
        guard.lock();

    sortUnlocked(rootLock, immutable);
}

void
MutableBehaviorDomain::BehaviorEntry::
sortUnlocked(GcLock & rootLock, bool immutable,
             DataNode * unsorted,
             ssize_t unsortedSize) const
{
    // This method will merge together an existing sorted range and a
    // set of unsorted element, replacing the current sorted range
    // with the lot.

    if (!unsorted)
        unsorted = this->unsorted;
    if (!unsorted)
        return;

    if (unsortedSize == -1)
        unsortedSize = unsorted->getCommittedSize();
    if (unsortedSize == 0)
        return;
    if (immutable) {
        ExcAssertEqual(unsortedSize, 0);
    }

#if 0
    cerr << "sortUnlocked-----------------------------" << endl;
    if (sorted) {
        cerr << "sorted = " << endl;
        sorted->dump();
    }
    cerr << "unsorted = " << endl;
    unsorted->dump();
    cerr << "mergedUpTo = " << unsorted->mergedUpTo << endl;
#endif

    // Copy them before sorting to make it more efficient

    int numToAlloc = unsortedSize - unsorted->mergedUpTo;
    PossiblyDynamicBuffer<SubjectEntry> unsorted2Storage(numToAlloc);
    SubjectEntry * unsorted2 = unsorted2Storage.data();

    int unsorted2Size = 0;
    for (unsigned i = unsorted->mergedUpTo;  i != unsortedSize;  ++i) {
        ExcAssert(unsorted->isPresentEntry(i));
        unsorted2[unsorted2Size++] = (*unsorted)[i];
    }

    std::sort(unsorted2, unsorted2 + unsorted2Size);
    
    if (unsorted2Size == 0)
        return;  // already is done

    size_t sortedSize = (sorted ? sorted->size() : 0);

    numToAlloc = unsorted2Size + sortedSize;
    PossiblyDynamicBuffer<SubjectEntry> newSortedStorage(numToAlloc);
    SubjectEntry * newSorted = newSortedStorage.data();
    uint32_t newSortedSize = 0;

    // Perform a merge of the old sorted and newly inserted values.  Note
    // that there may be duplicates in (p2,e2) but not in (p1,e1).
    int i1 = 0, e1 = sortedSize;
    int i2 = 0, e2 = unsorted2Size;

    SI2 lastSubj;

    uint64_t base1 = sorted ? sorted->timeBase : 0;
    uint64_t base2 = unsorted->timeBase;
    uint64_t base0 = this->earliest;  // base of output

    if (base0 > base2) {
        cerr << "base0 = " << base0 << endl;
        cerr << "base1 = " << base1 << endl;
        cerr << "base2 = " << base2 << endl;
        cerr << "earliest = " << earliest << " latest = " << latest << endl;
        cerr << "unsorted size = " << unsortedSize << endl;
        cerr << "unsorted time base = " << unsorted->timeBase << endl;
        cerr << "sorted size = " << sortedSize << endl;
    }

    ExcAssertLessEqual(base0, base2);

    auto addEntry = [&] (SI2 subj, uint64_t ts)
        {
            newSorted[newSortedSize].subj = subj;
            newSorted[newSortedSize].tsOfs = ts - base0;
            ++newSortedSize;
        };

    // Merge together the two ranges.  Unsorted2 may include duplicate
    // elements which we want to skip.
    while (i1 < e1 && i2 < e2) {
        SubjectEntry se1 = (*sorted)[i1];
        const SubjectEntry & se2 = unsorted2[i2];
        
        //cerr << "i1 = " << i1 << " e1 = " << e1 << " el = " << se1.subj
        //     << " --> " << se1.tsOfs << endl;
        //cerr << "i2 = " << i2 << " e2 = " << e2 << " el = " << se2.subj
        //     << " --> " << se2.tsOfs << endl;
        //cerr << endl;

        if (se1.subj == se2.subj) {
            lastSubj = se1.subj;

            addEntry(lastSubj, std::min(se1.tsOfs + base1, se2.tsOfs + base2));
            ++i1;
            ++i2;

            while (i2 < e2 && unsorted2[i2].subj == lastSubj)
                ++i2;
        }
        else if (se1.subj < se2.subj) {
            lastSubj = se1.subj;
            addEntry(lastSubj, se1.tsOfs + base1);
            ++i1;
        }
        else {
            lastSubj = se2.subj;
            addEntry(lastSubj, se2.tsOfs + base2);
            ++i2;
            while (i2 < e2 && unsorted2[i2].subj == lastSubj)
                ++i2;
        }
    }

    // Finish up anything left in sorted
    while (i1 < e1) {
        SubjectEntry se1 = (*sorted)[i1];
        lastSubj = se1.subj;
        addEntry(lastSubj, se1.tsOfs + base1);
        ++i1;
    }
    
    // Finish up anything left in unsorted
    while (i2 < e2) {
        const SubjectEntry & se2 = unsorted2[i2];
        if (se2.subj != lastSubj) {
            lastSubj = se2.subj;
            addEntry(lastSubj, se2.tsOfs + base2);
            ++i2;
        }
        while (i2 < e2 && unsorted2[i2].subj == lastSubj)
            ++i2;
    }

    unsorted->mergedUpTo = unsortedSize;

    // Could be deferred to allow lock to be given up quicker
    deleteDataNode(sorted);

    sorted = DataNode::allocate(newSortedSize, base0,
                                latest, lastSubj,
                                &newSorted[0], &newSorted[newSortedSize]);

    //cerr << "output:" << endl;
    //sorted->dump();
    
    ExcAssertEqual(sorted->size(), newSortedSize);
}

void
MutableBehaviorDomain::BehaviorEntry::
clear(GcLock & rootLock)
{
    EntryGuard guard(lock);

    earliest = 0;
    latest = std::numeric_limits<int64_t>::max();
    deleteDataNode(sorted);
    rootLock.defer([=] () { deleteDataNode(unsorted); });

    unsorted = nullptr;
}

template<typename F>
bool
MutableBehaviorDomain::BehaviorEntry::
forEachSubject(const F & onSubject, bool immutable,
               GcLock & rootLock) const
{
    EntryGuard guard(lock, std::defer_lock);
    if (!immutable)
        guard.lock();
    
    sortUnlocked(rootLock, immutable);
    if (!sorted)
        return true;
    for (unsigned i = 0;  i < sorted->size();  ++i) {
        auto entry = (*sorted)[i];
        if (!onSubject(entry.subj, entry.tsOfs + sorted->timeBase))
            return false;
    }
    return true;
}

void
MutableBehaviorDomain::BehaviorEntry::
deleteDataNode(DataNode * node)
{
    return DataNode::deallocate(node);
}

void
MutableBehaviorDomain::
makeImmutable()
{
    if (immutable_)
        return;
    std::unique_lock<std::mutex> guard(immutableLock);
    if (immutable_)
        return;

    {
        auto doSubjectRoot = [&] (int s)
            {
                auto subs = subjectRoots[s]->subjectEntryPtr();
                
                auto doSubjectEntry = [&] (int i)
                {
                    subs->entries[i].load()->sort(false, this);
                };

                //cerr << "root " << s << " has " << subs->size << " entries"
                //<< endl;

                parallelMap(0, subs->size.load(), doSubjectEntry);
            };

        for (unsigned r = 0;  r < NUM_SUBJECT_ROOTS;  ++r)
            doSubjectRoot(r);
        //parallelMap(0, NUM_SUBJECT_ROOTS, doSubjectRoot);

        auto behs = behaviorRoot();

        auto doBehaviorRoot = [&] (int i)
            {
                behs->entries[i].load()->makeImmutable(rootLock);
            };

        parallelMap(0, behs->size.load(), doBehaviorRoot);

        if (!nominalStart_.isADate())
            nominalStart_ = Date::fromSecondsSinceEpoch(earliest_);
        if (!nominalEnd_.isADate())
            nominalEnd_ = Date::fromSecondsSinceEpoch(latest_);

        {
            // Acquire and release all locks to be sure that nobody is using them
            std::unique_lock<IndexLock> guard1(behaviorIndexLock);
        }
    }

    rootLock.visibleBarrier();
    
    immutable_ = true;
}

uint64_t
MutableBehaviorDomain::
getApproximateFileSize() const
{
    uint64_t result = 4096;
    
    for (unsigned i = 0;  i < NUM_SUBJECT_ROOTS;  ++i) {
        auto & subjectRoot = *subjectRoots[i];
        auto root = subjectRoot.subjectEntryPtr();

        for (unsigned i = 0;  i < root->size;  ++i) {
            SubjectEntry * entry = root->entries[i];
            if (!entry)
                continue;

            result += entry->data.size() * 4;
            result += entry->id.toString().size();
            result += 16;
        }
    }

    auto r = behaviorRoot();

    for (unsigned i = 0;  i < r->size;  ++i) {
        BehaviorEntry * entry = r->entries[i];
        if (!entry)
            continue;

        BehaviorEntry::EntryGuard guard(entry->lock);

        result += entry->id.toString().size();

        entry->sortUnlocked(rootLock, immutable_);
        result += entry->sorted->size() * 16;
        result += 16;
    }

    return result;
}

Json::Value
MutableBehaviorDomain::
getMemoryStats() const
{
    Json::Value result;

    GcLock::SharedGuard pin(rootLock, GcLock::RD_YES,
                            immutable_ ? GcLock::DONT_LOCK : GcLock::DO_LOCK);

    size_t subjectMem = 0;
    size_t subjectRootMem = 0;
    for (unsigned i = 0;  i < NUM_SUBJECT_ROOTS;  ++i) {
        auto & subjectRoot = *subjectRoots[i];
        auto root = subjectRoot.subjectEntryPtr();

        subjectMem += root->memUsage();
        subjectRootMem += sizeof(subjectRoot)
            + subjectRoot.subjectIndexPtr()->capacity()
            * 12;
    };

    result["subjectBehaviorsMemoryMb"] = subjectMem / 1000000.0;

    auto r = behaviorRoot();
    size_t behRootMem = r->memUsage();
    result["behaviorSubjectsMemoryMb"] = behRootMem / 1000000.0;
    size_t behCacheMem = behaviorPartialCache()->capacity() * 12;
    result["behaviorPartialCacheMemoryMb"] =  behCacheMem / 1000000.0;
    result["totalMemoryMb"]
        = (subjectMem + subjectRootMem + behRootMem + behCacheMem + sizeof(this)) / 1000000.0;
    
    return result;
}

int64_t
MutableBehaviorDomain::
approximateMemoryUsage() const
{
    return getMemoryStats()["totalMemoryMb"].asDouble() * 1000000.0;
}

SH
MutableBehaviorDomain::
getSubjectHash(SI2 index) const
{
    ExcAssertNotEqual(index, SI2());

    if (immutable_)
        return subjectRoots[index.root]->subjectEntryPtr.unsafePtr()->entries[index.idx].load()->hash;
    else
        return subjectRoots[index.root]->subjectEntryPtr()->entries[index.idx].load()->hash;
}

std::ostream &
operator << (std::ostream & stream, MutableBehaviorDomain::SI2 si2)
{
    return stream << "{\"root\":" << si2.root << ",\"idx\":" << si2.idx
                  << ",\"bits\":" << si2.bits << "}";
}

int
MutableBehaviorDomain::BehaviorEntry::DataNode::
atomicRecord(SI2 sub, uint64_t ts)
{
    //cerr << "recording " << sub << " at " << ts << " at base "
    //     << timeBase << endl;

    if (ts < timeBase) {
        if (profile)
            ++recordsEarlierTimestamp;

        return 1;
    }

#if 0
    cerr << " capacity " << this->capacity_
         << " size " << this->size_ << " fnd " << this->firstNotDone_
         << " bits " << bitsPerEntry() << " words "
         << memWordsAllocated() << endl;
#endif

    int subjBits = ML::highest_bit(sub.bits, -1) + 1;
    int tsBits = ML::highest_bit(ts - timeBase, -1) + 1;
    if (subjBits > this->subjectBits
        || tsBits > this->timestampBits)
        return 2;  // can't fit

    uint32_t pos = size_.fetch_add(1);

    if (!(pos & SizeLockBit) && pos < this->capacity_) {
        //cerr << "val " << this->vals[pos].subj << endl;
        //ExcAssert(!this->isPresentEntry(pos));
        setEntryAtomic(pos, SubjectEntry{sub, (uint32_t)(ts - timeBase)});
        if (profile)
            ++recordsImmediate;

        for (;;) {
            uint32_t fnd = this->firstNotDone_;

            // Size can increment beyond capacity temporarily due to fresh
            // writers. This could cause fnd to be incremented beyond capacity
            // if we didn't check for it specifically which would lead to
            // isPresentEntry reading arbitrary memory.
            //
            // Note that we can resize before reaching capacity. In this case we
            // don't need any special checks before the memory is well defined
            // to contain only 0s which will cause isPresentEntry to always
            // false.
            if (fnd >= size() || fnd == capacity_)
                break;

            //cerr << "fnd = " << fnd << " size = " << size_ << " cap = "
            //     << capacity_ << " val " << vals[fnd].subj << endl;

            if (!this->isPresentEntry(fnd))
                break;

            firstNotDone_.compare_exchange_strong(fnd, fnd + 1);
        }
        //cerr << "finished: pos " << pos << " capacity " << this->capacity_
        //     << " size " << this->size_ << " fnd " << this->firstNotDone_
        //     << endl;

        return 0;
    }
    else {
        // Can't get a place... put things back
        size_--;
        if (profile)
            ++recordsNoSpace;
        return 3;
    }
}

void
MutableBehaviorDomain::BehaviorEntry::DataNode::
dump(std::ostream & stream)
{
    stream << "size " << this->size()
           << " cap " << this->capacity() << " fnd "
           << this->firstNotDone() << " mergedupto "
           << this->mergedUpTo << endl;
    for (unsigned i = 0;  i < this->size();  ++i) {
        stream << "  " 
               << " present " << isPresentEntry(i);
        if (isPresentEntry(i))
            cerr << " " << (*this)[i].subj << " -> "
                 << (*this)[i].tsOfs + this->timeBase;
        cerr << endl;
    }
    stream << endl;
}

MutableBehaviorDomain::BehaviorEntry::DataNode *
MutableBehaviorDomain::BehaviorEntry::DataNode::
allocate(uint32_t capacity, uint64_t timeBase,
         uint64_t maxTime, SI2 maxSubject,
         const SubjectEntry * first, const SubjectEntry * last)
{
    int subjectBits = ML::highest_bit(maxSubject.bits, -1) + 1;
    int timestampBits = ML::highest_bit(maxTime - timeBase) + 1;
    size_t bitsRequired = size_t(capacity) * (1 + subjectBits + timestampBits);

    size_t wordsRequired = (63 + bitsRequired) / 64;
    size_t bitsObtained = 64 * wordsRequired;
    size_t realCapacity = bitsObtained / (1 + subjectBits + timestampBits);
    if (realCapacity > UINT_MAX) {
        throw MLDB::Exception("capacity must hold in a uint32_t");
    }

    //cerr << "bitsRequired " << bitsRequired
    //     << " capacity = " << capacity
    //     << " wordsRequired = " << wordsRequired
    //     << " realCapacity = " << realCapacity << endl;

    size_t mem = sizeof(DataNode) + 8 * wordsRequired;
    void * obj = malloc(mem);
    if (bitsRequired > 0x7fffffff) {
        ::fprintf(stderr,
                  "huge allocation (success: %d):"
                  " capacity: %u; timeBase: %lu; maxTime: %lu\n"
                  "subjectBits: %d; timestampBits: %d; bitsRequired: %lu;"
                  " wordsRequired: %lu; bitsObtained: %lu; realCapacity: %lu\n"
                  "memSize: %lu\n",
                  (obj != nullptr),
                  capacity, timeBase, maxTime, subjectBits, timestampBits,
                  bitsRequired, wordsRequired, bitsObtained, realCapacity, mem);
    }
    if (!obj)
        throw std::bad_alloc();
    
    MutableBehaviorDomain::BehaviorEntry::DataNode * node
        = new (obj) MutableBehaviorDomain::BehaviorEntry::DataNode(capacity, timeBase);
    node->capacity_ = realCapacity;
    node->size_ = last - first;
    node->firstNotDone_ = node->size_.load();
    node->subjectBits = subjectBits;
    node->timestampBits = timestampBits;

    // Zero out the memory so everything starts unallocated
    memset(node->mem, 0, wordsRequired * 8);

    if (node->size_ > 0) {
        uint64_t maxBits64 = uint64_t(node->size_) * node->bitsPerEntry();
        if (maxBits64 > UINT_MAX) {
            throw MLDB::Exception("using position %lu would overflow in bitops"
                                " with %d bits/entry",
                                node->size_.load(), node->bitsPerEntry());
        }
    }

    std::atomic<uint64_t> * ptr = node->mem;
    uint64_t accum = 0;
    int64_t bitsDone = 0;

    auto writeBits = [&] (uint64_t val, int bits)
        {
            accum = accum | (val << bitsDone);
            bitsDone += bits;
            if (bitsDone >= 64) {
                ptr->store(accum, std::memory_order_relaxed);
                bitsDone -= 64;
                accum = val >> (bits - bitsDone);
                ++ptr;
            }
        };
    
    // Copy in the entries
    for (size_t i = 0;  i < node->size_;  ++i) {
        writeBits(1, 1);  // Flag saying we're ready
        writeBits(first[i].subj.bits, subjectBits);
        writeBits(first[i].tsOfs, timestampBits);
    }

    // Fill in the rest with zeros
    if (bitsDone > 0)
        writeBits(0, 64 - bitsDone);

    while (ptr < node->mem + wordsRequired) {
        ptr++->store(0, std::memory_order_relaxed);
    }

    return node;
}

void
MutableBehaviorDomain::BehaviorEntry::DataNode::
deallocate(DataNode * node)
{
    free(node);
}

size_t
MutableBehaviorDomain::BehaviorEntry::DataNode::
lockWaitAndReturnSize()
{
    // Lock the node to prevent any new additions in atomicRecord.
    uint32_t oldSize = size_.fetch_or(SizeLockBit);

    // Wait for the ongoing writers to finish up.
    //
    // Note that we need to re-read the size on every iteration because size_ is
    // preemptively incremented in atomicRecord and decremented if there's no
    // more room in the node. This means that size can be greater then the
    // actual size of the node but, since we locked the node, will eventually
    // shrink back down to its actual value.
    //
    // Since any size increment that leads to a decrement will not increment
    // firstNotDone, then we can be assured that if, at any point, size is equal
    // to firstNotDone then all actual writers are done and we can move on.
    while (oldSize > firstNotDone_) oldSize = size();

    ExcAssertEqual(oldSize, firstNotDone_);
    return oldSize;
}

/** Set the given range of bits in out to the given value.  Note that val
    mustn't have bits set outside bits, and it must entirely fit within
    the value. 

    This assumes that the underlying bits are already zero, and will do
    it atomically so that it won't interfere with any simultaneous non-
    overlapping writes.
*/
template<typename Data>
MLDB_ALWAYS_INLINE MLDB_PURE_FN
void confined_set_bits(std::atomic<Data> & in, Data val, shift_t bit, shift_t bits)
{
    // Create a mask with the bits to modify
    //Data mask = bits >= 64 ? -1 : (Data(1) << bits) - 1;
    //mask <<= bit;
    val  <<= bit;

    //ExcAssertEqual(in & mask, 0);
    in.fetch_or(val, std::memory_order_release);
}

/** Set the given range of bits in out to the given value.  Note that val
    mustn't have bits set outside bits, and it must entirely fit within
    the value. 

    This assumes that the underlying bits are already zero, and will do
    it atomically so that it won't interfere with any simultaneous non-
    overlapping writes.
*/
template<typename Data>
MLDB_ALWAYS_INLINE void
confined_set_bit_range(std::atomic<Data>& p0, std::atomic<Data>& p1,
                       Data val, shift_t bit, shift_t bits)
{
    if (MLDB_UNLIKELY(bits == 0)) return;

    //cerr << "setting bits " << bit << " to " << bit + bits << " to "
    //     << val << " current " << p1 << " and " << p1 << endl;

    /* There's some part of the first and some part of the second
       value (both zero or more bits) that need to be replaced by the
       low and high bits respectively. */

    enum { DBITS = sizeof(Data) * 8 };

    int bits0 = std::min<int>(bits, DBITS - bit);
    int bits1 = bits - bits0;

    confined_set_bits<Data>(p0, val, bit, bits0);
    if (bits1)
        confined_set_bits<Data>(p1, val >> bits0, 0, bits1);
}

template<typename T>
static MLDB_ALWAYS_INLINE void
atomicSet(int bitOffset, std::atomic<T> * data, T val, int bits)
{
    int wordOffset = bitOffset / (8 * sizeof(T));
    int startBit = bitOffset % (8 * sizeof(T));

    confined_set_bit_range<T>(data[wordOffset], data[wordOffset + 1],
                              val, startBit, bits);
}

// Set the given entry.  If it fails, that means that the entry
// is not wide enough, which should trigger a realloc
void
MutableBehaviorDomain::BehaviorEntry::DataNode::
setEntryAtomic(size_t pos, const SubjectEntry & entry)
{
    size_t startBit = pos * bitsPerEntry();
    size_t dataBits = this->subjectBits + this->timestampBits;
    size_t endBit = startBit + dataBits;

    // If all are within one word, then we can do one single operation
    // including the present bit.
    if (startBit / 64 == endBit / 64) {
        atomicSet<uint64_t>(startBit, mem,
                            1
                            | (uint64_t(entry.subj.bits) << 1)
                            | (uint64_t(entry.tsOfs) << (this->subjectBits + 1)),
                            this->subjectBits + this->timestampBits + 1);
    }
    else {
        // If we can fit both in one word, then do so
        if (this->subjectBits + this->timestampBits <= 64) {
            atomicSet<uint64_t>(startBit + 1, mem,
                                entry.subj.bits
                                | (uint64_t(entry.tsOfs) << this->subjectBits),
                                this->subjectBits + this->timestampBits);
        }
        else {
            // 1.  Set the subject and the valid bit, a 64 bit operation
            atomicSet<uint64_t>(startBit + 1, mem, entry.subj.bits,
                                this->subjectBits);
                
            // 2.  Set the timestamp, a 64 bit operation
            atomicSet<uint64_t>(startBit + 1 + this->subjectBits, mem, entry.tsOfs,
                                this->timestampBits);
        }

        // Commit the writes before we make it present
        // NOTE: we shouldn't need this...
        std::atomic_thread_fence(std::memory_order_release);

        // 3.  Set the present flag
        atomicSet<uint64_t>(startBit, mem, 1, 1);
    }

#if 0
    ExcAssertEqual(operator [] (pos).subj, entry.subj);
    ExcAssertEqual(operator [] (pos).tsOfs, entry.tsOfs);
    ExcAssert(isValidEntry(pos));
    ExcAssert(isPresentEntry(pos));
#endif
}

} // namespace MLDB
