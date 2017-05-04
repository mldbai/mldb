// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* merged_behavior_domain.cc
   Jeremy Barnes, 4 April 2012
   Copyright (c) 2012 mldb.ai inc.

*/

#include "merged_behavior_domain.h"
#include "mldb/base/exc_assert.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/arch/timers.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/arch/bitops.h"
#include "mldb/base/parallel.h"
#include "mldb/builtin/merge_hash_entries.h"
#include "mldb/ext/cityhash/src/city.h"
#include "mldb/server/parallel_merge_sort.h"
#include <thread>


using namespace std;
using namespace ML;


namespace std {
template<>
struct hash<pair<uint64_t, MLDB::Date> > {
    size_t operator () (const std::pair<uint64_t, MLDB::Date> & p) const
    {
        union {
            double d;
            uint64_t i;
        } u;
        u.d = p.second.secondsSinceEpoch();
        
        return Hash128to64(make_pair(u.i, p.first));
    };
};

} // namespace std

namespace MLDB {

namespace {

bool overlaps(Date s1, Date e1, Date s2, Date e2)
{
    ExcAssertGreaterEqual(e1, s1);
    ExcAssertGreaterEqual(e2, s2);

    if (s2 < s1) {
        std::swap(s1, s2);
        std::swap(e1, e2);
    }

    //
    //  +-----------+...........+
    //      +------------+
    if (e1 >= s2) return true;

    return false;
};

} // file scope

/*****************************************************************************/
/* MERGED BEHAVIOR DOMAIN                                                   */
/*****************************************************************************/


MergedBehaviorDomain::
MergedBehaviorDomain()
{
    clear();
}

MergedBehaviorDomain::
MergedBehaviorDomain(const MergedBehaviorDomain & other)
    : behs(other.behs),
      behIndex(other.behIndex),
      subjectIndex(other.subjectIndex),
      earliest_(other.earliest_),
      latest_(other.latest_),
      nominalStart_(other.nominalStart_),
      nominalEnd_(other.nominalEnd_),
      nonOverlappingInTime(other.nonOverlappingInTime)
{
    hasSubjectIds = other.hasSubjectIds;
    minSubjects = other.minSubjects;
    timeQuantum = other.timeQuantum;
}

MergedBehaviorDomain::
MergedBehaviorDomain(const std::vector<std::shared_ptr<BehaviorDomain> >
                          & toMerge,
                      bool preIndex)
{
    init(toMerge, preIndex);
}

void
MergedBehaviorDomain::
clear()
{
    behs.clear();
    behIndex.clear();
    subjectIndex.clear();
    earliest_ = nominalStart_ = Date::positiveInfinity();
    latest_ = nominalEnd_ = Date::negativeInfinity();
    hasSubjectIds = true;
    nonOverlappingInTime = true;
}

void
MergedBehaviorDomain::
init(std::vector<std::shared_ptr<BehaviorDomain> > toMerge_,
     bool preIndex)
{
    Timer total;

    std::sort(toMerge_.begin(), toMerge_.end(),
              [] (std::shared_ptr<BehaviorDomain> p1,
                  std::shared_ptr<BehaviorDomain> p2)
              {
                  //return p1->earliestTime() < p2->earliestTime();
                  return p1->behaviorCount() + p1->subjectCount()
                      > p2->behaviorCount() + p2->subjectCount();
              });

    std::vector<std::shared_ptr<BehaviorDomain> > toMerge2, toMerge;

    for (auto b: toMerge_)
        if (!b->empty())
            toMerge2.push_back(b);

    //cerr << "toMerge2.size() = " << toMerge2.size() << endl;

    int numSubMerges = (toMerge2.size() - 1) / 31;

    // The first 31 - numSubMerges get added directly
    int numDirect = std::max(0, 31 - numSubMerges);
    numDirect = std::min<int>(numDirect, toMerge2.size());

    //cerr << "numdirect = " << numDirect << " numSubMerges = "
    //     << numSubMerges << endl;

    for (unsigned i = 0;  i < numDirect;  ++i) {
        toMerge.push_back(toMerge2[i]);
    }

    // The rest get broken down into groups of 31, pre-merged, and then added
    for (int current = numDirect;  current < toMerge2.size();  ) {
        //cerr << "current = " << current << " toMerge.size() = "
        //     << toMerge.size() << endl;
        vector<std::shared_ptr<BehaviorDomain> > subToMerge;
        for (; current < toMerge2.size() && subToMerge.size() < 31;  ++current) {
            //cerr << "adding " << current << endl;
            subToMerge.push_back(toMerge2[current]);
        }

        //cerr << "merging " << subToMerge.size() << " files" << endl;

        toMerge.push_back(std::make_shared<MergedBehaviorDomain>(subToMerge, true));
        //cerr << "done merging " << subToMerge.size() << " files" << endl;
    }
    
    clear();

    for (unsigned i = 0;  i < toMerge.size();  ++i) {
        
        auto beh = toMerge[i];

        if (beh->empty())
            continue;

        if (!beh->hasSubjectIds) {
            hasSubjectIds = false;
        }
        
        ExcAssertGreater(beh->nominalStart(), Date());
        //ExcAssertGreater(beh->nominalEnd(), beh->nominalStart());

        //cerr << "merging file with " << beh->earliestTime() << "-"
        //     << beh->latestTime() << endl;

        if (nonOverlappingInTime) {
            for (unsigned i = 0;  i < behs.size();  ++i) {
                if (overlaps(behs[i]->earliestTime(), behs[i]->latestTime(),
                             beh->earliestTime(), beh->latestTime())) {
                    //cerr << "overlap between (" << behs[i]->earliestTime()
                    //     << "," << behs[i]->latestTime() << "] and )"
                    //     << beh->earliestTime() << "," << beh->latestTime()
                    //     << "]" << endl;
                    nonOverlappingInTime = false;
                }
            }

        }

        behs.push_back(beh);

        //cerr << "done merging" << endl;
        earliest_ = std::min(earliest_, beh->earliestTime());
        latest_ = std::max(latest_, beh->latestTime());
        nominalStart_ = std::min(nominalStart_, beh->nominalStart());
        nominalEnd_ = std::max(nominalEnd_, beh->nominalEnd());
    }


    Timer timer;

    auto getSubjectHashes = [&] (int first)
        {
            
            auto behs = toMerge[first];
            MergeHashEntries result;
            vector<SH> subs = behs->allSubjectHashes(SH::max(), true);
            result.reserve(subs.size());
            if (first >= 31)
                first = 31;
            for (auto s: subs)
                result.add(s.hash(), 1ULL << first);
            return result;
        };

    auto getBehaviorHashes = [&] (int first)
        {
            auto behs = toMerge[first];
            MergeHashEntries result;
            vector<BH> beh = behs->allBehaviorHashes(true);
            result.reserve(beh.size());
            if (first >= 31)
                first = 31;
            for (auto s: beh)
                result.add(s.hash(), 1ULL << first);
            return result;
        };

    auto initSubjBucket = [&] (int i, MergeHashEntryBucket & b)
        {
            auto & b2 = subjectIndex.buckets[i];

            b2.reserve(b.size());

            for (auto & e: b) {
                uint64_t h = e.hash;
                uint32_t bm = e.bitmap;
                b2.insert(make_pair(h, bm));
            }
        };

    auto initBehBucket = [&] (int i, MergeHashEntryBucket & b)
        {
            auto & b2 = behIndex.buckets[i];

            b2.reserve(b.size());

            for (auto & e: b) {
                uint64_t h = e.hash;
                uint32_t bm = e.bitmap;
                b2.insert(make_pair(h, bm));
            }
        };

    std::thread mergeBehs([&] () { extractAndMerge(toMerge.size(), getBehaviorHashes, initBehBucket); });
    std::thread mergeSubs([&] () { extractAndMerge(toMerge.size(), getSubjectHashes, initSubjBucket); });

    mergeBehs.join();
    mergeSubs.join();

    // Now merge the metadata

    std::map<std::string, std::vector<Json::Value> > keyValues;

    for (auto b: toMerge) {
        auto md = b->getAllFileMetadata();
        for (auto it = md.begin(), end = md.end();  it != end;  ++it)
            keyValues[it.memberName()].push_back(*it);
    }

    fileMetadata_.clear();

    for (auto kv: keyValues) {
        const string & key = kv.first;
        auto & vals = kv.second;

        std::set<Json::Value> allVals;

        for (auto v: vals)
            allVals.insert(std::move(v));

        auto & md = fileMetadata_[key];

        if (allVals.size() == 1)
            md = std::move(*allVals.begin());
        else {
            for (auto v: allVals)
                md[md.size()] = v;
        }
    }

    //cerr << "got " << subj.size() << " subjects and "
    //     << behs.size() << " behaviors in "
    //     << timer.elapsed() << endl;
}

MergedBehaviorDomain *
MergedBehaviorDomain::
makeShallowCopy() const
{
    return new MergedBehaviorDomain(*this);
}

MergedBehaviorDomain *
MergedBehaviorDomain::
makeDeepCopy() const
{
    throw MLDB::Exception("MergedBehaviorDomain::makeDeepCopy(): not done");
    return new MergedBehaviorDomain(*this);
}

size_t
MergedBehaviorDomain::
subjectCount() const
{
    return subjectIndex.size();
}
    
size_t
MergedBehaviorDomain::
behaviorCount() const
{
    return behIndex.size();
}

std::vector<BH>
MergedBehaviorDomain::
allBehaviorHashes(bool sorted) const
{
    std::vector<BH> result;

    auto onBeh = [&] (uint64_t hash, uint32_t val)
        {
            BH bh(hash);
            result.push_back(bh);
            return true;
        };
    
    behIndex.forEach(onBeh);
    
    if (sorted)
        std::sort(result.begin(), result.end());

    return result;
}

struct MergedBehaviorDomainBehaviorStream
    : public BehaviorDomain::BehaviorStream {
    MergedBehaviorDomainBehaviorStream(const MergedBehaviorDomain* source,
                                         size_t start) :source(source)
    {
        it = source->behIndex.begin();
        for (size_t i = 0; i < start; ++i)
            ++it;
    }   

    virtual Id next()
    { 
        Id result = current();
        advance();
        return result;
    }

    virtual Id current() const
    {
        BH behHash((*it).first);
        uint32_t bitmap = (*it).second;

        int n = __builtin_popcount(bitmap);

        if (n == 0)
            throw MLDB::Exception("attempt to get behavior ID for unknown subject");

        int bit = ML::lowest_bit(bitmap, -1);

        if (bit == 31) {
            for (unsigned i = 31;  i < source->behs.size();  ++i) {
                if (source->behs[i]->knownBehavior(behHash))
                    return source->behs[i]->getBehaviorId(behHash);
            }
            throw MLDB::Exception("logic error in getBehaviorId");
        }

        return source->behs[bit]->getBehaviorId(behHash);
    }

    virtual void advance()
    {
        ++it;
    }

    virtual void advanceBy(size_t n)
    {
        while (n--)
            ++it;
    } 

    IdHashes::const_iterator it;
    const MergedBehaviorDomain* source;
};


std::unique_ptr<BehaviorDomain::BehaviorStream>
MergedBehaviorDomain::
getBehaviorStream(size_t start) const
{
    return std::unique_ptr<BehaviorDomain::BehaviorStream>
        (new MergedBehaviorDomainBehaviorStream(this, start));
}

struct MergedBehaviorDomainSubjectStream
    : public BehaviorDomain::SubjectStream {
    MergedBehaviorDomainSubjectStream(const MergedBehaviorDomain* source,
                                       size_t start)
        : source(source)
    {
        it = source->subjectIndex.begin();
        for (size_t i = 0; i < start; ++i)
            ++it;
    }   

    virtual Id next()
    { 
        Id result = current();
        advance();
        return result;
    }

    virtual Id current() const
    {
        BH behHash((*it).first);
        uint32_t bitmap = (*it).second;

        int n = __builtin_popcount(bitmap);

        if (n == 0)
            throw MLDB::Exception("attempt to get behavior ID for unknown subject");

        int bit = ML::lowest_bit(bitmap, -1);

        if (bit == 31) {
            for (unsigned i = 31;  i < source->behs.size();  ++i) {
                if (source->behs[i]->knownBehavior(behHash))
                    return source->behs[i]->getBehaviorId(behHash);
            }
            throw MLDB::Exception("logic error in getBehaviorId");
        }

        return source->behs[bit]->getBehaviorId(behHash);
    }

    virtual void advance()
    {
        ++it;
    }

    virtual void advanceBy(size_t n)
    {
        while (n--)
            ++it;
    } 

    IdHashes::const_iterator it;
    const MergedBehaviorDomain* source;
};


std::unique_ptr<BehaviorDomain::SubjectStream>
MergedBehaviorDomain::
getSubjectStream(size_t start) const
{
    return std::unique_ptr<BehaviorDomain::SubjectStream>
        (new MergedBehaviorDomainSubjectStream(this, start));
}

std::vector<SH>
MergedBehaviorDomain::
allSubjectHashes(SH maxSubject, bool sorted) const
{
    std::vector<SH> result;

    auto onSubject = [&] (uint64_t hash, uint32_t val)
        {
            SH sh(hash);
            if (sh <= maxSubject)
                result.push_back(sh);
            return true;
        };
    
    subjectIndex.forEach(onSubject);

    if (sorted)
        MLDB::parallelQuickSortRecursive<SH>(result);

    return result;
}

bool
MergedBehaviorDomain::
forEachSubject(const OnSubject & onSubject,
               const SubjectFilter & filter) const
{
    auto onSubject2 = [&] (uint64_t hash, uint32_t val)
        {
            SubjectIterInfo info;

            if (filter.minDistinctBehaviors != -1
                && !subjectHasNDistinctBehaviors(SH(hash),
                                                  filter.minDistinctBehaviors))
                return true;
            
            return onSubject(SH(hash), info);
        };

    subjectIndex.forEach(onSubject2);

    return true;
}

bool
MergedBehaviorDomain::
knownSubject(SH subjectHash) const
{
    return getSubjectBitmap(subjectHash) != 0;
}

bool
MergedBehaviorDomain::
knownBehavior(BH behHash) const
{
    return behIndex.count(behHash);
}

bool
MergedBehaviorDomain::
forEachSubjectBehaviorHash(SH subjectHash,
                            const OnSubjectBehaviorHash & onBeh,
                            SubjectBehaviorFilter filter,
                            Order inOrder) const
{
    uint32_t bitmap = getSubjectBitmap(subjectHash);

    int n = __builtin_popcount(bitmap);
    if (n == 0)
        return true;
    if (n == 1) {
        int bit = ML::highest_bit(bitmap, -1);
        if (bit != 31) {
            return behs[bit]
                ->forEachSubjectBehaviorHash(subjectHash, onBeh, filter,
                                              inOrder);
        }
    }

    auto bitSet = [&] (int i)
        {
            if (i > 31) i = 31;
            return bitmap & ((uint32_t)1 << i);
        };
    
    if (inOrder == INORDER) {

        SubjectInfo allBehs;

        for (unsigned i = 0;  i < behs.size();  ++i) {
            if (!bitSet(i)) continue;

            auto onBeh2 = [&] (BH beh, Date ts, uint32_t count)
                {
                    allBehs.record(beh, ts, count);
                    return true;
                };
            
            // Any order since we're sorting and aggregating after
            behs[i]->forEachSubjectBehaviorHash(subjectHash, onBeh2,
                                                 filter, ANYORDER);
        }
        
        // Sort and aggregate them
        allBehs.sort();
        
        // Now do them in order
        for (auto it = allBehs.behaviors.begin(),
                 end = allBehs.behaviors.end();
             it != end;  ++it) {
            BH beh(it->behavior);
            if (filter.pass(beh, it->timestamp, it->count))
                if (!onBeh(beh, it->timestamp, it->count))
                    return false;
        }
        return true;
    }
    else {
        // not in order; do it one source at a time and simply go through
        // them
        for (unsigned i = 0;  i < behs.size();  ++i) {
            if (!bitSet(i)) continue;
            if (!behs[i]->forEachSubjectBehaviorHash(subjectHash, onBeh,
                                                       filter, ANYORDER))
                return false;
        }
        return true;
    }
}

int
MergedBehaviorDomain::
numDistinctTimestamps(SH subjectHash, uint32_t maxValue) const
{
    //cerr << "numDistinctTimestamps for " << subjectHash
    //     << " with maxValue " << maxValue << endl;

    uint32_t bitmap = getSubjectBitmap(subjectHash);

    int n = __builtin_popcount(bitmap);
    if (n == 0) return 0;

    //cerr << "n = " << n << " nonOverlappingInTime = " << nonOverlappingInTime
    //     << endl;

    auto bitSet = [&] (int i)
        {
            if (i > 31) i = 31;
            return bitmap & ((uint32_t)1 << i);
        };

#if 0 // debug
    for (unsigned i = 0;  i < behs.size();  ++i) {
        cerr << "file " << i
             << behs[i]->earliestTime()
             << "-"
             << behs[i]->latestTime()
             << " with bitSet "
             << bitSet(i)
             << " had "
             << behs[i]->numDistinctTimestamps(subjectHash, -1)
             << endl;

        auto onBehavior = [&] (BH beh, Date ts, uint32_t count)
            {
                cerr << beh << " " << ts << " " << count << endl;
                return true;
            };

        behs[i]->forEachSubjectBehaviorHash(subjectHash, onBehavior,
                                             SubjectBehaviorFilter(),
                                             ANYORDER);
    }    
#endif

    // If they are non-overlapping in time then there are at least as many
    // timestamps as there are files
    if (nonOverlappingInTime && n >= maxValue)
        return maxValue;

    if (n == 1) {
        int bit = ML::highest_bit(bitmap, -1);
        if (bit != 31)
            return behs[bit]->numDistinctTimestamps(subjectHash, maxValue);
    }
    
    if (n == 1 && maxValue == 1)
        return 1;

    int result = 0;

    if (nonOverlappingInTime) {
        for (unsigned i = 0;  i < behs.size() && result < maxValue;  ++i) {
            if (!bitSet(i)) continue;

            result += behs[i]->numDistinctTimestamps(subjectHash,
                                                     maxValue - result);
        }
    }
    else {
        set<Date> allTimestamps;
        
        for (unsigned i = 0;
             i < behs.size() && allTimestamps.size() < maxValue;
             ++i) {
            if (!bitSet(i)) continue;

            auto onBehavior = [&] (BH beh, Date ts, uint32_t count)
                {
                    allTimestamps.insert(ts);
                    return allTimestamps.size() < maxValue;
                };

            if (!behs[i]->forEachSubjectBehaviorHash(subjectHash, onBehavior,
                                                      SubjectBehaviorFilter(),
                                                      ANYORDER))
                break;
        }
        
        result = allTimestamps.size();
    }
    return result;
}

__thread bool debugMergedBehaviors = false;

int
MergedBehaviorDomain::
coIterateBehaviors(BH beh1, BH beh2, 
                    SH maxSubject,
                    const OnBehaviors & onBehaviors) const
{
    if (debugMergedBehaviors)
        cerr << "beh1 = " << beh1 << " beh2 = " << beh2 << endl;

    uint32_t bitmap1 = behIndex.getDefault(beh1, 0);
    uint32_t bitmap2 = behIndex.getDefault(beh2, 0);

    // Erroneous optimization... don't do
    // If there is no intersection then nothing to do
    //if (nonOverlappingInTime && (e1.bitmap & bitmap2) == 0)
    //    return 0;

    if (debugMergedBehaviors)
        cerr << "coiterate: bm1 = " << bitmap1 << " bm2 = " << bitmap2
             << endl;

    // If there is only one segment to co-iterate then do it
    // directly
    int nbits = __builtin_popcount(bitmap1 & bitmap2);

    if (debugMergedBehaviors)
        cerr << "nbits = " << nbits << endl;

    if (nbits == 1) {
        int bit = ML::highest_bit(bitmap1 & bitmap2, -1);
        if (bit != 31)
            return behs[bit]->coIterateBehaviors(beh1, beh2, maxSubject,
                                                  onBehaviors);
    }
    
    auto bitSet = [&] (uint32_t bitmap, int i)
        {
            if (i > 31) i = 31;
            return bitmap & ((uint32_t)1 << i);
        };

    //cerr << "debugMergedBehaviors nbits " << nbits << " onBehaviors = "
    //     << (bool)onBehaviors << endl;

    if (!onBehaviors) {

        Lightweight_Hash<uint64_t, uint32_t> allSubjects;

        for (unsigned i = 0;  i < behs.size();  ++i) {
            if (!bitSet(bitmap1, i)) continue;

            std::vector<SH> subjects
                = behs[i]->getSubjectHashes(beh1, maxSubject);

            if (debugMergedBehaviors)
                cerr << "beh1 " << i << " had " << subjects.size()
                     << " subjects" << endl;

            allSubjects.reserve(subjects.size() * 2);
            
            for (auto it = subjects.begin(), end = subjects.end();
                 it != end;  ++it)
                allSubjects[*it] = 1;
        }

        if (debugMergedBehaviors)
            cerr << "allSubjects.size() = " << allSubjects.size()
                 << endl;

        size_t result = 0;

        for (unsigned i = 0;  i < behs.size();  ++i) {
            if (!bitSet(bitmap2, i)) continue;

            std::vector<SH> subjects
                = behs[i]->getSubjectHashes(beh2, maxSubject);

            if (debugMergedBehaviors)
                cerr << "beh2 " << i << " had " << subjects.size()
                     << " subjects" << endl;
            
            for (auto it = subjects.begin(), end = subjects.end();
                 it != end;  ++it) {
                if (!allSubjects.count(*it)) continue;
                uint32_t & val = allSubjects[*it];
                if (val == 2) continue;
                ++result;
                val = 2;
            }
        }

        if (debugMergedBehaviors)
            cerr << " --> returned " << result << endl;

        return result;
    }

    Lightweight_Hash<uint64_t, uint32_t> allSubjects;

    for (unsigned b = 0;  b < 2;  ++b) {
        BH beh = (b == 0 ? beh1 : beh2);
        for (unsigned i = 0;  i < behs.size();  ++i) {
            std::vector<SH> subjects
                = behs[i]->getSubjectHashes(beh, maxSubject);

            allSubjects.reserve(subjects.size() * 2);

            for (auto it = subjects.begin(), end = subjects.end();
                 it != end;  ++it)
                allSubjects[*it] |= (1 << b);
        }
    }

    size_t result = 0;

    for (auto it = allSubjects.begin(), end = allSubjects.end();
         it != end;  ++it) {
        SH subj(it->first);
        int which(it->second);

        if (which != 3)
            continue;
        
        ++result;
        onBehaviors(subj);
    }

    return result;
}

bool
MergedBehaviorDomain::
subjectHasNDistinctBehaviors(SH subjectHash, int n) const
{
    //return getSubjectStats(subjectHash).numDistinctBehaviors >= n;

    uint32_t bitmap = getSubjectBitmap(subjectHash);

    int nin = __builtin_popcount(bitmap);
    if (nin == 0) return false;

    if (nin == 1) {
        int bit = ML::highest_bit(bitmap, -1);
        if (bit != 31)
            return behs[bit]->subjectHasNDistinctBehaviors(subjectHash, n);
    }
    
    auto bitSet = [&] (int i)
        {
            if (i > 31) i = 31;
            return bitmap & ((uint32_t)1 << i);
        };

    Lightweight_Hash_Set<uint64_t> behaviors;

    for (unsigned i = 0;  i < behs.size();  ++i) {
        if (!bitSet(i)) continue;
        if (behs[i]->subjectHasNDistinctBehaviors(subjectHash, n))
            return true;
        auto sbehs = behs[i]->getSubjectBehaviorCounts(subjectHash);
        for (auto it = sbehs.begin(), end = sbehs.end();
             it != end && behaviors.size() < n;  ++it)
            behaviors.insert(it->first);
        if (behaviors.size() >= n) return true;
    }

    return false;
}

BehaviorDomain::SubjectStats
MergedBehaviorDomain::
getSubjectStats(SH subjectHash,
                bool needDistinctBehaviors,
                bool needDistinctTimestamps) const
{
    uint32_t bitmap = getSubjectBitmap(subjectHash);

    int n = __builtin_popcount(bitmap);
    if (n == 0)
        return SubjectStats();
    if (n == 1) {
        int bit = ML::highest_bit(bitmap, -1);
        if (bit != 31)
            return behs[bit]->getSubjectStats(subjectHash);
    }

    auto bitSet = [&] (int i)
        {
            if (i > 31) i = 31;
            return bitmap & ((uint32_t)1 << i);
        };

    SubjectStats result;

    // Get the distinct timestamps
    if (needDistinctTimestamps) {
        set<Date> timestamps;

        for (unsigned i = 0;  i < behs.size();  ++i) {
            if (!bitSet(i)) continue;

            auto onBehavior = [&] (BH beh, Date ts, uint32_t count)
                {
                    timestamps.insert(ts);
                    return true;
                };

            behs[i]->forEachSubjectBehaviorHash(subjectHash, onBehavior,
                                                 SubjectBehaviorFilter(),
                                                 INORDER);

            result.numDistinctTimestamps = timestamps.size();
        }
    }
    else {
        result.numDistinctTimestamps = -1;
    }

    // Get the 1. distinct behaviors, 2. earliest timestamp,
    // 3. latest timestamp, and 4. number of recorded behaviors
    if (needDistinctBehaviors) {

        Lightweight_Hash_Set<uint64_t> hashes;

        for (unsigned i = 0;  i < behs.size();  ++i) {
            if (!bitSet(i)) continue;

            auto onBehavior = [&] (BH beh, Date ts, uint32_t count)
                {
                    result.earliest.setMin(ts);
                    result.latest.setMax(ts);
                    hashes.insert(beh);
                    ++result.numBehaviors;
                    return true;
                };

            behs[i]->forEachSubjectBehaviorHash(subjectHash, onBehavior,
                                                 SubjectBehaviorFilter(),
                                                 INORDER);
        }

        result.numDistinctBehaviors = hashes.size();
    }
    // Get only  the 1. earliest timestamp, 2. latest timestamp,
    // and 3. number of recorded behaviors
    else {
        result.numDistinctBehaviors = 0;

        for (unsigned i = 0;  i < behs.size();  ++i) {
            if (!bitSet(i)) continue;

            SubjectStats subInfo = behs[i]->getSubjectStats(subjectHash, false);
            result.earliest = std::min(result.earliest, subInfo.earliest);
            result.latest   = std::max(result.latest, subInfo.latest);
            result.numBehaviors += subInfo.numBehaviors;
        }
    }
    return result;
}

std::pair<Date, Date>
MergedBehaviorDomain::
getSubjectTimestampRange(SH subject) const
{
    uint32_t bitmap = getSubjectBitmap(subject);

    Date earliest = Date::positiveInfinity();
    Date latest = Date::positiveInfinity();

    int n = __builtin_popcount(bitmap);
    if (n == 0)
        return make_pair(earliest, latest);

    auto bitSet = [&] (int i)
        {
            if (i > 31) i = 31;
            return bitmap & ((uint32_t)1 << i);
        };

    for (unsigned i = 0;  i < behs.size();  ++i) {
        if (!bitSet(i)) continue;

        auto res = behs[i]->getSubjectTimestampRange(subject);
        earliest.setMin(res.first);
        latest.setMin(res.second);
    }

    return make_pair(earliest, latest);
}

std::vector<SH>
MergedBehaviorDomain::
getSubjectHashes(BH hash, SH maxSubject, bool sorted) const
{
    Lightweight_Hash_Set<SH> allSubjects;

    for (unsigned i = 0;  i < behs.size();  ++i) {
        std::vector<SH> subjects = behs[i]->getSubjectHashes(hash, maxSubject);
        for (auto it = subjects.begin(), end = subjects.end();
             it != end;  ++it)
            allSubjects.insert(*it);
    }

    vector<SH> result;
    result.reserve(allSubjects.size());
    for (auto it = allSubjects.begin(), end = allSubjects.end();
         it != end;  ++it)
        result.push_back(SH(*it));
    if (sorted)
        std::sort(result.begin(), result.end());
    return result;
}

typedef std::vector<std::pair<SH, Date> > TsVector;

TsVector mergeSubjects(TsVector & v1, TsVector & v2)
{
    TsVector result;

    int i1 = 0, e1 = v1.size(), i2 = 0, e2 = v2.size();

    while (i1 < e1 && i2 < e2) {
        SH s1 = v1[i1].first, s2 = v2[i2].first;
        Date d1 = v1[i1].second, d2 = v2[i2].second;
        if (s1 == s2) {
            result.push_back(make_pair(s1, std::min(d1, d2)));
            ++i1;
            ++i2;
        }
        else if (s1 < s2) {
            result.push_back(make_pair(s1, d1));
            ++i1;
        }
        else {
            result.push_back(make_pair(s2, d2));
            ++i2;
        }
    }

    result.insert(result.end(), v1.begin() + i1, v1.end());
    result.insert(result.end(), v2.begin() + i2, v2.end());

    return result;
}
        

std::vector<std::pair<SH, Date> >
MergedBehaviorDomain::
getSubjectHashesAndTimestamps(BH beh, SH maxSubject, bool sorted) const
{
    if (behs.empty())
        return std::vector<std::pair<SH, Date> >();
    //vector<vector<pair<SH, Date> > > allMerged;

    std::vector<pair<SH, Date> > subjects0
        = behs[0]->getSubjectHashesAndTimestamps(beh, maxSubject, true);
    float density = 1.0 * subjects0.size() / behs[0]->subjectCount();

    //cerr << "density = " << density << endl;

    if (density > 0.01 || true) {
        // Heuristic version with a fully dense output

        std::function<TsVector (int start, int end)> mergeRanges
            = [&] (int start, int end) -> TsVector
            {
                //cerr << "merging from " << start << " to " << end << endl;
                if (end - start == 1)
                    return behs[start]->getSubjectHashesAndTimestamps(beh, maxSubject, true);
                else if (end == start)
                    throw MLDB::Exception("end == start");
                else {
                    int mid = start + (end - start) / 2;
                    auto first = mergeRanges(start, mid);
                    auto second = mergeRanges(mid, end);
                    return mergeSubjects(first, second);
                }
            };

        return mergeRanges(0, behs.size());
    }

    Date start = Date::now();

    Lightweight_Hash<uint64_t, Date> allSubjects;

    bool printed = false;

    for (unsigned i = 0;  i < behs.size();  ++i) {
        std::vector<pair<SH, Date> > subjects
            = behs[i]->getSubjectHashesAndTimestamps(beh, maxSubject, false);
        for (auto it = subjects.begin(), end = subjects.end();
             it != end;  ++it) {
            auto jt = allSubjects.insert(*it).first;
            jt->second = std::min(jt->second, it->second);
        }
        if (!printed && Date::now().secondsSince(start) > 1.0) {
            printed = true;
        }
        if (printed)
            cerr << "  behavior " << getBehaviorId(beh) << " at iter "
                 << i << " of " << behs.size() << " in "
                 << Date::now().secondsSince(start) << "s with "
                 << allSubjects.size() << " total and "
                 << subjects.size() << " current subjects "
                 << "density = "
                 << 100.0 * subjects.size() / behs[i]->subjectCount()
                 << endl;
    }

    if (Date::now().secondsSince(start) > 1.0) {
        cerr << "finished " << getBehaviorId(beh) << " in "
             << Date::now().secondsSince(start) << "s" << endl;
    }

    vector<pair<SH, Date> > result;
    result.reserve(allSubjects.size());
    for (auto it = allSubjects.begin(), end = allSubjects.end();
         it != end;  ++it)
        result.push_back(make_pair(SH(it->first), it->second));
    if (sorted)
        std::sort(result.begin(), result.end());
    return result;
}

std::vector<std::pair<SH, Date> >
MergedBehaviorDomain::
getSubjectHashesAndAllTimestamps(BH beh, SH maxSubject,
                                 bool sorted) const
{
    return BehaviorDomain
        ::getSubjectHashesAndAllTimestamps(beh, maxSubject, sorted);
}

bool
MergedBehaviorDomain::
forEachBehaviorSubject(BH beh,
                        const OnBehaviorSubject & onSubject,
                        bool withTimestamps,
                        Order order,
                        SH maxSubject) const
{
    if (withTimestamps) {
        Lightweight_Hash<SH, Date> allSubjects;

        for (unsigned i = 0;  i < behs.size();  ++i) {
            std::vector<pair<SH, Date> > subjects
                = behs[i]->getSubjectHashesAndTimestamps(beh, maxSubject,
                                                         false /* sorted */);
            for (auto it = subjects.begin(), end = subjects.end();
                 it != end;  ++it) {
                SH subject = it->first;
                if (subject > maxSubject) continue;
                auto jt = allSubjects.insert(*it).first;
                jt->second.setMin(it->second);
            }
        }

        if (order == INORDER) {
            vector<pair<SH, Date> > result
                (allSubjects.begin(), allSubjects.end());
            std::sort(result.begin(), result.end());

            for (auto s: result)
                if (!onSubject(s.first, s.second))
                    return false;
            return true;
        }
        else {
            for (auto s: allSubjects)
                if (!onSubject(s.first, s.second))
                    return false;
            return true;
        }
    }
    else {
        Lightweight_Hash_Set<SH> allSubjects;

        for (unsigned i = 0;  i < behs.size();  ++i) {
            std::vector<SH> subjects
                = behs[i]->getSubjectHashes(beh, maxSubject, false /* sorted */);
            allSubjects.insert(subjects.begin(), subjects.end());
        }

        if (order == INORDER) {
            vector<SH> result(allSubjects.begin(), allSubjects.end());
            std::sort(result.begin(), result.end());

            for (auto s: result)
                if (!onSubject(s, Date()))
                    return false;
            return true;
        }
        else {
            for (auto s: allSubjects)
                if (!onSubject(s, Date()))
                    return false;
            return true;
        }
    }
}

Id
MergedBehaviorDomain::
getSubjectId(SH subjectHash) const
{
    if (!hasSubjectIds)
        throw MLDB::Exception("attempt to get Subject ID where none present");

    uint32_t bitmap = getSubjectBitmap(subjectHash);

    int n = __builtin_popcount(bitmap);

    if (n == 0)
        throw MLDB::Exception("attempt to get Subject ID for unknown subject");

    int bit = ML::lowest_bit(bitmap, -1);

    if (bit == 31) {
        for (unsigned i = 31;  i < behs.size();  ++i) {
            if (behs[i]->knownSubject(subjectHash))
                return behs[i]->getSubjectId(subjectHash);
        }
        throw MLDB::Exception("logic error in getSubjectId");
    }

    return behs[bit]->getSubjectId(subjectHash);
}

Id
MergedBehaviorDomain::
getBehaviorId(BH beh) const
{
    uint32_t bitmap = behIndex.getDefault(beh, 0);

    int n = __builtin_popcount(bitmap);

    if (n == 0)
        return Id();

    int bit = ML::lowest_bit(bitmap, -1);

    if (bit == 31) {
        for (unsigned i = 31;  i < behs.size();  ++i) {
            if (behs[i]->knownBehavior(beh))
                return behs[i]->getBehaviorId(beh);
        }
        throw MLDB::Exception("logic error in getBehaviorId");
    }
    
    return behs[bit]->getBehaviorId(beh);
}

BehaviorDomain::BehaviorStats
MergedBehaviorDomain::
getBehaviorStats(BH beh, int fields) const
{
    BehaviorStats result;
    
    // TODO: use bitmap
    //uint32_t bitmap = behIndex.getDefault(beh, 0);

    for (unsigned i = 0;  i < behs.size();  ++i) {
        BehaviorStats stats
            = behs[i]->getBehaviorStats(beh, BS_TIMES);
        result.earliest.setMin(stats.earliest);
        result.latest.setMax(stats.latest);
    }

    if (fields & BS_SUBJECT_COUNT)
        result.subjectCount = getSubjectHashes(beh).size();
    else result.subjectCount = -1;

    if (fields & BS_ID)
        result.id = getBehaviorId(beh);

    return result;
}

size_t
MergedBehaviorDomain::
getApproximateBehaviorSubjectCountImpl(BH beh) const
{
    uint32_t bitmap = behIndex.getDefault(beh, 0);

    int n = __builtin_popcount(bitmap);
    if (n == 0)
        return 0;
    if (n == 1) {
        int bit = ML::highest_bit(bitmap, -1);
        if (bit != 31)
            return behs[bit]->getBehaviorSubjectCount(beh);
    }

    auto bitSet = [&] (int i)
        {
            if (i > 31) i = 31;
            return bitmap & ((uint32_t)1 << i);
        };

    distribution<uint64_t> subjectCounts;
        
    for (unsigned i = 0;  i < behs.size();  ++i) {
        if (!bitSet(i)) continue;
        subjectCounts.push_back
            (behs[i]->getBehaviorSubjectCount(beh));
    }

    if (subjectCounts.empty()) return 0;
    if (subjectCounts.size() == 1) return subjectCounts[0];

    return (subjectCounts.two_norm() + subjectCounts.max()) * 2 / 3;
}

size_t
MergedBehaviorDomain::
getExactBehaviorSubjectCountImpl(BH beh, SH maxSubject, Precision p) const
{
    uint32_t bitmap = behIndex.getDefault(beh, 0);

    int n = __builtin_popcount(bitmap);
    if (n == 0)
        return 0;
    if (n == 1) {
        int bit = ML::highest_bit(bitmap, -1);
        if (bit != 31)
            return behs[bit]->getBehaviorSubjectCount(beh, maxSubject, p);
    }

    auto bitSet = [&] (int i)
        {
            if (i > 31) i = 31;
            return bitmap & ((uint32_t)1 << i);
        };

    Lightweight_Hash_Set<uint64_t> allSubjects;

    for (unsigned i = 0;  i < behs.size();  ++i) {
        if (!bitSet(i)) continue;
        std::vector<SH> subjects = behs[i]->getSubjectHashes(beh,
                                                             maxSubject);
        for (auto it = subjects.begin(), end = subjects.end();
             it != end;  ++it)
            allSubjects.insert(*it);
    }

    return allSubjects.size();
}

size_t
MergedBehaviorDomain::
getBehaviorSubjectCount(BH beh, SH maxSubject, Precision p) const
{
    if (p == EXACT || !maxSubject.isMax())
        return getExactBehaviorSubjectCountImpl(beh, maxSubject, p);
    return getApproximateBehaviorSubjectCountImpl(beh);
}

std::vector<std::pair<BH, uint32_t> >
MergedBehaviorDomain::
getSubjectBehaviorCounts(SH subjectHash, Order order) const
{
    Lightweight_Hash<uint64_t, uint32_t> counts;

    for (unsigned i = 0;  i < behs.size();  ++i) {
        auto mybehs = behs[i]->getSubjectBehaviorCounts(subjectHash);
        for (auto it = mybehs.begin(), end = mybehs.end();
             it != end;  ++it)
            counts[it->first] += it->second;
    }

    std::vector<std::pair<BH, uint32_t> > result;
    for (auto it = counts.begin(), end = counts.end();  it != end;  ++it)
        result.push_back(make_pair(BH(it->first), it->second));
    
    if (order == INORDER)
        std::sort(result.begin(), result.end());
    
    return result;
}

uint32_t
MergedBehaviorDomain::
getSubjectBitmap(SH subjectHash) const
{
    return subjectIndex.getDefault(subjectHash, 0);
}

bool
MergedBehaviorDomain::
fileMetadataExists(const std::string & key) const
{
    return fileMetadata_.isMember(key);
}

Json::Value
MergedBehaviorDomain::
getFileMetadata(const std::string & key) const
{
    return fileMetadata_[key];
}

Json::Value
MergedBehaviorDomain::
getAllFileMetadata() const
{
    return fileMetadata_;
}


} // namespace MLDB
