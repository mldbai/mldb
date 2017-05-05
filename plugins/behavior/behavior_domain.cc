// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* behavior_domain.cc
   Jeremy Barnes, 4 April 2012
   Copyright (c) 2012 mldb.ai inc.

*/

#include "behavior_domain.h"
#include "mldb/jml/utils/vector_utils.h"

#include "behavior_utils.h"
#include "mldb/arch/bitops.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/arch/timers.h"
#include "mldb/jml/utils/pair_utils.h"
#include <unordered_map>
#include "mldb/base/exc_assert.h"
#include "mapped_value.h"
#include "mapped_behavior_domain.h"
#include "mldb/jml/db/persistent.h"
#include "mldb/base/parallel.h"
#include <boost/thread/mutex.hpp>
#include <boost/shared_array.hpp>
#include "mldb/jml/utils/map_reduce.h"
#include "mldb/types/jml_serialization.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/base/scope.h"
#include "id_serialization.h"

//#include <gperftools/tcmalloc.h>
#include <atomic>


using namespace std;
using namespace ML;


namespace MLDB {


/*****************************************************************************/
/* SUBJECT FILTER                                                            */
/*****************************************************************************/

SubjectFilter::
SubjectFilter()
    : minDistinctBehaviors(-1), minSubject(0), maxSubject(SH::max()),
      hasFilterList(false)
{
}
    
bool
SubjectFilter::
fail(SH subject) const
{
    if (minDistinctBehaviors != -1) {
        throw MLDB::Exception("minDistinctBehaviors filtering not done");
    }

    if (subject <= minSubject)
        return true;
    if (subject > maxSubject)
        return true;

    if (hasFilterList && !inList.count(subject))
        return true;

    return false;
}

bool
SubjectFilter::
pass(SH subject, const SubjectStats & stats) const
{
    if (fail(subject))
        return false;

    if (includeRegex) {
        ExcAssert(stats.id.notNull());
        if (!boost::regex_match(stats.id.toString(), *includeRegex))
            return false;
    }

    return true;
}

bool
SubjectFilter::
passAll() const
{
    return minDistinctBehaviors == -1
        && minSubject == SH(0)
        && maxSubject == SH::max()
        && !hasFilterList
        && !includeRegex;
}

void
SubjectFilter::
filterByRegex(const std::string & expression)
{
    includeRegex.reset(new boost::regex(expression));
    includeRegexStr = expression;
}

int
SubjectFilter::
requiredStatsFields() const
{
    int result = BehaviorDomain::SS_NONE;
    if (minDistinctBehaviors != -1)
        result = result | BehaviorDomain::SS_DISTINCT_BEHAVIORS;
    if (includeRegex.get())
        result = result | BehaviorDomain::SS_ID;
    return result;
}


/*****************************************************************************/
/* BEHAVIOR FILTER                                                          */
/*****************************************************************************/

BehaviorFilter::
BehaviorFilter()
    : hasFilterList(false), minSubjectCount(-1)
{
}

bool
BehaviorFilter::
fail(BH beh) const
{
    if (hasFilterList && !inList.count(beh))
        return true;
    return false;
}

bool
BehaviorFilter::
pass(BH beh, const BehaviorStats & stats) const
{
    if (fail(beh))
        return false;

    if (minSubjectCount != -1) {
        ExcAssertGreaterEqual((int)stats.subjectCount, 0);
        if (stats.subjectCount < minSubjectCount)
            return false;
    }
    
    if (includeRegex) {
        ExcAssert(stats.id.notNull());
        if (!boost::regex_match(stats.id.toString(), *includeRegex))
            return false;
    }
    
    return true;
}

bool
BehaviorFilter::
passAll() const
{
    return !hasFilterList && !includeRegex;
}

void
BehaviorFilter::
filterByRegex(const std::string & expression)
{
    includeRegex.reset(new boost::regex(expression));
    includeRegexStr = expression;
}

void
BehaviorFilter::
filterByMinSubjectCount(int minCount)
{
    this->minSubjectCount = minCount;
}

int
BehaviorFilter::
requiredStatsFields() const
{
    int result = BehaviorDomain::BS_NONE;
    if (includeRegex.get())
        result = result | BehaviorDomain::BS_ID;
    if (minSubjectCount != -1)
        result = result | BehaviorDomain::BS_SUBJECT_COUNT;
    return result;
}


/*****************************************************************************/
/* BEHAVIOR DOMAIN                                                          */
/*****************************************************************************/

uint64_t
BehaviorDomain::
magicStr(const std::string & s)
{
    uint64_t res = 0;
    for (unsigned i = 0;  i < 8 && i < s.length();  ++i) {
        unsigned char c = s[i];
        res = res << 8 | c;
    }
    return res;
}


void
BehaviorDomain::
stats(std::ostream & stream) const
{
    stream << "total subjects: " << subjectCount() << endl;
    stream << "total behaviors: " << behaviorCount()
         << endl;
    stream << "total events recorded: " << totalEventsRecorded() << endl;

    stream << "earliest timestamp: " << earliestTime() << endl;
    stream << "latest timestamp: " << latestTime() << endl;
    
    //stream << behaviorCountDistribution() << endl;
    //stream << subjectCountDistribution() << endl;

#if 0
    // 1.  Sort behaviors
    std::vector<pair<uint64_t, BehaviorInfo> > sorted
        (info.begin(), info.end());
    std::sort(sorted.begin(), sorted.end(),
              [] (const pair<uint64_t, BehaviorInfo> & p1,
                  const pair<uint64_t, BehaviorInfo> & p2)
              -> bool
              {
                  return p1.second.count > p2.second.count;
              });
        
    uint64_t totalCount = 0;
    for (unsigned i = 0;  i < sorted.size();  ++i)
        totalCount += sorted[i].second.count;

    stream << "total count " << totalCount << endl;
    stream << "median count " << sorted[sorted.size() / 2].second.count
         << endl;
    stream << "mean count " << 1.0 * totalCount / sorted.size() << endl;
    stream << "max count " << sorted[0].second.count << endl;
    for (unsigned i = 0;  i < 10;  ++i) {
        stream << i << "0th percentile "
             << sorted[sorted.size() * i / 10].second.count
             << endl;
    }

    for (unsigned i = 0;  i < 100;  ++i) {
        const BehaviorInfo & info
            = this->info.find(sorted[i].first)->second;
        stream << i << ": " << sorted[i].second.index << " "
             << info.count << " "
             << info.name << endl;
    }
#endif
}

void
BehaviorDomain::
save(const std::string & filename,
     ssize_t maxSubjectBehaviors)
{
    cerr << "writing to " << filename << endl;
    //stats();

    /* Any exception thrown in saveToStream will cause the stream to be closed
       and, most likely, commited from filter_ostream::~filter_ostream, no
       matter the state of the data inside the file. We thus use RAII to
       ensure that the commiting of those incomplete files is reverted
       post-destruction. */
    bool good(false);
    auto onExit = ScopeExit([&] () noexcept { if (!good) { cerr << "bad" << endl;  tryEraseUriObject(filename); }});
    filter_ostream stream(filename);
    saveToStream(stream, maxSubjectBehaviors);
    stream.close();
    cerr << "finished writing" << endl;
    good = true;
}

void
BehaviorDomain::
saveToStream(std::ostream & stream,
             ssize_t maxSubjectBehaviors)
{
    DB::Store_Writer store(stream);
    uint64_t mdOffset = serialize(store, maxSubjectBehaviors);
    store.save_binary(mdOffset);
}

template<typename T1, typename T2>
void checkedSet(T1 & val, T2 setTo)
{
    val = setTo;
    T2 newVal = val;
    if (newVal != setTo) {
        cerr << "val = " << val << endl;
        cerr << "newVal = " << newVal << endl;
        cerr << "setTo = " << setTo << endl;
        throw MLDB::Exception("checkedSet: values differ");
    }
}

uint64_t
BehaviorDomain::
serialize(DB::Store_Writer & store, ssize_t maxSubjectBehaviors)
{
    Timer t;
    bool debug = false;

    finish();

    if (debug) {
        cerr << "finished finish() " << t.elapsed() << endl;
        t.restart();
    }

    //cerr << "serializing with " << minSubjects << " min subjects and "
    //     << timeQuantum << "s time quantum" << endl;

    struct BehaviorInfo : public BehaviorStats {
        BehaviorInfo()
            : index(-1)
        {
        }

        int index;             // Index of behavior
    };

    // Get all behaviors and save those that have a sufficient
    // number of entries.  Goal is to allow a) a behavior number to
    // turn into a behavior ID, and b) a behavior ID to be turned
    // into a behavior number.

    // All behaviors.  These should start at zero and increase up to
    // the total number of behaviors as we have remapped in recording
    // them.
    std::vector<BH> allBehaviors = allBehaviorHashes();

    // Shouldn't be necessary...
    std::sort(allBehaviors.begin(), allBehaviors.end());

    if (debug) {
        cerr << "finished sorting behaviors at " << t.elapsed()
             << endl;
        t.restart();
    }

    // Put them in decreasing order of subject count, and then ordered by first
    // subject
    std::vector<std::tuple<BH, int, SH> > subjectCounts(allBehaviors.size());

#if 0 // experimental key value code
    std::mutex keyMutex;
    std::unordered_map<BH, std::pair<Id, Id> > behToKeyValue;
    std::unordered_map<Id, int> keySubjects;
    std::unordered_map<Id, std::unordered_map<Id, int> > valueSubjects;
#endif

    auto onBehavior = [&] (int i)
        {
            BH hash = allBehaviors[i];
            int sc = this->getBehaviorSubjectCount(hash);
            SH first;
            
            auto onSubj = [&] (SH subj, Date ts)
            {
                first = subj;
                return false;  /* bail */
            };

            forEachBehaviorSubject(hash, onSubj);

            subjectCounts[i] = make_tuple(hash, sc, first);

#if 0  // experimental key/value compression code
            Id id = this->getBehaviorId(hash);
            std::string str = id.toString();
            auto equalPos = str.find(':', 3);
            //if (equalPos == string::npos) {
            //    equalPos = str.rfind(':');
            //}

            if (equalPos != string::npos) {
                Id key(string(str, 0, equalPos));
                Id value(string(str, equalPos + 1));

                std::unique_lock<std::mutex> guard(keyMutex);
                //cerr << "key " << key << " value " << value << endl;

                behToKeyValue[key] = { key, value };
                keySubjects[key] += sc;
                valueSubjects[key][value] = sc;
            }
#endif
        };

    parallelMap(0, allBehaviors.size(), onBehavior);

#if 0  // experimental key value code
    int keyValueBits = 0;

    for (auto & k: keySubjects) {
        cerr << "key " << k.first << " has " << k.second << " subjects of "
             << subjectCount() << " and "
             << valueSubjects[k.first].size() << " values" << endl;
        cerr << "average of " << 1.0 * k.second / subjectCount() << " values per subject"
             << endl;

        int bits = ML::highest_bit(valueSubjects[k.first].size() + 1, -1) + 1;
        keyValueBits += bits;
        
        cerr << "can be stored in " << bits << " bits" << endl;
    }

    cerr << "total of " << keyValueBits << " k/v bits per subject which is "
         << keyValueBits + 7 / 8 << " bytes" << " for total of "
         << (keyValueBits * 7 / 8) * subjectCount() / 1000000.0 << " MB" << endl;
#endif

    // Remove those with a count that is too low
    subjectCounts.erase(std::partition(subjectCounts.begin(),
                                       subjectCounts.end(),
                                       [=] (tuple<BH, int, SH> p)
                                       { return std::get<1>(p) >= minSubjects; }),
                        subjectCounts.end());
    
    // Sort the rest
    std::sort(subjectCounts.begin(), subjectCounts.end(),
              [] (const tuple<BH, int, SH> & first,
                  const tuple<BH, int, SH> & second)
              {
                  if (std::get<1>(first) > std::get<1>(second))
                      return true;
                  if (std::get<1>(first) < std::get<1>(second))
                      return false;
                  return std::get<2>(first) < std::get<2>(second);
              });

    // Get statistics on subject counts
    size_t totalSubjectCount = 0, maxSubjectCount = 0;
    if (!subjectCounts.empty())
        maxSubjectCount = std::get<1>(subjectCounts[0]);
    for (auto c: subjectCounts)
        totalSubjectCount += std::get<1>(c);

    if (debug) {
        cerr << "kept " << subjectCounts.size() << " of "
             << allBehaviors.size() << " behaviors" << endl;
        cerr << "finished sorting behaviors by subject counts at "
             << t.elapsed() << endl;
        cerr << "Max subject count = " << maxSubjectCount << endl;
        cerr << "Average subject count = " << 1.0 * totalSubjectCount / subjectCounts.size()
             << endl;

        if (!subjectCounts.empty())
            cerr << "max subject count = " << std::get<1>(subjectCounts[0]) << endl;
        t.restart();
    }
    
    int nKept = subjectCounts.size();
    
    //cerr << "kept " << nKept << " of " << allBehaviors.size()
    //     << " behaviors" << endl;

    // Now create a mapping from behavior hash to integer index
    std::unordered_map<BH, BehaviorInfo> allInfo;
    Lightweight_Hash<BH, unsigned> behToIndex;
    std::vector<BH> behByIndex;
    std::vector<KVEntry<BH, uint32_t> >
        behaviorToIndex(nKept);

    for (unsigned i = 0;  i < nKept;  ++i) {
        BH beh;
        int sc;
        SH first;

        std::tie(beh, sc, first) = subjectCounts[i];

        auto stats = getBehaviorStats(beh, BS_ALL & ~BS_SUBJECT_COUNT);

        BehaviorInfo & info = allInfo[beh];
        info.subjectCount = sc;
        info.index = i;
        info.earliest = stats.earliest;
        info.latest = stats.latest;
        info.id = stats.id;
        //std::tie(info.earliest, info.latest)
        //    = getBehaviorTimeRange(beh);

        behaviorToIndex[i].key = beh;
        behaviorToIndex[i].value = i;
        behByIndex.push_back(beh);
        behToIndex[beh] = i;
    }

    if (debug) {
        cerr << "finished behavior index at " << t.elapsed() << endl;
        t.restart();
    }

    // In-order list of behaviors so that we can go from a behavior
    // to a behavior index
    uint64_t behaviorIndexOffset = store.offset();
    std::sort(behaviorToIndex.begin(), behaviorToIndex.end());
    for (unsigned i = 0;  i < nKept;  ++i)
        store.save_binary(behaviorToIndex[i]);

    if (debug) {
        cerr << "finished writing behavior index at " << t.elapsed() << endl;
        t.restart();
    }

    // Ordered list of behavior IDs to allow behavior name to be looked
    // up
    uint64_t behaviorIdOffset = store.offset();

    // In-order list of IDs
    vector<uint32_t> idOffsets(nKept, 0);
    for (unsigned i = 0;  i < nKept;  ++i) {
        checkedSet(idOffsets[i], store.offset() - behaviorIdOffset);
        BH beh = behByIndex[i];
        store << allInfo[beh].id;
    }

    if (debug) {
        cerr << "finished writing behavior IDs at  " << t.elapsed() << endl;
        t.restart();
    }

    // List of offsets to find those IDs (which may be variable length)
    uint64_t behaviorIdIndexOffset = store.offset();
    for (unsigned i = 0;  i < nKept;  ++i)
        store.save_binary(idOffsets[i]);


    if (debug) {
        cerr << "finished behavior ID index offsets at "
             << t.elapsed() << endl;
        t.restart();
    }

    // List of behaviors
    uint64_t behaviorInfoOffset = store.offset();
    for (unsigned i = 0;  i < nKept;  ++i) {
        BH beh = behByIndex[i];
        const BehaviorInfo & info = allInfo[beh];

        MappedBehaviorDomain::BehaviorStatsFormat formatted;
        formatted.hash = beh;
        formatted.unused = formatted.unused2 = 0;
        formatted.subjectCount = info.subjectCount;
        formatted.earliest = info.earliest;
        formatted.latest = info.latest;

        store.save_binary<MappedBehaviorDomain::BehaviorStatsFormat>(formatted);
    }

    if (debug) {
        cerr << "finished behavior index at " << t.elapsed() << endl;
        t.restart();
    }

    // Align on a 4 byte boundary
    while (store.offset() % 4 != 0)
        store << '\0';
        
    uint64_t subjectDataOffset = store.offset();

    // Get all subjects in order
    std::vector<SH> allSubjects = allSubjectHashes();
    std::sort(allSubjects.begin(), allSubjects.end());


    if (debug) {
        cerr << "finished getting all subjects at " << t.elapsed() << endl;
        t.restart();
    }

    std::vector<KVEntry<SH, MappedBehaviorDomain::SubjectIndexEntry> > index;
    index.reserve(allSubjects.size());

    //cerr << "writing " << allSubjects.size() << " subjects"
    //     << endl;
    std::atomic<uint64_t> numWithMultipleBehaviors(0);
    std::atomic<uint64_t> maxNumBehaviors(0);

    auto quantizeTime = [&] (Date tm)
        {
            tm.quantize(timeQuantum);
            return (uint64_t)(tm.secondsSinceEpoch() / timeQuantum);
        };

    uint64_t fileEarliest = quantizeTime(earliestTime());

    Lightweight_Hash<SH, int64_t> subjectToIndex;

    struct SubjectToWrite {
        SubjectToWrite(uint32_t * buf = 0, uint32_t numWords = 0,
                       const MappedBehaviorDomain::SubjectIndexEntry & e
                       = MappedBehaviorDomain::SubjectIndexEntry())
            : buf(buf), numWords(numWords), e(e)
        {
        }

        uint32_t * buf;
        uint32_t numWords;
        MappedBehaviorDomain::SubjectIndexEntry e;
    };

    std::atomic<uint64_t> totalEventsRecorded(0);

    // Create the binary format for the subject and queue it up to be
    // written.  Happens in parallel as it's processor intensive.

    auto serializeSubject = [&] (int i) -> SubjectToWrite
        {
            SH subj = allSubjects[i];

            try {

                uint64_t subjectEarliest
                    = quantizeTime(getSubjectTimestampRange(subj).first);
            
                std::map<std::pair<uint64_t, int>, int> entryCounts; // (time, beh) -> count
                Lightweight_Hash_Set<uint64_t> timestamps;

                std::map<int, int> behCounts;
                int maxBeh = 0, maxCount = 0, maxEntryCount = 0;
                uint64_t maxTimeOffset = 0;

                int n = 0;
                auto onBehavior = [&] (BH beh, Date timestamp, uint64_t count)
                    {
                        auto it2 = behToIndex.find(beh);
                        if (it2 == behToIndex.end()) return true;
                        int index = it2->second;

                        ++n;
                        if (maxSubjectBehaviors != -1
                            && n > maxSubjectBehaviors)
                            return false;  // truncating
                        
                        int & bucket = behCounts[index];
                        maxBeh = std::max<int>(maxBeh, index);
                        bucket += count;
                        maxCount = std::max(bucket, maxCount);

                        // Figure out the time offset
                        uint64_t ts = quantizeTime(timestamp);

                        if (ts < subjectEarliest) {
                            cerr << "earliest out by " << (subjectEarliest - ts)
                                 << " seconds" << endl;
                            //cerr << "firstSeen: " << sstats.earliest << endl;
                            cerr << "timestamp: " << timestamp << endl;
                            ts = subjectEarliest;
                        }
                        ExcAssertGreaterEqual(ts, subjectEarliest);
                        uint64_t tofs = ts - subjectEarliest;

                        timestamps.insert(tofs + 1);


                        int & bucket2 = entryCounts[make_pair(tofs, index)];
                        bucket2 += count;
                        maxEntryCount = std::max(bucket2, maxEntryCount);
                        maxTimeOffset = std::max<uint64_t>(tofs, maxTimeOffset);

                        return true;
                    };

                // Go through each subject behavior, recording it
                if (!this->forEachSubjectBehaviorHash(subj, onBehavior,
                                                       EventFilter(),
                                                       ANYORDER)) {
                    SubjectStats sstats = this->getSubjectStats(subj, false);
                    cerr << "truncating very heavy user " << subj << " with "
                         << sstats.numBehaviors << " behaviors" << endl;
                }
            
                if (behCounts.empty()) {
                    return SubjectToWrite();
                }

                if (subjectEarliest < fileEarliest) {
                    cerr << "ts range = " << getSubjectTimestampRange(subj).first << endl;
                    cerr << "subjectEarliest = " << subjectEarliest << endl;
                    cerr << "uq subj time = " << unQuantizeTime(subjectEarliest) << endl;
                    cerr << endl;
                    cerr << "file time = " << earliestTime() << endl;
                    cerr << "fileEarliest = " << fileEarliest << endl;
                    cerr << "uq file time = " << unQuantizeTime(fileEarliest) << endl;
                    cerr << endl;
                }

                ExcAssertGreaterEqual(subjectEarliest, fileEarliest);

#if 0
                cerr << "behCounts.size() = " << behCounts.size() << endl;
                cerr << "entryCounts.size() = " << entryCounts.size() << endl;
                cerr << "maxBeh = " << maxBeh << endl;
                cerr << "maxCount = " << maxCount << endl;
                cerr << "maxEntryCount = " << maxEntryCount << endl;
                cerr << "maxTimeOffset = " << maxTimeOffset << endl;
#endif



                // Now an index of behaviorIndex to position in the behCounts array
                std::map<int, int> behIndexToBehCountsIndex;
                int j = 0;
                int numBehBits = ML::highest_bit(maxBeh, -1) + 1;

                size_t tableBitsNotSplit = numBehBits * behCounts.size();
                int highestBehSoFar = 0;

                bool splitBehaviorTable = false;
                int behaviorTableSplitBitsBefore = -1;
                int behaviorTableSplitBitsAfter = -1;
                int behaviorTableSplitIndex = -1;
                int behaviorTableSplitValue = -1;
                size_t halfTotalBits = 0;

                size_t bestTotalBits = tableBitsNotSplit;

                for (auto it = behCounts.begin(), end = behCounts.end();
                     it != end;  ++it, ++j) {
                    behIndexToBehCountsIndex[it->first] = j;

                    // To convert an index to a split point, we do
                    // split point = index * behCounts.size() / 256

                    // So to find the index for our split point, we do
                    // index = split point / behCounts.size() * 256

                    // Now since rounding happens down on both counts, we need to add
                    // 255 to make it go up on the first, so that the rounding down
                    // on the second half is cancelled out

                    int64_t splitPoint = j;
                    int64_t splitValue = (splitPoint * 256 + 255) / behCounts.size();

                    int addressable = (splitValue * behCounts.size()) / 256;
                    
                    bool ok = (addressable == splitPoint);
                    
#if 0
                    cerr << "splitPoint " << splitPoint
                         << " splitValue " << splitValue
                         << " addressable " << addressable
                         << " ok " << ok
                         << endl;
#endif

                    // If we were to split here
                    int beforeBehBits = (ML::highest_bit(highestBehSoFar, -1) + 1)
                        * j;
                    int afterBehBits = (ML::highest_bit(maxBeh, -1) + 1)
                        * (behCounts.size() - j);

                    int totalBits = beforeBehBits + afterBehBits /* for split info, etc */;

                    if (ok && totalBits < bestTotalBits) {
                        splitBehaviorTable = true;
                        behaviorTableSplitBitsBefore = ML::highest_bit(highestBehSoFar, -1) + 1;
                        behaviorTableSplitBitsAfter = ML::highest_bit(maxBeh, -1) + 1;
                        behaviorTableSplitIndex = j;
                        behaviorTableSplitValue = splitValue;
                        bestTotalBits = totalBits;
                    }

                    if (j == behCounts.size() / 2) {
                        halfTotalBits = totalBits;
                    }

                    highestBehSoFar = it->first;
                }


                if (splitBehaviorTable && false) {
                    cerr << "split at " << behaviorTableSplitIndex << " of "
                         << behCounts.size() << " total "
                         << " total " << bestTotalBits
                         << " at " << 100.0 * behaviorTableSplitIndex / behCounts.size()
                         << "% (is " << 100.0 * bestTotalBits / tableBitsNotSplit
                         << "% of total)" << endl;

                    cerr << "half split is " << halfTotalBits
                         << " (is " << 100.0 * halfTotalBits / tableBitsNotSplit
                         << "% of total)" << endl;
                }

#if 0  // later... tests indicate up to 20% saving using no table and recording the index directly
                // Do we want a behavior table at all?  Count how many bits are
                // needed both with and without, ignoring timestamps

                // No table at all: we store the overall behavior number inline
                size_t noTableBitsTotal
                    = entryCounts.size() * numBehBits;

                // With a table: we store the table itself, plus the reduced
                // behavior number.

                int indexBits = ML::highest_bit(behCounts.size() - 1, -1) + 1;

                size_t withTableBitsTotal
                    = entryCounts.size() * indexBits + bestTotalBits;

                cerr << "noTableBitsTotal = " << noTableBitsTotal
                     << " withTableBitsTotal = " << withTableBitsTotal
                     << " (" << 100.0 * noTableBitsTotal / withTableBitsTotal
                     << " %)" << endl;
#endif // later
                
                //splitBehaviorTable = false;

                if (behCounts.size() > 1)
                    numWithMultipleBehaviors += 1;

                // Atomically set the maximum number of behaviors
                size_t knownMaxBehaviors = maxNumBehaviors;
                while (knownMaxBehaviors < behCounts.size()) {
                    if (maxNumBehaviors.compare_exchange_weak(knownMaxBehaviors, behCounts.size()))
                        break;
                }

                // List of distinct timestamps (plus one)
                vector<uint64_t> tsSorted(timestamps.begin(), timestamps.end());
                std::sort(tsSorted.begin(), tsSorted.end());

                int timestampTableTimestampBits
                    = ML::highest_bit(tsSorted.back() -1, -1) + 1;
                int eventTableTimestampBits
                    = ML::highest_bit(tsSorted.size(), -1) + 1;

                int timeOffsetBits = ML::highest_bit(maxTimeOffset, -1) + 1;

                uint64_t totalTimestampTableBits
                    = tsSorted.size() * timestampTableTimestampBits;
                uint64_t eventTableBitsSaved
                    = entryCounts.size()
                    * (timeOffsetBits - eventTableTimestampBits);
                int64_t timestampTableExtraBits
                    = totalTimestampTableBits - eventTableBitsSaved;

                //cerr << "timestamp table will add "
                //     << timestampTableExtraBits
                //     << " bits" << endl;
     
                bool hasTimestampTable = timestampTableExtraBits < 0;


                //cerr << "subject " << getSubjectId(subj)
                //     << " has " << behCounts.size()
                //     << " behaviors with maxBeh " << maxBeh
                //     << " and maxCount " << maxCount << endl;

                MappedBehaviorDomain::SubjectIndexEntry e;
                e.behBits = ML::highest_bit(maxBeh, -1) + 1;
                ExcAssertGreaterEqual(maxCount, 1);
                e.indexCountBits = ML::highest_bit(maxCount - 1, -1) + 1;
                ExcAssertGreaterEqual(maxEntryCount, 1);
                e.countBits = ML::highest_bit(maxEntryCount - 1, -1) + 1;
                e.timeBits = timeOffsetBits;
                e.numDistinctBehaviors = behCounts.size();
                e.numBehaviors = entryCounts.size();
                e.earliestTime = subjectEarliest - fileEarliest;
                e.hasBehaviorTable = true;
                e.hasTimestampTable = hasTimestampTable;
                e.numDistinctTimestamps = timestamps.size();

                if (splitBehaviorTable) {
                    e.behTableSplitValue = behaviorTableSplitValue;
                    e.splitBehaviorTable = splitBehaviorTable;
                    e.behBits = behaviorTableSplitBitsBefore;
                    e.behTable2Bits = behaviorTableSplitBitsAfter;
                }


                //ExcAssertEqual(e.numBehaviorTableBits(), bestTotalBits);

                int tableIndexBits = e.numTableIndexBits();
                
                totalEventsRecorded += entryCounts.size();

                int numWords = e.numWords();

                if (false /*numWords > 100 && false*/) {
                    cerr << "subj = " << subj << " numWords = " << numWords << endl;
                    cerr << "behCounts.size() = " << behCounts.size() << endl;
                    cerr << "entryCounts.size() = " << entryCounts.size() << endl;
                    cerr << "maxBeh = " << maxBeh << endl;
                    cerr << "maxCount = " << maxCount << endl;
                    cerr << "maxEntryCount = " << maxEntryCount << endl;
                    cerr << "maxTimeOffset = " << maxTimeOffset << endl;
                    cerr << "behBits = " << e.behBits << endl;
                    cerr << "indexCountBits = " << e.indexCountBits << endl;
                    cerr << "countBits = " << e.countBits << endl;
                    cerr << "timeBits = " << e.timeBits << endl;
                    cerr << "earliestTime = " << e.earliestTime << endl;
                }

#if 0
                cerr << "behBits = " << e.behBits << endl;
                cerr << "indexCountBits = " << e.indexCountBits << endl;
                cerr << "countBits = " << e.countBits << endl;
                cerr << "timeBits = " << e.timeBits << endl;
                cerr << "earliestTime = " << e.earliestTime << endl;

                //cerr << "countBits " << countBits << " behBits "
                //     << behBits << " numBits " << numBits << " numWords "
                //     << numWords << endl;
                cerr << "numWords = " << numWords << endl;
#endif


                uint32_t * buf = new uint32_t[numWords];
                std::fill(buf, buf + numWords, 0);
                int totalBits = e.numBits();
                int bitsDone = 0;

                Bit_Writer<uint32_t> writer(buf);

                auto writeBits = [&] (uint32_t val, int nBits, const char * what,
                                      int index, int total)
                    {
                        //cerr << "writing " << val << " in " << nBits << " bits" << endl;
                        if (bitsDone + nBits > totalBits)
                            throw MLDB::Exception("total bits done is wrong: adding %d "
                                                "to %d done of %d total in %s "
                                                "on item %d of %d",
                                                nBits, bitsDone, totalBits,
                                                what, index, total);
                        bitsDone += nBits;
                        writer.write(val, nBits);
                    };

                // Serialize the behavior table.  This is an array with (BH, count)
                // pairs used to allow a zero offset index for behaviors in the
                // event table.

                // First, if we're splitting it, we write the split values
                if (splitBehaviorTable) {
                    j = 0;
                    for (auto it = behCounts.begin(), end = behCounts.end();
                         it != end;  ++it, ++j) {

                        if (j < behaviorTableSplitIndex) {
                            writeBits(it->first, e.behBits, "behInIndex", j, behCounts.size());
                        }
                        else {
                            writeBits(it->first,
                                      behaviorTableSplitBitsAfter, "behInIndex", j,
                                      behCounts.size());
                        }

                        writeBits(it->second - 1, e.indexCountBits, "behCountInIndex",
                                  j, behCounts.size());
                    }
                }
                else {
                    //cerr << "----------- beh table" << endl;
                    j = 0;
                    for (auto it = behCounts.begin(), end = behCounts.end();
                         it != end;  ++it, ++j) {
                        writeBits(it->first, e.behBits, "behInIndex", j, behCounts.size());
                        writeBits(it->second - 1, e.indexCountBits, "behCountInIndex",
                                  j, behCounts.size());
                    }
                }
                
                ExcAssertEqual(bitsDone, e.numBehaviorTableBits());

                Lightweight_Hash<uint64_t, uint32_t>
                    timeOfsToIndex;
                
                if (hasTimestampTable) {
                    //cerr << "----------- ts table" << endl;
                    j = 0;
                    for (auto & t: tsSorted) {
                        timeOfsToIndex[t] = j;
                        //cerr << "offset " << t << " at " << j << endl;
                        writeBits(t - 1, e.timeBits, "tsInTsTable", j, tsSorted.size());
                        ++j;
                    }
                }
                ExcAssertEqual(bitsDone,
                               e.numBehaviorTableBits()
                               + e.numTimestampTableBits());
                
                // How many timestamp bits in the event table?
                int eventTableTsBits = e.numEventTableTimestampBits();

                //cerr << "----------- event table" << endl;

                j = 0;
                for (auto it = entryCounts.begin(), end = entryCounts.end();
                     it != end;  ++it, ++j) {
                    int32_t timeOfs = it->first.first;

                    //cerr << "looking up " << timeOfs << endl;

                    if (hasTimestampTable) {
                        auto it = timeOfsToIndex.find(timeOfs + 1);
                        ExcAssertNotEqual(it, timeOfsToIndex.end());
                        timeOfs = it->second;
                    }

                    ExcAssertGreaterEqual(timeOfs, 0);

                    uint32_t behTableIndex = behIndexToBehCountsIndex[it->first.second];
                    uint32_t count = it->second;

                    writeBits(timeOfs, eventTableTsBits,
                              "timeOfsInBehs",
                              j, entryCounts.size());
                    
                    writeBits(behTableIndex, tableIndexBits, "behIndexInBehs",
                              j, entryCounts.size());
                    writeBits(count - 1, e.countBits, "countInBehs",
                              j, entryCounts.size());
                    //cerr << endl;
                }

                //cerr << "----------- done" << endl;

                ExcAssertEqual(bitsDone, totalBits);

                return SubjectToWrite(buf, numWords, e);
            } catch (const std::exception & exc) {
                cerr << "error on subject " << subj << " with ID "
                     << getSubjectId(subj) << endl;

                auto onBeh = [&] (BH beh, Date ts, int count)
                {
                    cerr << "  " << beh << " " << ts << " " << count
                    << " " << getBehaviorId(beh) << endl;
                    return true;
                };
                
                forEachSubjectBehaviorHash(subj, onBeh);

                throw;
            }
        };

    int numSubjectsAtOnce = 100;

    // Take a subject that was prepared for serialization and actually
    // write it to disk.  This happens serially and in order of the
    // i values.
    auto writeSerializedSubject = [&] (int i, SubjectToWrite & subject)
        {
            if (subject.buf) {
                int numWords = subject.numWords;
                uint32_t * buf = subject.buf;
                auto & e = subject.e;

                uint64_t offset = (store.offset() - subjectDataOffset) / 4;
                if (numWords > 1) {
                    store.save_binary(buf, numWords * 4);
                    e.setOffset(offset);
                }
                else if (numWords == 1) {
                    e.offsetLow = buf[0];
                }
                
                delete[] buf;

                KVEntry<SH, MappedBehaviorDomain::SubjectIndexEntry> entry;

                SH hash = allSubjects[i];
                entry.key = hash;
                entry.value = e;

                subjectToIndex[hash] = index.size();
                index.push_back(entry);
            }
        };

    parallelMapInOrderReduceChunked(0, allSubjects.size(), serializeSubject,
                                    writeSerializedSubject, numSubjectsAtOnce);
    
    //for (unsigned i = 0;  i < allSubjects.size();  ++i) {
    //    serializeSubject(i);
    //}

    if (debug) {
        cerr << "finished writing subjects at "
             << t.elapsed() << endl;
        t.restart();
    }

#if 0
    cerr << "wrote " << index.size() << " entries" << endl;
    cerr << numWithMultipleBehaviors << " had more than one" << endl;
    cerr << "max behaviors " << maxNumBehaviors << endl;
    cerr << "kept " << nKept << " of " << allBehaviors.size()
         << " behaviors" << endl;
#endif     
   
    uint64_t subjectIndexOffset = store.offset();

    for (unsigned i = 0; i < index.size();  ++i)
        store.save_binary(index[i]);

    if (debug) {
        cerr << "finished writing subject index at " << t.elapsed() << endl;
        t.restart();
    }

    int numSubjectBits = ML::highest_bit(index.size() - 1, -1) + 1;

    struct BehaviorEntryToWrite {
        BehaviorEntryToWrite(uint32_t * subjectsBuf = 0,
                              size_t subjectsWords = 0,
                              uint32_t * timestampsBuf = 0,
                              size_t timestampsWords = 0)
            : subjectsBuf(subjectsBuf),
              subjectsWords(subjectsWords),
              timestampsBuf(timestampsBuf),
              timestampsWords(timestampsWords)
        {
        }

        uint32_t * subjectsBuf;
        size_t subjectsWords;
        uint32_t * timestampsBuf;
        size_t timestampsWords;
    };

    auto serializeBehaviorSubjects = [&] (const vector<uint32_t> & indexes)
        {
            uint64_t numBits = indexes.size() * numSubjectBits;
            uint64_t numWords = (numBits + 31) / 32;
            uint64_t numBytes = numWords * 4;

            uint32_t * buf = new uint32_t[numWords];
            memset(buf, 0, numBytes);

            Bit_Writer<uint32_t> writer(buf);
            for (unsigned j = 0;  j < indexes.size();  ++j)
                writer.write(indexes[j], numSubjectBits);

            return make_pair(buf, numWords);
        };

    auto serializeBehaviorTimestamps
        = [&] (BH beh,
               Date latestTimestamp,
               const vector<pair<SH, Date> > & subjects)
        {
            const BehaviorInfo & info = allInfo[beh];
            uint64_t start = quantizeTime(info.earliest);
            uint64_t latest = quantizeTime(latestTimestamp);
            int64_t gap = latest - start;
            ExcAssertGreaterEqual(gap, 0);

            int numTimestampBits = ML::highest_bit(gap, -1) + 1;

            //cerr << "gap = " << gap << " numTimestampBits = "
            //     << numTimestampBits << endl;

            ExcAssertLessEqual(numTimestampBits, 63);
            
            uint64_t numBits = subjects.size() * numTimestampBits
                + 6;  // 6 to record the number of timestamp bits
            uint64_t numWords = (numBits + 31) / 32;
            uint64_t numBytes = numWords * 4;

            uint32_t * buf = new uint32_t[numWords];
            memset(buf, 0, numBytes);

            Bit_Writer<uint32_t> writer(buf);
            writer.write(numTimestampBits, 6);
            for (unsigned j = 0;  j < subjects.size();  ++j) {
                uint64_t ts = quantizeTime(subjects[j].second);
                ExcAssertGreaterEqual(ts, start);
                uint32_t ofs = ts - start;
                writer.write(ofs, numTimestampBits);
            }

            return make_pair(buf, numWords);
        };

    auto serializeBehavior = [&] (int i) -> BehaviorEntryToWrite
        {
            BH beh = std::get<0>(subjectCounts[i]);
            auto subjects
            = this->getSubjectHashesAndTimestamps(beh, (SH)-1,
                                                  true /* sorted */);
            ExcAssertEqual(allInfo[beh].subjectCount, subjects.size());

            // Map the 64 bit behavior codes onto a small integer space
            // to allow efficient bit compression
            vector<uint32_t> indexes;
            indexes.reserve(subjects.size());

            Date latestTimestamp = Date::negativeInfinity();

            for (unsigned j = 0;  j < subjects.size();  ++j) {
                auto it = subjectToIndex.find(subjects[j].first);
                if (it == subjectToIndex.end()) {

                    static std::mutex lock;
                    std::unique_lock<std::mutex> guard(lock);

                    cerr << "subject " << subjects[j]
                         << " not found in " << subjectToIndex.size()
                         << " entries" << endl;
                    cerr << "beh = " << i << " = " << beh
                         << " " << getBehaviorId(beh) << endl;
                    cerr << "subjects.size() = " << subjects.size() << endl;

                    cerr << "knownSubject = " << knownSubject(subjects[j].first)
                         << endl;

                    dumpSubject(subjects[j].first);

                    throw MLDB::Exception("subject not found");
                }
                else indexes.push_back(it->second);
                latestTimestamp = std::max(latestTimestamp, subjects[j].second);
            }

            auto buf1 = serializeBehaviorSubjects(indexes);
            auto buf2 = serializeBehaviorTimestamps(beh, latestTimestamp,
                                                 subjects);

            return BehaviorEntryToWrite(buf1.first, buf1.second,
                                         buf2.first, buf2.second);
        };

    // For each behavior, a list of subjects
    uint64_t behaviorSubjectsOffset = store.offset();

    std::vector<uint32_t> behaviorToSubjectsIndex;
    std::vector<uint32_t> behaviorToSubjectTimestampsIndex;

    // Timestamps get stored here
    uint64_t timestampBlockOffset = 0;
    std::vector<pair<uint32_t *, size_t> > timestampBlocks;
    timestampBlocks.reserve(nKept);

    auto writeBehavior = [&] (int i, const BehaviorEntryToWrite & entry)
        {
            {
                uint64_t ofs_ = store.offset() - behaviorSubjectsOffset;
                uint32_t ofs = ofs_ / 4;
                if (ofs_ != (uint64_t)ofs * 4)
                    throw MLDB::Exception("invalid offset");

                store.save_binary(entry.subjectsBuf, entry.subjectsWords * 4);
        
                behaviorToSubjectsIndex.push_back(ofs);
                
                delete[] entry.subjectsBuf;
            }

            {
                behaviorToSubjectTimestampsIndex.push_back
                    (timestampBlockOffset);
                timestampBlockOffset += entry.timestampsWords;
                timestampBlocks.push_back({entry.timestampsBuf, entry.timestampsWords});
            }
        };

#if 0 // debug only; slow
    for (unsigned i = 0;  i < nKept;  ++i)
        writeBehavior(i, serializeBehavior(i));

#elif 1 // fast, optimized version
    auto workForBehavior = [&] (int behIndex)
        {
            return std::get<1>(subjectCounts[behIndex]);
        };
    
    parallelMapInOrderReduceInEqualWorkChunks
        (0, nKept,
         serializeBehavior, writeBehavior, workForBehavior);

#elif 1

    // Range of indexes to write in each chunk
    vector<pair<int, int> > writeChunks;

    int amountPerChunk = totalSubjectCount / 512;  // create around 512 jobs

    // Try to split them into batches of behaviors with roughly equal amounts of work
    int lastEnd = 0;
    while (lastEnd < nKept) {
        int curr = lastEnd;
        size_t amountInChunk = subjectCounts[curr++].second;
        while (curr < nKept) {
            size_t thisChunk = subjectCounts[curr].second;
            if (amountInChunk + thisChunk > amountPerChunk)
                break;
            amountInChunk += thisChunk;
            ++curr;
        }

        writeChunks.push_back(make_pair(lastEnd, curr));
        lastEnd = curr;
    }

    parallelMapInOrderReducePreChunked(writeChunks, serializeBehavior, writeBehavior);

#elif 0
    parallelMapInOrderReduceChunked(0, nKept, serializeBehavior, writeBehavior, 1);

#else
    // Do it twice: in a chunk of 1 for the frequent ones at the start, and
    // in a chunk of 10 for the rest.
    parallelMapInOrderReduceChunked(0, nKept / 5,
                                    serializeBehavior, writeBehavior,
                                    1);
    
    parallelMapInOrderReduceChunked(nKept / 5, nKept,
                                    serializeBehavior, writeBehavior,
                                    10);
#endif

    //for (unsigned i = 0;  i < nKept;  ++i) {
    //    writeBehavior(i, serializeBehavior(i));
    //}

    if (debug) {
        cerr << "finished writing behavior subjects at "
             << t.elapsed() << endl;
        t.restart();
    }

    //cerr << "after writing behavior data: " << endl;
    //tc_malloc_stats();

    uint64_t behaviorToSubjectsIndexOffset = store.offset();
    for (unsigned i = 0;  i < behaviorToSubjectsIndex.size();  ++i)
        store.save_binary(behaviorToSubjectsIndex[i]);

    if (debug) {
        cerr << "finished writing subject index at " << t.elapsed() << endl;
        t.restart();
    }

    //cerr << "after writing timestamps: " << endl;
    //tc_malloc_stats();

    uint64_t behaviorToSubjectTimestampsOffset = store.offset();

    for (auto entry: timestampBlocks) {
        uint32_t * buf = entry.first;
        size_t size = entry.second;
        store.save_binary(buf, size * 4);
        delete[] buf;
    }    

    uint64_t behaviorToSubjectTimestampsIndexOffset = store.offset();
    for (unsigned i = 0;  i < behaviorToSubjectTimestampsIndex.size();  ++i)
        store.save_binary(behaviorToSubjectTimestampsIndex[i]);

    if (debug) {
        cerr << "finished writing subject timestamps index at "
             << t.elapsed() << endl;
        t.restart();
    }

    uint64_t subjectIdDataOffset = 0, subjectIdIndexOffset = 0;

    if (hasSubjectIds) {
        subjectIdDataOffset = store.offset();

        // Write the subject IDs to the store
        std::vector<uint32_t> subjectIdOffsets;
        
        for (unsigned i = 0;  i < index.size();  ++i) {
            SH subject = index[i].key;
            Id id = getSubjectId(subject);
            subjectIdOffsets.push_back(store.offset() - subjectIdDataOffset);
            store << id;
        }
    
        //cerr << "subjectIdOffsets.back() = " << subjectIdOffsets.back()
        //     << endl;

        subjectIdIndexOffset = store.offset();

        for (unsigned i = 0;  i < subjectIdOffsets.size();  ++i) {
            store.save_binary(subjectIdOffsets[i]);
        }
    }

    uint64_t fileMetadataOffset = store.offset();
    store << (unsigned char)0;  // metadata version
    store << getAllFileMetadata().toString();  // metadata contents

    uint64_t metadataOffset = store.offset();
    MappedBehaviorDomain::Metadata metadata;
    memset(&metadata, 0, sizeof(metadata));
    metadata.magic = magicStr("BehsHour");
    metadata.version = 5;
    metadata.subjectDataOffset = subjectDataOffset;
    metadata.subjectIndexOffset = subjectIndexOffset;
    metadata.behaviorIndexOffset = behaviorIndexOffset;
    metadata.behaviorIdOffset = behaviorIdOffset;
    metadata.behaviorIdIndexOffset = behaviorIdIndexOffset;
    metadata.behaviorInfoOffset = behaviorInfoOffset;
    metadata.behaviorToSubjectsIndexOffset = behaviorToSubjectsIndexOffset;
    metadata.behaviorSubjectsOffset = behaviorSubjectsOffset;
    metadata.subjectIdDataOffset = subjectIdDataOffset;
    metadata.subjectIdIndexOffset = subjectIdIndexOffset;
    //ExcAssertGreater(earliestTime(), Date());
    //ExcAssertGreaterEqual(latestTime(), earliestTime());
    metadata.earliest = quantizeTime(earliestTime());
    metadata.latest = quantizeTime(latestTime());
    metadata.numBehaviors = nKept;
    metadata.numSubjects = index.size();
    metadata.minSubjects = minSubjects;
    //ExcAssertGreater(nominalEnd(), nominalStart());
    metadata.nominalStart = nominalStart();
    metadata.nominalEnd = nominalEnd();
    metadata.timeQuantum = timeQuantum;
    metadata.behaviorToSubjectTimestampsIndexOffset
        = behaviorToSubjectTimestampsIndexOffset;
    metadata.behaviorToSubjectTimestampsOffset
        = behaviorToSubjectTimestampsOffset;
    metadata.idSpaceDeprecated = 0;
    metadata.fileMetadataOffset = fileMetadataOffset;
    metadata.totalEventsRecorded = totalEventsRecorded;

    store.save_binary(metadata);

    uint64_t lastOffset = behaviorIndexOffset;
    auto writeBytes = [&] (const std::string & what, uint64_t offset)
        {
            return;
            cerr << MLDB::format("%30s %10.4fkb (%6.2f%%)",
                               what.c_str(),
                               (offset - lastOffset) / 1024.0,
                               100.0 * (offset - lastOffset)
                               / (store.offset() - behaviorIndexOffset))
            << endl;
            lastOffset = offset;
        };

    writeBytes("preamble", behaviorIndexOffset);
    writeBytes("behaviorIndex", behaviorIdOffset);
    writeBytes("behaviorId", behaviorIdIndexOffset);
    writeBytes("behaviorIdIndex", behaviorInfoOffset);
    writeBytes("behaviorInfo", subjectDataOffset);
    writeBytes("subjectData", subjectIndexOffset);
    writeBytes("subjectIndex", behaviorSubjectsOffset);
    writeBytes("behaviorSubjects", behaviorToSubjectsIndexOffset);
    writeBytes("behaviorToSubjectsIndex", metadataOffset);
    writeBytes("metadata", store.offset());

    return metadataOffset;
}

void
BehaviorDomain::
verify()
{
    // cerr << "we have " << behaviorCount() << " behaviors and "
    //      << subjectCount() << " subjects" << endl;

    //SH prevHash(0);

    std::unordered_map<BH, std::map<SH, int> > aggCounts;
    std::unordered_map<SH, std::map<BH, int> > aggCountsBySubject;

    std::vector<SH> allSubjects = allSubjectHashes();
    ExcAssertEqual(subjectCount(), allSubjects.size());

    for (unsigned i = 0;  i < allSubjects.size();  ++i) {
        //auto stats = getSubjectStats(SI(i));
        SH subjectHash = allSubjects[i];
        //cerr << "i = " << i << " of " << subjectCount()
        //     << " subjectHash "
        //     << MLDB::format("%016llx", (unsigned long long)subjectHash)
        //     << endl;
        //if (i != 0 || subjectHash != SH(0))
        //    ExcAssertGreater(subjectHash, prevHash);
        //prevHash = subjectHash;
        //SI idx2 = getSubjectIndex(subjectHash);

#if 0
        if (idx2 != i) {
            cerr << "stats.offset = " << stats.offset << endl;
            cerr << "stats.numDistinctBehaviors = " << stats.numDistinctBehaviors
                 << endl;
            cerr << "stats.behBits = " << (int)stats.behBits << endl;
            cerr << "stats.countBits = " << (int)stats.indexCountBits << endl;
            cerr << "stats.numWords = " << stats.numWords() << endl;
        }
#endif
        
        //ExcAssertEqual(idx2, SI(i));

        //cerr << "subjectHash = " << subjectHash << " "
        //     << getSubjectId(subjectHash) << endl;

        auto behCounts = getSubjectBehaviorCounts(subjectHash);

        //cerr << "behCounts.size() = " << behCounts.size() << endl;

#if 0
        if (stats.numWords() > 1) {
            cerr << "subject " << subjectHash << " with "
                 << behCounts.size() << " behaviors, "
                 << stats.numWords() << " words and offset "
                 << MLDB::format("%08x", stats.offset) << endl;
        }
#endif

        //size_t totalCount = 0;
        BH prev(-1);
        for (unsigned j = 0;  j < behCounts.size();  ++j) {
            if (behCounts[j].second < 1
                || (prev != -1 && behCounts[j].first <= prev)) {
                cerr << "i = " << i << endl;
                cerr << "behCounts " << behCounts << endl;
                cerr << "subjectHash = " << subjectHash << endl;

#if 0
                cerr << "stats.offset = " << stats.offset << endl;
                cerr << "stats.numDistinctBehaviors = "
                     << stats.numDistinctBehaviors
                     << endl;
                cerr << "stats.behBits = " << (int)stats.behBits << endl;
                cerr << "stats.countBits = " << (int)stats.indexCountBits << endl;
                cerr << "stats.numWords = " << stats.numWords() << endl;
#endif
            }

            ExcAssert(knownBehavior(behCounts[j].first));
            ExcAssertGreaterEqual(behCounts[j].second, 1);
            if (prev != BH(-1))
                ExcAssertGreater(behCounts[j].first, prev);
            prev = behCounts[j].first;

            BH beh = behCounts[j].first;
            aggCounts[beh][subjectHash] += behCounts[j].second;
            aggCountsBySubject[subjectHash][beh] += behCounts[j].second;
            
#if 0
            if (stats.numWords() > 1)
                cerr << "    " << j << " "
                     << getBehaviorId(behCounts[j].first)
                     << " --> " << behCounts[j].second << endl;
#endif
        }
    }

    //for (unsigned i = 0;  i < 100;  ++i)
    //    cerr << "  " << i << " "
    //         << behaviorIndex[i].key << " " << behaviorIndex[i].value << endl;
    
    auto onBehavior = [&] (BH beh, const BehaviorIterInfo &)
        {
            Id id = getBehaviorId(beh);
            BehaviorStats stats = getBehaviorStats(beh, BS_ALL);
            BH hash(id);

            //cerr << "id = " << id << " hash = " << hash << endl;
            //cerr << "aggCounts.size() = " << aggCounts.size() << endl;

            ExcAssertEqual(id, stats.id);

            vector<SH> subjects = getSubjectHashes(hash, SH::max(), true);

            map<SH, int> & agg = aggCounts[hash];

            if (agg.size() != stats.subjectCount
                || agg.size() != subjects.size()) {
                cerr << "beh " << beh << " id " << id
                     << " agg.size() = " << agg.size()
                     << " subjects.size() = " << subjects.size()
                     << endl;

                cerr << "subjects: " << endl;
                for (auto & e: subjects) {
                    cerr << e << endl;
                }

                cerr << "agg: " << endl;
                for (auto & e: agg) {
                    cerr << e.first << " -> " << e.second << endl;
                }
            }

            ExcAssertEqual(agg.size(), stats.subjectCount);
            ExcAssertEqual(agg.size(), subjects.size());

            std::sort(subjects.begin(), subjects.end());

            int k = 0;
            for (auto it = agg.begin(), end = agg.end();  it != end;  ++it, ++k) {
                ExcAssertEqual(it->first, subjects[k]);
            }

            return true;
        };

    forEachBehavior(onBehavior);
}

bool
BehaviorDomain::
subjectHasNDistinctBehaviors(SH subjectHash, int n) const
{
    SubjectStats stats = getSubjectStats(subjectHash);
    return stats.numDistinctBehaviors >= n;
}

std::vector<SH>
BehaviorDomain::
subjectsWithNBehaviors(int n) const
{
    std::vector<SH> result;

    auto onSubject = [&] (const SH & subjectHash, const SubjectIterInfo & info)
        {
            if (this->subjectHasNDistinctBehaviors(subjectHash, n))
                result.push_back(subjectHash);
            return true;
        };

    forEachSubject(onSubject);

    return result;
}

std::vector<std::pair<SH, Date> >
BehaviorDomain::
allSubjectsAndEarliestBehavior(SH maxSubject,
                                bool sorted,
                                bool includeWithNoBeh) const
{
    std::vector<std::pair<SH, Date> > result;

    auto onSubject = [&] (const SH & subjectHash, const SubjectIterInfo & info)
        {
            if (subjectHash > maxSubject)
                return true;
            auto ts = getSubjectTimestampRange(subjectHash).first;
            if (!ts.isADate() && !includeWithNoBeh)
                return true;
            result.emplace_back(subjectHash, ts);
            return true;
        };

    forEachSubject(onSubject);

    if (sorted)
        std::sort(result.begin(), result.end());
    
    return result;
}
 
std::vector<std::pair<SH, Date> >
BehaviorDomain::
getSubjectHashesAndAllTimestamps(BH beh, SH maxSubject,
                                 bool sorted) const
{
    std::vector<std::pair<SH, Date> > result;

    // Get the subjects with their first timestamp to start things
    // off.
    auto subjs = getSubjectHashesAndTimestamps(beh, maxSubject, sorted);

    // No need to calculate this if there is only one timestamp; it is
    // not possible that there be a problem.
    if (earliestTime() == latestTime())
        return subjs;

    EventFilter filter;
    filter.filterBehInList(vector<BH>({beh}));

    for (auto & s: subjs) {
        SH subj = s.first;
        Date firstTs = s.second;
        int numFound = 0;

        auto onBeh = [&] (BH sbeh, Date ts, int count)
            {
                // Re-filter to be defensive against the BehaviorDomain
                // descendents not correctly implementing the filters.
                // They are not well unit-tested.

                if (sbeh != beh || ts < firstTs) {
                    static std::atomic<int> numWarnings(0);
                    if (numWarnings < 1000) {
                        ++numWarnings;
                        cerr << "WARNING: BehaviorDomain class "
                             << MLDB::type_name(*this)
                             << " doesn't properly filter behaviors in "
                             << "forEachSubjectBehaviorHash()"
                             << endl;
                    }
                }



                if (sbeh == beh) {
                    ExcAssertGreaterEqual(ts, firstTs);
                    ++numFound;
                    result.push_back(make_pair(subj, ts));
                }

                return true;
            };

        filter.earliest = firstTs;

        forEachSubjectBehaviorHash(subj, onBeh, filter);

        ExcAssertGreaterEqual(numFound, 1);
    }

    // Will be already sorted if sorted was true
    return result;
}

bool
BehaviorDomain::
forEachSubjectBehaviorHashBackwards
        (SH subject,
         const OnSubjectBehaviorHash & onBeh,
         EventFilter filter,
         Order order) const
{
    struct Entry {
        BH beh;
        Date ts;
        int count;
    };

    vector<Entry> entries;

    auto onBeh2 = [&] (BH beh, Date ts, int count)
        {
            Entry entry = { beh, ts, count };
            entries.push_back(entry);
            return true;
        };

    forEachSubjectBehaviorHash(subject, onBeh2, filter, order);

    for (int i = entries.size() - 1;  i >= 0;  --i) {
        if (!onBeh(entries[i].beh, entries[i].ts, entries[i].count))
            return false;
    }

    return true;
}

bool
BehaviorDomain::
forEachSubjectTimestamp(SH subject, const OnSubjectTimestampMap & onTs,
                        Direction direction,
                        Date earliest,
                        Date latest) const
{
    if (direction == FORWARDS) {
        // Scan through in time order, and group together the events from
        // the same timestamp.
        Date currentTs;
        bool hasCurrent = false;
        Lightweight_Hash<BH, int> current;

        EventFilter filter(earliest, latest);

        auto onBeh = [&] (BH beh, Date ts, int count)
            {
                if (!hasCurrent || ts != currentTs) {
                    if (!current.empty()) {
                        ExcAssertGreaterEqual(currentTs, earliest);
                        ExcAssertLessEqual(currentTs, latest);

                        bool res = onTs(currentTs, current);
                        if (!res)
                            return false;
                        current.clear();
                    }
                    currentTs = ts;
                    hasCurrent = true;
                }

                current[beh] += count;

                return true;
            };

        if (!this->forEachSubjectBehaviorHash(subject, onBeh, filter, INORDER))
            return false;

        if (hasCurrent && !current.empty()) {
            ExcAssertGreaterEqual(currentTs, earliest);
            ExcAssertLessEqual(currentTs, latest);
            return onTs(currentTs, current);
        }
        return true;
    }
    else {
        // Scan through in time order, and group together the events from
        // the same timestamp.
        Date currentTs;
        bool hasCurrent = false;
        Lightweight_Hash<BH, int> current;

        EventFilter filter(earliest, latest);

        auto onBeh = [&] (BH beh, Date ts, int count)
            {
                if (!hasCurrent || ts != currentTs) {
                    if (!current.empty()) {
                        if (!onTs(currentTs, current))
                            return false;
                        current.clear();
                    }
                    currentTs = ts;
                    hasCurrent = true;
                }

                current[beh] += count;

                return true;
            };

        if (!this->forEachSubjectBehaviorHashBackwards(subject, onBeh, filter,
                                                        INORDER))
            return false;
        if (hasCurrent && !current.empty())
            return onTs(currentTs, current);
        return true;
    }
}

Lightweight_Hash<BH, int>
BehaviorDomain::
getSubjectTimestamp(SH subject, Date atTs) const
{
    Lightweight_Hash<BH, int> result;
    
    EventFilter filter(atTs, atTs.plusSeconds(1));

    auto onBeh = [&] (BH beh, Date ts, int count)
        {
            if (ts > atTs)
                return false;
            if (ts < atTs)
                return true;

            result[beh] += count;
            
            return true;
        };
    
    this->forEachSubjectBehaviorHash(subject, onBeh, filter, INORDER);

    return result;
}

template<typename Beh>
int
BehaviorDomain::
coIterateBehaviorsImpl(Beh beh1, Beh beh2,
                        SH maxSubject,
                        const OnBehaviors & onBehaviors) const
{
    vector<SH> subs1 = getSubjectHashes(beh1, maxSubject, true /* sorted */);
    vector<SH> subs2 = getSubjectHashes(beh2, maxSubject, true /* sorted */);
    
    auto it1 = subs1.begin(), end1 = subs1.end();
    auto it2 = subs2.begin(), end2 = subs2.end();
    
    int res = 0;

    if (!onBehaviors) {
        while (it1 != end1 && it2 != end2) {
            SH s1 = *it1, s2 = *it2;
            if (s1 > maxSubject && s2 > maxSubject) break;
            res += s1 == s2;
            it1 += s1 <= s2;
            it2 += s1 >= s2;
        }
    }
    else {
        while (it1 != end1 && it2 != end2) {
            SH s1 = *it1, s2 = *it2;
            SH m = std::min(s1, s2);
            if (m > maxSubject) break;
            if (s1 == s2)
                onBehaviors(s1);
            
            res += s1 == s2;
            it1 += s1 <= s2;
            it2 += s1 >= s2;
        }
    }

    return res;
}

int
BehaviorDomain::
coIterateBehaviors(BH beh1, BH beh2,
                    SH maxSubject,
                    const OnBehaviors & onBehaviors) const
{
    return coIterateBehaviorsImpl(beh1, beh2, maxSubject, onBehaviors);
}

std::vector<uint32_t>
BehaviorDomain::
getSubjectBehaviorCounts(SH subjectHash,
                          const std::vector<BH> & behs,
                          uint32_t maxCount) const
{
    // TODO: very dumb algorithm

    vector<pair<BH, uint32_t> > counts
        = getSubjectBehaviorCounts(subjectHash);
    vector<uint32_t> result;
    for (unsigned i = 0;  i < behs.size();  ++i) {
        int entry = 0;
        for (unsigned j = 0;  j < counts.size();  ++j) {
            if (counts[j].first == behs[i]) {
                entry = counts[j].second;
                break;
            }
        }
        result.push_back(std::min<uint32_t>(maxCount, entry));
    }
    return result;
}

uint32_t
BehaviorDomain::
getSubjectBehaviorCount(SH subjectHash, BH beh, uint32_t maxCount) const
{
    return getSubjectBehaviorCounts(subjectHash, vector<BH>({ beh }),
                                     maxCount)[0];
}

Date
BehaviorDomain::
updateUnbiasedSubjectBehaviorCounts
    (SH subject,
     const Lightweight_Hash_Set<BH> & conv,
     const Lightweight_Hash_Set<BH> & ignore,
     BehaviorCounts & counts,
     Date earliest,
     const FixDate & fixDate) const
{
    //auto subj = behs.getSubjectBehaviorCounts(subject);
    
    //auto subj = getSubjectInfo(subject);

    Date conversionTime = Date::positiveInfinity();
    
    // Pass 1: go through until we hit the conversion and add counts
    Date latestRecorded;

    int i = 0;
    auto onBehavior = [&] (BH beh, Date ts, uint32_t count)
        {
            ++i;
            if (ignore.count(beh))
                return true;
            if (conversionTime.isADate()
                && conversionTime >= ts)
                return false;
            ExcAssertGreaterEqual(ts, latestRecorded);
            if (conv.count(beh)) {
                conversionTime = ts;
                if (fixDate)
                    fixDate(earliest, conversionTime);
                return false;
            }
            latestRecorded = ts;
            counts[beh] += count;
            return true;
        };

    forEachSubjectBehaviorHash(subject, onBehavior,
                                EventFilter(),
                                INORDER);
    
    if (latestRecorded >= conversionTime) {
        // Pass 2: fix up those that were added inadvertently

        //cerr << "Needed fixup" << endl;

        int j = 0;
        auto fixupBehavior = [&] (BH beh, Date ts, uint32_t count)
            {
                ++j;
                if (j >= i) return false;  // finished 

                if (ignore.count(beh))
                    return true;

                if (ts >= conversionTime)
                    counts[beh] -= count;
                
                return true;
            };

        forEachSubjectBehaviorHash(subject, fixupBehavior,
                                    EventFilter(),
                                    INORDER);
        
#if 0
        for (int j = i - 1;  j >= 0;  --j) {
            SubjectBehavior beh = subj.behaviors[j];
            if (beh.timestamp < conversionTime) break;
            counts[BH(beh.behavior)] -= beh.count;
        }
#endif
    }

    return conversionTime;
}

BehaviorCounts
BehaviorDomain::
getUnbiasedSubjectBehaviorCountsFixedDate
    (SH subject,
     const Lightweight_Hash_Set<BH> & converters,
     const Lightweight_Hash_Set<BH> & ignored,
     BehaviorCounts & counts,
     Date earliestDate,
     Date latestDate) const
{
    BehaviorCounts result;

    auto onBehavior = [&] (BH beh, Date ts, uint32_t count)
        {
            if (ignored.count(beh))
                return true;
            if (ts >= latestDate)
                return true;
            if (converters.count(beh)) {
                throw MLDB::Exception("converters not properly excluded");
            }
            counts[beh] += count;
            return true;
        };

    forEachSubjectBehaviorHash(subject, onBehavior,
                                EventFilter(),
                                ANYORDER);

    return result;
}

#if 0
SubjectInfo
BehaviorDomain::
getSubjectInfo(SH subject) const
{
    SubjectInfo result;
    auto stats = getSubjectStats(subject, true);
    result.firstSeen = stats.earliest;
    result.numDistinctBehaviors = stats.numDistinctBehaviors;

    auto onBeh = [&] (BH beh, Date ts, uint32_t count)
        {
            result.record(beh, ts, count);
            return true;
        };

    forEachSubjectBehaviorHash(subject, onBeh,
                                EventFilter(),
                                ANYORDER);

    result.sort();

    return result;
}
#endif

bool
BehaviorDomain::
forEachBehaviorSubject(BH beh,
                        const OnBehaviorSubject & onSubject,
                        bool withTimestamps,
                        Order order,
                        SH maxSubject) const
{
    if (withTimestamps) {
        auto r = getSubjectHashesAndTimestamps(beh, maxSubject, order == INORDER);
        for (auto s: r)
            if (!onSubject(s.first, s.second))
                return false;
        return true;
    }
    else {
        auto r = getSubjectHashes(beh, maxSubject, order == INORDER);
        for (auto s: r)
            if (!onSubject(s, Date()))
                return false;
        return true;
    }
}

bool
BehaviorDomain::
forEachBehavior(const OnBehavior & onBehavior,
                 const BehaviorFilter & filter) const
{
    auto behs = allBehaviorHashes();
    for (BH b: behs)
        if (!onBehavior(b, BehaviorIterInfo()))
            return false;
    return true;
}

void
BehaviorDomain::
dump(std::ostream & stream) const
{
    vector<BH> allBehs = allBehaviorHashes();
    vector<SH> allSubs = allSubjectHashes();
     
    for (auto s: allSubs)
        dumpSubject(s, stream);
    stream << endl;
            
    for (auto b: allBehs) {
        auto stats = getBehaviorStats(b, BS_ALL);

        stream << "behavior " << b << " " << getBehaviorId(b) << " "
               << stats.earliest << " " << stats.latest << " "
               << stats.subjectCount << endl;
                
        auto onBehSubject = [&] (SH subj, Date earliest)
            {
                stream << "  " << subj << " " << earliest << " "
                << getSubjectId(subj) << endl;
                return true;
            };

        forEachBehaviorSubject(b, onBehSubject, true);
    }
}

void
BehaviorDomain::
dumpSubject(SH s, std::ostream & stream) const
{
    auto stats = getSubjectStats(s, true);

    stream << "subject " << s << " " << getSubjectId(s)
           << " " << stats.earliest << " beh " << stats.numBehaviors
           << " dis " << stats.numDistinctBehaviors << endl;

    auto onSubjectBeh = [&] (BH beh, Date ts, int count)
        {
            stream << "  " << beh << " " << ts << " " << count << " "
            << getBehaviorId(beh) << endl;
            return true;
        };
    forEachSubjectBehavior(s, onSubjectBeh);
}


std::vector<BehaviorDomain::SubjectBehavior>
BehaviorDomain::
getSubjectBehaviors(SH subject,
                     EventFilter filter,
                     Order order) const
{
    vector<SubjectBehavior> result;

    auto onBehavior = [&] (BH beh, Date ts, int count)
        {
            result.push_back(SubjectBehavior(beh, ts, count));
            return true;
        };

    forEachSubjectBehavior(subject, onBehavior, filter, order);

    return result;
}

const SubjectFilter BehaviorDomain::ALL_SUBJECTS;
const EventFilter BehaviorDomain::ALL_EVENTS;
const BehaviorFilter BehaviorDomain::ALL_BEHAVIORS;

bool
BehaviorDomain::
forEachSubjectParallel(const OnSubjectStats & onSubject,
                       const SubjectFilter & filter,
                       int statsFields,
                       Parallelism parallelism) const
{
    // Add in fields needed for filtering
    statsFields = statsFields | filter.requiredStatsFields();

    auto doSubject = [&] (SH subj, int i)
        {
            // See if we can fail it early
            if (filter.fail(subj))
                return true;
            
            // If not get the stats
            SubjectStats stats;
            if (statsFields)
                stats = getSubjectStatsFields(subj, statsFields);
            
            // Now see if it passes
            if (!filter.pass(subj, stats))
                return true;
            
            SubjectIterInfo info;
            info.index = i;
            
            return onSubject(subj, info, stats);
        };

    std::vector<SH> subs;

    if (filter.hasList()) {
        subs = filter.getList();
    }
    else {
        subs = allSubjectHashes(SH::max(), false /* sorted */);
    }

    if (parallelism == PARALLEL) {

        std::atomic<bool> stopped(false);
        
        auto onSubject2 = [&] (int i)
            {
                if (stopped)
                    return false;
                SH subj = subs[i];
                bool res = doSubject(subj, i);
                if (!res)
                    stopped = true;
                return res;
            };
        
        // TODO: stop allowing it to escape
        parallelMap(0, subs.size(), onSubject2);

        return !stopped;
    }
    else {
        for (unsigned i = 0;  i < subs.size();  ++i) {
            SH subj = subs[i];
            if (!doSubject(subj, i))
                return false;
        }
        
        return true;
    }
}

bool
BehaviorDomain::
forEachSubjectGetBehaviorCounts(const std::function<bool (SH subject,
                                                           const SubjectIterInfo & info,
                                                           const SubjectStats & stats,
                                                           const std::vector<std::pair<BH, uint32_t> > & counts) > & onSubject,
                                 const SubjectFilter & subjectFilter,
                                 EventFilter eventFilter,
                                 Order order,
                                 int statsFields,
                                 Parallelism inParallel) const
{
    auto onSubject2 = [&] (SH subject, const SubjectIterInfo & info, const SubjectStats & stats)
        {
            auto counts = getSubjectBehaviorCountsFiltered(subject, eventFilter, order);
            return onSubject(subject, info, stats, std::move(counts));
        };

    return forEachSubjectParallel(onSubject2, subjectFilter, statsFields, inParallel);
}

bool
BehaviorDomain::
forEachSubjectGetEvents(const std::function<bool (SH subject,
                                                  const SubjectIterInfo & info,
                                                  const SubjectStats & stats,
                                                  const std::vector<std::tuple<BH, Date, uint32_t> > & counts) > & onSubject,
                        const SubjectFilter & subjectFilter,
                        EventFilter eventFilter,
                        Order resultOrder,
                        int statsFields,
                        Parallelism inParallel) const
{
    auto onSubject2 = [&] (SH subject, const SubjectIterInfo & info, const SubjectStats & stats)
        {
            std::vector<std::tuple<BH, Date, uint32_t> > events;

            auto onBeh = [&] (BH beh, Date ts, int32_t count)
            {
                if (eventFilter.pass(beh, ts, count))
                    events.emplace_back(beh, ts, count);
                return true;
            };

            forEachSubjectBehaviorHash(subject, onBeh, eventFilter, resultOrder);

            return onSubject(subject, info, stats, std::move(events));
        };
    
    return forEachSubjectParallel(onSubject2, subjectFilter, statsFields, inParallel);
}

bool
BehaviorDomain::
forEachSubjectGetTimestamps(const std::function<bool (SH subject,
                                                      const SubjectIterInfo & info,
                                                      const SubjectStats & stats,
                                                      const std::vector<std::pair<Date, Lightweight_Hash<BH, int> > > & onSubject) > & onSubject,
                            const SubjectFilter & subjectFilter,
                            EventFilter eventFilter,
                            Order resultOrder,
                            int statsFields,
                            Parallelism inParallel) const
{
    auto onSubject2 = [&] (SH subject, const SubjectIterInfo & info, const SubjectStats & stats)
        {
            std::vector<std::pair<Date, Lightweight_Hash<BH, int> > > atTs;

            auto onTs = [&] (Date ts, const Lightweight_Hash<BH, int> & events)
            {
                if (eventFilter.passAll())
                    atTs.emplace_back(ts, events);
                else {
                    Lightweight_Hash<BH, int> eventsOut;
                    for (auto & ev: events) {
                        if (eventFilter.pass(ev.first, ts, ev.second))
                            eventsOut.insert(ev);
                    }
                    
                    if (!eventsOut.empty())
                        atTs.emplace_back(ts, std::move(eventsOut));
                }

                return true;
            };

            forEachSubjectTimestamp(subject, onTs);

            return onSubject(subject, info, stats, std::move(atTs));
        };
    
    return forEachSubjectParallel(onSubject2, subjectFilter, statsFields, inParallel);
}

std::vector<std::pair<BH, uint32_t> >
BehaviorDomain::
getSubjectBehaviorCountsFiltered(SH subj,
                                  const EventFilter & filter,
                                  Order order) const
{
    // For the usual case, fast-path it
    if (filter.passAll())
        return getSubjectBehaviorCounts(subj, order);

    Lightweight_Hash<BH, uint32_t> result;

    auto onBeh = [&] (BH beh, Date ts, int count)
        {
            result[beh] += count;
            return true;
        };

    forEachSubjectBehaviorHash(subj, onBeh, filter, ANYORDER);

    vector<pair<BH, uint32_t> > sorted(result.begin(), result.end());
    if (order == INORDER)
        std::sort(sorted.begin(), sorted.end());
    
    return sorted;
}

bool
BehaviorDomain::
forEachBehaviorParallel(const OnBehaviorStats & onBehavior,
                         const BehaviorFilter & filter,
                         int statsFields,
                         Parallelism parallelism) const
{
    statsFields = statsFields | filter.requiredStatsFields();
    
    if (parallelism == PARALLEL) {
        auto behs = allBehaviorHashes(false /* sorted */);
        
        std::atomic<bool> stopped(false);
        
        auto onBehavior2 = [&] (int i)
            {
                if (stopped)
                    return false;
                
                BH beh = behs[i];

                if (filter.fail(beh))
                    return false;

                BehaviorStats stats;
                if (statsFields)
                    stats = getBehaviorStats(beh, statsFields);

                if (!filter.pass(beh, stats))
                    return true;
                
                BehaviorIterInfo info;
                info.index = i;

                if (!onBehavior(beh, info, stats)) {
                    stopped = true;
                    return false;
                }

                return true;
            };

        // TODO: stop allowing it to escape
        parallelMap(0, behs.size(), onBehavior2);

        return !stopped;
    }
    else {
        auto behs = allBehaviorHashes(parallelism == ORDERED /* sorted */);

        for (unsigned i = 0;  i < behs.size();  ++i) {
            BH beh = behs[i];
            if (filter.fail(beh))
                continue;
            
            BehaviorStats stats;
            if (statsFields)
                stats = getBehaviorStats(beh, statsFields);

            if (!filter.pass(beh, stats))
                continue;

            BehaviorIterInfo info;
            info.index = i;

            if (!onBehavior(beh, info, stats))
                return false;
        }
        
        return true;
    }
}

std::ostream &
operator << (std::ostream & stream, const BehaviorDomain::SubjectBehavior & beh)
{
    return stream << "(" << beh.beh << " " << beh.date << " " << beh.count << ")";
}

BehaviorDomain::SubjectStats
BehaviorDomain::
getSubjectStatsFields(SH subjectHash, int fields) const
{
    SubjectStats result = getSubjectStats(subjectHash,
                                          fields & SS_DISTINCT_BEHAVIORS,
                                          fields & SS_DISTINCT_TIMESTAMPS);

    if ((fields & SS_ID)
        && !result.id.notNull())
        result.id = getSubjectId(subjectHash);
    
    return result;
}

SH
BehaviorDomain::
getMaxSubjectForLimit(int limit) const
{
    auto hashes = allSubjectHashes(SH::max(), true /* sorted */);

    if (limit >= hashes.size())
        return SH::max();

    return hashes[limit];
}

bool
BehaviorDomain::
forEachBehaviorGetSubjects(const OnBehaviorSubjects & onBehavior,
                            const BehaviorFilter & filter,
                            int statsFields,
                            const SubjectFilter & subjects,
                            Order order,
                            Parallelism parallelism) const
{
    Lightweight_Hash_Set<SH> keepSubs;
    bool passAllSubs = true;

    // Create a list of viable subjects to filter with
    if (!subjects.passAll()) {
        auto allSubs = allSubjectHashes(subjects.maxSubject, false /* sorted */);

        int subStatsFields = subjects.requiredStatsFields();

        for (auto s: allSubs) {
            if (subjects.fail(s)) {
                passAllSubs = false;
                continue;
            }

            if (subStatsFields) {
                SubjectStats stats = getSubjectStats(s, subStatsFields);

                if (!subjects.pass(s, stats)) {
                    passAllSubs = false;
                    continue;
                }
            }

            keepSubs.insert(s);
        }
    }
    
    cerr << "passAllSubs = " << passAllSubs
         << " keepSubs.size() = " << keepSubs.size()
         << " subjectCount = " << subjectCount()
         << endl;

    // Now we can process each behavior in parallel
    auto onBeh2 = [&] (BH beh, const BehaviorIterInfo & info,
                       const BehaviorStats & stats)
        {
            auto subs = getSubjectHashes(beh, subjects.maxSubject, order != ANYORDER);
            if (!passAllSubs) {
                subs.erase(std::partition(subs.begin(), subs.end(),
                                          [&] (SH sh) { return keepSubs.count(sh); }));
            }

            return onBehavior(beh, info, stats, std::move(subs));
        };

    return forEachBehaviorParallel(onBeh2, filter, statsFields, parallelism);
}

std::vector<MLDB::Path>
BehaviorDomain::
allSubjectPath(ssize_t start /*= 0*/, ssize_t limit /*= -1*/) const
{
    auto subs = allSubjectHashes(SH::max(), true /* sorted */);

    if (limit == -1)
        limit = subs.size();

    size_t upperbound = std::min((size_t)(start + limit), subs.size());
    ssize_t numToGet = upperbound - start;
    std::vector<MLDB::Path> result;
    if (numToGet > 0)
    {
        if (numToGet < 10000) {
            result.reserve(numToGet);
            for (size_t i = start;  i < upperbound;  ++i)
                result.emplace_back(MLDB::behaviors::toPathElement(getSubjectId(subs[i])));
        }
        else {

            auto doGetSubjectId = [&] (size_t x0, size_t x1)
            {
                for (size_t i = x0;  i < x1;  ++i)
                    result[i-start] = MLDB::behaviors::toPathElement(getSubjectId(subs[i]));
                return true;
            };

            result.resize(numToGet);

            parallelMapChunked(start, upperbound, 1000, doGetSubjectId);

        }
    }

    return result;
}

#if 0
template<>
struct DefaultDescription<BehaviorDomain> :
    public PureValueDescription<BehaviorDomain> {
};
template ValueDescriptionT<BehaviorDomain>;
#endif

} // namespace MLDB
