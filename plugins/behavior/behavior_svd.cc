// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** behavior_svd.cc
    Jeremy Barnes, 7 May 2012
    Copyright (c) 2012 mldb.ai inc.  All rights reserved.

    Implementation of SVD for behaviors.
*/

#include "behavior_svd.h"
#include "mldb/ext/svdlibc/svdlib.h"
#include "mldb/base/parallel.h"
#include "mldb/jml/utils/vector_utils.h"
#include <boost/thread/thread.hpp>
#include "mldb/jml/db/persistent.h"
#include "mldb/jml/stats/distribution_simd.h"
#include "mldb/jml/stats/distribution_ops.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/arch/backtrace.h"
#include "mldb/arch/timers.h"
#include "mldb/arch/spinlock.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/types/value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/jml_serialization.h"
#include "mldb/base/scope.h"
#include <mutex>
#include <thread>
#include <unordered_map>


using namespace std;
using namespace ML;

namespace MLDB {

extern __thread bool debugMergedBehaviors;

/*****************************************************************************/
/* BEHAVIOR SVD                                                             */
/*****************************************************************************/

BehaviorSvd::
BehaviorSvd(SH maxSubject, int numDenseBehaviors,
             int numSingularValues,
             const std::vector<BH> & biasedBehaviors,
             SvdSpace space,
             bool calcLongTail)
    : maxSubject(maxSubject),
      numDenseBehaviors(numDenseBehaviors),
      numSingularValues(numSingularValues),
      biasedBehaviors(biasedBehaviors.begin(), biasedBehaviors.end()),
      space(space), calcLongTail(calcLongTail)
{
}

BehaviorSvd::
BehaviorSvd(const std::string & filename)
{
    load(filename);
}

void
BehaviorSvd::
partitionBehaviors(const BehaviorDomain & behs)
{
    int64_t nbehs = behs.behaviorCount();

    allBehaviors.clear();
    allBehaviors.resize(nbehs);

    std::vector<std::pair<BH, size_t> > subjectCounts;
    subjectCounts.reserve(nbehs);

    auto onBeh = [&] (BH beh, const BehaviorIterInfo & info,
                      const BehaviorDomain::BehaviorStats & stats)
        {
            size_t nSubj = 0;
            int i = info.index;
            ExcAssertGreaterEqual(i, 0);

            if (!biasedBehaviors.count(beh))
                nSubj = stats.subjectCount;
            subjectCounts.emplace_back(beh, nSubj);
            behaviorIndex[beh] = i;

            ExcAssertEqual(allBehaviors.at(i), BH());
            allBehaviors.at(i) = beh;
            return true;
        };

    behs.forEachBehaviorParallel(onBeh,
                                  BehaviorDomain::ALL_BEHAVIORS,
                                  BehaviorDomain::BS_SUBJECT_COUNT,
                                  SERIAL);

    for (unsigned i = 0;  i < allBehaviors.size();  ++i) {
        if (!allBehaviors[i])
            throw MLDB::Exception("Behavior %d not filled in", i);
    }
    
#if 0

    allBehaviors = behs.allBehaviorHashes();

    int nbehs = allBehaviors.size();
    std::vector<std::pair<BH, size_t> > subjectCounts;
    subjectCounts.reserve(nbehs);

    for (unsigned i = 0;  i < nbehs;  ++i) {
        BH beh = allBehaviors[i];
        size_t nSubj = 0;
        if (!biasedBehaviors.count(beh))
            nSubj = behs.getBehaviorSubjectCount(beh, maxSubject);
        subjectCounts.push_back(make_pair(beh, nSubj));
        behaviorIndex[beh] = i;
    }
#endif

    ML::sort_on_second_descending(subjectCounts);

    numDenseBehaviors = std::min<int64_t>(numDenseBehaviors, nbehs);

    denseBehaviors.clear();
    for (unsigned i = 0;  i < numDenseBehaviors;  ++i)
        denseBehaviors.push_back(subjectCounts[i].first);

    sparseBehaviors.clear();
    for (unsigned i = numDenseBehaviors;  i < nbehs;  ++i)
        sparseBehaviors.push_back(subjectCounts[i].first);

    singularVectors.resize(allBehaviors.size());
}

void
BehaviorSvd::
train(const BehaviorDomain & behs,
      std::function<bool (const Json::Value &)> onProgress)
{
    if (numSingularValues > behs.behaviorCount())
        throw MLDB::Exception("more singular values than behaviors");

    Date start = Date::now();

    auto finishedPhase = [&] (const char * phase)
        {
            Date now = Date::now();
            cerr << "finished phase " << phase << " in "
                 << now.secondsSince(start) << " seconds" << endl;
            start = now;

            if (onProgress) {
                Json::Value progress;
                progress["finishedPhase"] = phase;
                progress["elapsedSeconds"] = now.secondsSince(start);

                if (!onProgress(progress))
                    return false;
            }

            return true;
        };

    if (!finishedPhase("beginning"))
        return;
    
    partitionBehaviors(behs);

    if (!finishedPhase("partitionBehaviors"))
        return;

    BehaviorCache cache;
    updateBehaviorCache(behs, maxSubject, cache);

    if (!finishedPhase("updateBehaviorCache"))
        return;

    calcDenseCointersections(behs, cache);

    if (!finishedPhase("denseCointersections"))
        return;

    calcDenseSvd();

    if (!finishedPhase("denseSvd"))
        return;

    calcLongTailVectors(behs, cache);

    finishedPhase("longTailVectors");
}

float
BehaviorSvd::
calcOverlap(BH i, BH j, const BehaviorDomain & behs) const
{
    // Don't let biased behaviors contribute
    if (biasedBehaviors.count(i) || biasedBehaviors.count(j))
        return 0.0;

    switch (space) {
    case HAMMING: {
        uint32_t inter = behs.coIterateBehaviors(i, j, maxSubject);

#if 0
        auto subs1 = behs.getSubjectHashes(i);
        auto subs2 = behs.getSubjectHashes(j);

        std::sort(subs1.begin(), subs1.end());
        std::sort(subs2.begin(), subs2.end());

        vector<SH> all;
        std::set_intersection(subs1.begin(), subs1.end(),
                              subs2.begin(), subs2.end(),
                              back_inserter(all));

        ExcAssertEqual(all.size(), inter);
#endif

        //uint32_t sz1  = subjectCounts(i);
        //uint32_t sz2  
#if 0
        uint32_t inter2 = behs.coIterateBehaviors(j, i, maxSubject);
        if (inter != inter2) {
            using namespace std;
            cerr << "i = " << i << endl;
            cerr << "j = " << j << endl;
            cerr << "inter1 = " << inter << endl;
            cerr << "inter2 = " << inter2 << endl;
        }
        ExcAssertEqual(inter, inter2);
#endif
        return inter;
    }
    case PYTHAGOREAN: {
        // NOTE: this is much, much slower as we have to look each subject
        // up over and over...
        std::vector<BH> behVector = { i, j };
        double result = 0.0;

        // This will be called when there is a subject with both behaviors
        // We go and get the count for each of the behaviors
        auto onOverlap = [&] (SH subject) -> bool
            {
                std::vector<uint32_t> counts
                    = behs.getSubjectBehaviorCounts(subject, behVector);

                //cerr << "got " << counts << endl;

                result += (double)counts[0] * (double)counts[1];
                return true;
            };
        behs.coIterateBehaviors(i, j, maxSubject, onOverlap);

        return result;
    }
    default:
        throw MLDB::Exception("unkown space in Behavior SVD");
    }
}

float
BehaviorSvd::
calcOverlapCached(BH bi,
                  BH bj,
                  const BehaviorCache & cache,
                  const BehaviorDomain & behs) const
{
    if (biasedBehaviors.count(bi) || biasedBehaviors.count(bj))
        return 0.0;

    auto getIndex = [&] (BH beh) -> int
        {
            auto it = cache.behToIndex.find(beh);
            if (it == cache.behToIndex.end())
                return -1;
            return it->second;
        };

    int i1 = getIndex(bi);
    int i2 = getIndex(bj);
    
    if (i1 == -1 || i2 == -1)
        return calcOverlap(bi, bj, behs);

    float res = calcOverlapCached(bi, bj, cache.subjects[i1], cache.subjects[i2], behs);

#if 1
    float resShouldBe = calcOverlap(bi, bj, behs);

    if (res != resShouldBe) {
        cerr << "bi = " << bi << " bj = " << bj << " res " << res
             << " resShouldBe " << resShouldBe << endl;
    }

    //ExcAssertEqual(res, resShouldBe);
#endif

    return res;
}

namespace {

static bool shortCircuit = true;

} // file scope

float
BehaviorSvd::
calcOverlapCached(BH bi,
                  BH bj,
                  const SvdColumnEntry & ei,
                  const SvdColumnEntry & ej,
                  const BehaviorDomain & behs) const
{
    return ei.calcOverlap(ej, space, shortCircuit);
}

void
BehaviorSvd::
updateBehaviorCache(const BehaviorDomain & behs,
                     SH maxSubject,
                     BehaviorCache & cache) const
{
    vector<BH> allBehs;

    if (calcLongTail)
        allBehs = behs.allBehaviorHashes();
    else allBehs = denseBehaviors;

    // Make a dense set of subjects
    //vector<SH> subjects = behs.allSubjectHashes(maxSubject, true /* sorted */);

    //cerr << "got " << subjects.size() << " qualifying subjects" << endl;

#if 0
    // Remove those that have only one behavior

    int numWithOneBeh = 0;
    for (auto s: subjects) {
        auto stats = behs.getSubjectStats(s, true /* needSubjectCount */);
        if (stats.numBehaviors < 2)
            ++numWithOneBeh;

        if (stats.numBehaviors <= 11)
            continue;

        cerr << s << " " << stats.numBehaviors << " " << stats.numDistinctBehaviors << endl;
        
        auto onBeh = [&] (BH beh, Date ts, int count)
            {
                cerr << " " << beh << " " << ts << " " << count << " "
                << behs.getBehaviorId(beh) << endl;
                return true;
            };

        behs.forEachSubjectBehavior(s, onBeh);
    }

    cerr << "numWithOneBeh = " << numWithOneBeh << endl;
#endif

    Timer timer;

    cache.behToIndex.clear();

    for (unsigned i = 0;  i < allBehs.size();  ++i)
        cache.behToIndex[allBehs[i]] = i;

    std::vector<SvdColumnEntry> & entries = cache.subjects;
    entries.clear();  entries.resize(allBehs.size());
    
    // Make a dense set of subjects
    vector<SH> subjects = behs.allSubjectHashes(maxSubject, true /* sorted */);

    Lightweight_Hash<SH, int> indexes;
    int index = 0;
    for (auto & s: subjects)
        indexes[s] = ++index;


    if (space != HAMMING) {

        typedef Spinlock Lock;  // false sharing, it's true, but there are *lots* of locks

        std::vector<Lock> allLocks(allBehs.size());

        // Do it in subject-by-subject as this way we can get the counts.  This needs
        // locking.
        auto onSubject = [&] (SH sh,
                              const BehaviorDomain::SubjectIterInfo & info,
                              const BehaviorDomain::SubjectStats & stats,
                              const std::vector<std::pair<BH, uint32_t> > & counts)
            {
                auto it = indexes.find(sh);
                if (it == indexes.end())
                    throw MLDB::Exception("subject index not found");
                int i = it->second;

                for (auto & c: counts) {
                    BH b = c.first;
                    auto it = cache.behToIndex.find(b);
                    if (it == cache.behToIndex.end()) {
                        continue;
                    }

                    int cnt = c.second;
                    auto & entry = entries.at(it->second);
                
                    std::unique_lock<Lock> guard(allLocks[it->second]);
                    entry.add(i, sh, cnt);
                }

                return true;
            };

        SubjectFilter filter;
        filter.maxSubject = maxSubject;
    
        behs.forEachSubjectGetBehaviorCounts(onSubject,
                                              filter,
                                              BehaviorDomain::ALL_EVENTS,
                                              ANYORDER,
                                              BehaviorDomain::SS_NONE,
                                              PARALLEL);

        cerr << "indexed did " << behs.subjectCount() << " subject indexes"
             << " in " << timer.elapsed() << endl;

        // Now sort them

        auto sortEntry = [&] (int i)
            {
                SvdColumnEntry & e = entries[i];
                e.eligibleRows = subjects.size();
                e.sort();
            };
        
        parallelMap(0, entries.size(), sortEntry);
    
    }
    else {
        // Optimization for hamming space where count isn't important

        // For each behavior, which subjects does it have?
        auto onBehavior = [&] (BH bh, const BehaviorIterInfo & info,
                                const BehaviorStats &,
                                const std::vector<SH> & subs)
            {
                if (info.index % 10000 == 0)
                    cerr << "doing " << info.index << " of " << behs.behaviorCount()
                         << endl;

                //BH bh = allBehs[i];
            
                //auto subs = behs.getSubjectHashes(bh, maxSubject, true /* sorted */);

                SvdColumnEntry & e = entries[cache.behToIndex[bh]];

                for (auto & s: subs) {
                    auto it = indexes.find(s);
                    ExcAssert(it != indexes.end());

                    e.add(indexes[s], s);
                }

                e.eligibleRows = subjects.size();
                ExcAssert(e.bitmap || e.totalRows == 0);
                e.compress();
                return true;
            };

        SubjectFilter subjectFilter;
        subjectFilter.maxSubject = maxSubject;

        BehaviorFilter behFilter;
        behFilter.filterInList(allBehs);

        behs.forEachBehaviorGetSubjects(onBehavior,
                                         behFilter,
                                         BehaviorDomain::BS_NONE,
                                         subjectFilter,
                                         INORDER,
                                         PARALLEL);
    }
}

#if 0
void
BehaviorSvd::
calcDenseCointersectionsHamming(const BehaviorDomain & behs)
{
    denseOverlaps.clear();
    denseOverlaps.resize(numDenseBehaviors,
                         distribution<float>(numDenseBehaviors));
        
    // Calculate the cointersection sizes row by row
    std::atomic<size_t> numNonZero = 0;

    auto doCalcRow = [&] (int i)
        {
            int ngz = 0;

            BH bii = denseBehaviors[i];
            denseOverlaps[i][i] = this->calcOverlap(bii, bii, behs);

            for (unsigned j = 0;  j < i;  ++j) {
                BH bij = denseBehaviors[j];
                    
                float o = this->calcOverlap(bii, bij, behs);
                denseOverlaps[i][j] = denseOverlaps[j][i] = o;
                ngz += o > 0;
            }

            if (i && i % 1000 == 0) {
            cerr << "finished row " << i << " of " << numDenseBehaviors
            << " with " << ngz << " of " << i << "("
            << 100.0 * ngz / i << "%) > 0" << endl;
            }

            numNonZero += ngz * 2 + 1;
        };
    
    parallelMap(0, numDenseBehaviors, doCalcRow);

    cerr << "overall " << numNonZero << " of "
         << numDenseBehaviors * numDenseBehaviors
         << " (" << 100.0 * numNonZero / (numDenseBehaviors * numDenseBehaviors)
         << "%) non-zero entries" << endl;
}
#endif

void
BehaviorSvd::
calcDenseCointersections(const BehaviorDomain & behs,
                         const BehaviorCache & cache)
{
    // 1.  Cache the information
    //
    // For each behavior, get a list of eligible subjects and the
    // subject's count for that behavior.

    denseOverlaps.clear();
    denseOverlaps.resize(numDenseBehaviors,
                         distribution<float>(numDenseBehaviors));

    // Now do the intersections

    // Calculate the cointersection sizes row by row
    std::atomic<size_t> numNonZero(0);

    auto getIndex = [&] (BH beh) -> int
        {
            auto it = cache.behToIndex.find(beh);
            if (it == cache.behToIndex.end())
                throw MLDB::Exception("no behavior found");
            return it->second;
        };

    auto doCalcRow = [&] (int i)
        {
            int ngz = 0;

            BH bii = denseBehaviors[i];

            int iii = getIndex(bii);

            const SvdColumnEntry & ei = cache.subjects[iii];

            for (unsigned j = 0;  j <= i;  ++j) {
                BH bij = denseBehaviors[j];
                int iij = getIndex(bij);
                const SvdColumnEntry & ej = cache.subjects[iij];
                
                float o = calcOverlapCached(bii, bij, ei, ej, behs);

                if (false) {
                    float o2 = calcOverlap(bii, bij, behs);
                    ExcAssertEqual(o, o2);
                }

                denseOverlaps[i][j] = denseOverlaps[j][i] = o;
                ngz += o > 0;
            }

            if (i && i % 1000 == 0) {
            cerr << "finished row " << i << " of " << numDenseBehaviors
            << " with " << ngz << " of " << i << "("
            << 100.0 * ngz / i << "%) > 0" << endl;
           }
            numNonZero += ngz * 2 + 1;
        };
    
    parallelMap(0, numDenseBehaviors, doCalcRow);

    int numDenseTot = numDenseBehaviors * numDenseBehaviors;
    cerr << "overall " << numNonZero << " of "
         << numDenseTot
         << " (" << 100.0 * numNonZero / numDenseTot
         << "%) non-zero entries" << endl;
}

void
BehaviorSvd::
calcDenseSvd()
{
    
    /**************************************************************
     * multiplication of matrix B by vector x, where B = A'A,     *
     * and A is nrow by ncol (nrow >> ncol). Hence, B is of order *
     * n = ncol (y stores product vector).		              *
     **************************************************************/

    auto opb_fn = [&] (double *x, double *y)
    {
        int ncols = numDenseBehaviors;
        for (unsigned i = 0; i != ncols; i++) {
            double yval = 0.0;
            for (unsigned j = 0; j != ncols; j++)
                yval += denseOverlaps[i][j] * x[j];

            y[i] = yval;
        }
    };
    
    SVDParams params;
    params.opb = opb_fn;
    params.ierr = 0;
    params.nrows = numDenseBehaviors;  // we don't really calculate them...
    params.ncols = numDenseBehaviors;
    params.nvals = 0;
    params.doU = false;
    params.calcPrecision(params.ncols);
        

    svdrec * svdResult = svdLAS2A(numSingularValues, params);
    Scope_Exit(svdFreeSVDRec(svdResult));

#if 0
    cerr    << "Vt rows " << svdResult->Vt->rows << endl
            << "Vt cols " << svdResult->Vt->cols << endl;
#endif

    singularValues.resize(numSingularValues);
    std::copy(svdResult->S, svdResult->S + numSingularValues,
              singularValues.begin());

    cerr << "svalues = " << singularValues << endl;

    //cerr << "svdResult->Vt->value = " << svdResult->Vt->value << endl;

    // Extract the singular vectors for the dense behaviors
    for (unsigned i = 0;  i < numDenseBehaviors;  ++i) {
        //cerr << "i = " << i << "svdResult->Vt->value[i] = "
        //     << svdResult->Vt->value[i] << endl;
        distribution<float> & d = singularVectors[i];
        d.resize(numSingularValues);
        for (unsigned j = 0;  j < numSingularValues;  ++j)
            d[j] = svdResult->Vt->value[j][i];

        //std::copy(svdResult->Vt->value[i],
        //          svdResult->Vt->value[i] + numSingularValues,
        //          d.begin());
        denseVectors.push_back(d);
    }

    ExcAssertEqual(denseVectors.size(), numDenseBehaviors);
    ExcAssertEqual(denseBehaviors.size(), numDenseBehaviors);
}

distribution<float>
BehaviorSvd::
calculateBehaviorVectorCached(BH beh, const BehaviorDomain & behs,
                               const BehaviorCache & cache) const
{
    // NOTE: this is loooong... we need to calculate its overlap with
    // every one of the dense basis vectors

    distribution<double> result(numSingularValues, 0.0);

    // Zero vectors for biased ones
    if (biasedBehaviors.count(beh))
        return result.cast<float>();

    for (unsigned i = 0;  i < numDenseBehaviors;  ++i) {
        float ol = calcOverlapCached(beh, denseBehaviors[i], cache, behs);

        //float ol2 = calcOverlap(beh, denseBehaviors[i], behs);
        //ExcAssertEqual(ol, ol2);

        if (!ol)
            continue;
        result += ol * denseVectors[i];
    }

    result /= (singularValues * singularValues);

    return result.cast<float>();
}

distribution<float>
BehaviorSvd::
calculateBehaviorVector(BH beh, const BehaviorDomain & behs) const
{
    // NOTE: this is loooong... we need to calculate its overlap with
    // every one of the dense basis vectors

    distribution<double> result(numSingularValues, 0.0);

    // Zero vectors for biased ones
    if (biasedBehaviors.count(beh))
        return result.cast<float>();

    for (unsigned i = 0;  i < numDenseBehaviors;  ++i) {
        float ol = calcOverlap(beh, denseBehaviors[i], behs);
        if (!ol)
            continue;
        result += ol * denseVectors[i];
    }

    result /= (singularValues * singularValues);

    return result.cast<float>();
}

distribution<float>
BehaviorSvd::
getBehaviorVector(BH beh) const
{
    auto it = behaviorIndex.find(beh);
    if (it == behaviorIndex.end())
        return distribution<float>(numSingularValues);
    int i = it->second;
    return singularVectors[i];
}

const distribution<float> *
BehaviorSvd::
getBehaviorVectorCached(BH beh) const
{
    auto it = behaviorIndex.find(beh);
    if (it == behaviorIndex.end())
        return nullptr;
    int i = it->second;
    return &singularVectors[i];
}

bool
BehaviorSvd::
knownBehaviorHash(BH beh) const
{
    auto it = behaviorIndex.find(beh);
    return !(it == behaviorIndex.end());
}

int
BehaviorSvd::
getNumSingularValues() const
{
    return numSingularValues;
}

distribution<float>
BehaviorSvd::
calculateSubjectVectorForBh(const vector<std::pair<BH, uint32_t>> & behs) const
{
    // Note : Simon did this, use with caution !!
    distribution<double> result(numSingularValues);

    // NOTE: as things currently stand, the overlap function assumes that
    // all counts are zero or one... this should do the same
    for(const std::pair<BH, uint32_t> & beh : behs) {
        auto it = behaviorIndex.find(beh.first);
        if (it == behaviorIndex.end())
            continue;

        int index = it->second;
        result += singularVectors[index] * beh.second;
    };

    // TODO : Think about that
    // Question: shouldn't this be result /= (singularValues * singularValues)?
    // see above...
    result /= singularValues;
    return result.cast<float>();
}

distribution<float>
BehaviorSvd::
calculateSubjectVector(SH subjectHash, const BehaviorDomain & behs) const
{
    return calculateSubjectVectorForBh(
            behs.getSubjectBehaviorCounts(subjectHash, ANYORDER));
}

distribution<float>
BehaviorSvd::
calculateWeightedSubjectVector(const std::vector<std::pair<double, BH> > & behaviors) const
{
    distribution<double> result(numSingularValues);
    for (auto b: behaviors) {
        double w = b.first;
        BH beh = b.second;
        auto it = behaviorIndex.find(beh);
        if (it == behaviorIndex.end())
            continue;
        const distribution<float> & v = singularVectors[it->second];

        ML::SIMD::vec_add(&result[0], w, &v[0], &result[0], numSingularValues);
        // equivalent to result += v * w, but faster as it uses no temporary
    }

    result /= singularValues;
    // Question: isn't this result /= (singularValues * singularValues)?
    // see in calcBehaviorVector and calcLongTailVectors

    return result.cast<float>();
}

void
BehaviorSvd::
calcLongTailVectors(const BehaviorDomain & behs,
                    const BehaviorCache & cache)
{
    cerr << "calculating long-tail vectors for "
         << sparseBehaviors.size() << " behaviors" << endl;

    if (!shortCircuit) {
        // This test fails when short circuiting, as we don't calculate the
        // values exactly.

        cerr << "vector for dense beh 0 = " << denseBehaviors[0] << endl;
        cerr << denseVectors[0] << endl;
        distribution<float> v = calculateBehaviorVectorCached(denseBehaviors[0], behs, cache);
        distribution<float> v2 = calculateBehaviorVector(denseBehaviors[0], behs);

        cerr << "v = " << v << endl;
        cerr << "v2 = " << v2 << endl;
        cerr << "v - v2 = " << v - v2 << endl;

        ExcAssertEqual(abs(v - v2).total(), 0.0);

        cerr << "reconstructed = "
             << v << " two_norm = " << v.two_norm()
             << endl;
    }

    std::atomic<uint64_t> numZeros(0);

    auto calcAndCheckBehavior = [&] (BH beh)
        {
            auto it = behaviorIndex.find(beh);
            if (it == behaviorIndex.end())
                throw MLDB::Exception("couldn't find behavior");
            int i = it->second;

            distribution<float> v = this->calculateBehaviorVectorCached(beh, behs, cache);
            singularVectors[i] = v;

            if (v.two_norm() == 0)
                numZeros += 1;

            if (v.two_norm() == 0 && false) {
                static boost::mutex mutex;
                mutex.lock();

                debugMergedBehaviors = true;

                static std::map<BH, int> denseSet;
                if (denseSet.empty()) {
                    for (unsigned i = 0;  i < denseBehaviors.size();
                         ++i) {
                        denseSet[denseBehaviors[i]] = i;
                    }
                }
                
#if 0
                BehaviorDomain::BehaviorStats st
                    = behs.getBehaviorStats(beh);
                cerr << "beh = " << beh << " two_norm = " << v.two_norm()
                     << " count " << st.count << " subjects "
                     << st.subjectCount
                     << endl;
#endif

                auto subj = behs.getSubjectHashes(beh, maxSubject, false);

                for (auto it = subj.begin(), end = subj.end();
                     it != end;  ++it) {

                    SH sh(*it);

                    if (sh > maxSubject) continue;

                    auto sbehs = behs.getSubjectBehaviorCounts(sh);

                    cerr << "  subject " << sh << " has "
                         << sbehs.size() << " behaviors: " << endl;

                    for (unsigned i = 0;   i < sbehs.size();  ++i) {
                        cerr << "    " << i << " " << sbehs[i].first
                             << " --> " << sbehs[i].second;
                        int index = -1;
                        auto jt = denseSet.find(sbehs[i].first);
                        if (jt != denseSet.end()) {
                            index = jt->second;
                            cerr << " *** (" << index << ")";
                                
                            float ol
                                = this->calcOverlap(beh, denseBehaviors[index],
                                                    behs);
                            cerr << " ol = " << ol;

                            std::vector<SH> subj1
                                = behs.getSubjectHashes(beh, maxSubject,
                                                        true);
                            std::vector<SH> subj2
                                = behs.getSubjectHashes(denseBehaviors[index],
                                                        maxSubject, true);

                            std::set<SH> allSubjects;
                            std::set_intersection(subj1.begin(),
                                                  subj1.end(),
                                                  subj2.begin(),
                                                  subj2.end(),
                                                  inserter(allSubjects,
                                                           allSubjects.end()));
                            cerr << " int = " << allSubjects.size(); 
                            cerr << " cnt = " << allSubjects.count(sh);
                        }
                        cerr << endl;
                    }
                }

                debugMergedBehaviors = false;
                mutex.unlock();
            }

            return v;
        };

    Date denseStart = Date::now();
    Date lastDense = denseStart;
    
    auto calcDenseBehavior = [&] (int i)
        {
            BH beh = denseBehaviors[i];
            auto v = calcAndCheckBehavior(beh);

            if (i % 100 == 0) {
                cerr << "i = " << i << " beh = " << beh
                     << " two_norm = " << v.two_norm() << " "
                     << (i / Date::now().secondsSince(denseStart))
                     << "/s (o) "
                     << (100 / Date::now().secondsSince(lastDense))
                     << "/s (i) "
                     << endl;
                lastDense = Date::now();
            }
        };

    cerr << "done dense behaviors" << endl;

    parallelMap(0, denseBehaviors.size(),
                             calcDenseBehavior);

    if (!calcLongTail)
        return;

    Date sparseStart = Date::now();
    Date lastSparse = sparseStart;

    auto calcSparseBehavior = [&] (int i)
        {
            BH beh = sparseBehaviors[i];
            auto v = calcAndCheckBehavior(beh);

            if (i % 1000000 == 0) {
                cerr << "i = " << i << " beh = " << beh
                     << " two_norm = " << v.two_norm() << " "
                     << (i / Date::now().secondsSince(sparseStart))
                     << "/s (o) "
                     << (1000 / Date::now().secondsSince(lastSparse))
                     << "/s (i) "
                     << endl;
                lastSparse = Date::now();
            }
        };

    cerr << "done sparse behaviors" << endl;

    parallelMap(0, sparseBehaviors.size(),
                             calcSparseBehavior);
    cerr << numZeros << " of " << sparseBehaviors.size() << " are zero"
         << endl;
}

std::pair<std::vector<std::pair<BH, float> >,
          std::vector<std::pair<BH, float> > >
BehaviorSvd::
explainDimension(int dim, int numBehs) const
{
    // Extract the dense values for this beh
    vector<pair<BH, float> > vals;
    for (unsigned i = 0;  i < denseBehaviors.size();  ++i) {
        vals.push_back(make_pair(denseBehaviors[i],
                                 denseVectors[i].at(dim)));
    }

    ML::sort_on_second_descending(vals);

    if (numBehs > vals.size() / 2)
        numBehs = vals.size() / 2;

    vector<pair<BH, float> > pos(vals.begin(), vals.begin() + numBehs);
    vector<pair<BH, float> > neg(vals.end() - numBehs, vals.end());
    
    return make_pair(pos, neg);
}

void
BehaviorSvd::
save(const std::string & filename) const
{
    filter_ostream stream(filename);
    DB::Store_Writer store(stream);
    serialize(store);
}

BYTE_PERSISTENT_ENUM_IMPL(SvdSpace);

void
BehaviorSvd::
serialize(DB::Store_Writer & store) const
{
    string magic = "BehaviorSvdV1";
    store << magic;
    //cerr << "done magic" << endl;
    store << 3; // version
    //cerr << "done version" << endl;
    //cerr << "maxSubject = " << maxSubject << endl;
    store << maxSubject;
    //cerr << "done maxSubject" << endl;
    store << numDenseBehaviors;
    //cerr << "done numDenseBehaviors" << endl;
    store << numSingularValues;
    //cerr << "done numSignularValues" << endl;

    std::map<BH, int> savedIndex(behaviorIndex.begin(), behaviorIndex.end());

    store << allBehaviors
          << savedIndex
          << denseBehaviors
          << sparseBehaviors
          << singularValues
          << singularVectors
          << denseVectors
          << std::vector<uint64_t>(biasedBehaviors.begin(),
                                   biasedBehaviors.end())
          << space;
}

void
BehaviorSvd::
load(const std::string & filename)
{
    filter_istream stream(filename);
    DB::Store_Reader store(stream);
    reconstitute(store);
}

void
BehaviorSvd::
reconstitute(DB::Store_Reader & store)
{
    string magic;
    store >> magic;
    if (magic != "BehaviorSvdV1")
        throw MLDB::Exception("invalid magic on the behavior SVDs");

    //cerr << "got magic " << magic << endl;

    int version;
    store >> version;
    if (version < 1 || version > 3)
        throw MLDB::Exception("unknown behavior SVD version");
    
    //cerr << "got version " << version << endl;

    store >> maxSubject
          >> numDenseBehaviors
          >> numSingularValues
          >> allBehaviors;

    //cerr << "got " << allBehaviors.size() << " behaviors" << endl;

    std::map<BH, int> savedIndex;
    store >> savedIndex;
    behaviorIndex.clear();
    for (auto & i: savedIndex)
        behaviorIndex.insert(i);
    
    //cerr << "got " << behaviorIndex.size() << " index entries" << endl;

    store >> denseBehaviors
          >> sparseBehaviors;

    //cerr << "got " << denseBehaviors.size() << " dense and "
    //     << sparseBehaviors.size() << " sparse behaviors" << endl;

    store >> singularValues
          >> singularVectors
          >> denseVectors;

    vector<BH> biasedBehaviorVec;
    if (version >= 2) {
        store >> biasedBehaviorVec;
    }
    if (version >= 3) {
        store >> space;
    }
    else space = HAMMING;

    biasedBehaviors.clear();
    biasedBehaviors.insert(biasedBehaviors.begin(),
                            biasedBehaviors.end());
}

DEFINE_STRUCTURE_DESCRIPTION(BehaviorSvd);

BehaviorSvdDescription::
BehaviorSvdDescription()
{
}

int64_t
BehaviorSvd::
memusage() const
{
    int64_t result = sizeof(*this);

    result += sizeof(BH) * denseBehaviors.size();
    result += sizeof(BH) * sparseBehaviors.size();
    result += 2 * sizeof(int64_t) * biasedBehaviors.size();
    result += sizeof(float) * singularValues.size();
    result += (sizeof(float) * numSingularValues  + sizeof(distribution<float>) )
        * singularVectors.size();
    result += (sizeof(float) * numSingularValues + sizeof(distribution<float>))
        * denseVectors.size();
    result += sizeof(BH) * allBehaviors.size();

    return result;
}

} // namespace MLDB
