// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** das_behavior_test.cc                                 -*- C++ -*-
    Rémi Attab, 22 Oct 2012
    Copyright (c) 2012 mldb.ai inc.  All rights reserved.

    Tests for teh DasDB backed behavior domain. This component requires more
    testing because its writes are more complicated and the 3-way trie merge
    isn't entirely stable yet.

*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "behavior/das_behavior_domain.h"
#include "behavior/das_behavior_domain_writer.h"
#include "mldb/utils/testing/print_utils.h"
#include "mmap/mmap_trie.h"
#include "mmap/mmap_trie_merge.h"
#include "mmap/sync_stream.h"
#include "mmap/testing/mmap_test.h"
#include "mldb/arch/timers.h"

#include <boost/test/unit_test.hpp>

using namespace std;
using namespace ML;
using namespace MLDB;
using namespace MLDB::MMap;


struct DasBehaviorFixture : public MMap::MMapFileFixture
{
    DasBehaviorFixture() : MMapFileFixture("das_behavior") {}
    virtual ~DasBehaviorFixture() {}
};


typedef std::pair<uint64_t, uint64_t> Range;

uint64_t rnd(const Range& r)
{
    return random() % (r.second - r.first) + r.first + 1;
}

void step(Range& r, uint64_t step)
{
    r.first += step;
    r.second += step;
}


void summary(uint64_t entries, double elapsed)
{
    cerr << "Inserted " << printValue(entries)
        << " in " << printElapsed(elapsed) << endl;
    cerr << "Throughput of " << printValue(entries / elapsed)
        << " Latency of " << printElapsed(elapsed / entries)
        << endl;
}

template<typename Domain>
void doRecord(
        Domain& dom,
        const Range& sbjRange,
        const Range& behRange,
        const Range& timeRange)
{
    Id sbj(rnd(sbjRange));
    Id beh(rnd(behRange));
    uint64_t count = random() % 10 + 1;

    static Date start(2012, 01, 01, 00, 00, 00);
    Date ts = start.plusSeconds(rnd(timeRange));

    dom.record(sbj, beh, ts, count);
}

template<typename Domain>
void
doRecord(
        Domain& dom,
        int entries, int sbjRange, int behRange, int timeRange,
        bool doSummary = true)
{

    Timer tm;

    for (size_t i = 0; i < entries; ++i) {
        if (i % 1000 == 0) cerr << "\r" << (i/1000) << "k keys";
        doRecord(dom, {0, sbjRange}, {0, behRange}, {0, timeRange});
    }
    cerr << endl;

    double elapsed = tm.elapsed_wall();
    if (doSummary) summary(entries, elapsed);
}

void
verify(DasBehaviorDomain& dom, bool summary = true)
{
    auto version = dom.current();

    Timer tm;

    version.finish();
    double elapsedFinish = tm.elapsed_wall();

    version.verify();
    double elapsedVerify = tm.elapsed_wall() - elapsedFinish;

    if (summary) {
        cerr << "Generating BI index took " << printElapsed(elapsedFinish) << endl;
        cerr << "Verifying domain took " << printElapsed(elapsedVerify) << endl;
    }
}

#if 0

BOOST_FIXTURE_TEST_CASE( test_writer, DasBehaviorFixture )
{
    enum {
        Entries    = 1000000,
        Subjects   = 1000,
        Behaviors = 1000,
        TimeRange  = 3600,

        InsertNoise = true,
    };

    DasBehaviorDomain dom(filename);

    if (InsertNoise) {

        cerr << endl
            << "Noise ========================================================="
            << endl;

        auto tx = dom.transaction();
        doRecord(tx, Entries, Subjects, Behaviors, TimeRange);
        tx.commit();
        verify(dom);
    }

    cerr << endl
        << "Threaded Test ================================================="
        << endl;

    // We expand the key space so that we get new entries.
    // Behaviors' key space is lower because we see new one of these less often
    {
        DasBehaviorDomainWriter writer(dom);
        writer.start();

        Timer tm;

        doRecord(writer, Entries*4, Subjects*4, Behaviors*2, TimeRange*4, false);

        typedef DasBehaviorDomainWriter::WorkerStats WorkerStats;
        WorkerStats sum;
        uint64_t i = 0;
        do {
            auto stats = writer.getStats();
            sum = accumulate(stats.begin(), stats.end(), WorkerStats());
            if (++i % 1000 == 0) cerr << "\r" << (sum.records/1000) << "k keys";
        } while(sum.records < Entries * 4);
        cerr << endl;

        double idlePct = (double)MMap::mergeIdleTime /
            (MMap::mergeActiveTime + MMap::mergeIdleTime);

        cerr << "Merge: "
            << "idle%="   << idlePct
            << ", diff=" << MMap::Merge::dbg_mergeDiffTime
            << ", ins="  << MMap::Merge::dbg_mergeInsertTime
            << ", rmv="  << MMap::Merge::dbg_mergeRemoveTime
            << ", cnt="  << MMap::Merge::dbg_mergeCount
            << endl;

        double elapsed = tm.elapsed_wall();
        summary(Entries * 4, elapsed);
        cerr << "Commited " << sum.commits << " times" << endl;

        auto stats = writer.getStats();
        for (size_t i = 0; i < stats.size(); ++i) {
            double total = stats[i].idleSec + stats[i].activeSec;
            double idlePct = stats[i].idleSec / total;
            double commitPct = stats[i].commitSec / stats[i].activeSec;
            double commitAvg = stats[i].commitSec / stats[i].commits;

            cerr << "\t" << i << ":"
                << " records=" << stats[i].records
                << ", commits=" << stats[i].commits
                << ", idle%=" << idlePct
                << ", commit%=" << commitPct
                << ", commigAvgSec=" << commitAvg
                << endl;
        }

        writer.shutdown();

        verify(dom);
    }
}

#else

BOOST_FIXTURE_TEST_CASE( test_stream, DasBehaviorFixture )
{
    enum {
        QueueMin   = 5000,
        QueueMax   = 10000,

        WarmupMs   = 10   * 1000,
        DurationMs = 100  * 1000,

        SbjRange   = 1000,
        SbjStep    = 100,
        BehRange   = 1000,
        BehStep    = 50,
        TsRange    = 3600,
        TsStep     = 360,

        ReadThreads = 8,
    };

    typedef DasBehaviorDomainWriter::WorkerStats WorkerStats;

    cerr << endl
        << "Stream Test ==================================================="
        << endl;

    DasBehaviorDomain dom(filename);
    DasBehaviorDomainWriter writer(dom);
    writer.setSnapshotFrequency(-1.0);
    writer.start();

    Date begin = Date::now();

    Date start = begin;
    start.addSeconds(WarmupMs / 1000.0);

    Date end = begin;
    end.addSeconds(DurationMs / 1000.0);

    volatile uint64_t sent = 0;
    volatile uint64_t checks = 0;
    volatile uint64_t errors = 0;
    volatile uint64_t maxSbj = 0;

    array<uint64_t, ReadThreads> reads;
    fill(reads.begin(), reads.end(), 0ULL);

    auto checkTh = [&] (int id) -> int {
        Date now;
        while((now = Date::now()) < end) {
            checks++;
            try { verify(dom, false); }
            catch(const std::exception& ex) {
                errors++;
                sync_cerr() << "ERROR: " << ex.what() << endl << sync_dump;
                return 1;
            }
        }
        return 0;
    };

    auto readerTh = [&] (int id) -> int {
        while (maxSbj == 0); // Not ideal but meh...

        Date now;
        while((now = Date::now()) < end && !errors) {
            uint64_t max = maxSbj;
            auto version = dom.current();

            for (int i = 0; i < 100; ++i) {
                SH sh = { Id(rnd({0, max})) };
                reads[id]++;
                if (!version.knownSubject(sh)) continue;

                auto onSbjBeh = [&](BH bh, Date ts, uint64_t value) -> bool {
                    auto stats = version.getBehaviorStats(bh, 0);
                    reads[id]++;
                    return true;
                };

                version.forEachSubjectBehaviorHash(sh, onSbjBeh);
                reads[id]++;
            }
        }
        return 0;
    };

    auto feederTh = [&] (int id) -> int {
        Range sbjRange { 0, SbjRange };
        Range behRange { 0, BehRange };
        Range tsRange  { 0, TsRange };

        Date now;
        while((now = Date::now()) < end && !errors) {

            // Fetch stats.
            auto stats = writer.getStats();
            WorkerStats sumStats =
                accumulate(stats.begin(), stats.end(), WorkerStats());

            // Fill up the queue.
            uint64_t queueSize = sent - sumStats.records;
            if (queueSize < QueueMin) {
                uint64_t toSend = QueueMax - queueSize;

                for (uint64_t i = 0; i < toSend; ++i) {
                    doRecord(writer, sbjRange, behRange, tsRange);
                    sent++;
                }

                step(sbjRange, SbjStep);
                step(behRange, BehStep);
                step(tsRange, TsStep);

                maxSbj = sbjRange.second;
            }
        }
        return 0;
    };

    auto statsTh = [&] (int id) -> int {
        bool started = false;
        WorkerStats baseStats;
        uint64_t checkBase = 0;
        uint64_t baseReads = 0;

        Date now;
        while((now = Date::now()) < end && !errors) {

            auto stats = writer.getStats();
            WorkerStats sumStats =
                accumulate(stats.begin(), stats.end(), WorkerStats());

            uint64_t totalReads = accumulate(reads.begin(), reads.end(), 0ULL);

            if (now >= start && !started) {
                started = true;
                baseStats = sumStats;
                checkBase = checks;
                baseReads = totalReads;
            }

            uint64_t queueSize = sent - sumStats.records;
            uint64_t writeThroughput = 0;
            double writeLatency = 0;
            uint64_t readThroughput = 0;
            double readLatency = 0;
            uint64_t effChecks = 0;


            double elapsed = now.secondsSince(begin);
            double commitAvg = 0.0;
            double commitLockAvg = 0.0;

            double idlePct =
                (double)mergeIdleTime / (mergeActiveTime + mergeIdleTime);

            double commitFailPct = 0.0;

            if (now >= start) {
                double time = elapsed - (WarmupMs / 1000.0);

                uint64_t writeDiff = sumStats.records - baseStats.records;
                writeThroughput = writeDiff / time;
                writeLatency = time / writeDiff;

                uint64_t readDiff = totalReads - baseReads;
                readThroughput = readDiff / time;
                readLatency = time / readDiff;

                effChecks = checks - checkBase;

                uint64_t commits = sumStats.commits - baseStats.commits;
                uint64_t failed = sumStats.commitFailed - baseStats.commitFailed;
                if (max(failed, commits) > 0)
                    commitFailPct = (double)failed / (commits + failed);

                commitAvg = (sumStats.commitSec - baseStats.commitSec) /
                    stats.size();
                commitLockAvg =
                    (sumStats.commitLockSec - baseStats.commitLockSec)
                    / commits;
            }

            (void) idlePct;
            (void) queueSize;

            sync_cerr() << "\r" << fixed
                << "t="          << printElapsed(elapsed)
                // << " | q="    << printValue(queueSize)
                << " | wc="      << DasBehaviorDomainWriter::Partitions
                << " | w="       << printValue(sumStats.records)
                << " | w/s="     << printValue(writeThroughput)
                // << " | ns/w="    << printElapsed(writeLatency)
                << " | rc="      << ReadThreads
                << " | r="       << printValue(totalReads)
                << " | r/s="     << printValue(readThroughput)
                // << " | ns/r="    << printElapsed(readLatency)
                << " | cc="      << printValue(sumStats.commits)
                // << " | cf="   << printValue(sumStats.commitFailed)
                << " | ct="      << printElapsed(commitAvg)
                << " | cl="      << printElapsed(commitLockAvg)
                // << " | idl%=" << printPct(idlePct)
                << " |" << sync_dump;

            this_thread::sleep_for(chrono::milliseconds(100));
        }
        cerr << endl;
        return 0;
    };

    ThreadedTest test;
    test.start(checkTh, 1, 0);
    test.start(readerTh, ReadThreads, 1);
    test.start(feederTh, 1, 2);
    test.start(statsTh, 1, 3);

    this_thread::sleep_for(chrono::milliseconds(DurationMs));

    int errSum = test.joinAll(1000000);
    BOOST_CHECK_EQUAL(errSum, 0);

    verify(dom);
}

#endif
