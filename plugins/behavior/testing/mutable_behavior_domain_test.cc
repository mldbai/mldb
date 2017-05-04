/* behavior_domain_test.cc
   Jeremy Barnes, 9 June 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test of the behavior domain classes.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/plugins/behavior/mutable_behavior_domain.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/ml/jml/thread_context.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/arch/demangle.h"
#include "mldb/types/jml_serialization.h"

using namespace std;
using namespace MLDB;
using namespace ML;

int nToRecord = 0;
int nSubjects = 0;
int nBehaviors = 0;

#include "behavior_test_utils.h"

struct ZipfDistribution {

    ZipfDistribution()
    {
    }

    ZipfDistribution(int n, double alpha = 1.0)
    {
        init(n, alpha);
    }

    void init(int n, double alpha = 1.0)
    {
        // Get a zipf distributed cfd that we can binary search
        distribution<double> pdf(n);
        for (unsigned i = 0;  i < n;  ++i)
            pdf[i] = 1.0 / pow(i + 1.0, alpha);
        pdf.normalize();

        //cerr << "pdf = " << pdf << endl;

        distribution<double> cdf(n);
        double total = 0;
        for (unsigned i = 0;  i < n;  ++i) {
            total = cdf[i] = total + pdf[i];
        }

        //cerr << "cdf = " << cdf << endl;

        this->cdf.swap(cdf);
        this->total = total;
    }

    int sample(double r) const
    {
        ExcAssertGreaterEqual(r, 0.0);
        ExcAssertLessEqual(r, 1.0);
        ExcAssert(!cdf.empty());

        int res = std::lower_bound(cdf.begin(), cdf.end(), r)
            - cdf.begin();
        res = std::min<int>(res, cdf.size() - 1);

        //cerr << "r = " << r << " cdf.size() = " << cdf.size()
        //     << " total = " << total << " cdf.back() = "
        //     << cdf.back() << " cdf.front() = " << cdf.front()
        //     << " res = " << res << endl;

        return res;
    }

    distribution<double> cdf;
    double total;
};

BOOST_AUTO_TEST_CASE( test_data_node )
{
    cerr << "Testing data node" << endl;

    MutableBehaviorDomain::SI2 maxSubj;
    maxSubj.bits = 20000;


    auto * node = 
        MutableBehaviorDomain::BehaviorEntry::DataNode
        ::allocate(10000, 0, 1000, maxSubj);

    BOOST_CHECK_EQUAL(node->size(), 0);
    BOOST_CHECK_GE(node->capacity(), 10000);
    BOOST_CHECK_EQUAL(node->firstNotDone(), 0);

    MutableBehaviorDomain::SI2 subj;
    subj.bits = 123;

    BOOST_CHECK_EQUAL(node->atomicRecord(subj, 123), 0);

    BOOST_CHECK_EQUAL(node->size(), 1);
    BOOST_CHECK_GE(node->capacity(), 10000);
    BOOST_CHECK_EQUAL(node->firstNotDone(), 1);

    MutableBehaviorDomain::BehaviorEntry::SubjectEntry entry = (*node)[0];

    BOOST_CHECK_EQUAL(entry.subj.bits, subj.bits);
    BOOST_CHECK_EQUAL(entry.tsOfs, 123);

    auto * node2 = 
        MutableBehaviorDomain::BehaviorEntry::DataNode
        ::allocate(10000, 0, 1000, maxSubj, &entry, (&entry) + 1);

    BOOST_CHECK_EQUAL(node2->size(), 1);
    BOOST_CHECK_GE(node2->capacity(), 10000);
    BOOST_CHECK_EQUAL(node2->firstNotDone(), 1);

    entry = (*node2)[0];

    BOOST_CHECK_EQUAL(entry.subj.bits, subj.bits);
    BOOST_CHECK_EQUAL(entry.tsOfs, 123);
    cerr << "Done testing data node" << endl;


    MutableBehaviorDomain::BehaviorEntry::SubjectEntry entries[100];
    for (unsigned i = 0;  i < 100;  ++i) {
        entries[i].subj.bits = i;
        entries[i].tsOfs = i * 2;
    }

    auto * node3 = 
        MutableBehaviorDomain::BehaviorEntry::DataNode
        ::allocate(10000, 0, 1000, maxSubj, entries, entries + 100);

    for (unsigned i = 0;  i < 100;  ++i) {
        auto entry = (*node3)[i];
        BOOST_CHECK_EQUAL(entry.subj.bits, i);
        BOOST_CHECK_EQUAL(entry.tsOfs, i * 2);
    }
}

#if 1
BOOST_AUTO_TEST_CASE( test_subject_info_sort1 )
{
    MutableBehaviorDomain behs;

    MutableBehaviorDomain::profile = false;

#if 1
    int numBehs = 50000;
    int numSubj = 100000;
    int numIterations = 50000;
    int numThreads = 16;
#else
    int numBehs = 1000;
    int numSubj = 10000;
    int numIterations = 5000;
    int numThreads = 1;
#endif

    double alpha = 1;
    double subjAlpha = 0.5;

    ZipfDistribution behDist(numBehs, alpha);
    ZipfDistribution subjDist(numSubj, subjAlpha);

    std::atomic<uint64_t> subjDone(0), behsDone(0);

    auto testThread = [&] (int threadNum)
        {
            ML::Thread_Context context;
            context.seed(threadNum + 1000);

            for (unsigned i = 0;  i < numIterations;  ++i) {

                int numEntries = context.random() % 50 + 1;

                MutableBehaviorDomain::ManyEntryInt entries[numEntries];
            
                Date tsBase = Date(2014, 1, 1);

                int subj = subjDist.sample(context.random01()) + 1;

                for (int i = 0;  i < numEntries;  ++i) {
                    MutableBehaviorDomain::ManyEntryInt & entry = entries[i];

                    entry.behavior = behDist.sample(context.random01()) + 1;
                    entry.timestamp = tsBase.plusSeconds(context.random() % 86400);
                    entry.count = 1;
                }

                behs.recordMany(Id(subj), entries, numEntries);

                ++subjDone;
                behsDone += numEntries;
            }
        };

    Date start = Date::now();

    std::vector<std::unique_ptr<std::thread> > threads;
    for (unsigned i = 0;  i < numThreads;  ++i) {
        threads.emplace_back(new std::thread([=] () { testThread(i); }));
    }


    for (unsigned i = 0;  i < numThreads;  ++i)
        threads[i]->join();

    Date done = Date::now();
    double elapsed = done.secondsSince(start);

    cerr << "recorded " << behsDone << " events over "
         << subjDone << " total event records in " << elapsed
         << " seconds at " << subjDone / elapsed << " records/second and "
         << behsDone / elapsed << " events/second" << endl;
    
    cerr << "behs had " << behs.subjectCount() << " subjects and "
         << behs.behaviorCount() << " behaviors" << endl;

    cerr << behs.getMemoryStats() << endl;

    cerr << "records   " << MutableBehaviorDomain::records << endl;
    cerr << "  tries   " << MutableBehaviorDomain::recordTries << endl;
    cerr << "    immed " << MutableBehaviorDomain::recordsImmediate << endl;
    cerr << "    spins " << MutableBehaviorDomain::recordSpins << endl;
    cerr << "    width " << MutableBehaviorDomain::recordsWrongWidth << endl;
    cerr << "    empty " << MutableBehaviorDomain::recordsEmpty << endl;
    cerr << "    full  " << MutableBehaviorDomain::recordsNoSpace << endl;
    cerr << "    earts " << MutableBehaviorDomain::recordsEarlierTimestamp << endl;

    behs.makeImmutable();

    cerr << behs.getMemoryStats() << endl;

    filter_ostream stream("/dev/null");
    DB::Store_Writer writer(stream);
    behs.serialize(writer);

    //testIntegrity(behs);
}
#endif

BOOST_AUTO_TEST_CASE( test_recordManySubjects )
{
    MutableBehaviorDomain behs;

    MutableBehaviorDomain::profile = false;

    int numBehs = 50000;
    int numSubj = 100000;
    // int numBehs = 100000;
    // int numSubj = 50000;
    int numIterations = 50000;
    int numThreads = 16;

    double alpha = 1;
    double subjAlpha = 0.5;

    ZipfDistribution behDist(numBehs, alpha);
    ZipfDistribution subjDist(numSubj, subjAlpha);

    std::atomic<uint64_t> subjDone(0), behsDone(0);


    auto testThread = [&] (int threadNum)
        {
            ML::Thread_Context context;
            context.seed(threadNum + 1000);

            for (unsigned i = 0;  i < numIterations;  ++i) {

                int numEntries = context.random() % 50 + 1;

                MutableBehaviorDomain::ManySubjectId entries[numEntries];
            
                Date tsBase = Date(2014, 1, 1);

                Id beh(behDist.sample(context.random01()) + 1);

                for (int i = 0;  i < numEntries;  ++i) {
                    MutableBehaviorDomain::ManySubjectId & entry = entries[i];

                    entry.subject = Id(subjDist.sample(context.random01()) + 1);
                    entry.timestamp = tsBase.plusSeconds(context.random() % 86400);
                    entry.count = 1;
                }

                behs.recordManySubjects(beh, entries, numEntries);

                ++behsDone;
                subjDone += numEntries;
            }
        };

    Date start = Date::now();

    std::vector<std::unique_ptr<std::thread> > threads;
    for (unsigned i = 0;  i < numThreads;  ++i) {
        threads.emplace_back(new std::thread([=] () { testThread(i); }));
    }


    for (unsigned i = 0;  i < numThreads;  ++i)
        threads[i]->join();

    Date done = Date::now();
    double elapsed = done.secondsSince(start);

    cerr << "recorded " << subjDone << " events over "
         << behsDone << " total event records in " << elapsed
         << " seconds at " << behsDone / elapsed << " records/second and "
         << subjDone / elapsed << " events/second" << endl;
    
    cerr << "behs had " << behs.subjectCount() << " subjects and "
         << behs.behaviorCount() << " behaviors" << endl;

    cerr << behs.getMemoryStats() << endl;

    cerr << "records   " << MutableBehaviorDomain::records << endl;
    cerr << "  tries   " << MutableBehaviorDomain::recordTries << endl;
    cerr << "    immed " << MutableBehaviorDomain::recordsImmediate << endl;
    cerr << "    spins " << MutableBehaviorDomain::recordSpins << endl;
    cerr << "    width " << MutableBehaviorDomain::recordsWrongWidth << endl;
    cerr << "    empty " << MutableBehaviorDomain::recordsEmpty << endl;
    cerr << "    full  " << MutableBehaviorDomain::recordsNoSpace << endl;
    cerr << "    earts " << MutableBehaviorDomain::recordsEarlierTimestamp << endl;

    behs.makeImmutable();

    cerr << behs.getMemoryStats() << endl;

    filter_ostream stream("/dev/null");
    DB::Store_Writer writer(stream);
    behs.serialize(writer);

    // Commented out the following due to length of execution
    // testIntegrity(behs);
}


/* ensure that allocations request > 2Gb do not throw or segfault, due to
 * signedness issues */
BOOST_AUTO_TEST_CASE( test_DataNode_allocate )
{
    bool caughtBadAlloc(false);
    try {
         MutableBehaviorDomain::BehaviorEntry::DataNode::allocate(43832649,
                                                                    1409184000, 1411948799,
                                                                    MutableBehaviorDomain::SI2(957913941, 411233238));
    }
    catch (const std::bad_alloc & exc) {
        caughtBadAlloc = true;
    }
    BOOST_CHECK_EQUAL(caughtBadAlloc, false);
}


/* Ensure that multiple behaviors occurring at the same time for the same
 * subject will cause numDistinctTimestamps to return 1 */
BOOST_AUTO_TEST_CASE( test_numDistinctTimestamps )
{
    Id subjectId("mySubject");
    Id behId1("myBeh1");
    Id behId2("myBeh2");

    MutableBehaviorDomain dom;
    Date ts = Date::now();
    dom.recordId(subjectId, behId1, ts);
    dom.recordId(subjectId, behId2, ts);

    SH sh = dom.subjectIdToHash(subjectId);
    BOOST_CHECK_EQUAL(dom.numDistinctTimestamps(sh), 1);

    dom.recordId(subjectId, behId2, ts.plusSeconds(5));
    BOOST_CHECK_EQUAL(dom.numDistinctTimestamps(sh), 2);
}
