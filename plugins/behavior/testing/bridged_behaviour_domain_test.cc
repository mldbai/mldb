// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* bridged_behavior_domain_test.cc
   Jeremy Barnes, 7 December 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Test for the bridged behavior domain.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <memory>
#include <boost/test/unit_test.hpp>
#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/arch/demangle.h"
#include "mldb/plugins/behavior/mutable_behavior_domain.h"
#include "mldb/plugins/behavior/bridged_behavior_domain.h"

using namespace std;
using namespace MLDB;
using namespace ML;

namespace {

#if 0
    int nToRecord = 1000000;
    int nSubjects = 1000;
    int nBehaviors = 1000;
#elif 1
    int nToRecord = 10000;
    int nSubjects = 500;
    int nBehaviors = 500;
#else
    int nToRecord = 10;
    int nSubjects = 3;
    int nBehaviors = 3;

#endif

} // file scope

#include "behavior_test_utils.h"

#if 0
BOOST_AUTO_TEST_CASE( test_subj_identity_mapping )
{
    // 1.  Record 1 million behaviors
    auto mut = std::make_shared<MutableBehaviorDomain>();
    createTestBehaviors(mut);

    testIntegrity(*mut);

    // Identity mapping
    auto identity = std::make_shared<MutableBehaviorDomain>();
    for (unsigned i = 1;  i <= nSubjects;  ++i) {
        identity->recordId(Id(i), Id(i), Date());
    }
    
    testIntegrity(*identity);

    auto br = std::make_shared<BehaviorSubjectMapping>(identity);

    BridgedBehaviorDomain idBridged(mut, br);

    testIntegrity(idBridged);

    testEquivalent(idBridged, *mut);
}

BOOST_AUTO_TEST_CASE( test_beh_identity_mapping )
{
    // 1.  Record 1 million behaviors
    auto mut = std::make_shared<MutableBehaviorDomain>();
    createTestBehaviors(mut);

    testIntegrity(*mut);

    // Identity mapping
    auto identity = std::make_shared<MutableBehaviorDomain>();
    for (unsigned i = 1;  i <= nSubjects;  ++i) {
        identity->recordId(Id(i), Id(i), Date());
    }
    
    testIntegrity(*identity);

    auto br = std::make_shared<BehaviorBehaviorMapping>(identity);

    BridgedBehaviorDomain idBridged(mut, nullptr, br);

    testIntegrity(idBridged);

    testEquivalent(idBridged, *mut);
}

BOOST_AUTO_TEST_CASE( test_identity_mapping2 )
{
    // 1.  Record 1 million behaviors
    auto mut = std::make_shared<MutableBehaviorDomain>();
    createTestBehaviors(mut);

    testIntegrity(*mut);

    // Identity mapping
    auto identity = std::make_shared<MutableBehaviorDomain>();
    for (unsigned i = 1;  i <= nSubjects;  ++i) {
        identity->recordId(Id(i), Id(i), Date());
    }
    
    testIntegrity(*identity);

    auto sbr = std::make_shared<IdentitySubjectMapping>(mut);
    auto bbr = std::make_shared<IdentityBehaviorMapping>(mut);

    BridgedBehaviorDomain idBridged(mut, sbr, bbr);

    testIntegrity(idBridged);
    
    testEquivalent(idBridged, *mut);
}

BOOST_AUTO_TEST_CASE( test_subj_degenerate_mapping )
{
    // 1.  Record 1 million behaviors
    auto mut = std::make_shared<MutableBehaviorDomain>();
    createTestBehaviors(mut);

    testIntegrity(*mut);

    // Degenerate mapping: everything maps onto 1
    auto degenerate = std::make_shared<MutableBehaviorDomain>();
    for (unsigned i = 1;  i <= nSubjects;  ++i) {
        degenerate->recordId(Id(i), Id("degenerate"), Date());
    }
    
    testIntegrity(*degenerate);

    auto br = std::make_shared<BehaviorSubjectMapping>(degenerate);

    BridgedBehaviorDomain idBridged(mut, br);

    testIntegrity(idBridged);

    BOOST_CHECK_EQUAL(idBridged.subjectCount(), 1);
    auto subj = idBridged.allSubjectHashes();
    BOOST_CHECK_EQUAL(subj.size(), 1);
    BOOST_CHECK_EQUAL(subj[0], SH(Id("degenerate")));
    BOOST_CHECK_EQUAL(idBridged.behaviorCount(), nSubjects);
}
#endif

BOOST_AUTO_TEST_CASE( test_beh_mapping_1 )
{
    // 1.  Record 1 million behaviors
    auto mut = std::make_shared<MutableBehaviorDomain>();
    mut->recordId(Id("user0"), Id(1), Date());
    mut->recordId(Id("user1"), Id(2), Date());
    mut->recordId(Id("user2"), Id(3), Date());
    mut->recordId(Id("user3"), Id(4), Date());

    testIntegrity(*mut);

    // Degenerate mapping: everything maps onto 1
    auto mapping = std::make_shared<MutableBehaviorDomain>();
    mapping->recordId(Id(1), Id("zero"), Date());
    mapping->recordId(Id(2), Id("one"),  Date());
    mapping->recordId(Id(3), Id("two"),  Date());
    mapping->recordId(Id(3), Id("three"),Date());
    
    auto br = std::make_shared<BehaviorBehaviorMapping>(mapping);

    BridgedBehaviorDomain behs(mut, nullptr, br);

    auto dumpUser = [&] (Id id)
        {
            auto onBeh = [&] (BH beh, Date ts, int count)
            {
                cerr << " " << beh << " " << behs.getBehaviorId(beh) << " "
                << ts << " " << count << endl;
                return true;
            };

            cerr << "user " << id << endl;
            behs.forEachSubjectBehavior(id, onBeh);
        };


    BOOST_CHECK_EQUAL(behs.getSubjectStats(Id("user0"), true).numBehaviors, 1);
    BOOST_CHECK_EQUAL(behs.getSubjectStats(Id("user1"), true).numBehaviors, 1);
    BOOST_CHECK_EQUAL(behs.getSubjectStats(Id("user2"), true).numBehaviors, 2);
    BOOST_CHECK_EQUAL(behs.getSubjectStats(Id("user3"), true).numBehaviors, 0);

    BOOST_CHECK_EQUAL(behs.getSubjectStats(Id("user0"), true).numDistinctBehaviors, 1);
    BOOST_CHECK_EQUAL(behs.getSubjectStats(Id("user1"), true).numDistinctBehaviors, 1);
    BOOST_CHECK_EQUAL(behs.getSubjectStats(Id("user2"), true).numDistinctBehaviors, 2);
    BOOST_CHECK_EQUAL(behs.getSubjectStats(Id("user3"), true).numDistinctBehaviors, 0);
    
    int BS_ALL = BehaviorDomain::BS_ALL;

    BOOST_CHECK_EQUAL(behs.getBehaviorStats(Id("zero"), BS_ALL).earliest, Date());
    BOOST_CHECK_EQUAL(behs.getBehaviorStats(Id("zero"), BS_ALL).latest, Date());
    BOOST_CHECK_EQUAL(behs.getBehaviorStats(Id("one"), BS_ALL).earliest, Date());
    BOOST_CHECK_EQUAL(behs.getBehaviorStats(Id("one"), BS_ALL).latest, Date());
    BOOST_CHECK_EQUAL(behs.getBehaviorStats(Id("two"), BS_ALL).earliest, Date());
    BOOST_CHECK_EQUAL(behs.getBehaviorStats(Id("two"), BS_ALL).latest, Date());
    BOOST_CHECK_EQUAL(behs.getBehaviorStats(Id("three"), BS_ALL).earliest, Date());
    BOOST_CHECK_EQUAL(behs.getBehaviorStats(Id("three"), BS_ALL).latest, Date());

    dumpUser(Id("user0"));
    dumpUser(Id("user1"));
    dumpUser(Id("user2"));
    dumpUser(Id("user3"));

    auto allBehs = behs.allBehaviorHashes();
    set<Id> allBehIds;
    for (auto b: allBehs)
        allBehIds.insert(behs.getBehaviorId(b));
    
    BOOST_CHECK_EQUAL(allBehIds.count(Id("zero")), 1);
    BOOST_CHECK_EQUAL(allBehIds.count(Id("one")), 1);
    BOOST_CHECK_EQUAL(allBehIds.count(Id("two")), 1);
    BOOST_CHECK_EQUAL(allBehIds.count(Id("three")), 1);
    BOOST_CHECK_EQUAL(allBehIds.size(), 4);

    auto allSubs = behs.allSubjectHashes();
    set<Id> allSubIds;
    for (auto s: allSubs)
        allSubIds.insert(behs.getSubjectId(s));

    BOOST_CHECK_EQUAL(allSubIds.count(Id("user0")), 1);
    BOOST_CHECK_EQUAL(allSubIds.count(Id("user1")), 1);
    BOOST_CHECK_EQUAL(allSubIds.count(Id("user2")), 1);
    BOOST_CHECK_EQUAL(allSubIds.count(Id("user3")), 1);
    BOOST_CHECK_EQUAL(allSubIds.size(), 4);

    auto checkBehSubs = [&] (string beh, std::vector<string> result)
        {
            auto subs = behs.getSubjectHashes(Id(beh));
            vector<string> extracted;
            for (auto s: subs)
                extracted.push_back(behs.getSubjectId(s).toString());
            std::sort(extracted.begin(), extracted.end());
            std::sort(result.begin(), result.end());

            BOOST_CHECK_EQUAL_COLLECTIONS(extracted.begin(),
                                          extracted.end(),
                                          result.begin(),
                                          result.end());

            auto tss = behs.getSubjectHashesAndTimestamps(Id(beh));
            extracted.clear();
            for (auto s_ts: tss)
                extracted.push_back(behs.getSubjectId(s_ts.first).toString());

            BOOST_CHECK_EQUAL_COLLECTIONS(extracted.begin(),
                                          extracted.end(),
                                          result.begin(),
                                          result.end());

            auto stats = behs.getBehaviorStats(Id(beh), BS_ALL);

            BOOST_CHECK_EQUAL(stats.subjectCount, result.size());
        };

    checkBehSubs("zero", { "user0" });
    checkBehSubs("one", { "user1" });
    checkBehSubs("two", { "user2" });
    checkBehSubs("three", { "user2" });
    
    behs.dump(cerr);

    testIntegrity(behs);
}

#if 1
BOOST_AUTO_TEST_CASE( test_beh_degenerate_mapping )
{
    // 1.  Record 1 million behaviors
    auto mut = std::make_shared<MutableBehaviorDomain>();
    createTestBehaviors(mut);

    testIntegrity(*mut);

    // Degenerate mapping: everything maps onto 1
    auto degenerate = std::make_shared<MutableBehaviorDomain>();
    for (unsigned i = 1;  i <= nSubjects;  ++i) {
        degenerate->recordId(Id(i), Id("degenerate"), Date());
    }
    
    testIntegrity(*degenerate);

    auto br = std::make_shared<BehaviorBehaviorMapping>(degenerate);

    BridgedBehaviorDomain idBridged(mut, nullptr, br);

    testIntegrity(idBridged);

    BOOST_CHECK_EQUAL(idBridged.subjectCount(), nSubjects);
    BOOST_CHECK_EQUAL(idBridged.behaviorCount(), 1);
    auto behh = idBridged.allBehaviorHashes();
    BOOST_CHECK_EQUAL(behh.size(), 1);
    BOOST_CHECK_EQUAL(behh[0], SH(Id("degenerate")));
}

BOOST_AUTO_TEST_CASE( test_beh_null_mapping )
{
    // 1.  Record 1 million behaviors
    auto mut = std::make_shared<MutableBehaviorDomain>();
    createTestBehaviors(mut);

    testIntegrity(*mut);

    // Null mapping: nothing maps onto anything
    auto mapping = std::make_shared<MutableBehaviorDomain>();
    
    testIntegrity(*mapping);

    auto br = std::make_shared<BehaviorBehaviorMapping>(mapping);

    BridgedBehaviorDomain idBridged(mut, nullptr, br);

    testIntegrity(idBridged);

    BOOST_CHECK_EQUAL(idBridged.subjectCount(), nSubjects);
    BOOST_CHECK_EQUAL(idBridged.behaviorCount(), 0);
}

BOOST_AUTO_TEST_CASE( test_null_mapping )
{
    // 1.  Record 1 million behaviors
    auto mut = std::make_shared<MutableBehaviorDomain>();
    createTestBehaviors(mut);

    testIntegrity(*mut);

    // Null mapping: nothing maps onto anything
    auto mapping = std::make_shared<MutableBehaviorDomain>();
    
    testIntegrity(*mapping);

    auto br = std::make_shared<BehaviorSubjectMapping>(mapping);

    BridgedBehaviorDomain idBridged(mut, br);

    testIntegrity(idBridged);

    BOOST_CHECK_EQUAL(idBridged.subjectCount(), 0);
    BOOST_CHECK_EQUAL(idBridged.behaviorCount(), nSubjects);
}

BOOST_AUTO_TEST_CASE( test_double_mapping )
{
    // 1.  Record 1 million behaviors
    auto mut = std::make_shared<MutableBehaviorDomain>();
    createTestBehaviors(mut);

    testIntegrity(*mut);

    // Map each input ID onto two output IDs, which should double the
    // number of subjects.
    auto smapping = std::make_shared<MutableBehaviorDomain>();
    for (unsigned i = 1;  i <= nSubjects;  ++i) {
        smapping->recordId(Id(i), Id(i), Date());
        smapping->recordId(Id(i), Id(i + nSubjects), Date());
    }

    auto sbr = std::make_shared<BehaviorSubjectMapping>(smapping);

    // Map each input ID onto three output IDs, which should triple the
    // number of behaviors.
    auto bmapping = std::make_shared<MutableBehaviorDomain>();
    for (unsigned i = 1;  i <= nSubjects;  ++i) {
        bmapping->recordId(Id(i), Id(i), Date());
        bmapping->recordId(Id(i), Id(i + nBehaviors), Date());
        bmapping->recordId(Id(i), Id(i + nBehaviors * 2), Date());
    }

    auto bbr = std::make_shared<BehaviorBehaviorMapping>(bmapping);

    BridgedBehaviorDomain idBridged(mut, sbr, bbr);

    testIntegrity(idBridged);

    BOOST_CHECK_EQUAL(idBridged.subjectCount(), 2 * mut->subjectCount());
    BOOST_CHECK_EQUAL(idBridged.behaviorCount(), 3 * mut->behaviorCount());

    // For each subject, check that its pair is equivalent
    vector<SH> allSubs = mut->allSubjectHashes();

    for (auto s: allSubs) {
        Id id1 = mut->getSubjectId(s);
        Id id2(id1.toInt() + nSubjects);

        //auto behs1 = mut->getSubjectBehaviors(id1);
        auto behs2 = idBridged.getSubjectBehaviors(id1);
        auto behs3 = idBridged.getSubjectBehaviors(id2);

        //BOOST_CHECK_EQUAL_COLLECTIONS(behs1.begin(), behs1.end(),
        //                              behs2.begin(), behs2.end());
        BOOST_CHECK_EQUAL_COLLECTIONS(behs2.begin(), behs2.end(),
                                      behs3.begin(), behs3.end());
    }

    vector<BH> allBehs = mut->allBehaviorHashes();
    
    for (auto b: allBehs) {
        Id id1 = mut->getBehaviorId(b);
        if (id1.toInt() >= nBehaviors)
            continue;
        Id id2(id1.toInt() + nBehaviors);

        //auto subs1 = mut->getSubjectHashesAndTimestamps(id1);
        auto subs2 = idBridged.getSubjectHashesAndTimestamps(id1);
        auto subs3 = idBridged.getSubjectHashesAndTimestamps(id2);

        //BOOST_CHECK_EQUAL_COLLECTIONS(subs1.begin(), subs1.end(),
        //                              subs2.begin(), subs2.end());
        BOOST_CHECK_EQUAL_COLLECTIONS(subs2.begin(), subs2.end(),
                                      subs3.begin(), subs3.end());
    }
}

BOOST_AUTO_TEST_CASE( test_forwards_backwards_mapping )
{
    // 1.  Record 1 million behaviors
    auto mut = std::make_shared<MutableBehaviorDomain>();
    createTestBehaviors(mut);

    testIntegrity(*mut);

    // Forwards and backwards mappings.  One is the inverse of the other,
    // so applying both togther should be equivalent to applying the
    // identity mapping
    auto sforwards = std::make_shared<MutableBehaviorDomain>();
    auto sbackwards = std::make_shared<MutableBehaviorDomain>();
    for (unsigned i = 1;  i <= nSubjects;  ++i) {
        sforwards->recordId(Id(i), Id(i + 1000000), Date());
        sbackwards->recordId(Id(i + 1000000), Id(i), Date());
    }
    
    auto sbrf = std::make_shared<BehaviorSubjectMapping>(sforwards);
    auto sbrb = std::make_shared<BehaviorSubjectMapping>(sbackwards);

    auto bforwards = std::make_shared<MutableBehaviorDomain>();
    auto bbackwards = std::make_shared<MutableBehaviorDomain>();
    for (unsigned i = 1;  i <= nBehaviors;  ++i) {
        bforwards->recordId(Id(i), Id(i + 1000000), Date());
        bbackwards->recordId(Id(i + 1000000), Id(i), Date());
    }
    
    auto bbrf = std::make_shared<BehaviorBehaviorMapping>(bforwards);
    auto bbrb = std::make_shared<BehaviorBehaviorMapping>(bbackwards);

    auto behForwards = std::make_shared<BridgedBehaviorDomain>(mut, sbrf, bbrf);

    vector<tuple<BH, Date, int> > behs1, behs2;

    auto onBeh = [&] (BH beh, Date date, int count)
        {
            behs1.push_back(make_tuple(beh, date, count));
            return true;
        };

    mut->forEachSubjectBehavior(Id(1), onBeh);
    
    auto onBeh2 = [&] (BH beh, Date date, int count)
        {
            behs2.push_back(make_tuple(beh, date, count));
            return true;
        };

    behForwards->forEachSubjectBehavior(Id(1000001), onBeh2);

    BOOST_CHECK_EQUAL(behs1.size(), behs2.size());
    for (unsigned i = 0;  i < min(behs1.size(), behs2.size());  ++i) {
        BH beh1 = get<0>(behs1[i]);
        BH beh2 = get<0>(behs2[i]);
        Id id1 = mut->getBehaviorId(beh1);
        Id id2 = behForwards->getBehaviorId(beh2);
        Id id1_mapped = Id(id1.toInt() + 1000000);

        BOOST_CHECK_EQUAL(id1_mapped, id2);
        BOOST_CHECK_EQUAL(get<1>(behs1[i]), get<1>(behs2[i]));
        BOOST_CHECK_EQUAL(get<2>(behs1[i]), get<2>(behs2[i]));
    }        

    cerr << "testing integrity" << endl;

    testIntegrity(*behForwards);

    cerr << "done forwards" << endl;
    auto behForwardsBackwards
        = std::make_shared<BridgedBehaviorDomain>(behForwards, sbrb, bbrb);

    testIntegrity(*behForwardsBackwards);
    cerr << "done forwards backwards" << endl;

#if 0
    cerr << "--------------- forward" << endl;
    bforwards->dump(cerr);
    cerr << "--------------- backward" << endl;
    bbackwards->dump(cerr);
    cerr << "--------------- mut" << endl;
    mut->dump(cerr);
    cerr << "--------------- behForwardsBackwards" << endl;
    behForwardsBackwards->dump(cerr);
    cerr << "--------------- behForwards" << endl;
    behForwards->dump(cerr);
#endif


    testEquivalent(*behForwardsBackwards, *mut);
}
#endif


#if 1
BOOST_AUTO_TEST_CASE( test_filtered_subject_mapping )
{
    /* beh setup */
    auto beh = make_shared<MutableBehaviorDomain>();
    Date now = Date::now();
    beh->recordId(Id("1"), Id("beh1"), now);
    beh->recordId(Id("2"), Id("beh2"), now);
    beh->recordId(Id("3"), Id("beh4"), now);
    beh->recordId(Id("4"), Id("beh5"), now);

    SH oddSh = beh->subjectIdToHash(Id("1"));
    SH evenSh = beh->subjectIdToHash(Id("2"));
    SH missingSh = beh->subjectIdToHash(Id("no entry"));

    /* the filter function returns true for odd numbers */
    auto filterFunc = [&] (SH sh) {
        Id subjectId = beh->getSubjectId(sh);
        return (subjectId.toInt() % 2) > 0;
    };

    FilteredSubjectMapping mapping(beh, filterFunc);

    BOOST_CHECK_EQUAL(mapping.isUnique(), true);
    BOOST_CHECK_EQUAL(mapping.isOneToOne(), false);
    BOOST_CHECK_EQUAL(mapping.isIdentity(), false);

    {
        auto allLeaders = mapping.allLeaders();
        BOOST_CHECK_EQUAL(allLeaders.size(), 2);
    }

    {
        SH oddSh = beh->subjectIdToHash(Id("1"));
        SH evenSh = beh->subjectIdToHash(Id("2"));
        auto leaders = mapping.getLeaders(oddSh);
        BOOST_CHECK_EQUAL(leaders.size(), 1);
        BOOST_CHECK_EQUAL(leaders[0], oddSh);
        leaders = mapping.getLeaders(evenSh);
        BOOST_CHECK_EQUAL(leaders.size(), 0);
    }

    {
        vector<SH> passed;
        auto onSubject = [&] (SH sh) {
            passed.push_back(sh);
            return true;
        };
        mapping.forEachLeader(onSubject);
        BOOST_CHECK_EQUAL(passed.size(), 2);
    }

    {
        BOOST_CHECK_EQUAL(mapping.knownLeader(oddSh), true);
        BOOST_CHECK_EQUAL(mapping.knownLeader(evenSh), false);
        BOOST_CHECK_EQUAL(mapping.knownLeader(missingSh), false);
    }

    {
        BOOST_CHECK_EQUAL(mapping.getLeaderId(oddSh), Id("1"));
        BOOST_CHECK_EQUAL(mapping.getLeaderId(evenSh), Id());
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(mapping.getLeaderId(missingSh), MLDB::Exception);
    }

    BOOST_CHECK_EQUAL(mapping.leaderCount(), 2);

    {
        auto followers = mapping.getFollowers(oddSh);
        BOOST_CHECK_EQUAL(followers.size(), 1);
        BOOST_CHECK_EQUAL(followers[0], oddSh);
        followers = mapping.getFollowers(evenSh);
        BOOST_CHECK_EQUAL(followers.size(), 1);
        BOOST_CHECK_EQUAL(followers[0], evenSh);
    }

    {
        BOOST_CHECK_EQUAL(mapping.knownFollower(oddSh), true);
        BOOST_CHECK_EQUAL(mapping.knownFollower(evenSh), true);
        BOOST_CHECK_EQUAL(mapping.knownFollower(missingSh), false);
    }


    {
        BOOST_CHECK_EQUAL(mapping.getFollowerId(oddSh), Id("1"));
        BOOST_CHECK_EQUAL(mapping.getFollowerId(evenSh), Id("2"));
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(mapping.getFollowerId(missingSh), MLDB::Exception);
    }

    BOOST_CHECK_EQUAL(mapping.followerCount(), 4);
}
#endif

#if 1
/* A test that ensures that earliestTime and latestTime work correctly. */
BOOST_AUTO_TEST_CASE( test_bridged_domain_timefuncs )
{
    /* beh setup */
    auto beh = make_shared<MutableBehaviorDomain>();
    Date date = Date(2015, 02, 01, 12, 34, 56);
    Date latestDate = date;
    beh->recordId(Id("1"), Id("beh1"), date);
    date.addHours(-1);
    beh->recordId(Id("2"), Id("beh2"), date);
    date.addHours(-1);
    Date earliestDate = date;
    beh->recordId(Id("3"), Id("beh3"), date);
    date.addHours(-1);
    beh->recordId(Id("4"), Id("beh4"), date);

    /* the filter function returns true for odd numbers */
    auto filterFunc = [&] (SH sh) {
        Id subjectId = beh->getSubjectId(sh);
        return (subjectId.toInt() % 2) > 0;
    };
    auto mapping = make_shared<FilteredSubjectMapping>(beh, filterFunc);

    auto bridgedBeh = make_shared<BridgedBehaviorDomain>(beh, mapping);
    BOOST_CHECK_EQUAL(earliestDate, bridgedBeh->earliestTime());
    BOOST_CHECK_EQUAL(latestDate, bridgedBeh->latestTime());
}
#endif
