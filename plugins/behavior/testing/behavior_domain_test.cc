// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* behavior_domain_test.cc
   Jeremy Barnes, 9 June 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Test of the behavior domain classes.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/plugins/behavior/mutable_behavior_domain.h"
#include "mldb/plugins/behavior/mapped_behavior_domain.h"
#include "mldb/plugins/behavior/merged_behavior_domain.h"
//#include "mldb/plugins/behavior/filtered_behavior_domain.h"
#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/arch/demangle.h"

#include "mldb/utils/testing/fixtures.h"

using namespace std;
using namespace MLDB;
using namespace ML;

namespace {

#if 1
    int nToRecord = 100000;
    int nSubjects = 1000;
    int nBehaviors = 1000;
#elif 0
    int nToRecord = 1000000;
    int nSubjects = 1000;
    int nBehaviors = 1000;
#else
    int nToRecord = 50;
    int nSubjects = 10;
    int nBehaviors = 1;
#endif

} // file scope

#include "behavior_test_utils.h"

MLDB_FIXTURE(behavior_domain_test);

#if 1
BOOST_AUTO_TEST_CASE( test_subject_info_sort1 )
{
    SubjectInfo info;
    SubjectBehavior beh;
    beh.behavior = 1;
    beh.count = 1;

    info.behaviors.push_back(beh);
    info.behaviors.push_back(beh);

    info.sort();

    BOOST_CHECK_EQUAL(info.behaviors.size(), 1);
    BOOST_CHECK_EQUAL(info.behaviors[0].count, 2);
    BOOST_CHECK_EQUAL(info.behaviors[0].behavior, 1);
}
#endif

#if 1
BOOST_AUTO_TEST_CASE( test_subject_info_sort2 )
{
    SubjectInfo info;
    SubjectBehavior beh;
    beh.behavior = 1;
    beh.count = 1;

    SubjectBehavior beh2 = beh;
    beh2.timestamp.addSeconds(1);

    info.behaviors.push_back(beh);
    info.behaviors.push_back(beh2);
    info.behaviors.push_back(beh);
    info.behaviors.push_back(beh2);
    info.behaviors.push_back(beh);
    info.behaviors.push_back(beh2);

    info.sort();

    BOOST_CHECK_EQUAL(info.behaviors.size(), 2);
    BOOST_CHECK_EQUAL(info.behaviors[0].count, 3);
    BOOST_CHECK_EQUAL(info.behaviors[0].behavior, 1);
    BOOST_CHECK_EQUAL(info.behaviors[1].count, 3);
    BOOST_CHECK_EQUAL(info.behaviors[1].behavior, 1);
    BOOST_CHECK_LT(info.behaviors[0].timestamp,
                   info.behaviors[1].timestamp);
}
#endif

#if 0 // this test has issues with when we insert the same thing twice
BOOST_AUTO_TEST_CASE( test_mutable_behavior_domain )
{
    MutableBehaviorDomain dom;

    cerr << "size of subject entry = "
         << sizeof(MutableBehaviorDomain::SubjectEntry) << endl;
    cerr << "size of behavior entry = "
         << sizeof(MutableBehaviorDomain::BehaviorEntry) << endl;
    cerr << "size of Id = " << sizeof(Id) << endl;

    vector<tuple<Date, BH, int> > entries;

    Id subject("hello");

    for (unsigned i = 0;  i < 1000;  ++i) {
        Id id(random() % 1000 + 1);
        BH beh(id);
        Date date = Date::fromSecondsSinceEpoch(random() % 1000);
        uint32_t count = i + 1;
        
        dom.recordId(subject, id, date, count);
        entries.push_back(make_tuple(date, beh, count));

        std::sort(entries.begin(), entries.end());

        vector<tuple<Date, BH, int> > read;

        bool hadError = false;

        int x = 0;
        auto onBeh = [&] (BH beh, Date ts, uint32_t count)
            {
                if (x >= entries.size()
                    || ts != std::get<0>(entries[x])
                    || beh != std::get<1>(entries[x])
                    || count != std::get<2>(entries[x]))
                    hadError = true;

                BOOST_CHECK_LT(x, entries.size());
                BOOST_CHECK_EQUAL(ts, std::get<0>(entries[x]));
                BOOST_CHECK_EQUAL(beh, std::get<1>(entries[x]));
                BOOST_CHECK_EQUAL(count, std::get<2>(entries[x]));
                
                read.push_back(std::make_tuple(ts, beh, count));
                
                ++x;
                return true;
            };

        dom.forEachSubjectBehaviorHash(subject, onBeh,
                                        BehaviorDomain::
                                        SubjectBehaviorFilter(),
                                        INORDER);
        
        if (hadError) {
            auto dump = [&] (const vector<tuple<Date, BH, int> > & read)
                {
                    int i = 0;
                    for (auto e: read) {
                        Date ts;
                        BH beh;
                        uint32_t count;

                        std::tie(ts, beh, count) = e;

                        cerr << i << ": " << ts << " " << beh << " " << count
                             << endl;
                        ++i;
                    }
                };
        
            cerr << "inserted: " << endl;
            dump(entries);

            cerr << endl << "read: " << endl;
            dump(read);
        }

        BOOST_CHECK_EQUAL(x, entries.size());
    }
}
#endif

#if 1
BOOST_FIXTURE_TEST_CASE( test_multiple_timestamps, behavior_domain_test )
{
    cerr << "Test for multiple time stamps " << endl;
    MutableBehaviorDomain actions;
    actions.hasSubjectIds = true;
    Id id("id2dc16f46-0bc9-4001-7163-c6b0000003a3");
    Id strat1 = Id("IMP-belair_qcfr_opt");
    Id strat2 = Id("CLICK-belair_qcfr_opt");

    Date d1(2012, 9, 20, 22, 17, 52);
    Date d2(2012, 9, 20, 22, 30, 53);
    Date d3(2012, 9, 20, 23, 32, 32);
    Date d4(2012, 9, 20, 23, 55, 17);
    Date d5(2012, 9, 20, 23, 33, 32);

    actions.recordId(id, strat1, d1, 1);
    actions.recordId(id, strat1, d2, 1);
    actions.recordId(id, strat1, d3, 1);
    actions.recordId(id, strat1, d4, 1);
    actions.recordId(id, strat2, d5, 1);

    actions.makeImmutable();
    cerr << "before" << endl;
    actions.dump(cerr);
    cerr << endl;

    actions.save("multiple.beh");

    cerr << "Reloading actions file " << endl;
    auto actionsBeh = std::make_shared<MappedBehaviorDomain>("multiple.beh");

    cerr << "after" << endl;
    actionsBeh->dump(cerr);

    testEquivalent(actions, *actionsBeh);


    auto doCheck2 = [](const BehaviorDomain & behs,
                       BH beh, BehaviorIterInfo)
     {
         Id behId = behs.getBehaviorIdFromHash(beh);
         BOOST_CHECK(behId == Id("CLICK-belair_qcfr_opt") ||
                     behId == Id("IMP-belair_qcfr_opt")) ;

         if(behId == Id("CLICK-belair_qcfr_opt"))
         {
             cerr << "Got a click strategy " << endl;
             auto onClickStrat = [&](SH subject, Date ts)
             {
                 Id uid = behs.getSubjectId(subject);
                 cerr <<"uid: " << uid << " @ " << ts << endl;
                 return true;
             };
             behs.forEachBehaviorSubject(beh, onClickStrat);
         }
         else
         {
             std::vector<std::pair<SH, Date> > st = behs.getSubjectHashesAndTimestamps(beh) ;
             cerr << "Got an imp strategy with " << st.size() <<
                     " distinct timestamps " << endl;
             unsigned theCount = 0;
             auto onImpStrat = [&](SH subject, Date ts)
             {
                 Id uid = behs.getSubjectId(subject);
                 cerr <<"uid: " << uid << " @ " << ts << endl;
                 theCount++;
                 return true;
             };
             behs.forEachBehaviorSubject(beh, onImpStrat);
             BOOST_CHECK_EQUAL(theCount, 1);
         }
         return true;
     };
    cerr << "testing actions" << endl;
    actions.forEachBehavior(std::bind(doCheck2, std::ref(actions),
                                       std::placeholders::_1,
                                       std::placeholders::_2));
    cerr << "testing actionsBeh" << endl;
    actionsBeh->forEachBehavior(std::bind(doCheck2, std::ref(*actionsBeh),
                                           std::placeholders::_1,
                                           std::placeholders::_2));
}

#endif

#if 0
BOOST_AUTO_TEST_CASE( test_reconstitute1 )
{
    MappedBehaviorDomain dom("sojern/old/sojern-20120618.00.beh");
    testIntegrity(dom);
}
#endif

#if 1
BOOST_AUTO_TEST_CASE( test_behavior_serialization )
{
    // 1.  Record 1 million behaviors
    auto mut = std::make_shared<MutableBehaviorDomain>();
    createTestBehaviors(mut);

    cerr << "testing integrity 0" << endl;
    testIntegrity(*mut);

    //mut->dump(cerr);

    mut->save("tmp/behaviorDomainTest.beh");

    MappedBehaviorDomain m;
    m.load("tmp/behaviorDomainTest.beh");

    cerr << "testing integrity" << endl;
    testIntegrity(m);

    cerr << "testing equivalence" << endl;
    testEquivalent(m, *mut);

    cerr << "done testing integrity/equivalence" << endl;
    vector<std::shared_ptr<BehaviorDomain> > toMerge;
    toMerge.push_back(make_unowned_sp(m));

    MergedBehaviorDomain m2(toMerge, true);
    
    testIntegrity(m2);

    testEquivalent(m2, *mut);

    vector<SH> subjs = m.allSubjectHashes();
    int n = subjs.size() / 2;
    
    vector<SH> subjs1(subjs.begin(), subjs.begin() + n);
    vector<SH> subjs2(subjs.begin() + n, subjs.end());

#if 0
    FilteredBehaviorDomain f1(mut, subjs1);
    FilteredBehaviorDomain f2(make_unowned_sp(m),   subjs2);

    vector<std::shared_ptr<BehaviorDomain> > toMerge2;
    toMerge2.push_back(make_unowned_sp(f1));
    toMerge2.push_back(make_unowned_sp(f2));
    MergedBehaviorDomain m3(toMerge2, true);
    
    testIntegrity(m3);

    testEquivalent(m3, *mut);
#endif
}
#endif

#if 1
BOOST_AUTO_TEST_CASE(test_merge_with_empty_behaviors)
{
    auto mut = std::make_shared<MutableBehaviorDomain>();
    createTestBehaviors(mut);

    testIntegrity(*mut);

    auto mut2 = std::make_shared<MutableBehaviorDomain>();

    //testIntegrity(*mut2);

    vector<std::shared_ptr<BehaviorDomain> > toMerge({mut, mut2});
    MergedBehaviorDomain merged(toMerge, true);

    testIntegrity(merged);
    testEquivalent(*mut, merged);
}
#endif

#if 1
BOOST_AUTO_TEST_CASE(test_merge_with_empty_behaviors_serialized)
{
    /* test with empty vector */
    vector<std::shared_ptr<BehaviorDomain> > toMerge;

    auto merged = std::make_shared<MergedBehaviorDomain>(toMerge, false);
    merged->save("tmp/emptyMergedDomainTest1.beh");

    /* test with one empty element */
    {
        auto mut2 = std::make_shared<MutableBehaviorDomain>();
        mut2->save("tmp/empty.beh");
    }
    toMerge.push_back(std::make_shared<MappedBehaviorDomain>("tmp/empty.beh"));
    merged.reset(new MergedBehaviorDomain(toMerge, false));
    merged->save("tmp/emptyMergedDomainTest1.beh");

    /* test with one empty element and a non-empty one */
    auto mut = std::make_shared<MutableBehaviorDomain>();
    createTestBehaviors(mut);
    mut->save("tmp/nonEmpty.beh");
    toMerge.push_back(std::make_shared<MappedBehaviorDomain>("tmp/nonEmpty.beh"));

    merged.reset(new MergedBehaviorDomain(toMerge, true));

    testIntegrity(*merged);
    testEquivalent(*mut, *merged);
}
#endif

#if 1
BOOST_AUTO_TEST_CASE(test_streams)
{
    cerr << "Test streams " << endl;
    MutableBehaviorDomain actions;
    actions.hasSubjectIds = true;
    Id id("id2dc16f46-0bc9-4001-7163-c6b0000003a3");
    Id strat1 = Id("IMP-belair_qcfr_opt");
    Id strat2 = Id("CLICK-belair_qcfr_opt");

    Date d(2012, 9, 20, 22, 17, 52);

    actions.recordId(id, strat1, d, 1);
    actions.recordId(id, strat2, d, 1);

    actions.makeImmutable();
    cerr << "before" << endl;
    actions.dump(cerr);
    cerr << endl;

    auto mutableSubjectStream = actions.getSubjectStream(0);
    auto subject = actions.getSubjectId(mutableSubjectStream->next());
    cerr << "subject streamed: " << subject << endl;
    BOOST_CHECK(subject == id);
    mutableSubjectStream.reset();

    auto mutableBehaviorStream = actions.getBehaviorStream(0);
    auto behavior = actions.getBehaviorId(mutableBehaviorStream->next());
    cerr << "behavior streamed: " << behavior << endl;
    BOOST_CHECK(behavior == strat1);
    behavior = actions.getBehaviorId(mutableBehaviorStream->next());
    cerr << "behavior streamed: " << behavior << endl;
    BOOST_CHECK(behavior == strat2);
    mutableBehaviorStream.reset();

    actions.save("tmp/stream.beh");

    cerr << "Reloading actions file " << endl;
    auto actionsBeh = std::make_shared<MappedBehaviorDomain>("tmp/stream.beh");

    cerr << "after" << endl;
    actionsBeh->dump(cerr);

    testEquivalent(actions, *actionsBeh);

    vector<SH> subjs;

    auto mappedSubjectStream = actionsBeh->getSubjectStream(0);
    auto subjectSH = mappedSubjectStream->next();
    subjs.push_back(subjectSH);
    auto mappedsubject = actionsBeh->getSubjectId(subjectSH);
    cerr << "subject streamed: " << mappedsubject << endl;
    BOOST_CHECK(mappedsubject == id);
    mappedSubjectStream.reset();

    auto mappedBehaviorStream = actionsBeh->getBehaviorStream(0);
    auto mappedbehavior = actionsBeh->getBehaviorId(mappedBehaviorStream->next());
    cerr << "behavior streamed: " << mappedbehavior << endl;
    BOOST_CHECK(mappedbehavior == strat1);
    mappedbehavior = actionsBeh->getBehaviorId(mappedBehaviorStream->next());
    cerr << "behavior streamed: " << mappedbehavior << endl;
    BOOST_CHECK(mappedbehavior == strat2);
    mappedBehaviorStream.reset();

#if 0
    FilteredBehaviorDomain f1(actionsBeh, subjs);

    auto filteredSubjectStream = f1.getSubjectStream(0);
    auto filteredsubject = f1.getSubjectId(filteredSubjectStream->next());
    cerr << "subject streamed: " << filteredsubject << endl;
    BOOST_CHECK(filteredsubject == id);
    filteredSubjectStream.reset();

    auto filteredBehaviorStream = f1.getBehaviorStream(0);
    auto filteredbehavior = f1.getBehaviorId(filteredBehaviorStream->next());
    cerr << "behavior streamed: " << filteredbehavior << endl;
    BOOST_CHECK(filteredbehavior == strat1);
    filteredbehavior = f1.getBehaviorId(filteredBehaviorStream->next());
    cerr << "behavior streamed: " << filteredbehavior << endl;
    BOOST_CHECK(filteredbehavior == strat2);
    filteredBehaviorStream.reset();
#endif    
}
#endif
