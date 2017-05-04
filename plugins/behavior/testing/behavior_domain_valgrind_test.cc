// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* behavior_domain_valgrid_test.cc
   Jeremy Barnes, 2 July 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   Test that the BehaviorDomain valgrinds properly.
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


using namespace std;
using namespace MLDB;
using namespace ML;

namespace {

    int nToRecord = 1000;
    int nSubjects = 100;
    int nBehaviors = 100;

} // file scope

#include "behavior_test_utils.h"

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
BOOST_AUTO_TEST_CASE( test_behavior_serialization )
{
    // 1.  Record 1 million behaviors
    auto mut = std::make_shared<MutableBehaviorDomain>();
    createTestBehaviors(mut);

    testIntegrity(*mut);

    mut->save("tmp/behaviorDomainTest.beh");

    MappedBehaviorDomain m;
    m.load("tmp/behaviorDomainTest.beh");

    testIntegrity(m);

    testEquivalent(m, *mut);

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



