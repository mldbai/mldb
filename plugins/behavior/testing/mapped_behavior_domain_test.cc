/* mapped_behavior_domain_test.cc
   This file is part of MLDB.

   Wolfgang Sourdeau, 27 June 2016
   Copyright (c) 2016 mldb.ai inc.  All rights reserved.

*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/plugins/behavior/id.h"
#include "mldb/utils/testing/fixtures.h"
#include "mldb/plugins/behavior/mapped_behavior_domain.h"
#include "mldb/plugins/behavior/mutable_behavior_domain.h"


using namespace std;
using namespace MLDB;


/* Test the semantics of getSubjectIndex under various normal and exceptional
 * circumstances */
BOOST_AUTO_TEST_CASE(test_getSubjectIndex)
{
    /* Setup */
    TestFolderFixture fixture("beh_dom_single_timestamp");
    string filename("mapped-beh_getSubjectIndex");

    vector<Id> subjectIds{Id("s1"), Id("s2"), Id("s3")};

    MutableBehaviorDomain mutableBeh;
    for (const Id & subjectId: subjectIds) {
        mutableBeh.record(subjectId, Id("beh"), Date::now());
    }
    mutableBeh.save(filename);

    MappedBehaviorDomain mappedBeh(filename);
    auto subjectSHs = mappedBeh.allSubjectHashes();
    ExcAssertEqual(subjectSHs.size(), 3);

    /* Retrieval of known indexes */
    for (int i = 0; i < subjectSHs.size(); i++) {
        SH sh = subjectSHs[i];
        auto idx = mappedBeh.getSubjectIndex(sh);
        BOOST_CHECK_EQUAL(idx, i);
    }

    /* Retrieval of unknown indexes: sh right before first */
    ExcAssertGreater(subjectSHs[0].hash(), 1);
    SH sh = SH(subjectSHs[0].hash() - 1);
    auto idx = mappedBeh.getSubjectIndex(sh);
    BOOST_CHECK_EQUAL(idx, SI(-1));

    /* Retrieval of unknown indexes: sh right after first */
    sh = SH(subjectSHs[0].hash() + 1);
    idx = mappedBeh.getSubjectIndex(sh);
    BOOST_CHECK_EQUAL(idx, SI(-1));

    /* Retrieval of unknown indexes: sh between first and second */
    sh = SH((subjectSHs[0].hash() + subjectSHs[1].hash()) / 2);
    ExcAssertGreater(sh, subjectSHs[0]);
    ExcAssertLess(sh, subjectSHs[1]);
    idx = mappedBeh.getSubjectIndex(sh);
    BOOST_CHECK_EQUAL(idx, SI(-1));

    /* Retrieval of unknown indexes: sh right before last */
    ExcAssertLess(subjectSHs.back().hash(), SH(-1));
    sh = SH(subjectSHs.back().hash() - 1);
    idx = mappedBeh.getSubjectIndex(sh);
    BOOST_CHECK_EQUAL(idx, SI(-1));

    /* Retrieval of unknown indexes: sh right after last */
    ExcAssertLess(subjectSHs.back().hash(), SH(-1));
    sh = SH(subjectSHs.back().hash() + 1);
    idx = mappedBeh.getSubjectIndex(sh);
    BOOST_CHECK_EQUAL(idx, SI(-1));
}
