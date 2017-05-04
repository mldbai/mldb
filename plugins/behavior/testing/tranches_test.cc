// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tranches_test.cc
   Jeremy Barnes, 5 August 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   Test of tranches functionality.

*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/plugins/behavior/tranches.h"
#include "mldb/plugins/behavior/mutable_behavior_domain.h"
#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/arch/demangle.h"

#include "mldb/utils/testing/fixtures.h"

using namespace std;
using namespace MLDB;
using namespace ML;

#if 1

BOOST_AUTO_TEST_CASE( test_tranche_basics )
{
    TrancheSpec spec1;
    BOOST_CHECK_EQUAL(spec1.toString(), "0_1");
    BOOST_CHECK_EQUAL(spec1.parse(spec1.toString()).toString(), "0_1");
    BOOST_CHECK_EQUAL(spec1.modulusShift, 0);

    for (unsigned i = 1;  i < 1000;  ++i)
        BOOST_CHECK_EQUAL(spec1.matches(SH(Id(i))), true);

    TrancheSpec spec2(1, 2);
    BOOST_CHECK_EQUAL(spec2.toString(), "1_2");
    BOOST_CHECK_EQUAL(spec2.toString(),
                      TrancheSpec::parse(spec2.toString()).toString());

    int numTrue = 0;
    for (unsigned i = 1;  i < 1000;  ++i)
        if (spec2.matches(SH(Id(i))))
            ++numTrue;

    BOOST_CHECK_GE(numTrue, 480);
    BOOST_CHECK_LT(numTrue, 520);

    TrancheSpec spec3 = TrancheSpec::parse("0");
    BOOST_CHECK_EQUAL(spec3.toString(), "0");

    for (unsigned i = 1;  i < 1000;  ++i)
        BOOST_CHECK_EQUAL(spec3.matches(SH(Id(i))), false);

    TrancheSpec spec4 = TrancheSpec::parse("00_32");
    BOOST_CHECK_EQUAL(spec4.modulusShift, 5);

    TrancheSpec spec5 = TrancheSpec::parse("00-07_32");
    spec5.collapse();
    BOOST_CHECK_EQUAL(spec5.toString(), "00-07_32");

    // == and !=
    BOOST_CHECK_EQUAL(TrancheSpec(0,1), TrancheSpec(0,1));
    BOOST_CHECK_EQUAL(TrancheSpec({0,2},4), TrancheSpec({0,2},4));
    BOOST_CHECK_EQUAL(TrancheSpec({0,2},4), TrancheSpec(0,2));
    BOOST_CHECK_EQUAL(TrancheSpec({0,1},4), TrancheSpec({0,1,4,5},8));
    BOOST_CHECK(TrancheSpec(1,4) != TrancheSpec(0,4));
    BOOST_CHECK(TrancheSpec(0,4) != TrancheSpec(0,8));

    // more complicated matches
    TrancheSpec spec6({0,3},4);
    for (int i=0; i<100; ++i)
    {
        SH sh = SH(Id(i));
        int mod = sh.hash() % 4;
        BOOST_CHECK_EQUAL(mod == 0 || mod == 3, spec6.matches(sh));
    }

    // empty tranche
    TrancheSpec spec7(vector<int>(), 1);
    BOOST_CHECK_EQUAL(spec7.toString(), "0");
    TrancheSpec spec8(vector<int>(),8);
    BOOST_CHECK_EQUAL(spec8.toString(), "0");
    TrancheSpec spec9("0");
    BOOST_CHECK_EQUAL(spec9.set.size(), 0);
    BOOST_CHECK_EQUAL(spec9.modulusShift, 0);
    BOOST_CHECK_EQUAL(spec9.toString(), "0");

    // complement
    BOOST_CHECK_EQUAL(TrancheSpec({1,2},4).complement(), TrancheSpec({0,3}, 4));
    BOOST_CHECK_EQUAL(TrancheSpec("0").complement(), TrancheSpec("0_1"));
    BOOST_CHECK_EQUAL(TrancheSpec("0_1").complement(), TrancheSpec("0"));

}

BOOST_AUTO_TEST_CASE( test_tranche_infer_from_behs )
{
    TrancheSpec spec(0, 32);
    BOOST_CHECK_EQUAL(spec.toString(), "00_32");

    MutableBehaviorDomain behs;
    for (unsigned i = 1;  i < 10000;  ++i) {
        Id id(i);
        SH subj(id);
        int hash = id.hash() % 32;

        BOOST_CHECK_EQUAL(hash == 0, spec.matches(subj));

        // Only insert subjects with hash 0
        if (hash != 0)
            continue;

        behs.recordId(id, Id(1234), Date(), 1);
    }

    behs.finish();

    TrancheSpec spec2;
    spec2.infer(behs);

    BOOST_CHECK_EQUAL(spec2.toString(), "00_32");
}

BOOST_AUTO_TEST_CASE( test_tranche_intersects )
{
    TrancheSpec s0("0");
    TrancheSpec s1("*_32");
    TrancheSpec s2("0_32");
    TrancheSpec s3("0,1_32");
    TrancheSpec s4("0_16");
    TrancheSpec s5("0_64");
    TrancheSpec s6("0,32_64");
    TrancheSpec s7("3_32");

    BOOST_CHECK(!s0.intersects(s0));
    BOOST_CHECK(!s0.intersects(s1));
    BOOST_CHECK(!s0.intersects(s2));
    BOOST_CHECK(!s0.intersects(s3));
    BOOST_CHECK(!s0.intersects(s4));
    BOOST_CHECK(!s0.intersects(s5));

    BOOST_CHECK(!s1.intersects(s0));
    BOOST_CHECK(!s2.intersects(s0));
    BOOST_CHECK(!s3.intersects(s0));
    BOOST_CHECK(!s4.intersects(s0));
    BOOST_CHECK(!s5.intersects(s0));

    BOOST_CHECK(s1.intersects(s1));
    BOOST_CHECK(s1.intersects(s2));
    BOOST_CHECK(s1.intersects(s3));
    BOOST_CHECK(s1.intersects(s4));
    BOOST_CHECK(s1.intersects(s5));

    BOOST_CHECK(s2.intersects(s1));
    BOOST_CHECK(s3.intersects(s1));
    BOOST_CHECK(s4.intersects(s1));
    BOOST_CHECK(s5.intersects(s1));

    BOOST_CHECK(s3.intersects(s2));
    BOOST_CHECK(s4.intersects(s2));
    BOOST_CHECK(s5.intersects(s2));
    BOOST_CHECK(s6.intersects(s2));
    BOOST_CHECK(!s7.intersects(s2));

    BOOST_CHECK(s2.intersects(s3));
    BOOST_CHECK(s2.intersects(s4));
    BOOST_CHECK(s2.intersects(s5));
    BOOST_CHECK(s2.intersects(s6));
    BOOST_CHECK(!s2.intersects(s7));

    BOOST_CHECK(s3.intersects(s3));
    BOOST_CHECK(s3.intersects(s4));
    BOOST_CHECK(s3.intersects(s5));
    BOOST_CHECK(s3.intersects(s6));
    BOOST_CHECK(!s3.intersects(s7));

    BOOST_CHECK(s4.intersects(s3));
    BOOST_CHECK(s5.intersects(s3));
    BOOST_CHECK(s6.intersects(s3));
    BOOST_CHECK(!s7.intersects(s3));

    BOOST_CHECK(s4.intersects(s4));
    BOOST_CHECK(s4.intersects(s5));
    BOOST_CHECK(s4.intersects(s6));
    BOOST_CHECK(!s4.intersects(s7));

    BOOST_CHECK(s5.intersects(s4));
    BOOST_CHECK(s6.intersects(s4));
    BOOST_CHECK(!s7.intersects(s4));

    BOOST_CHECK(s5.intersects(s5));
    BOOST_CHECK(s5.intersects(s6));
    BOOST_CHECK(!s5.intersects(s7));

    BOOST_CHECK(s6.intersects(s5));
    BOOST_CHECK(!s7.intersects(s5));

    BOOST_CHECK(!s6.intersects(s7));
    BOOST_CHECK(!s7.intersects(s6));

    BOOST_CHECK(!s7.intersects(s2));
}
#endif

BOOST_AUTO_TEST_CASE (test_tranche_intersect)
{
    TrancheSpec s1(1);   // all of one tranche, ie everything
    TrancheSpec s2(32);
    TrancheSpec s3(vector<int>(), 1);
    TrancheSpec s4(0, 32);
    TrancheSpec s5(1, 32);
    TrancheSpec s6(1, 2);

    BOOST_CHECK_EQUAL(s1.toString(), "0_1");
    BOOST_CHECK_EQUAL(s2.toString(), "*_32");
    BOOST_CHECK_EQUAL(s3.toString(), "0");

    BOOST_CHECK_EQUAL(s1.intersect(s2).toString(), "*_32");
    BOOST_CHECK_EQUAL(s2.intersect(s1).toString(), "*_32");
    BOOST_CHECK_EQUAL(s3.intersect(s1).toString(), "0");
    BOOST_CHECK_EQUAL(s3.intersect(s2).toString(), "0");
    BOOST_CHECK_EQUAL(s4.intersect(s5).toString(), "0");
    BOOST_CHECK_EQUAL(s5.intersect(s4).toString(), "0");
    BOOST_CHECK_EQUAL(s6.intersect(s5).toString(), "01_32");
}
