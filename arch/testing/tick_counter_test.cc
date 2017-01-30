// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tick_counter_test.cc                                            -*- C++ -*-
   Jeremy Barnes, 16 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.

   Test of tick counter functionality.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/arch/tick_counter.h"

#include <boost/test/unit_test.hpp>
#include <iostream>


using namespace MLDB;
using namespace std;

using boost::unit_test::test_suite;

BOOST_AUTO_TEST_CASE( test1 )
{
    uint64_t before = ticks();
    uint64_t after = ticks();

    BOOST_CHECK(after > before);
}

BOOST_AUTO_TEST_CASE( test2 )
{
    double overhead = calc_ticks_overhead();

    cerr << "tick overhead = " << overhead << endl;
    
    BOOST_CHECK(overhead > 1.0);

    // Tick overhead for VMs may be quite high
    BOOST_CHECK(overhead < 10000.0);
}

BOOST_AUTO_TEST_CASE( test3 )
{
    double ticks_per_second = calc_ticks_per_second();

    cerr << "ticks_per_second = " << ticks_per_second << endl;

    BOOST_CHECK(ticks_per_second > 1e9);
    BOOST_CHECK(ticks_per_second < 10e9);
}
