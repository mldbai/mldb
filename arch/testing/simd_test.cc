// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* simd_test.cc
   Jeremy Barnes, 21 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.

   Test of the SIMD detection code.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/arch/simd.h"

#include <boost/test/unit_test.hpp>
#include <boost/test/auto_unit_test.hpp>
#include <vector>
#include <iostream>


using namespace MLDB;
using namespace std;

using boost::unit_test::test_suite;

BOOST_AUTO_TEST_CASE( test1 )
{
    BOOST_CHECK(has_mmx());
    BOOST_CHECK(has_sse1());
}
