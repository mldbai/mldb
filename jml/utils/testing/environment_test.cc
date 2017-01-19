// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* environment_test.cc
   Jeremy Barnes, 21 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.

   Test for the environment functions.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/jml/utils/environment.h"
#include "mldb/jml/utils/info.h"

#include <boost/test/unit_test.hpp>
#include <boost/test/auto_unit_test.hpp>

using namespace MLDB;

using boost::unit_test::test_suite;

BOOST_AUTO_TEST_CASE( test1 )
{
    BOOST_CHECK_EQUAL(Environment::instance()["USER"], username());
}
