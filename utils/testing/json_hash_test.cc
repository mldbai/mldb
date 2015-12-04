// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* json_diff_test.cc
   Jeremy Barnes, 2 November 2013
   Copyright (c) 2013 Datacratic.  All rights reserved.

   Test of JSON diffs.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/utils/json_utils.h"

using namespace std;
using namespace Datacratic;

BOOST_AUTO_TEST_CASE( test_hash_1 )
{
    Json::Value v1;
    Json::Value v2;

    BOOST_CHECK_EQUAL(jsonHash(v1), 1);
    BOOST_CHECK_EQUAL(jsonHash(1), 10234557827792954321ULL);
    BOOST_CHECK_EQUAL(jsonHash(2), 11162429202992555371ULL);
    BOOST_CHECK_EQUAL(jsonHash("hello"), 15271007394436749991ULL);
    BOOST_CHECK_EQUAL(jsonHash("world"), 10768803623877910639ULL);
    BOOST_CHECK_EQUAL(jsonHash({"hello","world"}), 6530281702236206815ULL);
    BOOST_CHECK_EQUAL(jsonHash({v1,v2,"world",2,3,{}}), 12977350397847970632ULL);
}
