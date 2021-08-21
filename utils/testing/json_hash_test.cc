// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* json_diff_test.cc
   Jeremy Barnes, 2 November 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   Test of JSON diffs.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/utils/json_utils.h"

using namespace std;
using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_hash_1 )
{
    Json::Value v1;
    Json::Value v2;

    BOOST_CHECK_EQUAL(jsonHash(v1), 1);
    BOOST_CHECK_EQUAL(jsonHash(1), 6012453320789867841ULL);
    BOOST_CHECK_EQUAL(jsonHash(2), 15701234083180486264ULL);
    BOOST_CHECK_EQUAL(jsonHash("hello"), 7271157661961213381ULL);
    BOOST_CHECK_EQUAL(jsonHash("world"), 17875392638580224582ULL);
    BOOST_CHECK_EQUAL(jsonHash({"hello","world"}), 14646923932092017472ULL);
    BOOST_CHECK_EQUAL(jsonHash({v1,v2,"world",2,3,{}}), 9746870974541480014ULL);
}
