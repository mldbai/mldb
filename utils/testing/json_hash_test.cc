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
    BOOST_CHECK_EQUAL(jsonHash(1), 17440209749015900358ULL);
    BOOST_CHECK_EQUAL(jsonHash(2), 15516055874414587543ULL);
    BOOST_CHECK_EQUAL(jsonHash("hello"), 2760398259093080837ULL);
    BOOST_CHECK_EQUAL(jsonHash("world"), 1294794898716980195ULL);
    BOOST_CHECK_EQUAL(jsonHash({"hello","world"}), 1167641850042591258ULL);
    BOOST_CHECK_EQUAL(jsonHash({v1,v2,"world",2,3,{}}), 3918262287598378465ULL);
}
