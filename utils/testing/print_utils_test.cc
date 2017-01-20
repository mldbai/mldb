// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** print_utils_test.cc                                            -*- C++ -*-
    Rémi Attab, 2 Apr 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/utils/testing/print_utils.h"

#include <boost/test/unit_test.hpp>

using namespace std;
using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_randomString )
{
    string a = randomString(6);
    string b = randomString(6);
    BOOST_CHECK_NE(a, b);
}
