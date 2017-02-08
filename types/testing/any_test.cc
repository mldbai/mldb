// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* any_test.cc                                        -*- C++ -*-
   Sunil Rottoo, April 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   Test any functionality
*/


#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#include <sstream>
#include <string>
#include <boost/lexical_cast.hpp>
#include <boost/test/unit_test.hpp>

#include "mldb/jml/utils/file_functions.h"

#include "mldb/types/any.h"
#include "mldb/types/basic_value_descriptions.h"

using namespace std;
using namespace ML;
using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_any_float )
{
    Any blah(0.0);
    std::string theStr = blah.asJsonStr();
    // we need this test because previously when the value was 0.0
    // asJsonStr was returning "0." which is of course invalid JSON
    BOOST_CHECK_EQUAL(theStr, "0.0");
}
