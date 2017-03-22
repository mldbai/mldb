// This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

/* feature_set_test.cc
   Mathieu Marquis Bolduc, March 22 2017

   Tests of the featureset class.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>
#include <boost/thread/barrier.hpp>
#include <vector>
#include <stdint.h>
#include <iostream>

#include "mldb/ml/jml/feature_set.h"


using namespace ML;
using namespace std;

using boost::unit_test::test_suite;

//MLDB-2159
BOOST_AUTO_TEST_CASE( test_featureset_replace_test )
{
    Mutable_Feature_Set featureset;
    featureset.add(Feature(1,2,3), 2.0);
    featureset.add(Feature(4,5,6), 3.0);
    featureset.add(Feature(7,8,9), 4.0);

    BOOST_CHECK_EQUAL(featureset[Feature(1,2,3)], 2.0);
    BOOST_CHECK_EQUAL(featureset.size(), 3);

    featureset.replace(Feature(1,2,3), 7.0);
    BOOST_CHECK_EQUAL(featureset[Feature(1,2,3)], 7.0);
    BOOST_CHECK_EQUAL(featureset.size(), 3);
}