// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* glz_classifier_test.cc
   Jeremy Barnes, 14 May 2010
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.

   Test of the GLZ classifier class.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#define MLDB_TESTING_GLZ_CLASSIFIER

#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>
#include <boost/thread/barrier.hpp>
#include <vector>
#include <stdint.h>
#include <iostream>

#include "mldb/ml/jml/dense_features.h"
#include "mldb/ml/jml/feature_info.h"
#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/arch/exception_handler.h"

using namespace ML;
using namespace std;

using boost::unit_test::test_suite;

BOOST_AUTO_TEST_CASE( test_glz_classifier_test )
{
    string toParse = "k=CATEGORICAL/c=2,Male,Female/o=OPTIONAL";
    ParseContext context(toParse, toParse.c_str(), toParse.length());
    Mutable_Feature_Info info;
    info.parse(context);

    BOOST_CHECK_EQUAL(info.type(), CATEGORICAL);
    BOOST_CHECK_EQUAL(info.optional(), true);
    BOOST_CHECK_EQUAL(info.biased(), false);
    BOOST_REQUIRE(info.categorical());
    BOOST_CHECK_EQUAL(info.categorical()->count(), 2);
    BOOST_CHECK_EQUAL(info.categorical()->print(0), "Male");
    BOOST_CHECK_EQUAL(info.categorical()->print(1), "Female");
}
