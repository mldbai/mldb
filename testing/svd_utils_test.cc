// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* svd_utils_test.cc
   Jeremy Barnes, 18 November 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Test for SVD utilities
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/ml/svd_utils.h"
#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/base/parallel.h"

#include "mldb/jml/stats/distribution.h"
#include <cmath>

using namespace std;

using namespace ML;

using namespace MLDB;

void testBucket(std::vector<uint32_t> subs1,
                std::vector<uint32_t> subs2)
{
    cerr << "doing " << subs1 << " and " << subs2 << endl;

    std::sort(subs1.begin(), subs1.end());
    std::sort(subs2.begin(), subs2.end());

    SvdColumnEntry::Bucket bucket1, bucket2;
    
    for (auto & s: subs1)
        bucket1.add(s, SH(s));

    for (auto & s: subs2)
        bucket2.add(s, SH(s));

    bucket1.compress();
    bucket2.compress();

    double count11 = bucket1.calcOverlap(bucket1, HAMMING);
    BOOST_CHECK_EQUAL(count11, subs1.size());

    double count22 = bucket2.calcOverlap(bucket2, HAMMING);
    BOOST_CHECK_EQUAL(count22, subs2.size());

    int expected = intersectionCount(&subs1[0], &subs1[0] + subs1.size(),
                                     &subs2[0], &subs2[0] + subs2.size());

    double count1 = bucket1.calcOverlap(bucket2, HAMMING);
    double count2 = bucket2.calcOverlap(bucket1, HAMMING);

    BOOST_CHECK_EQUAL(expected, count1);
    BOOST_CHECK_EQUAL(expected, count2);
}

BOOST_AUTO_TEST_CASE( test_buckets )
{
    testBucket({}, {});
    testBucket({1}, {0});
    testBucket({1}, {1});
    testBucket({1,2,3}, {1,2,3});
    testBucket({1,2,3}, {1});
    testBucket({1,2,3,100,200,300}, {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17});
}

