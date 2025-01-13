// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* least_squares_test.cc
   Jeremy Barnes, 25 February 2008
   Copyright (c) 2008 Jeremy Barnes.  All rights reserved.

   Test of the least squares class.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <boost/test/tools/floating_point_comparison.hpp>
#include <thread>

#include <vector>
#include <stdint.h>
#include <iostream>

#include "mldb/plugins/jml/algebra/irls.h"
#include "mldb/utils/vector_utils.h"

using namespace MLDB;
using namespace std;

using boost::unit_test::test_suite;
using namespace boost::test_tools;

namespace MLDB {
extern __thread std::ostream * debug_remove_dependent;
}

template<typename Float>
void do_test_identity()
{
    MLDB::Matrix<Float, 2> array(2, 2);
    array[0][0] = 1;
    array[1][1] = 1;

    vector<int> result = remove_dependent(array);
    BOOST_CHECK_EQUAL(array[0][0], 1.0);
    BOOST_CHECK_EQUAL(array[0][1], 0.0);
    BOOST_CHECK_EQUAL(array[1][0], 0.0);
    BOOST_CHECK_EQUAL(array[1][1], 1.0);

    BOOST_REQUIRE_EQUAL(result.size(), 2);
    BOOST_CHECK_EQUAL(result[0], 0);
    BOOST_CHECK_EQUAL(result[1], 1);
}

BOOST_AUTO_TEST_CASE( test_identity )
{
    do_test_identity<double>();
    do_test_identity<float>();
}

template<typename Float>
void do_test_null()
{
    MLDB::Matrix<Float, 2> array(2, 2);

    BOOST_CHECK_EQUAL(array.dim(0), 2);
    BOOST_CHECK_EQUAL(array.dim(1), 2);

    debug_remove_dependent = &cerr;
    vector<int> result = remove_dependent(array);
    debug_remove_dependent = 0;

    BOOST_CHECK_EQUAL(array.dim(0), 0);
    BOOST_CHECK_EQUAL(array.dim(1), 2);

    BOOST_REQUIRE_EQUAL(result.size(), 2);
    BOOST_CHECK_EQUAL(result[0], -1);
    BOOST_CHECK_EQUAL(result[1], -1);
}

BOOST_AUTO_TEST_CASE( test_null )
{
    do_test_null<double>();
    do_test_null<float>();
}

template<typename Float>
void do_test_uniform()
{
    MLDB::Matrix<Float, 2> array(2, 2);
    array[0][0] = array[0][1] = array[1][0] = array[1][1] = 1.0;

    BOOST_CHECK_EQUAL(array.dim(0), 2);
    BOOST_CHECK_EQUAL(array.dim(1), 2);

    debug_remove_dependent = &cerr;
    vector<int> result = remove_dependent(array);
    debug_remove_dependent = 0;

    BOOST_CHECK_EQUAL(array.dim(0), 1);
    BOOST_CHECK_EQUAL(array.dim(1), 2);

    BOOST_CHECK_EQUAL(array[0][0], 1.0);
    BOOST_CHECK_EQUAL(array[0][1], 1.0);

    BOOST_REQUIRE_EQUAL(result.size(), 2);
    BOOST_CHECK_EQUAL(result[0],  -1);
    BOOST_CHECK_EQUAL(result[1],   0);
}

BOOST_AUTO_TEST_CASE( test_uniform )
{
    do_test_uniform<double>();
    do_test_uniform<float>();
}

template<typename Float>
void do_test_dependent()
{
    MLDB::Matrix<Float, 2> array(3, 2);
    array[0][0] = 1;
    array[1][1] = 1;
    array[2][1] = 1;

    BOOST_CHECK_EQUAL(array.dim(0), 3);
    BOOST_CHECK_EQUAL(array.dim(1), 2);

    vector<int> result = remove_dependent(array);

    cerr << "result = " << result << endl;

    BOOST_CHECK_EQUAL(array.dim(0), 2);
    BOOST_CHECK_EQUAL(array.dim(1), 2);

    BOOST_CHECK_EQUAL(array[0][0], 1.0);
    BOOST_CHECK_EQUAL(array[0][1], 0.0);
    BOOST_CHECK_EQUAL(array[1][0], 0.0);
    BOOST_CHECK_EQUAL(array[1][1], 1.0);

    BOOST_REQUIRE_EQUAL(result.size(), 3);

    BOOST_CHECK_EQUAL(result[0],  0);
    BOOST_CHECK_EQUAL(result[1], -1);
    BOOST_CHECK_EQUAL(result[2],  1);
}

BOOST_AUTO_TEST_CASE( test_dependent )
{
    do_test_dependent<double>();
    do_test_dependent<float>();
}


