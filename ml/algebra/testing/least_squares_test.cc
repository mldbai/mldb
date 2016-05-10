// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* least_squares_test.cc
   Jeremy Barnes, 25 February 2008
   Copyright (c) 2008 Jeremy Barnes.  All rights reserved.

   Test of the least squares class.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <boost/test/floating_point_comparison.hpp>
#include <boost/thread.hpp>
#include <boost/thread/barrier.hpp>

#include <vector>
#include <stdint.h>
#include <iostream>

#include "mldb/ml/algebra/least_squares.h"
#include "mldb/ml/algebra/matrix_ops.h"

using namespace ML;
using namespace std;

using boost::unit_test::test_suite;
using namespace boost::test_tools;

template<typename Float>
void do_test1()
{
    /* See https://www-old.cae.wisc.edu/pipermail/bug-octave/2007-October/003689.html for the test case */

    boost::multi_array<Float, 2> A(boost::extents[5][4]);
    distribution<Float> b(5), x(4);

    A[0][0] = 0.00002;
    A[1][0] = 0.00003;
    A[2][0] = 0.00004;
    A[3][0] = 0.00007;
    A[4][0] = 0.00010;

    for (unsigned i = 0;  i < 5;  ++i)
        A[i][1] = 0.0;

    for (unsigned i = 0;  i < 5;  ++i)
        A[i][2] = 0.0;

    for (unsigned i = 0;  i < 5;  ++i)
        A[i][3] = 1.0;

    b[0] = 3.0389e-07;
    b[1] = 3.5608e-07;
    b[2] = 4.1412e-07;
    b[3] = 4.9866e-07;
    b[4] = 5.8619e-07;

    x[0] = 0.0034078;
    x[1] = 0.0000000;
    x[2] = 0.0000000;
    x[3] = 0.0000003;

    distribution<Float> x2 = least_squares(A, b);

    cerr << "A = " << A << endl;
    cerr << "x = " << x << endl;
    cerr << "x2 = " << x2 << endl;
    cerr << "b = " << b << endl;

    cerr << "A x  = " << (A * x) << endl;
    cerr << "A x2 = " << (A * x2) << endl;
    cerr << "A x - b = " << ((A * x) - b) << endl; 
    cerr << "A x2 - b = " << ((A * x2) - b) << endl; 

    Float tol = 20; /* percent */

    BOOST_REQUIRE_EQUAL(x.size(), 4);
    BOOST_CHECK_CLOSE(x2[0], (Float)0.0034078, tol);
    BOOST_CHECK_LT(fabs(x2[1]), 1e-5);
    BOOST_CHECK_LT(fabs(x2[2]), 1e-5);
    BOOST_CHECK_CLOSE(x2[3], (Float)0.0000003, tol);
}

BOOST_AUTO_TEST_CASE( test1 )
{
    do_test1<double>();
    do_test1<float>();
}


template<typename Float>
void do_test2()
{
    /* See https://www-old.cae.wisc.edu/pipermail/bug-octave/2007-October/003689.html for the test case */

    boost::multi_array<Float, 2> A(boost::extents[5][4]);
    distribution<Float> b(5), x(4);

    A[0][0] = 0.00002;
    A[1][0] = 0.00003;
    A[2][0] = 0.00004;
    A[3][0] = 0.00007;
    A[4][0] = 0.00010;

    for (unsigned i = 0;  i < 5;  ++i)
        A[i][1] = 0.0;

    for (unsigned i = 0;  i < 5;  ++i)
        A[i][2] = 0.0;

    for (unsigned i = 0;  i < 5;  ++i)
        A[i][3] = 1.0;

    b[0] = 3.0389e-07;
    b[1] = 3.5608e-07;
    b[2] = 4.1412e-07;
    b[3] = 4.9866e-07;
    b[4] = 5.8619e-07;

    x[0] = 0.0034078;
    x[1] = 0.0000000;
    x[2] = 0.0000000;
    x[3] = 0.0000003;

    distribution<Float> x2 = ridge_regression(A, b, 0.0000000001);

    cerr << "A = " << A << endl;
    cerr << "x = " << x << endl;
    cerr << "x2 = " << x2 << endl;
    cerr << "b = " << b << endl;

    cerr << "A x  = " << (A * x) << endl;
    cerr << "A x2 = " << (A * x2) << endl;
    cerr << "A x - b = " << ((A * x) - b) << endl; 
    cerr << "A x2 - b = " << ((A * x2) - b) << endl; 

    Float tol = 20; /* percent */

    BOOST_REQUIRE_EQUAL(x.size(), 4);

    BOOST_CHECK_CLOSE(x2[0], (Float)0.0034078, tol);

    if (x2[1] == 0.0)
        BOOST_CHECK_CLOSE(x2[1], (Float)0.0000000, tol);
    else BOOST_CHECK(abs(x2[1]) < 1e-10);

    if (x2[2] == 0.0)
        BOOST_CHECK_CLOSE(x2[2], (Float)0.0000000, tol);
    else BOOST_CHECK(abs(x2[2]) < 1e-10);
    
    BOOST_CHECK_CLOSE(x2[3], (Float)0.0000003, tol);
}

template<typename Float>
void do_test3()
{
    /* See https://www-old.cae.wisc.edu/pipermail/bug-octave/2007-October/003689.html for the test case */

    boost::multi_array<Float, 2> A(boost::extents[5][4]);
    distribution<Float> b(5), x(4);

    A[0][0] = 0.00002;
    A[1][0] = 0.00003;
    A[2][0] = 0.00004;
    A[3][0] = 0.00007;
    A[4][0] = 0.00010;

    for (unsigned i = 0;  i < 5;  ++i)
        A[i][1] = 0.0;

    for (unsigned i = 0;  i < 5;  ++i)
        A[i][2] = 0.0;

    for (unsigned i = 0;  i < 5;  ++i)
        A[i][3] = 1.0;

    b[0] = 3.0389e-07;
    b[1] = 3.5608e-07;
    b[2] = 4.1412e-07;
    b[3] = 4.9866e-07;
    b[4] = 5.8619e-07;

    x[0] = 0.0034078;
    x[1] = 0.0000000;
    x[2] = 0.0000000;
    x[3] = 0.0000003;

    distribution<Float> x2 = ridge_regression(A, b, -1);

    cerr << "A = " << A << endl;
    cerr << "x = " << x << endl;
    cerr << "x2 = " << x2 << endl;
    cerr << "b = " << b << endl;

    cerr << "A x  = " << (A * x) << endl;
    cerr << "A x2 = " << (A * x2) << endl;
    cerr << "A x - b = " << ((A * x) - b) << endl; 
    cerr << "A x2 - b = " << ((A * x2) - b) << endl; 

    Float tol = 20; /* percent */

    BOOST_REQUIRE_EQUAL(x.size(), 4);

    BOOST_CHECK_CLOSE(x2[0], (Float)0.0034078, tol);

    if (x2[1] == 0.0)
        BOOST_CHECK_CLOSE(x2[1], (Float)0.0000000, tol);
    else BOOST_CHECK(abs(x2[1]) < 1e-10);

    if (x2[2] == 0.0)
        BOOST_CHECK_CLOSE(x2[2], (Float)0.0000000, tol);
    else BOOST_CHECK(abs(x2[2]) < 1e-10);
    
    BOOST_CHECK_CLOSE(x2[3], (Float)0.0000003, tol);
}

BOOST_AUTO_TEST_CASE( test2 )
{
    do_test2<double>();
    do_test2<float>();
}

BOOST_AUTO_TEST_CASE( test3 )
{
    do_test3<double>();
    do_test3<float>();
}

template<typename Float>
void do_test_lasso()
{
    /* See https://www-old.cae.wisc.edu/pipermail/bug-octave/2007-October/003689.html for the test case */

    boost::multi_array<Float, 2> A(boost::extents[5][4]);
    distribution<Float> b(5);

    A[0][0] = 0.00002;
    A[1][0] = 0.00003;
    A[2][0] = 0.00004;
    A[3][0] = 0.00007;
    A[4][0] = 0.00010;

    for (unsigned i = 0;  i < 5;  ++i)
        A[i][1] = 0.0;

    for (unsigned i = 0;  i < 5;  ++i)
        A[i][2] = 0.0;

    for (unsigned i = 0;  i < 5;  ++i)
        A[i][3] = 1.0;

    b[0] = 3.0389e-07;
    b[1] = 3.5608e-07;
    b[2] = 4.1412e-07;
    b[3] = 4.9866e-07;
    b[4] = 5.8619e-07;

    distribution<Float> x = lasso_regression(A, b, 0.0000000001);

    cerr << "A = " << A << endl;
    cerr << "x = " << x << endl;
    cerr << "b = " << b << endl;

    cerr << "A x  = " << (A * x) << endl;
    cerr << "A x - b = " << ((A * x) - b) << endl; 

    auto error = ((A * x) - b);

    BOOST_REQUIRE_EQUAL(x.size(), 4);

    BOOST_CHECK(abs(error[0]) < 1e-5);
    BOOST_CHECK(abs(error[1]) < 1e-5);
    BOOST_CHECK(abs(error[2]) < 1e-5);
    BOOST_CHECK(abs(error[3]) < 1e-5);

    //check that lasso sparsifies by putting the first 3 weights to zero

    BOOST_CHECK(abs(x[0]) == 0);
    BOOST_CHECK(abs(x[1]) == 0);
    BOOST_CHECK(abs(x[2]) == 0);
}

template<typename Float>
void do_test_lasso2()
{
    /* See https://www-old.cae.wisc.edu/pipermail/bug-octave/2007-October/003689.html for the test case */

    boost::multi_array<Float, 2> A(boost::extents[5][4]);
    distribution<Float> b(5), secret(4);

    A[0][0] = 1;
    A[1][0] = 3;
    A[2][0] = 5;
    A[3][0] = 7;
    A[4][0] = 11;

    for (unsigned j = 1;  j < 4;  ++j){
        for (unsigned i = 0;  i < 5;  ++i){
            A[i][j] = A[i][0] * (j+1);
        }
    }

    secret[0] = 1;
    secret[1] = 1;
    secret[2] = 1;
    secret[3] = 1;

    b = A*secret;

    cerr << "A = " << A << endl;
    cerr << "b = " << b << endl;

    distribution<Float> x = lasso_regression(A, b, /*1e-5*/0.001);
    
    cerr << "x = " << x << endl;
    cerr << "A x  = " << (A * x) << endl;
    cerr << "A x - b = " << ((A * x) - b) << endl; 

    auto error = ((A * x) - b);

    BOOST_CHECK(abs(error[0]) < 1e-5);
    BOOST_CHECK(abs(error[1]) < 1e-5);
    BOOST_CHECK(abs(error[2]) < 1e-5);
    BOOST_CHECK(abs(error[3]) < 1e-5);

    BOOST_CHECK(abs(x[0]) > 1);
    BOOST_CHECK(abs(x[1]) < 1e-5);
    BOOST_CHECK(abs(x[2]) < 1e-5);
    BOOST_CHECK(abs(x[2]) < 1e-5);

}

BOOST_AUTO_TEST_CASE( test_lasso )
{   
    do_test_lasso<double>();
    do_test_lasso<double>();
    do_test_lasso2<float>();
    do_test_lasso2<float>();
}


