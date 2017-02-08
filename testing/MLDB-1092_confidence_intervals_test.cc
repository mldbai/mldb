// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/*
   confidence_interval_test.cc
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#include "ml/confidence_intervals.h"
#include <boost/test/unit_test.hpp>

using namespace std;
using namespace ML;
using namespace MLDB;


BOOST_AUTO_TEST_CASE( conf_intervals )
{
    ConfidenceIntervals ci(0.5);
    vector<double> sample = {1.0,1.0,2.0,3.0,3.0};
    BOOST_CHECK_GE(2, ci.bootstrapMeanLowerBound(sample, 10000, 4));
    BOOST_CHECK_LE(2, ci.bootstrapMeanUpperBound(sample, 10000, 4));
    auto b = ci.bootstrapMeanTwoSidedBound(sample, 10000, 4);
    BOOST_CHECK_GE(2, b.first);
    BOOST_CHECK_LE(2, b.second);
}

BOOST_AUTO_TEST_CASE( conf_intervals_wilson )
{
    // from boost doc:
    // In order to obtain a two sided bound on the success fraction, you call both find_lower_bound_on_p and find_upper_bound_on_p each with the same arguments. If the desired risk level that the true success fraction lies outside the bounds is α, then you pass α/2 to these functions. So for example a two sided 95% confidence interval would be obtained by passing α = 0.025 to each of the functions. 
    float confidence = 0.8;
    float alpha = (1 - confidence) / 2;

    // comparing against http://epitools.ausvet.com.au/content.php?page=CIProportion&SampleSize=200&Positive=35&Conf=0.8&Digits=3
    // and https://gist.github.com/paulgb/6627336

    cout << "Wilson" << endl;
    ConfidenceIntervals cI(alpha, "wilson");
    BOOST_CHECK_CLOSE( cI.binomialLowerBound(200, 35), 0.1432, 0.1);
    BOOST_CHECK_CLOSE( cI.binomialUpperBound(200, 35), 0.212, 0.1);

    cout << "CP" << endl;
    ConfidenceIntervals cI2(alpha, "clopper_pearson");
    BOOST_CHECK_CLOSE( cI2.binomialLowerBound(200, 35), 0.14065, 0.1);
    BOOST_CHECK_CLOSE( cI2.binomialUpperBound(200, 35), 0.2144, 0.1);
}

