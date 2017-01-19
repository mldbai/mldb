// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* bucketing_probabilizer_test.cc
Francois Maillet, 13 mars 2013
Copyright (c) 2013 mldb.ai inc. All rights reserved.

*/


#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <math.h>
#include <boost/test/unit_test.hpp>
#include <boost/static_assert.hpp>
#include <boost/test/floating_point_comparison.hpp>
using boost::test_toolbox::close_at_tolerance;
#include "ml/bucketing_probabilizer.h"

#include <string>
#include <vector>

#define PI 3.14159265

using namespace std;
using namespace MLDB;


BOOST_AUTO_TEST_CASE( getProb_test )
{
    auto doTest = [] (Bucketing_Probabilizer & prob) 
        {
            // Check bucket caching
            vector<double> ctrs = prob.getCTRs();
            BOOST_CHECK_CLOSE(ctrs[0], 0.01, /* tolerance */ 1e-4 );
            BOOST_CHECK_CLOSE(ctrs[1], 1/10.0, /* tolerance */ 1e-4 );
            BOOST_CHECK_CLOSE(ctrs[2], 2/10.0, /* tolerance */ 1e-4 );
            BOOST_CHECK_CLOSE(ctrs[3], 5/10.0, /* tolerance */ 1e-4 );

            // Check probs with smoothing
            BOOST_CHECK_CLOSE(prob.getProb(0.15), 0.15, /* tolerance */ 1e-4 );
            BOOST_CHECK_CLOSE(prob.getProb(0.75), 0.40625, /* tolerance */ 1e-4 );

            BOOST_CHECK_CLOSE(prob.getProb(0), 0.01, /* tolerance */ 1e-4 ); // first bucket
            BOOST_CHECK_CLOSE(prob.getProb(2000), 1, /* tolerance */ 1e-4 ); // last bucket
        };


     Prediction_Accumulator::CTR_Buckets ctr_buckets = 
        {{0,  {1, 100}},
        {0.1, {1, 10}},
        {0.2, {2, 10}},
        {1,   {5, 10}},
        {1000,   {10, 10}}};

    string name = "bouya";
    string prob_type = "raw";

    Bucketing_Probabilizer prob(name, prob_type, ctr_buckets);
    doTest(prob);

    // Save and reload
//     string filename = "build/x86_64/tests/bucketing_prob_test.bin.gz";
//     prob.save(filename);
// 
//     Bucketing_Probabilizer prob2(filename);
//     doTest(prob2);
}

BOOST_AUTO_TEST_CASE( getProb_decreasing_bucket_cxr_test )
{
    auto doTest = [] (Bucketing_Probabilizer & prob) 
        {
            // Check bucket caching
            vector<double> ctrs = prob.getCTRs();
            BOOST_CHECK_CLOSE(ctrs[0], 0.01, /* tolerance */ 1e-4 );
            BOOST_CHECK_CLOSE(ctrs[1], 1/10.0, /* tolerance */ 1e-4 );
            BOOST_CHECK_CLOSE(ctrs[2], 0.01, /* tolerance */ 1e-4 );
            BOOST_CHECK_CLOSE(ctrs[3], 5/10.0, /* tolerance */ 1e-4 );

            // Check probs with smoothing
            BOOST_CHECK_CLOSE(prob.getProb(0.15), 0.055, /* tolerance */ 1e-4 );
        };


     Prediction_Accumulator::CTR_Buckets ctr_buckets = 
        {{0,  {1, 100}},
        {0.1, {1, 10}},
        {0.2, {1, 100}},    // decreasing back gown
        {1,   {5, 10}}};

    string name = "bouya";
    string prob_type = "raw";

    Bucketing_Probabilizer prob(name, prob_type, ctr_buckets);
    doTest(prob);
}

// currently should not return 0 for a prob. it should return a tiny prob
BOOST_AUTO_TEST_CASE( getProb_zero_prob )
{
    auto doTest = [] (Bucketing_Probabilizer & prob) 
        {
            float tinyProb = 0.0000001;

            // Check bucket caching
            vector<double> ctrs = prob.getCTRs();
            BOOST_CHECK_CLOSE(ctrs[0], tinyProb, /* tolerance */ 1e-4 );

            // Check probs with smoothing
            BOOST_CHECK_CLOSE(prob.getProb(0.05), tinyProb, /* tolerance */ 1e-4 );
        };


     Prediction_Accumulator::CTR_Buckets ctr_buckets = 
        {{0,  {0, 100}},
        {0.1, {0, 10}},
        {0.2, {1, 2}}};

    string name = "bouya";
    string prob_type = "raw";

    Bucketing_Probabilizer prob(name, prob_type, ctr_buckets);
    doTest(prob);
}


BOOST_AUTO_TEST_CASE( autoProb_test )
{
    // Generate test data
    Prediction_Accumulator predAccum;

    auto myRand = [] () { return ((double) rand() / (RAND_MAX)); };

    auto f  = [&] (int x) { return sin(x)*(x/2)+(x/75.0*myRand())+1; };
    auto f2 = [&] (int x) { return f(x)+(f(x)/4); };
    auto rndNorm = [&] (float mu=0) -> vector<double>
        {
            // Sample two numbers from normal distribution using Box-Muller transform
            float r = myRand();
            float r2 = myRand();
            return {(sqrt(-2*log(r)) * sin(2*PI*r2))+mu,
                    (sqrt(-2*log(r)) * cos(2*PI*r2))+mu};
        };

    for(int x=0; x<=2500; x++) {
        float ctr = f2(x)/100000.0;
        for(int pt=0; pt<=14; pt++) {
            for(auto pos : rndNorm(x)) {
                bool click = myRand()<ctr;
                predAccum.addPrediction(click, (float)pos);
            }
        }
    }

    auto ctr_buckets =
        Bucketing_Probabilizer::autoTrainBucketingProbabilizer(predAccum); 

    BOOST_REQUIRE(ctr_buckets.size() < 75);

    // Make sure CTR is good
    float lastCTR = -1;
    for(auto bucket : ctr_buckets) {
        float clicks = bucket.second.first;
        float imps = bucket.second.second;
        float currCTR = clicks/imps;
        BOOST_REQUIRE(lastCTR*0.85 < currCTR);
        lastCTR = currCTR;
    }
}


// essentially the same test as in vm_probabilizers_test.coffee, without the
// dataset handling
BOOST_AUTO_TEST_CASE( uidWeightedProbabilizerTraining )
{
    // Generate test data
    Prediction_Accumulator predAccum;

    predAccum.addPrediction(1, 0.25, Utf8String("a"));
    predAccum.addPrediction(1, 0.25, Utf8String("a"));
    predAccum.addPrediction(1, 0.25, Utf8String("a"));
    predAccum.addPrediction(0, 0.25, Utf8String("weras"));
    
    predAccum.addPrediction(1, 0.5, Utf8String("b"));
    predAccum.addPrediction(0, 0.5, Utf8String("asdf"));
    predAccum.addPrediction(0, 0.5, Utf8String("asdf23"));
    predAccum.addPrediction(1, 0.5, Utf8String("b"));
    
    predAccum.addPrediction(1, 0.75, Utf8String("c"));
    predAccum.addPrediction(0, 0.75, Utf8String("z"));
    predAccum.addPrediction(1, 0.75, Utf8String("a"));

    auto ctr_buckets = Bucketing_Probabilizer::
            trainBucketingProbabilizer(predAccum, 3);


    // Make sure CTR is good
    BOOST_REQUIRE(ctr_buckets.size() == 3);

    BOOST_REQUIRE(ctr_buckets[0].second.first  == 3/4.0);
    BOOST_REQUIRE(ctr_buckets[0].second.second == 4);

    BOOST_REQUIRE(ctr_buckets[1].second.first  == 1);
    BOOST_REQUIRE(ctr_buckets[1].second.second == 4);

    BOOST_REQUIRE(ctr_buckets[2].second.first  == (1/4.0 + 1));
    BOOST_REQUIRE(ctr_buckets[2].second.second == 3);
}


BOOST_AUTO_TEST_CASE( test_extremes_with_weighted_probs )
{
    Json::Value root;
    Json::Reader reader;

    string js = "[[0.4146241545677185,[0.005565082654356956,164846.0]],"
                "[0.4151647686958313,[0.2207595258951187,164835.0]],"
                "[0.4158402383327484,[0.9053164124488831,164838.0]],"
                "[3.402823466385289e+38,[2.000886678695679,164806.0]]]";

    bool parsingSuccessful = reader.parse(js, root);
    BOOST_REQUIRE(parsingSuccessful);

    auto prob = Bucketing_Probabilizer("test", "raw",
            Bucketing_Probabilizer::jsValToCtrBuckets(root));

    BOOST_CHECK_CLOSE(prob.getProb(0), 3.375928232627395e-8, /* tolerance */ 1e-4);
    BOOST_CHECK_CLOSE(prob.getProb(0.41550250351428986), 0.0000034156, /* tolerance */ 1e-2);
    BOOST_CHECK_CLOSE(prob.getProb(0.42), 0.000012140860640363088, /* tolerance */ 1e-4);
    BOOST_CHECK_CLOSE(prob.getProb(1), 0.000012140860640363088, /* tolerance */ 1e-4);
}

