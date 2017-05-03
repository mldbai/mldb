/* noise_injector_test.cc
   Guy Dumais, 3 May 2017

   This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

   Test of the noise injector
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/utils/noise_injector.h"
#include <boost/test/unit_test.hpp>
#include <iostream>

using namespace std;
using namespace MLDB;

BOOST_AUTO_TEST_CASE(test_uniform)
{
    NoiseInjector noise;
    double sum = 0;
    uint samples = 10000;
    
    for(int i = 0; i < samples; i++)
        sum += noise.rand_uniform();

    cerr << "average of uniform sampling should be small: " << sum / samples << endl;
    BOOST_CHECK_SMALL(sum/samples, 0.01);
}

BOOST_AUTO_TEST_CASE(test_laplace)
{
    NoiseInjector noise;
    double inRange = 0;
    uint samples = 10000;
    double sum = 0;
    
    for(int i = 0; i < samples; i++) {
        double sample = noise.sample_laplace();
        if (sample > -2*noise.b && sample < 2*noise.b)
            inRange++;
        sum += sample;
    }

    cerr << "with mu = 0 most of the mass is between [-2b, 2b]: " << inRange / samples << endl;
    BOOST_CHECK_SMALL((samples - inRange)/samples, 0.2);
}

BOOST_AUTO_TEST_CASE(add_noise)
{
    NoiseInjector noise;
    uint inRange = 0;
    uint samples = 100000;
    int64_t count = 729;
    double smaller = 0;
    double bigger = 0;
    
    for(int i = 0; i < samples; i++) {
        uint64_t sample = noise.add_noise(count);
        if (sample > -2*noise.b + count && sample < 2*noise.b + count) {
            inRange++;
        }
        if (sample > count)
            bigger++;
        if (sample < count)
            smaller++;
    }

    cerr << "with mu = 0 most of the mass is between [-2b, 2b]: " << inRange / samples << endl;
    cerr << "values smaller and bigger to the target should be closed in count: bigger " << bigger 
         << " smaller " << smaller << endl;
    BOOST_CHECK_SMALL((samples - inRange)*1.0/samples, 0.2);
    BOOST_CHECK_CLOSE(bigger, smaller, 2); // the difference should be less than 2% of the values
}
