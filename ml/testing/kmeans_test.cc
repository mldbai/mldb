// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

// 
// centroid_feature_generator_test.cc
// Simon Lemieux - 19 Jun 2013
// Copyright (c) 2013 mldb.ai inc. All rights reserved.
// 

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/vfs/filter_streams.h"
#include "ml/kmeans.h"
#include "mldb/utils/testing/fixtures.h"
#include <iostream>
#include <stdlib.h>

using namespace MLDB;
using namespace ML;
using namespace std;

MLDB_FIXTURE( kmeans_test );

BOOST_FIXTURE_TEST_CASE( test_kmeans, kmeans_test )
// BOOST_AUTO_TEST_CASE( test_kmeans)
{

    // Lets' add three "kind of" clusters
    vector<distribution<float>> centroids;
    distribution<float> c1(2);
    c1[0] = 0.;
    c1[1] = 5.;
    distribution<float> c2(2);
    c2[0] = -20.;
    c2[1] = 0.;
    distribution<float> c3(2);
    c3[0] = 10.;
    c3[1] = -20.;
    distribution<float> c4(2);
    c4[0] = -20.;
    c4[1] = -20.;
    centroids = {c1,c2,c3,c4};

    vector<distribution<float>> data;
    int nbPerClass = 10;


    for (int k=0; k < centroids.size(); ++k)
        for (int i=0; i < nbPerClass; i++) {
            distribution<float> point = centroids[k];
            distribution<float> noise(2);
            noise[0] = ((rand() % 100) - 50) / 50.;
            noise[1] = ((rand() % 100) - 50) / 50.;
            data.push_back(point + noise);
        }

    // add trivial points
    // it causes problems for cosine distance
    distribution<float> zero(2);
    zero[0] = 0.;
    zero[1] = 0.;
    for (int i=0; i < nbPerClass; ++i)
        data.push_back(zero);

    KMeans kmeans;
    vector<int> in_cluster;
    // kmeans.train(data, in_cluster, centroids.size());

    auto test = [&] () {
        for (int i=0; i < centroids.size(); ++i) {
            int cluster = in_cluster[nbPerClass * i];
            for (int j=0; j < nbPerClass; ++j) {
                BOOST_CHECK(in_cluster[i*nbPerClass + j] == cluster);
                cerr << in_cluster[i*nbPerClass + j] << " ";
            }
            cerr << endl;
        }
        for (int i=0; i < centroids.size()-1; ++i)
            BOOST_CHECK(in_cluster[i*nbPerClass] != in_cluster[(i+1)*nbPerClass]);
    };

    // test();

    KMeans kmeans2(new KMeansCosineMetric());

    kmeans2.train(data,
                 in_cluster,
                 centroids.size()+1,
                 100);

    test();

    // FIXME finish the test
    kmeans2.save("test_kmeans.bin.gz");

    KMeans kmeans3(new KMeansCosineMetric());
    kmeans3.load("test_kmeans.bin.gz");

    for (int i=0; i<data.size(); ++i)
        in_cluster[i] = kmeans3.assign(data[i]);

    test();

}
