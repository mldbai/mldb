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

#include "mldb/ml/jml/glz_classifier_generator.h"
#include "mldb/ml/jml/training_data.h"
#include "mldb/ml/jml/dense_features.h"
#include "mldb/ml/jml/feature_info.h"
#include "mldb/ml/jml/training_index.h"
#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/arch/exception_handler.h"

using namespace ML;
using namespace std;

using boost::unit_test::test_suite;

static const char * config_options = "\
verbosity=3\n\
";

int nfv = 1000;


BOOST_AUTO_TEST_CASE( test_glz_classifier_test )
{
    cerr << endl << endl << endl << "test test" << endl;

    /* Create the dataset */

    Dense_Feature_Space fs;
    fs.add_feature("LABEL", Feature_Info(BOOLEAN, false, true));
    fs.add_feature("feature1", REAL);
    fs.add_feature("feature2", REAL);

    std::shared_ptr<Dense_Feature_Space> fsp(make_unowned_sp(fs));

    Training_Data data(fsp);
    
    //float NaN = std::numeric_limits<float>::quiet_NaN();

    for (unsigned i = 0;  i < nfv;  ++i) {
        distribution<float> features;

        features.push_back(i % 3  == 0);
        features.push_back(i % 3  == 0);
        features.push_back(i % 5  == 0);

        std::shared_ptr<Feature_Set> fset
            = fs.encode(features);

        data.add_example(fset);
    }

    /* Create the decision tree generator */
    Configuration config;
    config.parse_string(config_options, "inbuilt config file");

    GLZ_Classifier_Generator generator;
    vector<string> unparsedKeys; // should be used to root out invalid keys
    generator.configure(config, unparsedKeys);
    generator.init(fsp, fs.features()[0]);

    distribution<float> training_weights(nfv, 1);

    vector<Feature> features = fs.features();
    features.erase(features.begin(), features.begin() + 1);

    Thread_Context context;

    std::shared_ptr<Classifier_Impl> classifier
        = generator.generate(context, data, training_weights, features);

    float accuracy MLDB_UNUSED = classifier->accuracy(data).first;

    cerr << "accuracy = " << accuracy << endl;

    BOOST_CHECK_EQUAL(accuracy, 1);
}

BOOST_AUTO_TEST_CASE( test_glz_classifier_missing )
{
    cerr << endl << endl << endl << "missing1" << endl;

    /* In this test, feature1a and feature1b contain the information about
       the label between them.  Exactly one of them is always present. */
    
    /* Create the dataset */

    Dense_Feature_Space fs;
    fs.add_feature("LABEL", Feature_Info(BOOLEAN, false, true));
    fs.add_feature("feature1a", REAL);
    fs.add_feature("feature1b", REAL);
    fs.add_feature("feature2",  REAL);

    std::shared_ptr<Dense_Feature_Space> fsp(make_unowned_sp(fs));

    Training_Data data(fsp);
    
    float NaN = std::numeric_limits<float>::quiet_NaN();

    for (unsigned i = 0;  i < nfv;  ++i) {
        distribution<float> features;

        features.push_back(i % 3  == 0);
        if (i % 2 == 0) {
            features.push_back(i % 3  == 0);
            features.push_back(NaN);
        }
        else {
            features.push_back(NaN);
            features.push_back(i % 3  == 0);
        }
        features.push_back(i % 5  == 0);

        std::shared_ptr<Feature_Set> fset
            = fs.encode(features);

        data.add_example(fset);
    }

    /* Create the decision tree generator */
    Configuration config;
    config.parse_string(config_options, "inbuilt config file");

    GLZ_Classifier_Generator generator;
    vector<string> unparsedKeys; // should be used to root out invalid keys
    generator.configure(config, unparsedKeys);
    generator.init(fsp, fs.features()[0]);

    distribution<float> training_weights(nfv, 1);

    vector<Feature> features = fs.features();
    features.erase(features.begin(), features.begin() + 1);

    Thread_Context context;

    std::shared_ptr<Classifier_Impl> classifier
        = generator.generate(context, data, training_weights, features);

    float accuracy MLDB_UNUSED = classifier->accuracy(data).first;

    cerr << "accuracy = " << accuracy << endl;

    BOOST_CHECK_EQUAL(accuracy, 1);

    BOOST_CHECK_EQUAL(classifier->all_features(), features);

    // Check that we can optimize
    Optimization_Info info = classifier->optimize(fs.features());

    BOOST_CHECK(info);
}

BOOST_AUTO_TEST_CASE( test_glz_classifier_missing2 )
{
    cerr << endl << endl << endl << "missing2" << endl;


    /* In this test, feature1 contains the information about
       the label: if it's missing, the label is 1. */
    
    /* Create the dataset */

    Dense_Feature_Space fs;
    fs.add_feature("LABEL", Feature_Info(BOOLEAN, false, true));
    fs.add_feature("feature1", REAL);
    fs.add_feature("feature2",  REAL);

    std::shared_ptr<Dense_Feature_Space> fsp(make_unowned_sp(fs));

    Training_Data data(fsp);
    
    float NaN = std::numeric_limits<float>::quiet_NaN();

    for (unsigned i = 0;  i < nfv;  ++i) {
        distribution<float> features;

        features.push_back(i % 3  == 0);

        if (i % 3 == 0)
            features.push_back(0.0);
        else features.push_back(NaN);
        features.push_back(i % 5  == 0);

        std::shared_ptr<Feature_Set> fset
            = fs.encode(features);
        
        data.add_example(fset);
    }

    BOOST_CHECK(data.index().constant(Feature(1)));


    /* Create the decision tree generator */
    Configuration config;
    config.parse_string(config_options, "inbuilt config file");

    GLZ_Classifier_Generator generator;
    vector<string> unparsedKeys; // should be used to root out invalid keys
    generator.configure(config, unparsedKeys);
    generator.init(fsp, fs.features()[0]);

    distribution<float> training_weights(nfv, 1);

    vector<Feature> features = fs.features();
    features.erase(features.begin(), features.begin() + 1);

    Thread_Context context;

    std::shared_ptr<Classifier_Impl> classifier
        = generator.generate(context, data, training_weights, features);

    float accuracy MLDB_UNUSED = classifier->accuracy(data).first;

    cerr << "accuracy = " << accuracy << endl;

    BOOST_CHECK_EQUAL(accuracy, 1);

    // Check that we can optimize
    Optimization_Info info = classifier->optimize(fs.features());

    BOOST_CHECK(info);
}

#define do_decode(val, type)                           \
    classifier.decode_value(val, \
                            GLZ_Classifier::Feature_Spec(Feature(1),    \
                                                         GLZ_Classifier::Feature_Spec::type))

BOOST_AUTO_TEST_CASE( test_decode_value )
{
    Dense_Feature_Space fs;
    fs.add_feature("LABEL", Feature_Info(BOOLEAN, false, true));
    fs.add_feature("feature1", REAL);
    fs.add_feature("feature2",  REAL);

    GLZ_Classifier classifier(make_unowned_sp(fs), Feature(0));

    float NaN = std::numeric_limits<float>::quiet_NaN();
    float Inf = INFINITY;

    BOOST_CHECK_EQUAL(do_decode(0.0, VALUE), 0.0);
    BOOST_CHECK_EQUAL(do_decode(1.0, VALUE), 1.0);

    BOOST_CHECK_EQUAL(do_decode(0.0, VALUE_IF_PRESENT), 0.0);
    BOOST_CHECK_EQUAL(do_decode(1.0, VALUE_IF_PRESENT), 1.0);
    BOOST_CHECK_EQUAL(do_decode(NaN, VALUE_IF_PRESENT), 0.0);

    BOOST_CHECK_EQUAL(do_decode(0.0, PRESENCE), 1.0);
    BOOST_CHECK_EQUAL(do_decode(1.0, PRESENCE), 1.0);
    BOOST_CHECK_EQUAL(do_decode(Inf, PRESENCE), 1.0);
    BOOST_CHECK_EQUAL(do_decode(NaN, PRESENCE), 0.0);

    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(do_decode(Inf, VALUE), Exception);
        BOOST_CHECK_THROW(do_decode(Inf, VALUE_IF_PRESENT), Exception);
        
        // TODO: re-enable this; make throwing on missing values an option
        //BOOST_CHECK_THROW(do_decode(NaN, VALUE), Exception);
    }
}
