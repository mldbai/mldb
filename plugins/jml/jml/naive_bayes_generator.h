/* naive_bayes_generator.h                                          -*- C++ -*-
   Jeremy Barnes, 15 March 2006
   Copyright (c) 2006 Jeremy Barnes.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Generator for a naive_bayes.
*/

#pragma once

#include "classifier_generator.h"
#include "naive_bayes.h"


namespace ML {


/*****************************************************************************/
/* NAIVE_BAYES_GENERATOR                                                     */
/*****************************************************************************/

/** Class to generate a classifier.  The meta-algorithms (bagging, boosting,
    etc) can use this algorithm to generate weak-learners.
*/

class Naive_Bayes_Generator : public Classifier_Generator {
public:
    Naive_Bayes_Generator();

    virtual ~Naive_Bayes_Generator();

    /** Configure the generator with its parameters. */
    virtual void
    configure(const Configuration & config,
              std::vector<std::string> & unparsedKeys) override;
    
    /** Return to the default configuration. */
    virtual void defaults() override;

    /** Return possible configuration options. */
    virtual Config_Options options() const override;

    /** Initialize the generator, given the feature space to be used for
        generation. */
    virtual void init(std::shared_ptr<const Feature_Space> fs,
                      Feature predicted) override;

    using Classifier_Generator::generate;

    /** Generate a classifier from one training set. */
    virtual std::shared_ptr<Classifier_Impl>
    generate(Thread_Context & context,
             const Training_Data & training_data,
             const distribution<float> & training_weights,
             const std::vector<Feature> & features, int) const override;

    int trace;
    float feature_prop;

    /* Once init has been called, we clone our potential models from this
       one. */
    Naive_Bayes model;

    Naive_Bayes
    train_weighted(Thread_Context & context,
                   const Training_Data & data,
                   const boost::multi_array<float, 2> & weights,
                   const std::vector<Feature> & features_) const;
};


} // namespace ML
