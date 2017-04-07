// This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

/* onevsall_generator.h                                          -*- C++ -*-
   Mathieu Marquis Bolduc, 8 March 2017
   Copyright (c) 2017 MLDB.ai  All rights reserved.
   $Source$

   Generator for a onevsall classifier
*/

#pragma once


#include "classifier_generator.h"
#include "onevsall.h"

namespace ML {


/*****************************************************************************/
/* ONEVSALL_GENERATOR                                                        */
/*****************************************************************************/

class OneVsAll_Classifier_Generator : public Classifier_Generator {
public:
    OneVsAll_Classifier_Generator(std::shared_ptr<Classifier_Generator> weak_learner);
    OneVsAll_Classifier_Generator();

    virtual ~OneVsAll_Classifier_Generator();

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
             const distribution<float> & weights,
             const std::vector<Feature> & features,
             int recursion = 0) const;

    void setMultilabelMapping(const std::vector<std::vector<int>>& uniqueMultiLabelList, size_t numUniqueLabels_) {
        multiLabelList = uniqueMultiLabelList;
        numUniqueLabels = numUniqueLabels_;
    }

    /* Once init has been called, we clone our potential models from this
       one. */
    OneVsAllClassifier model;

    std::shared_ptr<Classifier_Generator> weak_learner;

    std::vector<std::vector<int>> multiLabelList;
    size_t numUniqueLabels;
};


} // namespace ML


