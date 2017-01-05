// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* bagging_generator.h                                          -*- C++ -*-
   Jeremy Barnes, 15 March 2006
   Copyright (c) 2006 Jeremy Barnes.  All rights reserved.
   $Source$

   Generator for a bagging.
*/

#ifndef __boosting__bagging_generator_h__
#define __boosting__bagging_generator_h__


#include "mldb/ml/jml/early_stopping_generator.h"


namespace ML {

/*****************************************************************************/
/* BAGGING_GENERATOR_CONFIG                                                  */
/*****************************************************************************/
struct Bagging_Generator_Config : Classifier_Generator_Config {
    std::shared_ptr<Classifier_Generator> weak_learner;
    int num_bags;
    float validation_split;

    Bagging_Generator_Config();
    virtual void validateFct() override;
    virtual void defaults() override;
};
DECLARE_STRUCTURE_DESCRIPTION(Bagging_Generator_Config);

/*****************************************************************************/
/* BAGGING_GENERATOR                                                         */
/*****************************************************************************/

/** Class to generate a classifier.  The meta-algorithms (bagging, boosting,
    etc) can use this algorithm to generate weak-learners.
*/

class Bagging_Generator : public Classifier_Generator {
public:
    Bagging_Generator();

    virtual ~Bagging_Generator();

    /** Initialize the generator, given the feature space to be used for
        generation. */
    virtual void init(std::shared_ptr<const Feature_Space> fs,
                      Feature predicted);

    using Classifier_Generator::generate;

    /** Generate a classifier from one training set. */
    virtual std::shared_ptr<Classifier_Impl>
    generate(Thread_Context & context,
             const Training_Data & training_data,
             const distribution<float> & ex_weights,
             const std::vector<Feature> & features,
             int recursion) const;

};


} // namespace ML


#endif /* __boosting__bagging_generator_h__ */
