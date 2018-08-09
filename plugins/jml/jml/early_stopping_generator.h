// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* early_stopping_generator.h                                      -*- C++ -*-
   Jeremy Barnes, 17 March 2006
   Copyright (c) 2006 Jeremy Barnes.  All rights reserved.
   $Source$

   A classifier generator base that uses early stopping to control capacity.
*/

#ifndef __boosting__early_stopping_generator_h__
#define __boosting__early_stopping_generator_h__

#include "classifier_generator.h"

namespace ML {


/*****************************************************************************/
/* EARLY_STOPPING_GENERATOR                                                  */
/*****************************************************************************/

/** Class to generate a classifier.  The meta-algorithms (bagging, boosting,
    etc) can use this algorithm to generate weak-learners.
*/

class Early_Stopping_Generator : public Classifier_Generator {
public:
    virtual ~Early_Stopping_Generator();

    /** Configure the generator with its parameters. */
    virtual void
    configure(const Configuration & config,
              std::vector<std::string> & unparsedKeys) override;
    
    /** Return to the default configuration. */
    virtual void defaults() override;

    /** Return possible configuration options. */
    virtual Config_Options options() const override;

    using Classifier_Generator::generate;

    /** Generate a classifier from a training set with data weighted
        by example.

        Splits it up into a training and a validation set, and then
        calls the expanded generate method.
    */
    virtual std::shared_ptr<Classifier_Impl>
    generate(Thread_Context & context,
             const Training_Data & training_data,
             const distribution<float> & ex_weights,
             const std::vector<Feature> & features,
             int recursion = 0) const override;
    
    float validate_split;
};

} // namespace ML

#endif /* __boosting__early_stopping_generator_h__ */
