/* boosting_training.h                                             -*- C++ -*-
   Jeremy Barnes, 16 March 2006
   Copyright (c) 2006 Jeremy Barnes.   All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Algorithms and data structures for boosting of any type of classifier.
*/

#pragma once

#include <boost/multi_array.hpp>
#include "training_data.h"
#include "stump.h"

namespace ML {

/** This enum is used to control the cost function that is used.  It is
    passed under the key "cost_function" in the training params. */
enum Cost_Function {
    CF_EXPONENTIAL,  ///< Use an exponential cost function (AdaBoost)
    CF_LOGISTIC      ///< Use a logistic cost function (~LogitBoost)
};

/** Update the weights which we have been maintaining for a set of data,
    using the last decision stump learned.  This can remove an O(n) from
    the iterative training complexity. */
void update_scores(boost::multi_array<float, 2> & example_scores,
                   const Training_Data & data,
                   const Stump & stump,
                   const Optimization_Info & opt_info);

void update_scores(boost::multi_array<float, 2> & example_scores,
                   const Training_Data & data,
                   const Classifier_Impl & classifier,
                   const Optimization_Info & opt_info);

/** Update the weights which we have been maintaining for a set of data,
    using the set of last decision stumps learned.  This can remove an
    O(n) from the iterative training complexity. */
void update_scores(boost::multi_array<float, 2> & example_scores,
                   const Training_Data & data,
                   const std::vector<Stump> & stumps,
                   const std::vector<Optimization_Info> & opt_info);

void update_weights(boost::multi_array<float, 2> & weights,
                    const Stump & stump,
                    const Training_Data & data,
                    Cost_Function cost,
                    bool bin_sym);

void update_weights(boost::multi_array<float, 2> & weights,
                    const std::vector<Stump> & stumps,
                    const std::vector<Optimization_Info> & opt_info,
                    const distribution<float> & cl_weights,
                    const Training_Data & data,
                    Cost_Function cost,
                    bool bin_sym);

} // namespace ML

DECLARE_ENUM_INFO(ML::Cost_Function, 2);
