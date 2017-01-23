// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** value_descriptions.h                                           -*- C++ -*-
    Jeremy Barnes, 5 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#pragma once

#include "mldb/ml/jml/feature_info.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/ml/algebra/irls.h"

namespace ML {

struct Classifier_Impl;
struct Feature_Space;

} // namespace ML

namespace ML {

/** Scope object allowing a feature space to be entered so that functions that
    print or parse features can use the feature space.  This pushes the given
    context onto a thread specific stack.
*/
struct FeatureSpaceContext {
    FeatureSpaceContext(const ML::Feature_Space * fs)
        : entered(fs)
    {
        push(fs);
    }

    ~FeatureSpaceContext()
    {
        pop(entered);
    }
    
    const ML::Feature_Space * entered;

    static void push(const ML::Feature_Space * fs);
    static void pop(const ML::Feature_Space * fs);
    static const ML::Feature_Space * current();
};

struct Dense_Feature_Space;
struct Feature_Info;
struct Configuration;

PREDECLARE_VALUE_DESCRIPTION(ML::Dense_Feature_Space);

PREDECLARE_VALUE_DESCRIPTION(ML::Feature_Info);

DECLARE_ENUM_DESCRIPTION_NAMED(FeatureTypeDescription, ML::Feature_Type);
DECLARE_ENUM_DESCRIPTION_NAMED(LinkFunctionDescription, ML::Link_Function);
DECLARE_ENUM_DESCRIPTION_NAMED(RegularizationDescription, ML::Regularization);

PREDECLARE_VALUE_DESCRIPTION(ML::Configuration);
PREDECLARE_VALUE_DESCRIPTION(std::shared_ptr<ML::Classifier_Impl>);

} // namespace ML
