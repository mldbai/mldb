/** value_descriptions.h                                           -*- C++ -*-
    Jeremy Barnes, 5 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/plugins/jml/jml/feature_info.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/utils/distribution.h"
#include "mldb/plugins/jml/algebra/irls.h"

namespace MLDB {

class Classifier_Impl;
class Feature_Space;

} // namespace MLDB

namespace MLDB {

/** Scope object allowing a feature space to be entered so that functions that
    print or parse features can use the feature space.  This pushes the given
    context onto a thread specific stack.
*/
struct FeatureSpaceContext {
    FeatureSpaceContext(const MLDB::Feature_Space * fs)
        : entered(fs)
    {
        push(fs);
    }

    ~FeatureSpaceContext()
    {
        pop(entered);
    }
    
    const MLDB::Feature_Space * entered;

    static void push(const MLDB::Feature_Space * fs);
    static void pop(const MLDB::Feature_Space * fs);
    static const MLDB::Feature_Space * current();
};

class Dense_Feature_Space;
struct Feature_Info;
class Configuration;

PREDECLARE_VALUE_DESCRIPTION(MLDB::Dense_Feature_Space);

PREDECLARE_VALUE_DESCRIPTION(MLDB::Feature_Info);

DECLARE_ENUM_DESCRIPTION_NAMED(FeatureTypeDescription, MLDB::Feature_Type);
DECLARE_ENUM_DESCRIPTION_NAMED(RegularizationDescription, MLDB::Regularization);

PREDECLARE_VALUE_DESCRIPTION(MLDB::Configuration);
PREDECLARE_VALUE_DESCRIPTION(std::shared_ptr<MLDB::Classifier_Impl>);

} // namespace MLDB
