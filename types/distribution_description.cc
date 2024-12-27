// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** distribution_description.cc
    Jeremy Barnes, 24 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Value description for distributions.
*/

#include "distribution_description.h"

namespace MLDB {

template struct DistributionValueDescription<float, std::vector<float> >;
template struct DistributionValueDescription<double, std::vector<double> >;

} // namespace MLDB
