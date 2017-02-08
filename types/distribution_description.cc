// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** distribution_description.cc
    Jeremy Barnes, 24 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Value description for distributions.
*/

#include "distribution_description.h"

namespace MLDB {

template class DistributionValueDescription<float, std::vector<float> >;
template class DistributionValueDescription<double, std::vector<double> >;

} // namespace MLDB
