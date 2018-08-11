/* distribution_pooler.h                                                -*- C++ -*-
   Francois Maillet, Oct 23 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#pragma once

#include <functional>
#include <vector>
#include <stdint.h>
#include "mldb/utils/smart_ptr_utils.h"
#include "mldb/utils/distribution.h"

namespace MLDB {

/*****************************************************************************/
/* DISTRIBUTION POOLER */
/*****************************************************************************/

struct DistributionPooler {
public:
    DistributionPooler() { init = false;};
    distribution<float> pool();
    void add(std::shared_ptr<distribution<float>> d);

private:
    bool init;
    std::vector<distribution<float>> feature_vectors;
};

} // namespace MLDB
