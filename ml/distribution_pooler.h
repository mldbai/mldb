// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* distribution_pooler.h                                                -*- C++ -*-
   Francois Maillet, Oct 23 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

*/

#ifndef __recoset__ml__distribution_pooler_h__
#define __recoset__ml__distribution_pooler_h__

#include <functional>
#include <vector>
#include <stdint.h>
#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/jml/stats/distribution.h"

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

}

#endif
