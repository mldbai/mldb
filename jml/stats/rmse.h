/* rmse.h                                                          -*- C++ -*-
   Jeremy Barnes, 9 November 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Root Mean Squared Error calculation routines.
*/

#pragma once

#include "mldb/jml/stats/distribution.h"
#include "mldb/jml/stats/distribution_ops.h"

namespace ML {

using namespace MLDB;

template<typename Float1, typename Float2>
double
calc_rmse(const distribution<Float1> & outputs,
          const distribution<Float2> & targets)
{
    return sqrt(sqr((targets - outputs)).total()
                * (1.0 / outputs.size()));
}

template<typename Float1, typename Float2, typename Float3>
double
calc_rmse(const distribution<Float1> & outputs,
          const distribution<Float2> & targets,
          const distribution<Float3> & weights)
{
    return sqrt((sqr((targets - outputs)) * weights).total()
                / weights.total());
}


} // namespace ML
