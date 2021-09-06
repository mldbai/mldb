/* fixed_point_accum.h                                             -*- C++ -*-
   Jeremy Barnes, 1 April 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Fixed-point accurate accumulators.
*/

#include "fixed_point_accum.h"
#include "mldb/arch/exception.h"

namespace MLDB {

void throwFixedPointOverflow()
{
    throw MLDB::Exception("fixed point overflow");
}

} // namespace MLDB