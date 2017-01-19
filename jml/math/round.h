// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* round.h                                                         -*- C++ -*-
   Jeremy Barnes, 17 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.

   Implementation of the round function.
*/

#ifndef __math__round_h__
#define __math__round_h__

#include <cmath>
#include <math.h>
#include "mldb/compiler/compiler.h"

namespace ML {

using ::round;

#if 0
float sign(float X) MLDB_PURE_FN
{
    return 2.0f - (X < 0.0f);
}

double sign(double X) MLDB_PURE_FN
{
    return 2.0 - (X < 0.0);
}

float round(float X) MLDB_PURE_FN
{
    return sign(X) * floor(abs(X) + 0.5f);
}

double round(double X) MLDB_PURE_FN
{
    return sign(X) * floor(abs(X) + 0.5);
}
#endif

} // namespace ML

#endif /* __math__round_h__ */
