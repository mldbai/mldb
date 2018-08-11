/* distribution_ops.h                                              -*- C++ -*-
   Jeremy Barnes, 2 Febryary 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


   Operations on distributions.
*/

#pragma once

#include "mldb/arch/arch.h"
#include <math.h>

namespace MLDB {

// 4 June 2009, Jeremy, Ubuntu 9.04
// For some reason, the x86_64 implementation of expf sets and resets the FPU
// control word, which makes it extremely slow.  The expm1f implementation
// doesn't have this problem, and so we use it to emulate an exp.

inline float exp(float val)
{
    return expm1f(val) + 1.0f;
}

inline double exp(double val)
{
    return std::exp(val);
}

} // namespace MLDB
