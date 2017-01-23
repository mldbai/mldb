// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* distribution_ops.h                                              -*- C++ -*-
   Jeremy Barnes, 2 Febryary 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   


   Operations on distributions.
*/

#ifndef __arch__math_builtins_h__
#define __arch__math_builtins_h__

#include "mldb/arch/arch.h"
#include <math.h>

namespace ML {

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

} // namespace ML

#endif /* __arch__math_builtins_h__ */
