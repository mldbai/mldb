/*  xdiv.h                                                          -*- C++ -*-
    Jeremy Barnes, 30 January 2005
    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

   ---

   Our old friend the xdiv function.
*/

#pragma once

#include "mldb/jml/utils/float_traits.h"
#include "mldb/compiler/compiler.h"

namespace ML {

template<typename F1, typename F2>
typename float_traits<F1, F2>::fraction_type
xdiv(F1 x, F2 y)
{
    return (y == 0 ? 0 : x / y);
}

/* Divide, but round up */
template<class X, class Y>
JML_COMPUTE_METHOD
X rudiv(X val, Y by)
{
    X result = (val / by);
    X missing = val - (result * by);
    result += (missing > 0);
    return result;
}


} // namespace ML

