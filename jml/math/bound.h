/* bound.h                                                         -*- C++ -*-
   Jeremy Barnes, 15 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


   ---

   Bound function.  Returns a value bounded by a minimum and maximum.
*/

#pragma once

#include "mldb/utils/float_traits.h"

namespace MLDB {

template<class X, class Y, class Z>
typename float_traits3<X, Y, Z>::return_type
bound(X x, Y min, Z max)
{
    typename float_traits3<X, Y, Z>::return_type result = x;
    if (result < min) result = min;
    if (result > max) result = max;
    return result;
}

template<class X>
X bound(X x, X min, X max)
{
    if (x < min) x = min;
    if (x > max) x = max;
    return x;
}

} // namespace MLDB
