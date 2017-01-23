// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* bound.h                                                         -*- C++ -*-
   Jeremy Barnes, 15 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
      


   ---

   Bound function.  Returns a value bounded by a minimum and maximum.
*/

#ifndef __math__bound_h__
#define __math__bound_h__

#include "mldb/jml/utils/float_traits.h"

namespace ML {

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

} // namespace ML

#endif /* __math__bound_h__ */
