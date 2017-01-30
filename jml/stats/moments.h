// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* moments.h                                                       -*- C++ -*-
   Jeremy Barnes, 2 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   


   ---

   Functions to do with calculating moments (mean, std dev, etc).
*/

#ifndef __stats__moments_h__
#define __stats__moments_h__

#include <limits>
#include <cmath>

namespace ML {

template<class Iterator>
double mean(Iterator first, Iterator last)
{
    double result = 0.0;
    int divisor = 0;

    while (first != last) {
        result += *first++;
        divisor += 1;
    }

    return result / divisor;
}

inline double sqr(double val)
{
    return val * val;
}

/** Unbiased estimate of standard deviation. */

template<class Iterator>
double std_dev(Iterator first, Iterator last, double mean)
{
    double total = 0.0;
    size_t count = std::distance(first, last);

    while (first != last)
        total += sqr(*first++ - mean);
    
    if (count == 0 || count == 1)
        return std::numeric_limits<double>::quiet_NaN();

    return std::sqrt(total / (double)(count - 1));
}

} // namespace ML


#endif /* __stats__moments_h__ */


