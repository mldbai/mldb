// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* less.h                                                          -*- C++ -*-
   Jeremy Barnes, 5 March 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.

   Functions to implement an operator <.
*/

#ifndef __utils__less_h__
#define __utils__less_h__

#include <algorithm>

namespace ML {

template<typename T1>
bool less_all(const T1 & x1, const T1 & y1)
{
    return x1 < y1;
}

template<typename T1, typename T2>
bool less_all(const T1 & x1, const T1 & y1,
              const T2 & x2, const T2 & y2)
{
    return x1 < y1
        || (x1 == y1
            && (x2 < y2));
}

template<typename T1, typename T2, typename T3>
bool less_all(const T1 & x1, const T1 & y1,
              const T2 & x2, const T2 & y2,
              const T3 & x3, const T3 & y3)
{
    return x1 < y1
        || (x1 == y1
            && (x2 < y2
                || (x2 == y2
                    && (x3 < y3))));
}

template<typename T1, typename T2, typename T3, typename T4>
bool less_all(const T1 & x1, const T1 & y1,
              const T2 & x2, const T2 & y2,
              const T3 & x3, const T3 & y3,
              const T4 & x4, const T4 & y4)
{
    return x1 < y1
        || (x1 == y1
            && (x2 < y2
                || (x2 == y2
                    && (x3 < y3
                        || (x3 == y3
                            && (x4 < y4))))));
}

template<typename T1, typename T2, typename T3, typename T4, typename T5>
bool less_all(const T1 & x1, const T1 & y1,
              const T2 & x2, const T2 & y2,
              const T3 & x3, const T3 & y3,
              const T4 & x4, const T4 & y4,
              const T5 & x5, const T5 & y5)
{
    return x1 < y1
        || (x1 == y1
            && (x2 < y2
                || (x2 == y2
                    && (x3 < y3
                        || (x3 == y3
                            && (x4 < y4
                                || (x4 == y4
                                    && (x5 < y5))))))));
}

template<typename T>
int compare_3way(const T & o1, const T & o2)
{
    if (o1 < o2) return -1;
    if (o2 < o1) return 1;
    return 0;
}

template<typename T, class Less>
int compare_3way(const T & o1, const T & o2)
{
    if (Less()(o1, o2)) return -1;
    if (Less()(o2, o1)) return 1;
    return 0;
}

template<typename T> struct compare {
    int operator() (const T & x, const T & y) const 
    { return x < y ? -1 : x == y ? 0 : 1; }
    typedef T first_argument_type;
    typedef T second_argument_type;
    typedef int result_type;
};

template <typename T, typename Compare> 
typename Compare::result_type compare_sorted( T & x, T & y, Compare comp) {
    std::sort(x.begin(), x.end());
    std::sort(y.begin(), y.end());
    return comp(x,y);
}

} // namespace ML

#endif /* __utils__less_h__ */


