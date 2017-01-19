// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* set_utils.h                                                  -*- C++ -*-
   Jeremy Barnes, 1 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
     


   ---

   Helpful functions for dealing with sets.
*/

#ifndef __utils__set_utils_h__
#define __utils__set_utils_h__

#include <set>
#include <algorithm>
#include <iostream>

namespace std {

template<class T, class A>
std::ostream &
operator << (std::ostream & stream, const set<T, A> & s)
{
    stream << "[";
    for (typename set<T, A>::const_iterator it = s.begin(), end = s.end();
         it != end;  ++it)
        stream << " " << *it;
    return stream << " ]";
}

} // namespace std

#endif /* __utils__set_utils_h__ */
