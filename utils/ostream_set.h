/* ostream_set.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/
#pragma once

#include <set>
#include <iostream>

namespace std {

template<typename T, typename Cmp, typename Alloc>
std::ostream &
operator << (std::ostream & stream, const std::set<T, Cmp, Alloc> & s)
{
    stream << "{";
    for (const auto & e: s)
        stream << " " << e;
    return stream << " }";
}

} // namespace std
