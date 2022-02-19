/* ostream_vector.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <vector>
#include <iostream>

namespace std {
template<typename T, typename Alloc>
std::ostream &
operator << (std::ostream & stream, const std::vector<T, Alloc> & v)
{
    stream << "[";
    for (const auto & e: v)
        stream << " " << e;
    return stream << " ]";
}
} // namespace std