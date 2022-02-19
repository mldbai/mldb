/* ostream_vector.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <array>
#include <iostream>

namespace std {
template<typename T, size_t N>
std::ostream &
operator << (std::ostream & stream, const std::array<T, N> & a)
{
    stream << "[";
    for (unsigned i = 0;  i < a.size();  ++i) {
        stream << " " << a[i];
    }
    return stream << " ]";
}
} // namespace std
