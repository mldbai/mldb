/* ostream_pair.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <utility>
#include <iostream>

namespace std {
template<typename X, typename Y>
std::ostream &
operator << (std::ostream & stream, const std::pair<X, Y> & a)
{
    return stream << "( " << a.first << ", " << a.second << ")";
}
} // namespace std

