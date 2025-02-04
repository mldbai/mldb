/* ostream_span.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <span>
#include <iostream>

template<typename T, size_t Size>
std::ostream &
operator << (std::ostream & stream, const std::span<T, Size> & s)
{
    stream << "[";
    for (const auto & e: s)
        stream << " " << e;
    return stream << " ]";
}
