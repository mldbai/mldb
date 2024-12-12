/** starts_with.h                                                       -*- C++ -*-
    Jeremy Barnes, 22 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Function to securely create a unique directory.
*/

#pragma once

#include <utility>

namespace MLDB {

template<typename String1, typename String2>
bool starts_with(const String1& s, const String2& prefix)
{
    using std::begin;
    using std::end;
    using std::size;

    return size(s) >= size(prefix) &&
        std::equal(begin(prefix), end(prefix), begin(s));
}

template<typename String1, typename String2>
bool ends_with(const String1& s, const String2& suffix)
{
    using std::rbegin;
    using std::rend;
    using std::size;

    return size(s) >= size(suffix) &&
        std::equal(rbegin(suffix), rend(suffix), rbegin(s));
}

} // namespace MLDB
