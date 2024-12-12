/** split.h                                                       -*- C++ -*-
    Jeremy Barnes, 22 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Function to securely create a unique directory.
*/

#pragma once

#include <utility>

namespace MLDB {

// match on a single character
template<typename Char>
inline bool split_match(const Char c, const char match)
{
    return match == c;
}

// Match on a lambda taking a character and returning a bool
template<typename Char, typename Match, typename = decltype(std::declval<Match>()(std::declval<Char>()))>
inline bool split_match(const Char c, Match&& match)
{
    return match(c);
}

template<typename ResultString, typename String, typename Match>
void split(std::vector<ResultString> & result, String&& str, Match&& match)
{
    for (auto it = str.begin(), end = str.end(); it != end; ++it) {
        auto start = it;
        while (it != end && !split_match(*it, match)) ++it;
        result.emplace_back(start, it);
        if (it != end) ++it;
    }
}

} // namespace MLDB
