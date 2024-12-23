/** starts_with.h                                                       -*- C++ -*-
    Jeremy Barnes, 22 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Function to securely create a unique directory.
*/

#pragma once

#include <utility>
#include "mldb/utils/array_limits.h"

namespace MLDB {

template<typename String1, typename String2>
bool starts_with(const String1& s, const String2& prefix)
{
    return arr_size(s) >= arr_size(prefix) &&
        std::equal(arr_begin(prefix), arr_end(prefix), arr_begin(s));
}

template<typename String1, typename String2>
bool remove_if_starts_with(String1& s, const String2& prefix)
{
    if (!starts_with(s, prefix))
        return false;
    auto it = arr_begin(s);
    std::advance(it, arr_size(prefix));
    s.erase(arr_begin(s), it);
    return true;
}

template<typename String1, typename String2>
String1 must_remove_prefix(const String1& s, String2&& prefix)
{
    if (!starts_with(s, std::forward<String2>(prefix)))
        throw std::invalid_argument("String does not start with the given prefix");
    auto it = arr_begin(s);
    std::advance(it, arr_size(std::forward<String2>(prefix)));
    return String1(it, arr_end(s));
}

template<typename String1, typename String2>
bool ends_with(const String1& s, const String2& suffix)
{
    return arr_size(s) >= arr_size(suffix) &&
        std::equal(arr_rbegin(suffix), arr_rend(suffix), arr_rbegin(s));
}

template<typename String1, typename String2>
bool remove_if_ends_with(String1& s, String2&& suffix)
{
    if (!ends_with(s, std::forward<String2>(suffix)))
        return false;
    auto it = arr_end(s);
    std::advance(it, -arr_size(std::forward<String2>(suffix)));
    s.erase(it, arr_end(s));
    return true;
}

template<typename String1, typename String2>
String1 must_remove_suffix(const String1& s, const String2& suffix)
{
    if (!ends_with(s, suffix))
        throw std::invalid_argument("String does not start with the given suffix");
    auto it = arr_end(s);
    std::advance(it, -arr_size(suffix));
    return String1(arr_begin(s), it);
}

} // namespace MLDB
