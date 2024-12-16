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
bool ends_with(const String1& s, const String2& suffix)
{
    return arr_size(s) >= arr_size(suffix) &&
        std::equal(arr_rbegin(suffix), arr_rend(suffix), arr_rbegin(s));
}

} // namespace MLDB
