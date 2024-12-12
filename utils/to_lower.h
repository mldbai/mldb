/** to_lower.h                                                       -*- C++ -*-
    Jeremy Barnes, 22 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Function to securely create a unique directory.
*/

#pragma once

#include <algorithm>
#include <cstring>
#include <type_traits>

namespace MLDB {

template<typename String>
inline void to_lower(String& s)
{
    static_assert(std::is_same_v<typename String::value_type, char>,
                  "to_lower only works on strings of char");
    std::transform(s.begin(), s.end(), s.begin(), ::tolower);
}

} // namespace MLDB