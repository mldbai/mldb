/** trim.h                                                       -*- C++ -*-
    Jeremy Barnes, 22 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Function to securely create a unique directory.
*/

#pragma once

#include <algorithm>

namespace MLDB {

// https://stackoverflow.com/questions/216823/how-to-trim-a-stdstring
// trim from start (in place)
template<typename String>
static inline void ltrim(String &s) {
    using std::isspace;
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](auto ch) {
        return !isspace(ch);
    }));
}

// trim from end (in place)
template<typename String>
static inline void rtrim(String &s) {
    using std::isspace;
    auto it = s.end();
    while (it != s.begin()) {
        --it;
        if (!isspace(*it)) {
            ++it;
            s.erase(it, s.end());
            break;
        }
    }
}

// trim from both ends (in place)
template<typename String>
static inline void trim(String &s) {
    ltrim(s);
    rtrim(s);
}

template<typename String>
static String trimmed(String s) {
    trim(s);
    return s;
}

} // namespace MLDB
