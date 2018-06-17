/** string_view.h                                                  -*- C++ -*-
    Jeremy Barnes, 17 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Simple include to find the compiler's string_view implementation
    and pull it into namespace std.
*/

#pragma once

#if __has_include(<string_view>) && __cplusplus >= 201703L
#include <string_view>
#else
#  include <experimental/string_view>

namespace std {

using std::experimental::string_view;

} // namespace std

#endif // string_view polyfill
