/** join.h                                                       -*- C++ -*-
    Jeremy Barnes, 22 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    String join utility.
*/

#pragma once

#include <string>

namespace MLDB {

template<typename String, typename Container>
typename Container::value_type join(const Container & container, const String & separator)
{
    typename Container::value_type result;
    for (auto it = container.begin();  it != container.end();  ++it) {
        if (it != container.begin())
            result += separator;
        result += *it;
    }
    return result;
}

} // namespace MLDB
