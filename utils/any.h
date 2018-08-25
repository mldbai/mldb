/** any.h                                                          -*- C++ -*-
    Jeremy Barnes, 25 August 2018
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#pragma once

#if __has_include(<any>)
#  include <any>
#elif __has_include(<boost/any.hpp>)
#  include <boost/any.hpp>
namespace std {
    using boost::any;
    using boost::any_cast;
    using boost::bad_any_cast;
} // namespace std
#else
#  error "No any implementation of any (C++17 <any>, boost any) found"
#endif
