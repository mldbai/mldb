/* float_traits.h                                                  -*- C++ -*-
   Jeremy Barnes, 30 January 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


   ---
   
   Floating point traits.
*/

#pragma once

#include <utility>

namespace MLDB {

template<typename F1, typename F2>
struct float_traits {
    using return_type = decltype(std::declval<F1>() + std::declval<F2>());
    using fraction_type = decltype(std::declval<F1>() / std::declval<F2>());
};

template <typename F>
struct float_traits<F, F> {
    using return_type = F;
    using fraction_type = decltype(std::declval<F>() / std::declval<F>());
};

template<typename F1, typename F2, typename F3>
struct float_traits3 {
    using return_type = decltype(std::declval<F1>() + std::declval<F2>() + std::declval<F3>());
};

template <typename F>
struct float_traits3<F, F, F> {
    using return_type = F;
};

} // namespace MLDB
