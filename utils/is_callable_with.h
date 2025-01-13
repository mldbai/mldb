/** is_callable_with.h                                                -*- C++ -*-
    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Class to process each line in a file in parallel.
*/

#pragma once

#include <utility>

namespace MLDB {

template<typename Fn, typename... Args>
struct is_callable_with {
    template<typename F, typename... A> static auto test(int) -> decltype(std::declval<F>()(std::declval<A>()...), std::true_type());
    template<typename F, typename... A> static auto test(...) -> std::false_type;
    using type = decltype(test<Fn, Args...>(0));
    static constexpr bool value = type::value;
};

template<typename Fn, typename... Args> constexpr bool is_callable_with_v = is_callable_with<Fn, Args...>::value;
template<typename Fn, typename... Args> using is_callable_with_t = is_callable_with<Fn, Args...>::type;

} // namespace MLDB