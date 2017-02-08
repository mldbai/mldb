// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tuple_utils.h                                                   -*- C++ -*-
   Jeremy Barnes, 21 April 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Utilities for use with tuples.
*/

#pragma once

#include <tuple>

namespace MLDB {


// implementation details, users never invoke these directly
namespace detail
{
    template <typename F, typename Tuple, bool Done, int Total, int... N>
    struct call_impl
    {
        template<typename... Extra>
        static void call(F f, Tuple && t, Extra&&... extra)
        {
            call_impl<F, Tuple, Total == 1 + sizeof...(N), Total, N..., sizeof...(N)>::call(f, std::forward<Tuple>(t), std::forward<Extra>(extra)...);
        }
    };

    template <typename F, typename Tuple, int Total, int... N>
    struct call_impl<F, Tuple, true, Total, N...>
    {
        template<typename... Extra>
        static void call(F f, Tuple && t, Extra&&... extra)
        {
            f(std::forward<Extra>(extra)...,
              std::get<N>(std::forward<Tuple>(t))...);
        }
    };
}

// user invokes this
template <typename F, typename Tuple, typename... Extra>
void callFromTuple(F f, Tuple && t, Extra&&... extra)
{
    typedef typename std::decay<Tuple>::type ttype;
    detail::call_impl<F, Tuple, 0 == std::tuple_size<ttype>::value, std::tuple_size<ttype>::value>::call(f, std::forward<Tuple>(t), std::forward<Extra>(extra)...);
}


/** These functions can be used to present an interface whereby a tuple of
    a single type just returns an unpacked version of that type, which is
    what most people expect.
*/

template<typename... Args>
std::tuple<Args...> tuplize(std::tuple<Args...> && res)
{
    return std::move(res);
}

template<typename T>
std::tuple<T> tuplize(const T & val)
{
    return std::make_tuple(val);
}

template<typename T>
T detuplize(T val)
{
    return val;
}

template<typename T>
T detuplize(std::tuple<T> && val)
{
    return std::move(std::get<0>(std::move(val)));
}

template<typename T>
const T & detuplize(const std::tuple<T> & val)
{
    return std::get<0>(std::move(val));
}

template<typename... Args>
std::tuple<Args...> detuplize(const std::tuple<Args...> & res)
{
    return res;
}

template<typename... Args>
std::tuple<Args...> detuplize(std::tuple<Args...> && res)
{
    return res;
}

} // namespace MLDB
