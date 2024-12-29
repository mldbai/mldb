/* is_visitable.h                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <type_traits>

namespace MLDB {

// Default (fallback) causes a compile assert if we pass something non-visitable
template<typename Visitor, typename Method>
struct IsVisitable {
    static_assert(
        std::integral_constant<Method, false>::value,
        "Second template parameter (Method) needs to be of function type.");
};

// It's visitable if the method is callable with the arguments and returns the correct type
template<typename Visitor, typename Ret, typename... Args>
struct IsVisitable<Visitor, Ret(Args...)> {
private:
    template<typename T>
    static constexpr auto check(T*)
    -> typename
        std::is_convertible<
            decltype( std::declval<T>().visit( std::declval<Args>()... ) ),
            Ret
        >::type;  // attempt to call it and see if the return type is correct

    template<typename>
    static constexpr std::false_type check(...);

    typedef decltype(check<Visitor>(nullptr)) type;

public:
    static constexpr bool value = type::value;
};

template<typename Visitor, typename Method>
constexpr bool isVisitable()
{
    return IsVisitable<Visitor, Method>::value;
}

} // namespace MLDB
