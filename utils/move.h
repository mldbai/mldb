// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

// Copied from boost; under the Boost license and copyright
// Compatible with non-commercial and commercial 

#pragma once

#include <iterator>
#include <algorithm>

namespace MLDB {

template<typename I, typename F>
F uninitialized_move(I first, I last, F result)
    noexcept(std::is_nothrow_move_constructible<typename std::iterator_traits<F>::value_type>::value)

{
   for (; first != last; ++result, ++first)
       new (static_cast<void*>(&*result))
           typename std::iterator_traits<F>::value_type
               (std::move(*first));

   return first;
}

template<typename T>
    void destroy(T & t) noexcept (std::is_nothrow_destructible<T>::value)
{
    t.~T();
}

template<typename I, typename F>
    F uninitialized_move_and_destroy(I first, I last, F result)
    noexcept(std::is_nothrow_move_constructible<typename std::iterator_traits<F>::value_type>::value)
{
    for (; first != last; ++result, ++first) {
        auto && v = std::move(*first);
        new (static_cast<void*>(&*result))
            typename std::iterator_traits<F>::value_type(std::move(v));
        destroy(v);
    }

   return first;
}

} // namespace MLDB
