/* bitops.h                                                        -*- C++ -*-
   Jeremy Barnes, 23 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


   Bitwise operations.
*/

#pragma once

#include "mldb/compiler/compiler.h"
#include <stdint.h>
#include <cassert>
#include <iostream>
#include <type_traits>

namespace ML {

template<class T>
int highest_bit(T arg, int none_set = -1)
{
    return arg
        ? 64 - __builtin_clzll(typename std::make_unsigned<T>::type(arg)) - 1
        : none_set;
}

template<class T>
constexpr int lowest_bit(T arg, int none_set = -1)
{
    return arg
        ? __builtin_ctzll(arg)
        : none_set;
}

template<class T>
constexpr int num_bits_set(T arg)
{
    return __builtin_popcount(arg);
}

constexpr MLDB_ALWAYS_INLINE int num_bits_set(unsigned long arg)
{
    return __builtin_popcountl(arg);
}

constexpr MLDB_ALWAYS_INLINE int num_bits_set(signed long arg)
{
    return __builtin_popcountl(arg);
}

constexpr MLDB_ALWAYS_INLINE int num_bits_set(unsigned long long arg)
{
    return __builtin_popcountll(arg);
}

constexpr MLDB_ALWAYS_INLINE int num_bits_set(signed long long arg)
{
    return __builtin_popcountll(arg);
}

} // namespace ML
