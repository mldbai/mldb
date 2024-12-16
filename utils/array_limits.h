/** array_limits.h                                                       -*- C++ -*-
    Jeremy Barnes, 22 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Fix stdlib functions that include the null termination in char arrays.
*/

#pragma once

#include <string>
#include <iterator>

namespace MLDB {

namespace details {

using std::size;
template<typename Array>
constexpr auto arr_size(const Array & array) -> decltype(size(array))
{
    return size(array);
}

using std::begin;
template<typename Array>
constexpr auto arr_begin(const Array & array) -> decltype(begin(array))
{
    return begin(array);
}

using std::end;
template<typename Array>
constexpr auto arr_end(const Array & array) -> decltype(end(array))
{
    return end(array);
}

using std::rbegin;
template<typename Array>
constexpr auto arr_rbegin(const Array & array) -> decltype(rbegin(array))
{
    return rbegin(array);
}

using std::rend;
template<typename Array>
constexpr auto arr_rend(const Array & array) -> decltype(rend(array))
{
    return rend(array);
}


// Specializations for fixed length arrays

// Fix the std::size() including the null termination in a char array problem
// Make sure we only do it for things that have std::char_traits<Char> defined
template<typename Char, size_t N, typename Enable = std::char_traits<Char>>
constexpr size_t arr_size(Char (&array)[N])
{
    return N - (array[N-1] == 0 ? 1 : 0);
}

// Make sure the signature corresponds to the arr_end() specialization
template<typename Char, size_t N, typename Enable = std::char_traits<Char>>
constexpr Char * arr_begin(Char (&array)[N])
{
    return array;
}

// Fix the std::end() including the null termination in a char array problem
// Make sure we only do it for things that have std::char_traits<Char> defined
template<typename Char, size_t N, typename Enable = std::char_traits<Char>>
constexpr Char * arr_end(Char (&array)[N])
{
    return array + N - (array[N-1] == 0 ? 1 : 0);
}

// Fix the std::rbegin() including the null termination in a char array problem
// Make sure we only do it for things that have std::char_traits<Char> defined
template<typename Char, size_t N, typename Enable = std::char_traits<Char>>
constexpr auto arr_rbegin(Char (&array)[N]) -> decltype(std::make_reverse_iterator(declval<Char *>()))
{
    return std::make_reverse_iterator(arr_end(array));
}

// Make sure the signature corresponds to the arr_rend() specialization
template<typename Char, size_t N, typename Enable = std::char_traits<Char>>
constexpr auto arr_rend(Char (&array)[N]) -> decltype(std::make_reverse_iterator(declval<Char *>()))
{
    return std::make_reverse_iterator(arr_begin(array));
}

#if 0

// Specializations for const char *

// Fix the std::size() including the null termination in a char array problem
// Make sure we only do it for things that have std::char_traits<Char> defined
template<typename Char, typename Enable = std::char_traits<Char>>
constexpr size_t arr_size(const Char * array)
{
    return std::strlen(array);
}

// Make sure the signature corresponds to the arr_end() specialization
template<typename Char, typename Enable = std::char_traits<Char>>
constexpr const Char * arr_begin(Char * array)
{
    return array;
}

// Fix the std::end() including the null termination in a char array problem
// Make sure we only do it for things that have std::char_traits<Char> defined
template<typename Char, typename Enable = std::char_traits<Char>>
constexpr const Char * arr_end(Char * array)
{
    return array + strlen(array);
}

// Fix the std::rbegin() including the null termination in a char array problem
// Make sure we only do it for things that have std::char_traits<Char> defined
template<typename Char, typename Enable = std::char_traits<Char>>
constexpr auto arr_rbegin(Char * array) -> decltype(std::make_reverse_iterator(declval<Char *>()))
{
    return std::make_reverse_iterator(arr_end(array));
}

// Make sure the signature corresponds to the arr_rend() specialization
template<typename Char, typename Enable = std::char_traits<Char>>
constexpr auto arr_rend(Char * array) -> decltype(std::make_reverse_iterator(declval<Char *>()))
{
    return std::make_reverse_iterator(arr_begin(array));
}

#endif

} // namespace details

using details::arr_begin;
using details::arr_end;
using details::arr_size;
using details::arr_rbegin;
using details::arr_rend;

} // namespace MLDB