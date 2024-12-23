/** split.h                                                       -*- C++ -*-
    Jeremy Barnes, 22 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Function to securely create a unique directory.
*/

#pragma once

#include <utility>
#include <vector>
#include <algorithm>
//#include <iostream> // for debugging
#include "mldb/utils/array_limits.h"

namespace MLDB {

// match on a single character
template<typename Char>
inline bool split_match(const Char c, const char match)
{
    return match == c;
}

// Match on a lambda taking a character and returning a bool
template<typename Char, typename Match, typename = decltype(std::declval<Match>()(std::declval<Char>()))>
inline bool split_match(const Char c, Match&& match)
{
    return match(c);
}

template<typename It, typename Enable = decltype(std::declval<It>() < std::declval<It>())>
void check_in_range(It it, It end)
{
    if (it < end || it == end)
        return;
    throw std::logic_error("it < end");
}

template<typename It>
void check_in_range(It it, It end)
{
}

template<typename ResultString, typename String, typename Match>
void split(std::vector<ResultString> & result, String&& str, Match&& match, bool include_empty_at_end = true)
{
    //constexpr bool debug = false;

    //using namespace std;

    //if constexpr (debug) { 
    //    cerr << "split(\"" << str << "\",\"" << match << "\")" << endl;
    //    cerr << "distance is " << std::distance(arr_begin(str), arr_end(str)) << endl;
    //    cerr << "type of str is " << typeid(str).name() << endl;
    //    cerr << "type of iterator is " << typeid(arr_begin(str)).name() << endl;
    //}

    auto ibegin = arr_begin(std::forward<String>(str)), iend = arr_end(std::forward<String>(str)), it = ibegin;
    for (; it != iend; /* no inc */) {
        //check_in_range(it, iend);
        //if constexpr (debug) { cerr << "at character \'" << *it << "\' at distance " << std::distance(arr_begin(str), it) << endl; }
        auto istart = it;
        while (it != iend && !split_match(*it, match)) ++it;
        //if constexpr (debug) {
        //    if (it == iend)
        //        cerr << "skipped to end" << endl;
        //    else
        //        cerr << "  skipped to character \'" << *it << "\' at distance " << std::distance(arr_begin(str), it) << endl;
        //    cerr << "  distance from istart to it is " << std::distance(istart, it) << endl;
        //    for (auto istart2 = istart; istart2 != it; ++istart2)
        //        cerr << "    char '" << *istart2 << "' (" << ((int)*istart2) << ")" << endl;
        //}
        result.emplace_back(istart, it);
        //if constexpr (debug) { cerr << "  added result \"" << result.back() << "\"" << endl; }
        if (it == iend) break;
        ++it;
        if (it == iend && include_empty_at_end)
            result.emplace_back();
    }
    if (ibegin == iend && include_empty_at_end)
        result.emplace_back();
}

// Returns before match, after match, was match found
template<typename String, typename Match>
std::tuple<String, String, bool>
split_on_first(const String & str, Match&& match)
{
    auto ibegin = arr_begin(str), iend = arr_end(str);
    auto mbegin = arr_begin(std::forward<Match>(match)), mend = arr_end(std::forward<Match>(match));

    auto found = std::search(ibegin, iend, mbegin, mend);
    if (found == iend)
        return { str, String(), false };
    String before(ibegin, found);
    auto mlen = arr_size(std::forward<Match>(match));
    std::advance(found, mlen);
    String after(found, iend);
    return { std::move(before), std::move(after), true };
}

// Returns before match, after match, was match found
template<typename String, typename Match>
std::tuple<String, String, bool>
split_on_last(const String & str, Match&& match)
{
    auto ibegin = arr_begin(str), iend = arr_end(str);
    auto mbegin = arr_begin(std::forward<Match>(match)), mend = arr_end(std::forward<Match>(match));

    auto found = std::find_end(ibegin, iend, mbegin, mend);
    if (found == iend)
        return { str, String(), false };
    String before(ibegin, found);
    auto mlen = arr_size(std::forward<Match>(match));
    std::advance(found, mlen);
    String after(found, iend);
    return { std::move(before), std::move(after), true };
}

} // namespace MLDB
