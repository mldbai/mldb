/** trim.h                                                       -*- C++ -*-
    Jeremy Barnes, 22 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Function to securely create a unique directory.
*/

#pragma once

#include <algorithm>
#include <string>
#include <vector>
#include <type_traits>

namespace MLDB {

#if 0
inline size_t get_length(const char * s) { return std::strlen(s); }
template<typename Container>
size_t get_length(const Container & container, decltype(std::declval<const Container>().length()) * = 0)
{
    return container.length();
}

template<typename Char>
size_t get_length(const Char * s)
{
    using std::strlen;
    return strlen(s);
}
#endif

template<typename Haystack, typename Needle, typename Replacement>
void replace_all(Haystack& haystack, Needle&& needle, Replacement&& replacement)
{
    using HaystackIt = decltype(haystack.begin());
    using std::begin;
    using std::end;
    using std::size;

    std::vector<std::pair<HaystackIt, HaystackIt>> matches;

    const auto nbeg = begin(needle);
    const auto nend = end(needle);

    // Find all places where it matches. We find them all at the beginning so that the replacement
    // doesn't cause any problems.
    for (auto it = haystack.begin(), end = haystack.end(); it != end; /* no inc */) {
        auto it2 = it;
        auto nit = nbeg;
        for (auto it2 = it; it2 != end && nit != nend && *it == *nit; ++it2, ++nit);
        if (nit == nend) {
            matches.emplace_back(it, it2);
            it = it2;
        }
        else {
            ++it;
        }
    }

    if (matches.empty())
        return;

    // Total length of the replacement
    size_t replacement_length = haystack.size() + (matches.size() * (size(replacement) - size(needle)));

    auto rbeg = begin(replacement);
    auto rend = end(replacement);

    // TODO: lots of possible optimizations
    typename std::remove_reference_t<Haystack> result;
    result.reserve(replacement_length);
    auto it = haystack.begin();
    for (auto [mbeg, mend]: matches) {
        result.append(it, mbeg);
        result.append(rbeg, rend);
        it = mend;
    }
    result.append(it, haystack.end());

    std::swap(haystack, result);
}

template<typename Haystack, typename Needle, typename Replacement>
Haystack replace_all_copy(Haystack haystack, Needle&& needle, Replacement&& replacement)
{
    replace_all(haystack, std::forward<Needle>(needle), std::forward<Replacement>(replacement));
    return haystack;
}

} // namespace MLDB
