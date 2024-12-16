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
#include "array_limits.h"

namespace MLDB {

template<typename Haystack, typename Needle, typename Replacement>
void replace_all(Haystack& haystack, Needle&& needle, Replacement&& replacement)
{
    using namespace std;

    using HaystackIt = decltype(arr_begin(haystack));

    std::vector<std::pair<HaystackIt, HaystackIt>> matches;

    const auto nbeg = arr_begin(needle), nend = arr_end(needle);
    const auto hbeg = arr_begin(haystack), hend = arr_end(haystack);

#if 0
    int i = 0;
    for (auto it = nbeg; it != nend; ++it, ++i) {
        cerr << "needle[" << i << "] = " << *it << endl;
    }

    i = 0;
    for (auto it = hbeg; it != hend; ++it, ++i) {
        cerr << "haystack[" << i << "] = " << *it << endl;
    }
#endif

    // Find all places where it matches. We find them all at the beginning so that the replacement
    // doesn't cause any problems.
    //auto last = hbeg;
    for (auto it = hbeg, end = hend; it != end; /* no inc */) {
        auto it2 = it;
        auto nit = nbeg;
        for (; it2 != end && nit != nend && *it2 == *nit; ++it2, ++nit);
        if (nit == nend) {
            matches.emplace_back(it, it2);
            //cerr << "added match " << string(it, it2) << endl;
            //cerr << "needle distance = " << std::distance(nbeg, nend) << endl;
            it = it2;
            if (nbeg == nend && it != end) {
                // Special case: empty needle. We need to make sure we don't get stuck in an infinite loop
                ++it;
            }
        }
        else {
            ++it;
        }

        //if (it == last) {
        //    throw std::logic_error("no progress");
        //}
        //last = it;
    }

#if 0
    // Special case: empty needle and empty haystack. Should be one replacement to match
    // boost::replace_all
    if (nbeg == nend && hbeg == hend) {
        matches.emplace_back(hbeg, hend);
    }
#endif

    if (matches.empty())
        return;

    //cerr << "got " << matches.size() << " matches" << endl;

    // Total length of the replacement
    size_t replacement_length = arr_size(haystack) + (arr_size(matches) * (arr_size(replacement) - arr_size(needle)));

    auto rbeg = arr_begin(replacement);
    auto rend = arr_end(replacement);

    // TODO: lots of possible optimizations
    typename std::remove_reference_t<Haystack> result;
    result.reserve(replacement_length);
    auto it = hbeg;
    for (auto [mbeg, mend]: matches) {
        result.append(it, mbeg);
        result.append(rbeg, rend);
        it = mend;
    }
    result.append(it, hend);

    std::swap(haystack, result);
}

template<typename Haystack, typename Needle, typename Replacement>
Haystack replace_all_copy(Haystack haystack, Needle&& needle, Replacement&& replacement)
{
    replace_all(haystack, std::forward<Needle>(needle), std::forward<Replacement>(replacement));
    return haystack;
}

} // namespace MLDB
