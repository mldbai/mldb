/* map_to.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

namespace MLDB {

/// Map a function over a range, returning a container of the specified type with the
/// results in.
template<typename OutputContainer, typename ForwardIterator, typename Pred>
OutputContainer mapTo(ForwardIterator first, ForwardIterator last, Pred && pred)
{
    OutputContainer result;
    result.reserve(std::distance(first, last));
    for (auto it = result.end(); first != last;  ++first, ++it) {
        it = result.insert(it, pred(*first));
    }
    return result;
}

} // namespace MLDB
