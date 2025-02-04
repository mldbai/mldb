/* coalesced_range.h"

    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Class to process each line in a file in parallel.
*/

#pragma once

#include <vector>
#include <span>

namespace MLDB {

template<typename Value, typename Ranges> struct CoalescedRangeIterator;
template<typename Value, typename Ranges=std::vector<std::span<Value>>> struct CoalescedRange;

} // namespace MLDB
