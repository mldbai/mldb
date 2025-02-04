/** block_splitter_impl.h

    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Classes to identify split points of blocks of records (lines, CSV rows, JSON objects, etc).
*/

#pragma once

#include "block_splitter.h"
#include "coalesced_range.h"

namespace MLDB {

/* BlockSplitter with a specific state type. */
template<typename State>
BlockSplitterT<State>::NextRecordResult
BlockSplitterT<State>::
nextRecord(const TextBlock & data, TextBlockIterator curr, bool noMoreData, const std::any & state) const
{
    const State & stateT = std::any_cast<State>(state);
    auto [found, skip_end, skip_sep, new_state] = nextRecordT(data, noMoreData, stateT);
    return { found, skip_end, skip_sep, std::move(new_state) };
};

/* BlockSplitter with no state. */
BlockSplitterT<void>::NextRecordResult
BlockSplitterT<void>::
nextRecord(const TextBlock & data, TextBlockIterator curr, bool noMoreData, const std::any & state) const
{
    auto [found, skip_end, skip_sep] = nextRecordT(data, curr, noMoreData);
    if (!found) return { false };
    return { true, skip_end, skip_sep, std::any() };
}

} // namespace MLDB