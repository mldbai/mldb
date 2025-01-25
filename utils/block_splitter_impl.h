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
std::optional<std::tuple<TextBlockIterator, std::any>>
BlockSplitterT<State>::
nextRecord(const TextBlock & data, TextBlockIterator curr, bool noMoreData, const std::any & state) const
{
    const State & stateT = std::any_cast<State>(state);
    auto [newPos, newState] = nextRecordT(data, noMoreData, stateT);
    return { newPos, std::move(newState) };
};

/* BlockSplitter with no state. */
std::optional<std::tuple<TextBlockIterator, std::any>>
BlockSplitterT<void>::
nextRecord(const TextBlock & data, TextBlockIterator curr, bool noMoreData, const std::any & state) const
{
    auto newPos = nextRecordT(data, curr, noMoreData);
    if (!newPos)
        return std::nullopt;
    std::tuple<TextBlockIterator, std::any> result(*newPos, std::any());
    return result;
}

} // namespace MLDB