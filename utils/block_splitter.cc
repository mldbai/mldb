/** block_splitter.cc

    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Classes to identify split points of blocks of records (lines, CSV rows, JSON objects, etc).
*/

#include "block_splitter.h"
#include "block_splitter_impl.h"
#include "mldb/utils/coalesced_range.h"

namespace MLDB {

const NewlineSplitter newLineSplitter;

std::span<const char>
BlockSplitter::
fixupBlock(std::span<const char> block) const
{
    return block;
}

std::optional<TextBlockIterator>
NewlineSplitter::
nextRecordT(const TextBlock & data, TextBlockIterator curr, bool noMoreData) const
{
    auto result = data.find(curr, data.end(), '\n');

    // Found a newline, skip it and return
    if (result != data.end())
        return result + 1;

    // No newline but EOF, return up to the last character
    if (noMoreData)
        return data.end();

    // No newline and not EOF, we need more data
    return std::nullopt;
}

std::span<const char>
NewlineSplitter::
removeNewlines(std::span<const char> block)
{
    const char * p = block.data();
    const char * e = p + block.size();
    if (e > p && e[-1] == '\n') --e;
    if (e > p && e[-1] == '\r') --e;
    return { p, size_t(e - p) };
}

std::span<const char>
NewlineSplitter::
fixupBlock(std::span<const char> block) const
{
    return removeNewlines(block);
}

} // namespace MLDB
