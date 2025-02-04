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

using namespace std;

const NewlineSplitter newLineSplitter;

NewlineSplitter::NextRecordResultT<void>
NewlineSplitter::
nextRecordT(const TextBlock & data, TextBlockIterator curr, bool noMoreData) const
{
    auto result = data.find(curr, data.end(), '\n');
    if (result != data.end()) {
        // Found a newline, skip it and return
        size_t skip = 1; // skip the newline
        size_t record_end = result - curr;

        // Skip back through the \r if it's a DOS line ending
        if (result != curr && result[-1] == '\r') {
             ++skip;
             --record_end;
        };

        return { true, record_end, skip };
    }

    // No newline but EOF, return up to the last character
    if (noMoreData) {
        size_t skip = 0;
        if (curr != data.end() && result[-1] == '\r') { ++skip; --result; };
        return { true, data.end() - curr - skip, skip };
    }

    // No newline and not EOF, we need more data
    return { false };
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

} // namespace MLDB
