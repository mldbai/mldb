/** block_splitter.h

    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Classes to identify split points of blocks of records (lines, CSV rows, JSON objects, etc).
*/

#pragma once

#include <span>
#include <any>
#include <vector>
#include <optional>
#include "mldb/utils/coalesced_range_fwd.h"

namespace MLDB {

using TextBlock = CoalescedRange<const char, std::vector<std::span<const char>>>;
using TextBlockIterator = CoalescedRangeIterator<const char, std::vector<std::span<const char>>>;

/* Structure that encapsulates logic to split a block of memory into separate
 * chunks that can be handled in parallel.  The simplest is to split per line
 * of text, but this interface allows state to be carried around which allows
 * for other splitting schemes that can handle quoted or structured records
 * like CSV and JSON.
 */ 
struct BlockSplitter {
    virtual ~BlockSplitter() = default;
    virtual std::any newState() const = 0;
    virtual bool isStateless() const { return false; };
    virtual size_t requiredBlockPadding() const { return 0; }
    virtual std::optional<std::tuple<TextBlockIterator, std::any>>
    nextRecord(const TextBlock & data, TextBlockIterator curr, bool noMoreData, const std::any & state) const = 0;
    virtual std::span<const char> fixupBlock(std::span<const char> block) const;
};

/* BlockSplitter with a specific state type. */
template<typename State>
struct BlockSplitterT: public BlockSplitter {
    virtual ~BlockSplitterT() = default;
    virtual State newStateT() const { return State(); }
    virtual std::optional<std::tuple<TextBlockIterator, State>>
    nextRecordT(const TextBlock & data, TextBlockIterator curr, bool noMoreData, const State & state) const = 0;
    virtual std::any newState() const override
    {
        return newStateT();
    }

    // in block_splitter_impl.h
    virtual std::optional<std::tuple<TextBlockIterator, std::any>>
    nextRecord(const TextBlock & data, TextBlockIterator curr, bool noMoreData, const std::any & state) const override;
};

/* BlockSplitter with no state. */
template<>
struct BlockSplitterT<void>: public BlockSplitter {
    virtual ~BlockSplitterT() = default;
    virtual std::any newState() const override
    {
        return {};
    }
    virtual bool isStateless() const override { return true; };
    virtual std::optional<TextBlockIterator>
    nextRecordT(const TextBlock & data, TextBlockIterator curr, bool noMoreData) const = 0;

    // in block_splitter_impl.h
    virtual std::optional<std::tuple<TextBlockIterator, std::any>>
    nextRecord(const TextBlock & data, TextBlockIterator curr, bool noMoreData, const std::any & state) const override;
};

/* BlockSplitter that splits on the newline character. */
struct NewlineSplitter: public BlockSplitterT<void> {
    virtual std::optional<TextBlockIterator>
    nextRecordT(const TextBlock & data, TextBlockIterator curr, bool noMoreData) const override;

    static std::span<const char> removeNewlines(std::span<const char> block);

    virtual std::span<const char> fixupBlock(std::span<const char> block) const override;
};

/* Instance of the newline splitter that we can use for default reference arguments. */
extern const NewlineSplitter newLineSplitter;


} // namespace MLDB
