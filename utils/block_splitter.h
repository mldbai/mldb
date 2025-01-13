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

    // Return an initial splitter state. This is used to initialize the splitter state before the first
    // invocation of nextRecord.
    virtual std::any newState() const = 0;

    // If this is true, then the splitter does not need to carry any state around. This is useful for
    // parallelization as it means there is no dependency chain between calls of the splitter.
    virtual bool isStateless() const { return false; };

    // How many extra characters beyond the end of data are required for the scanning algorithms. Some high
    // speed algorithms operate a full cache line or two at a time, and need this amount of extra data to be
    // readable (even though the contents don't matter) in order to avoid segfaults and operate at maximum
    // speed.
    virtual size_t requiredBlockPadding() const { return 0; }

    // Result of the NextRecord function
    template<typename State>
    struct NextRecordResultT {
        bool foundRecord = false;      // True if a new record was found within the data bounds
        size_t skipToEndOfRecord = 0;  // Number of bytes to skip from the iterator to the end of the record
        size_t skipSeparators = 0;     // Number of extra bytes to skip over the separator

        operator bool() const { return foundRecord; }
        State newState;             // New splitter state corresponding to the beginning of the next record
    };

    // Result of the stateless next record function
    template<>
    struct NextRecordResultT<void> {
        bool foundRecord = false;      // True if a new record was found within the data bounds
        size_t skipToEndOfRecord = 0;  // Number of bytes to skip from the iterator to the end of the record
        size_t skipSeparators = 0;     // Number of extra bytes to skip over the separator

        operator bool() const { return foundRecord; }
    };

    using NextRecordResult = NextRecordResultT<std::any>;


    // Jump the iterator to the beginning of the next record. If a new record has not begun, foundRecord will be
    // false.
    //
    // If foundRecord is true, then the output contains:
    // - The iterator position of the end of the current record
    // - The new splitter state to pass to the splitter
    // - The number of bytes to skip the separator (if any) to get to the next record. The iterator must be
    //   advanced by this number of bytes before this function is called again.
    virtual NextRecordResult
    nextRecord(const TextBlock & data, TextBlockIterator curr, bool noMoreData, const std::any & state) const = 0;
};

/* BlockSplitter with a specific state type. */
template<typename State>
struct BlockSplitterT: public BlockSplitter {
    virtual ~BlockSplitterT() = default;
    virtual State newStateT() const { return State(); }

    virtual NextRecordResultT<State>
    nextRecordT(const TextBlock & data, TextBlockIterator curr, bool noMoreData, const State & state) const = 0;

    virtual std::any newState() const override
    {
        return newStateT();
    }

    // in block_splitter_impl.h
    virtual NextRecordResult
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

    virtual NextRecordResultT<void>
    nextRecordT(const TextBlock & data, TextBlockIterator curr, bool noMoreData) const = 0;

    // in block_splitter_impl.h
    virtual NextRecordResult
    nextRecord(const TextBlock & data, TextBlockIterator curr, bool noMoreData, const std::any & state) const override;
};

/* BlockSplitter that splits on the newline character. */
struct NewlineSplitter: public BlockSplitterT<void> {
    virtual NextRecordResultT<void>
    nextRecordT(const TextBlock & data, TextBlockIterator curr, bool noMoreData) const override;

    static std::span<const char> removeNewlines(std::span<const char> block);
};

/* Instance of the newline splitter that we can use for default reference arguments. */
extern const NewlineSplitter newLineSplitter;


} // namespace MLDB
