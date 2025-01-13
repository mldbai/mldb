/** for_each_line.h                                                -*- C++ -*-
    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Class to process each line in a file in parallel.
*/

#pragma once

#include "mldb/utils/log_fwd.h"
#include "mldb/block/content_descriptor_fwd.h"
#include "mldb/utils/block_splitter.h"
#include "mldb/utils/is_callable_with.h"
#include "mldb/base/compute_context.h"
#include <iostream>
#include <optional>
#include <functional>
#include <string>
#include <string_view>
#include <memory>
#include <any>
#include <span>
#include <cstring>

namespace MLDB {


/*************************************************************************/
/* FOR EACH LINE OPTIONS                                                 */
/*************************************************************************/

// Options for the for_each_line functions. Not all options apply to all
// functions.
struct ForEachLineOptions {
    int maxParallelism = -1;                           // Controls the number of extra threads; 0 = use the main thread only, -1 = all available cores
    ssize_t defaultBlockSize = -1;                     // Granularity of blocks being passed to the processing function
    size_t startOffset = 0;                            // How much of the file to skip at the beginning
    size_t skipLines = 0;                              // How many lines to skip at the beginning
    ssize_t maxLines = -1;                             // Maximum number of lines to process
    bool outputTrailingEmptyLines = false;             // Whether to output trailing empty lines if the file ends with blank lines
    std::shared_ptr<spdlog::logger> logger = nullptr;  // Logger to use for logging
    size_t logInterval = 1000000;                      // How often to log a progress message to the logger (every n lines)
    const BlockSplitter * splitter = &newLineSplitter; // Block splitter to split into lines
};


template<typename Struct, typename Result>
struct OptionValue {
    using Member = Result Struct::*;
    Member member;
    Result value;

    constexpr OptionValue(Member member, Result value): member(member), value(std::move(value)) { }
    void apply(ForEachLineOptions & opts) const { opts.*member = value; }
};

template<typename Struct, typename Result>
struct OptionSpec {
    using Type = Result Struct::*;
    Type member;

    template<typename Arg>
    constexpr OptionValue<Struct, Result> operator = (Arg&& val) const {
        return { member, Result(std::move(val)) };
    }
};

#define DECLARE_OPTION_SPEC(Struct, Member) \
    constexpr OptionSpec<Struct, decltype(Struct::Member)> Member = { &Struct::Member }

namespace ForEachLine {
namespace options {

DECLARE_OPTION_SPEC(ForEachLineOptions, maxParallelism);
DECLARE_OPTION_SPEC(ForEachLineOptions, defaultBlockSize);
DECLARE_OPTION_SPEC(ForEachLineOptions, startOffset);
DECLARE_OPTION_SPEC(ForEachLineOptions, skipLines);
DECLARE_OPTION_SPEC(ForEachLineOptions, maxLines);
DECLARE_OPTION_SPEC(ForEachLineOptions, outputTrailingEmptyLines);
DECLARE_OPTION_SPEC(ForEachLineOptions, logger);
DECLARE_OPTION_SPEC(ForEachLineOptions, logInterval);
DECLARE_OPTION_SPEC(ForEachLineOptions, splitter);

template<typename... Options>
const inline ForEachLineOptions & apply_options(ForEachLineOptions & opts, Options&&... options)
{
    (void)std::initializer_list<int> { (void(options.apply(opts)), 0)... };
    return opts;
}

const inline ForEachLineOptions & apply_options(ForEachLineOptions & opts, ForEachLineOptions & real_opts)
{
    return real_opts;
}

} // namespace ForEachLine
} // namespace options

using namespace ForEachLine;

/*************************************************************************/
/* LINE CONTINUATION FUNCTION                                            */
/*************************************************************************/

struct LineInfo {
    std::string_view line;
    int64_t lineNumber = -1;
    int64_t blockNumber = -1;
    bool lastLine = false;
    //int64_t startOffset = -1;
};

struct LineContinuationFn: public ContinuationFn<bool (LineInfo line)> {
    using Base = ContinuationFn<bool (LineInfo)>;

    // Forward the base constructor    
    template<typename Callable>
    LineContinuationFn(Callable&& fn, std::enable_if_t<is_callable_with_v<Callable, LineInfo>> * = 0)
        : Base(std::forward<Callable>(fn)) {}

    // Constructor for a function<bool/void (const char *, size_t, int64_t lineNumber, size_t numLinesInBlock)>
    template<typename Callable>
    LineContinuationFn(Callable fn, std::enable_if_t<is_callable_with_v<Callable, const char *, size_t, int64_t, size_t>
                                                     && !is_callable_with_v<Callable, const char *, size_t, int64_t>> * = 0)
        : Base([fn = std::forward<Callable>(fn)] (LineInfo line) { return fn(line.line.data(), line.line.size(), line.blockNumber, line.lineNumber); }) { }

    // Constructor for a function<bool/void (std::string line, int64_t lineNumber)>
    template<typename Callable>
    LineContinuationFn(Callable fn, std::enable_if_t<is_callable_with_v<Callable, std::string, int64_t>> * = 0)
        : Base([fn = std::forward<Callable>(fn)] (LineInfo line) { return fn(std::string(line.line.data(), line.line.data() + line.line.size()), line.lineNumber); }) { }

    // Constructor for a function<bool/void (const char *, size_t, int64_t lineNumber)>
    template<typename Callable>
    LineContinuationFn(Callable fn, std::enable_if_t<is_callable_with_v<Callable, const char *, size_t, int64_t>> * = 0)
        : Base([fn = std::forward<Callable>(fn)] (LineInfo line) { return fn(line.line.data(), line.line.size(), line.lineNumber); }) { }
};


/*************************************************************************/
/* BLOCK CONTINUATION FUNCTION                                           */
/*************************************************************************/

struct LineBlockInfo {
    int64_t blockNumber = -1;
    int64_t startOffset = -1;
    int64_t lineNumber = -1;
    int64_t numLinesInBlock = -1;
    bool lastBlock = false;
};

struct BlockContinuationFn: public ContinuationFn<bool (LineBlockInfo block)> {
    using Base = ContinuationFn<bool (LineBlockInfo)>;
    using Base::Base;

    // Constructor for a function<bool/void (int64_t blockNumber, int64_t lineNumber, size_t numLinesInBlock)>
    template<typename Callable>
    BlockContinuationFn(Callable && fn, decltype(fn(1,2,3)) * = 0)
        : Base([fn = std::move(fn)] (LineBlockInfo block) { return fn(block.blockNumber, block.lineNumber, block.numLinesInBlock); }) { }
};

/*************************************************************************/
/* FOR EACH LINE FUNCTIONS                                               */
/*************************************************************************/

/** Run the given lambda over every line read from the stream, with the
    work distributed over the given number of threads.

    The processLine function takes a string with the contents of the line,
    without the newline, as a beginning and a length.

    Returns the number of lines produced.
*/
size_t
forEachLine(std::istream & stream,
            const LineContinuationFn & processLine,
            const ForEachLineOptions & options);

template<typename... Options>
size_t forEachLine(std::istream & stream,
                   const LineContinuationFn & processLine,
                   Options&&... options)
{
    ForEachLineOptions opts;
    return forEachLine(stream, processLine, ForEachLine::options::apply_options(opts, std::forward<Options>(options)...));
}


/** Run the given lambda over every line read from the file, with the
    work distributed over threads each of which receive one block.  The
    threads will be taken from the default thread pool, with a maximum of
    maxParalellism being active.

    If a filter_istream is passed, the code is optimized as it allows
    for the file to be memory mapped.  It should in that case be opened
    with the "mapped" option.

    The startBlock and endBlock functions are called, in the context of
    the processing thread, at the beginning and end of the block
    respectively.

    Returns true if and only if it was stopped by a lambda returning false.
    In that case, the process will be stopped once all previous lambdas
    have finished executing without any guarantees as to which lambdas
    will be started in the meantime.

    This is the second fastest way to parse a text file.
*/

bool forEachLineBlock(std::istream & stream,
                      const LineContinuationFn & onLine,
                      const BlockContinuationFn & startBlock = nullptr,
                      const BlockContinuationFn & endBlock = nullptr,
                      const ForEachLineOptions & options = ForEachLineOptions());

template<typename... Options>
size_t forEachLineBlock(std::istream & stream,
                        const LineContinuationFn & onLine,
                        const BlockContinuationFn & startBlock = nullptr,
                        const BlockContinuationFn & endBlock = nullptr,
                        Options&&... options)
{
    ForEachLineOptions opts;
    return forEachLineBlock(stream, onLine, startBlock, endBlock, ForEachLine::options::apply_options(opts, std::forward<Options>(options)...));
}

/** Run the given lambda over every line read from the file, with the
    work distributed over threads each of which receive one block.  The
    threads will be taken from the default thread pool, with a maximum of
    maxParalellism being active.

    The code is optimized as it uses the memory mapping and caching machinery
    in the Block layer.

    The startBlock and endBlock functions are called, in the context of
    the processing thread, at the beginning and end of the block
    respectively.

    This is the fastest way to parse a text file.
*/

bool forEachLineBlock(std::shared_ptr<const ContentHandler> content,
                      const LineContinuationFn & onLine,
                      const BlockContinuationFn & startBlock = nullptr,
                      const BlockContinuationFn & endBlock = nullptr,
                      const ForEachLineOptions & options = ForEachLineOptions());

template<typename... Options>
size_t forEachLineBlock(std::shared_ptr<const ContentHandler> content,
                        const LineContinuationFn & onLine,
                        const BlockContinuationFn & startBlock = nullptr,
                        const BlockContinuationFn & endBlock = nullptr,
                        Options&&... options)
{
    ForEachLineOptions opts;
    return forEachLineBlock(content, onLine, startBlock, endBlock, ForEachLine::options::apply_options(opts, std::forward<Options>(options)...));
}

} // namespace MLDB
