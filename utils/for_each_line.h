/** for_each_line.h                                                -*- C++ -*-
    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Class to process each line in a file in parallel.
*/

#pragma once

#include "mldb/utils/log_fwd.h"
#include "mldb/utils/block_splitter.h"
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

template<typename Fn, typename... Args>
struct is_callable_with {
    template<typename F, typename... A> static auto test(int) -> decltype(std::declval<F>()(std::declval<A>()...), std::true_type());
    template<typename F, typename... A> static auto test(...) -> std::false_type;
    using type = decltype(test<Fn, Args...>(0));
    static constexpr bool value = type::value;
};

template<typename Fn, typename... Args> constexpr bool is_callable_with_v = is_callable_with<Fn, Args...>::value;
template<typename Fn, typename... Args> using is_callable_with_t = is_callable_with<Fn, Args...>::type;

struct ContentHandler;

struct LineInfo {
    std::string_view line;
    int64_t lineNumber = -1;
    int64_t blockNumber = -1;
    //int64_t startOffset = -1;
};

struct LineContinuationFn: public ContinuationFn<bool (LineInfo line)> {
    using Base = ContinuationFn<bool (LineInfo)>;
    //template<typename... Args> LineContinuationFn(Args&&... args, std::enable_if_t<std::is_constructible_v<Base, Args...>> * = 0): Base(std::forward<Args>(args)...) {}
    using Base::Base;

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

#if 1
    // Constructor for a function<bool/void (int64_t blockNumber, int64_t lineNumber, size_t numLinesInBlock)>
    template<typename Callable>
    BlockContinuationFn(Callable && fn, decltype(fn(1,2,3)) * = 0)
        : Base([fn = std::move(fn)] (LineBlockInfo block) { return fn(block.blockNumber, block.lineNumber, block.numLinesInBlock); }) { }
#endif
};

struct ChunkInfo {
    std::span<const char> data;
    int64_t chunkNumber = -1;
    int64_t startOffset = -1;
    bool lastChunk = false;
};

struct ChunkContinuationFn: public ContinuationFn<bool (ChunkInfo)> {
    using Base = ContinuationFn<bool (ChunkInfo)>;
    using Base::Base;
};

/** Run the given lambda over every line read from the stream, with the
    work distributed over the given number of threads.

    The processLine function takes a string with the contents of the line,
    without the newline, as a beginning and a length.

    Returns the number of lines produced.
*/
size_t
forEachLine(std::istream & stream,
            const LineContinuationFn & processLine,
            std::shared_ptr<spdlog::logger> logger,
            int numThreads = 4,
            bool ignoreStreamExceptions = false,
            int64_t maxLines = -1);


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
                      int64_t maxLines = -1,
                      int maxParallelism = 8,
                      const BlockContinuationFn & startBlock = nullptr,
                      const BlockContinuationFn & endBlock = nullptr,
                      ssize_t blockSize = -1,
                      const BlockSplitter & splitter = newLineSplitter);

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
                      uint64_t startOffset,
                      const LineContinuationFn & onLine,
                      int64_t maxLines = -1,
                      int maxParallelism = 8,
                      const BlockContinuationFn & startBlock = nullptr,
                      const BlockContinuationFn & endBlock = nullptr,
                      size_t blockSize = 20'000'000,
                      const BlockSplitter & splitter = newLineSplitter);

/** Run the given lambda over fixed size chunks read from the stream, in parallel
    as much as possible.  If there is a smaller chunk at the end (EOF is obtained),
    then the smaller chunk will be returned by itself.

    If any of the lambdas return false, then the process will be stopped once
    all previous lambdas have finished executing.

    If any throw an exception, then the exception will be rethrown once all
    concurrent lambdas have finished executing.
*/
void forEachChunk(std::istream & stream,
                  const ChunkContinuationFn & onChunk,
                  size_t chunkLength,
                  int64_t maxChunks,
                  int maxParallelism);
    
} // namespace MLDB
