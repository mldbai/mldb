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

struct ContentHandler;

struct LineInfo {
    std::string_view line;
    int64_t lineNumber = -1;
    int64_t blockNumber = -1;
    int64_t startOffset = -1;
};

struct LineContinuationFn: public ContinuationFn<bool (LineInfo line)> {
    using Base = ContinuationFn<bool (LineInfo)>;
    //template<typename... Args> LineContinuationFn(Args&&... args, std::enable_if_t<std::is_constructible_v<Base, Args...>> * = 0): Base(std::forward<Args>(args)...) {}
    using Base::Base;

#if 1
    // Constructor for a function<bool/void (const char *, size_t, int64_t lineNumber, size_t numLinesInBlock)>
    template<typename Callable/*, typename Return = decltype(std::declval<Callable>()(std::declval<const char *>(), std::declval<int64_t>(), std::declval<int64_t>(), std::declval<int64_t>()))*/>
    LineContinuationFn(Callable fn, std::enable_if_t<std::is_void_v<decltype(fn("hello",5,1,2))> || std::is_convertible_v<bool, decltype(fn("hello",5,0,0))>> * = 0)
        : Base([fn = std::forward<Callable>(fn)] (LineInfo line) { return fn(line.line.data(), line.line.size(), line.blockNumber, line.lineNumber); }) { }

    template<typename Callable/*, typename Return = decltype(std::declval<Callable>()(std::declval<const char *>(), std::declval<int64_t>(), std::declval<int64_t>(), std::declval<int64_t>()))*/>
    LineContinuationFn(Callable fn, std::enable_if_t<std::is_void_v<decltype(fn(std::declval<std::string>(),2))> || std::is_convertible_v<bool, decltype(fn(std::declval<std::string>(),2))>> * = 0)
        : Base([fn = std::forward<Callable>(fn)] (LineInfo line) { return fn(std::string(line.line.data(), line.line.data() + line.line.size()), line.lineNumber); }) { }

    template<typename Callable>
    LineContinuationFn(Callable fn, std::enable_if_t<std::is_void_v<decltype(fn("hello",1,2))> || std::is_convertible_v<bool, decltype(fn("hello",1,2))>> * = 0)
        : Base([fn = std::forward<Callable>(fn)] (LineInfo line) { return fn(line.line.data(), line.line.size(), line.lineNumber); }) { }
#endif
};

struct LineBlockInfo {
    std::span<const char> block;
    int64_t blockNumber = -1;
    int64_t startOffset = -1;
    int64_t lineNumber = -1;
    int64_t numLinesInBlock = -1;
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
    std::span<const char> chunk;
    int64_t chunkNumber = -1;
    int64_t startOffset = -1;
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
                      std::function<bool (const char * line,
                                          size_t lineLength,
                                          int64_t blockNumber,
                                          int64_t lineNumber)> onLine,
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
                      BlockContinuationFn startBlock = nullptr,
                      BlockContinuationFn endBlock = nullptr,
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
