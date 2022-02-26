/** for_each_line.h                                                -*- C++ -*-
    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Class to process each line in a file in parallel.
*/

#pragma once

#include "mldb/utils/log_fwd.h"
#include <iostream>
#include <functional>
#include <string>
#include <memory>
#include <any>

namespace MLDB {

struct ContentHandler;

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
    virtual std::pair<const char *, std::any>
    nextBlock(const char * current, size_t n, const std::any & state) const = 0;
};

/* BlockSplitter with a specific state type. */
template<typename State>
struct BlockSplitterT: public BlockSplitter {
    virtual ~BlockSplitterT() = default;
    virtual State newStateT() const { return State(); }
    virtual std::pair<const char *, State>
    nextBlockT(const char * current, size_t n, const State & state) const = 0;
    virtual std::any newState() const override
    {
        return newStateT();
    }
    virtual std::pair<const char *, std::any>
    nextBlock(const char * current, size_t n, const std::any & state) const override
    {
        const State & stateT = std::any_cast<State>(state);
        auto [newPos, newState] = nextBlockT(current, n, stateT);
        return { newPos, std::move(newState) };
    };
};

/* BlockSplitter with no state. */
template<>
struct BlockSplitterT<void>: public BlockSplitter {
    virtual ~BlockSplitterT() = default;
    virtual std::any newState() const override
    {
        return {};
    }
    virtual bool isStateless() const { return true; };
    virtual const char * nextBlockT(const char * current, size_t n) const = 0;
    virtual std::pair<const char *, std::any>
    nextBlock(const char * current, size_t n, const std::any & state) const override
    {
        auto newPos = nextBlockT(current, n);
        return { newPos, {} };
    };
};

/* BlockSplitter that splits on the newline character. */
struct NewlineSplitter: public BlockSplitterT<void> {
    virtual const char * nextBlockT(const char * current, size_t n) const override
    {
        return (const char *)memchr(current, '\n', n);
    }
};

/* Instance of the newline splitter that we can use for default reference arguments. */
extern const NewlineSplitter newLineSplitter;


/** Run the given lambda over every line read from the stream, with the
    work distributed over the given number of threads.

    The processLine function takes a string with the contents of the line,
    without the newline, as a beginning and a length.

    Returns the number of lines produced.
*/
size_t
forEachLine(std::istream & stream,
            const std::function<void (const char * lineStr, size_t lineLen,
                                      int64_t lineNum)> & processLine,
            std::shared_ptr<spdlog::logger> logger,
            int numThreads = 4,
            bool ignoreStreamExceptions = false,
            int64_t maxLines = -1);


/** Run the given lambda over every line read from the stream, with the
    work distributed over the given number of threads.

    The processLine function takes a string with the contents of the line,
    without the newline.

    Returns the number of lines produced.
*/
size_t
forEachLineStr(std::istream & stream,
               const std::function<void (const std::string & line,
                                         int64_t lineNum)> & processLine,
               std::shared_ptr<spdlog::logger> logger,
               int numThreads = 4,
               bool ignoreStreamExceptions = false,
               int64_t maxLines = -1);


/** Run the given lambda over every line read from the file, with the
    work distributed over the given number of threads.

    The processLine function takes a string with the contents of the line,
    without the newline, as a beginning and a length.

    Uses an filter_istream under the hood.
    
    Returns the number of lines produced.
*/
size_t
forEachLine(const std::string & filename,
            const std::function<void (const char * line, size_t lineLength,
                                      int64_t lineNum)> & processLine,
            std::shared_ptr<spdlog::logger> logger,
            int numThreads = 4,
            bool ignoreStreamExceptions = false,
            int64_t maxLines = -1);


/** Run the given lambda over every line read from the file, with the
    work distributed over the given number of threads.

    The processLine function takes a string with the contents of the line,
    without the newline.

    Uses an filter_istream under the hood.
    
    Returns the number of lines produced.
*/
size_t
forEachLineStr(const std::string & filename,
               const std::function<void (const std::string & line,
                                         int64_t lineNum)> & processLine,
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

    This is the second fastest way to parse a text file.
*/

void forEachLineBlock(std::istream & stream,
                      std::function<bool (const char * line,
                                          size_t lineLength,
                                          int64_t blockNumber,
                                          int64_t lineNumber)> onLine,
                      int64_t maxLines = -1,
                      int maxParallelism = 8,
                      std::function<bool (int64_t blockNumber, int64_t lineNumber)> startBlock
                          = nullptr,
                      std::function<bool (int64_t blockNumber, int64_t lineNumber)> endBlock
                          = nullptr,
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

void forEachLineBlock(std::shared_ptr<const ContentHandler> content,
                      uint64_t startOffset,
                      std::function<bool (const char * line,
                                          size_t lineLength,
                                          int64_t blockNumber,
                                          int64_t lineNumber)> onLine,
                      int64_t maxLines = -1,
                      int maxParallelism = 8,
                      std::function<bool (int64_t blockNumber,
                                          int64_t lineNumber,
                                          uint64_t numLines)> startBlock
                          = nullptr,
                      std::function<bool (int64_t blockNumber,
                                          int64_t lineNumber)> endBlock
                          = nullptr,
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
                  std::function<bool (const char * chunk,
                                      size_t chunkLength,
                                      int64_t chunkNumber)> onChunk,
                  size_t chunkLength,
                  int64_t maxChunks,
                  int maxParallelism);
    
} // namespace MLDB
