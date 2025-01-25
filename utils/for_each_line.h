/** for_each_line.h                                                -*- C++ -*-
    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Class to process each line in a file in parallel.
*/

#pragma once

#include "mldb/utils/log_fwd.h"
#include "mldb/utils/block_splitter.h"
#include <iostream>
#include <optional>
#include <functional>
#include <string>
#include <memory>
#include <any>
#include <span>
#include <cstring>

namespace MLDB {

struct ContentHandler;

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
                      std::function<bool (int64_t blockNumber, int64_t lineNumber, uint64_t numLinesInBlock)> startBlock
                          = nullptr,
                      std::function<bool (int64_t blockNumber, int64_t lineNumber, uint64_t numLinesInBlock)> endBlock
                          = nullptr,
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
                                          int64_t lineNumber,
                                          uint64_t numLinesInBlock)> endBlock
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
