/** for_each_line.h                                                -*- C++ -*-
    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Class to process each line in a file in parallel.
*/

#pragma once

#include <iostream>
#include <functional>
#include <string>

namespace Datacratic {


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

    This is the fastest way to parse a text file.
*/

void forEachLineBlock(std::istream & stream,
                      std::function<bool (const char * line,
                                          size_t lineLength,
                                          int64_t blockNumber,
                                          int64_t lineNumber)> onLine,
                      int64_t maxLines = -1,
                      int maxParallelism = 8);
    
    
} // namespace Datacratic
