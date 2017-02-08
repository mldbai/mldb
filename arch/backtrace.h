/* backtrace.h                                                      -*- C++ -*-
   Jeremy Barnes, 26 February 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Interface to a bactrace function.
*/

#include <iostream>
#include <vector>

#pragma once

namespace MLDB {

/** Basic backtrace information */
struct BacktraceInfo {
    BacktraceInfo()
    {
        frames = new void * [50];
    }

    ~BacktraceInfo()
    {
        delete[] frames;
    }

    const std::type_info * type;
    std::string message;
    void ** frames;
    size_t size;
};


/** Dump a backtrace to the given stream, skipping the given number of
    frames from the top of the trace.
*/
void backtrace(std::ostream & stream = std::cerr, int num_to_skip = 1);
size_t backtrace(char * buffer, size_t bufferSize, int num_to_skip = 1);

/** The information in a backtrace frame. */
struct BacktraceFrame {

    BacktraceFrame(int number = -1, const void * frame = 0,
                   const std::string & symbol = "");

    void init(int number, const void * frame, const std::string & symbol = "");

    int number;
    const void * address;
    std::string function;
    const void * function_start;
    std::string object;
    const void * object_start;
    std::string symbol;

    /** Return a string with all the information. */
    std::string print() const;

    /** Return a string with the specific information for this trace. */
    std::string print_for_trace() const;
};

/** Dump a backtrace into a vector of strings, skipping the given number of
    frames from the top of the trace.
*/
std::vector<BacktraceFrame> backtrace(int num_to_skip);

std::vector<BacktraceFrame>
backtrace(const BacktraceInfo & info, int num_to_skip);

} // namespace MLDB
