// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* format.cc                                                       -*- C++ -*-
   Jeremy Barnes, 26 February 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.
   

   Functions for the manipulation of strings.
*/

#include "format.h"
#include "exception.h"
#include <stdlib.h>
#include <errno.h>
#include <stdio.h>

using namespace std;

namespace MLDB {

struct va_ender {
    va_ender(va_list & ap)
        : ap(ap)
    {
    }

    ~va_ender()
    {
        va_end(ap);
    }

    va_list & ap;
};

std::string formatImpl(const char * fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    try {
        string result = vformat(fmt, ap);
        va_end(ap);
        return result;
    }
    catch (...) {
        va_end(ap);
        throw;
    }
}

std::string vformat(const char * fmt, va_list ap)
{
    char * mem;
    string result;
    int res = vasprintf(&mem, fmt, ap);
    if (res < 0)
        throw Exception("format(): vasprintf error on %s", fmt);

    try {
        result = mem;
        free(mem);
        return result;
    }
    catch (...) {
        free(mem);
        throw;
    }
}

} // namespace MLDB
