// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* string_functions.h                                              -*- C++ -*-
   Jeremy Barnes, 30 January 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   


   ---

   Functions for the manipulation of strings.
*/

#pragma once

#include <sstream>
#include <string>
#include <vector>
#include "mldb/arch/format.h"

namespace ML {


template<typename T>
std::string ostream_format(const T & val)
{
    std::ostringstream str;
    str << val;
    return str.str();
}

std::vector<std::string> split(const std::string & str, char c = ' ', int limit = -1);

std::string lowercase(const std::string & str);

std::string remove_trailing_whitespace(const std::string & str);

/** Implementation of "isspace" that emulates the use of the "C" locale only,
 * without actually executing any locale code. */
inline int isspace_nolocale(int c)
{
    return (c == ' ' || c == '\t' || c == '\n' || c == '\r'
            || c == '\v' || c == '\f');
}

/** If the given string ends with the ending, then remove that ending from the
    string and return true.  Otherwise return false.
*/
bool removeIfEndsWith(std::string & str, const std::string & ending);

bool endsWith(const std::string & haystack, const std::string & needle);

/* replace unprintable characters with a hex representation thereof */
std::string hexify_string(const std::string & str);

/* Parse an integer stored in the chars between "start" and "end",
   where all characters are expected to be strict digits. The name is inspired
   from "atoi" with the "n" indicating that it is reading only from a numbered
   set of bytes. Base 10 can be negative. */
int antoi(const char * start, const char * end, int base = 10);

/* Replace all occurences of search by replace in haystack in place. Returns
 * the number of matching occurences. */
unsigned replace_all(std::string & haystack, const std::string & search,
        const std::string & replace);

/* Remove space and tab characters from both ends of a copy of the given
 * string. */
std::string trim(const std::string & other);

} // namespace ML
