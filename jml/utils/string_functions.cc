// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* string_functions.cc
   Jeremy Barnes, 7 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   


   ---
   
   String manipulation functions.
*/

#include "string_functions.h"
#include <stdarg.h>
#include <stdio.h>
#include "mldb/arch/exception.h"
#include <sys/errno.h>
#include <stdlib.h>
#include <boost/algorithm/string.hpp>
#include "mldb/base/exc_assert.h"
#include <iostream>


using namespace std;
using namespace MLDB;


namespace ML {

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

std::string format(const char * fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    try {
        string result = MLDB::vformat(fmt, ap);
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
        throw Exception(errno, "vasprintf", "format()");

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

std::vector<std::string> split(const std::string & str, char c, int limit)
{
    ExcAssert(limit == -1 || limit > 0);
    vector<string> result;
    size_t start = 0;
    size_t pos = 0;
    while (pos < str.size() && (limit == -1 || limit > 1)) {
        if (str[pos] == c) {
            result.push_back(string(str, start, pos - start));
            start = pos + 1;
            if (limit != -1) {
                --limit;
            }
        }
        ++pos;
    }

    result.push_back(string(str, start));

    return result;
}

std::string lowercase(const std::string & str)
{
    string result = str;
    for (unsigned i = 0;  i < str.size();  ++i)
        result[i] = tolower(result[i]);
    return result;
}

std::string remove_trailing_whitespace(const std::string & str)
{
    int startOfSpace = -1;
    for (unsigned i = 0;  i < str.length();  ++i) {
        if (isspace(str[i])) {
            if (startOfSpace == -1) startOfSpace = i;
        }
        else startOfSpace = -1;
    }

    if (startOfSpace == -1) return str;
    return string(str, 0, startOfSpace);
}

bool removeIfEndsWith(std::string & str, const std::string & ending)
{
    if (str.rfind(ending) == str.size() - ending.length()) {
        str = string(str, 0, str.size() - ending.length());
        return true;
    }
    
    return false;
}

bool endsWith(const std::string & haystack, const std::string & needle)
{
    string::size_type result = haystack.rfind(needle);
    return result != string::npos
        && result == haystack.size() - needle.size();
}

std::string hexify_string(const std::string & str)
{
    size_t i, len(str.size());
    std::string newString;
    newString.reserve(len * 3);

    for (i = 0; i < len; i++) {
        if (str[i] < 32 || str[i] > 127) {
            newString += format("\\x%.2x", int(str[i] & 0xff));
        }
        else {
            newString += str[i];
        }
    }

    return newString;
}

int
antoi(const char * start, const char * end, int base)
{
    int result(0);
    bool neg = false;
    if (*start == '-') {
        if (base == 10) {
            neg = true;
        }
        else {
            throw MLDB::Exception("Cannot negate non base 10");
        }
        start++;
    }
    else if (*start == '+') {
        start++;
    }

    for (const char * ptr = start; ptr < end; ptr++) {
        int digit;
        if (*ptr >= '0' and *ptr <= '9') {
            digit = *ptr - '0';
        }
        else if (*ptr >= 'A' and *ptr <= 'F') {
            digit = *ptr - 'A' + 10;
        }
        else if (*ptr >= 'a' and *ptr <= 'f') {
            digit = *ptr - 'a' + 10;
        }
        else {
            throw MLDB::Exception("expected digit");
        }
        if (digit > base) {
            intptr_t offset = ptr - start;
            throw MLDB::Exception("digit '%c' (%d) exceeds base '%d'"
                                " at offset '%d'",
                                *ptr, digit, base, offset);
        }
        result = result * base + digit;
    }

    if (neg) {
        return result * -1;
    }

    return result;
}

unsigned replace_all(std::string & haystack, const std::string & search,
        const std::string & replace)
{
    unsigned num_found = 0;
    size_t pos = 0;
    while(true) {
        pos = haystack.find(search, pos);
        if(pos == string::npos)
            break;

        num_found ++;
        haystack.replace(pos, search.length(), replace);
        pos += replace.length();
    }
    return num_found;
}

string
trim(const string & other)
{
    size_t len = other.size();

    size_t start(0);
    while (start < len && isspace_nolocale(other[start])) {
        start++;
    }

    size_t end(len);
    while (end > start && isspace_nolocale(other[end-1])) {
        end--;
    }

    if (start == 0 && end == len) {
        return other;
    }

    string result;
    if (start != end) {
        result = other.substr(start, end-start);
    }

    return result;
}

} // namespace ML
