/** itoa.h                                                         -*- C++ -*-
    Jeremy Barnes, 23 March 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Fast integer printing functions, since std::to_string(integral) uses
    sprintf and is sloooooow in GCC 4.8.
*/

#pragma once

#include <utility>
#include <type_traits>
#include <limits>
#include <string>
#include <iostream>

namespace MLDB {

constexpr int ITOA_BUF_LENGTH = 64;  // long enough for any integral type

// Buffer type for ITOA
typedef char (ItoaBuf) [ITOA_BUF_LENGTH];

/** Convert the given integer to a string representation, in the given
    statically allocated buffer.  Returns the start and length of the
    returned string; note that buf will be filled from the end, so it
    cannot be used.
*/
template<typename T>
std::pair<char *, char *>
itoa(T i, ItoaBuf buf,
     typename std::enable_if<std::is_unsigned<T>::value>::type * = 0)
{
    char * p = buf + ITOA_BUF_LENGTH;
    do {
        *(--p) = '0' + (i % 10);
        i /= 10;
    } while (i);

    return { p, buf + ITOA_BUF_LENGTH };
}

template<typename T>
std::pair<char *, char *>
itoa(T i, ItoaBuf buf,
     typename std::enable_if<std::is_signed<T>::value>::type * = 0)
{
    using namespace std;

    bool neg = false;
    if (i < 0) {
        if (i == std::numeric_limits<T>::min()) {
            // Can't represent; special case this one
            std::string s = std::to_string(i);
            std::copy(s.begin(), s.end(), buf);
            return { buf, buf + s.length() };
        }
        neg = true;
        i = -i;
    }

    char * p = buf + ITOA_BUF_LENGTH;
    do {
        *(--p) = '0' + (i % 10);
        i /= 10;
    } while (i);

    if (neg)
        *(--p) = '-';

    return { p, buf + ITOA_BUF_LENGTH };
}

template<typename T>
std::string itoa(T i)
{
    ItoaBuf buf;
    char * begin;
    char * end;
    std::tie(begin, end) = itoa(i, buf);
    return std::string(begin, end);
}

} // namespace MLDB
