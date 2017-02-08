/* fast_float_parsing.h                                            -*- C++ -*-
   Jeremy Barnes, 25 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.   

   Fast inline float parsing routines.
*/

#pragma once

#include "mldb/base/parse_context.h"
#include <limits>
#include <errno.h>

namespace MLDB {

double binary_exp10 [10] = {
    10,
    100,
    1e4,
    1e8,
    1e16,
    1e32,
    1e64,
    1e128,
    1e256,
    INFINITY
};

double binary_exp10_neg [10] = {
    0.1,
    0.01,
    1e-4,
    1e-8,
    1e-16,
    1e-32,
    1e-64,
    1e-128,
    1e-256,
    0.0
};

double
exp10_int(int val)
{
    double result = 1.0;

    if (val >= 0) {
        for (unsigned i = 0;  val;  ++i) {
            if (i >= 9)
                return INFINITY;
            if (val & 1)
                result *= binary_exp10[i];
            val >>= 1;
        }
    }
    else {
        val = -val;
        for (unsigned i = 0;  val;  ++i) {
            if (i >= 9)
                return 0.0;
            if (val & 1)
                result *= binary_exp10_neg[i];
            val >>= 1;
        }
    }

    return result;
}

/* Floating point parsing is not so easy...
   We assume that the parse context will be at the beginning of the float decimal
   representation, that is, there is no attempt to trim whitespaces.  The parsing 
   will stop at the last character matching a float decimal representation irrespective
   of the following characters (e.g. 1.0e-100somegargagehere will parse as
   1.0e-100).

   If lenient is set to false, the function will not accept integers.

   The user is responsible for setting the locale before this function is called;
   its output is undefined if the system locale is set to anything other than
   LC_NUMERIC=C.
*/

template<typename Float>
inline bool match_float(Float & result, ParseContext & c, bool lenient = true)
{
    if (c.eof()) return false;

    ParseContext::Revert_Token tok(c);
    // perfect decimal representation of double precision floats
    // should fit in 256 chars.
    char buf[256];
    size_t from = c.get_offset();
    bool some_digits = true;

    // sign
    if (*c == '+' || *c == '-') {
        buf[0] = *c;
        ++c;
    }

    if (c.eof()) return false;
    
    // special cases NaN and Inf
    if (*c == 'n' || *c == 'N') {
        buf[c.get_offset() - from] = 'N';
        ++c;
        if (!c.eof() && (*c == 'a' || *c == 'A')) {
            buf[c.get_offset() - from] = 'a';
            ++c;
            if (!c.eof() && (*c == 'n' || *c == 'N')) {
                buf[c.get_offset() - from] = 'N';
                c++;
            } else return false;
        } else return false;
    } else if (!c.eof() && (*c == 'i' || *c == 'I')) {
        buf[c.get_offset() - from] = 'I';
        ++c;
        if (!c.eof() && (*c == 'n' || *c == 'N')) {
            buf[c.get_offset() - from] = 'n';
            ++c;
            if (!c.eof() && (*c == 'f' || *c == 'F')) {
                buf[c.get_offset() - from] = 'f';
                c++;
            } else return false;
        } else return false;
    } else {
        // digit
        if (!c.eof() && (isdigit(*c) || *c == '.')) {
            for (; c.get_offset() - from < 255 && !c.eof() && isdigit(*c); c++) {
                some_digits = true;
                buf[c.get_offset() - from] = *c;
            }
        }
        else return false;
        
        bool is_a_float = false;
        
        // . and more digit
        if (!c.eof() && *c == '.') {
            buf[c.get_offset() - from] = '.';
            ++c;

            if (isdigit(*c)) {
                for(; c.get_offset() - from < 255 && !c.eof() && isdigit(*c); c++) {
                    buf[c.get_offset() - from] = *c;
                    is_a_float = true;
                }
            }
            else return false;
        } 

        // exponential marker
        if (!c.eof() && (*c == 'e' || *c == 'E')) {
            // allow backtracking in case this is not part of the float
            ParseContext::Revert_Token token(c);
            buf[c.get_offset() - from] = 'e';
            ++c;
        
            if (!c.eof() && (*c == '+' || *c == '-')) {
                buf[c.get_offset() - from] = *c;
                ++c;
            }

            if (isdigit(*c)) {
                for(; c.get_offset() - from < 255 && !c.eof() && isdigit(*c); c++)
                    buf[c.get_offset() - from] = *c;
                token.ignore();
                is_a_float = true;
            }
            else if (!some_digits) return false;
        }

        if (!is_a_float && !lenient)
            return false;
    }
    
    buf[c.get_offset() - from] = 0;
    char * endptr = nullptr;
    //const char* previous = setlocale(LC_NUMERIC, "C");
    double parsed = strtod(buf, &endptr);
    //setlocale(LC_NUMERIC, previous);
    
    if (!endptr && !parsed)
        return false;

    if (endptr != buf + c.get_offset() - from)
        throw Exception("wrong endptr");

    tok.ignore();
    result = parsed;
    return true;        
}

template<typename Float>
inline Float expect_float(ParseContext & c,
                          const char * error = "expected real number")
{
    Float result;
    if (!match_float(result, c))
        c.exception(error);
    return result;
}


} // namespace MLDB
