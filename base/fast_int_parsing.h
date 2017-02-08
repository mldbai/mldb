/* fast_int_parsing.h                                              -*- C++ -*-
   Jeremy Barnes, 24 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Routines to quickly parse a base 10 integer.
*/

#pragma once

#include "mldb/base/parse_context.h"
#include <iostream>


using namespace std;


namespace MLDB {

inline bool match_unsigned(unsigned long & val, ParseContext & c)
{
    ParseContext::Revert_Token tok(c);

    val = 0;
    unsigned digits = 0;
    
    while (c) {
        if (isdigit(*c)) {
            int digit = *c - '0';
            val = val * 10 + digit;
            ++digits;
        }
        else break;
        
        ++c;
    }
    
    if (!digits) return false;
    
    tok.ignore();  // we are returning true; ignore the token
    
    return true;
}

inline bool match_int(long int & result, ParseContext & c)
{
    ParseContext::Revert_Token tok(c);

    int sign = 1;
    if (c.match_literal('+')) ;
    else if (c.match_literal('-')) sign = -1;

    long unsigned mag;
    if (!match_unsigned(mag, c)) return false;

    result = mag * sign;

    tok.ignore();
    return true;
}

inline bool match_hex4(long int & result, ParseContext & c)
{
    ParseContext::Revert_Token tok(c);

    int code = 0;
    unsigned digits = 0;
    
    for(unsigned i=0 ; i<4; ++i) {
        int digit;
        if(c && isalnum(*c)) {
            int car = *c;
            if (car >= '0' && car <= '9')
                digit = car - '0';
            else if (car >= 'a' && car <= 'f')
                digit = car - 'a' + 10 ;
            else if (car >= 'A' && car <= 'F')
                digit = car - 'A' + 10;
            else {
                return false;
            }
            code = (code << 4) | digit;
            ++digits;
        }
        else {
            break;
        }
        c++;
    }

    if(digits!=4)
        return false;

    result = code;

    tok.ignore();
    return true;
}

inline bool match_unsigned_long(unsigned long & val,
                                ParseContext & c)
{
    ParseContext::Revert_Token tok(c);

    val = 0;
    int digits = 0;
    
    while (c) {
        if (isdigit(*c)) {
            int digit = *c - '0';
            val = val * 10 + digit;
            ++digits;
        }
        else break;
        
        ++c;
    }
    
    if (!digits) return false;
    
    tok.ignore();  // we are returning true; ignore the token
    
    return true;
}

inline bool match_long(long int & result, ParseContext & c)
{
    ParseContext::Revert_Token tok(c);

    long sign = 1;
    if (c.match_literal('+')) ;
    else if (c.match_literal('-')) sign = -1;

    long unsigned mag;
    if (!match_unsigned_long(mag, c)) return false;
    
    result = (long)mag;
    result *= sign;
    
    tok.ignore();
    return true;
}


inline bool match_unsigned_long_long(unsigned long long & val,
                                     ParseContext & c)
{
    ParseContext::Revert_Token tok(c);

    val = 0;
    int digits = 0;
    
    while (c) {
        if (isdigit(*c)) {
            int digit = *c - '0';
            val = val * 10 + digit;
            ++digits;
        }
        else break;
        
        ++c;
    }
    
    if (!digits) return false;
    
    tok.ignore();  // we are returning true; ignore the token
    
    return true;
}

inline bool match_long_long(long long int & result, ParseContext & c)
{
    ParseContext::Revert_Token tok(c);

    long long sign = 1;
    if (c.match_literal('+')) ;
    else if (c.match_literal('-')) sign = -1;

    long long unsigned mag;
    if (!match_unsigned_long_long(mag, c)) return false;

    result = (long long)mag;
    result *= sign;

    tok.ignore();
    return true;
}

} // namespace MLDB

