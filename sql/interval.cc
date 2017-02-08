// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** interval.cc
    Mathieu Marquis Bolduc, October 14th 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#include "interval.h"
#include "mldb/base/parse_context.h"
#include "mldb/http/http_exception.h"

using namespace std;


namespace MLDB {


void expect_interval(ParseContext & context, uint32_t& months, uint32_t& days, double& seconds)
{ 
    months = 0;
    days = 0;
    seconds = 0.0;  

    bool signedbit = false;  

    context.skip_whitespace();
    if (context.match_literal('-'))
    {
        signedbit = true;
        context.skip_whitespace();
    } 

    for (;;) {

    	if (context.eof())
            break;
        
        context.skip_whitespace();
        unsigned value = 0;
        // Parse an unsigned value
        {
            ParseContext::Revert_Token token(context);
            if (!context.match_unsigned(value))
        	break;
        }

        // Backtrack and parse a fraction
        double fraction = 0.0;
        context.match_double(fraction);
        context.skip_whitespace();

        // We don't need both; choose one depending upon whether there is a fraction
        // or not
        if (fraction == value)
            fraction = 0.0;
        else value = 0;

        if (context.match_literal("second") || context.match_literal("SECOND") || context.match_literal('s') || context.match_literal('S'))
        {
            seconds += value + fraction;
        }
        else if (context.match_literal("month") || context.match_literal("MONTH"))
        {
            //do abreviation for month because it conflicts with minutes and we dont differenciate between dates and times
            //we read month out of order to that it has priority over "M"...
            if (fraction != 0.0) {
                context.exception("Fractional months not accepted in interval");
            }
            months += value;
        }
        else if (context.match_literal("minute") || context.match_literal("MINUTE") || context.match_literal('m') || context.match_literal('M'))
        {
            if (fraction != 0.0) {
                context.exception("Fractional minutes not accepted in interval");
            }
            seconds += value*60;
        }
        else if (context.match_literal("hour") || context.match_literal("HOUR") || context.match_literal('h') || context.match_literal('H'))
        {
            if (fraction != 0.0) {
                context.exception("Fractional hours not accepted in interval");
            }
            seconds += value*60*60;
        }         
        else if (context.match_literal("day") || context.match_literal("DAY") || context.match_literal('d') || context.match_literal('D'))
        {
            if (fraction != 0.0) {
                context.exception("Fractional days not accepted in interval");
            }
            days += value;
        }
        else if (context.match_literal("week") || context.match_literal("WEEK") || context.match_literal('w') || context.match_literal('W'))
        {
            if (fraction != 0.0) {
                context.exception("Fractional weeks not accepted in interval");
            }
            days += value*7;
        }            
        else if (context.match_literal("year") || context.match_literal("YEAR") || context.match_literal('y') || context.match_literal('Y'))
        {
            if (fraction != 0.0) {
                context.exception("Fractional years not accepted in interval");
            }
            months += 12;
        }
        else
        {
            context.exception("Unexpected symbol parsing time interval");
        }
    }
        
    if (signedbit)
        seconds = -seconds;
}

}
