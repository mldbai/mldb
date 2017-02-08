// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* json_handling_test.cc
   Jeremy Barnes, 29 March 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   Test of the functionality to handle JSON.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <iostream>
#include "mldb/jml/db/persistent.h"
#include "mldb/types/string.h"
#include "mldb/types/json_parsing.h"
#include "mldb/types/json_printing.h"
#include "mldb/base/parse_context.h"

using namespace MLDB;
using namespace std;

BOOST_AUTO_TEST_CASE(test_utf8_round_trip_streaming_binary)
{
    Utf8String str("\xe2\x80\xa2skin");
    
    std::ostringstream stream;

    {
        StreamJsonPrintingContext context(stream);
        context.writeUtf8 = true;
        context.writeStringUtf8(str);
    }

    cerr << stream.str() << endl;

    BOOST_CHECK_EQUAL(stream.str(), "\"\xe2\x80\xa2skin\"");

    {
        StringJsonParsingContext context(stream.str());
        Utf8String str2 = context.expectStringUtf8();
        BOOST_CHECK_EQUAL(str, str2);
    }
}

BOOST_AUTO_TEST_CASE(test_utf8_round_trip_streaming_ascii)
{
    Utf8String str("\xe2\x80\xa2skin");
    
    std::ostringstream stream;

    {
        StreamJsonPrintingContext context(stream);
        context.writeUtf8 = false;
        context.writeStringUtf8(str);
    }

    cerr << stream.str() << endl;

    BOOST_CHECK_EQUAL(stream.str(), "\"\\u2022skin\"");

    {
        StringJsonParsingContext context(stream.str());
        Utf8String str2 = context.expectStringUtf8();
        BOOST_CHECK_EQUAL(str, str2);
    }
}

BOOST_AUTO_TEST_CASE(test_utf8_bad_string)
{
    const std::string payload = "\"http\\u00253A\\u00252F\\u";
    const char* start = payload.c_str();                                        
    StreamingJsonParsingContext context(payload, start, start + payload.size());
    BOOST_CHECK_THROW(context.expectStringUtf8(), ParseContext::Exception);
}

BOOST_AUTO_TEST_CASE(test_json_encode_decode_long_strings)
{
    string needsEscaping;
    string needsNoEscaping;
    for (unsigned i = 0;  i < 10000000;  ++i) {
        needsEscaping += (' ' + (i % 90));
        needsNoEscaping += 'A' + (i % 26);
    }

    string result1 = jsonEscape(needsEscaping);

    BOOST_CHECK_GT(result1.length(), needsEscaping.length());

    string result2 = jsonEscape(needsNoEscaping);

    BOOST_CHECK_EQUAL(result2.length(), needsNoEscaping.length());
    BOOST_CHECK(result2 == needsNoEscaping);


    const std::string payload = "\"http\\u00253A\\u00252F\\u";
    const char* start = payload.c_str();                                        
    StreamingJsonParsingContext context(payload, start, start + payload.size());
    BOOST_CHECK_THROW(context.expectStringUtf8(), ParseContext::Exception);
}
