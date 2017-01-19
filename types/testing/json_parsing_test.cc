/* json_parsing_test.cc
   Jeremy Barnes, 21 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test for the environment functions.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/types/json_parsing.h"
#include "mldb/base/parse_context.h"
#include <boost/test/unit_test.hpp>
#include <boost/test/auto_unit_test.hpp>
#include <math.h>

using namespace MLDB;

using boost::unit_test::test_suite;

void testUnsigned(const std::string & str, unsigned long long expected)
{
    ParseContext context(str, str.c_str(), str.c_str() + str.size());
    auto val = expectJsonNumber(context);
    context.expect_eof();
    BOOST_CHECK_EQUAL(val.uns, expected);
    BOOST_CHECK_EQUAL(val.type, JsonNumber::UNSIGNED_INT);
}

void testSigned(const std::string & str, long long expected)
{
    ParseContext context(str, str.c_str(), str.c_str() + str.size());
    auto val = expectJsonNumber(context);
    context.expect_eof();
    BOOST_CHECK_EQUAL(val.uns, expected);
    BOOST_CHECK_EQUAL(val.type, JsonNumber::SIGNED_INT);
}

void testFp(const std::string & str, double expected)
{
    ParseContext context(str, str.c_str(), str.c_str() + str.size());
    auto val = expectJsonNumber(context);
    context.expect_eof();
    BOOST_CHECK_EQUAL(val.fp, expected);
    BOOST_CHECK_EQUAL(val.type, JsonNumber::FLOATING_POINT);
}

void testHex4(const std::string & str, long long expected)
{
    ParseContext context(str, str.c_str(), str.c_str() + str.size());
    auto val = context.expect_hex4();
    context.expect_eof();
    BOOST_CHECK_EQUAL(val, expected);
}

BOOST_AUTO_TEST_CASE( test1 )
{
    testUnsigned("0", 0);
    testSigned("-0", 0);
    testSigned("-1", -1);
    testFp("0.", 0.0);
    testFp(".1", 0.1);
    testFp("-.1", -0.1);
    testFp("0.0", 0.0);
    testFp("1e0", 1e0);
    testFp("-1e0", -1e0);
    testFp("-1e+0", -1e+0);
    testFp("-1e-0", -1e-0);
    testFp("-1E+3", -1e+3);
    testFp("1.0E-3", 1.0E-3);
    testFp("Inf", INFINITY);
    testFp("-Inf", -INFINITY);

    testHex4("0026", 38);
    testHex4("001A", 26);
    
    MLDB_TRACE_EXCEPTIONS(false);
    BOOST_CHECK_THROW(testFp(".", 0.1), std::exception);
    BOOST_CHECK_THROW(testFp("", 0.1), std::exception);
    BOOST_CHECK_THROW(testFp("e3", 0.1), std::exception);
    BOOST_CHECK_THROW(testFp("3e", 0.1), std::exception);
    BOOST_CHECK_THROW(testFp("3.1aade", 0.1), std::exception);
    
    BOOST_CHECK_THROW(testHex4("002", 2), std::exception);
    BOOST_CHECK_THROW(testHex4("002G", 2), std::exception);
    BOOST_CHECK_THROW(testHex4("002.", 2), std::exception);
}

void testExpectStringUtf8(ParseContext * context)
{
    skipJsonWhitespace((*context));
    context->expect_literal('"');

    char internalBuffer[4096];

    char * buffer = internalBuffer;
    size_t bufferSize = 4096;
    size_t pos = 0;

    // Keep expanding until it fits
    while (!context->match_literal('"')) {
        // We need up to 4 characters to add a new UTF-8 code point
        if (pos >= bufferSize - 4) {
            size_t newBufferSize = bufferSize * 8;
            char * newBuffer = new char[newBufferSize];
            std::copy(buffer, buffer + bufferSize, newBuffer);
            if (buffer != internalBuffer)
                delete[] buffer;
            buffer = newBuffer;
            bufferSize = newBufferSize;
        }

        int c = *(*context);
        
        //cerr << "c = " << c << " " << (char)c << endl;

        if (c < 0 || c > 127) {
            // Unicode
            c = utf8::unchecked::next(*context);

            char * p1 = buffer + pos;
            char * p2 = p1;
            pos += utf8::append(c, p2) - p1;

            continue;
        }
        ++(*context);

        if (c == '\\') {
            c = *(*context)++;
            switch (c) {
            case 't': c = '\t';  break;
            case 'n': c = '\n';  break;
            case 'r': c = '\r';  break;
            case 'f': c = '\f';  break;
            case 'b': c = '\b';  break;
            case '/': c = '/';   break;
            case '\\':c = '\\';  break;
            case '"': c = '"';   break;
            case 'u': {
                (void) *(*context);
                int code = context->expect_hex4();
                c = code;
                break;
            }
            default:
                context->exception("invalid escaped char");
            }
        }

        if (c < ' ' || c >= 127) {
            char * p1 = buffer + pos;
            char * p2 = p1;
            pos += utf8::append(c, p2) - p1;
        }
        else buffer[pos++] = c;
    }
}

BOOST_AUTO_TEST_CASE(test_utf8_bad_string)
{
    std::string s = "\"http\\u00253A\\u00252F\\u";
    ParseContext context(s, s.c_str(), s.c_str() + s.length());
    BOOST_CHECK_THROW(testExpectStringUtf8(&context), ParseContext::Exception);
}
