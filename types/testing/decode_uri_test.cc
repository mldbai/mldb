/**
 * decode_uri_test.cc
 * Mich, 2016-02-10
 * This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
 **/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include "mldb/arch/exception.h"
#include "mldb/types/url.h"
#include "mldb/types/string.h"
#include "ext/googleurl/src/url_util.h"

using namespace std;
using namespace MLDB;

#define TEST_ALL 1

#if TEST_ALL
BOOST_AUTO_TEST_CASE(test_nothing_to_do)
{
    Utf8String expected("nothing to do");
    Utf8String result = Url::decodeUri(expected);
    BOOST_CHECK_EQUAL(result, expected);
}
#endif

#if TEST_ALL
BOOST_AUTO_TEST_CASE(test_percent_sign)
{
    Utf8String in("%25");
    Utf8String expected("%");
    Utf8String result = Url::decodeUri(in);
    BOOST_CHECK_EQUAL(result, expected);
}
#endif

#if TEST_ALL
BOOST_AUTO_TEST_CASE(test_percent_sign_repetition)
{
    Utf8String in("%25%25");
    Utf8String expected("%%");
    Utf8String result = Url::decodeUri(in);
    BOOST_CHECK_EQUAL(result, expected);
}
#endif

#if TEST_ALL
BOOST_AUTO_TEST_CASE(test_percent_sign_with_spaces)
{
    Utf8String in(" %25");
    Utf8String expected(" %");
    Utf8String result = Url::decodeUri(in);
    BOOST_CHECK_EQUAL(result, expected);

    in = " %25 ";
    expected = " % ";
    result = Url::decodeUri(in);
    BOOST_CHECK_EQUAL(result, expected);
}
#endif

#if TEST_ALL
BOOST_AUTO_TEST_CASE(test_invalid_input)
{
    MLDB_TRACE_EXCEPTIONS(false);
    Utf8String in("%");
#if TOLERATE_URL_BAD_ENCODING
    BOOST_CHECK_EQUAL(Url::decodeUri(in), in);
#else
    BOOST_CHECK_THROW(Url::decodeUri(in), MLDB::Exception);
#endif


    in = "%2";
#if TOLERATE_URL_BAD_ENCODING
    BOOST_CHECK_EQUAL(Url::decodeUri(in), in);
#else
    BOOST_CHECK_THROW(Url::decodeUri(in), MLDB::Exception);
#endif

    in = "%a";
#if TOLERATE_URL_BAD_ENCODING
    BOOST_CHECK_EQUAL(Url::decodeUri(in), in);
#else
    BOOST_CHECK_THROW(Url::decodeUri(in), MLDB::Exception);
#endif
}
#endif

#if TEST_ALL
BOOST_AUTO_TEST_CASE(test_utf8)
{
    Utf8String in("%C3%A9");
    Utf8String expected("é");
    Utf8String result = Url::decodeUri(in);
    BOOST_CHECK_EQUAL(result, expected);

    in = "%C3%A9pluche";
    expected = "épluche";
    BOOST_CHECK_EQUAL(Url::decodeUri(in), expected);
}
#endif

#if TEST_ALL
BOOST_AUTO_TEST_CASE(test_utf8_repetitions)
{
    Utf8String in = "%C3%A9%C3%A9";
    Utf8String expected = "éé";
    BOOST_CHECK_EQUAL(Url::decodeUri(in), expected);
}
#endif

#if TEST_ALL
BOOST_AUTO_TEST_CASE(test_invalid_utf8)
{
    MLDB_TRACE_EXCEPTIONS(false);
    Utf8String in = "%C3";
#if TOLERATE_URL_BAD_ENCODING
    Utf8String expected = "Ã";
    BOOST_CHECK_EQUAL(Url::decodeUri(in), expected);
#else
    BOOST_CHECK_THROW(Url::decodeUri(in), MLDB::Exception);
#endif
}
#endif


#if TEST_ALL
BOOST_AUTO_TEST_CASE(test_plus_sign)
{
    Utf8String in("+");
    Utf8String expected("+");
    BOOST_CHECK_EQUAL(Url::decodeUri(in), expected);

    in = "%2B";
    BOOST_CHECK_EQUAL(Url::decodeUri(in), expected);
}
#endif

#if TEST_ALL
BOOST_AUTO_TEST_CASE(test_3_bytes)
{
    Utf8String in("%E2%82%AC");
    Utf8String expected("€");
    BOOST_CHECK_EQUAL(Url::decodeUri(in), expected);
}
#endif

#if TEST_ALL
BOOST_AUTO_TEST_CASE(test_4_bytes)
{
    Utf8String in("%F0%90%8D%88");
    Utf8String expected("𐍈");
    BOOST_CHECK_EQUAL(Url::decodeUri(in), expected);
}
#endif

#if TEST_ALL
BOOST_AUTO_TEST_CASE(test_encode_uri)
{
    string res = Url::encodeUri("http://a b c.com");
    BOOST_CHECK_EQUAL(res, "http://a%20b%20c.com");

    res = Url::encodeUri("http://a%20b.com?1+2=3##𐍈");
    BOOST_CHECK_EQUAL(res, "http://a%2520b.com?1+2=3##%F0%90%8D%88");

    res = Url::encodeUri("http://a/b/c/d");
    BOOST_CHECK_EQUAL(res, "http://a/b/c/d");
}
#endif

#if TEST_ALL
BOOST_AUTO_TEST_CASE(test_url_round_tripping)
{
    string org = "file://./with%20whitespace.py";
    Url url(org);
    BOOST_CHECK_EQUAL(url.toDecodedString(), org);
    BOOST_CHECK_EQUAL(url.toString(), "file://./with%2520whitespace.py");

    org = "file://./with whitespace.py";
    url = Url(org);
    BOOST_CHECK_EQUAL(url.toDecodedString(), org);
    BOOST_CHECK_EQUAL(url.toString(), "file://./with%20whitespace.py");

    org = "file://./with%20white space.py";
    url = Url(org);
    BOOST_CHECK_EQUAL(url.toDecodedString(), org);
    BOOST_CHECK_EQUAL(url.toString(), "file://./with%2520white%20space.py");
}
#endif
