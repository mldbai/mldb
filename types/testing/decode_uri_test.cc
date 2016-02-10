/**
 * decode_uri_test.cc
 * Mich, 2016-02-10
 * This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
 **/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include "mldb/arch/exception.h"
#include "mldb/types/url.h"
#include "mldb/types/string.h"

using namespace std;
using namespace Datacratic;

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
    JML_TRACE_EXCEPTIONS(false);
    Utf8String in("%");
    BOOST_CHECK_THROW(Url::decodeUri(in), ML::Exception);

    in = "%2";
    BOOST_CHECK_THROW(Url::decodeUri(in), ML::Exception);

    in = "%a";
    BOOST_CHECK_THROW(Url::decodeUri(in), ML::Exception);
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
    Utf8String in = "%C3";
    BOOST_CHECK_THROW(Url::decodeUri(in), ML::Exception);
}
#endif
