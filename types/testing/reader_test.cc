// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* date_test.cc
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#include <boost/test/unit_test.hpp>
#include "mldb/ext/jsoncpp/json.h"
#include <stdexcept>
#include "mldb/arch/exception.h"
#include "mldb/arch/exception_handler.h"

using namespace std;


BOOST_AUTO_TEST_CASE( test_free_reader )
{
    Json::Value x = Json::parse("{\"a\":\"b\"}");
    BOOST_CHECK_EQUAL(x["a"], "b");
}

BOOST_AUTO_TEST_CASE( test_single_quotes )
{
    Json::Value x = Json::parse("{'a':'b'}");
    BOOST_CHECK_EQUAL(x["a"], "b");
}

BOOST_AUTO_TEST_CASE( test_escapes )
{
    {
        Json::Value x = Json::parse("{'a':'b\\'s'}");
        BOOST_CHECK_EQUAL(x["a"], "b's");
    }

    {
        Json::Value x = Json::parse("{'a':'b\"s'}");
        BOOST_CHECK_EQUAL(x["a"], "b\"s");
    }

    {
        Json::Value x = Json::parse("{'a':'b\\\"s'}");
        BOOST_CHECK_EQUAL(x["a"], "b\"s");
    }

    {
        Json::Value x = Json::parse("{\"a\":\"b\\\"s\"}");
        BOOST_CHECK_EQUAL(x["a"], "b\"s");
    }

    {
        Json::Value x = Json::parse("{\"a\":\"b's\"}");
        BOOST_CHECK_EQUAL(x["a"], "b's");
    }

    {
        Json::Value x = Json::parse("{\"a\":\"b\\'s\"}");
        BOOST_CHECK_EQUAL(x["a"], "b's");
    }
}

BOOST_AUTO_TEST_CASE( test_bad_parse )
{
    MLDB_TRACE_EXCEPTIONS(false);
    BOOST_CHECK_THROW(auto x = Json::parse("foo");, Json::Exception);
    BOOST_CHECK_THROW(auto x = Json::parse("{'a's':'a'}");, Json::Exception);
    BOOST_CHECK_THROW(auto x = Json::parse("{\"a\"s\":\"b\"}");, Json::Exception);
}

BOOST_AUTO_TEST_CASE( test_no_assert )
{
    MLDB_TRACE_EXCEPTIONS(false);
    Json::Value x = 1;
    BOOST_CHECK_THROW(x[0u], std::runtime_error);
}


BOOST_AUTO_TEST_CASE( test_iterators )
{
    Json::Value x;
    x[0u] = "elem0";
    x[1u] = "elem1";
    x[2u] = "elem2";

    std::vector<Json::Value> xCopy(3);

    std::copy(x.begin(), x.end(), xCopy.begin());

    BOOST_CHECK_EQUAL_COLLECTIONS(x.begin(), x.end(), xCopy.begin(), xCopy.end());

}

BOOST_AUTO_TEST_CASE( test_from_file )
{
    Json::Value json = Json::parseFromFile("mldb/types/testing/fixtures/somejson.json");
    BOOST_CHECK_EQUAL(json["octo"], "sanchez");
}
