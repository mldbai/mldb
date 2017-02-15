// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#define BOOST_AUTO_TEST_MAIN
#include <boost/test/auto_unit_test.hpp>

#include "mldb/jml/utils/string_functions.h"
#include "mldb/arch/exception.h"

using namespace std;
using namespace ML;

using boost::unit_test::test_suite;

BOOST_AUTO_TEST_CASE( test_antoi )
{
    const char * c = "12";
    BOOST_CHECK(antoi(c, c + 2) == 12);

    const char * d  = "-1";
    BOOST_CHECK(antoi(d, d + 2) == -1);

    const char * p = "patate";
    BOOST_CHECK_THROW(antoi(p, p + 6), MLDB::Exception);
}

BOOST_AUTO_TEST_CASE( test_string_replace_in_place )
{
    string my_str = "Bouya$toReplaceBouya$toReplace2";

    unsigned num_found = replace_all(my_str, "$toReplace", "hoho");
    BOOST_CHECK_EQUAL(num_found, 2);
    BOOST_CHECK_EQUAL(my_str, "BouyahohoBouyahoho2");
    
    my_str = "$toRepBouya";
    num_found = replace_all(my_str, "$toRep", "hoho");
    BOOST_CHECK_EQUAL(my_str, "hohoBouya");
    
    my_str = "Bouya$toRep";
    num_found = replace_all(my_str, "$toRep", "hoho");
    BOOST_CHECK_EQUAL(my_str, "Bouyahoho");
    
    my_str = "$toRepBouya$toRep";
    num_found = replace_all(my_str, "$toRep", "hoho");
    BOOST_CHECK_EQUAL(my_str, "hohoBouyahoho");
}

BOOST_AUTO_TEST_CASE( test_string_trim )
{
    {
        string original = "notrimming";
        string expected = original;
        string result = ML::trim(original);
        BOOST_CHECK_EQUAL(result, expected);
    }

    {
        string original = " \t  left trimming";
        string expected = "left trimming";
        string result = ML::trim(original);
        BOOST_CHECK_EQUAL(result, expected);
    }

    {
        string original = "right trimming    \t  ";
        string expected = "right trimming";
        string result = ML::trim(original);
        BOOST_CHECK_EQUAL(result, expected);
    }

    {
        string original = "";
        string expected = "";
        string result = ML::trim(original);
        BOOST_CHECK_EQUAL(result, expected);
    }

    {
        string original = " ";
        string expected = "";
        string result = ML::trim(original);
        BOOST_CHECK_EQUAL(result, expected);
    }

    {
        string original = "\t\t\ta";
        string expected = "a";
        string result = ML::trim(original);
        BOOST_CHECK_EQUAL(result, expected);
    }

    {
        string original = "a ";
        string expected = "a";
        string result = ML::trim(original);
        BOOST_CHECK_EQUAL(result, expected);
    }

    {
        string original = " \f\r\n\t\v \f\r\n\t\va \f\r\n\t\v \f\r\n\t\v";
        string expected = "a";
        string result = ML::trim(original);
        BOOST_CHECK_EQUAL(result, expected);
    }
}

BOOST_AUTO_TEST_CASE( test_string_split )
{
    vector<string> res;
    res = ML::split("", '-');
    BOOST_REQUIRE_EQUAL(res.size(), 1);
    BOOST_REQUIRE_EQUAL(res[0], "");

    res = ML::split("a-b-c", '-');
    BOOST_REQUIRE_EQUAL(res.size(), 3);
    BOOST_REQUIRE_EQUAL(res[0], "a");
    BOOST_REQUIRE_EQUAL(res[1], "b");
    BOOST_REQUIRE_EQUAL(res[2], "c");

    res = ML::split("a-b-c", '-', -1);
    BOOST_REQUIRE_EQUAL(res.size(), 3);
    BOOST_REQUIRE_EQUAL(res[0], "a");
    BOOST_REQUIRE_EQUAL(res[1], "b");
    BOOST_REQUIRE_EQUAL(res[2], "c");

    BOOST_REQUIRE_THROW(ML::split("a-b-c", '-', 0), std::exception);
    BOOST_REQUIRE_THROW(ML::split("a-b-c", '-', -2), std::exception);

    res = ML::split("a-b-c", '-', 1);
    BOOST_REQUIRE_EQUAL(res.size(), 1);
    BOOST_REQUIRE_EQUAL(res[0], "a-b-c");

    res = ML::split("a-b-c", '-', 2);
    BOOST_REQUIRE_EQUAL(res.size(), 2);
    BOOST_REQUIRE_EQUAL(res[0], "a");
    BOOST_REQUIRE_EQUAL(res[1], "b-c");

    res = ML::split("a-b-c", '-', 3);
    BOOST_REQUIRE_EQUAL(res.size(), 3);
    BOOST_REQUIRE_EQUAL(res[0], "a");
    BOOST_REQUIRE_EQUAL(res[1], "b");
    BOOST_REQUIRE_EQUAL(res[2], "c");

    res = ML::split("a-b-c", '-', 1000);
    BOOST_REQUIRE_EQUAL(res.size(), 3);
    BOOST_REQUIRE_EQUAL(res[0], "a");
    BOOST_REQUIRE_EQUAL(res[1], "b");
    BOOST_REQUIRE_EQUAL(res[2], "c");
}
