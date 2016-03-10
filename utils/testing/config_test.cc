/* config_test.cc
   Guy Dumais, 9 March 2016
   
   This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

   Test of JSON diffs.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/utils/config.h"
#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <iostream>

using namespace std;
using namespace Datacratic;
using namespace MLDB;
using namespace boost::program_options;

BOOST_AUTO_TEST_CASE(test_map_config)
{
    std::unordered_map<std::string, std::string> map = {
        {"string", "string"}, 
        {"int", "123"}, 
        {"bool_int_true", "1"},
        {"bool_int_false", "0"},
        {"bool_str_true", "true"},
        {"bool_str_false", "FaLsE"},
        {"bool_str_true_case", "True"}};

    auto config = Config::createFromMap(map);

    BOOST_CHECK_EQUAL(config->getString("string"), "string");
    BOOST_CHECK_EQUAL(config->getInt("int"), 123);
    BOOST_CHECK_EQUAL(config->getBool("bool_int_true"), true);
    BOOST_CHECK_EQUAL(config->getBool("bool_int_false"), false);
    BOOST_CHECK_EQUAL(config->getBool("bool_str_true"), true);
    BOOST_CHECK_EQUAL(config->getBool("bool_str_false"), false);
    BOOST_CHECK_EQUAL(config->getBool("bool_str_true_case"), true);
    BOOST_CHECK_EQUAL(config->getBool("string"), false);
    BOOST_REQUIRE_THROW(config->getInt("string"), boost::bad_lexical_cast);
}

BOOST_AUTO_TEST_CASE(test_options_config)
{
auto parsed_options = parse_config_file<char>("mldb/utils/testing/config_test.conf", options_description(), true);
    auto config = Config::createFromProgramOptions(parsed_options);   

    BOOST_CHECK_EQUAL(config->getString("string"), "string");
    BOOST_CHECK_EQUAL(config->getInt("int"), 123);
    BOOST_CHECK_EQUAL(config->getBool("bool_int_true"), true);
    BOOST_CHECK_EQUAL(config->getBool("bool_int_false"), false);
    BOOST_CHECK_EQUAL(config->getBool("bool_str_true"), true);
    BOOST_CHECK_EQUAL(config->getBool("bool_str_false"), false);
    BOOST_CHECK_EQUAL(config->getBool("bool_str_true_case"), true);
    BOOST_CHECK_EQUAL(config->getBool("string"), false);
    BOOST_REQUIRE_THROW(config->getInt("string"), boost::bad_lexical_cast);
}


