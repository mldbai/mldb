// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* for_each_line_test.cc
   Wolfgang Sourdeau, 28 August 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

   Test for_each_line
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <atomic>
#include <sstream>
#include <string>
#include <vector>
#include <boost/test/unit_test.hpp>
#include "mldb/arch/exception.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/jml/utils/vector_utils.h"

#include "mldb/plugins/for_each_line.h"


using namespace std;
using namespace Datacratic;


namespace {

vector<string> dataStrings{"line1", "line2", "", "line forty 2"};

}

BOOST_AUTO_TEST_CASE( test_forEachLine_data )
{
    vector<string> expected(dataStrings);
    expected.emplace_back("");

    string data;
    for (const auto & line: dataStrings) {
        data += line + "\n";
    }
    istringstream stream(data);

    vector<string> result;
    auto processLine = [&] (const char * data, size_t dataSize, int64_t lineNum) {
        result.emplace_back(data, dataSize);
    };

    forEachLine(stream, processLine);
    BOOST_CHECK_EQUAL(result, expected);
}

BOOST_AUTO_TEST_CASE( test_forEachLineStr_data )
{
    vector<string> expected(dataStrings);
    expected.emplace_back("");

    string data;
    for (const auto & line: dataStrings) {
        data += line + "\n";
    }
    istringstream stream(data);

    vector<string> result;
    auto processLine = [&] (const string & data, int64_t lineNum) {
        result.emplace_back(data);
    };

    forEachLineStr(stream, processLine);
    BOOST_CHECK_EQUAL(result, expected);
}

BOOST_AUTO_TEST_CASE( test_forEachLine_throw )
{
    string data;
    for (int i = 0; i < 1000; i++) {
        data += to_string(i) + "\n";
    }
    istringstream stream(data);

    atomic<int> count(0);
    auto processLine = [&] (const string & data, int64_t lineNum) {
        if (count.fetch_add(1) > 500) {
            throw ML::Exception("thrown");
        }
    };

    BOOST_CHECK_THROW(forEachLineStr(stream, processLine), ML::Exception);
}
