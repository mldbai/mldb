/* json_diff_test.cc
   Jeremy Barnes, 2 November 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test of JSON streams.
*/

#include "catch2/catch_all.hpp"
#include "mldb/utils/json_stream.h"
#include "mldb/arch/demangle.h"
#include "mldb/types/value_description.h"
#include <iostream>
#include <iomanip>
#include <fstream>

using namespace std;
using namespace MLDB;

TEST_CASE("basics", "[none]")
{

    auto parser = createJqStreamProcessor(".");

}

const std::vector<std::string> & getTest(const std::string & file)
{
    static std::map<std::string, std::vector<std::string>> tests;
    if (tests.count(file))
        return tests[file];
    else {
        std::vector<std::string> lines;

        std::ifstream stream("mldb/utils/testing/fixtures/jq/" + file);
        if (!stream)
            throw MLDB::Exception("couldn't open test file");

        while (stream) {
            std::string line;
            std::getline(stream, line);
            if (line.empty())
                continue;
            if (line[0] == '#')
                continue;
            lines.emplace_back(std::move(line));
        }

        cerr << "read " << lines.size() << " lines from file " << file << endl;
        return tests[file] = std::move(lines);
    }
}

void runTestFile(const std::string & file)
{
    auto lines = getTest(file);

    for (int i = 0;  i < 48 /*lines.size()*/;  i += 3) {

        SECTION(file + ":" + std::to_string(i/3) + ": " + lines.at(i)) {
            const string & source   = lines.at(i + 0);
            const string & input    = lines.at(i + 1);
            const string & expected = lines.at(i + 2);

            // Strip byte order mark
            string input2 = input;
            if (input.size() >= 3
                && (unsigned char)input[0] == 0xef
                && (unsigned char)input[1] == 0xbb
                && (unsigned char)input[2] == 0xbf) {
                input2 = string(input2, 3);
            }
            string expected2 = jsonDecodeStr<Json::Value>(expected).toStringNoNewLine();

            auto program = createJqStreamProcessor(source);
            auto json = jsonDecodeStr<Json::Value>(input2);

            //std::vector<Json::Value> array = { json };
            ArrayJsonStreamParsingContext inputContext({json});
            ArrayJsonStreamPrintingContext outputContext;
            program->process(inputContext, outputContext);

            std::string result;
            for (auto & val: outputContext.values()) {
                result += val.toStringNoNewLine();
            }

            CHECK(result == expected2);
        }
    }
}

TEST_CASE("jq-compat-jq", "[none]")
{
    runTestFile("jq.test");
}

#if 0
TEST_CASE("jq-compat-base64", "[none]")
{
    runTestFile("base64.test");
}

TEST_CASE("jq-compat-man", "[none]")
{
    runTestFile("man.test");
}

TEST_CASE("jq-compat-optional", "[none]")
{
    runTestFile("optional.test");
}

TEST_CASE("jq-compat-onig", "[none]")
{
    runTestFile("onig.test");
}
#endif