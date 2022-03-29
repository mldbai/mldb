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

struct Test {
    int lineStart;
    std::string program;
    std::string input;
    std::vector<std::string> expected;
    bool expectedFail = false;
};

const std::vector<Test> & getTests(const std::string & file)
{
    static std::map<std::string, std::vector<Test>> tests;
    if (tests.count(file))
        return tests[file];
    else {
        std::vector<Test> parsed;

        std::ifstream stream("mldb/utils/testing/fixtures/jq/" + file);
        if (!stream)
            throw MLDB::Exception("couldn't open test file");

        int lineNumber = 1;
        while (stream) {
            std::string line;
            std::getline(stream, line);
            ++lineNumber;
            if (line.empty()) {
                continue;
            }
            if (line[0] == '#') {
                ++lineNumber;
                continue;
            }

            Test test;
            test.lineStart = lineNumber;

            if (line == "%%FAIL") {
                ++lineNumber;
                std::getline(stream, line);
                test.expectedFail = true;
            }
            test.program = line;
            if (!test.expectedFail) {
                std::getline(stream, test.input);
                ++lineNumber;
            }
            for (;;) {
                std::getline(stream, line);
                ++lineNumber;
                if (line.empty())
                    break;
                test.expected.emplace_back(std::move(line));
            }

            parsed.emplace_back(std::move(test));
        }

        cerr << "read " << parsed.size() << " tests from file " << file << endl;
        return tests[file] = std::move(parsed);
    }
}

void runTestFile(const std::string & file)
{
    auto tests = getTests(file);

    int n = tests.size();
    n = 60;
    for (int i = 0;  i < n;  ++i) {

        SECTION(file + ":" + std::to_string(i) + " (" + file + ":" + std::to_string(tests[i].lineStart) + "): " + tests.at(i).program) {
            const Test & test = tests[i];

            // Strip byte order mark
            string input2 = test.input;
            if (test.input.size() >= 3
                && (unsigned char)test.input[0] == 0xef
                && (unsigned char)test.input[1] == 0xbb
                && (unsigned char)test.input[2] == 0xbf) {
                input2 = string(input2, 3);
            }

            std::vector<std::string> expected2;
            for (auto & line: test.expected) {
                if (test.expectedFail)
                    expected2.emplace_back(line);
                else
                    expected2.emplace_back(jsonDecodeStr<Json::Value>(line).toStringNoNewLine());
            }

            //cerr << "program " << test.program << endl;
            //cerr << "fail " << test.expectedFail << endl;
            //cerr << "input " << test.program << endl;

            auto program = createJqStreamProcessor(test.program);
            cerr << format("-%45s -> ", test.program) << program->toLisp() << endl;
#if 0            
            auto json = jsonDecodeStr<Json::Value>(input2);

            //std::vector<Json::Value> array = { json };
            ArrayJsonStreamParsingContext inputContext({json});
            ArrayJsonStreamPrintingContext outputContext;
            program->process(inputContext, outputContext);

            std::vector<std::string> result;
            for (auto & val: outputContext.values()) {
                result.emplace_back(val.toStringNoNewLine());
            }

            CHECK(result == expected2);
#endif
        }
    }
}

#if 0
TEST_CASE("jq-compat-jq", "[none]")
{
    runTestFile("jq.test");
}
#endif

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