/* for_each_line_test.cc
   Wolfgang Sourdeau, 28 August 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

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
#include "mldb/utils/string_functions.h"
#include "mldb/utils/vector_utils.h"
#include "mldb/utils/for_each_line.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/block/content_descriptor.h"

using namespace std;
using namespace MLDB;


namespace {

void testForEachLineBlock(const std::string & data,
                          size_t blockSize,
                          size_t startOffset,
                          int64_t maxLines = -1)
{
    std::vector<std::string> splitLines = split(string(data, startOffset), '\n');
    for (auto & l: splitLines) {
        if (!l.empty() && l.back() == '\r') {
            l.pop_back();
        }
    }
    
    if (!data.empty() && data.back() == '\n' && !splitLines.empty())
        splitLines.pop_back();

    if (maxLines >= 0 && maxLines < splitLines.size()) {
        splitLines.resize(maxLines);
    }
    
    static int testNumber = 0;
    
    string url = "mem://testdata" + std::to_string(++testNumber);

    cerr << endl;
    cerr << "test " << testNumber << " with "
         << splitLines.size() << " lines, " << startOffset << " start offset and "
         << maxLines << " max lines" << endl;
    cerr << "test has " << data.size() << " characters" << endl;
    
    {
        filter_ostream stream(url);
        stream << data;
        stream.close();
    }
    
    ContentDescriptor descriptor;
    descriptor.content = { { "url", url } };
    
    auto content = getContent(descriptor);

    std::atomic<int> numBlocksStarted(0);
    std::atomic<int> numBlocksFinished(0);
    std::atomic<int> numLines(0);
    std::atomic<int> numLinesInBlockTotal(0);
    
    auto onStartBlock = [&] (int64_t blockNumber, int64_t lineNumber,
                             size_t numLinesInBlock) -> bool
        {
            ++numBlocksStarted;
            numLinesInBlockTotal += numLinesInBlock;
            return true;
        };

    auto onEndBlock = [&] (int64_t blockNumber, int64_t lineNumber) -> bool
        {
            ++numBlocksFinished;
            return true;
        };
    
    auto onLine = [&] (const char * line, size_t length,
                       int64_t blockNumber, int64_t lineNumber) -> bool
        {
            BOOST_CHECK_EQUAL(lineNumber, numLines);
            BOOST_CHECK_LT(lineNumber, splitLines.size());
            if (lineNumber < splitLines.size()) {
                if (splitLines[lineNumber] != string(line, length))
                    cerr << "error on line " << lineNumber << endl;
                BOOST_CHECK_EQUAL(splitLines[lineNumber], string(line, length));
            }
            ++numLines;
            return true;
        };

    int maxParallelism = 1;

    forEachLineBlock(content, startOffset, onLine, maxLines, maxParallelism,
                     onStartBlock, onEndBlock, blockSize);

    BOOST_CHECK_EQUAL(numLines, splitLines.size());
    if (maxLines == -1)
        BOOST_CHECK_EQUAL(numLinesInBlockTotal, numLines);
    BOOST_CHECK_EQUAL(numBlocksStarted, numBlocksFinished);

    cerr << "did " << numBlocksStarted << " blocks" << endl;
}

BOOST_AUTO_TEST_CASE(test_forEachLineBlock)
{
    /* 1 */ testForEachLineBlock("", 1 /* blockSize */, 0 /* startOffset */);
    /* 2 */ testForEachLineBlock("\n", 1 /* blockSize */, 0 /* startOffset */);
    /* 3 */ testForEachLineBlock("\n", 1 /* blockSize */, 1 /* startOffset */);
    /* 4 */ testForEachLineBlock("          ", 1 /* blockSize */, 0 /* startOffset */);
    /* 5 */ testForEachLineBlock("\n\n\n\n\n\n\n\n\n\n", 1 /* blockSize */, 0 /* startOffset */);
    /* 6 */ testForEachLineBlock("\n\n\n\n\n\n\n\n\n\n", 100 /* blockSize */, 0 /* startOffset */);
    /* 7 */ testForEachLineBlock("\n\n\n\n\n\n\n\n\n\n", 100 /* blockSize */, 10 /* startOffset */);
    /* 8 */ testForEachLineBlock("\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n", 1 /* blockSize */, 10 /* startOffset */);
    /* 9 */ testForEachLineBlock("\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n", 2 /* blockSize */, 0 /* startOffset */);
    /* 10 */ testForEachLineBlock("\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n", 2 /* blockSize */, 1 /* startOffset */);
    /* 11 */ testForEachLineBlock("a\nab\nabc\nabcd\nabcde\nabcd\nabc\nab\na\n\n", 100 /* blockSize */, 0 /* startOffset */);
    /* 12 */ testForEachLineBlock("a\nab\nabc\nabcd\nabcde\nabcd\nabc\nab\na\n\n", 1 /* blockSize */, 0 /* startOffset */);
    /* 13 */ testForEachLineBlock("a\nab\nabc\nabcd\nabcde\nabcd\nabc\nab\na\n\n", 2 /* blockSize */, 0 /* startOffset */);
    /* 14 */ testForEachLineBlock("a\nab\nabc\nabcd\nabcde\nabcd\nabc\nab\na\n\n", 5 /* blockSize */, 0 /* startOffset */);

    /* 15 */ testForEachLineBlock("", 1 /* blockSize */, 0 /* startOffset */, 0 /* max lines */);
    /* 16 */ testForEachLineBlock("\n", 1 /* blockSize */, 0 /* startOffset */, 0 /* max lines */);
    /* 17 */ testForEachLineBlock("\n", 1 /* blockSize */, 0 /* startOffset */, 1 /* max lines */);
    /* 18 */ testForEachLineBlock("\n", 1 /* blockSize */, 0 /* startOffset */, 2 /* max lines */);
    /* 19 */ testForEachLineBlock("a\nb\nc\n", 1 /* blockSize */, 0 /* startOffset */, 0 /* max lines */);
    /* 20 */ testForEachLineBlock("a\nb\nc\n", 1 /* blockSize */, 0 /* startOffset */, 1 /* max lines */);
    /* 21 */ testForEachLineBlock("a\nb\nc\n", 1 /* blockSize */, 0 /* startOffset */, 2 /* max lines */);
    /* 22 */ testForEachLineBlock("a\nb\nc\n", 1 /* blockSize */, 0 /* startOffset */, 3 /* max lines */);
    /* 23 */ testForEachLineBlock("a\nb\nc\n", 1 /* blockSize */, 0 /* startOffset */, 4 /* max lines */);
    
    /* 24 */ testForEachLineBlock("a\r\nab\r\nabc\r\nabcd\r\nabcde\r\nabcd\r\nabc\r\nab\r\na\r\n\r\n", 5 /* blockSize */, 0 /* startOffset */);
}

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

    auto logger = getMldbLog("test");
    forEachLine(stream, processLine, logger);
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

    auto logger = getMldbLog("test");
    forEachLineStr(stream, processLine, logger);
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
            throw MLDB::Exception("thrown");
        }
    };

    auto logger = getMldbLog("test");
    BOOST_CHECK_THROW(forEachLineStr(stream, processLine, logger), MLDB::Exception);
}

