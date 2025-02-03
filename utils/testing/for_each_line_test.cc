/* for_each_line_test.cc
   Wolfgang Sourdeau, 28 August 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test for_each_line
*/

#include <atomic>
#include <sstream>
#include <string>
#include <vector>
#include <functional>
#include "mldb/arch/exception.h"
#include "mldb/utils/string_functions.h"
#include "mldb/utils/vector_utils.h"
#include "mldb/utils/for_each_line.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/block/content_descriptor.h"
#include "mldb/utils/coalesced_range.h"
#include "mldb/utils/testing/mldb_catch2.h"

using namespace std;
using namespace std::placeholders;
using namespace MLDB;


namespace {

void testForEachLineBlock(const std::string & data,
                          size_t blockSize,
                          size_t startOffset,
                          int64_t maxLines = -1,
                          int maxParallelism = 1)
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

    //cerr << endl;
    //cerr << "test " << testNumber << " with "
    //     << splitLines.size() << " lines, " << startOffset << " start offset and "
    //     << maxLines << " max lines" << endl;
    //cerr << "test has " << data.size() << " characters" << endl;
    
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

    auto onEndBlock = [&] (int64_t blockNumber, int64_t lineNumber, size_t numLinesInBlock) -> bool
        {
            ++numBlocksFinished;
            return true;
        };

    std::mutex onLineMutex;

    auto onLine = [&] (const char * line, size_t length,
                       int64_t blockNumber, int64_t lineNumber) -> bool
        {
            std::unique_lock lock{onLineMutex};
            if (maxParallelism == 0) {
                CHECK(lineNumber == numLines);
            }
            CHECK(lineNumber < splitLines.size());
            if (lineNumber < splitLines.size()) {
                if (splitLines[lineNumber] != string(line, length))
                    cerr << "error on line " << lineNumber << endl;
                CHECK(splitLines[lineNumber] == string(line, length));
            }
            ++numLines;
            return true;
        };

    forEachLineBlock(content, startOffset, onLine, maxLines, maxParallelism,
                     onStartBlock, onEndBlock, blockSize);

    CHECK(numLines == splitLines.size());
    if (maxLines == -1)
        CHECK(numLinesInBlockTotal == numLines);
    CHECK(numBlocksStarted == numBlocksFinished);

    //cerr << "did " << numBlocksStarted << " blocks" << endl;
}

TEST_CASE("test_forEachLineBlock")
{
    for (auto parallelism: { 0 , 1, -1 }) {
        SECTION("parallelism " + std::to_string(parallelism)) {
#if 1
            SECTION("test 1") { testForEachLineBlock("", 1 /* blockSize */, 0 /* startOffset */, -1, parallelism); }
            SECTION("test 2") { testForEachLineBlock("\n", 1 /* blockSize */, 0 /* startOffset */, -1, parallelism); }
            SECTION("test 3") { testForEachLineBlock("\n", 1 /* blockSize */, 1 /* startOffset */, -1, parallelism); }
#endif
            SECTION("test 4") { testForEachLineBlock("          ", 1 /* blockSize */, 0 /* startOffset */, -1, parallelism); }
#if 1
            SECTION("test 5") { testForEachLineBlock("\n\n\n\n\n\n\n\n\n\n", 1 /* blockSize */, 0 /* startOffset */, -1, parallelism); }
            SECTION("test 6") { testForEachLineBlock("\n\n\n\n\n\n\n\n\n\n", 100 /* blockSize */, 0 /* startOffset */, -1, parallelism); }
            SECTION("test 7") { testForEachLineBlock("\n\n\n\n\n\n\n\n\n\n", 100 /* blockSize */, 10 /* startOffset */, -1, parallelism); }
            SECTION("test 8") { testForEachLineBlock("\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n", 1 /* blockSize */, 10 /* startOffset */, -1, parallelism); }
            SECTION("test 9") { testForEachLineBlock("\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n", 2 /* blockSize */, 0 /* startOffset */, -1, parallelism); }
            SECTION("test 10") { testForEachLineBlock("\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n", 2 /* blockSize */, 1 /* startOffset */, -1, parallelism); }
            SECTION("test 11") { testForEachLineBlock("a\nab\nabc\nabcd\nabcde\nabcd\nabc\nab\na\n\n", 100 /* blockSize */, 0 /* startOffset */, -1, parallelism); }
            SECTION("test 12") { testForEachLineBlock("a\nab\nabc\nabcd\nabcde\nabcd\nabc\nab\na\n\n", 1 /* blockSize */, 0 /* startOffset */, -1, parallelism); }
            SECTION("test 13") { testForEachLineBlock("a\nab\nabc\nabcd\nabcde\nabcd\nabc\nab\na\n\n", 2 /* blockSize */, 0 /* startOffset */, -1, parallelism); }
            SECTION("test 14") { testForEachLineBlock("a\nab\nabc\nabcd\nabcde\nabcd\nabc\nab\na\n\n", 5 /* blockSize */, 0 /* startOffset */, -1, parallelism); }
            SECTION("test 15") { testForEachLineBlock("", 1 /* blockSize */, 0 /* startOffset */, 0 /* max lines */, parallelism); }
            SECTION("test 16") { testForEachLineBlock("\n", 1 /* blockSize */, 0 /* startOffset */, 0 /* max lines */, parallelism); }
            SECTION("test 17") { testForEachLineBlock("\n", 1 /* blockSize */, 0 /* startOffset */, 1 /* max lines */, parallelism); }
            SECTION("test 18") { testForEachLineBlock("\n", 1 /* blockSize */, 0 /* startOffset */, 2 /* max lines */, parallelism); }
            SECTION("test 19") { testForEachLineBlock("a\nb\nc\n", 1 /* blockSize */, 0 /* startOffset */, 0 /* max lines */, parallelism); }
            SECTION("test 20") { testForEachLineBlock("a\nb\nc\n", 1 /* blockSize */, 0 /* startOffset */, 1 /* max lines */, parallelism); }
            SECTION("test 21") { testForEachLineBlock("a\nb\nc\n", 1 /* blockSize */, 0 /* startOffset */, 2 /* max lines */, parallelism); }
            SECTION("test 22") { testForEachLineBlock("a\nb\nc\n", 1 /* blockSize */, 0 /* startOffset */, 3 /* max lines */, parallelism); }
            SECTION("test 23") { testForEachLineBlock("a\nb\nc\n", 1 /* blockSize */, 0 /* startOffset */, 4 /* max lines */, parallelism); }
            SECTION("test 24") { testForEachLineBlock("a\r\nab\r\nabc\r\nabcd\r\nabcde\r\nabcd\r\nabc\r\nab\r\na\r\n\r\n", 5 /* blockSize */, 0 /* startOffset */, parallelism); }
            SECTION("test 24") { testForEachLineBlock("a\r\nab\r\nabc\r\nabcd\r\nabcde\r\nabcd\r\nabc\r\nab\r\na\r\n\r\n", 5 /* blockSize */, 0 /* startOffset */, -1, parallelism); }
#endif
        }
    }
}

vector<string> dataStrings{"line1", "line2", "", "line forty 2"};

}

TEST_CASE(" test_forEachLine_data ")
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
    CHECK(result == expected);
}

TEST_CASE(" test_forEachLineStr_data ")
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
    forEachLine(stream, processLine, logger);
    CHECK(result == expected);
}

TEST_CASE(" test_forEachLine_throw ")
{
    string data;
    for (int i = 0; i < 1000; i++) {
        data += to_string(i) + "\n";
    }
    istringstream stream(data);

    atomic<int> count(0);
    auto processLine = [&] (const string & data, int64_t lineNum) {
        if (count.fetch_add(1) > 500) {
            MLDB_TRACE_EXCEPTIONS(false);
            throw MLDB::Exception("thrown");
        }
    };

    auto logger = getMldbLog("test");
    CHECK_THROWS(forEachLine(stream, processLine, logger));
}

TEST_CASE("split newlines")
{
    return;
    const auto & splitter = newLineSplitter;

    for (auto filename: { "file://mldb/testing/MLDB-1043-bucketize-data.csv"}) {
        filter_istream stream(filename);

        std::vector<std::string> lines;
        std::string withNewLines;
        std::string withDosNewLines;

        while (stream) {
            std::string line;
            std::getline(stream, line);
            lines.push_back(line);
            withNewLines += line + "\n";
            withDosNewLines += line + "\r\n";
        }

        // Make sure the block padding is respected
        withNewLines.reserve(withNewLines.size() + splitter.requiredBlockPadding());
        withDosNewLines.reserve(withDosNewLines.size() + splitter.requiredBlockPadding());

        auto testSplitter = [&] (size_t blockSize, const char * data, const char * end)
        {
            int lineNum = 0;
            auto checkLine = [&] (const std::string & line)
            {
                CHECK(line == lines[lineNum]);
                ++lineNum;
            };

            const char * p = data;

            auto currentState = splitter.newState();
            TextBlock block;

            for (; p < end; p += blockSize) {
                // Add a new block
                block.add(p, std::min(p + blockSize, end));
                bool isLastBlock = p + blockSize == end;

                auto splitter_res = splitter.nextRecord(block, block.begin(), isLastBlock, currentState);
                if (!splitter_res) continue; // No newline, we need a new block
                auto [newline_pos, newState] = splitter_res.value();
                currentState = std::move(newState);
                CHECK(*newline_pos == '\n');
                std::string line = block.to_string(block.begin(), newline_pos);
                block.reduce(newline_pos + 1, block.end());

                checkLine(line);
            }
        };

        for (auto blockSize: { 1, 5, 8, 11, 17, 231, 1024 }) {
            SECTION("blockSize " + std::to_string(blockSize)) {
                SECTION("Unix line endings") {
                    testSplitter(blockSize, withNewLines.c_str(), withNewLines.c_str() + withNewLines.size());
                }
                SECTION("DOS line endings") {
                    testSplitter(blockSize, withDosNewLines.c_str(), withDosNewLines.c_str() + withDosNewLines.size());
                }
            }
        }
    }
}

struct ForEachLineTester {
    ForEachLineTester()
        : splitter(new NewlineSplitter())
    {
    }

    bool debug = false;
    bool blocked = true;
    ssize_t maxLines = -1;

    std::unique_ptr<BlockSplitter> splitter;

    atomic<size_t> lineCount = 0;

    struct LineInfo {
        std::string contents;
        bool found = false;
    };

    struct BlockInfo {
        int64_t blockNumber = -1;
        int64_t firstLineNumber = -1;
        uint64_t numLines = 0;
        std::vector<LineInfo> lines;
        bool ended = false;

        LineInfo & getLineInfo(int64_t lineNum, bool blocked)
        {
            if (!blocked) {
                static std::mutex linesMutex; // for unblocked only
                REQUIRE(firstLineNumber == 0);
                std::lock_guard<std::mutex> guard(linesMutex);
                if (lines.size() <= lineNum) {
                    lines.resize(lineNum + 1);
                }
                return lines[lineNum];
            }
            else {
                CHECK(lineNum >= firstLineNumber);
                CHECK(lineNum < firstLineNumber + numLines);
                REQUIRE(lineNum - firstLineNumber < lines.size());
            }
            return lines[lineNum - firstLineNumber];
        }
    };

    // Call the first time we see a block
    BlockInfo & addBlockInfo(int64_t blockNumber)
    {
        //CHECK(blockNumber < 50);
        std::lock_guard<std::mutex> guard(blockInfoMutex);
        if (blocks.size() <= blockNumber)
            blocks.resize(blockNumber + 1);
        CHECK(blocks[blockNumber].blockNumber == -1);
        return blocks[blockNumber];
    };

    // Call after we've seen a block
    BlockInfo & getBlockInfo(int64_t blockNumber)
    {
        std::lock_guard<std::mutex> guard(blockInfoMutex);

        if (!blocked) {
            CHECK(blockNumber == 0);
            if (blocks.empty()) {
                blocks.resize(1);
                blocks[0].firstLineNumber = 0;
            }
            return blocks[0];
        }

        REQUIRE(blockNumber < blocks.size());
        CHECK(blocks[blockNumber].ended == false);
        CHECK(blocks[blockNumber].blockNumber == blockNumber);
        return blocks[blockNumber];
    };

    std::mutex blockInfoMutex;
    std::vector<BlockInfo> blocks;

    void processUsingBlockedStream(const std::string & filename, ssize_t blockSize = -1, ssize_t maxLines = -1, int maxParallelism = 0)
    {
        filter_istream stream(filename);
        REQUIRE(stream);

        forEachLineBlock(stream,
                         std::bind(&ForEachLineTester::processLine, this, _1, _2, _3, _4),
                         maxLines, maxParallelism,
                         std::bind(&ForEachLineTester::startBlock, this, _1, _2, _3),
                         std::bind(&ForEachLineTester::endBlock, this, _1, _2, _3),
                         blockSize,
                         *splitter);

        this->maxLines = maxLines;
    }

    void processUsingBlockedMappedStream(const std::string & filename, ssize_t blockSize = -1, ssize_t maxLines = -1, int maxParallelism = 0)
    {
        filter_istream stream(filename, { { "mapped", "true" } } );
        REQUIRE(stream);

        forEachLineBlock(stream,
                         std::bind(&ForEachLineTester::processLine, this, _1, _2, _3, _4),
                         maxLines, maxParallelism,
                         std::bind(&ForEachLineTester::startBlock, this, _1, _2, _3),
                         std::bind(&ForEachLineTester::endBlock, this, _1, _2, _3),
                         blockSize,
                         *splitter);

        this->maxLines = maxLines;
    }

    void processUsingLineByLine(const std::string & filename, ssize_t maxLines = -1, int maxParallelism = 0)
    {
        this->blocked = false;
        filter_istream stream(filename);
        REQUIRE(stream);

        forEachLine(stream,
                    std::bind(&ForEachLineTester::processLine, this, _1, _2, 0, _3),
                    getMldbLog("test"),
                    maxParallelism,
                    false /* ignore exceptions */,
                    maxLines);

        this->maxLines = maxLines;
    }

    void processUsingContentDescriptor(const std::string & filename, ssize_t blockSize = -1, ssize_t maxLines = -1, int maxParallelism = 0)
    {
        ContentDescriptor descriptor;
        descriptor.addUrl(filename);
        auto content = getContent(descriptor);

        forEachLineBlock(content,
                         0 /* startOffset */,
                         std::bind(&ForEachLineTester::processLine, this, _1, _2, _3, _4),
                         maxLines, maxParallelism,
                         std::bind(&ForEachLineTester::startBlock, this, _1, _2, _3),
                         std::bind(&ForEachLineTester::endBlock, this, _1, _2, _3),
                         blockSize,
                         *splitter);

        this->maxLines = maxLines;
    }

    bool processLine(const char * line, size_t length, int64_t blockNumber, int64_t lineNum)
    {
        if (debug)
            cerr << "got line " << lineNum << " with length " << length << " at " << lineNum << " of block " << blockNumber << endl;
        BlockInfo & blockInfo = getBlockInfo(blockNumber);
        LineInfo & lineInfo = blockInfo.getLineInfo(lineNum, blocked);
        lineInfo.contents = std::string(line, line + length);
        lineInfo.found = true;
        ++lineCount;
        if (maxLines != -1)
            CHECK(lineNum < maxLines);
        return true;
    }

    bool startBlock(int64_t blockNumber, int64_t lineNumber, int64_t numLines)
    {
        if (debug)
            cerr << "startBlock " << blockNumber << " with " << numLines << " lines from " << lineNumber << endl;

        BlockInfo & blockInfo = addBlockInfo(blockNumber);
        blockInfo.blockNumber = blockNumber;
        blockInfo.firstLineNumber = lineNumber;
        blockInfo.numLines = numLines;
        blockInfo.lines.resize(numLines);

        //REQUIRE(blockNumber < 5);
        //return blockNumber < 5;

        return true;
    }

    bool endBlock(int64_t blockNumber, int64_t lineNumber, int64_t numLines)
    {
        if (debug)
            cerr << "endBlock " << blockNumber << " at " << lineNumber << " with " << numLines << " lines" << endl;
        BlockInfo & blockInfo = getBlockInfo(blockNumber);
        blockInfo.ended = true;
        return true;
    }

    void validate()
    {
        int b = 0;
        for (auto & block: blocks) {
            if (blocked) REQUIRE(block.ended);
            int n = 0;
            for (auto & line: block.lines) {
                if (!line.found)
                    cerr << "line not found: line " << n << " of block " << b << " of " << blocks.size()
                         << " (" << b + block.firstLineNumber << " of " << lineCount.load() << " in file)" << endl;
                REQUIRE(line.found);
                ++n;
            }
            ++b;
        }

        if (maxLines != -1)
            CHECK(lineCount == maxLines);
    }
};


TEST_CASE("for_each_line newline splitter")
{
    for (auto parallelism: { 0, 1, -1 }) {
        SECTION("parallelism " + std::to_string(parallelism)) {
            //cerr << "parallelism " << parallelism << endl;
            for (auto blockSize: { -1, 1, 2, 3, 5, 17, 1000000 }) {
                SECTION("blockSize " + std::to_string(blockSize)) {
                    //cerr << "blockSize " << blockSize << endl;

                    SECTION("blocked stream") {
                        ForEachLineTester tester;
                        tester.processUsingBlockedStream("file://mldb/testing/MLDB-1043-bucketize-data.csv", blockSize);
                        tester.validate();
                    }

                    // Mapping requires the file to be non-compressed
                    SECTION("blocked mapped stream") {
                        ForEachLineTester tester;
                        tester.processUsingBlockedMappedStream("file://mldb/testing/MLDB-1043-bucketize-data.csv", blockSize);
                        tester.validate();
                    }

                    SECTION("blocked gzip stream") {
                        ForEachLineTester tester;
                        tester.processUsingBlockedMappedStream("file://mldb/mldb_test_data/reviews_Digital_Music_5.json.zstd", blockSize, blockSize > 5 ? 64000: 640 /* maxLines */);
                        tester.validate();
                    }

                    SECTION("line by line") {
                        ForEachLineTester tester;
                        tester.processUsingLineByLine("file://mldb/mldb_test_data/reviews_Digital_Music_5.json.zstd", 1000 /* maxLines */);
                        tester.validate();
                    };

                    SECTION("blocked content descriptor") {
                        ForEachLineTester tester;
                        tester.processUsingContentDescriptor("file://mldb/testing/MLDB-1043-bucketize-data.csv", blockSize);
                        tester.validate();
                    }

                    if (blockSize > 5) {
                        SECTION("blocked content descriptor large stream") {
                            ForEachLineTester tester;
                            tester.processUsingContentDescriptor("file://mldb/mldb_test_data/train-1m.csv.lz4", blockSize, 100000 /* maxLines */);
                            tester.validate();
                        }
                    }
                }
            }
        }
    }

#if 0
    SECTION("blocked filter stream") {
        ForEachLineTester tester;
        tester.processUsingBlockedStream("file://mldb/testing/MLDB-1043-bucketize-data.csv");
        tester.validate();
    }
#endif
}
