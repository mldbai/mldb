/* csv_line_splitter_test.cc
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
#include "mldb/utils/for_each_line.h"
#include "mldb/block/content_descriptor.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/plugins/textual/csv_splitter.h"
#include "mldb/types/value_description.h"

using namespace std;
using namespace MLDB;

CSVSplitter csvSplitter('"', false /* multiLines */, CsvLineEncoding::UTF8, false /* invalid chars */);

struct ComparativeBlockSplitter: public BlockSplitterT<CSVSplitterState> {
    using State = CSVSplitterState;
    mutable size_t callNumber = 0;

    ComparativeBlockSplitter()
    {
    }

    virtual ~ComparativeBlockSplitter()
    {
    }

    virtual State newStateT() const
    {
        return csvSplitter.newStateT();
    }

    virtual bool isStateless() const { return false; };

    virtual size_t requiredBlockPadding() const { return 0; }

    virtual std::pair<const char *, State>
    nextBlockT(const char * block1, size_t n1, const char * block2, size_t n2,
              bool noMoreData, const State & state) const
    {
        auto out1 = newLineSplitter.nextBlockT(block1, n1, block2, n2, noMoreData);
        auto [out2, newState] = csvSplitter.nextBlockT(block1, n1, block2, n2, noMoreData, state);

        ++callNumber;
        if (out1 != out2) {
            cerr << "callNumber = " << callNumber << endl;
        }
        BOOST_REQUIRE_EQUAL((const void *)out1, (const void *)out2);

        return { out1, newState };
    }

    virtual std::span<const char> fixupBlock(std::span<const char> block) const
    {
        return csvSplitter.fixupBlock(block);
    }
};


BOOST_AUTO_TEST_CASE( test_csv_line_splitter_lz4 )
{
    std::string filename = "file://mldb_test_data/train-1m.csv.lz4";
    ContentDescriptor desc = jsonDecode<ContentDescriptor>(filename);
    auto handler = getDecompressedContent(desc);

    uint64_t startOffset = 0;
    auto onLine = [&] (const char * line, size_t lineLength,
                       int64_t blockNumber, int64_t lineNumber) -> bool
    {
        return true;
    };

    int64_t maxLines = -1;
    int maxParallelism = 8;

    auto startBlock = [&] (int64_t blockNumber, int64_t lineNumber, uint64_t numLines) -> bool
    {
        return true;
    };

    auto endBlock = [&] (int64_t blockNumber, int64_t lineNumber) -> bool
    {
        return true;
    };
    size_t blockSize = 20'000'000;
    ComparativeBlockSplitter splitter;

    forEachLineBlock(handler, startOffset, onLine, maxLines, maxParallelism, startBlock, endBlock, blockSize, splitter);

    auto startBlock2 = [&] (int64_t blockNumber, int64_t lineNumber) -> bool
    {
        return true;
    };

    filter_istream stream(filename);
    forEachLineBlock(stream, onLine, maxLines, maxParallelism, startBlock2, endBlock, splitter);
}

BOOST_AUTO_TEST_CASE( test_csv_line_splitter_zstd )
{
    std::string filename = "file://mldb_test_data/train-1m.csv.zst";
    ContentDescriptor desc = jsonDecode<ContentDescriptor>(filename);
    auto handler = getDecompressedContent(desc);

    uint64_t startOffset = 0;
    auto onLine = [&] (const char * line, size_t lineLength,
                       int64_t blockNumber, int64_t lineNumber) -> bool
    {
        return true;
    };

    int64_t maxLines = -1;
    int maxParallelism = 8;

    auto startBlock = [&] (int64_t blockNumber, int64_t lineNumber, uint64_t numLines) -> bool
    {
        return true;
    };

    auto endBlock = [&] (int64_t blockNumber, int64_t lineNumber) -> bool
    {
        return true;
    };
    size_t blockSize = 20'000'000;
    ComparativeBlockSplitter splitter;

    forEachLineBlock(handler, startOffset, onLine, maxLines, maxParallelism, startBlock, endBlock, blockSize, splitter);

    auto startBlock2 = [&] (int64_t blockNumber, int64_t lineNumber) -> bool
    {
        return true;
    };

    filter_istream stream(filename);
    forEachLineBlock(stream, onLine, maxLines, maxParallelism, startBlock2, endBlock, splitter);
}

BOOST_AUTO_TEST_CASE( test_csv_line_splitter_mmap )
{
    string filename = "file://tmp/train-1m.csv";
    {
        std::string inFilename = "file://mldb_test_data/train-1m.csv.zst";
        filter_istream in(inFilename);
        filter_ostream out(filename);
        out << in.rdbuf();
    }

    ContentDescriptor desc = jsonDecode<ContentDescriptor>(filename);
    auto handler = getDecompressedContent(desc);

    uint64_t startOffset = 0;
    auto onLine = [&] (const char * line, size_t lineLength,
                       int64_t blockNumber, int64_t lineNumber) -> bool
    {
        return true;
    };

    int64_t maxLines = -1;
    int maxParallelism = 8;

    auto startBlock = [&] (int64_t blockNumber, int64_t lineNumber, uint64_t numLines) -> bool
    {
        return true;
    };

    auto endBlock = [&] (int64_t blockNumber, int64_t lineNumber) -> bool
    {
        return true;
    };
    size_t blockSize = 20'000'000;
    ComparativeBlockSplitter splitter;

    forEachLineBlock(handler, startOffset, onLine, maxLines, maxParallelism, startBlock, endBlock, blockSize, splitter);

    auto startBlock2 = [&] (int64_t blockNumber, int64_t lineNumber) -> bool
    {
        return true;
    };

    filter_istream stream(filename);
    forEachLineBlock(stream, onLine, maxLines, maxParallelism, startBlock2, endBlock, splitter);
}
