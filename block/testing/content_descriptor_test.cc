/* content_descriptor_test.cc
   Jeremy Barnes, 29 June 2011
   Copyright (c) 2011 mldb.ai inc.
   Copyright (c) 2011 Jeremy Barnes.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "mldb/vfs/filter_streams.h"
#include "mldb/block/content_descriptor.h"
#include "mldb/types/value_description.h"
#include "mldb/types/json.h"
#include "mldb/arch/atomic_min_max.h"
#include "mldb/utils/testing/mldb_catch2.h"

using namespace std;
using namespace MLDB;

using boost::unit_test::test_suite;

void testSequential(const std::string & filename,
                     size_t blockSize,
                     size_t compressorBlockSize)
{
    cerr << "test_sequential " << filename << " " << blockSize << " "
         << compressorBlockSize << endl;

    filter_istream stream(filename);
    string data = stream.readAll();

    ContentDescriptor content;
    content.content.push_back({"url", filename});

    //cerr << jsonEncode(content) << endl;
    
    auto handler = getDecompressedContent(content, compressorBlockSize);

    size_t ofs = 0;
    for (; ofs < data.size();  ofs += blockSize) {

        uint64_t startOffset;
        FrozenMemoryRegion region;
        std::tie(startOffset, region)
            = handler->getRangeContaining(ofs, blockSize);

        uint64_t endOffset = startOffset + region.length();

        //cerr << "ofs " << ofs << " startOffset " << startOffset
        //     << " endOffset " << endOffset << endl;
        
        if (ofs + blockSize < data.size()) {
            // Verify it's in range
            BOOST_REQUIRE_LE(startOffset, ofs);
            BOOST_REQUIRE_GE(endOffset, ofs + blockSize);

            // Verify the data is correct
            std::string data1(region.data(), region.data() + region.length());
            std::string data2(data, startOffset, endOffset - startOffset);

            BOOST_REQUIRE_EQUAL(data1, data2);
        }
        else {
            BOOST_REQUIRE_EQUAL(endOffset, data.size());
        }
    }

    // Verify that an off-the-end block returns the right size
    {
        uint64_t startOffset;
        FrozenMemoryRegion region;
        std::tie(startOffset, region)
            = handler->getRangeContaining(data.size(), blockSize);
        BOOST_CHECK_EQUAL(startOffset, data.size());
        BOOST_CHECK(!region);
    }
        
    // Now move to random, make sure it still works
    BOOST_CHECK_EQUAL(handler->getPattern(), ADV_SEQUENTIAL);

    for (size_t i = 0;  i < 100;  ++i) {
        size_t ofs = random() % (data.length() - blockSize);
        
        uint64_t startOffset;
        FrozenMemoryRegion region;
        std::tie(startOffset, region)
            = handler->getRangeContaining(ofs, blockSize);

        uint64_t endOffset = startOffset + region.length();

        //cerr << "ofs " << ofs << " startOffset " << startOffset
        //     << " endOffset " << endOffset << endl;
        
        if (ofs + blockSize < data.size()) {
            // Verify it's in range
            BOOST_REQUIRE_LE(startOffset, ofs);
            BOOST_REQUIRE_GE(endOffset, ofs + blockSize);

            // Verify the data is correct
            std::string data1(region.data(), region.data() + region.length());
            std::string data2(data, startOffset, endOffset - startOffset);

            BOOST_REQUIRE_EQUAL(data1, data2);
        }
        else {
            BOOST_REQUIRE_EQUAL(endOffset, data.size());
        }
    }

    // Verify that an off-the-end block returns the right size still
    {
        uint64_t startOffset;
        FrozenMemoryRegion region;
        std::tie(startOffset, region)
            = handler->getRangeContaining(data.size(), blockSize);
        BOOST_CHECK_EQUAL(startOffset, data.size());
        BOOST_CHECK(!region);
    }
    
    
    BOOST_CHECK_EQUAL(handler->getPattern(), ADV_RANDOM);
}


BOOST_AUTO_TEST_CASE( test_compressed_sequential_access )
{
    testSequential("file://mldb/testing/fixtures/letters.dat.gz",
                   65536, 65536);
    testSequential("file://mldb/testing/fixtures/letters.dat.gz",
                   1024, 1024);
    testSequential("file://mldb/testing/fixtures/letters.dat.gz",
                   1024, 4096);
    testSequential("file://mldb/testing/fixtures/letters.dat.gz",
                   4096, 256);
    testSequential("file://mldb/testing/fixtures/letters.dat.gz",
                   1, 1);
    testSequential("file://mldb/testing/fixtures/letters.dat.gz",
                   1, 4096);
    testSequential("file://mldb/testing/fixtures/letters.dat.gz",
                   4096, 1);
}
    

BOOST_AUTO_TEST_CASE( test_compressed_random_access )
{
    
}


struct TestParallelDecompressData {
    size_t numBlocks = 0, totalLength = 0, maxNumParallel = 0, minBlock = 10000000, maxBlock = 0;
    bool hasLastBlock = false;
};

TestParallelDecompressData testParallelDecompress(string input_file, int maxParallelism = 16)
{
    ContentDescriptor descriptor = jsonDecode<ContentDescriptor>("file://" + input_file);
    std::shared_ptr<ContentHandler> handler = getDecompressedContent(descriptor);

    std::atomic<size_t> numBlocks{0}, totalLength{0}, numParallel{0}, maxNumParallel{0}, minBlock(10000000), maxBlock(0);
    bool hasLastBlock = false;

    auto onBlock = [&] (size_t blockNum, uint64_t blockOffset,
                        FrozenMemoryRegion block, bool lastBlock)
    {
        if (lastBlock) {
            BOOST_CHECK(!hasLastBlock);
            hasLastBlock = true;
        }
        auto par = numParallel.fetch_add(1) + 1;
        atomic_max(maxNumParallel, par);
        atomic_min(minBlock, blockNum);
        atomic_max(maxBlock, blockNum);

        numBlocks.fetch_add(1);
        totalLength.fetch_add(block.length());

        std::this_thread::sleep_for(std::chrono::milliseconds(1));

        //cerr << "got block " << blockNum << " from " << blockOffset << " of length " << block.length() << endl;

        numParallel.fetch_sub(1);
        return true;
    };

    ComputeContext compute(maxParallelism);

    handler->forEachBlockParallel(0, 1024 * 1024 /* requested block size */, compute, 1 /* priority */, onBlock);
    BOOST_CHECK_EQUAL(numParallel, 0);

    cerr << "maxNumParallel = " << maxNumParallel << endl;
    return { numBlocks, totalLength, maxNumParallel, minBlock, maxBlock, hasLastBlock };
}


BOOST_AUTO_TEST_CASE( test_parallel_decompress_zstd )
{
    string input_file = "mldb_test_data/reviews_Digital_Music_5.json.zstd";

    for (auto maxParallelism: {0, 1, 4, 16}) {
        SECTION("maxParallelism = " + to_string(maxParallelism)) {

            auto [numBlocks, totalLength, maxNumParallel, minBlock, maxBlock, hasLastBlock]
                = testParallelDecompress(input_file, 16 /* maxParallelism */);

            BOOST_CHECK_EQUAL(hasLastBlock, true);
            BOOST_CHECK_EQUAL(numBlocks, 680);
            BOOST_CHECK_EQUAL(totalLength, 88964528);
            if (maxParallelism > 0)
                BOOST_CHECK_GT(maxNumParallel, 1);
            BOOST_CHECK_EQUAL(minBlock, 0);
            BOOST_CHECK_EQUAL(maxBlock, 679);
        }
    }
}

BOOST_AUTO_TEST_CASE( test_parallel_decompress_lz4 )
{
    string input_file = "mldb_test_data/train-1m.csv.lz4";
    for (auto maxParallelism: {0, 1, 4, 16}) {
        SECTION("maxParallelism = " + to_string(maxParallelism)) {

            auto [numBlocks, totalLength, maxNumParallel, minBlock, maxBlock, hasLastBlock]
                = testParallelDecompress(input_file, maxParallelism);

            BOOST_CHECK_EQUAL(hasLastBlock, true);
            BOOST_CHECK_EQUAL(numBlocks, 13);
            BOOST_CHECK_EQUAL(totalLength, 48882727);
            if (maxParallelism > 0)
                BOOST_CHECK_GT(maxNumParallel, 1);
            BOOST_CHECK_EQUAL(minBlock, 0);
            BOOST_CHECK_EQUAL(maxBlock, 12);
        }
    }
}

BOOST_AUTO_TEST_CASE( test_parallel_decompress_gzip )
{
    string input_file = "mldb_test_data/enron.csv.gz";
    for (auto maxParallelism: {0, 1, 4, 16}) {
        SECTION("maxParallelism = " + to_string(maxParallelism)) {
            auto [numBlocks, totalLength, maxNumParallel, minBlock, maxBlock, hasLastBlock]
                = testParallelDecompress(input_file, 16 /* maxParallelism */);

            BOOST_CHECK_EQUAL(hasLastBlock, true);
            BOOST_CHECK_EQUAL(numBlocks, 408);
            BOOST_CHECK_EQUAL(totalLength, 52229289);
            if (maxParallelism > 0)
                BOOST_CHECK_GT(maxNumParallel, 1);
            BOOST_CHECK_EQUAL(minBlock, 0);
            BOOST_CHECK_EQUAL(maxBlock, 407);
        }
    }
}