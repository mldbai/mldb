/* filter_streams_test.cc
   Jeremy Barnes, 29 June 2011
   Copyright (c) 2011 mldb.ai inc.
   Copyright (c) 2011 Jeremy Barnes.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/vfs/filter_streams.h"
#include "mldb/block/content_descriptor.h"
#include "mldb/types/value_description.h"
#include "mldb/types/json.h"

#include <boost/test/unit_test.hpp>

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
