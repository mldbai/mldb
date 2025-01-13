/* for_each_line_test.cc
   Wolfgang Sourdeau, 28 August 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test for_each_line
*/

#include "mldb/utils/testing/mldb_catch2.h"
#include "mldb/utils/block_splitter.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/utils/coalesced_range.h"

using namespace std;
using namespace MLDB;

TEST_CASE("split newlines")
{
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

                auto [found_split, skip_to_end, skip_separator, newState] = splitter.nextRecord(block, block.begin(), isLastBlock, currentState);
                if (!found_split) continue; // No newline, we need a new block
                currentState = std::move(newState);
                auto newline_pos = block.begin() + skip_to_end;
                CHECK(newline_pos[skip_separator-1] == '\n');
                std::string line = block.to_string(block.begin(), newline_pos);
                block.reduce(newline_pos + skip_separator, block.end());

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
