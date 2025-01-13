/* iostream_adaptors_test.cc
   Jeremy Barnes, 17 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
   
   This file is part of "Jeremy's Machine Learning Library", copyright (c)
   1999-2015 Jeremy Barnes.
   
   Apache 2.0 license.

   ---
   
   IOStream adaptors for the implementation of filter streams.
   Interface is based on boost::iostream.
*/

#include "catch2/catch_all.hpp"
#include "mldb/base/iostream_adaptors.h"
#include <filesystem>
#include <fstream>

using namespace std;
using namespace MLDB;

TEST_CASE("proper EOF sequencing")
{
    string input_file = "mldb/utils/testing/parse_context_test_data.csv";
    size_t input_length = std::filesystem::file_size(input_file);

    for (auto chunk_size: {  1, 2, 3, 5, 7, 11, 50, 71, 1000, 1000000 }) {
        SECTION("chunk_size " + std::to_string(chunk_size)) {
            for (auto buffer_size: { /*1, 2, 3, 5, 7, 11, 16,*/ 128, 1000 }) {
                SECTION("buffer_size " + std::to_string(buffer_size)) {
                    ifstream file(input_file);
                    filtering_istream stream;
                    stream.push(null_filter(), buffer_size);
                    stream.push(*file.rdbuf(), buffer_size);         

                    std::vector<char> buffer(chunk_size);
                    std::string contents;

                    ssize_t total_read = 0;
                    bool has_read_zero = false;

                    while (!stream.eof()) {
                        stream.read(buffer.data(), buffer.size());
                        contents.append(buffer.data(), stream.gcount());
                        total_read += stream.gcount();
                        if (stream.gcount() == 0) {
                            CHECK(!has_read_zero);
                            has_read_zero = true;
                            CHECK(total_read == input_length);
                            REQUIRE(stream.eof());
                        }

                        // Reading zero characters shouldn't mess up the stream
                        stream.read(buffer.data(), 0);
                        CHECK(stream.gcount() == 0);
                    }

                    if (!has_read_zero) {
                        stream.read(buffer.data(), buffer.size());
                        CHECK(stream.gcount() == 0);
                        has_read_zero = true;
                    }

                    CHECK(contents.size() == input_length);
                    CHECK(has_read_zero);
                }
            }
        }
    }
}

TEST_CASE("mapped stream tellg")
{
    string input_file = "mldb/vfs/testing/fixtures/minimal.csv";

    for (auto buffer_size: { /*1, 2, 3, 5, 7, 11, 16, 128,*/ 1000 }) {
        SECTION("buffer_size " + std::to_string(buffer_size)) {
            filtering_istream stream;
            stream.push(mapped_file_source({input_file}), buffer_size);

            CHECK(stream.tellg() == 0);
            std::string line;
            getline(stream, line);
            CHECK(line == "a,b,c");
            CHECK(stream.tellg() == 6);
            getline(stream, line);
            CHECK(line == "d,e,f");
            CHECK(stream.tellg() == 12);
            getline(stream, line);
            CHECK(line == "");
            CHECK(stream.eof());
        }
    }
}
