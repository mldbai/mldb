/** int_table_real_world_test.cc
    Jeremy Barnes, 10 April 2016
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/

#include "catch2/catch_all.hpp"
#include <fstream>
#include "mldb/plugins/tabular/mapped_int_table.h"
#include "int_table_test_utils.h"
#include "mldb/utils/memusage.h"
#include "mldb/vfs/filter_streams.h"

using namespace std;
using namespace MLDB;

std::vector<uint32_t> loadFile(const std::string & filename, size_t expectedLength)
{
    std::vector<uint32_t> result;

    filter_istream stream(filename);
    if (stream.fail()) {
        stream = filter_istream("../../" + filename);
    }

    while (stream) {
        uint32_t i;
        stream >> i;
        result.push_back(i);
    }

    ExcAssert(result.size() == expectedLength);

    return result;
}

static constexpr size_t maxRealWorldLength = 4096;

#if 1
TEST_CASE("real world 1") {
    auto input = loadFile("plugins/tabular/testing/int-table-crooked-interleaved.txt.gz", 188952);
    if (input.size() > maxRealWorldLength)
        input.resize(maxRealWorldLength);

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input, DUMP_MEMORY_MAP=true, DUMP_TYPE_STATS=true, INT_TABLE_TRACE_LEVEL=5);
    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 5);
}
#endif

#if 0
TEST_CASE("real world 2") {
    auto input = loadFile("plugins/tabular/testing/int-table-2.txt.gz", 188952);
    if (input.size() > maxRealWorldLength)
        input.resize(maxRealWorldLength);

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input);
    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 1);
}

TEST_CASE("real world 3") {
    auto input = loadFile("plugins/tabular/testing/int-table-3.txt.gz", 188953);
    if (input.size() > maxRealWorldLength)
        input.resize(maxRealWorldLength);

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input);
    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 1);
}

TEST_CASE("real world 4") {
    auto input = loadFile("plugins/tabular/testing/int-table-4.txt.gz", 64522);
    if (input.size() > maxRealWorldLength)
        input.resize(maxRealWorldLength);

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input);
    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 1);
}
#endif
