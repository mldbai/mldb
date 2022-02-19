/** path_test.cc
    Jeremy Barnes, 10 April 2016
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Test of coordinate classes.
*/

#include "catch2/catch_all.hpp"
#include "mldb/plugins/tabular/bit_compressed_int_table.h"
#include "int_table_test_utils.h"

using namespace std;
using namespace MLDB;

static_assert(std::is_default_constructible_v<MappedBitCompressedIntTable>);
static_assert(std::is_default_constructible_v<InternalMappedBitCompressedIntTable<1>>);
static_assert(std::is_default_constructible_v<InternalMappedBitCompressedIntTable<2>>);
static_assert(std::is_default_constructible_v<InternalMappedBitCompressedIntTable<3>>);

#define ALL_BIT_COMPRESSED_TABLES MappedBitCompressedIntTable, \
                                  InternalMappedBitCompressedIntTable<1>, \
                                  InternalMappedBitCompressedIntTable<2>, \
                                  InternalMappedBitCompressedIntTable<3> 

TEMPLATE_TEST_CASE("bit compressed basics", "[bit compressed]", ALL_BIT_COMPRESSED_TABLES)
{
    doIntTableBasicsTest<TestType>();
}

#if 1

TEMPLATE_TEST_CASE("spanning multiple words", "[bit compressed]", ALL_BIT_COMPRESSED_TABLES)
{
    std::vector<uint32_t> input(7 * 640);
    for (size_t i = 0;  i < input.size();  ++i) {
        input[i] = i % (1 << 7);
    }

    auto [context, table] = freeze_table<TestType>(input);

    double bits_per_element = 8.0 * context.getOffset() / input.size();

    REQUIRE(bits_per_element < 7.1);  // to select between lines
}

TEMPLATE_TEST_CASE("count values uniform", "[bit compressed]", ALL_BIT_COMPRESSED_TABLES)
{
    std::vector<uint32_t> input(1000, 10);
    auto [context, table] = freeze_table<TestType>(input);

    CHECK(table->countValues(0, 1000, 10) == 1000);
    CHECK(table->countValues(0, 1, 10) == 1);
    CHECK(table->countValues(0, 10, 10) == 10);
    CHECK(table->countValues(0, 0, 10) == 0);
    CHECK(table->countValues(1000, 1000, 10) == 0);
    CHECK(table->countValues(1000, 1000, 1) == 0);
    CHECK(table->countValues(0, 1000, 0) == 0);
    CHECK(table->countValues(0, 1000, 1000) == 0);
    CHECK_THROWS(table->countValues(1, 0, 0));
    CHECK_THROWS(table->countValues(-1, 0, 0));
    CHECK_THROWS(table->countValues(0, 1001, 0));
    CHECK_THROWS(table->countValues(1001, 1001, 0));
    CHECK_THROWS(table->countValues(1001, 1001, 1000));
}

TEMPLATE_TEST_CASE("count values nonuniform", "[bit compressed]", ALL_BIT_COMPRESSED_TABLES)
{
    std::vector<uint32_t> input(1000);
    for (size_t i = 0;  i < input.size();  ++i) {
        input[i] = i % 10;
    }
    auto [context, table] = freeze_table<TestType>(input);

    CHECK(table->countValues(0, 1000, 9) == 100);
    CHECK(table->countValues(0, 1000, 2) == 100);
    CHECK(table->countValues(0, 1000, 0) == 100);
    CHECK(table->countValues(0, 1000, 10) == 0);
    CHECK(table->countValues(0, 9, 9) == 0);
    CHECK(table->countValues(0, 10, 9) == 1);
    CHECK(table->countValues(0, 100, 9) == 10);
    CHECK(table->countValues(990, 999, 9) == 0);

    CHECK(table->countValues(0, 1, 0) == 1);
    CHECK(table->countValues(0, 10, 0) == 1);
    CHECK(table->countValues(0, 0, 10) == 0);
    CHECK(table->countValues(1000, 1000, 10) == 0);
    CHECK(table->countValues(1000, 1000, 1) == 0);
    CHECK(table->countValues(0, 1000, 0) == 100);
    CHECK(table->countValues(0, 1000, 1000) == 0);
    CHECK_THROWS(table->countValues(1, 0, 0));
    CHECK_THROWS(table->countValues(-1, 0, 0));
    CHECK_THROWS(table->countValues(0, 1001, 0));
    CHECK_THROWS(table->countValues(1001, 1001, 0));
    CHECK_THROWS(table->countValues(1001, 1001, 1000));
}

TEMPLATE_TEST_CASE("count values range", "[bit compressed]", ALL_BIT_COMPRESSED_TABLES)
{
    std::vector<uint32_t> input(1000);
    for (size_t i = 0;  i < input.size();  ++i) {
        input[i] = i / 100;
    }
    auto [context, table] = freeze_table<TestType>(input);

    CHECK(table->countValues(0, 1000, 9) == 100);
    CHECK(table->countValues(0, 1000, 2) == 100);
    CHECK(table->countValues(0, 1000, 0) == 100);
    CHECK(table->countValues(0, 1000, 10) == 0);
    CHECK(table->countValues(0, 9, 0) == 9);
    CHECK(table->countValues(1, 9, 0) == 8);
    CHECK(table->countValues(1, 9, 10) == 0);
    CHECK(table->countValues(900, 1000, 9) == 100);
    CHECK(table->countValues(0, 1000, 9) == 100);
    CHECK(table->countValues(850, 1000, 9) == 100);
    CHECK(table->countValues(850, 1000, 8) == 50);
    CHECK(table->countValues(800, 1000, 8) == 100);
    CHECK(table->countValues(801, 1000, 8) == 99);
    CHECK(table->countValues(799, 1000, 8) == 100);
    CHECK(table->countValues(799, 800, 8) == 0);
    CHECK(table->countValues(799, 801, 8) == 1);
    CHECK(table->countValues(0, 100, 9) == 0);
    CHECK(table->countValues(990, 999, 9) == 9);

    CHECK(table->countValues(0, 1, 0) == 1);
    CHECK(table->countValues(0, 10, 0) == 10);
    CHECK(table->countValues(0, 0, 10) == 0);
    CHECK(table->countValues(1000, 1000, 10) == 0);
    CHECK(table->countValues(1000, 1000, 1) == 0);
    CHECK(table->countValues(0, 1000, 0) == 100);
    CHECK(table->countValues(0, 1000, 1000) == 0);

    CHECK_THROWS(table->countValues(1, 0, 0));
    CHECK_THROWS(table->countValues(-1, 0, 0));
    CHECK_THROWS(table->countValues(0, 1001, 0));
    CHECK_THROWS(table->countValues(1001, 1001, 0));
    CHECK_THROWS(table->countValues(1001, 1001, 1000));
    CHECK_THROWS(table->at(1000));
    CHECK(table->at(999) == 9);
}
#endif

