/** mapped_selector_table_test.cc
    Jeremy Barnes, 10 April 2016
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/

#include "catch2/catch_all.hpp"
#include "mldb/plugins/tabular/mapped_selector_table.h"

using namespace std;
using namespace MLDB;

template<typename InputContainer, typename... Options>
std::pair<MappingContext, MappedSelectorTable *>
freeze_table(const InputContainer & input, Options&&... options)
{
    MappingContext context(1000 << 10, std::forward<Options>(options)...);
    auto & output = context.alloc<MappedSelectorTable>();
    freeze(context, output, input);

    REQUIRE(input.size() == output.size());

    for (size_t i = 0;  i < input.size();  ++i) {
        CHECK(input[i] == output.at(i).first);
    }

    cerr << "froze " << input.size() << " elements into " << context.getOffset() << " bytes at "
         << 8.0 * context.getOffset() / input.size() << " bits/element" << endl;

    // Check that the bytes required are correctly calculated
    size_t maxInputValue = 0;
    size_t maxInputCount [[maybe_unused]] = 0;  // for a subsequent test...
    if (!input.empty()) {
        maxInputValue = *std::max_element(input.begin(), input.end());
        std::vector<uint32_t> valueCounts(maxInputValue + 1);
        for (auto i: input) {
            valueCounts.at(i) += 1;
        }
        maxInputCount = *std::max_element(valueCounts.begin(), valueCounts.end());
    }

    // TODO: it's the RLE that causes the problem, making it smaller than expected sometimes...
    int64_t bytesUsed = context.getOffset() - sizeof(MappedSelectorTable);
    CHECK(abs((int64_t)mapped_selector_table_bytes_required(input) - bytesUsed) < 36 + std::max<int64_t>(32, bytesUsed / 10));

    return std::make_pair(context, &output);
}

#if 1
TEST_CASE("selector table test basics")
{

    SECTION("empty table") {
        std::vector<uint8_t> values;

        auto [context, table] = freeze_table(values);

        REQUIRE(table->size() == 0);
        CHECK_THROWS(table->at(0));
        CHECK_THROWS(table->at(-1));
    }

    SECTION("one entry") {
        std::vector<uint8_t> values(1);

        auto [context, table] = freeze_table(values);

        REQUIRE(table->size() == 1);
        CHECK(table->at(0).first == 0);
        CHECK(table->at(0).second == 0);
        CHECK_THROWS(table->at(-1));
        CHECK_THROWS(table->at(1));
    }

    SECTION("all zeros") {
        std::vector<uint8_t> values(513);

        auto [context, table] = freeze_table(values);

        REQUIRE(table->size() == values.size());
        for (size_t i = 0;  i < values.size();  ++i) {
            CHECK(table->at(i).first == 0);
            CHECK(table->at(i).second == i);
        }
        CHECK_THROWS(table->at(-1));
    }

    SECTION("all ones") {
        std::vector<uint8_t> values(513, 1);

        auto [context, table] = freeze_table(values);

        REQUIRE(table->size() == values.size());
        for (size_t i = 0;  i < values.size();  ++i) {
            CHECK(table->at(i).first == 1);
            CHECK(table->at(i).second == i);
        }
        CHECK_THROWS(table->at(-1));
    }
}
#endif

TEST_CASE("selector table test large values")
{
    SECTION("large value") {
        std::vector<uint8_t> values(1025, 255);

        auto [context, table] = freeze_table(values);

        REQUIRE(table->size() == values.size());
        for (size_t i = 0;  i < values.size();  ++i) {
            CHECK(table->at(i).first == 255);
            CHECK(table->at(i).second == i);
        }
        CHECK_THROWS(table->at(-1));
    }
}

#if 1
void test_interleaved(size_t n, size_t l = 1024)
{
    std::vector<uint8_t> values(l);
    for (size_t i = 0;  i < values.size();  ++i) {
        values[i] = i % n;
    }

    auto [context, table] = freeze_table(values);

    REQUIRE(table->size() == values.size());
    for (size_t i = 0;  i < values.size();  ++i) {
        CHECK(table->at(i).first == i % n);
        CHECK(table->at(i).second == i / n);
    }
}

void test_random(size_t n, size_t l = 1024)
{
    std::vector<uint8_t> values(l);
    for (size_t i = 0;  i < values.size();  ++i) {
        values[i] = std::rand() % n;
    }

    auto [context, table] = freeze_table(values, TRACE=true, DEBUG=true);

    REQUIRE(table->size() == values.size());

    std::vector<uint32_t> accum(n);
    for (size_t i = 0;  i < values.size();  ++i) {
        auto [val, index] = table->at(i);
        REQUIRE(val == values[i]);
        CHECK(index == accum.at(val));
        accum[val] += 1;
    }
}

// Hook for testing in mapped_selector_table.cpp
namespace MLDB {
extern bool allowSkipSelectors;
} // namespace MLDB

#if 0

TEST_CASE("selector table test real world-like no skipping")
{
    allowSkipSelectors = false;

    SECTION("interleaved 2") {
        test_interleaved(2);
    }

    SECTION("interleaved 3") {
        test_interleaved(3);
    }

    SECTION("interleaved 16") {
        test_interleaved(16);
    }

    SECTION("interleaved 256") {
        test_interleaved(256);
    }

    SECTION("random 2") {
        test_random(2);
    }

    SECTION("random 3") {
        test_random(3);
    }

    SECTION("random 16") {
        test_random(16);
    }

    SECTION("random 256") {
        test_random(256);
    }

    allowSkipSelectors = true;
}
#endif

TEST_CASE("selector table test real world-like with skipping")
{
    allowSkipSelectors = true;

    SECTION("interleaved 2") {
        test_interleaved(2);
    }

    SECTION("interleaved 3") {
        test_interleaved(3);
    }

    SECTION("interleaved 16") {
        test_interleaved(16);
    }

    SECTION("interleaved 256") {
        test_interleaved(256);
    }

    SECTION("random 2") {
        test_random(2);
    }

    SECTION("random 3") {
        test_random(3);
    }

    SECTION("random 16") {
        test_random(16);
    }

    SECTION("random 256") {
        test_random(256);
    }
}

TEST_CASE("selector table storage efficiency")
{
    allowSkipSelectors = true;

    SECTION("uniform value") {
        std::vector<uint8_t> values(16384, 0);
        auto [context, table] = freeze_table(values);
        double bitsPerEntry = 8.0 * context.getOffset() / values.size();
        CHECK(bitsPerEntry < 0.1);
    }

    SECTION("uniform two values") {
        std::vector<uint8_t> values(16384, 1);
        auto [context, table] = freeze_table(values);
        double bitsPerEntry = 8.0 * context.getOffset() / values.size();
        CHECK(bitsPerEntry < 0.1);
    }

    SECTION("two values rare ones") {
        std::vector<uint8_t> values(16384, 0);
        for (size_t i = 0;  i < values.size();  i += 1024) {
            values[i] = 1;
        }
        auto [context, table] = freeze_table(values);
        double bitsPerEntry = 8.0 * context.getOffset() / values.size();
        CHECK(bitsPerEntry < 0.1);
    }

    SECTION("two values rare zeros") {
        std::vector<uint8_t> values(16384, 1);
        for (size_t i = 0;  i < values.size();  i += 1024) {
            values[i] = 0;
        }
        auto [context, table] = freeze_table(values);
        double bitsPerEntry = 8.0 * context.getOffset() / values.size();
        CHECK(bitsPerEntry < 0.1);
    }

    SECTION("two values interleaved") {
        std::vector<uint8_t> values(16384, 1);
        for (size_t i = 0;  i < values.size();  i += 1) {
            values[i] = i % 2;
        }
        auto [context, table] = freeze_table(values);
        double bitsPerEntry = 8.0 * context.getOffset() / values.size();
        CHECK(bitsPerEntry < 1.2);
    }

    SECTION("two values random") {
        std::vector<uint8_t> values(16384, 1);
        for (size_t i = 0;  i < values.size();  i += 1) {
            values[i] = rand() % 2;
        }
        auto [context, table] = freeze_table(values);
        double bitsPerEntry = 8.0 * context.getOffset() / values.size();
        CHECK(bitsPerEntry < 1.2);
    }
}
#endif