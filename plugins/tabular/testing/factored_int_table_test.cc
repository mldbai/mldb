#include "catch2/catch_all.hpp"
#include "mldb/vfs/filter_streams.h"
#include "mldb/utils/memusage.h"
#include "mldb/plugins/tabular/factored_int_table.h"
#include "mldb/plugins/tabular/factor_packing.h"
#include "int_table_test_utils.h"

using namespace std;
using namespace MLDB;

static_assert(std::is_default_constructible_v<MappedFactoredIntTable>);

#define ALL_FACTORED_TABLES MappedFactoredIntTable, \
                            InternalMappedFactoredIntTable<1>, \
                            InternalMappedFactoredIntTable<2>

TEMPLATE_TEST_CASE("factored basics", "[factored]", ALL_FACTORED_TABLES)
{
    doIntTableBasicsTest<TestType>();
}

#if 0
TEST_CASE("is_it_worth_it")
{
    auto bitsPerEntryFactor = [] (size_t n) -> double
    {
        if (n == 0) return INFINITY;
        if (n == 1) return 64.0;
        // how many can we fit in 64 bits?
        double max = std::pow(2.0, 64.0);
        double current = 1.0;
        for (int i = 0;  /* run forever */;  ++i) {
            current *= n;
            if (current > max)
                return i;
        }
    };

    auto bitsPerEntryBitPacked = [] (size_t n) -> double
    {
        if (n == 0) return INFINITY;
        if (n == 1) return 64;
        return 64.0 / bits(n - 1);
    };

    double lastBits1 = -INFINITY;
    double lastBits2 = -INFINITY;
    double lastRatio = INFINITY;
    size_t i = 0;

    size_t lastPoint = 0;

    auto pointOfInterest = [&] (size_t point, double ratio)
    {
        if (lastRatio < 1 && point > 0) {
            // factor mapping
            cerr << lastPoint << "-" << (point-1) << " factor " << (point-1) << " ratio " << lastRatio;
            std::vector<uint64_t> factors;
            double max = std::pow(2.0, 64.0);
            double current = 1.0;
            uint64_t product = 1;
            int i = 0;
            for (;;++i) {
                current *= (point-1);
                product *= (point-1);
                if (current > max)
                    break;
                factors.push_back(current);
            }
            cerr << " " << factors.size() << " factors " << factors << endl;
        }
        else if (point > 0) {
            // bit compressed mapping
            cerr << lastPoint << "-" << (point-1) << " bit cmp " << bits(point-2) << " ratio 1" << endl;
        }
        lastPoint = point;
        lastRatio = ratio;
    };

    for (i = 0;  i < 1ULL << 24;  ++i) {
        double bits1 = bitsPerEntryFactor(i);
        double bits2 = bitsPerEntryBitPacked(i);

        if (bits1 == lastBits1 && bits2 == lastBits2)
            continue;

        lastRatio = lastBits2 / lastBits1;
        double ratio = bits2 / bits1;

        if (lastRatio != ratio || bits2 != lastBits2)
            pointOfInterest(i, ratio);

        if (lastRatio != ratio && (ratio < 1.0 || (lastRatio < 1.0 && ratio >= 1.0))) {
            //pointOfInterest(i, ratio);
            //cerr << "maximum " << i-1 << " bits1 " << bits1 << " bits2 " << bits2 << " ratio "
            //     << 100.0 * ratio << endl;
        }

        lastBits1 = bits1;
        lastBits2 = bits2;
    }

    pointOfInterest(1ULL << 32, 1.0);

    //if (lastBits1 > lastBits2 || true) {
    //    cerr << "maximum " << i << " bits1 " << lastBits1 << " bits2 " << lastBits2 << " ratio "
    //           << 100.0 * lastBits2 / lastBits1 << endl;
    //}
}
#endif

TEST_CASE("factor extract 32")
{
    CHECK(extract_factor32(10, 0, 0) == 0);
    CHECK(extract_factor32(10, 0, 1234567) == 7);
    CHECK(extract_factor32(10, 1, 1234567) == 6);
    CHECK(extract_factor32(10, 2, 1234567) == 5);
    CHECK(extract_factor32(10, 3, 1234567) == 4);
    CHECK(extract_factor32(10, 4, 1234567) == 3);
    CHECK(extract_factor32(10, 5, 1234567) == 2);
    CHECK(extract_factor32(10, 6, 1234567) == 1);
    CHECK(extract_factor32(10, 7, 1234567) == 0);
}

TEST_CASE("factor extract 64")
{
    CHECK(extract_factor64(10, 0, 0) == 0);
    CHECK(extract_factor64(10, 0, 1234567) == 7);
    CHECK(extract_factor64(10, 1, 1234567) == 6);
    CHECK(extract_factor64(10, 2, 1234567) == 5);
    CHECK(extract_factor64(10, 3, 1234567) == 4);
    CHECK(extract_factor64(10, 4, 1234567) == 3);
    CHECK(extract_factor64(10, 5, 1234567) == 2);
    CHECK(extract_factor64(10, 6, 1234567) == 1);
    CHECK(extract_factor64(10, 7, 1234567) == 0);
}

TEST_CASE("factor insert 32")
{
    CHECK(insert_factor32(10, 0, 0, 3) == 3);
    CHECK(insert_factor32(10, 0, 0, 5) == 5);
    CHECK(insert_factor32(10, 1, 0, 5) == 50);
    CHECK(insert_factor32(10, 3, 0, 5) == 5000);
    CHECK(insert_factor32(10, 3, 123, 5) == 5123);
    CHECK(insert_factor32(10, 3, 5123, 5) == 5123);
    CHECK(insert_factor32(10, 3, 5123, 4) == 4123);
    CHECK(insert_factor32(10, 3, 60123, 5) == 65123);
    CHECK(insert_factor32(10, 3, 65123, 5) == 65123);
    CHECK(insert_factor32(10, 3, 65123, 4) == 64123);
}

TEST_CASE("factor insert 64")
{
    CHECK(insert_factor64(10, 0, 0, 3) == 3);
    CHECK(insert_factor64(10, 0, 0, 5) == 5);
    CHECK(insert_factor64(10, 1, 0, 5) == 50);
    CHECK(insert_factor64(10, 3, 0, 5) == 5000);
    CHECK(insert_factor64(10, 3, 123, 5) == 5123);
    CHECK(insert_factor64(10, 3, 5123, 5) == 5123);
    CHECK(insert_factor64(10, 3, 5123, 4) == 4123);
    CHECK(insert_factor64(10, 3, 60123, 5) == 65123);
    CHECK(insert_factor64(10, 3, 65123, 5) == 65123);
    CHECK(insert_factor64(10, 3, 65123, 4) == 64123);
}

TEST_CASE("factored solution")
{
    CHECK(get_factor_packing_solution(0).factor == 1);
    CHECK(get_factor_packing_solution(1).factor == 2);
    CHECK(get_factor_packing_solution(2).factor == 3);
    CHECK(get_factor_packing_solution(4).factor == 5);
    CHECK(get_factor_packing_solution(9).factor == 10);
    CHECK(get_factor_packing_solution(14).factor == 16);
}

TEST_CASE("factored bytes required")
{
    CHECK(factored_table_indirect_bytes(0, 0) == 0);
    CHECK(factored_table_indirect_bytes(1, 0) == 0);
    CHECK(factored_table_indirect_bytes(1000000, 0) == 0);
    CHECK(factored_table_indirect_bytes(0, 1) == 0);
    CHECK(factored_table_indirect_bytes(0, 100000) == 0);

    CHECK(factored_table_indirect_bytes(1, 2) == 4);
    CHECK(factored_table_indirect_bytes(20, 2) == 4);
    CHECK(factored_table_indirect_bytes(21, 2) == 8);
    CHECK(factored_table_indirect_bytes(40, 2) == 8);
    CHECK(factored_table_indirect_bytes(41, 2) == 12);

    CHECK(factored_table_indirect_bytes(1024, 1) == 128);
    CHECK(factored_table_indirect_bytes(1024, 1024) == 1368);

    // factor 5 has an odd number (27) per 64 bits, so ensure we're in chunks of 64
    CHECK(factored_table_indirect_bytes(26, 4) == 8);
    CHECK(factored_table_indirect_bytes(27, 4, true) == 8);
    CHECK(factored_table_indirect_bytes(27, 4, false) == 12);
    CHECK(factored_table_indirect_bytes(28, 4) == 12);
}

TEST_CASE("pow64")
{
    CHECK(pow64(0, 0) == 0);
    CHECK(pow64(0, 1) == 0);
    CHECK(pow64(1, 0) == 1);
    CHECK(pow64(2, 0) == 1);
    CHECK(pow64(102312321312, 1) == 102312321312);
    CHECK(pow64(102312321312, 0) == 1);
    CHECK(pow64(std::numeric_limits<uint64_t>::max(), 0) == 1);
    CHECK(pow64(std::numeric_limits<uint64_t>::max(), 1) == std::numeric_limits<uint64_t>::max());
    for (int i = 0;  i < 63;  ++i) {
        CHECK(pow64(2, i) == 1ULL << i);
    }
    CHECK(pow64(3, 0) == 1);
    CHECK(pow64(3, 1) == 3);
    CHECK(pow64(3, 2) == 9);
    CHECK(pow64(3, 3) == 27);
    CHECK(pow64(3, 4) == 81);
}

TEST_CASE("pow32")
{
    CHECK(pow32(0, 0) == 0);
    CHECK(pow32(0, 1) == 0);
    CHECK(pow32(1, 0) == 1);
    CHECK(pow32(2, 0) == 1);
    CHECK(pow32(std::numeric_limits<uint32_t>::max(), 0) == 1);
    CHECK(pow32(std::numeric_limits<uint32_t>::max(), 1) == std::numeric_limits<uint32_t>::max());
    for (int i = 0;  i < 31;  ++i) {
        CHECK(pow32(2, i) == 1 << i);
    }
    CHECK(pow32(3, 0) == 1);
    CHECK(pow32(3, 1) == 3);
    CHECK(pow32(3, 2) == 9);
    CHECK(pow32(3, 3) == 27);
    CHECK(pow32(3, 4) == 81);
}

TEMPLATE_TEST_CASE("factored modulo 3 non packed", "[factored]", ALL_FACTORED_TABLES)
{
    uint32_t n = 3;
    uint32_t sz = 50;

    FactoredIntTable table(sz, n-1);
    for (size_t i = 0;  i < 50;  ++i) {
        table.set(i, i % 3);
        //cerr << "i = " << i << endl;
        for (size_t j = 0;  j <= i;  ++j) {
            if (table.at(j) != j % 3) {
                cerr << "i = " << i << " j = " << j << endl;
            }
            CHECK(table.at(j) == j % 3);
        }
        
    }
}

TEMPLATE_TEST_CASE("factored modulo 3", "[factored]", ALL_FACTORED_TABLES)
{
    size_t n = 5000;
    int m = 3;
    std::vector<uint32_t> input(n);
    for (size_t i = 0;  i < input.size();  ++i) {
        input[i] = i % m; 
    }

    auto [context, table] = freeze_table<TestType>(input);

    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 2);  // 2 bits/element is base 2; we can do better
}

TEST_CASE("prev factor")
{
    // Test that we can find the next smallest factor from a given factor, so we know
    // how much we need to reduce a range to make it take up less bits in storage.
    int64_t prevFactor = 1;
    for (uint64_t i = 1;  i < 1ULL << 31;  /* no inc */) {
        auto [type, factor, bits] = get_factor_packing_solution(i);
        CHECK(factor > i);
        auto prev = get_previous_factor(factor);
        //cerr << "i = " << i << " factor " << factor << " prev " << prev << " prevFactor " << prevFactor << endl;
        CHECK(prev == prevFactor);
        prevFactor = factor;
        i = factor;
    }
}

TEMPLATE_TEST_CASE("factored spanning multiple words", "[factored]", ALL_FACTORED_TABLES)
{
    std::vector<uint32_t> input(7 * 640);
    for (size_t i = 0;  i < input.size();  ++i) {
        input[i] = i % (1 << 7);
    }

    auto [context, table] = freeze_table<TestType>(input);
}

TEMPLATE_TEST_CASE("factored maxint", "[factored]", ALL_FACTORED_TABLES)
{
    std::vector<uint32_t> input(1000, std::numeric_limits<uint32_t>::max());
    auto [context, table] = freeze_table<TestType>(input);
}


TEMPLATE_TEST_CASE("factored count values uniform", "[factored]", ALL_FACTORED_TABLES)
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

TEMPLATE_TEST_CASE("factored count values nonuniform", "[factored]", ALL_FACTORED_TABLES)
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

TEMPLATE_TEST_CASE("factored count values range", "[factored]", ALL_FACTORED_TABLES)
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
