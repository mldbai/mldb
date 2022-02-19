#include "catch2/catch_all.hpp"
#include <fstream>
#include "mldb/plugins/tabular/mapped_int_table.h"
#include "mldb/utils/memusage.h"
#include "int_table_test_utils.h"

using namespace std;
using namespace MLDB;

#if 1
TEMPLATE_TEST_CASE("int table basics", "[MappedIntTable][Empty]",
                   RawMappedIntTable, MappedBitCompressedIntTable, MappedRunLengthIntTable,
                   MappedIntTable<uint32_t>, MappedFactoredIntTable)
{
    doIntTableBasicsTest<TestType>();    
}

TEST_CASE("crooked curve") {
    // curve that has a certain gradient for part, and another for another part

    size_t n = 5000;
    std::vector<uint32_t> input(3 * n);
    std::iota(input.begin() + 0 * n, input.begin() + 1 * n, 0);
    std::fill(input.begin() + 1 * n, input.begin() + 3 * n, n);

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input);

    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 0.2);  // trivially representable as two segments
}

TEST_CASE("guard_value") {
    size_t n = 5000;
    std::vector<uint32_t> input(2 * n);
    for (size_t i = 0;  i < n;  ++i) {
        input[2 * i] = i;
    }

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input);

    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 1.2);  // one bit to select between guard value or element
}

TEST_CASE("large_guard_values") {
    size_t n = 5000;
    std::vector<uint32_t> input(3 * n);
    for (size_t i = 0;  i < n;  ++i) {
        input[3 * i + 0] = i;
        input[3 * i + 1] = 0;
        input[3 * i + 2] = std::numeric_limits<uint32_t>::max();
    }

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input);

    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 2.1);  // two bits for 3 interleaved curves
}

TEST_CASE("large_guard_values_full_range") {
    size_t n = 5000;
    std::vector<uint32_t> input(4 * n);
    for (size_t i = 0;  i < n;  ++i) {
        input[4 * i + 0] = i;
        input[4 * i + 1] = 0;
        input[4 * i + 2] = std::numeric_limits<uint32_t>::max();
        input[4 * i + 3] = std::numeric_limits<int32_t>::max();
    }

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input);

    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 2.2);  // two bits for 4 interleaved curves
}

TEST_CASE("large_guard_values_fixed") {
    size_t n = 5000;
    std::vector<uint32_t> input(3 * n);
    for (size_t i = 0;  i < n;  ++i) {
        input[3 * i + 0] = i;
        input[3 * i + 1] = 0;
        input[3 * i + 2] = std::numeric_limits<uint32_t>::max();
    }

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input);

    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 2);  // two bits for 3 interleaved curves
}

TEST_CASE("large_guard_value_range") {
    size_t n = 5000;
    std::vector<uint32_t> input(4 * n);
    for (size_t i = 0;  i < n;  ++i) {
        input[4 * i + 0] = i;
        input[4 * i + 1] = 0;
        input[4 * i + 2] = std::numeric_limits<uint32_t>::max();
        input[4 * i + 3] = std::numeric_limits<int32_t>::max();
    }

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input);

    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 2.2);  // two bits to select between curves
}
#endif

TEST_CASE("outliers") {
    size_t n = 5000;
    std::vector<uint32_t> input(n);
    for (size_t i = 0;  i < n;  ++i) {
        input[i] = i;
        if (i % 1000 == 0)
            input[i] = (1 << 30) - i;
    }

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input, TRACE=true, DEBUG=true, DUMP_MEMORY_MAP=true);

    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 0.4);  // less than one bit to select something rare
}

TEST_CASE("easy outliers") {
    size_t n = 5000;
    std::vector<uint32_t> input(n);
    for (size_t i = 0;  i < n;  ++i) {
        input[i] = i;
        if (i % 1000 == 0)
            input[i] = (1 << 30);
    }

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input);

    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 0.4);  // less than one bit to select something rare
}

TEST_CASE("interleaved_lines 1") {
    size_t n = 5000;
    std::vector<uint32_t> input(2 * n);
    for (size_t i = 0;  i < n;  ++i) {
        input[2 * i] = i;
        input[2 * i + 1] = i * 10;
    }

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input);

    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 1.2);  // to select between lines
}

TEST_CASE("interleaved_lines 2") {
    size_t n = 5000;
    std::vector<uint32_t> input(2 * n);
    for (size_t i = 0;  i < n;  ++i) {
        input[2 * i] = i;
        input[2 * i + 1] = n - i;
    }

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input);

    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 1.2);  // to select between lines
}

TEST_CASE("interleaved_lines 3") {
    size_t n = 5000;
    std::vector<uint32_t> input(2 * n);
    for (size_t i = 0;  i < n;  ++i) {
        input[2 * i] = i;
        input[2 * i + 1] = i + (1 << 20);
    }

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input);

    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 1.2);
}

TEST_CASE("alternating 1") {
    size_t n = 5000;
    std::vector<uint32_t> input(2 * n);
    for (size_t i = 0;  i < n;  ++i) {
        input[2 * i] = 1000;
    }

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input);

    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 1.2);  // one bit to select between alternatives
}

TEST_CASE("lookup 8") {
    size_t n = 10000;
    std::vector<uint32_t> input(n);
    for (size_t i = 0;  i < n;  ++i) {
        input[i] = 100000 * (std::hash<std::string>()(std::to_string(i)) % 8);
    }

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input);

    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 3.3);  // selecting between 8 values needs 3 bits
}

TEST_CASE("lookup 5") {
    size_t n = 10000;
    std::vector<uint32_t> input(n);
    for (size_t i = 0;  i < n;  ++i) {
        input[i] = 100000 * (std::hash<std::string>()(std::to_string(i)) % 5);
    }

    auto [context, table] = freeze_table<MappedIntTable<uint32_t>>(input);

    double bits_per_element = 8.0 * context.getOffset() / input.size();

    CHECK(bits_per_element < 2.7);  // selecting between 5 values needs well under 3 bits
}

