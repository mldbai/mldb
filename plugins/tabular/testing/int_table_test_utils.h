#pragma once
#include "catch2/catch_all.hpp"
#include <iostream>
#include <fstream>

namespace MLDB {

template<typename T> struct TypeTestIndex {};
template<size_t N> struct TestType {};

template<typename Int> struct MappedIntTable;
struct RawMappedIntTable;
struct MappedRunLengthIntTable;
struct MappedFactoredIntTable;
struct MappedBitCompressedIntTable;
template<size_t N> struct InternalMappedBitCompressedIntTable;
template<size_t N> struct InternalMappedFactoredIntTable;

#define DEFINE_TEST_INDEX(type, num) \
template<> struct TypeTestIndex<type> { static constexpr int val = num; }; \
template<> struct TestType<num> { using Type = type; }; 

DEFINE_TEST_INDEX(MappedIntTable<uint32_t>, 0);
DEFINE_TEST_INDEX(RawMappedIntTable, 1);
DEFINE_TEST_INDEX(MappedRunLengthIntTable, 2);
DEFINE_TEST_INDEX(MappedFactoredIntTable, 3);
DEFINE_TEST_INDEX(MappedBitCompressedIntTable, 4);
DEFINE_TEST_INDEX(InternalMappedBitCompressedIntTable<1>, 64);
DEFINE_TEST_INDEX(InternalMappedBitCompressedIntTable<2>, 65);
DEFINE_TEST_INDEX(InternalMappedBitCompressedIntTable<3>, 66);
DEFINE_TEST_INDEX(InternalMappedFactoredIntTable<1>, 96);
DEFINE_TEST_INDEX(InternalMappedFactoredIntTable<2>, 97);


static size_t intTableTestIndex = 0;
static bool dumpTestCorpus = false;  // set to true to create a seed corpus for fuzzing

template<typename OutputContainer, typename InputContainer, typename... Options>
std::pair<MappingContext, OutputContainer *> freeze_table(const InputContainer & input, Options&&... options)
{
    using namespace std;

    // This is to create an initial corpus for fuzzing, by dumping the tables we create for our
    // test cases.
    if (dumpTestCorpus) {
        constexpr uint32_t index = TypeTestIndex<OutputContainer>::val;
        auto testName = Catch::getResultCapture().getCurrentTestName();
        std::string filename = "../../mmap/fixtures/fuzz1/seed/" + testName + "-" + std::to_string(++intTableTestIndex) + ".bin";
        std::ofstream stream(filename);
        auto writeBytes = [&] (const void * p, size_t n)
        {
            stream.write((const char *)p, n);
        };
        auto writeVal = [&] (const auto & v)
        {
            writeBytes(&v, sizeof(v));
        };
        writeVal(index);
        int numWritten = 0;
        for (uint32_t val: input) {
            writeVal(val);
            if (++numWritten == 16384)
                break;
        }
    }

    MappingContext context(1000 << 10, std::forward<Options>(options)...);
    context.setOption(VALIDATE=true);
    auto & output = context.alloc_field<OutputContainer>("test");
    freeze_field(context, "test", output, input);

    for (size_t i = 0;  i < input.size();  ++i) {
        CHECK(input[i] == output.at(i));
    }

    if (OutputContainer::indirectBytesRequiredIsExact) {
        CHECK(OutputContainer::indirectBytesRequired(input) == context.getOffset() - sizeof(OutputContainer));
    }

    cerr << "froze " << input.size() << " elements into " << context.getOffset() << " bytes at "
         << 8.0 * context.getOffset() / input.size() << " bits/element" << endl;

    return std::make_pair(std::move(context), &output);
}

template<typename TestType>
void doIntTableBasicsTest()
{
    using ValueType = typename std::remove_const_t<typename TestType::value_type>;

    SECTION("empty vector") {
        std::vector<ValueType> empty;

        auto && [context, table] = freeze_table<TestType>(empty);
        
        CHECK(table->size() == 0);
        CHECK(table->empty());
        CHECK(table->begin() == table->end());
        CHECK(context.getOffset() == sizeof(*table));
        CHECK_THROWS(table->at(0));
        CHECK_THROWS(table->at(-1));
        CHECK_THROWS(*table->begin());
        CHECK_THROWS(*table->end());
    }

    SECTION("zero") {
        std::vector<ValueType> input = {0};

        CHECK(input.size() == 1);

        auto [context, table] = freeze_table<TestType>(input);

        CHECK(table->size() == input.size());
        CHECK(!table->empty());
        for (size_t i = 0;  i < input.size();  ++i) {
            CHECK(table->at(i) == input.at(i));
        }

        CHECK_THROWS(table->at(1));
        CHECK_THROWS(table->at(100000));
        CHECK(*table->begin() == 0);
        CHECK_THROWS(*table->end());
    }

    SECTION("minint") {
        std::vector<ValueType> input = {std::numeric_limits<ValueType>::min()};

        auto [context, table] = freeze_table<TestType>(input);

        CHECK(table->size() == input.size());
        CHECK(!table->empty());
        for (size_t i = 0;  i < input.size();  ++i) {
            CHECK(table->at(i) == input.at(i));
        }

        CHECK_THROWS(table->at(1));
        CHECK_THROWS(table->at(1000000));
    }

    SECTION("double minint") {
        std::vector<ValueType> input(2, std::numeric_limits<ValueType>::min());

        auto [context, table] = freeze_table<TestType>(input);

        CHECK(table->size() == input.size());
        CHECK(!table->empty());
        for (size_t i = 0;  i < input.size();  ++i) {
            CHECK(table->at(i) == input.at(i));
        }

        CHECK_THROWS(table->at(2));
        CHECK_THROWS(table->at(1000000));
    }

    SECTION("maxint") {
        std::vector<ValueType> input = {std::numeric_limits<ValueType>::max()};

        auto [context, table] = freeze_table<TestType>(input);

        CHECK(table->size() == input.size());
        CHECK(!table->empty());
        for (size_t i = 0;  i < input.size();  ++i) {
            CHECK(table->at(i) == input.at(i));
        }

        CHECK_THROWS(table->at(1));
        CHECK_THROWS(table->at(1000000));
    }

    SECTION("maxint-1") {
        std::vector<ValueType> input = {std::numeric_limits<ValueType>::max() - 1};

        auto [context, table] = freeze_table<TestType>(input);

        CHECK(table->size() == input.size());
        CHECK(!table->empty());
        for (size_t i = 0;  i < input.size();  ++i) {
            CHECK(table->at(i) == input.at(i));
        }

        CHECK_THROWS(table->at(1));
        CHECK_THROWS(table->at(1000000));
    }

    SECTION("double maxint") {
        std::vector<ValueType> input(2,std::numeric_limits<ValueType>::max());

        auto [context, table] = freeze_table<TestType>(input);

        CHECK(table->size() == input.size());
        CHECK(!table->empty());
        for (size_t i = 0;  i < input.size();  ++i) {
            CHECK(table->at(i) == input.at(i));
        }

        CHECK_THROWS(table->at(2));
        CHECK_THROWS(table->at(1000000));
    }

    SECTION("maxint, maxint-1") {
        std::vector<ValueType> input { std::numeric_limits<ValueType>::max(), std::numeric_limits<ValueType>::max() -1 };

        auto [context, table] = freeze_table<TestType>(input);

        CHECK(table->size() == input.size());
        CHECK(!table->empty());
        for (size_t i = 0;  i < input.size();  ++i) {
            CHECK(table->at(i) == input.at(i));
        }

        CHECK_THROWS(table->at(2));
        CHECK_THROWS(table->at(1000000));
    }

    SECTION("double maxint - 1") {
        std::vector<ValueType> input(2,std::numeric_limits<ValueType>::max());

        auto [context, table] = freeze_table<TestType>(input);

        CHECK(table->size() == input.size());
        CHECK(!table->empty());
        for (size_t i = 0;  i < input.size();  ++i) {
            CHECK(table->at(i) == input.at(i));
        }

        CHECK_THROWS(table->at(2));
        CHECK_THROWS(table->at(1000000));
    }

}

} // namespace MLDB

