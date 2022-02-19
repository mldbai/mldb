/** mmap_test.cc
    Jeremy Barnes, 10 April 2016
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/
#include "mldb/ext/catch2/src/catch2/catch_all.hpp"
#include "mldb/plugins/tabular/mmap.h"
#include "mldb/plugins/tabular/predictor.h"

using namespace std;
using namespace MLDB;

TEST_CASE("MappingContext option basics")
{
    MappingContext context;
    CHECK(context.getOption(DEBUG) == false);
    CHECK(context.getOption(TRACE) == false);

    context.setOption(DEBUG = true);

    CHECK(context.getOption(DEBUG) == true);
    CHECK(context.getOption(TRACE) == false);

    context.setOption(DEBUG = false);

    CHECK(context.getOption(DEBUG) == false);
    CHECK(context.getOption(TRACE) == false);

    context.setOption(DEBUG = true);

    CHECK(context.getOption(DEBUG) == true);

    context.unsetOption(DEBUG);

    CHECK(context.getOption(DEBUG) == false);
}

TEST_CASE("MappingContext nested")
{
    MappingContext context;
    CHECK(context.getOption(DEBUG) == false);

    MappingContext nested = context.with(DEBUG = true);
    CHECK(context.getOption(DEBUG) == false);
    CHECK(nested.getOption(DEBUG) == true);

    context.setOption(DEBUG = true);
    CHECK(context.getOption(DEBUG) == true);
    CHECK(nested.getOption(DEBUG) == true);

    nested.setOption(DEBUG = false);
    CHECK(context.getOption(DEBUG) == true);
    CHECK(nested.getOption(DEBUG) == false);

    nested.unsetOption(DEBUG);
    CHECK(context.getOption(DEBUG) == true);
    CHECK(nested.getOption(DEBUG) == true);
}

template<typename T>
std::pair<MappingContext, MappedVector<T> *> freeze_vector(const std::vector<T> & input)
{
    using namespace std;

    MappingContext context(1000 << 10);
    auto & output = context.alloc<MappedVector<T> >();
    freeze(context, output, input);

    cerr << "froze " << input.size() << " elements into " << context.getOffset() << " bytes at "
         << 8.0 * context.getOffset() / input.size() << " bits/element" << endl;

    REQUIRE(output.size() == input.size());

    return std::make_pair(context, &output);
}

TEST_CASE("mmap vector easy case")
{
    std::vector<Predictor> input;

    size_t n = 100;
    for (size_t i = 0;  i < n;  ++i) {
        Predictor p;
        p.offset = i;
        for (auto & param: p.params)
            param = i;
        input.push_back(p);
    }

    auto [context, output] = freeze_vector(input);

    REQUIRE(output->size() == n);
    for (size_t i = 0;  i < n;  ++i) {
        CHECK(output->at(i).offset == (int64_t)i);
        for (auto & param: output->at(i).params)
            CHECK(param == i);
    }
}

struct SmallObject {
    uint8_t val;
} __attribute__((__packed__));

TEST_CASE("mmap vector small object")
{
    std::vector<SmallObject> input;

    size_t n = 100;

    REQUIRE(n < 256);

    for (size_t i = 0;  i < n;  ++i) {
        SmallObject o;
        o.val = i;
        input.push_back(o);
    }

    auto [context, output] = freeze_vector(input);

    REQUIRE(output->size() == n);
    for (size_t i = 0;  i < n;  ++i) {
        CHECK(output->at(i).val == (int64_t)i);
    }
}

struct UnalignedObject {
    uint8_t val;
    uint8_t vals[2];
} __attribute__((__packed__));

TEST_CASE("mmap vector unaligned object")
{
    CHECK(alignof(UnalignedObject) == 1);
    CHECK(sizeof(UnalignedObject) == 3);

    std::vector<UnalignedObject> input;

    size_t n = 100;

    REQUIRE(n < 256);

    for (size_t i = 0;  i < n;  ++i) {
        UnalignedObject o;
        o.val = i;
        o.vals[0] = o.vals[1] = i;
        input.push_back(o);
    }

    auto [context, output] = freeze_vector(input);

    REQUIRE(output->size() == n);
    for (size_t i = 0;  i < n;  ++i) {
        CHECK(output->at(i).val == (int64_t)i);
    }
}

struct WeirdlyAlignedObject {
    uint32_t val;
    uint64_t val2;
};

TEST_CASE("mmap vector weirdly aligned object")
{
    CHECK(alignof(WeirdlyAlignedObject) == 8);
    CHECK(sizeof(WeirdlyAlignedObject) == 16);

    std::vector<WeirdlyAlignedObject> input;

    size_t n = 100;

    REQUIRE(n < 256);

    for (size_t i = 0;  i < n;  ++i) {
        WeirdlyAlignedObject o;
        o.val = i;
        o.val2 = i;
        input.push_back(o);
    }

    auto [context, output] = freeze_vector(input);

    REQUIRE(output->size() == n);
    for (size_t i = 0;  i < n;  ++i) {
        CHECK(output->at(i).val == (int64_t)i);
        CHECK(output->at(i).val2 == (uint64_t)i);
    }
}

struct WeirdlyAlignedObject2 {
    union {
        uint64_t val2;
        uint32_t val2s[2];
    };
    uint32_t val;
} __attribute__((__packed__)) __attribute__((__aligned__(4)));

TEST_CASE("mmap vector weirdly aligned object 2")
{
    CHECK(alignof(WeirdlyAlignedObject2) == 4);
    CHECK(sizeof(WeirdlyAlignedObject2) == 12);

    std::vector<WeirdlyAlignedObject2> input;

    size_t n = 100;

    REQUIRE(n < 256);

    for (size_t i = 0;  i < n;  ++i) {
        WeirdlyAlignedObject2 o;
        o.val = i;
        o.val2 = i;
        input.push_back(o);
    }

    auto [context, output] = freeze_vector(input);

    REQUIRE(output->size() == n);
    for (int64_t i = 0;  i < (int64_t)n;  ++i) {
        CHECK((int64_t)output->at(i).val == i);  // explicit casts to avoid undefined behavior
        CHECK((int64_t)output->at(i).val2 == i);
    }
}
