/* coalesced_range_test.cc
   Wolfgang Sourdeau, 28 August 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test CoalescedRange
*/

#include "mldb/utils/coalesced_range.h"
#include "mldb/utils/testing/mldb_catch2.h"

using namespace std;
using namespace MLDB;

TEST_CASE("coalesced range basics")
{
    CoalescedRange<const char> range;

    range.add(span<const char>("hello", 5));
    range.add(span<const char>(" ", 1));
    range.add(span<const char>("world", 5));
    range.add(span<const char>("\n", 1));

    CHECK(range.size() == 12);

    std::string s;
    for (char c: range) s += c;

    CHECK(s == "hello world\n");

    auto it = range.begin(), end = range.end();
    CHECK(std::distance(it, end) == 12);
    CHECK(end - it == 12);
    CHECK(it != end);
    CHECK(it < end);
    CHECK(it <= end);
    CHECK(end > it);
    CHECK(end >= it);

    CHECK(it[0] == 'h');
    CHECK(it[5] == ' ');
    CHECK(it[6] == 'w');
    CHECK(it[11] == '\n');

    ++it;
    CHECK(std::distance(it, end) == 11);
    CHECK(it[0] == 'e');
    CHECK(it[5] == 'w');
    CHECK(it[6] == 'o');
    CHECK_THROWS(it[11]);

    --end;
    CHECK(std::distance(it, end) == 10);

    auto found = range.find(' ');
    CHECK(found != range.end());
    CHECK(std::distance(range.begin(), found) == 5);
    CHECK(*found == ' ');

    auto found2 = range.find(found + 1, range.end(), ' ');
    CHECK(found2 == range.end());

    auto found3 = range.find(range.begin() + 1, range.end(), 'h');
    CHECK(found3 == range.end());

    auto found4 = range.find(range.begin(), range.end() - 1, '\n');
    CHECK(found4 == range.end() - 1);
}

TEST_CASE("coalesced range reduce")
{
    CoalescedRange<const char> range;

    range.add(span<const char>("hello", 5));
    range.add(span<const char>(" ", 1));
    range.add(span<const char>("world", 5));
    range.add(span<const char>("\n", 1));

    SECTION("no-op reduce") {
        auto [first, last] = range.reduce(range.begin(), range.end());
        CHECK(first == 0);
        CHECK(last == 4);
        CHECK(range.to_string() == "hello world\n");
    }

    SECTION("null reduce at beginning") {
        auto [first, last] = range.reduce(range.begin(), range.begin());
        CHECK(first == 0);
        CHECK(last == 0);
        CHECK(range.to_string() == "");
    }

    SECTION("null reduce at all positions") {
        for (size_t i = 0; i < range.size(); ++i) {
            SECTION("position " + to_string(i)) {
                auto [first, last] = range.reduce(range.begin() + i, range.begin() + i);
                CHECK(first == last);
                CHECK(range.to_string() == "");
            }
        }
    }

    SECTION("keep n characters") {
        for (size_t keep = 0; keep < range.size(); ++keep) {
            SECTION("keep " + to_string(keep)) {
                for (size_t i = 0; i < range.size() - keep; ++i) {
                    SECTION("position " + to_string(i)) {
                        auto it = range.begin() + i, end = range.begin() + i + keep;
                        string str(it, end);
                        auto [first, last] = range.reduce(it, end);
                        CHECK(first <= last);
                        CHECK(range.to_string() == str);
                        CHECK(range.to_string() == string("hello world\n").substr(i, keep));
                    }
                }
            }
        }
    }

    SECTION("reduce remove first word") {
        auto [first, last] = range.reduce(range.begin(), range.begin() + 5);
        CHECK(first == 0);
        CHECK(last == 1);
        CHECK(range.to_string() == "hello");
    }
    
    SECTION("reduce remove second word") {
        auto [first, last] = range.reduce(range.begin() + 6, range.begin() + 11);
        CHECK(first == 2);
        CHECK(last == 3);
        CHECK(range.to_string() == "world");
    }
}

TEST_CASE("coalesced range to_string()")
{
    CoalescedRange<const char> range;

    range.add(span<const char>("hello", 5));
    range.add(span<const char>(" ", 1));
    range.add(span<const char>("world", 5));
    range.add(span<const char>("\n", 1));

    SECTION("full range") {
        CHECK(range.to_string() == "hello world\n");
    }

    SECTION("partial range") {
        CHECK(range.to_string(range.begin() + 6, range.end()) == "world\n");
    }

    SECTION("empty range") {
        CHECK(range.to_string(range.end(), range.end()) == "");
        CHECK(range.to_string(range.begin(), range.begin()) == "");
    }

    SECTION("keep n characters") {
        for (size_t keep = 0; keep < range.size(); ++keep) {
            SECTION("keep " + to_string(keep)) {
                for (size_t i = 0; i < range.size() - keep; ++i) {
                    SECTION("position " + to_string(i)) {
                        auto it = range.begin() + i, end = range.begin() + i + keep;
                        string str(it, end);
                        CHECK(range.to_string(it, end) == str);
                    }
                }
            }
        }
    }
}

TEST_CASE("coalesced range get_span")
{
    CoalescedRange<const char> range;

    range.add(span<const char>("hello, world!", 13));

    SECTION("full range") {
        auto found = range.get_span(range.begin(), range.end());
        REQUIRE(found.has_value());
        CHECK(std::string(found->data(), found->data() + found->size()) == "hello, world!");
    }

    SECTION("partial range at end") {
        auto found = range.get_span(range.begin() + 7, range.end());
        REQUIRE(found.has_value());
        CHECK(std::string(found->data(), found->data() + found->size()) == "world!");
    }

    SECTION("partial range at beginning") {
        auto found = range.get_span(range.begin(), range.begin() + 5);
        REQUIRE(found.has_value());
        CHECK(std::string(found->data(), found->data() + found->size()) == "hello");
    }

    SECTION("empty range at beginning") {
        auto found = range.get_span(range.begin(), range.begin());
        CHECK(found.has_value());
        CHECK(found->size() == 0);
    }

    SECTION("empty range at end") {
        auto found = range.get_span(range.end(), range.end());
        CHECK(found.has_value());
        CHECK(found->size() == 0);
    }

    SECTION("empty range in middle") {
        auto found = range.get_span(range.begin() + 5, range.begin() + 5);
        CHECK(found.has_value());
        CHECK(found->size() == 0);
    }

    SECTION("backwards range") {
        CHECK_THROWS(range.get_span(range.end(), range.begin()));
    }
}