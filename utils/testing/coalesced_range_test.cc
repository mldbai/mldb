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
