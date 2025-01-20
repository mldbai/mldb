/* scoreboard_test.cc
   Jeremy Barnes, 28 August 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test the scoreboard class.
*/

#include "mldb/utils/scoreboard.h"
#include "mldb/utils/testing/mldb_catch2.h"

using namespace std;
using namespace MLDB;

TEST_CASE("test linear scoreboard")
{
    LinearScoreboard s(8 /* max ahead */);

    SECTION("empty") {
        CHECK(s.progress() == 0);

    }

    SECTION("one item") {
        auto [started, toofarahead] = s.beginWorkItem(0);
        CHECK(started);
        CHECK(!toofarahead);
        CHECK(s.progress() == 0);

        CHECK(!s.beginWorkItem(0).started);

        auto [first, last] = s.endWorkItem(0);
        CHECK(first == 0);
        CHECK(last == 1);
        CHECK(s.progress() == 1);
        CHECK_THROWS(s.endWorkItem(0));
        CHECK(s.beginWorkItem(0).started == false);
    }

    SECTION("too far ahead") {
        auto [started, toofarahead] = s.beginWorkItem(8);
        CHECK(!started);
        CHECK(toofarahead);
    }

    SECTION("out of order, first ending at start") {
        auto [started2, toofarahead2] = s.beginWorkItem(3);
        CHECK(started2);
        CHECK(!toofarahead2);
        CHECK(s.progress() == 0);

        auto [first, last] = s.endWorkItem(3);
        CHECK(first == 0);
        CHECK(last == 0);
        CHECK(s.progress() == 0);

        CHECK(s.beginWorkItem(1).started);
        CHECK(s.beginWorkItem(2).started);
        CHECK(s.endWorkItem(2).count() == 0);
        CHECK(s.endWorkItem(1).count() == 0);

        CHECK(s.beginWorkItem(0).started);
        auto [first2, last2] = s.endWorkItem(0);
        CHECK(first2 == 0);
        CHECK(last2 == 4);
    }

    SECTION("out of order, first ending in middle") {
        auto [started2, toofarahead2] = s.beginWorkItem(3);
        CHECK(started2);
        CHECK(!toofarahead2);
        CHECK(s.progress() == 0);

        auto [first, last] = s.endWorkItem(3);
        CHECK(first == 0);
        CHECK(last == 0);
        CHECK(s.progress() == 0);

        CHECK(s.beginWorkItem(0).started);
        CHECK(s.beginWorkItem(2).started);
        CHECK(s.endWorkItem(2).count() == 0);
        CHECK(s.endWorkItem(0).count() == 1);

        CHECK(s.beginWorkItem(1).started);
        auto [first2, last2] = s.endWorkItem(1);
        CHECK(first2 == 1);
        CHECK(last2 == 4);
    }

    SECTION("one at a time") {
        for (size_t i = 0; i < 1000; ++i) {
            auto [started, toofarahead] = s.beginWorkItem(i);
            CHECK(started);
            CHECK(!toofarahead);
            CHECK(s.progress() == i);

            auto [first, last] = s.endWorkItem(i);
            CHECK(first == i);
            CHECK(last == i + 1);
            CHECK(s.progress() == i + 1);
        }
    }

    SECTION("five at a time") {
        for (size_t i = 0; i < 5; ++i) {
            auto [started, toofarahead] = s.beginWorkItem(i);
            CHECK(started);
            CHECK(!toofarahead);
            CHECK(s.progress() == 0);
        }

        for (size_t i = 5; i < 1000; ++i) {
            auto [started, toofarahead] = s.beginWorkItem(i);
            CHECK(started);
            CHECK(!toofarahead);
            CHECK(s.progress() == i - 5);

            auto [first, last] = s.endWorkItem(i - 5);
            CHECK(first == i - 5);
            CHECK(last == i - 4);
            CHECK(s.progress() == i - 4);
        }

        for (size_t i = 995; i < 1000; ++i) {
            auto [first, last] = s.endWorkItem(i);
            CHECK(first == i);
            CHECK(last == i + 1);
            CHECK(s.progress() == i + 1);
        }
    }
}