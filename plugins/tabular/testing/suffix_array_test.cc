/* suffix_array_test.cc                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "catch2/catch_all.hpp"
#include "mldb/vfs/filter_streams.h"
#include "mldb/utils/memusage.h"
#include "mldb/plugins/tabular/suffix_array.h"
#include "mldb/types/value_description.h"
#include "mldb/types/map_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/arch/format.h"
#include "mldb/arch/ansi.h"


using namespace std;
using namespace MLDB;

void validateSuffixArray(const SuffixArray & arr)
{
    CHECK(arr.str.size() == arr.suffixes.size());

    std::string_view last;
    size_t totalLen = 0;
    for (auto v: arr) {
        if (!last.empty())
            CHECK(last < v);
        last = v;
        totalLen += v.length();
    }

    CHECK(totalLen == arr.size() * (arr.size() + 1) / 2);
}

TEST_CASE("test null table")
{
    std::string s;
    SuffixArray array(s);
    CHECK(array.size() == 0);
    CHECK(array.begin() == array.end());
    validateSuffixArray(array);
}

TEST_CASE("test one char table")
{
    std::string s = "0";
    SuffixArray array(s);
    CHECK(array.size() == 1);
    CHECK(array.begin() != array.end());
    CHECK(array.offset(array.begin()) == 0);

    CHECK(array.at(0) == "0");
    validateSuffixArray(array);
}

TEST_CASE("test two char table")
{
    std::string s = "12";
    SuffixArray array(s);
    CHECK(array.size() == 2);
    CHECK(array.begin() != array.end());
    CHECK(array.offset(array.begin()) == 0);

    CHECK(array.at(0) == "12");
    CHECK(array.at(1) == "2");
    CHECK_THROWS(array.at(2));
    validateSuffixArray(array);
}

TEST_CASE("test small char table")
{
    std::string s = "hello";
    SuffixArray array(s);
    CHECK(array.size() == 5);
    CHECK(array.begin() != array.end());
    CHECK(array.offset(array.begin()) == 1);

    CHECK(array.at(0) == "ello");
    CHECK(array.at(1) == "hello");
    CHECK(array.at(2) == "llo");
    CHECK(array.at(3) == "lo");
    CHECK(array.at(4) == "o");
    validateSuffixArray(array);
}