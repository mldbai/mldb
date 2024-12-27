/* multi_suffix_array_test.cc                                              -*- C++ -*-
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

namespace std {

std::ostream & printArgs(std::ostream & stream)
{
    return stream;
}

template<typename First>
std::ostream & printArgs(std::ostream & stream, First&& first)
{
    return stream << first;    
}

template<typename First, typename... Ts>
std::ostream & printArgs(std::ostream & stream, First&& first, Ts&&... rest)
{
    printArgs(stream, first);
    stream << ", ";
    return printArgs(stream, std::forward<Ts>(rest)...);
}

template<typename... Ts>
std::ostream & operator << (std::ostream & stream, const std::tuple<Ts...> & tup)
{
    auto strTup = std::make_tuple(std::reference_wrapper(stream));
    stream << "(";
    std::apply(printArgs<Ts...>, std::tuple_cat(strTup, tup));
    return stream << ")";
}

} // namespace std

void validateMultiSuffixArray(const MultiSuffixArray & arr)
{
    std::string_view last;
    for (auto [v, n, ofs]: arr) {
        if (!last.empty())
            CHECK(last <= v);
        last = v;
    }
}

TEST_CASE("test null table")
{
    std::vector<string> s;
    MultiSuffixArray array(s.begin(), s.end());
    CHECK(array.size() == 0);
    CHECK(array.begin() == array.end());
    validateMultiSuffixArray(array);
}

TEST_CASE("test one char table")
{
    std::vector<string> s = {"1"};
    MultiSuffixArray array(s.begin(), s.end());
    CHECK(array.size() == 1);
    CHECK(array.begin() != array.end());
    validateMultiSuffixArray(array);
}

TEST_CASE("test two char table")
{
    std::vector<string> s = { "1", "12" };
    MultiSuffixArray array(s.begin(), s.end());
    CHECK(array.size() == 3);
    CHECK(array.begin() != array.end());
    CHECK_THROWS(array.at(3));
    validateMultiSuffixArray(array);
}

TEST_CASE("test small char table")
{
    std::vector<string> s = { "hello", "world" };
    MultiSuffixArray array(s.begin(), s.end());
    CHECK(array.size() == 10);
    CHECK(array.begin() != array.end());
    CHECK(array.at(0) == make_tuple("d", 1, 4));
    CHECK(array.at(1) == make_tuple("ello", 0, 1));
    CHECK(array.at(2) == make_tuple("hello", 0, 0));
    CHECK(array.at(3) == make_tuple("ld", 1, 3));
    CHECK(array.at(4) == make_tuple("llo", 0, 2));
    CHECK(array.at(5) == make_tuple("lo", 0, 3));
    CHECK(array.at(6) == make_tuple("o", 0, 4));
    CHECK(array.at(7) == make_tuple("orld", 1, 1));
    CHECK(array.at(8) == make_tuple("rld", 1, 2));
    CHECK(array.at(9) == make_tuple("world", 1, 0));
    validateMultiSuffixArray(array);
}

TEST_CASE("test repeated chars")
{
    std::vector<string> s = { "*****", "*****" };
    MultiSuffixArray array(s.begin(), s.end());
    CHECK(array.size() == 10);
    CHECK(array.begin() != array.end());
    CHECK(array.at(0) == make_tuple("*", 1, 4));
    CHECK(array.at(1) == make_tuple("*", 0, 4));
    CHECK(array.at(2) == make_tuple("**", 1, 3));
    CHECK(array.at(3) == make_tuple("**", 0, 3));
    CHECK(array.at(4) == make_tuple("***", 1, 2));
    CHECK(array.at(5) == make_tuple("***", 0, 2));
    CHECK(array.at(6) == make_tuple("****", 1, 1));
    CHECK(array.at(7) == make_tuple("****", 0, 1));
    CHECK(array.at(8) == make_tuple("*****", 1, 0));
    CHECK(array.at(9) == make_tuple("*****", 0, 0));
    validateMultiSuffixArray(array);
}

TEST_CASE("test count prefixes")
{
    std::vector<string> s = { "hello", "world" };
    MultiSuffixArray array(s.begin(), s.end());

    auto pref = countPrefixes(array);

    for (auto & [p,cnt]: pref) {
        CHECK(cnt > 0);
        cerr << "  '" << p << "' (len " << p.size() << "): " << cnt << endl;
    }

    auto getPref = [&] (const char * s) -> int
    {
        std::string_view sv(s);
        for (auto && [pref, res]: pref) {
            if (pref == sv)
                return res;
        }
        return 0;
    };

    CHECK(pref.size() == 2);
    CHECK(getPref("o") == 2);
    CHECK(getPref("l") == 3);
}

TEST_CASE("test count prefixes 2")
{
    std::vector<string> s = { "*****" };
    MultiSuffixArray array(s.begin(), s.end());

    auto pref = countPrefixes(array);

    for (auto & [p,cnt]: pref) {
        CHECK(cnt > 0);
        cerr << "  '" << p << "' (len " << p.size() << "): " << cnt << endl;
    }

    auto getPref = [&] (const char * s) -> int
    {
        std::string_view sv(s);
        for (auto && [pref, res]: pref)
            if (pref == sv)
                return res;
        return 0;
    };

    CHECK(pref.size() == 4);
    CHECK(getPref("*") == 5);
    CHECK(getPref("**") == 4);
    CHECK(getPref("***") == 3);
    CHECK(getPref("****") == 2);
}

TEST_CASE("test count prefixes 3")
{
    std::vector<string> s = { "*****", "*****" };
    MultiSuffixArray array(s.begin(), s.end());

    auto pref = countPrefixes(array);

    for (auto & [p,cnt]: pref) {
        CHECK(cnt > 0);
        cerr << "  '" << p << "' (len " << p.size() << "): " << cnt << endl;
    }

    auto getPref = [&] (const char * s) -> int
    {
        std::string_view sv(s);
        for (auto && [pref, res]: pref)
            if (pref == sv)
                return res;
        return 0;
    };

    CHECK(pref.size() == 5);
    CHECK(getPref("*") == 10);
    CHECK(getPref("**") == 8);
    CHECK(getPref("***") == 6);
    CHECK(getPref("****") == 4);
    CHECK(getPref("*****") == 2);
}