/* prefix_trie_test.cc                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "catch2/catch_all.hpp"
#include "mldb/utils/memusage.h"
#include "mldb/utils/min_max.h"
#include "mldb/plugins/tabular/prefix_trie.h"
#include "mldb/arch/format.h"
#include "mldb/arch/ansi.h"
#include "mldb/vfs/filter_streams.h"
#include <unordered_set>

using namespace std;
using namespace MLDB;

namespace std {

template<typename T>
std::ostream & operator << (std::ostream & stream, const std::optional<T> & val)
{
    if (val.has_value()) {
        return stream << val.value();
    }
    else {
        return stream << "<NULLOPT>";
    }
}

} // namespace std

//TEST_CASE("test rank and select")
//{
//    CHECK(brank(0, {1U}) == 0);
//    CHECK(brank(1, {3U}) == 1); 
//}

template<typename Str>
void CHECK_LONGEST(const CharPrefixTrie & trie, Str&& s, uint32_t nn, uint32_t ll)
{
    //cerr << "longest " << s << endl;
    auto [n,l] = trie.longest(s);
    CHECK(n == nn);
    CHECK(l == ll);
}

void CHECK_NO_LONGEST(const CharPrefixTrie & trie, const char * s)
{
    //cerr << "no longest " << s << endl;
    auto [n,l] = trie.longest(s);
    CHECK(n == -1);
    CHECK(l == 0);
}

TEST_CASE("test null table")
{
    std::map<std::string, uint16_t> s;
    CharPrefixTrie trie = construct_trie(s);
    trie.dump(cerr);
    CHECK(trie.size() == 0);
    CHECK(memUsageIndirect(trie, {}) == 1);
    auto v = trie.get("hello");
    CHECK(v == std::nullopt);
    CHECK_NO_LONGEST(trie, "");
}

TEST_CASE("test one null entry")
{
    std::map<std::string, uint16_t> s = { { "", 1 }};
    CharPrefixTrie trie = construct_trie(s);
    //cerr << "trie.mem.size() = " << trie.mem.size() << endl;
    //for (std::byte c: trie.mem)
    //    cerr << (int)c << endl;
    trie.dump(cerr);
    CHECK(trie.size() == 1);
    CHECK(trie.get("").value() == 0);
    auto v = trie.get("hello");
    CHECK(v == std::nullopt);

    CHECK_LONGEST(trie, "", 0, 0);
    CHECK_NO_LONGEST(trie, " ");
}

TEST_CASE("test one entry")
{
    std::map<std::string, uint16_t> s = { { "hello", 1 }};
    CharPrefixTrie trie = construct_trie(s);
    cerr << "trie.mem.size() = " << trie.mem.size() << endl;
    trie.dump(cerr);
    CHECK(trie.size() == 1);
    CHECK(trie.get("hello") == 0);
    auto v = trie.get("world");
    CHECK(v == nullopt);

    CHECK_NO_LONGEST(trie, "");
    CHECK_NO_LONGEST(trie, "hell");
    CHECK_LONGEST(trie, "hello", 0, 5);
    CHECK_LONGEST(trie, "helloWORLD", 0, 5);
}

TEST_CASE("test two entries")
{
    std::map<std::string, uint16_t> s = { { "hello", 1 }, { "world", 2 }};
    CharPrefixTrie trie = construct_trie(s);
    cerr << "trie.mem.size() = " << trie.mem.size() << endl;
    trie.dump(cerr);
    CHECK(trie.size() == 2);
    CHECK(trie.get("hello") == 0);
    auto v = trie.get("world");
    CHECK(v == 1);

    CHECK_NO_LONGEST(trie, "");
    CHECK_NO_LONGEST(trie, "hell");
    CHECK_LONGEST(trie, "hello", 0, 5);
    CHECK_LONGEST(trie, "helloWORLD", 0, 5);
    CHECK_LONGEST(trie, "world", 1, 5);
    CHECK_LONGEST(trie, "worldWORLD", 1, 5);
}

TEST_CASE("test common prefix")
{
    std::map<std::string, uint16_t> s = { { "2", 1 }, { "23", 2 }};
    CharPrefixTrie trie = construct_trie(s);
    cerr << "****** trie.mem.size() = " << trie.mem.size() << endl;
    trie.dump(cerr);
    CHECK(trie.size() == 2);
    CHECK(trie.get("2") == 0);
    CHECK(trie.get("23") == 1);

    CHECK_LONGEST(trie, "234", 1, 2);
    CHECK_LONGEST(trie, "23", 1, 2);
    CHECK_LONGEST(trie, "2", 0, 1);
}

TEST_CASE("test telescoping")
{
    std::map<std::string, uint16_t> s;
    size_t n = 100;
    for (size_t i = 0;  i < n;  ++i) {
        s.emplace(std::string(i, 'a'), i);
    }
    CharPrefixTrie trie = construct_trie(s);
    cerr << "telescoping trie.mem.size() = " << trie.mem.size() << endl;
    //trie.dump(cerr);

    for (size_t i = 0;  i < n;  ++i) {
        std::string k(i, 'a');
        auto val = trie.get(k);
        CHECK(val == i);
    }
}

TEST_CASE("test large map")
{
    filter_istream stream("archive+file://mldb/plugins/tabular/testing/fixtures/words.zip#words.txt");
    std::map<std::string, uint16_t> s;
    int l = 0;
    int n = 100000;
    while (stream) {
        std::string word;
        std::getline(stream, word);
        if (word.empty())
            continue;
        s[word] = l++;
        if (l >= n)
            break;
    }

    CharPrefixTrie trie = construct_trie(s);
    cerr << "memusage of trie = " << memUsage(trie) << endl;
    cerr << "memusage of map = " << memUsage(s) << endl;
    //trie.dump(cerr);
    CHECK(trie.size() == s.size());

    std::unordered_set<uint32_t> done;

    for (auto [k,v]: s) {
        auto v2 = trie.get(k);
        CHECK(v2.has_value());
        CHECK(v2.value() < s.size());
        //cerr << "inserting " << k << " -> " << v2 << endl;
        CHECK(done.insert(v2.value()).second);

        CHECK_LONGEST(trie, k, v2.value(), k.size());
    }
}
