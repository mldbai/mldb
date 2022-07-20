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


using namespace std;
using namespace MLDB;

TEST_CASE("test null table")
{
    std::map<std::string, uint16_t> s;
    CharPrefixTrie trie = construct_trie(s);
    cerr << "---" << endl;
    trie.dump(cerr);
    cerr << "---" << endl;
    CHECK(trie.size() == 0);
    CHECK(memUsageIndirect(trie, {}) == 1);
    uint16_t v = trie.get(u8"hello", MAX_LIMIT);
    CHECK(v == MAX_LIMIT);
}

TEST_CASE("test one null entry")
{
    std::map<std::string, uint16_t> s = { { "", 1 }};
    CharPrefixTrie trie = construct_trie(s);
    cerr << "trie.mem.size() = " << trie.mem.size() << endl;
    for (std::byte c: trie.mem)
        cerr << (int)c << endl;
    cerr << "---" << endl;
    trie.dump(cerr);
    cerr << "---" << endl;
    CHECK(trie.size() == 1);
    CHECK(trie.get(u8"", MAX_LIMIT) == 1);
    uint16_t v = trie.get(u8"hello", MAX_LIMIT);
    CHECK(v == MAX_LIMIT);
}

TEST_CASE("test one entry")
{
    std::map<std::string, uint16_t> s = { { "hello", 1 }};
    CharPrefixTrie trie = construct_trie(s);
    cerr << "trie.mem.size() = " << trie.mem.size() << endl;
    cerr << "---" << endl;
    trie.dump(cerr);
    cerr << "---" << endl;
    CHECK(trie.size() == 1);
    CHECK(trie.get(u8"hello", MAX_LIMIT) == 1);
    uint16_t v = trie.get(u8"world", MAX_LIMIT);
    CHECK(v == MAX_LIMIT);
}

TEST_CASE("test two entries")
{
    std::map<std::string, uint16_t> s = { { "hello", 1 }, { "world", 2 }};
    CharPrefixTrie trie = construct_trie(s);
    cerr << "trie.mem.size() = " << trie.mem.size() << endl;
    cerr << "---" << endl;
    trie.dump(cerr);
    cerr << "---" << endl;
    CHECK(trie.size() == 2);
    CHECK(trie.get(u8"hello", MAX_LIMIT) == 1);
    uint16_t v = trie.get(u8"world", MAX_LIMIT);
    CHECK(v == 2);
}

TEST_CASE("test common prefix")
{
    std::map<std::string, uint16_t> s = { { "2", 1 }, { "23", 2 }};
    CharPrefixTrie trie = construct_trie(s);
    cerr << "trie.mem.size() = " << trie.mem.size() << endl;
    cerr << "---" << endl;
    trie.dump(cerr);
    cerr << "---" << endl;
    CHECK(trie.size() == 2);
    CHECK(trie.get(u8"2", MAX_LIMIT) == 1);
    uint16_t v = trie.get(u8"23", MAX_LIMIT);
    CHECK(v == 2);
}

TEST_CASE("test large map")
{
    filter_istream stream("archive+file://mldb/plugins/tabular/testing/fixtures/words.zip#words.txt");
    std::map<std::string, uint16_t> s;
    int l = 0;
    int n = 10000;
    while (stream) {
        ++l;
        std::string word;
        std::getline(stream, word);
        if (word.empty())
            continue;
        s[word] = l;
        if (l >= n)
            break;
    }

    if (false) {
        for (auto & [w,i]: s) {
            cerr << w << " -> " << i << endl;
        }
    }

    CharPrefixTrie trie = construct_trie(s);
    cerr << "memusage of trie = " << memUsage(trie) << endl;
    cerr << "memusage of map = " << memUsage(s) << endl;
    //trie.dump(cerr);
    CHECK(trie.size() == s.size());

}