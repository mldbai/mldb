/** path_order_test.cc
    Jeremy Barnes, 10 April 2016
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Test of coordinate classes.
*/

#include "mldb/types/path.h"
#include "mldb/arch/exception_handler.h"
#include "mldb/types/value_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/http/http_exception.h"
#include "mldb/vfs/filter_streams.h"
#include <set>
#include <unordered_set>

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <tuple>
#include <iostream>

using namespace std;
using namespace MLDB;

// MLDB-1936
BOOST_AUTO_TEST_CASE(test_ordered2)
{
    vector<Path> paths;

    vector<int> lengthBuckets;

    filter_istream stream("mldb/sql/testing/path_test_columns.txt");
    while (stream) {
        std::string s;
        getline(stream, s);
        if (s.empty())
            continue;
        paths.emplace_back(Path::parse(s));
        int l = paths.back().size();
        if (l >= lengthBuckets.size())
            lengthBuckets.resize(l + 1);
        lengthBuckets[l] += 1;
    }
   
    cerr << "got " << paths.size() << " paths" << endl;
    cerr << jsonEncodeStr(lengthBuckets) << endl;

    for (unsigned i = 0;  i < 10;  ++i) {
        std::random_shuffle(paths.begin(), paths.end());
        std::unordered_set<Path> unordered;
        std::unordered_set<uint64_t> unorderedHashes;
        std::set<Path> ordered;
        std::set<uint64_t> orderedHashes;

        for (const Path & p: paths) {
            BOOST_CHECK(!ordered.count(p));
            BOOST_CHECK(!unordered.count(p));
            BOOST_CHECK(unordered.insert(p).second);
            BOOST_CHECK(unorderedHashes.insert(p.hash()).second);
            BOOST_CHECK(ordered.insert(p).second);
            BOOST_CHECK(orderedHashes.insert(p.hash()).second);
            BOOST_CHECK(ordered.count(p));
            BOOST_CHECK(unordered.count(p));
        }

        BOOST_CHECK_EQUAL(ordered.size(), paths.size());
        BOOST_CHECK_EQUAL(unordered.size(), paths.size());
        BOOST_CHECK_EQUAL(orderedHashes.size(), paths.size());
        BOOST_CHECK_EQUAL(unorderedHashes.size(), paths.size());

        for (const Path & p: paths) {
            if (!ordered.count(p))
                cerr << "missing path " << p << endl;
            BOOST_CHECK(ordered.count(p));
            BOOST_CHECK(unordered.count(p));
            BOOST_CHECK(orderedHashes.count(p.hash()));
            BOOST_CHECK(unorderedHashes.count(p.hash()));
        }
    }
}

