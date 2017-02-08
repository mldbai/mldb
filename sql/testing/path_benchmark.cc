/** path_test.cc
    Jeremy Barnes, 10 April 2016
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Test of coordinate classes.
*/

#include "mldb/sql/path.h"
#include "mldb/arch/exception_handler.h"
#include "mldb/types/value_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/date.h"
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

BOOST_AUTO_TEST_CASE( test_hash_speed )
{
    vector<Path> paths;

    filter_istream stream("mldb/sql/testing/path_test_columns.txt");
    while (stream) {
        std::string s;
        getline(stream, s);
        if (s.empty())
            continue;
        paths.emplace_back(Path::parse(s));
    }

    int numIter = 10;

    size_t totalHash = 0;

    Date before = Date::now();

    for (size_t i = 0;  i < numIter;  ++i) {
        for (auto & p: paths) {
            totalHash += p.oldHash();
        }
    }

    Date between = Date::now();

    for (size_t i = 0;  i < numIter;  ++i) {
        for (auto & p: paths) {
            totalHash += p.newHash();
        }
    }

    Date after = Date::now();

    cerr << "totalHash = " << totalHash << endl;

    double elapsed1 = between.secondsSince(before);
    double elapsed2 = after.secondsSince(between);

    cerr << "Old hash : " << paths.size() * numIter << " in " << elapsed1 * 1000
         << "ms at " << paths.size() * numIter / elapsed1 << " hashes per second" << endl;
    cerr << "New hash : " << paths.size() * numIter << " in " << elapsed2 * 1000
         << "ms at " << paths.size() * numIter / elapsed2 << " hashes per second" << endl;
}
