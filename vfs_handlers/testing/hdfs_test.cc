// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* hdfs_test.cc
   Wolfgang Sourdeau, March 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   Unit tests for hdfs.

   This test requires a setup similar to the one documented in
   http://www.drweiwang.com/install-hadoop-2-2-0-debian/
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <iostream>
#include <boost/test/unit_test.hpp>
#include "mldb/vfs/filter_streams.h"

using namespace std;
// using namespace MLDB;

#if 1
BOOST_AUTO_TEST_CASE( test_hdfs_istream )
{
    string expected = "12345\ncoucou\n";
    filter_istream stream("hdfs://localhost:9000/test-directory/test1");
    string content = stream.readAll();
    stream.close();

    BOOST_CHECK_EQUAL(content, expected);
}
#endif

#if 1
BOOST_AUTO_TEST_CASE( test_hdfs_ostream )
{
    string expected = "Hello\n HDFS\n\t\n";
    {
        filter_ostream stream("hdfs://hadoopuser@localhost:9000/test-directory/write-test");
        stream << expected;
        stream.close();
    }

    {
        filter_istream stream("hdfs://localhost:9000/test-directory/write-test");
        string content = stream.readAll();
        stream.close();
        BOOST_CHECK_EQUAL(content, expected);
    }
}
#endif
