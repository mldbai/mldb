/* rf_kernel_test.cc                                           -*- C++ -*-
   Jeremy Barnes, 29 December 2018
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test of tick counter functionality.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "plugins/jml/randomforest_kernels.h"
#include <boost/test/unit_test.hpp>

using namespace ML;
using namespace std;
using namespace ML::RF;

using boost::unit_test::test_suite;

BOOST_AUTO_TEST_CASE( test_partition_index )
{
    PartitionIndex none;

    BOOST_CHECK_EQUAL(none.index, 0);
    BOOST_CHECK_EQUAL(none.path(), "none");
    BOOST_CHECK_EQUAL(none.parent(), none);
    BOOST_CHECK_EQUAL(none, PartitionIndex::none());
    BOOST_CHECK_EQUAL(none.depth(), -1);
    
    PartitionIndex root = PartitionIndex::root();
    
    BOOST_CHECK_EQUAL(root.index, 1);
    BOOST_CHECK_EQUAL(root.path(), "root");
    BOOST_CHECK_EQUAL(root.parent(), none);
    BOOST_CHECK_NE(root, none);
    BOOST_CHECK_EQUAL(root.depth(), 0);

    PartitionIndex left = root.leftChild();
    BOOST_CHECK_EQUAL(left.index, 2);
    BOOST_CHECK_EQUAL(left.depth(), 1);
    BOOST_CHECK_EQUAL(left.parent(), root);
    BOOST_CHECK_EQUAL(left.path(), "l");

    PartitionIndex right = root.rightChild();
    BOOST_CHECK_EQUAL(right.index, 3);
    BOOST_CHECK_EQUAL(right.depth(), 1);
    BOOST_CHECK_EQUAL(right.parent(), root);
    BOOST_CHECK_EQUAL(right.path(), "r");
}
