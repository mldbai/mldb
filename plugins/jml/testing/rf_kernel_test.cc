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
    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(none.parent(), std::exception);
    }
    BOOST_CHECK_EQUAL(none, PartitionIndex::none());
    BOOST_CHECK_EQUAL(none.depth(), -1);
    
    PartitionIndex root = PartitionIndex::root();
    
    BOOST_CHECK_EQUAL(root.index, 1);
    BOOST_CHECK_EQUAL(root.path(), "root");
    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(root.parent(), std::exception);
    }
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

    PartitionIndex ll = left.leftChild();
    PartitionIndex lr = left.rightChild();
    PartitionIndex rl = right.leftChild();
    PartitionIndex rr = right.rightChild();
    BOOST_CHECK_EQUAL(ll.parent(), left);
    BOOST_CHECK_EQUAL(lr.parent(), left);
    BOOST_CHECK_EQUAL(rl.parent(), right);
    BOOST_CHECK_EQUAL(rr.parent(), right);
    BOOST_CHECK_EQUAL(ll.depth(), 2);
    BOOST_CHECK_EQUAL(lr.depth(), 2);
    BOOST_CHECK_EQUAL(rl.depth(), 2);
    BOOST_CHECK_EQUAL(rr.depth(), 2);
    BOOST_CHECK_EQUAL(ll.path(), "ll");
    BOOST_CHECK_EQUAL(lr.path(), "lr");
    BOOST_CHECK_EQUAL(rl.path(), "rl");
    BOOST_CHECK_EQUAL(rr.path(), "rr");
    BOOST_CHECK_EQUAL(ll.index, 4);
    BOOST_CHECK_EQUAL(rl.index, 5);
    BOOST_CHECK_EQUAL(lr.index, 6);
    BOOST_CHECK_EQUAL(rr.index, 7);
}
