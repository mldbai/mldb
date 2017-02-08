// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* link_test.cc
   Jeremy Barnes, 31 March 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Test of watches.
*/

#include "mldb/rest/link.h"
#include "mldb/types/basic_value_descriptions.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

using namespace std;
using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_connected_propagation )
{
    std::shared_ptr<LinkToken> t1, t2;
    std::tie(t1, t2) = createLinkT<LinkToken>(LS_CONNECTING);

    BOOST_CHECK_EQUAL(t1->getState(), LS_CONNECTING);
    BOOST_CHECK_EQUAL(t2->getState(), LS_CONNECTING);
    t1->updateState(LS_CONNECTED);
    BOOST_CHECK_EQUAL(t1->getState(), LS_CONNECTED);
    BOOST_CHECK_EQUAL(t2->getState(), LS_CONNECTED);

    t1.reset();
    BOOST_CHECK_EQUAL(t2->getState(), LS_DISCONNECTED);
}
