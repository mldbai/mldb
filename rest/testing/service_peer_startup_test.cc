// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** service_peer_startup_test.cc                                   -*- C++ -*-
    Jeremy Barnes, 14 November 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Test of service peer startup.
*/

#include "temporary_etcd_server.h"
#include "mldb/rest/service_peer.h"
#include "mldb/rest/etcd_client.h"
#include "mldb/utils/runner.h"
#include "mldb/rest/rest_service_endpoint.h"
#include "mldb/rest/rest_request_router.h"
#include <boost/algorithm/string.hpp>
#include <sys/wait.h>
#include "test_peer.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;
using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_startup )
{
    for (unsigned i = 0;  i < 2000;  ++i) {
        cerr << endl << endl << endl << "------------------------"
             << "Iteration " << i << endl;
        
        auto peer1 = std::make_shared<TestPeer>("peer1", "", "");
        peer1->init(PortRange(15000, 16000), "127.0.0.1");
        peer1->start();
    }
}
