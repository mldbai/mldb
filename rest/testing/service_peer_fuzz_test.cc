// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* service_peer_test.cc
   Jeremy Barnes, 4 11 February 2015
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

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
#include <thread>
#include <chrono>

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

using namespace std;
using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_two_members )
{
    auto peer1 = std::make_shared<TestPeer>("peer1", "", "");
    peer1->init(PortRange(15000, 16000), "127.0.0.1");
    peer1->start();

    std::this_thread::sleep_for(std::chrono::seconds(1));

    cerr << "fuzzing on peer connection at " << peer1->getFromPeersAddress() << endl;

    HttpRestProxy proxy(peer1->getFromPeersAddress());

    // Check we get a connection refused
    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(proxy.get("/"), std::exception);
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    peer1->shutdown();
}


