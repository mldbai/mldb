// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* service_peer_process_discovery_test.cc
   Jeremy Barnes, 4 March 2014
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
#include <chrono>
#include <thread>

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;
using namespace MLDB;


BOOST_AUTO_TEST_CASE(test_separate_process_discovery)
{
    cerr << endl << endl << endl << endl
         << "----------------- separate process discovery" << endl;
    signal(SIGPIPE, SIG_IGN);

    string etcdUri = getEtcdUri();
    string etcdPath = getEtcdPath();
    clearEtcd(etcdUri, etcdPath);

    ForkedTestPeer peer2("peer2", etcdUri, etcdPath);
    HttpRestProxy proxy2(peer2.httpAddr);
    //cerr << proxy2.get("").jsonBody() << endl;
    cerr << proxy2.get("/peerStatus").jsonBody() << endl;

    ForkedTestPeer peer1("peer1", etcdUri, etcdPath);
    cerr << "peer listening on " << peer1.httpAddr << " and "
         << peer2.httpAddr << endl;

    HttpRestProxy proxy1(peer1.httpAddr);

    Date beforeDiscovery = Date::now();

    for (;;) {
        auto resp = jsonDecodeStr<vector<PeerStatus> >(proxy1.get("/peerStatus").body());
        int numGoodConnections = 0;
        int numBadConnections = 0;
        for (auto & p: resp) {
            numGoodConnections += p.state == PS_OK;
            numBadConnections += p.state != PS_OK;
        }
        BOOST_REQUIRE_EQUAL(numBadConnections, 0);
        if (numGoodConnections > 1)
            break;
    }

    for (;;) {
        auto resp = jsonDecodeStr<vector<PeerStatus> >(proxy2.get("/peerStatus").body());
        int numGoodConnections = 0;
        int numBadConnections = 0;
        for (auto & p: resp) {
            numGoodConnections += p.state == PS_OK;
            numBadConnections += p.state != PS_OK;
        }
        BOOST_REQUIRE_EQUAL(numBadConnections, 0);
        if (numGoodConnections > 1)
            break;
    }

    Date afterDiscovery = Date::now();

    double discoveryTook = afterDiscovery.secondsSince(beforeDiscovery);
    
    cerr << "discovery took " << discoveryTook << endl;

    BOOST_CHECK_LE(discoveryTook, 1.0);

    // Let some pinging happen...
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Simulate the peer hanging
    peer1.signal(SIGSTOP);

    cerr << "sleeping..." << endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    cerr << "awake" << endl;

    // Check that the connection status is not OK anymore
    cerr << proxy2.get("/peerStatus", {}, {}, 2).jsonBody();
    auto peer1Status = jsonDecodeStr<PeerStatus>(proxy2.get("/peers/peer1/status", {}, {}, 2).body());

    BOOST_CHECK_EQUAL(peer1Status.state, PS_ERROR);

    cerr << "sleeping..." << endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));
    cerr << "awake!" << endl;

    // Check that the peer has disappeared and been cleaned up
    cerr << proxy2.get("/peerStatus").jsonBody() << endl;
    BOOST_CHECK_EQUAL(proxy2.get("/peers/peer1/status", {}, {}, 2).code(),
                      400);
    cerr << "awake" << endl;

    peer1.signal(SIGCONT);

    cerr << "sleeping..." << endl;
    std::this_thread::sleep_for(std::chrono::seconds(3));
    cerr << "awake" << endl;

    peer1Status = jsonDecodeStr<PeerStatus>(proxy2.get("/peers/peer1/status", {}, {}, 2).body());
    BOOST_CHECK_EQUAL(peer1Status.state, PS_OK);
}
