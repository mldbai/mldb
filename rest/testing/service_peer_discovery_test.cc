// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* service_peer_test.cc
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

BOOST_AUTO_TEST_CASE( test_two_members_discovery )
{
    string etcdUri = getEtcdUri();
    string etcdPath = getEtcdPath();
    clearEtcd(etcdUri, etcdPath);

    auto peer1 = std::make_shared<TestPeer>("peer1", etcdUri, etcdPath);
    peer1->init(PortRange(15000, 16000), "127.0.0.1");
    peer1->start();

    // Make sure peer1 has already discovered itself
    auto peers1 = peer1->getPeerStatuses();

    BOOST_CHECK_EQUAL(peers1.size(), 1);

    for (auto & p: peers1) {
        BOOST_CHECK_EQUAL(p.peerName, "peer1");
        BOOST_CHECK_EQUAL(p.connection.state, ST_CONNECTED);
    }

    TestPeer peer2("peer2", etcdUri, etcdPath);
    peer2.init(PortRange(15000, 16000), "127.0.0.1");
    peer2.start();

    // Give 2 seconds for them to discover each other
    all(peer1->watch({ "peers", "peer2" }, true),
        peer2.watch({ "peers", "peer1" }, true))
        .wait(2.0);
    
    std::this_thread::sleep_for(std::chrono::seconds(1));

    {
        auto peers1 = peer1->getPeerStatuses();
        auto peers2 = peer2.getPeerStatuses();

        cerr << jsonEncode(peers1) << endl;
        cerr << jsonEncode(peers2) << endl;

        ExcAssertEqual(peers1.size(), 2);
        ExcAssertEqual(peers2.size(), 2);

        for (auto & s: peers1) {
            if (s.error != "")
                cerr << "peer1 connection error: s = " << jsonEncode(s) << endl;
            BOOST_CHECK_EQUAL(s.error, "");
        }
        for (auto & s: peers2) {
            if (s.error != "")
                cerr << "peer2 connection error: s = " << jsonEncode(s) << endl;
            BOOST_CHECK_EQUAL(s.error, "");
        }
    }

#if 0
    for (unsigned i = 0;  i < 5;  ++i) {
        //cerr << jsonEncode(peer1->getPeerConnections()) << endl;
        //cerr << jsonEncode(peer1->getPeerStatuses()) << endl;
        ML::sleep(1.0);
    }
#endif

    // Make sure we're not busy waiting sending messages
    for (auto & p: peer1->getPeerStatuses()) {
        BOOST_CHECK_LT(p.messagesSent, 50);
    }

    //ML::sleep(1.0);

    peer1->shutdown();
    peer1.reset();

    //ML::sleep(2.0);
    //cerr << jsonEncode(peer2.getPeerConnections()) << endl;

    peer2.shutdown();
}
