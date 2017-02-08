// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* service_peer_test.cc
   Jeremy Barnes, 4 March 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "mldb/rest/service_peer.h"
#include "temporary_etcd_server.h"
#include "mldb/rest/etcd_client.h"
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

BOOST_AUTO_TEST_CASE(test_peer_generic_links)
{
    signal(SIGPIPE, SIG_IGN);
    signal(SIGSEGV, SIG_IGN);

    string etcdUri = getEtcdUri();
    string etcdPath = getEtcdPath();
    clearEtcd(etcdUri, etcdPath);

    auto peer1 = std::make_shared<TestPeer>("peer1", etcdUri, etcdPath);
    peer1->init(PortRange(15000, 16000), "127.0.0.1");
    peer1->start();

    auto peer2 = std::make_shared<TestPeer>("peer2", etcdUri, etcdPath);
    peer2->init(PortRange(15000, 16000), "127.0.0.1");
    peer2->start();

    WatchT<string> p1w = peer1->watch({"peers", "names:peer2"}, true /* catchUp*/);

    BOOST_CHECK_EQUAL(p1w.wait(1.0), "+peer2");

    WatchT<string> p2w = peer2->watch({"peers", "names:peer1"}, true /* catchUp*/);

    BOOST_CHECK_EQUAL(p2w.wait(1.0), "+peer1");

    BOOST_CHECK_EQUAL(peer1->knownPeers(),
                      vector<string>({"peer1", "peer2"}));


    ResourcePath peer1Address{"peers", "peer1"};

    auto link = peer1->createLink(peer1Address, "subscription", string("hello"));
    BOOST_CHECK_EQUAL(link->getRemoteAddress(), peer1Address);
    BOOST_CHECK_EQUAL(link->getLocalAddress(), ResourcePath{});

    // NOTE: possible race condition there
    BOOST_CHECK_EQUAL(link->getState(), LS_CONNECTING);

    cerr << "waiting for connected" << endl;

    // Wait for the link to get connected (give it one second)
    link->waitUntilConnected(1.0);

    BOOST_CHECK_EQUAL(link->getState(), LS_CONNECTED);

    auto subStatus = peer1->subscriptions.getStatus();

    cerr << jsonEncode(subStatus) << endl;

    BOOST_REQUIRE_EQUAL(subStatus.size(), 1);
    BOOST_CHECK_EQUAL(subStatus.at(0).state, LS_CONNECTED);
    BOOST_CHECK_EQUAL(subStatus.at(0).remoteAddress, peer1Address);

    cerr << "connected" << endl;

    peer1->subscriptions.clear();

    cerr << "waiting for disconnected" << endl;

    link->waitUntilDisconnected(1.0);

    cerr << "disconnected" << endl;

#if 0  // temporary; should be re-enabled
    // Now create one to an unknown link channel
    auto link2 = peer1->createLink({"peers", "peer1"},
                                   "subscription2",
                                   string("hello"));

    // possible race condition
    BOOST_CHECK_EQUAL(link2->getState(), LS_CONNECTING);

    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(link2->waitUntilConnected(1.0), std::exception);
    }

    BOOST_CHECK_EQUAL(link2->getState(), LS_ERROR);
#endif

    BOOST_CHECK_EQUAL(peer2->subscriptions.size(), 0);

    auto link3 = peer1->createLink({"peers", "peer2"},
                                   "subscription",
                                   string("hello"));

    // Wait for the link to get connected (give it one second)
    link3->waitUntilConnected(1.0);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    BOOST_CHECK_EQUAL(peer2->subscriptions.size(), 1);

    cerr << jsonEncode(peer2->getRemoteLinksForPeer("peer1")) << endl;
    cerr << jsonEncode(peer2->getLocalLinksForPeer("peer1")) << endl;
    cerr << jsonEncode(peer1->getRemoteLinksForPeer("peer2")) << endl;
    cerr << jsonEncode(peer1->getLocalLinksForPeer("peer2")) << endl;


    cerr << "-------------- reseting ---------------" << endl;
    link3.reset();
    cerr << "-------------- reset done -------------" << endl;

    std::this_thread::sleep_for(std::chrono::seconds(1));

    cerr << "-------------- sleep done -------------" << endl;

    cerr << jsonEncode(peer2->getRemoteLinksForPeer("peer1")) << endl;
    cerr << jsonEncode(peer2->getLocalLinksForPeer("peer1")) << endl;
    cerr << jsonEncode(peer1->getRemoteLinksForPeer("peer2")) << endl;
    cerr << jsonEncode(peer1->getLocalLinksForPeer("peer2")) << endl;

    BOOST_CHECK_EQUAL(peer2->getRemoteLinksForPeer("peer1").size(), 0);
    BOOST_CHECK_EQUAL(peer2->getLocalLinksForPeer("peer1").size(), 0);
    BOOST_CHECK_EQUAL(peer1->getRemoteLinksForPeer("peer2").size(), 0);
    BOOST_CHECK_EQUAL(peer1->getLocalLinksForPeer("peer2").size(), 0);

    BOOST_CHECK_EQUAL(peer2->subscriptions.size(), 0);


    auto link4 = peer1->createLink({"peers", "peer2"},
                                   "subscription",
                                   string("hello"));

    // Wait for the link to get connected (give it one second)
    link4->waitUntilConnected(1.0);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    BOOST_CHECK_EQUAL(peer2->subscriptions.size(), 1);

    cerr << jsonEncode(peer2->getRemoteLinksForPeer("peer1")) << endl;
    cerr << jsonEncode(peer2->getLocalLinksForPeer("peer1")) << endl;
    cerr << jsonEncode(peer1->getRemoteLinksForPeer("peer2")) << endl;
    cerr << jsonEncode(peer1->getLocalLinksForPeer("peer2")) << endl;

    peer2->subscriptions.clear();

    cerr << "waiting for disconnected" << endl;

    link4->waitUntilDisconnected(1.0);

    cerr << "disconnected" << endl;

    std::this_thread::sleep_for(std::chrono::seconds(1));

    cerr << jsonEncode(peer2->getRemoteLinksForPeer("peer1")) << endl;
    cerr << jsonEncode(peer2->getLocalLinksForPeer("peer1")) << endl;
    cerr << jsonEncode(peer1->getRemoteLinksForPeer("peer2")) << endl;
    cerr << jsonEncode(peer1->getLocalLinksForPeer("peer2")) << endl;

    BOOST_CHECK_EQUAL(peer2->getRemoteLinksForPeer("peer1").size(), 0);
    BOOST_CHECK_EQUAL(peer2->getLocalLinksForPeer("peer1").size(), 0);
    BOOST_CHECK_EQUAL(peer1->getRemoteLinksForPeer("peer2").size(), 0);
    BOOST_CHECK_EQUAL(peer1->getLocalLinksForPeer("peer2").size(), 0);



    BOOST_CHECK_EQUAL(peer2->subscriptions.size(), 0);
}

BOOST_AUTO_TEST_CASE(test_peer_link_messages)
{
    signal(SIGPIPE, SIG_IGN);
    signal(SIGSEGV, SIG_IGN);

    string etcdUri = getEtcdUri();
    string etcdPath = getEtcdPath();
    clearEtcd(etcdUri, etcdPath);

    auto peer1 = std::make_shared<TestPeer>("peer1", etcdUri, etcdPath);
    peer1->init(PortRange(15000, 16000), "127.0.0.1");
    peer1->start();

    auto peer2 = std::make_shared<TestPeer>("peer2", etcdUri, etcdPath);
    peer2->init(PortRange(15000, 16000), "127.0.0.1");
    peer2->start();

    WatchT<string> p1w = peer1->watch({"peers", "names:peer2"}, true /* catchUp*/);

    BOOST_CHECK_EQUAL(p1w.wait(1.0), "+peer2");

    WatchT<string> p2w = peer2->watch({"peers", "names:peer1"}, true /* catchUp*/);

    BOOST_CHECK_EQUAL(p2w.wait(1.0), "+peer1");

    // The two peers have discovered each other

    cerr << "------------ creating link" << endl;
    auto link = peer1->createLink({"peers", "peer2"},
                                  "subscription",
                                  string("hello"));

    // Wait for the link to get connected (give it one second)
    link->waitUntilConnected(1.0);
    cerr << "------------ link is connected" << endl;
    
    BOOST_CHECK_EQUAL(link->getState(), LS_CONNECTED);

    cerr << jsonEncode(peer2->subscriptions.getStatus()) << endl;

    // Send a message back from the accepted link to the linker
    WatchT<Any> onRecvBwd = link->onRecv();

    auto onSubBack = [&] (LinkToken & token)
        {
            token.send(string("world"));
        };

    peer2->subscriptions.forEach(onSubBack);

    Any msgBack = onRecvBwd.wait(1.0);

    BOOST_CHECK_EQUAL(msgBack.as<std::string>(), "world");

    // Watch for a message coming through
    WatchT<Any> onRecv;

    auto onSub = [&] (LinkToken & token)
        {
            BOOST_CHECK(!onRecv.attached());
            onRecv = token.onRecv(); 
        };
    
    peer2->subscriptions.forEach(onSub);
    
    BOOST_CHECK(onRecv.attached());

    // Send a message through
    link->send(string("hello"));

    // Wait to receive it
    Any msg = onRecv.wait(1.0);

    BOOST_CHECK_EQUAL(msg.as<std::string>(), "hello");

    auto lst = link->getStatus();
    BOOST_CHECK_EQUAL(lst.messagesReceived, 1);
    BOOST_CHECK_EQUAL(lst.messagesSent, 1);

    auto sst = peer2->subscriptions.getStatus();
    BOOST_CHECK_EQUAL(sst.at(0).messagesReceived, 1);
    BOOST_CHECK_EQUAL(sst.at(0).messagesSent, 1);
}
