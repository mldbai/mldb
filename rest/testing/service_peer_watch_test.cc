// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* service_peer_watch_test.cc
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
#include <chrono>
#include <thread>
#include "test_peer.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;
using namespace MLDB;

#if 0
BOOST_AUTO_TEST_CASE(test_peer_generic_watches)
{
    signal(SIGPIPE, SIG_IGN);

    string etcdUri = getEtcdUri();
    clearEtcd(etcdUri);

    auto peer1 = std::make_shared<TestPeer>("peer1", etcdUri);
    peer1->init(PortRange(15000, 16000), "127.0.0.1");
    peer1->start();

    BOOST_CHECK_EQUAL(peer1->knownPeers(),
                      vector<string>({"peer1"}));

    // Check we can get a generic watch on all peers
    WatchT<RestEntityChildEvent> watch
        = peer1->watch( { "peers", "*" },
                        true /* catchup */, string("testWatch"));

    BOOST_CHECK_EQUAL(watch.attached(), true);
    BOOST_REQUIRE(watch.any());

    RestEntityChildEvent ev = watch.pop();
    
    BOOST_CHECK_EQUAL(ev.name, "peer1");
    BOOST_CHECK_EQUAL(ev.event, CE_NEW);
}
#endif

#if 0 // needs rework
BOOST_AUTO_TEST_CASE(test_peer_watches)
{
    signal(SIGPIPE, SIG_IGN);

    string etcdUri = getEtcdUri();
    clearEtcd(etcdUri);

    auto peer1 = std::make_shared<TestPeer>("peer1", etcdUri);
    peer1->init(PortRange(15000, 16000), "127.0.0.1");
    peer1->start();

    auto watch = peer1->watch({"peers", "*"}, true /* catchup */, nullptr);
    BOOST_CHECK_EQUAL(watch.attached(), true);
    
    std::vector<std::pair<std::string, CollectionEvent> > events;

    auto onElementChange = [&] (const std::string & peerName,
                                CollectionEvent event)
        {
            cerr << "got element change: peer " << peerName
                 << " event " << jsonEncode(event) << endl;

            events.push_back(make_pair(peerName, event));
        };
    
    watch.bind(onElementChange);

    // There should be one peer, peer1
    BOOST_REQUIRE_EQUAL(events.size(), 1);
    BOOST_CHECK_EQUAL(events.at(0).first, "peer1");
    BOOST_CHECK_EQUAL(events.at(0).second, CE_NEW);

    watch.unbind();

    auto peer2 = std::make_shared<TestPeer>("peer2", etcdUri);
    peer2->init(PortRange(15000, 16000), "127.0.0.1");
    peer2->start();

    // Wait for it to come up for 2 seconds
    std::string peerName;
    CollectionEvent ev;

    std::tie(peerName, ev) = watch.wait(2.0);

    BOOST_CHECK_EQUAL(peerName, "peer2");
    BOOST_CHECK_EQUAL(ev, CE_NEW);

    cerr << "peer2 is up" << endl;

    peer2->shutdown();

    // Wait to get the shutdown message
    std::tie(peerName, ev) = watch.wait(2.0);

    BOOST_CHECK_EQUAL(peerName, "peer2");
    BOOST_CHECK_EQUAL(ev, CE_DELETED);

    peer1->shutdown();
    peer1.reset();

    // Make sure our watch told us it was deleted
    BOOST_CHECK_EQUAL(watch.attached(), false);
}
#endif

#if 1
BOOST_AUTO_TEST_CASE(test_peer_remote_watches_simple)
{
    signal(SIGPIPE, SIG_IGN);

    string etcdUri = getEtcdUri();
    string etcdPath = getEtcdPath();
    clearEtcd(etcdUri, etcdPath);

    auto peer1 = std::make_shared<TestPeer>("peer1", etcdUri, etcdPath);
    peer1->init(PortRange(15000, 16000), "127.0.0.1");
    peer1->start();

    BOOST_CHECK_EQUAL(peer1->knownPeers(), vector<string>({"peer1"}));

    // Check we can get a generic watch on all peers
    WatchT<std::string> watch
        = peer1->watch( { "peers", "peer1", "peers", "names:*" },
                        true /* catchup */,
                        string("test watch 1"));
    
    BOOST_CHECK_EQUAL(watch.attached(), true);

    cerr << "--------------- detaching" << endl;
    watch.detach();
    cerr << "--------------- detached" << endl;

    BOOST_CHECK(!watch.attached());

    cerr << "--------------- sleeping" << endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    cerr << "--------------- awake" << endl;

    BOOST_CHECK_EQUAL(jsonEncode(peer1->getLocalWatchesForPeer("peer1")),
                      Json::Value({}));
    BOOST_CHECK_EQUAL(jsonEncode(peer1->getRemoteWatchesForPeer("peer1")),
                      Json::Value({}));
    peer1->shutdown();
}
#endif

#if 1
BOOST_AUTO_TEST_CASE(test_peer_remote_watches)
{
    signal(SIGPIPE, SIG_IGN);

    string etcdUri = getEtcdUri();
    string etcdPath = getEtcdPath();
    clearEtcd(etcdUri, etcdPath);

    auto peer1 = std::make_shared<TestPeer>("peer1", etcdUri, etcdPath);
    peer1->init(PortRange(15000, 16000), "127.0.0.1");
    peer1->start();

    BOOST_CHECK_EQUAL(peer1->knownPeers(),
                      vector<string>({"peer1"}));

    // Get a remote watch on the list of peers

    cerr << jsonEncode(peer1->getLocalWatchesForPeer("peer1")) << endl;
    cerr << jsonEncode(peer1->getRemoteWatchesForPeer("peer1")) << endl;

    // Check we can get a generic watch on all peers
    WatchT<std::string> watch
        = peer1->watch( { "peers", "peer1", "peers", "names:*" },
                        true /* catchup */,
                        string("test watch 1"));
    
    BOOST_CHECK_EQUAL(watch.attached(), true);
    //BOOST_REQUIRE(watch.any());

    std::string name = watch.wait();
    
    BOOST_CHECK_EQUAL(name, "+peer1");

    cerr << jsonEncode(peer1->getPeerWatches()) << endl;

    cerr << jsonEncode(peer1->getLocalWatchesForPeer("peer1")) << endl;
    cerr << jsonEncode(peer1->getRemoteWatchesForPeer("peer1")) << endl;

    watch.detach();

    BOOST_CHECK(!watch.attached());

    std::this_thread::sleep_for(std::chrono::seconds(1));

    cerr << jsonEncode(peer1->getPeerWatches()) << endl;

    cerr << jsonEncode(peer1->getLocalWatchesForPeer("peer1")) << endl;
    cerr << jsonEncode(peer1->getRemoteWatchesForPeer("peer1")) << endl;

    WatchT<int> watch2
        = peer1->watch( { "peers", "*", "pulse:*" },
                        true /* catchup */,
                        string("test watch 2"));

    std::this_thread::sleep_for(std::chrono::seconds(1));

    cerr << "watch2 has " << watch2.count() << " events outstanding" << endl;

    peer1->shutdown();


}
#endif
