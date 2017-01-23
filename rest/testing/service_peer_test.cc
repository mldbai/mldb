// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* service_peer_test.cc
   Jeremy Barnes, 4 March 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "mldb/rest/service_peer.h"
#include "temporary_etcd_server.h"
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

#if 1
BOOST_AUTO_TEST_CASE( test_two_members )
{
    string etcdUri = getEtcdUri();
    string etcdPath = getEtcdPath();

    for (unsigned i = 0;  i < 20;  ++i) {
        cerr << endl << endl << endl << "------------------------"
             << "Iteration " << i << endl;
        
        clearEtcd(etcdUri, etcdPath);

        auto peer1 = std::make_shared<TestPeer>("peer1", etcdUri, etcdPath);
        peer1->init(PortRange(15000, 16000), "127.0.0.1");
        peer1->start();

#if 1

        // Test that we get an error when sending to a peer that doesn't exist

        std::timed_mutex responseMutex;
        PeerMessage::State responseState;
        std::string responseError;
        std::string response;
        responseMutex.lock();

        bool gotResponse = false, gotError = false;
        auto onResponse = [&] (PeerMessage && msg,
                               std::vector<std::string> && resp)
            {
                responseState = msg.state;
                gotResponse = true;
                responseMutex.unlock();
                response = resp.at(0);
            };

        auto onError = [&] (PeerMessage && msg)
            {
                responseState = msg.state;
                gotError = true;
                responseError = msg.error;
                responseMutex.unlock();
            };

        peer1->sendPeerMessage("peerThatDoesntExist",
                               PRI_NORMAL,
                               Date::now().plusSeconds(0.01),
                               1, 1, {}, onResponse, onError);

        BOOST_CHECK(responseMutex.try_lock_for(std::chrono::milliseconds(1000)));

        BOOST_CHECK(gotError);
        BOOST_CHECK(!gotResponse);
        BOOST_CHECK_EQUAL(responseState, PeerMessage::SEND_ERROR);
        BOOST_CHECK_EQUAL(responseError, "Unknown peer");

        gotError = false;
        gotResponse = false;
        responseState = PeerMessage::NONE;
        responseError = "";

        // Wait until our peer discovers itself
        for (unsigned i = 0;  i <= 10 && peer1->knownPeers().empty();  ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        BOOST_REQUIRE_GT(peer1->knownPeers().size(), 0);

        // Check a timeout when deadline is in the past
        peer1->sendPeerMessage("peer1",
                               PRI_NORMAL,
                               Date::now().plusSeconds(-0.01),
                               1, 1, {}, onResponse, onError);
    
        BOOST_CHECK(responseMutex.try_lock_for(std::chrono::milliseconds(1000)));

        BOOST_CHECK(gotError);
        BOOST_CHECK(!gotResponse);
        BOOST_CHECK_EQUAL(responseState, PeerMessage::TIMEOUT_SEND);
        BOOST_CHECK_EQUAL(responseError, "Timeout before sending");

        // Ensure a timeout because we don't give enough time to send and make
        // sure it's handled
        gotError = false;
        gotResponse = false;
        responseState = PeerMessage::NONE;
        responseError = "";

        // Check a timeout when deadline is in the past
        peer1->sendPeerMessage("peer1",
                               PRI_NORMAL,
                               Date::now().plusSeconds(0.000001),
                               1, 1, {}, onResponse, onError);
    
        BOOST_CHECK(responseMutex.try_lock_for(std::chrono::milliseconds(1000)));

        BOOST_CHECK(gotError);
        BOOST_CHECK(!gotResponse);
        BOOST_CHECK_EQUAL(responseState, PeerMessage::TIMEOUT_SEND);
        BOOST_CHECK_EQUAL(responseError, "Timeout before sending");


        // Check a timeout when calling something that takes too long to respond
        gotError = false;
        gotResponse = false;
        responseState = PeerMessage::NONE;
        responseError = "";

        peer1->sendPeerMessage("peer1",
                               PRI_NORMAL,
                               Date::now().plusSeconds(0.01),
                               2, 1, {}, onResponse, onError);
    
        BOOST_CHECK(responseMutex.try_lock_for(std::chrono::milliseconds(1000)));

        BOOST_CHECK(gotError);
        BOOST_CHECK(!gotResponse);
        BOOST_CHECK_EQUAL(responseState, PeerMessage::TIMEOUT_RESPONSE);
        BOOST_CHECK_EQUAL(responseError, "Timeout waiting for response");


        // Check a timeout when calling something that takes too long to respond
        // but does respond eventually
        gotError = false;
        gotResponse = false;
        responseState = PeerMessage::NONE;
        responseError = "";

        peer1->sendPeerMessage("peer1",
                               PRI_NORMAL,
                               Date::now().plusSeconds(0.01),
                               2, 2 /* sleep then respond */,
                               {}, onResponse, onError);
    
        BOOST_CHECK(responseMutex.try_lock_for(std::chrono::milliseconds(1000)));

        BOOST_CHECK(gotError);
        BOOST_CHECK(!gotResponse);
        BOOST_CHECK_EQUAL(responseState, PeerMessage::TIMEOUT_RESPONSE);
        BOOST_CHECK_EQUAL(responseError, "Timeout waiting for response");


        // Check a timeout when calling something that takes too long to respond
        // but does respond eventually
        gotError = false;
        gotResponse = false;
        responseState = PeerMessage::NONE;
        responseError = "";

        peer1->sendPeerMessage("peer1",
                               PRI_NORMAL,
                               Date::now().plusSeconds(0.01),
                               2, 2 /* sleep then respond */,
                               {}, onResponse, onError);
    
        BOOST_CHECK(responseMutex.try_lock_for(std::chrono::milliseconds(1000)));

        BOOST_CHECK(gotError);
        BOOST_CHECK(!gotResponse);
        BOOST_CHECK_EQUAL(responseState, PeerMessage::TIMEOUT_RESPONSE);
        BOOST_CHECK_EQUAL(responseError, "Timeout waiting for response");


        // Check we can get a valid response
        cerr << "------------- valid response " << Date::now().print(6) << endl;
        gotError = false;
        gotResponse = false;
        responseState = PeerMessage::NONE;
        responseError = "";

        peer1->sendPeerMessage("peer1",
                               PRI_NORMAL,
                               Date::now().plusSeconds(0.5),
                               2, 2 /* sleep then respond */,
                               {}, onResponse, onError);
    
        BOOST_CHECK(responseMutex.try_lock_for(std::chrono::milliseconds(1000)));

        BOOST_CHECK(!gotError);
        BOOST_CHECK(gotResponse);
        BOOST_CHECK_EQUAL(responseState, PeerMessage::RESPONDED);
        BOOST_CHECK_EQUAL(responseError, "");
        BOOST_CHECK_EQUAL(response, "true");


        auto status = peer1->getPeerStatus("peer1");
        cerr << jsonEncode(status) << endl;

        BOOST_CHECK_EQUAL(status.messagesAwaitingResponse, 0);
        BOOST_CHECK_EQUAL(status.messagesWithDeadline, 0);

#endif
        peer1->shutdown();
    }
}

#endif
