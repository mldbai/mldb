// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* endpoint_test.cc
   Jeremy Barnes, 31 January 2011
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.

   Tests for the endpoints.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/http/http_endpoint.h"
#include "mldb/soa/service/active_endpoint.h"
#include "mldb/soa/service/passive_endpoint.h"
#include <sys/socket.h>
#include "mldb/jml/utils/guard.h"
#include "mldb/arch/exception_handler.h"
#include "mldb/utils/testing/watchdog.h"
#include "mldb/jml/utils/testing/fd_exhauster.h"
#include "test_connection_error.h"
#include "mldb/arch/semaphore.h"
#include "ping_pong.h"
#include <poll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>


using namespace std;
using namespace ML;
using namespace MLDB;


void runAcceptThread(int & port, ML::Semaphore & started, bool & finished)
{
    int backlog = 1;
    
    int s = socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(s > 0);

    for (port = 9666;  port < 9777;  ++port) {
        struct sockaddr_in addr = { AF_INET, htons(port), { INADDR_ANY } }; 
        int b = ::bind(s, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
        if (b == -1 && errno == EADDRINUSE) continue;
        if (b == 0) break;
        throw Exception("runAcceptThread: bind returned %s",
                        strerror(errno));
    }
    
    if (port == 9777)
        throw Exception("couldn't bind to any port");

    int r = listen(s, backlog);

    if (r == -1)
        throw Exception("error on listen: %s", strerror(errno));

    started.release();

    pollfd fds[1] = { { s, POLLIN, 0 } };

    while (!finished) {
        int res = poll(fds, 1, 10);
        //cerr << "poll: res = " << res << endl;
        if (res == 0) continue;
        if (res == -1)
            throw Exception("error on poll: %s", strerror(errno));

        res = accept(s, 0, 0);
        
        //cerr << "accept returned " << res << endl;
        
        if (res == -1)
            throw Exception("error on accept: %s", strerror(errno));
    }
}

BOOST_AUTO_TEST_CASE( test_connect_speed )
{
    BOOST_REQUIRE_EQUAL(TransportBase::created, TransportBase::destroyed);
    BOOST_REQUIRE_EQUAL(ConnectionHandler::created,
                        ConnectionHandler::destroyed);

    //Watchdog watchdog(5.0);

    int port = 0;
    ML::Semaphore started(0);
    bool finished = false;

    std::thread thread([&] () { runAcceptThread(port, started, finished); });

    started.acquire();

    cerr << "accept started, port = " << port << endl;

    ActiveEndpointT<SocketTransport> connector("connector");
    int nconnections = 100;

    Date before = Date::now();

    connector.init(port, "localhost", nconnections);

    Date after = Date::now();

    BOOST_CHECK_LT(after.secondsSince(before), 1);

    BOOST_CHECK_EQUAL(connector.numConnections(), nconnections);
    BOOST_CHECK_EQUAL(connector.numActiveConnections(), 0);
    BOOST_CHECK_EQUAL(connector.numInactiveConnections(), nconnections);

    finished = true;

    connector.shutdown();
    
    BOOST_CHECK_EQUAL(TransportBase::created, TransportBase::destroyed);
    BOOST_CHECK_EQUAL(ConnectionHandler::created,
                      ConnectionHandler::destroyed);
}

#if 0
BOOST_AUTO_TEST_CASE( test_ping_pong )
{
    BOOST_REQUIRE_EQUAL(TransportBase::created, TransportBase::destroyed);
    BOOST_REQUIRE_EQUAL(ConnectionHandler::created,
                        ConnectionHandler::destroyed);

    Watchdog watchdog(5.0);

    string connectionError;

    PassiveEndpointT<SocketTransport> acceptor;
    
    acceptor.onMakeNewHandler = [&] ()
        {
            return ML::make_std_sp(new PongConnectionHandler(connectionError));
        };
    
    int port = acceptor.init();

    cerr << "port = " << port << endl;

    BOOST_CHECK_EQUAL(acceptor.numConnections(), 1);

    ActiveEndpointT<SocketTransport> connector;
    int nconnections = 100;

    Date before = Date::now();

    connector.init(port, "localhost", nconnections);

    Date after = Date::now();

    BOOST_CHECK_LT(after.secondsSince(before), 1);

    BOOST_CHECK_EQUAL(acceptor.numConnections(), nconnections + 1);
    BOOST_CHECK_EQUAL(connector.numConnections(), nconnections);
    BOOST_CHECK_EQUAL(connector.numActiveConnections(), 0);
    BOOST_CHECK_EQUAL(connector.numInactiveConnections(), nconnections);

    acceptor.closePeer();

    connector.shutdown();
    acceptor.shutdown();
    
    BOOST_CHECK_EQUAL(TransportBase::created, TransportBase::destroyed);
    BOOST_CHECK_EQUAL(ConnectionHandler::created,
                      ConnectionHandler::destroyed);
}
#endif
