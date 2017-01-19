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
#include "mldb/arch/semaphore.h"
#include "test_connection_error.h"
#include "ping_pong.h"

using namespace std;
using namespace ML;
using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_ping_pong )
{
    BOOST_REQUIRE_EQUAL(TransportBase::created, TransportBase::destroyed);
    BOOST_REQUIRE_EQUAL(ConnectionHandler::created,
                        ConnectionHandler::destroyed);

    Watchdog watchdog(5.0);

    string connectionError;

    PassiveEndpointT<SocketTransport> acceptor("acceptor");
    
    acceptor.onMakeNewHandler = [&] ()
        {
            return ML::make_std_sp(new PongConnectionHandler(connectionError));
        };
    
    int port = acceptor.init();

    cerr << "port = " << port << endl;

    BOOST_CHECK_EQUAL(acceptor.numConnections(), 0);

    ActiveEndpointT<SocketTransport> connector("connector");
    int nconnections = 1;
    connector.init(port, "localhost", nconnections);

    ML::Semaphore gotConnectionSem(0), finishedTestSem(0);

    auto onNewConnection = [&] (std::shared_ptr<TransportBase> transport)
        {
            transport->associate
            (ML::make_std_sp(new PingConnectionHandler(connectionError,
                                                   finishedTestSem)));
            gotConnectionSem.release();
        };

    auto onConnectionError = [&] (const std::string & error)
        {
            cerr << "onConnectionError " << error << endl;

            connectionError = error;
            gotConnectionSem.release();
        };

    connector.getConnection(onNewConnection,
                            onConnectionError,
                            1.0);
    
    int semWaitRes = gotConnectionSem.acquire(1.0);

    BOOST_CHECK_EQUAL(semWaitRes, 0);
    BOOST_CHECK_EQUAL(connectionError, "");
    

    BOOST_CHECK_EQUAL(acceptor.numConnections(), 1);
    BOOST_CHECK_EQUAL(connector.numConnections(), 1);
    BOOST_CHECK_EQUAL(connector.numActiveConnections(), 1);
    BOOST_CHECK_EQUAL(connector.numInactiveConnections(), 0);

    semWaitRes = finishedTestSem.acquire(5.0);
    
    BOOST_CHECK_EQUAL(semWaitRes, 0);
    BOOST_CHECK_EQUAL(connectionError, "");

    acceptor.closePeer();

    connector.sleepUntilIdle();
    acceptor.sleepUntilIdle();

    connector.shutdown();
    acceptor.shutdown();
    
    BOOST_CHECK_EQUAL(TransportBase::created, TransportBase::destroyed);
    BOOST_CHECK_EQUAL(ConnectionHandler::created,
                      ConnectionHandler::destroyed);
}
