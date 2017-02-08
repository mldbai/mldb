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

using namespace std;
using namespace ML;
using namespace MLDB;


BOOST_AUTO_TEST_CASE( test_active_endpoint_nothing_listening )
{
    Watchdog watchdog(60.0);  // give it a reasonable amount of time

    for (unsigned i = 0;  i < 50;  ++i) {
        BOOST_REQUIRE_EQUAL(TransportBase::created, TransportBase::destroyed);
        BOOST_REQUIRE_EQUAL(ConnectionHandler::created,
                            ConnectionHandler::destroyed);
        Watchdog watchdog;

        cerr << endl << "iter " << i << endl;
        ActiveEndpointT<SocketTransport> connector("connector");
        connector.init(9997, "localhost", 0, 1, true,
                       false /* throw on error */);
        doTestConnectionError(connector, "Connection refused",
                              "Timer expired");

        connector.sleepUntilIdle();
        connector.shutdown();
        
        BOOST_CHECK_EQUAL(connector.numActiveConnections(), 0);
        BOOST_CHECK_EQUAL(connector.numInactiveConnections(), 0);
        BOOST_CHECK_EQUAL(connector.threadsActive(), 0);
        BOOST_CHECK_EQUAL(TransportBase::created, TransportBase::destroyed);
        BOOST_CHECK_EQUAL(ConnectionHandler::created,
                          ConnectionHandler::destroyed);
    }
}

