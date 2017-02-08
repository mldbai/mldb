// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* json_service_endpoint_test.cc
   Jeremy Barnes, 9 November 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Test for the JSON service endpoint.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <mutex>
#include <boost/test/unit_test.hpp>

#include "mldb/rest/rest_service_endpoint.h"
#include "mldb/http/http_rest_proxy.h"
#include <sys/socket.h>
#include "mldb/jml/utils/guard.h"
#include "mldb/arch/exception_handler.h"
#include "mldb/utils/testing/watchdog.h"
#include "mldb/jml/utils/testing/fd_exhauster.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/arch/timers.h"
#include <thread>


using namespace std;
using namespace ML;
using namespace MLDB;


/*****************************************************************************/
/* ECHO SERVICE                                                              */
/*****************************************************************************/

/** Simple test service that listens on zeromq and simply echos everything
    that it gets back.
*/

struct EchoService : public RestServiceEndpoint {

    EchoService()
    {
        RestServiceEndpoint::init();
    }

    ~EchoService()
    {
        shutdown();
    }

    virtual void handleRequest(ConnectionId & connection,
                               const RestRequest & request) const
    {
        //cerr << "handling request " << request << endl;
        if (request.verb != "POST")
            throw MLDB::Exception("echo service needs POST");
        if (request.resource != "/echo")
            throw MLDB::Exception("echo service only responds to /echo");
        connection.sendResponse(200, request.payload, "text/plain");
    }
};

BOOST_AUTO_TEST_CASE( test_named_endpoint )
{
    int totalPings = 1000;

    EchoService service;
    auto addr = service.bindTcp();
    cerr << "echo service is listening on " << addr << endl;

    volatile int numPings = 0;
    std::mutex checkLock;

    auto runHttpThread = [&] ()
        {
            HttpRestProxy proxy(addr);

            while (numPings < totalPings) {
                int i = __sync_add_and_fetch(&numPings, 1);

                if (i && i % 1000 == 0)
                    cerr << i << endl;

                auto response = proxy.post("/echo", to_string(i));
                std::unique_lock<std::mutex> checkGuard(checkLock);
                BOOST_CHECK_EQUAL(response.code(), 200);
                BOOST_CHECK_EQUAL(response.body(), to_string(i));
            }
        };

    std::vector<std::thread> threads;
    unsigned int numThread = 5;
    
    for (unsigned i = 0;  i < numThread;  ++i) {
       threads.emplace_back(runHttpThread);
    }

    for (auto & t: threads)
        t.join();

    cerr << "finished requests" << endl;

    service.shutdown();
}
