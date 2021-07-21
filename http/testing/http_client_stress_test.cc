/* http_client_test.cc
   Wolfgang Sourdeau, January 2014
   This file is part of MLDB. Copyright 2014-20176 mldb.ai inc.
   All rights reserved.

   Test for HttpClient
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <chrono>
#include <memory>
#include <iostream>
#include <ostream>
#include <string>
#include <tuple>
#include <thread>
#include <boost/test/unit_test.hpp>

#include "http_client_test_common.h"

#include "mldb/ext/jsoncpp/value.h"
#include "mldb/ext/jsoncpp/reader.h"
#include "mldb/arch/wait_on_address.h"
#include "mldb/utils/testing/watchdog.h"
#include "mldb/io/asio_thread_pool.h"
#include "mldb/io/event_loop.h"
#include "mldb/io/legacy_event_loop.h"
#include "mldb/http/http_client.h"
#include "mldb/http/testing/test_http_services.h"
#include "mldb/utils/testing/print_utils.h"

using namespace std;
using namespace MLDB;


/* helpers functions used in tests */
namespace {

}

/* Ensures that all requests are correctly performed under load, including
   when "Connection: close" is encountered once in a while.
   Not a performance test. */
BOOST_AUTO_TEST_CASE( test_http_client_stress_test )
{
    cerr << "stress_test\n";
    // const int mask = 0x3ff; /* mask to use for displaying counts */
    MLDB::Watchdog watchdog(300);
    auto doStressTest = [&] (int numParallel) {
        cerr << ("stress test with "
                 + to_string(numParallel) + " parallel connections\n");

        EventLoop eventLoop;
        AsioThreadPool threadPool(eventLoop);

        TestHttpGetService service(eventLoop);
        string baseUrl = service.start();
        LegacyEventLoop legacyLoop;
        legacyLoop.start();

        HttpClient client(legacyLoop, baseUrl, numParallel);
        int maxReqs(30000), numReqs(0), missedReqs(0);
        std::atomic<int> numResponses(0);

        auto onDone = [&] (const HttpRequest & rq,
                           HttpClientError errorCode, int status,
                           string && headers, string && body) {
            numResponses++;

            BOOST_CHECK_EQUAL(errorCode, HttpClientError::None);
            BOOST_CHECK_EQUAL(status, 200);

            if (numResponses == numReqs) {
                MLDB::wake_by_address(numResponses);
            }
        };

        while (numReqs < maxReqs) {
            const char * url = "/counter";
            auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);
            if (client.get(url, cbs)) {
                numReqs++;
                // if ((numReqs & mask) == 0 || numReqs == maxReqs) {
                //     cerr << "performed " + to_string(numReqs) + " requests\n";
                // }
            }
            else {
                missedReqs++;
            }
        }

        cerr << "all requests performed, awaiting responses...\n";
        while (numResponses < maxReqs) {
            int old(numResponses);
            MLDB::wait_on_address(numResponses, old);
        }
        cerr << ("performed " + to_string(maxReqs)
                 + " requests; missed: " + to_string(missedReqs)
                 + "\n");

        threadPool.shutdown();
    };

    doStressTest(1);
    doStressTest(8);
    doStressTest(50);
}
