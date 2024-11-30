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



#if 1
BOOST_AUTO_TEST_CASE( test_http_client_get )
{
    cerr << "client_get\n";
    MLDB::Watchdog watchdog(60);
    EventLoop eventLoop;
    AsioThreadPool threadPool(eventLoop);
    TestHttpGetService service(eventLoop);

    service.addResponse("GET", "/coucou", 200, "coucou");
    string baseUrl = service.start();

    LegacyEventLoop legacyLoop;
    legacyLoop.start();

#if 0
    /* request to bad ip
       Note: if the ip resolution timeout is very high on the router, the
       Watchdog timeout might trigger first */
    {
        cerr << "request to bad ip\n";
        string baseUrl("http://123.234.12.23");
        auto resp = doGetRequest(legacyLoop, baseUrl, "/");
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::CouldNotConnect);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }
#endif

#if 0
    /* request to bad hostname
       Note: will fail when the name service returns a "default" value for all
       non resolved hosts */
    {
        cerr << "request to bad hostname\n";
        string baseUrl("http://somewhere.lost");
        auto resp = doGetRequest(legacyLoop, baseUrl, "/");
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::HostNotFound);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }
#endif

    /* request with timeout */
    {
        cerr << "request with timeout\n";
        auto resp = doGetRequest(legacyLoop, baseUrl, "/timeout", {}, {}, 1);
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::Timeout);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }

    /* request connection close */
    {
        cerr << "testing behaviour with connection: close\n";
        auto resp = doGetRequest(legacyLoop, baseUrl, "/connection-close");
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::None);
        BOOST_CHECK_EQUAL(get<1>(resp), 204);
    }

    /* request to /nothing -> 404 */
    {
        cerr << "request with 404\n";
        auto resp = doGetRequest(legacyLoop, baseUrl, "/nothing");
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::None);
        BOOST_CHECK_EQUAL(get<1>(resp), 404);
    }

    /* request to /coucou -> 200 + "coucou" */
    {
        cerr << "request with 200\n";
        auto resp = doGetRequest(legacyLoop, baseUrl, "/coucou");
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::None);
        BOOST_CHECK_EQUAL(get<1>(resp), 200);
        BOOST_CHECK_EQUAL(get<2>(resp), "coucou");
    }

    /* headers and cookies */
    {
        auto resp = doGetRequest(legacyLoop, baseUrl, "/headers", {},
                                 {{"someheader", "somevalue"}});
        // Json::Value expBody;
        // expBody["accept"] = "*/*";
        // expBody["host"] = baseUrl.substr(7);
        // expBody["someheader"] = "somevalue";
        // Json::Value jsonBody = Json::parse(get<2>(resp));
        // BOOST_CHECK_EQUAL(jsonBody, expBody);
    }

    /* query-params */
    {
        auto resp = doGetRequest(legacyLoop, baseUrl, "/query-params",
                                 {{"value", "hello"}});
        string body = get<2>(resp);
        BOOST_CHECK_EQUAL(body, "?value=hello");
    }

    threadPool.shutdown();
}
#endif

#if 1
BOOST_AUTO_TEST_CASE( test_http_client_post )
{
    cerr << "client_post\n";
    MLDB::Watchdog watchdog(10);
    EventLoop eventLoop;
    AsioThreadPool threadPool(eventLoop);
    TestHttpUploadService service(eventLoop);
    string baseUrl = service.start();

    LegacyEventLoop legacyLoop;
    legacyLoop.start();

    /* request to /coucou -> 200 + "coucou" */
    {
        auto resp = doUploadRequest(legacyLoop, false, baseUrl, "/post-test",
                                    "post body", "application/x-nothing");
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::None);
        BOOST_CHECK_EQUAL(get<1>(resp), 200);
        // Json::Value jsonBody = Json::parse(get<2>(resp));
        // BOOST_CHECK_EQUAL(jsonBody["verb"], "POST");
        // BOOST_CHECK_EQUAL(jsonBody["payload"], "post body");
        // BOOST_CHECK_EQUAL(jsonBody["type"], "application/x-nothing");
    }

    threadPool.shutdown();
}
#endif

#if 1
BOOST_AUTO_TEST_CASE( test_http_client_put )
{
    cerr << "client_put\n";
    MLDB::Watchdog watchdog(10);
    EventLoop eventLoop;
    AsioThreadPool threadPool(eventLoop);
    TestHttpUploadService service(eventLoop);
    string baseUrl = service.start();

    LegacyEventLoop legacyLoop;
    legacyLoop.start();

    string bigBody;
    for (int i = 0; i < 65535; i++) {
        bigBody += "this is one big body,";
    }
    auto resp = doUploadRequest(legacyLoop, true, baseUrl, "/put-test",
                                bigBody, "application/x-nothing");
    BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::None);
    BOOST_CHECK_EQUAL(get<1>(resp), 200);
    // Json::Value jsonBody = Json::parse(get<2>(resp));
    // BOOST_CHECK_EQUAL(jsonBody["verb"], "PUT");
    // BOOST_CHECK_EQUAL(jsonBody["payload"], bigBody);
    // BOOST_CHECK_EQUAL(jsonBody["type"], "application/x-nothing");

    threadPool.shutdown();
}
#endif

#if 1
BOOST_AUTO_TEST_CASE( http_test_client_delete )
{
    cerr << "client_delete\n";
    MLDB::Watchdog watchdog(10);

    EventLoop eventLoop;
    AsioThreadPool threadPool(eventLoop);
    TestHttpGetService service(eventLoop);

    service.addResponse("DELETE", "/deleteMe", 200, "Deleted");
    string baseUrl = service.start();

    LegacyEventLoop legacyLoop;
    legacyLoop.start();

    auto resp = doDeleteRequest(legacyLoop, baseUrl, "/deleteMe", {}, {}, 1);

    BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::None);
    BOOST_CHECK_EQUAL(get<1>(resp), 200);

    threadPool.shutdown();
}
#endif

#if 1
BOOST_AUTO_TEST_CASE( test_http_client_put_multi )
{
    cerr << "client_put_multi\n";
    EventLoop eventLoop;
    AsioThreadPool threadPool(eventLoop);
    TestHttpUploadService service(eventLoop);
    string baseUrl = service.start();

    LegacyEventLoop legacyLoop;
    legacyLoop.start();

    HttpClient client(legacyLoop, baseUrl);
    size_t maxRequests(50);  // can't be too high as on OSX we only have 256 fds
    std::atomic<int> done(0);

    auto makeBody = [&] (size_t i) {
        int multiplier = (i < maxRequests / 2) ? -2 : 2;
        size_t bodySize = 2000 + multiplier * i;
        string body = MLDB::format("%.4x", bodySize);
        size_t rndSize = bodySize - body.size();
        body += randomString(rndSize);

        return body;
    };

    for (size_t i = 0; i < maxRequests; i++) {
        auto sendBody = makeBody(i);
        auto onResponse = [&, sendBody] (const HttpRequest & rq,
                                         HttpClientError error,
                                         int status,
                                         string && headers,
                                         string && body) {
            BOOST_CHECK_EQUAL(error, HttpClientError::None);
            BOOST_CHECK_EQUAL(status, 200);
            // Json::Value jsonBody = Json::parse(body);
            // BOOST_CHECK_EQUAL(jsonBody["verb"], "PUT");
            // BOOST_CHECK_EQUAL(jsonBody["payload"], sendBody);
            // BOOST_CHECK_EQUAL(jsonBody["type"], "text/plain");
            done++;
            if (done == maxRequests) {
                MLDB::wake_by_address(done);
            }
        };

        auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);
        HttpRequestContent content(sendBody, "text/plain");
        while (!client.put("/", cbs, content)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    };

    while (done < maxRequests) {
        int oldDone = done;
        MLDB::wait_on_address(done, oldDone);
    }

    threadPool.shutdown();
}
#endif

#if 1
/* Ensure that the move constructor and assignment operator behave
   reasonably well. */
BOOST_AUTO_TEST_CASE( test_http_client_move_constructor )
{
    cerr << "move_constructor\n";
    MLDB::Watchdog watchdog(30);
    EventLoop eventLoop;
    AsioThreadPool threadPool(eventLoop);

    TestHttpGetService service(eventLoop);
    service.addResponse("GET", "/", 200, "coucou");
    string baseUrl = service.start();

    LegacyEventLoop legacyLoop;
    legacyLoop.start();

    auto doGet = [&] (HttpClient & getClient) {
        std::atomic<int> done(false);

        auto onDone = [&] (const HttpRequest & rq,
                           HttpClientError errorCode, int status,
                           string && headers, string && body) {
            done = true;
            MLDB::wake_by_address(done);
        };
        auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);

        getClient.get("/", cbs);
        while (!done) {
            int old = done;
            MLDB::wait_on_address(done, old);
        }
    };

    /* move constructor */
    cerr << "testing move constructor\n";
    auto makeClient = [&] () {
        return HttpClient(legacyLoop, baseUrl, 1);
    };
    HttpClient client1(makeClient());
    doGet(client1);

    /* move assignment operator */
    cerr << "testing move assignment op.\n";
    HttpClient client2(legacyLoop, "http://nowhere", 1);
    client2 = std::move(client1);
    doGet(client2);

    threadPool.shutdown();
}
#endif

#if 1
/* Ensure that an infinite number of requests can be queued when queue size is
 * 0, even from within callbacks. */
BOOST_AUTO_TEST_CASE( test_http_client_unlimited_queue )
{
    static const int maxLevel(4);

    MLDB::Watchdog watchdog(30);
    EventLoop eventLoop;
    AsioThreadPool threadPool(eventLoop);

    TestHttpGetService service(eventLoop);
    service.addResponse("GET", "/", 200, "coucou");
    string baseUrl = service.start();

    LegacyEventLoop legacyLoop;
    legacyLoop.start();

    HttpClient client(legacyLoop, baseUrl, 4, 0);

    atomic<int> pending(0);
    std::atomic<int> done(0);

    function<void(int)> doGet = [&] (int level) {
        pending++;
        auto onDone = [&,level] (const HttpRequest & rq,
                                 HttpClientError errorCode, int status,
                                 string && headers, string && body) {
            if (level < maxLevel) {
                for (int i = 0; i < 10; i++) {
                    doGet(level+1);
                }
            }
            pending--;
            done++;
        };
        auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);
        client.get("/", cbs);
    };

    doGet(0);

    while (pending > 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        cerr << "requests done: " + to_string(done) + "\n";
    }

    threadPool.shutdown();
}
#endif

#if 1
/* Test connection restoration after a timeout occurs. */
BOOST_AUTO_TEST_CASE( test_http_client_connection_timeout )
{
    MLDB::Watchdog watchdog(30);
    EventLoop eventLoop;
    AsioThreadPool threadPool(eventLoop);

    TestHttpGetService service(eventLoop);
    service.addResponse("GET", "/", 200, "coucou");
    string baseUrl = service.start();

    LegacyEventLoop legacyLoop;
    legacyLoop.start();

    HttpClient client(legacyLoop, baseUrl, 1);
    client.enableDebug(true);

    std::atomic<int> done(0);
    auto onDone = [&] (const HttpRequest & rq,
                       HttpClientError errorCode, int status,
                       string && headers, string && body) {
        done++;
        MLDB::wake_by_address(done);
    };
    auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);
    client.get("/timeout", cbs, {}, {}, 1);
    client.get("/", cbs, {}, {}, 1);

    while (done < 2) {
        MLDB::wait_on_address(done, done);
    }

    threadPool.shutdown();
}
#endif

#if 1
/* Test connection restoration after the server closes the connection, under
 * various circumstances. */
BOOST_AUTO_TEST_CASE( test_http_client_connection_closed )
{
    MLDB::Watchdog watchdog(30);
    EventLoop eventLoop;
    AsioThreadPool threadPool(eventLoop);

    TestHttpGetService service(eventLoop);
    service.addResponse("GET", "/", 200, "coucou");
    string baseUrl = service.start();

    LegacyEventLoop legacyLoop;
    legacyLoop.start();

    /* response sent, "Connection: close" header */
    {
        cerr << "* connection-close\n";
        HttpClient client(legacyLoop, baseUrl, 1);
        std::atomic<int> done(0);
        auto onDone = [&] (const HttpRequest & rq,
                           HttpClientError errorCode, int status,
                           string && headers, string && body) {
            done++;
            MLDB::wake_by_address(done);
        };
        auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);
        client.get("/connection-close", cbs);
        client.get("/", cbs);

        while (done < 2) {
            MLDB::wait_on_address(done, done);
        }
    }

    /* Response sent, no "Connection: close" header. Performed multiple times
     * to ensure that extranenous close events are ignored appropriately when
     * the response was received and the actual connection released.
     * Unstable test due to the very specific condition during which the error
     * occurs: no request must be queued when the request is done processing
     * but one must be present when the closing of the connection is being
     * handled. */
    cerr << "* no connection-close\n";
    for (int i = 0; i < 10; i++)
    {
        HttpClient client(legacyLoop, baseUrl, 1);
        atomic<int> done(0);
        auto onDone = [&] (const HttpRequest & rq,
                           HttpClientError errorCode, int status,
                           string && headers, string && body) {
            done++;
            MLDB::wake_by_address(done);
        };
        auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);
        for (size_t i = 0; i < 3; i++) {
            client.post("/quiet-connection-close", cbs, string("no data"));
            while (done < i) {
                MLDB::wait_on_address(done, done);
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    /* response not sent */
    {
        cerr << "* no response at all\n";
        HttpClient client(legacyLoop, baseUrl, 1);
        std::atomic<int> done(0);
        auto onDone = [&] (const HttpRequest & rq,
                           HttpClientError errorCode, int status,
                           string && headers, string && body) {
            done++;
            MLDB::wake_by_address(done);
        };
        auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);
        client.get("/abrupt-connection-close", cbs);
        client.get("/", cbs);

        while (done < 2) {
            MLDB::wait_on_address(done, done);
        }
    }

    threadPool.shutdown();
}
#endif
