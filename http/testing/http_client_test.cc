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

#include "mldb/ext/jsoncpp/value.h"
#include "mldb/ext/jsoncpp/reader.h"
#include "mldb/arch/futex.h"
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

typedef tuple<HttpClientError, int, string> ClientResponse;

#define CALL_MEMBER_FN(object, pointer)  (object.*(pointer))

/* sync request helpers */
template<typename Func>
ClientResponse
doRequest(LegacyEventLoop & legacyLoop,
          const string & baseUrl, const string & resource,
          Func func,
          const RestParams & queryParams, const RestParams & headers,
          int timeout = -1)
{
    ClientResponse response;

    HttpClient client(legacyLoop, baseUrl, 4);

    int done(false);
    auto onResponse = [&] (const HttpRequest & rq,
                           HttpClientError error,
                           int status,
                           string && headers,
                           string && body) {
        int & code = get<1>(response);
        code = status;
        string & body_ = get<2>(response);
        body_ = move(body);
        HttpClientError & errorCode = get<0>(response);
        errorCode = error;
        done = true;
        ML::futex_wake(done);
    };
    auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);

    CALL_MEMBER_FN(client, func)(resource, cbs, queryParams, headers,
                                 timeout);

    while (!done) {
        int oldDone = done;
        ML::futex_wait(done, oldDone);
    }

    return response;
}

ClientResponse
doGetRequest(LegacyEventLoop & loop,
             const string & baseUrl, const string & resource,
             const RestParams & queryParams = RestParams(),
             const RestParams & headers = RestParams(),
             int timeout = -1)
{
    return doRequest(loop, baseUrl, resource, &HttpClient::get,
                     queryParams, headers, timeout);
}

ClientResponse
doDeleteRequest(LegacyEventLoop & loop,
                const string & baseUrl, const string & resource,
                const RestParams & queryParams = RestParams(),
                const RestParams & headers = RestParams(),
                int timeout = -1)
{
    return doRequest(loop, baseUrl, resource, &HttpClient::del,
                     queryParams, headers, timeout);
}

ClientResponse
doUploadRequest(LegacyEventLoop & loop,
                bool isPut,
                const string & baseUrl, const string & resource,
                const string & body, const string & type)
{
    ClientResponse response;

    HttpClient client(loop, baseUrl, 4);

    int done(false);
    auto onResponse = [&] (const HttpRequest & rq,
                           HttpClientError error,
                           int status,
                           string && headers,
                           string && body) {
        int & code = get<1>(response);
        code = status;
        string & body_ = get<2>(response);
        body_ = move(body);
        HttpClientError & errorCode = get<0>(response);
        errorCode = error;
        done = true;
        ML::futex_wake(done);
    };

    auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);
    HttpRequestContent content(body, type);
    if (isPut) {
        client.put(resource, cbs, content);
    }
    else {
        client.post(resource, cbs, content);
    }

    while (!done) {
        int oldDone = done;
        ML::futex_wait(done, oldDone);
    }

    return response;
}

}

#if 1
BOOST_AUTO_TEST_CASE( test_http_client_get )
{
    cerr << "client_get\n";
    ML::Watchdog watchdog(1000);
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
    ML::Watchdog watchdog(10);
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
    ML::Watchdog watchdog(10);
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
    ML::Watchdog watchdog(10);

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
    size_t maxRequests(500);
    int done(0);

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
                ML::futex_wake(done);
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
        ML::futex_wait(done, oldDone);
    }

    threadPool.shutdown();
}
#endif

#if 1
/* Ensures that all requests are correctly performed under load, including
   when "Connection: close" is encountered once in a while.
   Not a performance test. */
BOOST_AUTO_TEST_CASE( test_http_client_stress_test )
{
    cerr << "stress_test\n";
    // const int mask = 0x3ff; /* mask to use for displaying counts */
    // ML::Watchdog watchdog(300);
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
        int numResponses(0);

        auto onDone = [&] (const HttpRequest & rq,
                           HttpClientError errorCode, int status,
                           string && headers, string && body) {
            numResponses++;

            BOOST_CHECK_EQUAL(errorCode, HttpClientError::None);
            BOOST_CHECK_EQUAL(status, 200);

            if (numResponses == numReqs) {
                ML::futex_wake(numResponses);
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
            ML::futex_wait(numResponses, old);
        }
        cerr << ("performed " + to_string(maxReqs)
                 + " requests; missed: " + to_string(missedReqs)
                 + "\n");

        threadPool.shutdown();
    };

    doStressTest(1);
    doStressTest(8);
    doStressTest(128);
}
#endif

#if 1
/* Ensure that the move constructor and assignment operator behave
   reasonably well. */
BOOST_AUTO_TEST_CASE( test_http_client_move_constructor )
{
    cerr << "move_constructor\n";
    ML::Watchdog watchdog(30);
    EventLoop eventLoop;
    AsioThreadPool threadPool(eventLoop);

    TestHttpGetService service(eventLoop);
    service.addResponse("GET", "/", 200, "coucou");
    string baseUrl = service.start();

    LegacyEventLoop legacyLoop;
    legacyLoop.start();

    auto doGet = [&] (HttpClient & getClient) {
        int done(false);

        auto onDone = [&] (const HttpRequest & rq,
                           HttpClientError errorCode, int status,
                           string && headers, string && body) {
            done = true;
            ML::futex_wake(done);
        };
        auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);

        getClient.get("/", cbs);
        while (!done) {
            int old = done;
            ML::futex_wait(done, old);
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
    client2 = move(client1);
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

    ML::Watchdog watchdog(30);
    EventLoop eventLoop;
    AsioThreadPool threadPool(eventLoop);

    TestHttpGetService service(eventLoop);
    service.addResponse("GET", "/", 200, "coucou");
    string baseUrl = service.start();

    LegacyEventLoop legacyLoop;
    legacyLoop.start();

    HttpClient client(legacyLoop, baseUrl, 4, 0);

    atomic<int> pending(0);
    int done(0);

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
    ML::Watchdog watchdog(30);
    EventLoop eventLoop;
    AsioThreadPool threadPool(eventLoop);

    TestHttpGetService service(eventLoop);
    service.addResponse("GET", "/", 200, "coucou");
    string baseUrl = service.start();

    LegacyEventLoop legacyLoop;
    legacyLoop.start();

    HttpClient client(legacyLoop, baseUrl, 1);
    client.enableDebug(true);

    int done(0);
    auto onDone = [&] (const HttpRequest & rq,
                       HttpClientError errorCode, int status,
                       string && headers, string && body) {
        done++;
        ML::futex_wake(done);
    };
    auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);
    client.get("/timeout", cbs, {}, {}, 1);
    client.get("/", cbs, {}, {}, 1);

    while (done < 2) {
        ML::futex_wait(done, done);
    }

    threadPool.shutdown();
}
#endif

#if 1
/* Test connection restoration after the server closes the connection, under
 * various circumstances. */
BOOST_AUTO_TEST_CASE( test_http_client_connection_closed )
{
    ML::Watchdog watchdog(30);
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
        int done(0);
        auto onDone = [&] (const HttpRequest & rq,
                           HttpClientError errorCode, int status,
                           string && headers, string && body) {
            done++;
            ML::futex_wake(done);
        };
        auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);
        client.get("/connection-close", cbs);
        client.get("/", cbs);

        while (done < 2) {
            ML::futex_wait(done, done);
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
            ML::futex_wake(done);
        };
        auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);
        for (size_t i = 0; i < 3; i++) {
            client.post("/quiet-connection-close", cbs, string("no data"));
            while (done < i) {
                ML::futex_wait(done, done);
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));    }

    /* response not sent */
    {
        cerr << "* no response at all\n";
        HttpClient client(legacyLoop, baseUrl, 1);
        int done(0);
        auto onDone = [&] (const HttpRequest & rq,
                           HttpClientError errorCode, int status,
                           string && headers, string && body) {
            done++;
            ML::futex_wake(done);
        };
        auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);
        client.get("/abrupt-connection-close", cbs);
        client.get("/", cbs);

        while (done < 2) {
            ML::futex_wait(done, done);
        }
    }

    threadPool.shutdown();
}
#endif
