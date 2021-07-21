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
#include "mldb/arch/wait_on_address.h"
#include "mldb/utils/testing/watchdog.h"
#include "mldb/io/asio_thread_pool.h"
#include "mldb/io/event_loop.h"
#include "mldb/io/legacy_event_loop.h"
#include "mldb/io/message_loop.h"
#include "mldb/http/http_client_impl_v1.h"
#include "mldb/http/http_client.h"
#include "mldb/http/testing/test_http_services.h"
#include "mldb/utils/testing/print_utils.h"

using namespace std;
using namespace MLDB;

#if 1
BOOST_AUTO_TEST_CASE( test_http_client_get_minimal )
{
    string baseUrl = "http://127.0.0.1/";

    MessageLoop messageLoop(1, 0, -1);

    cerr << "started legacy loop" << endl << endl;

    auto client = std::make_shared<HttpClientImplV1>(baseUrl, 1024, 0 /* queue size */);

    // Give it a chance to initialize before we initialize the get
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    cerr << "starting message loop" << endl << endl;

    messageLoop.start();

    // Give it a chance to initialize before we initialize the get
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    cerr << "adding client to message loop" << endl << endl;

    //TimerFD tfd(TIMER_REALTIME); int fd = tfd.fd(); // no problem adding, either in thread or not
    //int fd = STDIN_FILENO;  // no problem adding, either in thread or not
    int fd = client->selectFd(); // crashes, in thread or not

#if 1
    messageLoop.addFd(fd, EPOLL_INPUT, client.get());
#else
    auto runThread = [&] ()
    {
        messageLoop.addFd(fd, EPOLL_INPUT, client.get());
    };
    std::thread thr(runThread);
    thr.join();
#endif

    // Give it a chance to initialize before we initialize the get
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    cerr << "initialized client" << endl << endl;

    //messageLoop.start();

    // Give it a chance to initialize before we initialize the get
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
#if 0
    ClientResponse response;

    std::atomic<int> done(false);
    auto onResponse = [&] (const HttpRequest & rq,
                           HttpClientError error,
                           int status,
                           string && headers,
                           string && body) {
        cerr << "returning response" << endl << endl;
        int & code = get<1>(response);
        code = status;
        string & body_ = get<2>(response);
        body_ = move(body);
        HttpClientError & errorCode = get<0>(response);
        errorCode = error;
        done = true;
        MLDB::wake_by_address(done);
    };
    auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);

    client.get("/coucou", cbs, {}, {}, -1);

    cerr << "finished get" << endl << endl;

    while (!done) {
        int oldDone = false;
        MLDB::wait_on_address(done, oldDone);
    }
#endif
}
#endif

BOOST_AUTO_TEST_CASE( test_http_client_get_minimal2 )
{
    cerr << "client_get\n";
    string baseUrl = "http://127.0.0.1/";

    MessageLoop messageLoop(1, 0, -1);
    auto client = std::make_shared<HttpClientImplV1>(baseUrl, 1024, 0 /* queue size */);

    // Give it a chance to initialize before we initialize the get
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    cerr << "starting message loop" << endl << endl;

    messageLoop.start();

    // Give it a chance to initialize before we initialize the get
    std::this_thread::sleep_for(std::chrono::milliseconds(50));


#if 1 // crashes
    messageLoop.addSource("client", client);
#else // works
    int fd = client->selectFd(); // crashes, in thread or not
    messageLoop.addFd(fd, EPOLL_INPUT, client.get());
#endif

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
}

#if 0
BOOST_AUTO_TEST_CASE( test_http_client_get_full )
{
    cerr << "client_get\n";
    string baseUrl = "http://127.0.0.1/";

    MessageLoop messageLoop(1, 0, -1);
    auto client = std::make_shared<HttpClientImplV1>(baseUrl, 1024, 0 /* queue size */);

    // Give it a chance to initialize before we initialize the get
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    cerr << "starting message loop" << endl << endl;

    messageLoop.start();

    // Give it a chance to initialize before we initialize the get
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    messageLoop.addSource("client", client);

#if 0
    ClientResponse response;

    std::atomic<int> done(false);
    auto onResponse = [&] (const HttpRequest & rq,
                           HttpClientError error,
                           int status,
                           string && headers,
                           string && body) {
        cerr << "returning response" << endl << endl;
        int & code = get<1>(response);
        code = status;
        string & body_ = get<2>(response);
        body_ = move(body);
        HttpClientError & errorCode = get<0>(response);
        errorCode = error;
        done = true;
        MLDB::wake_by_address(done);
    };
    auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);

    client->enqueueRequest("GET", "/coucou", cbs, {}, {}, {}, -1);

    cerr << "finished get" << endl << endl;

    while (!done) {
        int oldDone = false;
        MLDB::wait_on_address(done, oldDone);
    }
#endif
}
#endif
