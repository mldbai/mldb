/**                                                                -*- C++ -*-
 * http_streambuf_test.cc
 * Mich, 2017-01-19
 * This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
 *
 * Make sure http errors print the proper status code in the terminal
 **/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <thread>
#include <chrono>

#include "mldb/vfs/filter_streams.h"
#include "mldb/utils/runner.h"
#include "mldb/io/message_loop.h"
#include "mldb/server/mldb_server.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/utils/testing/watchdog.h"

#include "mldb/compiler/filesystem.h"
#include <boost/test/unit_test.hpp>
#include <vector>
#include <iostream>


using namespace std;
namespace fs = std::filesystem;
using namespace MLDB;

using boost::unit_test::test_suite;

struct TestServer {
    MLDB::Runner runner;
    string baseUrl;
    MLDB::MessageLoop loop;

    TestServer() {

        // Find free port
        int freePort = 15385;
        {
            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            auto sockfd = socket(AF_INET, SOCK_STREAM, 0);
            addr.sin_port = 0; //htons(freePort);
            addr.sin_addr.s_addr = INADDR_ANY;
            if (::bind(sockfd, (struct sockaddr *) &addr, sizeof(addr))) {
                throw Exception("Failed to bind on free port");
            }
            socklen_t addrLen = sizeof(addr);
            if (getsockname(sockfd, (struct sockaddr *)&addr, &addrLen) == -1) {
                throw Exception("Failed to getsockname");
            }
            freePort = addr.sin_port;
            close(sockfd);
        }
        baseUrl = "http://localhost:" + to_string(freePort) + "/";
        cerr << "listening on " << baseUrl << endl;

        loop.addSource("runner", runner);
        loop.start();

        auto onTerminate = [](const RunResult & result) {
            cerr << "ON TERMINATE" << endl;
        };
        auto onStdOut = [] (string && message) {
            cerr << "received message on stdout: /" + message + "/" << endl;
        };
        auto stdOutSink = make_shared<CallbackInputSink>(onStdOut);

        // this is ok because it doesn't require a virtualenv
        // well actually, in Python 3 it does but it's still OK :)
        runner.run({"/usr/bin/env", "python3",
                    "mldb/testing/test_server.py",
                    to_string(freePort)},
                    onTerminate, nullptr, stdOutSink);

        // test connection
        for (int i = 0; i < 100; ++i) {
            cerr << "Waiting for http server to start..." << endl;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            try {
                filter_istream in(baseUrl);
            }
            catch (const Exception & exc) {
                cerr << "Got exception: " << exc.what() << endl;
                continue;
            }
            break;
        }
        cerr << "http server ready for testing" << endl;
    }

    ~TestServer() {
        // Unsafe, those may throw, but it's "only" a test...
        runner.kill();
        runner.waitTermination();
    }

} testServer;

BOOST_AUTO_TEST_CASE( test_error_codes )
{
    MLDB::Watchdog watchdog(60);
    for (auto code: {400, 404, 500}) {
        streambuf* oldCerrStreamBuf = cerr.rdbuf();
        ostringstream strCerr;
        cerr.rdbuf( strCerr.rdbuf() );

        // Provoke the error
        try {
            filter_istream in(testServer.baseUrl + to_string(code));
            string line;
            while(getline(in, line)) {
                cout << line << endl;
            }
            in.close();
        } catch (const Exception & e) {
            cerr << "Got exception: " << e.what() << endl;
        }

        // Restore cout and cerr
        cerr.rdbuf( oldCerrStreamBuf );

        cout << "====CERR====" << endl;
        cout << strCerr.str() << endl;
        cout << "============" << endl;
        // Validate error
        string expect = "HTTP code " + to_string(code) + " reading http://";
        BOOST_REQUIRE(strCerr.str().find(expect) != string::npos);
    }
}

BOOST_AUTO_TEST_CASE( test_infinite_redirect )
{
    MLDB::Watchdog watchdog(60);
    streambuf* oldCerrStreamBuf = cerr.rdbuf();
    ostringstream strCerr;
    cerr.rdbuf( strCerr.rdbuf() );

    // Provoke the error
    try {
        filter_istream in(testServer.baseUrl + "infinite_redirect");
        string line;
        while(getline(in, line)) {
            cout << line << endl;
        }
        in.close();
    } catch (const Exception & e) {
        cerr << "Got exception: " << e.what() << endl;
    }

    // Restore cout and cerr
    cerr.rdbuf( oldCerrStreamBuf );

    cout << "====CERR====" << endl;
    cout << strCerr.str() << endl;
    cout << "============" << endl;

    // Validate error
    string expect = "Number of redirects hit maximum amount";
    BOOST_REQUIRE(strCerr.str().find(expect) != string::npos);
}

BOOST_AUTO_TEST_CASE( test_fetcher_error )
{
    MLDB::Watchdog watchdog(60);
    //MLDB-2150
    MldbServer server;
    server.init();
    string httpBoundAddress =
        server.bindTcp(PortRange(17000, 18000), "127.0.0.1");
    server.start();
    HttpRestProxy proxy(httpBoundAddress);

    Json::Value data;
    data["type"] = "transform";
    Json::Value params;
    params["inputData"] =
        "SELECT fetcher('" + testServer.baseUrl + "404') AS *";
    params["outputDataset"] = Json::objectValue;
    const string dsId = "fetcher_404_test";
    params["outputDataset"]["id"] = dsId;
    params["outputDataset"]["type"] = "tabular";
    data["params"] = params;

    auto res = proxy.post("/v1/procedures", data);
    BOOST_REQUIRE_EQUAL(res.code(), 201);

    res = proxy.get("/v1/query?q=SELECT%20*%20FROM%20" + dsId);
    BOOST_REQUIRE_EQUAL(res.code(), 200);
    auto body = res.jsonBody();
    BOOST_REQUIRE_EQUAL(body.size(), 1);
    BOOST_REQUIRE_EQUAL(body[0]["columns"].size(), 1);
    BOOST_REQUIRE_EQUAL(body[0]["columns"][0][0].asString(), "error");
    BOOST_REQUIRE(
        body[0]["columns"][0][1].asString().find(" 404 ") != string::npos);
}
