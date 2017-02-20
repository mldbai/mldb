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

#include <boost/filesystem.hpp>
#include <boost/test/unit_test.hpp>
#include <vector>
#include <iostream>


using namespace std;
namespace fs = boost::filesystem;
using namespace ML;
using namespace MLDB;

using boost::unit_test::test_suite;

struct TestServer {
    MLDB::Runner runner;
    string baseUrl;
    MLDB::MessageLoop loop;

    TestServer() {

        // Find free port
        int freePort = 0;
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
        runner.run({"/usr/bin/env", "python",
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
