// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** MLDB-1040-invalid-requests.cc
    Wolfgang Sourdeau, 25 October 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Test the behaviour of MLDB service when receiving all sorts of invalid
    requests.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "mldb/io/tcp_acceptor.h"
#include "mldb/server/mldb_server.h"


using namespace std;
using namespace boost;

using namespace MLDB;


/* Test various forms of invalid HTTP requests. Currently, this test is itself
 * invalid. */
BOOST_AUTO_TEST_CASE( test_invalid_requests )
{
    MldbServer server;
    
    server.init();
    string httpBoundAddress = server.bindTcp(PortRange(17000,18000),
                                             "127.0.0.1");
    cerr << "http listening on " << httpBoundAddress << endl;
    
    server.start();
    auto & acceptor = *server.httpEndpoint->acceptor_;
    int port = acceptor.effectiveTCPv4Port();
    auto address = asio::ip::address::from_string("127.0.0.1");
    asio::ip::tcp::endpoint serverEndpoint(address, port);
    asio::io_service ioService;

    auto openSocket = [&] () {
        auto socket = asio::ip::tcp::socket(ioService);
        socket.connect(serverEndpoint);
        return socket;
    };
    auto sendBytes = [&] (asio::ip::tcp::socket & sock,
                          const string & bytes) {
        sock.send(boost::asio::buffer(bytes.c_str(), bytes.size()));
        char data[1000];
        size_t nBytes = sock.receive(boost::asio::buffer(data, sizeof(data)));
        return string(data, nBytes);
    };

    /* valid request with variable case headers
       -> should be tolerated */
    {
        auto client = openSocket();
        string payload = ("PUT /v1/datasets/test1 HTTP/1.1\r\n"
                          "hoSt: *\r\n"
                          "coNtenT-tyPe: application/json\r\n"
                          "COnteNt-LENGTH: 57\r\n\r\n"
                          "{\"id\":\"test1\",\"type\":\"sparse.mutable\""
                          ",\"persistent\":false}");
        auto data = sendBytes(client, payload);
        cerr << "data = " << data << endl;
        string line = data.substr(0, 20);
        BOOST_CHECK_EQUAL(line, "HTTP/1.1 201 Created");
    }

    /* invalid HTTP method
       -> should close the connection with "bad request" */
    {
        auto client = openSocket();
        string payload = ("CLAP /v1/datasets/test1 HTTP/1.1\r\n"
                          "hoSt: *\r\n\r\n");
        auto data = sendBytes(client, payload);
        string line = data.substr(0, 22);
        BOOST_CHECK_EQUAL(line, "HTTP/1.1 404 Not Found");
    }

    /* len(payload) > content-length
       2 problems: first request with invalid payload,
       second request with garbage
       -> connection should be closed after first error with "bad request",
          second error payload is discarded */
    {
        auto client = openSocket();
        string payload = ("PUT /v1/datasets/test2 HTTP/1.1\r\n"
                          "hoSt: *\r\n"
                          "coNtenT-tyPe: application/json\r\n"
                          "COnteNt-LENGTH: 20\r\n\r\n"
                          "{\"id\":\"test2\",\"type\":\"sparse.mutable\""
                          ",\"persistent\":false}");
        auto data = sendBytes(client, payload);
        string line = data.substr(0, 24);
        BOOST_CHECK_EQUAL(line, "HTTP/1.1 400 Bad Request");
    }

    /* len(payload) = content-length, invalid contents
       -> connection should be closed after error with "bad request" */
    {
        auto client = openSocket();
        string payload = ("PUT /v1/datasets/test1 HTTP/1.1\r\n"
                          "Content-Type: application/json\r\n"
                          "Content-Length: 10\r\n"
                          "Host: *\r\n\r\n"
                          "{\"key1\":12");
        auto data = sendBytes(client, payload);
        string line = data.substr(0, 24);
        BOOST_CHECK_EQUAL(line, "HTTP/1.1 400 Bad Request");
    }

    /* complete garbage
       -> connection should be closed after error with "bad request" */
    // {
    //     auto client = openSocket();
    //     string payload = "das;h98ehdspdoui29388c3298jid130923[09cjasd";
    //     auto data = sendBytes(client, payload);
    //     string line = data.substr(0, 20);
    //     BOOST_CHECK_EQUAL(line, "HTTP/1.1 400 Bad Request");
    // }
}
