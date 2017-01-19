// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tcp_acceptor_test+http.cc
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   Unit test for TcpAcceptor and HttpSocketHandler
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <iostream>
#include <string>
#include <boost/test/unit_test.hpp>
#include <boost/asio.hpp>
#include "base/exc_assert.h"

#include "mldb/io/asio_thread_pool.h"
#include "mldb/io/event_loop.h"
#include "mldb/io/event_loop_impl.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/http/http_socket_handler.h"
#include "mldb/io/port_range_service.h"
#include "mldb/io/tcp_acceptor.h"


using namespace std;
using namespace boost;
using namespace MLDB;

struct MyHandler : public HttpLegacySocketHandler {
    MyHandler(TcpSocket && socket);

    virtual void handleHttpPayload(const HttpHeader & header,
                                   const std::string & payload);
};

MyHandler::
MyHandler(TcpSocket && socket)
    : HttpLegacySocketHandler(std::move(socket))
{
}

void
MyHandler::
handleHttpPayload(const HttpHeader & header,
                  const std::string & payload)
{
    HttpResponse response(200, "text/plain", "pong");

    if (header.resource == "/wait") {
        cerr << "service sleeping\n";
        ::sleep(2);
        cerr << "service done sleeping\n";
    }

    // static string responseStr("HTTP/1.1 200 OK\r\n"
    //                           "Content-Type: text/plain\r\n"
    //                           "Content-Length: 4\r\n"
    //                           "\r\n"
    //                           "pong");
    putResponseOnWire(response);
    
    // send(responseStr);
}


/* Test simple random request */
BOOST_AUTO_TEST_CASE( tcp_acceptor_http_test )
{
    EventLoop loop;
    AsioThreadPool pool(loop);

    auto onNewConnection = [&] (TcpSocket && socket) {
        return std::make_shared<MyHandler>(std::move(socket));
    };

    TcpAcceptor acceptor(loop, onNewConnection);
    // acceptor.spinup(concurrency);
    acceptor.listen(0, "localhost");

    cerr << ("service accepting connections on port "
             + to_string(acceptor.effectiveTCPv4Port())
             + "\n");
    BOOST_REQUIRE_GT(acceptor.effectiveTCPv4Port(), 0);
    BOOST_REQUIRE_EQUAL(acceptor.effectiveTCPv6Port(), -1);
    HttpRestProxy proxy("http://localhost:"
                        + to_string(acceptor.effectiveTCPv4Port()));
    auto resp = proxy.get("/v1/ping");
    BOOST_REQUIRE_EQUAL(resp.code(), 200);

    pool.shutdown();
}


/* Test request and close the socket before the response */
BOOST_AUTO_TEST_CASE( tcp_acceptor_http_disconnect_test )
{
    EventLoop loop;
    AsioThreadPool pool(loop);

    auto onNewConnection = [&] (TcpSocket && socket) {
        return std::make_shared<MyHandler>(std::move(socket));
    };

    TcpAcceptor acceptor(loop, onNewConnection);
    acceptor.listen(0, "localhost");

    auto address = asio::ip::address::from_string("127.0.0.1");
    asio::ip::tcp::endpoint serverEndpoint(address,
                                           acceptor.effectiveTCPv4Port());

    {
        auto socket = asio::ip::tcp::socket(loop.impl().ioService());
        socket.connect(serverEndpoint);
        string request = ("GET /wait HTTP/1.1\r\n"
                          "Host: *\r\n"
                          "\r\n");
        cerr << "sending\n";
        socket.send(asio::buffer(request.c_str(), request.size()));
        cerr << "sent\n";
    }
    ::sleep(1);

    pool.shutdown();
}

/* Test handling of 100-Continue headers */
BOOST_AUTO_TEST_CASE( tcp_acceptor_http_100_continue )
{
    EventLoop loop;
    AsioThreadPool pool(loop);

    auto onNewConnection = [&] (TcpSocket && socket) {
        return std::make_shared<MyHandler>(std::move(socket));
    };

    TcpAcceptor acceptor(loop, onNewConnection);
    acceptor.listen(0, "localhost");

    auto address = asio::ip::address::from_string("127.0.0.1");
    asio::ip::tcp::endpoint serverEndpoint(address,
                                           acceptor.effectiveTCPv4Port());

    {
        auto socket = asio::ip::tcp::socket(loop.impl().ioService());
        socket.connect(serverEndpoint);

        {
            /* First we send a 100 continue request. */
            char request[] = ("POST /something HTTP/1.1\r\n"
                              "Host: *\r\n"
                              "Content-Length: 5\r\n"
                              "Expect: 100-continue\r\n"
                              "\r\n");
            socket.send(asio::buffer(request, sizeof(request)));
            char recvBuffer[1024];
            size_t nBytes = socket.receive(asio::buffer(recvBuffer,
                                                        sizeof(recvBuffer)));
            BOOST_REQUIRE(strncmp(recvBuffer, "HTTP/1.1 100 Continue\r\n\r\n", nBytes)
                          == 0);
            char body[] = "abcde";
            socket.send(asio::buffer(body, sizeof(body)));
        }

        {
            /* Then we send a second request to ensure that the states were
               correctly reset. */
            char request[] = ("POST /ping HTTP/1.1\r\n"
                              "Host: *\r\n"
                              "Content-Length: 5\r\n"
                              "\r\n"
                              "12345");
            socket.send(asio::buffer(request, sizeof(request)));

            char recvBuffer[1024];
            size_t nBytes = socket.receive(asio::buffer(recvBuffer,
                                                        sizeof(recvBuffer)));
            string expectedResponse("HTTP/1.1 200 OK\r\n"
                                    "Content-Type: text/plain\r\n"
                                    "Content-Length: 4\r\n"
                                    "Connection: Keep-Alive\r\n"
                                    "\r\n"
                                    "pong");
            string response(recvBuffer, nBytes);
            BOOST_CHECK_EQUAL(response, expectedResponse);
        }
    }
    ::sleep(1);

    pool.shutdown();
}
