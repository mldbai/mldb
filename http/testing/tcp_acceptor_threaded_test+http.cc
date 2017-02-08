// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tcp_acceptor_threaded_test+http.cc
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   Unit test for TcpAcceptor and HttpSocketHandler in multi-threaded context.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <iostream>
#include <string>
#include <boost/test/unit_test.hpp>
#include <boost/asio.hpp>
#include "mldb/base/exc_assert.h"
#include "mldb/jml/utils/ring_buffer.h"

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


struct MyHandler;

/****************************************************************************/
/* THREADED REQUEST                                                         */
/****************************************************************************/

struct ThreadedRequest {
    ThreadedRequest()
        : handler(nullptr)
    {
    }

    ThreadedRequest(const std::shared_ptr<MyHandler> & newHandler,
                    const HttpHeader & newHeader,
                    const std::string & newPayload)
        : handler(newHandler), header(newHeader), payload(newPayload)
    {
    }

    std::shared_ptr<MyHandler> handler;
    HttpHeader header;
    std::string payload;
};

typedef ML::RingBufferSWMR<ThreadedRequest> RequestQueue;


/****************************************************************************/
/* MY HANDLER                                                               */
/****************************************************************************/

struct MyHandler : public HttpLegacySocketHandler {
    MyHandler(TcpSocket && socket, RequestQueue * queue)
        : HttpLegacySocketHandler(std::move(socket)), queue_(queue)
    {
    }

    virtual void handleHttpPayload(const HttpHeader & header,
                                   const std::string & payload);

    RequestQueue * queue_;
};

void
MyHandler::
handleHttpPayload(const HttpHeader & header, const std::string & payload)
{
    auto ptr = acceptor().findHandlerPtr(this);
    ThreadedRequest request(static_pointer_cast<MyHandler>(ptr), header, payload);
    queue_->push(std::move(request));
}


/* Test */
void
processRequests(RequestQueue & queue)
{
    while (true) {
        ThreadedRequest request = queue.pop();

        if (!request.handler) {
            break;
        }

        HttpResponse response(200, "text/plain", "pong");
        if (request.header.resource == "/wait") {
            cerr << "service sleeping\n";
            ::sleep(2);
            cerr << "service done sleeping\n";
        }
        request.handler->putResponseOnWire(response);
    }
    cerr << "exiting queue thread...\n";
}

void
sendRequest(int port)
{
    boost::asio::io_service ioService;
    auto socket = asio::ip::tcp::socket(ioService);
    auto address = asio::ip::address::from_string("127.0.0.1");
    asio::ip::tcp::endpoint serverEndpoint(address, port);
    socket.connect(serverEndpoint);
    string request = ("GET /wait HTTP/1.1\r\n"
                      "Connection: close\r\n"
                      "Host: *\r\n"
                      "\r\n");
    cerr << "sending\n";
    socket.send(boost::asio::buffer(request.c_str(), request.size()));
    cerr << "sent, closing\n";
    socket.close();
    cerr << "closed\n";
}

/* Test request and close the socket before the response */
BOOST_AUTO_TEST_CASE( tcp_acceptor_http_disconnect_test )
{
    RequestQueue queue(16);
    auto processRequestsFn = [&] () {
        processRequests(queue);
    };
    std::thread queueThread(processRequestsFn);

    EventLoop loop;
    AsioThreadPool pool(loop);

    auto onNewConnection = [&] (TcpSocket && socket) {
        return std::make_shared<MyHandler>(std::move(socket), &queue);
    };

    TcpAcceptor acceptor(loop, onNewConnection);
    acceptor.listen(0, "localhost");
    sendRequest(acceptor.effectiveTCPv4Port());
    while (queue.writePosition == 0) {
        ML::futex_wait(queue.writePosition, 0);
    }

    cerr << "pushing finalisation request\n";
    queue.push(ThreadedRequest());
    queueThread.join();

    pool.shutdown();
}
