// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* http_service_bench.cc
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   Benchmark utility for testing the ASIO-based http services.
*/

#include <iostream>
#include <string>

#include "base/exc_assert.h"

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include "mldb/io/asio_thread_pool.h"
#include "mldb/io/event_loop.h"
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

    // static string responseStr("HTTP/1.1 200 OK\r\n"
    //                           "Content-Type: text/plain\r\n"
    //                           "Content-Length: 4\r\n"
    //                           "\r\n"
    //                           "pong");
    putResponseOnWire(response);
    
    // send(responseStr);
}

int
main(int argc, char * argv[])
{
    using namespace boost::program_options;
    unsigned int concurrency(0);
    unsigned int port(20000);

    options_description all_opt;
    all_opt.add_options()
        ("concurrency,c", value(&concurrency),
         "Number of concurrent requests (mandatory)")
        ("port,p", value(&port),
         "port to listen on (20000)")
        ("help,H", "show help");

    variables_map vm;
    store(command_line_parser(argc, argv)
          .options(all_opt)
          .run(),
          vm);
    notify(vm);

    if (vm.count("help")) {
        cerr << all_opt << endl;
        return 1;
    }

    ExcAssert(concurrency > 0);

    EventLoop loop;
    AsioThreadPool pool(loop);

    auto onNewConnection = [&] (TcpSocket && socket) {
        return std::make_shared<MyHandler>(std::move(socket));
    };

    TcpAcceptor acceptor(loop, onNewConnection);
    acceptor.listen(port);

    cerr << ("service accepting connections on port "
             + to_string(acceptor.effectiveTCPv4Port())
             + "\n");
    while (true) {
        ::sleep(1234567);
    }
}
