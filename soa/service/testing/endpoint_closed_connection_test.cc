// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* endpoint_closed_connection_test.cc
   Jeremy Barnes, 26 July 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

*/
#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/arch/format.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/base/exc_assert.h"
#include "mldb/jml/utils/hex_dump.h"
#include "mldb/jml/utils/environment.h"
#include "mldb/arch/exception_handler.h"
#include "mldb/arch/futex.h"
#include <chrono>
#include <thread>
#include "mldb/http/http_endpoint.h"
#include "mldb/soa/service/json_endpoint.h"
#include <boost/thread/thread.hpp>
#include <boost/thread.hpp>
#include <boost/thread/barrier.hpp>
#include "mldb/soa/service/connectfd.h"

#include <poll.h>
#include <sys/socket.h>

using namespace std;
using namespace ML;
using namespace MLDB;


BOOST_AUTO_TEST_CASE( test_protocol_dump )
{
    ML::set_default_trace_exceptions(false);

    std::function<std::shared_ptr<JsonConnectionHandler> ()> handlerFactory;

    auto onGotJson = [&] (const HttpHeader & header,
                          const Json::Value & payload,
                          const std::string & jsonStr,
                          AdHocJsonConnectionHandler * conn)
        {
            //cerr << "hello I got some JSON " << payload << endl;

            auto onSendFinished = [=] ()
            {
                conn->transport().associateWhenHandlerFinished
                    (handlerFactory(), "gotMeSomeJson");
            };

            conn->sendHttpChunk("1", PassiveConnectionHandler::NEXT_CONTINUE,
                                onSendFinished);
        };

    HttpEndpoint server("testJsonServer");
    server.handlerFactory = handlerFactory = [&] ()
        {
            return std::make_shared<AdHocJsonConnectionHandler>(onGotJson);
        };
    
    int nServerThreads = 2;

    int port = server.init(-1, "localhost", nServerThreads);
    cerr << "listening on port " << port << endl;

    int nClientThreads = 10;

    boost::thread_group tg;

    int shutdown = false;

    volatile int maxFd = 0;

    std::atomic<uint64_t> doneRequests(0);

    auto doReadyThread = [&] ()
        {
            int res = -1;

            while (!shutdown) {
                // Get a connection
                int fd = connectLocalhost(port);

                if (fd > maxFd) {
                    maxFd = fd;
                    cerr << "maxFd now " << fd << endl;
                    cerr << "done " << doneRequests << " requests" << endl;
                }

                //cerr << "connected on fd " << fd << endl;

                //int nrequests = 0;
                //int errors = 0;

                while (!shutdown) {
                    string request = 
                        "POST /ready HTTP/1.1\r\n"
                        "Transfer-Encoding: Chunked\r\n"
                        "Content-Type: application/json\r\n"
                        "Keepalive: true\r\n"
                        "\r\n"
                        "2\r\n"
                        "{}";

                    const char * current = request.c_str();
                    const char * end = current + request.size();

                    // Date before = Date::now();

                    while (current != end) {
                        res = send(fd, current, end - current, MSG_NOSIGNAL);
                        if (res == -1)
                            throw MLDB::Exception(errno, "send()");
                        current += res;
                    }
                    
                    // Close our writing half
                    //res = ::shutdown(fd, SHUT_WR);
                    //cerr << "shutdown reader " << res << " " << strerror(errno)
                    //<< endl;
                    if (res == -1)
                        throw MLDB::Exception(errno, "shutdown");
                    
                    ExcAssertEqual((void *)current, (void *)end);
                    
                    struct pollfd fds[1] = {
                        { fd, POLLIN | POLLRDHUP, 0 }
                    };

                    res = poll(fds, 1, 500 /* ms timeout */);
                    if (res == -1)
                        throw MLDB::Exception(errno, "poll");

                    if (res == 0) {
                        cerr << "fd " << fd << " timed out after 500ms"
                             << endl;
                        break;
                    }

                    // Wait for a response
                    char buf[16384];
                    res = recv(fd, buf, 16384, 0);
                    if (res == -1)
                        throw MLDB::Exception(errno, "recv");
                    
                    //double timeTaken = Date::now().secondsSince(before);
                    //cerr << "took " << timeTaken * 1000 << "ms" << endl;

                    if (res == 0) {
                        cerr << "connection " << fd << " was closed" << endl;
                        break;  // connection closed
                    }

                    //cerr << "got " << res << " bytes back from server" << endl;
                    //string response(buf, buf + res);
                    //cerr << "response is " << response << endl;
                
                    futex_wait(shutdown, 0, 0.001 /* seconds */);

                    doneRequests += 1;

                    //break;  // close the connection
                }

                errno = 0;

                // Close our writing half
                //res = ::shutdown(fd, SHUT_WR);
                //cerr << "shutdown reader " << res << " " << strerror(errno)
                //<< endl;
                //if (res == -1)
                //    throw MLDB::Exception(errno, "shutdown");

                // Wait for the other end to close down
                //char buf[16384];
                //res = recv(fd, buf, 16384, 0);
                //cerr << "recv " << res << " " << strerror(errno)
                //<< endl;
                //if (res == -1)
                //    throw MLDB::Exception(errno, "recv");
                //if (res != 0)
                //    throw MLDB::Exception("got garbage");
                
                // Close our writing half
                //res = ::shutdown(fd, SHUT_RD);
                //cerr << "shutdown writer " << res << " " << strerror(errno)
                //<< endl;
                if (res == -1)
                    throw MLDB::Exception(errno, "shutdown");
            
                res = close(fd);
                //cerr << "close " << res << " " << strerror(errno)
                //<< endl;
                if (res == -1)
                    throw MLDB::Exception(errno, "close");
            }            
        };
    
    
    for (unsigned i = 0;  i <= nClientThreads;  ++i)
        tg.create_thread(doReadyThread);

    for (unsigned i = 0;  i < 10;  ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        cerr << "done " << doneRequests << " requests" << endl;
    }

    //ML::sleep(10.0);

    shutdown = true;
    futex_wake(shutdown);

    tg.join_all();

    cerr << "done " << doneRequests << " requests" << endl;
}
