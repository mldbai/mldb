// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* http_long_header_test.cc
   Jeremy Barnes, 31 January 2011
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.

   Test that we can't crash the server sending long headers.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <poll.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <boost/system/error_code.hpp>
#include <boost/test/unit_test.hpp>
#include "mldb/jml/utils/guard.h"
#include "mldb/base/exc_assert.h"
#include "mldb/arch/exception_handler.h"
#include "mldb/utils/testing/watchdog.h"
#include "mldb/jml/utils/testing/fd_exhauster.h"
#include "mldb/io/asio_thread_pool.h"
#include "mldb/io/event_loop.h"
#include "mldb/io/port_range_service.h"
#include "mldb/io/tcp_acceptor.h"
#include "mldb/io/tcp_socket_handler.h"

using namespace std;
using namespace ML;
using namespace MLDB;


BOOST_AUTO_TEST_CASE( test_connection_overflow )
{
    signal(SIGPIPE, SIG_IGN);

    // Leave a fair bit of time... we probably don't need it all
    Watchdog watchdog(35.0);

    string connectionError;

    string error;
    
    struct TestHandler : TcpSocketHandler {
        TestHandler(TcpSocket && socket)
            : TcpSocketHandler(std::move(socket))
        {
        }

        virtual void bootstrap()
        {
            requestReceive();
        }

        /** Function called out to when we got some data */
        virtual void onReceivedData(const char * data, size_t size)
        {
            cerr << "handleData " << string(data, size) << endl;

            // Once we get some data, ping a lot of data back
            string s(1000000, '0');  // 1MB of nulls

            auto onWritten = [&] (const boost::system::error_code & ec,
                                  std::size_t written) {
                requestClose();
                cerr << "finished sending data" << endl;
            };
            // 10000 x 1MB = 10GB
            for (unsigned i = 0;  i < 10000;  ++i) {
                //cerr << "sending " << i << endl;
                requestWrite(s);
            }

            requestWrite(s, onWritten);
        }
    
        /** Function called out to when we got an error from the socket. */
        virtual void onReceiveError(const boost::system::error_code & ec, size_t)
        {
            cerr << "got error " << ec.message() << endl;
        }
    };

    auto onMakeNewHandler = [&] (TcpSocket && socket) {
        return std::make_shared<TestHandler>(std::move(socket));
    };
    
    EventLoop eventLoop;
    AsioThreadPool threadPool(eventLoop);
    TcpAcceptor acceptor(eventLoop, onMakeNewHandler);

    acceptor.listen(0);
    int port = acceptor.effectiveTCPv4Port();

    cerr << "port = " << port << endl;

    /* Open a connection */
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == -1)
        throw Exception("socket");

    struct sockaddr_in addr = { AF_INET, htons(port), { INADDR_ANY } }; 
    int res = connect(s, reinterpret_cast<const sockaddr *>(&addr),
                      sizeof(addr));

    if (res == -1) {
        cerr << "connect error: " << strerror(errno) << endl;
        close(s);
    }

    res = write(s, "hello", 5);
    cerr << "write returned " << res << endl;
    ExcAssertEqual(res, 5);

    // Now read everything that we can
    int64_t bytesRead = 0;

    vector<char> buf(10000000);

    while (true) {
        int res = read(s, &buf[0], buf.size());
        cerr << "read " << res << " bytes total is " << bytesRead + res << endl;
        if (res == -1 && errno == EINTR)
            continue;
        if (res == 0)
            break;
        if (res == -1)
            throw MLDB::Exception(errno, "Failed to read()");
        bytesRead += res;
    }

    cerr << "total of " << bytesRead << " bytes read" << endl;

    BOOST_CHECK_EQUAL(bytesRead, 10001000000LL);

    close(s);

    cerr << "shutting down" << endl;
    threadPool.shutdown();
}
