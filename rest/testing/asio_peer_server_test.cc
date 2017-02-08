// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** asio_server_test.cc                                            -*- C++ -*-
    Jeremy Barnes, 25 June 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "mldb/rest/asio_peer_server.h"
#include <boost/asio.hpp>
#include <thread>
#include <future>
#include "mldb/jml/utils/string_functions.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

using namespace std;
using namespace MLDB;

#if 0
BOOST_AUTO_TEST_CASE( test_rapid_creation_destruction_1 )
{
    for (unsigned i = 0;  i < 1000;  ++i) {

        AsioPeerServer server;

        server.init(PortRange(20000,30000), "localhost");

        PeerInfo info;
    }

}

BOOST_AUTO_TEST_CASE( test_rapid_creation_destruction_2 )
{
    for (unsigned i = 0;  i < 100;  ++i) {

        AsioPeerServer server;

        server.init(PortRange(20000,30000), "localhost");

        PeerInfo info;
        info.peerName = "hello";
        info.location = "world";
        info.serviceType = "test";

        info = server.listen(info);
    }
}
#endif

BOOST_AUTO_TEST_CASE( test_rapid_creation_sync_send )
{
    for (unsigned i = 0;  i < 100;  ++i) {

        AsioPeerServer server;

        server.init(PortRange(20000,30000), "localhost");

        PeerInfo info;
        info.peerName = "hello";
        info.location = "world";
        info.serviceType = "test";

        info = server.listen(info);

        std::promise<bool> promise;

        server.setNewConnectionHandler([&] (std::shared_ptr<PeerConnection> conn)
                                       {
                                           cerr << "got new connection" << endl;
                                           std::string data("hello");
                                           conn->send(std::move(data));
                                       });
        
        auto conn = server.connect(info);

        auto onRead = [&] (std::string && data)
            {
                cerr << "read data" << endl;
                BOOST_CHECK_EQUAL(data, "hello");
                promise.set_value(true);
                return false;
            };

        conn->startReading(onRead);

        auto future = promise.get_future();

        future.wait_for(std::chrono::seconds(1));

        BOOST_CHECK_EQUAL(future.get(), true);

        server.shutdown();
    }
}

#if 0  // TODO: enable once error handling is reasonable
BOOST_AUTO_TEST_CASE( test_listen_socket_close )
{
    for (unsigned i = 0;  i < 100;  ++i) {

        AsioPeerServer server;

        server.init(PortRange(20000,30000), "localhost");

        PeerInfo info;
        info.peerName = "hello";
        info.location = "world";
        info.serviceType = "test";

        info = server.listen(info);

        int fd = server.acceptFd();
        
        std::promise<bool> promise;

        server.onNewConnection = [&] (std::shared_ptr<PeerConnection> conn)
            {
                cerr << "got new connection" << endl;
                std::string data("hello");
                conn->send(std::move(data));
            };

        auto conn = server.connect(info);
        ::close(fd);

        auto onRead = [&] (std::string && data)
            {
                cerr << "read data" << endl;
                BOOST_CHECK_EQUAL(data, "hello");
                promise.set_value(true);
                return false;
            };

        conn->startReading(onRead);

        auto future = promise.get_future();

        future.wait_for(std::chrono::seconds(1));

        BOOST_CHECK_EQUAL(future.get(), true);
    }
}
#endif

#if 0

        info = server.listen(info);

        void init(PortRange bindPort, const std::string & bindHost,
                  int publishPort = -1, std::string publishHost = "");

        /** Listen, updating our peer info for this server. */
        virtual PeerInfo listen(PeerInfo info);

        /** Shutdown the peer server. */
        virtual void shutdown();

        virtual std::shared_ptr<PeerConnection>
            connect(const PeerInfo & peer);

        /** Return a timer that triggers at the given expiry and optionally
            resets to fire periodically.
        */
        virtual WatchT<Date> getTimer(Date expiry, double period = -1.0,
                                      std::function<void (Date)> toBind = nullptr);


#endif

