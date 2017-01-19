// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* asio_peer_server.cc
   Jeremy Barnes, 20 June 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "asio_peer_server.h"
#include "mldb/io/asio_thread_pool.h"
#include "mldb/io/asio_timer.h"
#include "mldb/io/event_loop_impl.h"
#include "mldb/jml/utils/string_functions.h"
#include "asio_peer_connection.h"

using namespace std;


namespace MLDB {

/*****************************************************************************/
/* ASIO PEER SERVER                                                          */
/*****************************************************************************/

int bindAndReturnOpenTcpPort(boost::asio::ip::tcp::acceptor & acceptor,
                             PortRange const & portRange,
                             const std::string & host)
{
    std::string uri;

    auto protocol = boost::asio::ip::tcp::v4();
    acceptor.open(protocol);
    acceptor.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));

    int port = portRange.bindPort
        ([&](int port)
         {
             //cerr << "trying port " << port << endl;
             boost::asio::ip::tcp::endpoint endpoint(protocol, port);
             boost::system::error_code err;
             acceptor.bind(endpoint, err);
             //cerr << "err = " << err << endl;
             
             return !err;
         });
    
    if (port == -1)
        // Throw is OK: this will happen in a synchronous context.
        throw MLDB::Exception("no open TCP port '%s': %s",
                            uri.c_str(),
                            strerror(errno));
    
    uri = MLDB::format("tcp://%s:%d", host.c_str(), port);

    return port;
}

struct AsioPeerServer::Impl {
    Impl(AsioPeerServer * server)
        : server(server),
          acceptor(eventLoop.impl().ioService()),
          threads(eventLoop),
          shutdown_(false)
    {
        threads.ensureThreads(4);  // So connect and accept can happen
    }

    ~Impl()
    {
        shutdown();
    }

    AsioPeerServer * server;

    void init(PortRange bindPort, const std::string & bindHost,
              int publishPort, std::string publishHost)
    {
        this->bindPort = bindPort;
        this->bindHost = bindHost;
        this->publishPort = publishPort;
        this->publishHost = publishHost;
    }

    PeerInfo listen(PeerInfo info)
    {
        int port = bindAndReturnOpenTcpPort(acceptor, bindPort, bindHost);

        if (publishPort == -1)
            publishPort = port;
        if (publishHost == "")
            publishHost = bindHost;

        string fromPeersAddress = publishHost + ":" + std::to_string(publishPort);

        acceptor.listen(1000 /* backlog */);

        cerr << "peer " << info.peerName << " listening on " << fromPeersAddress << endl;

        info.uri = fromPeersAddress;

        startAccepting();

        return this->peerInfo = info;
    }

    void shutdown()
    {
        //cerr << "AsioPeerServer shutdown" << endl;
        shutdown_ = true;
        eventLoop.terminate();
        threads.shutdown();
        acceptor.close();
    }

    std::shared_ptr<PeerConnection> connect(const PeerInfo & info)
    {
        shutdown_ = false;

        using namespace boost::asio;

        auto uriParts = ML::split(info.uri, ':');
        ExcAssertEqual(uriParts.size(), 2);
    
        string hostName = uriParts[0];
        string portName = uriParts[1];

        auto & ioService = eventLoop.impl().ioService();
        ip::tcp::resolver resolver(ioService);
        ip::tcp::resolver::query query(hostName, portName);
        boost::system::error_code error;
        ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query, error);

        if (error) {
            // Throw is OK: synchronous context
            throw MLDB::Exception("error resolving peer name " + info.uri + ": "
                                + error.message());
        }
    
        auto sock = std::make_shared<boost::asio::ip::tcp::socket>(ioService);

        cerr << "peer " << peerInfo.peerName << " is connecting to "
             << info.peerName << " at " << info.uri << endl;
    
        boost::asio::connect(*sock, endpoint_iterator, error);

        cerr << "error = " << error.message() << endl;

        if (error) {
            // Throw is OK: synchronous context
            throw MLDB::Exception("error connecting to " + info.uri + ": "
                                + error.message());
        }
    
        cerr << "connected to " << info.uri << endl;

        // Send our handshake over the new connection
        string handshake = "APSv1000" + MLDB::format("%8zd", info.peerName.size())
            + info.peerName;
        boost::asio::write(*sock, boost::asio::buffer(handshake));

        auto result = std::make_shared<AsioPeerConnection>(sock);

        return result;
    }

    void
    acceptPeerConnection(boost::system::error_code error,
                         std::shared_ptr<boost::asio::ip::tcp::socket> sock)
    {
        cerr << "AcceptPeerConnection" << endl;
        if (error) {
            cerr << "Error accepting peer connections: " << error.message() << endl;
        }

        if (shutdown_)
            return;

        if (error) {
            cerr << "FATAL Error accepting peer connections: " << error.message() << endl;
            abort();
        }

        cerr << "Accepted peer connection" << endl;

        // Accept another connection straight away
        startAccepting();

        ExcAssert(sock);

        string version(8, ' ');
        string length(8, ' ');

        size_t bytes = boost::asio::read(*sock, boost::asio::buffer(&version[0], 8));

        if (bytes != 8 || version != "APSv1000") {
            cerr << "WARNING: something attempted to connect to peer socket: handshake "
                 << version << endl;
            return;
        }

        bytes = boost::asio::read(*sock, boost::asio::buffer(&length[0], 8));
        if (bytes != 8) {
            cerr << "WARNING: peer handshake error: length too short" << endl;
            return;
        }
        char * endptr;
        unsigned long long len = strtoll(length.c_str(), &endptr, 10);
        if (*endptr != '\0' || len > 1024) {
            cerr << "WARNING: peer handshake error: length couldn't be parsed: "
                 << length << endl;
            return;
        }

        string peername(len, ' ');
        bytes = boost::asio::read(*sock, boost::asio::buffer(&peername[0], len));
        if (bytes != len) {
            cerr << "WARNING: peer handshake error: peername was truncated"
                 << length << endl;
            return;
        }
        
        if (peername != this->peerInfo.peerName) {
            cerr << "WARNING: peer name error: peername was " << peername
                 << " not " << this->peerInfo.peerName << endl;
            return;
        }

        // OK, passed basic handshaking.  Now pass off the connection

        auto conn = std::make_shared<AsioPeerConnection>(sock);

        {
            std::unique_lock<std::mutex> guard(onNewConnectionMutex);
            onNewConnection(conn);
        }

        if (conn.use_count() == 1)
            cerr << "WARNING: acceptPeerConnection did not take ownership"
                 << endl;
    }

    void startAccepting()
    {
        if (shutdown_)
            return;

        auto & ioService = eventLoop.impl().ioService();
        auto sock = std::make_shared<boost::asio::ip::tcp::socket>(ioService);

        acceptor.async_accept(*sock,
                              std::bind(&Impl::acceptPeerConnection,
                                        this,
                                        std::placeholders::_1,
                                        sock));
        cerr << "Now accepting" << endl;
    }

    WatchT<Date> getTimer(Date expiry, double period,
                          std::function<void (Date)> toBind)
    {
        return MLDB::getTimer(expiry, period, eventLoop, toBind);
    }


    EventLoop eventLoop;
    boost::asio::ip::tcp::acceptor acceptor;
    AsioThreadPool threads;
    
    PortRange bindPort;
    std::string bindHost;
    int publishPort;
    std::string publishHost;

    /// Our local peer information
    PeerInfo peerInfo;
    std::atomic<int> shutdown_;

    std::mutex onNewConnectionMutex;

    /** Called when we got a new connection. */
    std::function<void (std::shared_ptr<PeerConnection>)> onNewConnection;
};


AsioPeerServer::
AsioPeerServer()
    : impl(new Impl(this))
{
}

AsioPeerServer::
~AsioPeerServer()
{
}

void
AsioPeerServer::
init(PortRange bindPort, const std::string & bindHost,
     int publishPort, std::string publishHost)
{
    impl->init(bindPort, bindHost, publishPort, publishHost);
}

PeerInfo
AsioPeerServer::
listen(PeerInfo info)
{
    return impl->listen(info);
}

void
AsioPeerServer::
shutdown()
{
    impl->shutdown();
}

std::shared_ptr<PeerConnection>
AsioPeerServer::
connect(const PeerInfo & info)
{
    return impl->connect(info);
}

std::shared_ptr<PeerConnection>
AsioPeerServer::
connectToSelf()
{
    auto & ioService = impl->eventLoop.impl().ioService();
    return std::make_shared<MirrorPeerConnection>(ioService);
}

void
AsioPeerServer::
postWork(std::function<void ()> work)
{
    impl->eventLoop.post(work);
}

void
AsioPeerServer::
setNewConnectionHandler(std::function<void (std::shared_ptr<PeerConnection>)> onNewConnection)
{
    std::unique_lock<std::mutex> guard(impl->onNewConnectionMutex);
    impl->onNewConnection = onNewConnection;
}

WatchT<Date>
AsioPeerServer::
getTimer(Date expiry, double period,
         std::function<void (Date)> toBind)
{
    return impl->getTimer(expiry, period, toBind);
}

int
AsioPeerServer::
acceptFd() const
{
    return impl->acceptor.native_handle();
}

} // namespace MLDB
