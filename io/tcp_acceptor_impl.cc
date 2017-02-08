// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tcp_acceptor_impl.cc
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#include <iostream>
#include <mutex>

#include "boost/asio/write.hpp"
#include "boost/asio/ip/tcp.hpp"

#include "mldb/base/exc_assert.h"
#include "mldb/types/url.h"
#include "mldb/base/scope.h"
#include "mldb/io/port_range_service.h"
#include "mldb/io/event_loop.h"
#include "mldb/io/tcp_acceptor.h"
#include "mldb/io/tcp_socket.h"
#include "event_loop_impl.h"
#include "tcp_socket_impl.h"
#include "tcp_acceptor_impl.h"


using namespace std;
using namespace boost;
using namespace MLDB;

static asio::ip::tcp::resolver::iterator endIterator;


/****************************************************************************/
/* TCP ACCEPTOR IMPL                                                        */
/****************************************************************************/

TcpAcceptorImpl::
TcpAcceptorImpl(EventLoop & eventLoop,
                TcpAcceptor & frontAcceptor)
    : eventLoop_(eventLoop),
      frontAcceptor_(frontAcceptor),
      v4Endpoint_(eventLoop_.impl().ioService()),
      v6Endpoint_(eventLoop_.impl().ioService()),
      acceptCnt_(0)
{
}

TcpAcceptorImpl::
~TcpAcceptorImpl()
{
    shutdown();
}

void
TcpAcceptorImpl::
listen(const PortRange & portRange, const string & hostname, int backlog)
{
    ExcAssert(!hostname.empty());

    asio::ip::tcp::resolver resolver(eventLoop_.impl().ioService());
    asio::ip::tcp::resolver::query query(hostname,
                                         to_string(portRange.first));

    for (auto it = resolver.resolve(query);
         it != endIterator && !(v4Endpoint_.isOpen() && v6Endpoint_.isOpen());
         it++) {
        auto result = *it;
        auto ep = result.endpoint();
        if (ep.protocol() == asio::ip::tcp::v4()) {
            if (!v4Endpoint_.isOpen()) {
                v4Endpoint_.open(ep, portRange, backlog);
                accept(v4Endpoint_);
            }
        }
        else if (ep.protocol() == asio::ip::tcp::v6()) {
            if (!v6Endpoint_.isOpen()) {
                v6Endpoint_.open(ep, portRange, backlog);
                accept(v6Endpoint_);
            }
        }
    }

    if (!v4Endpoint_.isOpen() && !v6Endpoint_.isOpen()) {
        throw MLDB::Exception("impossible to bind to specified port(s)");
    }
}

void
TcpAcceptorImpl::
shutdown()
{
    v4Endpoint_.close();
    v6Endpoint_.close();
}

void
TcpAcceptorImpl::
accept(TcpAcceptorImpl::Endpoint & endpoint)
{
    auto & acceptor = endpoint.acceptor_;
    auto & loopService = eventLoop_.impl().ioService();
    auto nextSocket = make_shared<TcpSocketImpl>(loopService);
    auto onAcceptFn = [&, nextSocket] (const system::error_code & ec) {
        if (ec) {
            if (acceptor.is_open() || ec != asio::error::operation_aborted) {
                cerr << "exception in accept: " + ec.message() + "\n";
            }
        }
        else {
            TcpSocket frontSocket(std::move(nextSocket));
            auto newConn
                = frontAcceptor_.onNewConnection(std::move(frontSocket));
            frontAcceptor_.associate(std::move(newConn));
            accept(endpoint);
        }
    };
    acceptor.async_accept(nextSocket->socket, onAcceptFn);
}


/****************************************************************************/
/* TCP ACCEPTOR IMPL :: ENDPOINT                                            */
/****************************************************************************/

TcpAcceptorImpl::Endpoint::
Endpoint(boost::asio::io_service & ioService)
    : isOpen_(false), ioService_(ioService), acceptor_(ioService_)
{
}

TcpAcceptorImpl::Endpoint::
~Endpoint()
{
    /* Destructor defined here to avoid issues with std::unique_ptr */
}

void
TcpAcceptorImpl::Endpoint::
open(const asio::ip::tcp::endpoint & asioEndpoint,
     const PortRange & portRange, int backlog)
{
    /* Exception safety: we close the socket if we could not bind it
       appropriately */
    auto cleanupAcceptor = ScopeExit([&] () noexcept { if (!isOpen_) { acceptor_.close(); } });

    asio::ip::tcp::endpoint bindEndpoint = asioEndpoint;
    std::unique_ptr<asio::ip::tcp::acceptor> acceptorPtr;

    while (!isOpen_) {
        bool bound(false);
        for (int i = portRange.first; i < portRange.last; i++) {
            acceptorPtr.reset(new asio::ip::tcp::acceptor(ioService_));
            acceptorPtr->open(bindEndpoint.protocol());
            acceptorPtr->set_option(asio::socket_base::reuse_address(true));
            bindEndpoint.port(i);
            system::error_code ec;
            acceptorPtr->bind(bindEndpoint, ec);
            if (!ec) {
                bound = true;
                break;
            }
            if (ec != asio::error::address_in_use) {
                throw MLDB::Exception("error binding socket: " + ec.message());
            }
        }
        if (bound) {
            try {
                MLDB_TRACE_EXCEPTIONS(false);
                acceptorPtr->listen(backlog);
                acceptor_ = std::move(*acceptorPtr);
                isOpen_ = true;
            }
            catch (const system::system_error & error) {
                if (error.code() != asio::error::address_in_use) {
                    throw;
                }
            }
        }
    }
}

void
TcpAcceptorImpl::Endpoint::
close()
{
    acceptor_.close();
    isOpen_ = false;
}

int
TcpAcceptorImpl::Endpoint::
effectivePort()
    const
{
    if (!isOpen_) {
        return -1;
    }

    auto ep = acceptor_.local_endpoint();
    return ep.port();
}
