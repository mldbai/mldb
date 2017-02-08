// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tcp_acceptor_impl.h                                             -*- C++ -*-
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#pragma once

#include <memory>
#include <functional>
#include <set>
#include <string>
#include <vector>

#include <boost/asio/ip/tcp.hpp>
#include "mldb/io/tcp_acceptor.h"


namespace MLDB {

/* Forward declarations */
struct EventLoop;
struct PortRange;
struct TcpSocketImpl;


/****************************************************************************/
/* TCP ACCEPTOR IMPL                                                        */
/****************************************************************************/

/* A class that listens on TCPv4 ports invokes a handler factory function upon
 * connection. */

struct TcpAcceptorImpl {
    TcpAcceptorImpl(EventLoop & parentLoop, TcpAcceptor & frontAcceptor);
    virtual ~TcpAcceptorImpl();

    /* Starts listening on the first available of the given ports (in
     * ascending order) and interface. */
    void listen(const PortRange & portRange, const std::string & hostname,
                int backlog);

    /* Shutdowns the worker threads (except the main listening thread) as well
     * as the listening socket. */
    void shutdown();

    /* Returns the port used effectively for listening. */
    int effectiveTCPv4Port()
        const
    {
        return v4Endpoint_.effectivePort();
    }
    int effectiveTCPv6Port()
        const
    {
        return v6Endpoint_.effectivePort();
    }

private:
    struct Endpoint {
        Endpoint(boost::asio::io_service & ioService);
        ~Endpoint();

        void open(const boost::asio::ip::tcp::endpoint & resolverEntry,
                  const PortRange & portRange,
                  int backlog);
        void close();
        void accept();
        bool isOpen()
            const
        {
            return isOpen_;
        }
        int effectivePort() const;

        bool isOpen_;
        boost::asio::io_service & ioService_;
        boost::asio::ip::tcp::acceptor acceptor_;
    };

    void accept(Endpoint & endpoint);

    EventLoop & eventLoop_;
    TcpAcceptor & frontAcceptor_;

    Endpoint v4Endpoint_;
    Endpoint v6Endpoint_;
    int acceptCnt_;
};

} // namespace MLDB
