// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tcp_acceptor.h                                                  -*- C++ -*-
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include "mldb/io/tcp_socket.h"


namespace MLDB {

/* Forward declarations */
struct PortRange;
struct TcpSocketHandler;
struct EventLoop;
struct TcpAcceptorImpl;


/****************************************************************************/
/* TCP ACCEPTOR                                                             */
/****************************************************************************/

/* A class that listens on TCP ports and invokes a handler factory function
 * upon connection. */

struct TcpAcceptor {
    /* A type of function that is invoked upon connection and which returns a
       handler for the given socket. */
    typedef std::function<std::shared_ptr<TcpSocketHandler> (TcpSocket &&)>
        OnNewConnection;

    TcpAcceptor(EventLoop & eventLoop,
                const OnNewConnection & onNewConnection);
    ~TcpAcceptor();

    /* Starts listening on either of the given ports (in ascending order) and
     * interface. Returns the effective port of the listening socket. */
    void listen(const PortRange & portRange,
                const std::string & hostname = "localhost",
                int backlog = 128);

    /* Shutdowns the worker threads (except the main listening thread) as well
     * as the listening socket. */
    void shutdown();

    /* Returns the port used effectively for listening. -1 indicates that the
     * port is not open. */
    int effectiveTCPv4Port() const;
    int effectiveTCPv6Port() const;

    virtual std::shared_ptr<TcpSocketHandler> onNewConnection(TcpSocket
                                                              && socket);

    /* Associate and retain ownership of the given handler. For internal
       purpose only. */
    void associate(std::shared_ptr<TcpSocketHandler> handler);

    /* Dissociate and the given handler. For internal purpose only. */
    void dissociate(TcpSocketHandler * handler);

    /* Associate and retain ownership of the given handler. For internal
       purpose only. */
    std::shared_ptr<TcpSocketHandler> findHandlerPtr(TcpSocketHandler
                                                     * handler) const;

private:
    EventLoop & eventLoop_;
    OnNewConnection onNewConnection_;
    std::unique_ptr<TcpAcceptorImpl> impl_;

    mutable std::mutex associatedHandlersLock_;
    std::set<std::shared_ptr<TcpSocketHandler> > associatedHandlers_;
};

} // namespace MLDB
