// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tcp_acceptor.cc
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#include <iostream>
#include <mutex>

#include "mldb/arch/exception.h"
#include "mldb/base/exc_assert.h"
#include "mldb/io/tcp_acceptor_impl.h"
#include "event_loop.h"
#include "port_range_service.h"
#include "tcp_socket_handler.h"
#include "tcp_acceptor.h"

using namespace std;
using namespace MLDB;


/****************************************************************************/
/* TCP ACCEPTOR                                                             */
/****************************************************************************/

TcpAcceptor::
TcpAcceptor(EventLoop & eventLoop, const OnNewConnection & onNewConnection)
    : eventLoop_(eventLoop),
      onNewConnection_(onNewConnection),
      impl_(new TcpAcceptorImpl(eventLoop_, *this))
{
}

TcpAcceptor::
~TcpAcceptor()
{
    shutdown();
}

void
TcpAcceptor::
listen(const PortRange & portRange, const string & hostname, int backlog)
{
    impl_->listen(portRange, hostname, backlog);
}

int
TcpAcceptor::
effectiveTCPv4Port() const
{
    return impl_->effectiveTCPv4Port();
}

int
TcpAcceptor::
effectiveTCPv6Port() const
{
    return impl_->effectiveTCPv6Port();
}

void
TcpAcceptor::
shutdown()
{
    impl_->shutdown();
}

std::shared_ptr<TcpSocketHandler>
TcpAcceptor::
onNewConnection(TcpSocket && socket)
{
    return onNewConnection_(std::move(socket));
}

void
TcpAcceptor::
associate(std::shared_ptr<TcpSocketHandler> handler)
{
    auto doAssociate = [handler, this] () {
        std::unique_lock<std::mutex> guard(associatedHandlersLock_);
        associatedHandlers_.insert(handler);
        handler->setAcceptor(this);
        handler->bootstrap();
    };
    eventLoop_.post(doAssociate);
}

std::shared_ptr<TcpSocketHandler>
TcpAcceptor::
findHandlerPtr(TcpSocketHandler * handler)
    const
{
    auto result = tryFindHandlerPtr(handler);
    if (result) return result;
    throw MLDB::Exception("socket handler not found");
}

std::shared_ptr<TcpSocketHandler>
TcpAcceptor::
tryFindHandlerPtr(TcpSocketHandler * handler)
    const
{
    std::unique_lock<std::mutex> guard(associatedHandlersLock_);
    for (auto & handlerPtr: associatedHandlers_) {
        if (handlerPtr.get() == handler) {
            return handlerPtr;
        }
    }
    return nullptr;
}

/* FIXME: inefficient implementation */
bool
TcpAcceptor::
dissociate(TcpSocketHandler * handler)
{
    auto handlerPtr = tryFindHandlerPtr(handler);
    if (!handlerPtr)
        return false;
    auto doDissociate = [=,this] {
        std::unique_lock<std::mutex> guard(associatedHandlersLock_);
        associatedHandlers_.erase(handlerPtr);
    };
    eventLoop_.post(doDissociate);
    return true;
}
