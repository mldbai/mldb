// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tcp_socket_handler.cc
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#include "mldb/io/tcp_socket_handler_impl.h"
#include "tcp_acceptor.h"
#include "tcp_socket_handler.h"
#include "tcp_socket.h"

using namespace std;
using namespace MLDB;


/****************************************************************************/
/* TCP HANDLER                                                              */
/****************************************************************************/

TcpSocketHandler::
TcpSocketHandler(TcpSocket socket)
    : impl_(new TcpSocketHandlerImpl(*this, std::move(socket)))
{
}

TcpSocketHandler::
~TcpSocketHandler()
{
}

void
TcpSocketHandler::
bootstrap()
{
}

void
TcpSocketHandler::
close()
{
    impl_->close();
    acceptor_->dissociate(this);
}

void
TcpSocketHandler::
requestClose(OnClose onClose)
{
    auto onCloseHandler = [=] () {
        if (onClose) {
            onClose();
        }
        acceptor_->dissociate(this);
    };
    impl_->requestClose(onCloseHandler);
}

void
TcpSocketHandler::
requestReceive()
{
    impl_->requestReceive();
}

void
TcpSocketHandler::
requestWrite(string data, OnWritten onWritten)
{
    impl_->requestWrite(std::move(data), std::move(onWritten));
}

void
TcpSocketHandler::
disableNagle()
{
    impl_->disableNagle();
}

std::string
TcpSocketHandler::
getPeerName()
    const
{
    return impl_->getPeerName();
}

bool
TcpSocketHandler::
isConnected()
    const
{
    return impl_->isConnected();
}
