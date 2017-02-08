// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tcp_socket.cc
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/


#include "mldb/io/tcp_socket_impl.h"
#include "mldb/io/tcp_socket.h"


using namespace MLDB;

TcpSocket::
TcpSocket(std::shared_ptr<TcpSocketImpl> impl)
    : impl_(std::move(impl))
{
}

TcpSocket::
~TcpSocket()
{
}
