// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tcp_socket_impl.h                                               -*- C++ -*-
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>


namespace MLDB {

struct TcpSocketImpl {
    TcpSocketImpl(boost::asio::io_context & ioContext)
        : socket(ioContext)
    {
    }

    boost::asio::ip::tcp::socket socket;
};

} // namespace MLDB
