// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tcp_socket_handler_impl.h                                       -*- C++ -*-
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#pragma once

#include <atomic>
#include <string>
#include <boost/asio/ip/tcp.hpp>
#include "mldb/io/tcp_socket_handler.h"


namespace MLDB {

/* Forward declarations */
struct TcpSocket;


/****************************************************************************/
/* TCP CONNECTION HANDLER IMPL                                              */
/****************************************************************************/

struct TcpSocketHandlerImpl {
    TcpSocketHandlerImpl(TcpSocketHandler & handler,
                             TcpSocket && socket);
    virtual ~TcpSocketHandlerImpl();

    /* Disable the Nagle algorithm. */
    void disableNagle();

    /* Returns the host name of the peer. */
    std::string getPeerName() const;

    bool isConnected() const;

    /* Immediately close the connection. */
    void close();

    /* Request the closing of the connection via the handling thread. */
    void requestClose(TcpSocketHandler::OnClose onClose = nullptr);

    /* Request the sending of a given payload. */
    void requestWrite(std::string data,
                      TcpSocketHandler::OnWritten onWritten = nullptr);

    /* Request the reading of any available data from the socket. */
    void requestReceive();

    TcpSocketHandlerImpl(const TcpSocketHandlerImpl & other) = delete;
    TcpSocketHandlerImpl &
        operator = (const TcpSocketHandlerImpl & other) = delete;

private:
    TcpSocketHandler & handler_;
    boost::asio::ip::tcp::socket socket_;

    size_t recvBufferSize_;

    std::unique_ptr<char[]> recvBuffer_;

    typedef std::function<void(const boost::system::error_code & ec,
                               size_t bufferSize)> OnReadSome;
    OnReadSome onReadSome_;
    std::atomic<bool> closed_;
};

} // namespace MLDB
