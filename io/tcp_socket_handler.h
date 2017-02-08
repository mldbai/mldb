// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tcp_socket_handler.h                                            -*- C++ -*-
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#pragma once

#include <functional>
#include <string>


namespace boost
{
namespace system
{

/* Forward declarations */
class error_code;

}
}


namespace MLDB {

/* Forward declarations */
struct TcpAcceptor;
struct TcpSocketHandlerImpl;
struct TcpSocket;


/****************************************************************************/
/* TCP SOCKET HANDLER                                                       */
/****************************************************************************/

struct TcpSocketHandler {
    typedef std::function<void ()> OnClose;
    typedef std::function<void (const boost::system::error_code &,
                                size_t)> OnWritten;

    TcpSocketHandler(TcpSocket socket);
    virtual ~TcpSocketHandler();

    /* Initiates the reception of data on the socket, for protocols where the
     * client emits data first. */
    virtual void bootstrap();

    /* Disable the Nagle algorithm. */
    void disableNagle();

    /* Returns the host name of the peer. */
    std::string getPeerName() const;

    /* Returns whether the connection is alive. */
    bool isConnected() const;

    /* Immediately close the connection. */
    void close();

    /* Request the closing of the connection via the handling thread. */
    void requestClose(OnClose onClose = nullptr);

    /* Request the sending of a given payload. */
    void requestWrite(std::string data, OnWritten onWritten = nullptr);

    /* Request the reading of any available data from the socket. */
    void requestReceive();

    /* Virtual base method called when data has been read from the associated
       socket. */
    virtual void onReceivedData(const char * buffer, size_t bufferSize) = 0;

    /* Virtual base method called when a receive error has occurred. */
    virtual void onReceiveError(const boost::system::error_code & ec,
                                size_t bufferSize) = 0;

    /* Declare which acceptor initiated the creation of this handler. For
       internal purpose only. */
    void setAcceptor(TcpAcceptor * acceptor)
    {
        acceptor_ = acceptor;
    }

    /* Deleted functions */
    TcpSocketHandler(const TcpSocketHandler & other) = delete;
    TcpSocketHandler & operator = (const TcpSocketHandler & other) = delete;

protected:
    TcpAcceptor & acceptor()
        const
    {
        return *acceptor_;
    }

private:
    std::unique_ptr<TcpSocketHandlerImpl> impl_;
    TcpAcceptor * acceptor_;
};

} // namespace MLDB
