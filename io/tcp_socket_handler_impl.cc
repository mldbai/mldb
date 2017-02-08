// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tcp_socket_handler_impl.cc
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#include <memory>
#include <boost/asio/write.hpp>
#include <boost/system/error_code.hpp>
#include "mldb/io/tcp_socket.h"
#include "tcp_socket_impl.h"
#include "tcp_socket_handler_impl.h"

using namespace std;
using namespace boost;
using namespace MLDB;


/****************************************************************************/
/* TCP HANDLER IMPL                                                         */
/****************************************************************************/

TcpSocketHandlerImpl::
TcpSocketHandlerImpl(TcpSocketHandler & handler, TcpSocket && socket)
    : handler_(handler), socket_(std::move(socket.impl().socket)),
      recvBufferSize_(262144),
      recvBuffer_(new char[recvBufferSize_]),
      closed_(false)
{
    onReadSome_ = [&] (const system::error_code & ec, size_t bufferSize) {
        if (ec) {
            if (!(closed_ && ec == boost::asio::error::operation_aborted)) {
                handler_.onReceiveError(ec, bufferSize);
            }
        }
        else {
            handler_.onReceivedData(recvBuffer_.get(), bufferSize);
        }
    };
}

TcpSocketHandlerImpl::
~TcpSocketHandlerImpl()
{
}

void
TcpSocketHandlerImpl::
close()
{
    socket_.close();
    closed_ = true;
}

void
TcpSocketHandlerImpl::
requestClose(TcpSocketHandler::OnClose onClose)
{
    auto doCloseFn = [=] () {
        close();
        if (onClose) {
            onClose();
        }
    };
    socket_.get_io_service().post(doCloseFn);
}

void
TcpSocketHandlerImpl::
requestReceive()
{
    socket_.async_read_some(asio::buffer(recvBuffer_.get(), recvBufferSize_),
                            onReadSome_);
}

void
TcpSocketHandlerImpl::
requestWrite(string data, TcpSocketHandler::OnWritten onWritten)
{
    auto dataPtr = std::make_shared<std::string>(std::move(data));
    auto writeCompleteCond = [=] (const system::error_code & ec,
                                  std::size_t written) {
        return written == dataPtr->size();
    };
    auto onWriteComplete = [=] (const system::error_code & ec,
                                size_t written)
        mutable
    {
        if (onWritten) {
            onWritten(ec, written);
        }
        (void) dataPtr;
    };
    asio::const_buffers_1 writeBuffer(dataPtr->c_str(), dataPtr->size());
    async_write(socket_, writeBuffer, writeCompleteCond, onWriteComplete);
}

void
TcpSocketHandlerImpl::
disableNagle()
{
    asio::ip::tcp::no_delay option(true);
    socket_.set_option(option);
}

std::string
TcpSocketHandlerImpl::
getPeerName()
    const
{
    return socket_.remote_endpoint().address().to_string();
}

bool
TcpSocketHandlerImpl::
isConnected()
    const
{
    return socket_.is_open();
}
