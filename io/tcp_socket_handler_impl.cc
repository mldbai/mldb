// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tcp_socket_handler_impl.cc
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#include <memory>
#include <boost/asio/write.hpp>
#include <boost/system/error_code.hpp>
#include "mldb/io/tcp_socket.h"
#include "mldb/arch/wait_on_address.h"
#include "tcp_socket_impl.h"
#include "tcp_socket_handler_impl.h"

using namespace std;
using namespace boost;
using namespace MLDB;

constexpr auto NOT_CLOSED = 0;
constexpr auto CLOSING = 1;
constexpr auto CLOSED = 2;

/****************************************************************************/
/* TCP HANDLER IMPL                                                         */
/****************************************************************************/

TcpSocketHandlerImpl::
TcpSocketHandlerImpl(TcpSocketHandler & handler, TcpSocket && socket)
    : handler_(handler), socket_(std::move(socket.impl().socket)),
      recvBufferSize_(262144),
      recvBuffer_(new char[recvBufferSize_]),
      closed_(NOT_CLOSED)
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
    int closed_val = NOT_CLOSED;
    if (closed_.compare_exchange_strong(closed_val, CLOSING)) {
        MLDB::wake_by_address(closed_);
        close();
    }
}

void
TcpSocketHandlerImpl::
close()
{
    if (closed_ == CLOSED)
        return;

    socket_.cancel();
    socket_.close();
    closed_ = CLOSED;
    MLDB::wake_by_address(closed_);
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

    boost::asio::post(socket_.get_executor(), std::move(doCloseFn));
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

    // These lambdas capture DataPtr not because they need it, but so that
    // its lifetime exceeds those of the ASIO operations which use it (the
    // const_buffer does not own its data, so we need to ensure that the
    // data is pinned by another mechanism.
    auto writeCompleteCond = [dataPtr] (const system::error_code & ec,
                                   std::size_t written)
    {
        return written == dataPtr->size();
    };

    auto onWriteComplete = [dataPtr, onWritten] (const system::error_code & ec,
                            size_t written)
    {
        if (onWritten) {
            onWritten(ec, written);
        }
    };

    asio::const_buffer writeBuffer(dataPtr->data(), dataPtr->size());

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
