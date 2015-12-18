// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* http_socket_handler.cc
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 Datacratic.  All rights reserved.
*/

#include "boost/system/error_code.hpp"
#include "boost/asio/error.hpp"
#include "http_socket_handler.h"
#include "tcp_socket.h"

using namespace std;
using namespace Datacratic;


/****************************************************************************/
/* HTTP HANDLER                                                             */
/****************************************************************************/

HttpSocketHandler::
HttpSocketHandler(TcpSocket socket)
    : TcpSocketHandler(std::move(socket))
{
    parser_.onRequestStart = [&] (const char * methodData, size_t methodSize,
                                  const char * urlData, size_t urlSize,
                                  const char * versionData,
                                  size_t versionSize) {
        this->onRequestStart(methodData, methodSize, urlData, urlSize,
                             versionData, versionSize);
    };
    parser_.onHeader = [&] (const char * data, size_t dataSize) {
        this->onHeader(data, dataSize);
    };
    parser_.onData = [&] (const char * data, size_t dataSize) {
        this->onData(data, dataSize);
    };
    parser_.onDone = [&] (bool shouldClose) {
        this->onDone(shouldClose);
    };
}

void
HttpSocketHandler::
bootstrap()
{
    disableNagle();
    requestReceive();
}

void
HttpSocketHandler::
onReceivedData(const char * data, size_t size)
{
    try {
        parser_.feed(data, size);
        requestReceive();
    }
    catch (const ML::Exception & exc) {
        requestClose();
    }
}

void
HttpSocketHandler::
onReceiveError(const boost::system::error_code & ec, size_t bufferSize)
{
    if (ec == boost::system::errc::connection_reset
        || ec == boost::asio::error::eof) {
        requestClose();
    }
    else {
        throw ML::Exception("unhandled error: " + ec.message());
    }
}


/****************************************************************************/
/* HTTP CLASSIC HANDLER                                                     */
/****************************************************************************/

HttpLegacySocketHandler::
HttpLegacySocketHandler(TcpSocket && socket)
    : HttpSocketHandler(std::move(socket)), bodyStarted_(false)
{
}

void
HttpLegacySocketHandler::
send(std::string str,
     NextAction action, OnWriteFinished onWriteFinished)
{
    if (str.size() > 0) {
        auto onWritten = [=] (const boost::system::error_code & ec,
                              size_t) {
            if (onWriteFinished) {
                onWriteFinished();
            }
            if (action == NEXT_CLOSE || action == NEXT_RECYCLE) {
                requestClose();
            }
        };
        requestWrite(std::move(str), onWritten);
    }
}

void
HttpLegacySocketHandler::
putResponseOnWire(HttpResponse response,
                  std::function<void ()> onSendFinished,
                  NextAction next)
{
    string responseStr;
    responseStr.reserve(16384 + response.body.length());

    responseStr.append("HTTP/1.1 ");
    responseStr.append(to_string(response.responseCode));
    responseStr.append(" ");
    responseStr.append(response.responseStatus);
    responseStr.append("\r\n");

    if (response.contentType != "") {
        responseStr.append("Content-Type: ");
        responseStr.append(std::move(response.contentType));
        responseStr.append("\r\n");
    }

    if (response.sendBody) {
        responseStr.append("Content-Length: ");
        responseStr.append(to_string(response.body.length()));
        responseStr.append("\r\n");
        responseStr.append("Connection: Keep-Alive\r\n");
    }

    for (auto & h: response.extraHeaders) {
        responseStr.append(std::move(h.first));
        responseStr.append(": ");
        responseStr.append(std::move(h.second));
        responseStr.append("\r\n");
    }

    responseStr.append("\r\n");
    responseStr.append(std::move(response.body));

    send(std::move(responseStr), next, onSendFinished);
}

void
HttpLegacySocketHandler::
onRequestStart(const char * methodData, size_t methodSize,
               const char * urlData, size_t urlSize,
               const char * versionData, size_t versionSize)
{
    headerPayload.reserve(8192);
    headerPayload.append(methodData, methodSize);
    headerPayload.append(" ", 1);
    headerPayload.append(urlData, urlSize);
    headerPayload.append(" ", 1);
    headerPayload.append(versionData, versionSize);
    headerPayload.append("\r\n", 2);
}

void
HttpLegacySocketHandler::
onHeader(const char * data, size_t size)
{
    headerPayload.append(data, size);
}

void
HttpLegacySocketHandler::
onData(const char * data, size_t size)
{
    if (!bodyStarted_) {
        bodyStarted_ = true;
    }
    bodyPayload.append(data, size);
}

void
HttpLegacySocketHandler::
onDone(bool requireClose)
{
    HttpHeader header;
    header.parse(headerPayload);
    handleHttpPayload(header, bodyPayload);
    headerPayload.clear();
    bodyPayload.clear();
    bodyStarted_ = false;
}
