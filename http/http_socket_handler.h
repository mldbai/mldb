/* http_socket_handler.h - This file is part of MLDB               -*- C++ -*-
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   TCP handler classes for an HTTP service
*/

#pragma once

#include "mldb/ext/jsoncpp/value.h"
#include "mldb/http/http_header.h"
#include "mldb/http/http_parsers.h"
#include "mldb/io/tcp_socket_handler.h"


namespace MLDB {

/****************************************************************************/
/* HTTP CONNECTION HANDLER                                                  */
/****************************************************************************/

/* A base class for handling HTTP connections. */

struct HttpSocketHandler : public TcpSocketHandler {
    HttpSocketHandler(TcpSocket socket);

    /* Callback used when to report the request line. */
    virtual void onRequestStart(const char * methodData, size_t methodSize,
                                const char * urlData, size_t urlSize,
                                const char * versionData,
                                size_t versionSize) = 0;

    /* Callback used to report a header-line, including the header key and the
     * value. */
    virtual void onHeader(const char * data, size_t dataSize) = 0;

    /* Callback used to request whether to proceed with a 100-continue
     * request. */
    virtual bool onExpect100Continue() = 0;

    /* Callback used to report a chunk of the response body. Only invoked
       when the body is larger than 0 byte. */
    virtual void onData(const char * data, size_t dataSize) = 0;

    /* Callback used to report the end of a response. */
    virtual void onDone(bool requireClose) = 0;

private:
    /* TcpSocketHandler interface */
    virtual void bootstrap();
    virtual void onReceivedData(const char * buffer, size_t bufferSize);
    virtual void onReceiveError(const boost::system::error_code & ec,
                                size_t bufferSize);

    HttpRequestParser parser_;
};


/*****************************************************************************/
/* HTTP RESPONSE                                                             */
/*****************************************************************************/

/** Structure used to return an HTTP response.
    TODO: make use of the the HttpHeader class
*/

struct HttpResponse {
    HttpResponse(int responseCode,
                 std::string contentType,
                 std::string body,
                 std::vector<std::pair<std::string, std::string> > extraHeaders
                     = std::vector<std::pair<std::string, std::string> >());

    /** Construct an HTTP response header only, with no body.  No content-
        length will be inferred. */

    HttpResponse(int responseCode,
                 std::string contentType,
                 std::vector<std::pair<std::string, std::string> > extraHeaders
                     = std::vector<std::pair<std::string, std::string> >());

    HttpResponse(int responseCode,
                 Json::Value body,
                 std::vector<std::pair<std::string, std::string> > extraHeaders
                     = std::vector<std::pair<std::string, std::string> >());

    int responseCode;
    std::string responseStatus;
    std::string contentType;
    std::string body;
    std::vector<std::pair<std::string, std::string> > extraHeaders;
    bool sendBody;
};


/****************************************************************************/
/* HTTP LEGACY CONNECTION HANDLER                                           */
/****************************************************************************/

/* A drop-in replacement class for PassiveSocketHandler. So that old
 * handler code can easily plugged into the recent versions of the service
 * classes. */

struct HttpLegacySocketHandler : public HttpSocketHandler {
    /** Action to perform once we've finished sending. */
    enum NextAction {
        NEXT_CLOSE,
        NEXT_RECYCLE,
        NEXT_CONTINUE
    };

    /* Type of function called when a write operation has finished. */
    typedef std::function<void ()> OnWriteFinished;

    HttpLegacySocketHandler(TcpSocket && socket);

    virtual void handleHttpPayload(const HttpHeader & header,
                                   const std::string & payload) = 0;

    void putResponseOnWire(const HttpResponse & response,
                           std::function<void ()> onSendFinished
                           = std::function<void ()>(),
                           NextAction next = NEXT_CONTINUE);
    void send(std::string str,
              NextAction action = NEXT_CONTINUE,
              OnWriteFinished onWriteFinished = nullptr);

protected:
    /* Overridable method returning whether the given request should be
       accepted for processing or not. The default implementation returns
       "true". */
    virtual bool shouldReturn100Continue(const HttpHeader & header);

private:
    virtual void onRequestStart(const char * methodData, size_t methodSize,
                                const char * urlData, size_t urlSize,
                                const char * versionData,
                                size_t versionSize);
    virtual void onHeader(const char * data, size_t dataSize);
    virtual bool onExpect100Continue();
    virtual void onData(const char * data, size_t dataSize);
    virtual void onDone(bool requireClose);

    void handleExpect100Continue();

    std::string headerPayload;
    std::string bodyPayload;
    bool bodyStarted_;

    std::string writeData_;
};

} // namespace MLDB
