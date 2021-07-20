/** in_process_rest_connection.h                                  -*- C++ -*-

    Jeremy Barnes, January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "http_rest_service.h"
#include <memory>

namespace MLDB {

/*****************************************************************************/
/* IN PROCESS HTTP REST CONNECTION                                           */
/*****************************************************************************/

struct InProcessRestConnection
    : public HttpRestConnection,
      public std::enable_shared_from_this<InProcessRestConnection> {

private:    
    InProcessRestConnection();

public:
    // Use instead of constructor; only the shared_ptr version is
    // available as the capture() functionality requires a shared
    // pointer.
    static std::shared_ptr<InProcessRestConnection> create();

    virtual ~InProcessRestConnection() override;

    using RestConnection::sendResponse;

    /** Send the given response back on the connection. */
    virtual void sendResponse(int responseCode,
                              Utf8String response,
                              std::string contentType) override;

    /** Send the given response back on the connection. */
    virtual void
    sendJsonResponse(int responseCode,
                     const Json::Value & response,
                     std::string contentType = "application/json") override;

    virtual void sendRedirect(int responseCode, std::string location) override;

    /** Send an HTTP-only response with the given headers.  If it's not
        an HTTP connection, this will fail.
    */
    virtual void sendHttpResponse(int responseCode,
                                  Utf8String response, std::string contentType,
                                  RestParams headers) override;

    virtual void
    sendHttpResponseHeader(int responseCode,
                           std::string contentType, ssize_t contentLength,
                           RestParams headers = RestParams()) override;

    virtual void sendPayload(Utf8String payload) override;

    virtual void finishResponse() override;

    /** Send the given error string back on the connection. */
    virtual void sendErrorResponse(int responseCode,
                                   Utf8String error,
                                   std::string contentType) override;

    using RestConnection::sendErrorResponse;

    virtual void sendJsonErrorResponse(int responseCode, const Json::Value & error) override;
    virtual bool responseSent() const override;
    virtual bool isConnected() const override;

    int responseCode() const;
    const std::string & contentType() const;
    const RestParams & headers() const;
    const Utf8String & response() const;
    Utf8String stealResponse();

    virtual std::shared_ptr<RestConnection>
    capture(std::function<void ()> onDisconnect = nullptr) override;

    virtual std::shared_ptr<RestConnection>
    captureInConnection(std::shared_ptr<void> toCapture = nullptr) override;

    // In the case of a captured connection, this will block until the full
    // response has been received.  This is used to deal with asynchronous
    // responses.
    virtual void waitForResponse();

private:
    struct Itl;
    std::unique_ptr<Itl> itl;
};

} // namespace MLDB


