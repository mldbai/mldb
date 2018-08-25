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

    virtual ~InProcessRestConnection();

    using RestConnection::sendResponse;

    /** Send the given response back on the connection. */
    virtual void sendResponse(int responseCode,
                              std::string response,
                              std::string contentType);

    /** Send the given response back on the connection. */
    virtual void
    sendResponse(int responseCode,
                 const Json::Value & response,
                 std::string contentType = "application/json");

    virtual void sendRedirect(int responseCode, std::string location);

    /** Send an HTTP-only response with the given headers.  If it's not
        an HTTP connection, this will fail.
    */
    virtual void sendHttpResponse(int responseCode,
                                  std::string response, std::string contentType,
                                  RestParams headers);

    virtual void
    sendHttpResponseHeader(int responseCode,
                           std::string contentType, ssize_t contentLength,
                           RestParams headers = RestParams());

    virtual void sendPayload(std::string payload);

    virtual void finishResponse();

    /** Send the given error string back on the connection. */
    virtual void sendErrorResponse(int responseCode,
                                   std::string error,
                                   std::string contentType);

    using RestConnection::sendErrorResponse;

    virtual void sendErrorResponse(int responseCode, const Json::Value & error);
    virtual bool responseSent() const;
    virtual bool isConnected() const;

    int responseCode() const;
    const std::string & contentType() const;
    const RestParams & headers() const;
    const std::string & response() const;
    std::string stealResponse();

    virtual std::shared_ptr<RestConnection>
    capture(std::function<void ()> onDisconnect = nullptr);

    virtual std::shared_ptr<RestConnection>
    captureInConnection(std::shared_ptr<void> toCapture = nullptr);

    // In the case of a captured connection, this will block until the full
    // response has been received.  This is used to deal with asynchronous
    // responses.
    virtual void waitForResponse();

private:
    struct Itl;
    std::unique_ptr<Itl> itl;
};

} // namespace MLDB


