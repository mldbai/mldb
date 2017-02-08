/** in_process_rest_connection.h                                  -*- C++ -*-
 *
    Jeremy Barnes, January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "http_rest_service.h"

namespace MLDB {

/*****************************************************************************/
/* IN PROCESS HTTP REST CONNECTION                                           */
/*****************************************************************************/

struct InProcessRestConnection: public HttpRestConnection {

    InProcessRestConnection();

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

    int responseCode;
    std::string contentType;
    RestParams headers;
    std::string response;

    virtual std::shared_ptr<RestConnection>
    capture(std::function<void ()> onDisconnect);

    virtual std::shared_ptr<RestConnection>
    captureInConnection(std::shared_ptr<void> toCapture);

};

}

