// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** in_process_rest_connection.cc                                  -*- C++ -*-
 *
    Jeremy Barnes, January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "mldb/rest/in_process_rest_connection.h"


using namespace std;


namespace Datacratic {

/*****************************************************************************/
/* IN PROCESS HTTP REST CONNECTION                                           */
/*****************************************************************************/

InProcessRestConnection::
InProcessRestConnection()
        : responseCode(-1)
{
}

InProcessRestConnection::
~InProcessRestConnection()
{
}

/** Send the given response back on the connection. */
void InProcessRestConnection::
sendResponse(int responseCode,
            const std::string & response,
            const std::string & contentType)
{
    this->responseCode = responseCode;
    this->response = response;
    this->contentType = contentType;
}

/** Send the given response back on the connection. */
void InProcessRestConnection::
sendResponse(int responseCode,
             const Json::Value & response,
             const std::string & contentType)
{
    this->responseCode = responseCode;
    this->response = response.toStringNoNewLine();
    this->contentType = contentType;
}

void InProcessRestConnection::
sendRedirect(int responseCode,
            const std::string & location)
{
    this->responseCode = responseCode;
    this->headers.emplace_back("location", location);
}

/** Send an HTTP-only response with the given headers.  If it's not
    an HTTP connection, this will fail.
*/
void InProcessRestConnection::
sendHttpResponse(int responseCode,
                 const std::string & response,
                 const std::string & contentType,
                 const RestParams & headers)
{
    this->responseCode = responseCode;
    this->response = response;
    this->contentType = contentType;
    this->headers = headers;
}

void InProcessRestConnection::
sendHttpResponseHeader(int responseCode,
                       const std::string & contentType,
                       ssize_t contentLength,
                       const RestParams & headers)
{
    this->responseCode = responseCode;
    this->contentType = contentType;
    this->headers = headers;
}

void InProcessRestConnection::
sendPayload(const std::string & payload)
{
    this->response += payload;
}

void InProcessRestConnection::
finishResponse()
{
}

/** Send the given error string back on the connection. */
void InProcessRestConnection::
sendErrorResponse(int responseCode,
                  const std::string & error,
                  const std::string & contentType)
{
    this->responseCode = responseCode;
    this->response = error;
    this->contentType = contentType;
}

void InProcessRestConnection::
sendErrorResponse(int responseCode, const Json::Value & error)
{
    this->responseCode = responseCode;
    this->response = error.toStringNoNewLine();
    this->contentType = "application/json";
}

bool InProcessRestConnection::
responseSent() const
{
    return responseCode != -1;
}

bool InProcessRestConnection::
isConnected() const
{
    return true;
}

std::shared_ptr<RestConnection> InProcessRestConnection::
capture(std::function<void ()> onDisconnect)
{
    throw ML::Exception("Capturing not supported");
}

std::shared_ptr<RestConnection> InProcessRestConnection::
captureInConnection(std::shared_ptr<void> toCapture)
{
    throw ML::Exception("Capturing not supported");
}

};

