// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** in_process_rest_connection.cc                                  -*- C++ -*-
 *
    Jeremy Barnes, January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "mldb/rest/in_process_rest_connection.h"


using namespace std;


namespace MLDB {

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
sendResponse(int responseCode, std::string response, std::string contentType)
{
    this->responseCode = responseCode;
    this->response = std::move(response);
    this->contentType = std::move(contentType);
}

/** Send the given response back on the connection. */
void InProcessRestConnection::
sendResponse(int responseCode,
             const Json::Value & response, std::string contentType)
{
    this->responseCode = responseCode;
    this->response = response.toStringNoNewLine();
    this->contentType = std::move(contentType);
}

void InProcessRestConnection::
sendRedirect(int responseCode, std::string location)
{
    this->responseCode = responseCode;
    this->headers.emplace_back("location", std::move(location));
}

/** Send an HTTP-only response with the given headers.  If it's not
    an HTTP connection, this will fail.
*/
void InProcessRestConnection::
sendHttpResponse(int responseCode,
                 std::string response, std::string contentType,
                 RestParams headers)
{
    this->responseCode = responseCode;
    this->response = std::move(response);
    this->contentType = std::move(contentType);
    this->headers = std::move(headers);
}

void InProcessRestConnection::
sendHttpResponseHeader(int responseCode,
                       std::string contentType,
                       ssize_t contentLength,
                       RestParams headers)
{
    this->responseCode = responseCode;
    this->contentType = std::move(contentType);
    this->headers = std::move(headers);
}

void InProcessRestConnection::
sendPayload(std::string payload)
{
    this->response += std::move(payload);
}

void InProcessRestConnection::
finishResponse()
{
}

/** Send the given error string back on the connection. */
void InProcessRestConnection::
sendErrorResponse(int responseCode,
                  std::string error, std::string contentType)
{
    this->responseCode = responseCode;
    this->response = std::move(error);
    this->contentType = std::move(contentType);
}

void InProcessRestConnection::
sendErrorResponse(int responseCode, const Json::Value & error)
{
    Json::Value augmentedError;
    Json::ValueType type = error.type();
    if (type == Json::nullValue || type == Json::objectValue) {
        augmentedError = error;
        if ( !error.isMember("httpCode") ){
            augmentedError["httpCode"] = responseCode;
        }
    }
    else {
        augmentedError["error"] = error.asString();
        augmentedError["httpCode"] = responseCode;
    }
    this->responseCode = responseCode;
    this->response = augmentedError.toStringNoNewLine();
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
    throw MLDB::Exception("Capturing not supported");
}

std::shared_ptr<RestConnection> InProcessRestConnection::
captureInConnection(std::shared_ptr<void> toCapture)
{
    throw MLDB::Exception("Capturing not supported");
}

};

