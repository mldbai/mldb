/** in_process_rest_connection.cc                                  -*- C++ -*-
 
    Jeremy Barnes, January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#include "mldb/rest/in_process_rest_connection.h"
#include <atomic>
#include <mutex>
#include <condition_variable>


using namespace std;


namespace MLDB {

/*****************************************************************************/
/* IN PROCESS HTTP REST CONNECTION                                           */
/*****************************************************************************/

struct InProcessRestConnection::Itl {
    std::mutex responseMutex;
    std::condition_variable cv;

    int responseCode = -1;
    std::string contentType;
    RestParams headers;
    std::string response;

    enum State {
        NOTHING_SENT,
        HEADER_SENT,
        ALL_SENT
    } state = NOTHING_SENT;
    
    void startResponse()
    {
        ExcAssertEqual(state, NOTHING_SENT);
    }
    
    void finishResponse(std::unique_lock<std::mutex> & guard)
    {
        ExcAssertEqual(state, HEADER_SENT);
        state = ALL_SENT;
        guard.unlock();
        cv.notify_all();
    }

    void finishHeader(std::unique_lock<std::mutex> & guard)
    {
        ExcAssertEqual(state, NOTHING_SENT);
        state = HEADER_SENT;
    }

    void continueResponse()
    {
        ExcAssertEqual(state, HEADER_SENT);
    }

    bool responseSent()
    {
        return state == ALL_SENT;
    }

    void assertResponseFinished()
    {
        ExcAssertEqual(state, ALL_SENT);
    }

    void waitForResponse(std::unique_lock<std::mutex> & guard)
    {
        cv.wait(guard, [this] () { return this->state == ALL_SENT; });
    }
};


InProcessRestConnection::
InProcessRestConnection()
    : itl(new Itl())
{
}

std::shared_ptr<InProcessRestConnection>
InProcessRestConnection::
create()
{
    return std::shared_ptr<InProcessRestConnection>
        (new InProcessRestConnection());
}

InProcessRestConnection::
~InProcessRestConnection()
{
}

void
InProcessRestConnection::
sendResponse(int responseCode, std::string response, std::string contentType)
{
    std::unique_lock<std::mutex> guard(itl->responseMutex);
    itl->startResponse();
    itl->responseCode = responseCode;
    itl->contentType = std::move(contentType);
    itl->finishHeader(guard);
    itl->response = std::move(response);
    itl->finishResponse(guard);
}

void
InProcessRestConnection::
sendResponse(int responseCode,
             const Json::Value & response,
             std::string contentType)
{
    std::unique_lock<std::mutex> guard(itl->responseMutex);
    itl->startResponse();
    itl->responseCode = responseCode;
    itl->contentType = std::move(contentType);
    itl->finishHeader(guard);
    itl->response = response.toStringNoNewLine();
    itl->finishResponse(guard);
}

void
InProcessRestConnection::
sendRedirect(int responseCode, std::string location)
{
    std::unique_lock<std::mutex> guard(itl->responseMutex);
    itl->startResponse();
    itl->responseCode = responseCode;
    itl->headers.emplace_back("location", std::move(location));
    itl->finishHeader(guard);
    itl->finishResponse(guard);
}

void
InProcessRestConnection::
sendHttpResponse(int responseCode,
                 std::string response, std::string contentType,
                 RestParams headers)
{
    std::unique_lock<std::mutex> guard(itl->responseMutex);
    itl->startResponse();
    itl->responseCode = responseCode;
    itl->contentType = std::move(contentType);
    itl->headers = std::move(headers);
    itl->finishHeader(guard);
    itl->response = std::move(response);
    itl->finishResponse(guard);
}

void
InProcessRestConnection::
sendHttpResponseHeader(int responseCode,
                       std::string contentType,
                       ssize_t contentLength,
                       RestParams headers)
{
    std::unique_lock<std::mutex> guard(itl->responseMutex);
    itl->startResponse();
    itl->responseCode = responseCode;
    itl->contentType = std::move(contentType);
    itl->headers = std::move(headers);
    itl->finishHeader(guard);
}

void
InProcessRestConnection::
sendPayload(std::string payload)
{
    std::unique_lock<std::mutex> guard(itl->responseMutex);
    itl->continueResponse();
    itl->response += std::move(payload);
}

void
InProcessRestConnection::
finishResponse()
{
    std::unique_lock<std::mutex> guard(itl->responseMutex);
    itl->finishResponse(guard);
}

void InProcessRestConnection::
sendErrorResponse(int responseCode,
                  std::string error, std::string contentType)
{
    std::unique_lock<std::mutex> guard(itl->responseMutex);
    itl->startResponse();
    itl->responseCode = responseCode;
    itl->contentType = std::move(contentType);
    itl->finishHeader(guard);
    itl->response = std::move(error);
    itl->finishResponse(guard);
}

void InProcessRestConnection::
sendErrorResponse(int responseCode, const Json::Value & error)
{
    std::unique_lock<std::mutex> guard(itl->responseMutex);
    itl->startResponse();
    itl->responseCode = responseCode;
    itl->contentType = "application/json";
    itl->finishHeader(guard);
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
    itl->response = augmentedError.toStringNoNewLine();
    itl->finishResponse(guard);
}

bool
InProcessRestConnection::
responseSent() const
{
    return itl->responseSent();
}

bool
InProcessRestConnection::
isConnected() const
{
    return true;
}

int
InProcessRestConnection::
responseCode() const
{
    std::unique_lock<std::mutex> guard(itl->responseMutex);
    itl->assertResponseFinished();
    return itl->responseCode;
}

const std::string &
InProcessRestConnection::
contentType() const
{
    std::unique_lock<std::mutex> guard(itl->responseMutex);
    itl->assertResponseFinished();
    return itl->contentType;
}

const RestParams &
InProcessRestConnection::
headers() const
{
    std::unique_lock<std::mutex> guard(itl->responseMutex);
    itl->assertResponseFinished();
    return itl->headers;
}

const std::string &
InProcessRestConnection::
response() const
{
    std::unique_lock<std::mutex> guard(itl->responseMutex);
    itl->assertResponseFinished();
    return itl->response;
}

std::string
InProcessRestConnection::
stealResponse()
{
    std::unique_lock<std::mutex> guard(itl->responseMutex);
    itl->assertResponseFinished();
    return std::move(itl->response);
}

std::shared_ptr<RestConnection>
InProcessRestConnection::
capture(std::function<void ()> onDisconnect)
{
    auto captured = this->shared_from_this();
    auto onFinish = [captured, onDisconnect=std::move(onDisconnect)]
        (RestConnection *)
        {
                onDisconnect();
        };

    return std::shared_ptr<RestConnection>(captured.get(), onFinish);
}

std::shared_ptr<RestConnection>
InProcessRestConnection::
captureInConnection(std::shared_ptr<void> toCapture)
{
    auto captured = this->shared_from_this();

    // The destruction of the captures does the work here; the function
    // itself is empty
    auto onFinish = [captured, toCapture=std::move(toCapture)] (RestConnection *)
        {
        };

    return std::shared_ptr<RestConnection>(captured.get(), onFinish);
}

void
InProcessRestConnection::
waitForResponse()
{
    std::unique_lock<std::mutex> guard(itl->responseMutex);
    itl->waitForResponse(guard);
}

} // namespace MLDB


