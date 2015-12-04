// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* http_rest_service.cc
   Jeremy Barnes, 11 November 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Endpoint to talk with a REST service.
*/

#include "mldb/jml/utils/vector_utils.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/ext/cityhash/src/city.h"
#include "mldb/base/exc_assert.h"
#include "mldb/http/event_loop.h"
#include "http_rest_endpoint.h"
#include "http_rest_service.h"

using namespace std;


namespace Datacratic {

/*****************************************************************************/
/* REST SERVICE ENDPOINT CONNECTION ID                                       */
/*****************************************************************************/

void
HttpRestConnection::
sendResponse(int responseCode,
             const std::string & response,
             const std::string & contentType)
{
    if (responseSent_)
        throw ML::Exception("response already sent");

    if (endpoint->logResponse)
        endpoint->logResponse(*this, responseCode, response,
                              contentType);
    
    http->sendResponse(responseCode, response, contentType);
    
    responseSent_ = true;
}

void
HttpRestConnection::
sendResponse(int responseCode,
                  const Json::Value & response,
                  const std::string & contentType)
{
    using namespace std;
    //cerr << "sent response " << responseCode << " " << response
    //     << endl;

    if (responseSent_)
        throw ML::Exception("response already sent");

    if (endpoint->logResponse)
        endpoint->logResponse(*this, responseCode, response.toString(),
                                   contentType);

    http->sendResponse(responseCode, response, contentType);
    
    responseSent_ = true;
}

void
HttpRestConnection::
sendErrorResponse(int responseCode,
                  const std::string & error, const std::string & contentType)
{
    using namespace std;
    cerr << "sent error response " << responseCode << " " << error
         << endl;

    if (responseSent_)
        throw ML::Exception("response already sent");


    if (endpoint->logResponse)
        endpoint->logResponse(*this, responseCode, error,
                                   contentType);
            
    http->sendResponse(responseCode, error);

    responseSent_ = true;
}

void
HttpRestConnection::
sendErrorResponse(int responseCode, const Json::Value & error)
{
    using namespace std;
    cerr << "sent error response " << responseCode << " " << error
         << endl;
    
    if (responseSent_)
        throw ML::Exception("response already sent");
    
    if (endpoint->logResponse)
        endpoint->logResponse(*this, responseCode, error.toString(),
                              "application/json");
    
    http->sendResponse(responseCode, error);

    responseSent_ = true;
}

void
HttpRestConnection::
sendRedirect(int responseCode, const std::string & location)
{
    if (responseSent_)
        throw ML::Exception("response already sent");
    
    if (endpoint->logResponse)
        endpoint->logResponse(*this, responseCode, location,
                                   "REDIRECT");
    
    http->sendResponse(responseCode, string(""), "",
                       { { "Location", location } });

    responseSent_ = true;
}

void
HttpRestConnection::
sendHttpResponse(int responseCode,
                 const std::string & response,
                 const std::string & contentType,
                 const RestParams & headers)
{
    if (responseSent_)
        throw ML::Exception("response already sent");

    if (endpoint->logResponse)
        endpoint->logResponse(*this, responseCode, response,
                                   contentType);

    http->sendResponse(responseCode, response, contentType,
                       headers);
    responseSent_ = true;
}

void
HttpRestConnection::
sendHttpResponseHeader(int responseCode,
                       const std::string & contentType,
                       ssize_t contentLength,
                       const RestParams & headers_)
{
    if (responseSent_)
        throw ML::Exception("response already sent");

    if (!http)
        throw ML::Exception("sendHttpResponseHeader only works on HTTP connections");

    if (endpoint->logResponse)
        endpoint->logResponse(*this, responseCode, "", contentType);

    RestParams headers = headers_;
    if (contentLength == CHUNKED_ENCODING) {
        chunkedEncoding = true;
        headers.push_back({"Transfer-Encoding", "chunked"});
    }
    else if (contentLength >= 0) {
        headers.push_back({"Content-Length", to_string(contentLength) });
    }
    else {
        keepAlive = false;
    }

    http->sendResponseHeader(responseCode, contentType, headers);
}

bool
HttpRestConnection::
isConnected()
    const
{
    return http->isConnected();
}

void
HttpRestConnection::
sendPayload(const std::string & payload)
{
    if (chunkedEncoding) {
        if (payload.empty()) {
            throw ML::Exception("Can't send empty chunk over a chunked connection");
        }
        http->sendHttpChunk(payload, HttpLegacySocketHandler::NEXT_CONTINUE);
    }
    else http->sendHttpPayload(payload);
}

void
HttpRestConnection::
finishResponse()
{
    if (chunkedEncoding) {
        http->sendHttpChunk("", HttpLegacySocketHandler::NEXT_CLOSE);
    }
    else if (!keepAlive) {
        http->send("", HttpLegacySocketHandler::NEXT_CLOSE);
    } else {
        http->send("", HttpLegacySocketHandler::NEXT_RECYCLE);
    }

    responseSent_ = true;
}

std::shared_ptr<RestConnection>
HttpRestConnection::
capture(std::function<void ()> onDisconnect)
{
    ExcAssert(onDisconnect);
    if (this->http->onDisconnect)
        throw ML::Exception("Connection has already been captured");
    auto result = std::make_shared<HttpRestConnection>(std::move(*this));
    result->http->onDisconnect = std::move(onDisconnect);
    return result;
}

std::shared_ptr<RestConnection>
HttpRestConnection::
captureInConnection(std::shared_ptr<void> toCapture)
{
    ExcAssert(toCapture);
    auto result = std::make_shared<HttpRestConnection>(std::move(*this));
    result->piggyBack.emplace_back(std::move(toCapture));
    result->http->onDisconnect = [] () { cerr << "ONDISCONNECT" << endl; };
    return result;
}


/*****************************************************************************/
/* HTTP REST SERVICE                                                         */
/*****************************************************************************/

HttpRestService::
HttpRestService()
    : eventLoop(new EventLoop()),
      threadPool(new AsioThreadPool(*eventLoop)),
      httpEndpoint(new HttpRestEndpoint(*eventLoop))
{
}

HttpRestService::
~HttpRestService()
{
    shutdown();
}

void
HttpRestService::
shutdown()
{
    // 1.  Shut down the http endpoint, since it needs our threads to
    //     complete its shutdown
    httpEndpoint->shutdown();

    threadPool->shutdown();
}

void
HttpRestService::
init()
{
    httpEndpoint->init();

    httpEndpoint->onRequest
        = [=] (std::shared_ptr<HttpRestEndpoint::RestConnectionHandler> connection,
               const HttpHeader & header,
               const std::string & payload)
        {
            std::string requestId = this->getHttpRequestId();
            HttpRestConnection restConnection(connection, requestId, this);
            this->doHandleRequest(restConnection,
                                  RestRequest(header, payload));
        };
}

std::string
HttpRestService::
bindTcp(PortRange const & httpRange, std::string host)
{
    std::string httpAddr = httpEndpoint->bindTcp(httpRange, host);
    return httpAddr;
}

std::string
HttpRestService::
bindFixedHttpAddress(std::string host, int port)
{
    return httpEndpoint->bindTcpFixed(host, port);
}

std::string
HttpRestService::
bindFixedHttpAddress(std::string address)
{
    return httpEndpoint->bindTcpAddress(address);
}

void
HttpRestService::
handleRequest(RestConnection & connection,
              const RestRequest & request) const
{
    using namespace std;

    //cerr << "got request " << request << endl;
    if (onHandleRequest) {
        onHandleRequest(connection, request);
    }
    else {
        throw ML::Exception("need to override handleRequest or assign to "
                            "onHandleRequest");
    }
}

std::string
HttpRestService::
getHttpRequestId() const
{
    std::string s = Date::now().print(9) + ML::format("%d", random());
    uint64_t jobId = CityHash64(s.c_str(), s.size());
    return ML::format("%016llx", jobId);
}

void
HttpRestService::
logToStream(std::ostream & stream)
{
    auto logLock = std::make_shared<std::mutex>();

    logRequest = [=,&stream] (HttpRestConnection & conn, const RestRequest & req)
        {
            std::unique_lock<std::mutex> guard(*logLock);

            stream << "--> ------------------------- new request "
            << conn.requestId
            << " at " << conn.startDate.print(9) << endl;
            stream << req << endl << endl;
        };

    logResponse = [=,&stream] (HttpRestConnection & conn,
                       int code,
                       const std::string & resp,
                       const std::string & contentType)
        {
            std::unique_lock<std::mutex> guard(*logLock);

            Date now = Date::now();

            stream << "<-- ========================= finished request "
            << conn.requestId
            << " at " << now.print(9)
            << ML::format(" (%.3fms)", conn.startDate.secondsUntil(now) * 1000)
            << endl;
            stream << code << " " << contentType << " " << resp.length() << " bytes" << endl;;
                
            if (resp.size() <= 16384)
                stream << resp;
            else {
                stream << string(resp, 0, 16386) << "...";
            }
            stream << endl << endl;
        };
}


} // namespace Datacratic
