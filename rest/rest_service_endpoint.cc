// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* rest_service_endpoint.cc
   Jeremy Barnes, 11 November 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Endpoint to talk with a REST service.
*/

#include "mldb/jml/utils/vector_utils.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/types/date.h"
#include "mldb/ext/cityhash/src/city.h"
#include "mldb/rest/rest_service_endpoint.h"
#include "mldb/http/http_socket_handler.h"
#include "mldb/utils/log.h"

using namespace std;


namespace MLDB {

/*****************************************************************************/
/* REST SERVICE ENDPOINT CONNECTION ID                                       */
/*****************************************************************************/

void
RestServiceEndpoint::ConnectionId::
sendResponse(int responseCode, std::string response, std::string contentType)
{
    if (itl->responseSent)
        throw MLDB::Exception("response already sent");

    if (itl->endpoint->logResponse)
        itl->endpoint->logResponse(*this, responseCode, response,
                                   contentType);

    if (itl->http)
        itl->http->sendResponse(responseCode,
                                std::move(response), std::move(contentType));
    else
        throw MLDB::Exception("missing connection handler");

    itl->responseSent = true;
}

void
RestServiceEndpoint::ConnectionId::
sendResponse(int responseCode,
             const Json::Value & response, std::string contentType)
{
    if (itl->responseSent)
        throw MLDB::Exception("response already sent");

    if (itl->endpoint->logResponse)
        itl->endpoint->logResponse(*this, responseCode, response.toString(),
                                   contentType);

    if (itl->http)
        itl->http->sendResponse(responseCode,
                                std::move(response), std::move(contentType));
    else
        throw MLDB::Exception("missing connection handler");

    itl->responseSent = true;
}

void
RestServiceEndpoint::ConnectionId::
sendErrorResponse(int responseCode,
                  std::string error, std::string contentType)
{
    using namespace std;
    cerr << "sent error response " << responseCode << " " << error
         << endl;

    if (itl->responseSent)
        throw MLDB::Exception("response already sent");


    if (itl->endpoint->logResponse)
        itl->endpoint->logResponse(*this, responseCode, error,
                                   contentType);
            
    if (itl->http)
        itl->http->sendResponse(responseCode, std::move(error));
    else
        throw MLDB::Exception("missing connection handler");
    
    itl->responseSent = true;
}

void
RestServiceEndpoint::ConnectionId::
sendErrorResponse(int responseCode, const Json::Value & error)
{
    using namespace std;
    cerr << "sent error response " << responseCode << " " << error
         << endl;

    if (itl->responseSent)
        throw MLDB::Exception("response already sent");

    if (itl->endpoint->logResponse)
        itl->endpoint->logResponse(*this, responseCode, error.toString(),
                                   "application/json");

    if (itl->http)
        itl->http->sendResponse(responseCode, error);
    else
        throw MLDB::Exception("missing connection handler");
    
    itl->responseSent = true;
}

void
RestServiceEndpoint::ConnectionId::
sendRedirect(int responseCode, std::string location)
{
    if (itl->responseSent)
        throw MLDB::Exception("response already sent");

    if (itl->endpoint->logResponse)
        itl->endpoint->logResponse(*this, responseCode, location,
                                   "REDIRECT");

    if (itl->http)
        itl->http->sendResponse(responseCode, string(""), "",
                                { { "Location", std::move(location) } });
    else
        throw MLDB::Exception("missing connection handler");
    
    itl->responseSent = true;
}

void
RestServiceEndpoint::ConnectionId::
sendHttpResponse(int responseCode,
                 std::string response, std::string contentType,
                 RestParams headers)
{
    if (itl->responseSent)
        throw MLDB::Exception("response already sent");

    if (itl->endpoint->logResponse)
        itl->endpoint->logResponse(*this, responseCode, response,
                                   contentType);

    if (itl->http)
        itl->http->sendResponse(responseCode,
                                std::move(response), std::move(contentType),
                                std::move(headers));
    else
        throw MLDB::Exception("missing connection handler");
    
    itl->responseSent = true;
}

void
RestServiceEndpoint::ConnectionId::
sendHttpResponseHeader(int responseCode,
                       std::string contentType, ssize_t contentLength,
                       RestParams headers_)
{
    if (itl->responseSent)
        throw MLDB::Exception("response already sent");

    if (!itl->http)
        throw MLDB::Exception("sendHttpResponseHeader only works on HTTP connections");

    if (itl->endpoint->logResponse)
        itl->endpoint->logResponse(*this, responseCode, "", contentType);

    RestParams headers = headers_;
    if (contentLength == CHUNKED_ENCODING) {
        itl->chunkedEncoding = true;
        headers.push_back({"Transfer-Encoding", "chunked"});
    }
    else if (contentLength >= 0) {
        headers.push_back({"Content-Length", to_string(contentLength) });
    }
    else {
        itl->keepAlive = false;
    }

    if (itl->http)
        itl->http->sendResponseHeader(responseCode, std::move(contentType), std::move(headers));
    else
        throw MLDB::Exception("missing connection handler");
}

void
RestServiceEndpoint::ConnectionId::
sendPayload(std::string payload)
{
    if (itl->chunkedEncoding) {
        if (payload.empty()) {
            throw MLDB::Exception("Can't send empty chunk over a chunked connection");
        }
        itl->http->sendHttpChunk(std::move(payload),
                                 HttpLegacySocketHandler::NEXT_CONTINUE);
    }
    else itl->http->send(std::move(payload));
}

void
RestServiceEndpoint::ConnectionId::
finishResponse()
{
    if (itl->chunkedEncoding) {
        itl->http->sendHttpChunk("", HttpLegacySocketHandler::NEXT_CLOSE);
    }
    else if (!itl->keepAlive) {
        itl->http->send("", HttpLegacySocketHandler::NEXT_CLOSE);
    } else {
        itl->http->send("", HttpLegacySocketHandler::NEXT_RECYCLE);
    }

    itl->responseSent = true;
}

std::shared_ptr<RestConnection>
RestServiceEndpoint::ConnectionId::
capture(std::function<void ()> onDisconnect)
{
    throw MLDB::Exception("RestServiceEndpoint::ConnectionId::capture(): "
                        "needs to be implemented");
}

std::shared_ptr<RestConnection>
RestServiceEndpoint::ConnectionId::
captureInConnection(std::shared_ptr<void> piggyBack)
{
    throw MLDB::Exception("RestServiceEndpoint::ConnectionId::captureInConnection(): "
                        "needs to be implemented");
}


/*****************************************************************************/
/* REST SERVICE ENDPOINT                                                     */
/*****************************************************************************/

RestServiceEndpoint::
RestServiceEndpoint(bool enableLogging /*= false*/)
    : threadPool(eventLoop), 
      httpEndpoint(eventLoop, enableLogging),  
      logger(MLDB::getMldbLog<RestServiceEndpoint>())
{
}

RestServiceEndpoint::
~RestServiceEndpoint()
{
    shutdown();
}

void
RestServiceEndpoint::
shutdown()
{
    // 1.  Shut down the http endpoint, since it needs our threads to
    //     complete its shutdown
    httpEndpoint.shutdown();

    // 2.  Shut down the thread pool
    threadPool.shutdown();
}

void
RestServiceEndpoint::
init()
{
    httpEndpoint.init();
    httpEndpoint.onRequest
        = [=] (std::shared_ptr<HttpRestEndpoint::RestConnectionHandler> connection,
               const HttpHeader & header,
               const std::string & payload)
        {
            std::string requestId = this->getHttpRequestId();
            ConnectionId connectionId(connection, requestId, this);
            this->doHandleRequest(connectionId,
                                  RestRequest(header, payload));
        };
}

std::string
RestServiceEndpoint::
bindTcp(PortRange const & httpRange,
        std::string host)
{
    std::string httpAddr = httpEndpoint.bindTcp(httpRange, host);
    DEBUG_MSG(logger) << "http listening on " << httpAddr;
    return httpAddr;
}

void
RestServiceEndpoint::
handleRequest(ConnectionId & connection,
              const RestRequest & request) const
{
    using namespace std;

    //cerr << "got request " << request << endl;
    if (onHandleRequest) {
        onHandleRequest(connection, request);
    }
    else {
        throw MLDB::Exception("need to override handleRequest or assign to "
                            "onHandleRequest");
    }
}

std::string
RestServiceEndpoint::
getHttpRequestId() const
{
    std::string s = Date::now().print(9) + MLDB::format("%d", random());
    uint64_t jobId = CityHash64(s.c_str(), s.size());
    return MLDB::format("%016llx", jobId);
}

void
RestServiceEndpoint::
logToStream(std::ostream & stream)
{
    auto logLock = std::make_shared<std::mutex>();

    logRequest = [=,&stream] (const ConnectionId & conn, const RestRequest & req)
        {
            std::unique_lock<std::mutex> guard(*logLock);

            stream << "--> ------------------------- new request "
            << conn.itl->requestId
            << " at " << conn.itl->startDate.print(9) << endl;
            stream << req << endl << endl;
        };

    logResponse = [=,&stream] (const ConnectionId & conn,
                       int code,
                       const std::string & resp,
                       const std::string & contentType)
        {
            std::unique_lock<std::mutex> guard(*logLock);

            Date now = Date::now();

            stream << "<-- ========================= finished request "
            << conn.itl->requestId
            << " at " << now.print(9)
            << MLDB::format(" (%.3fms)", conn.itl->startDate.secondsUntil(now) * 1000)
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

} // namespace MLDB
