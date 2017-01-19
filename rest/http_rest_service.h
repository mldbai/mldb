// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** http_rest_service.h                                            -*- C++ -*-
    Jeremy Barnes, 1 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    REST service, HTTP only.  Originally was RestServiceEndpoint.
*/

#pragma once

#include <memory>
#include <string>

#include "mldb/io/asio_thread_pool.h"
#include "mldb/rest/http_rest_endpoint.h"
#include "mldb/io/port_range_service.h"
#include "mldb/rest/rest_connection.h"
#include "mldb/rest/rest_request.h"
#include "mldb/types/date.h"
#include "mldb/utils/log_fwd.h"


namespace MLDB {

/* Forward declarations */

struct EventLoop;
struct HttpRestEndpoint;
struct HttpRestService;


/*****************************************************************************/
/* HTTP REST CONNECTION                                                      */
/*****************************************************************************/

/** An HTTP REST connection. */
struct HttpRestConnection: public RestConnection {
    /// Don't initialize for now
    HttpRestConnection()
    {
    }
        
    /// Initialize for http
    HttpRestConnection(std::shared_ptr<HttpRestEndpoint::RestConnectionHandler> http,
                       const std::string & requestId,
                       HttpRestService * endpoint)
        : http(http),
          requestId(requestId),
          endpoint(endpoint),
          responseSent_(false),
          startDate(Date::now()),
          chunkedEncoding(false),
          keepAlive(true)
    {
    }

    std::shared_ptr<HttpRestEndpoint::RestConnectionHandler> http;
    std::string requestId;
    HttpRestService * endpoint;
    bool responseSent_;
    Date startDate;
    bool chunkedEncoding;
    bool keepAlive;

    /** Data that is maintained with the connection.  This is where control
        data required for asynchronous or long-running connections can be
        put.
    */
    std::vector<std::shared_ptr<void> > piggyBack;

    using RestConnection::sendResponse;

    /** Send the given response back on the connection. */
    virtual void sendResponse(int responseCode,
                              std::string response,
                              std::string contentType);
    
    /** Send the given response back on the connection. */
    virtual void sendResponse(int responseCode,
                              const Json::Value & response,
                              std::string contentType = "application/json");
    
    virtual void sendRedirect(int responseCode, std::string location);

    /** Send an HTTP-only response with the given headers.  If it's not
        an HTTP connection, this will fail.
    */
    virtual void sendHttpResponse(int responseCode,
                                  std::string response, std::string contentType,
                                  RestParams headers);

    /** Send an HTTP-only response header.  This will not close the connection.  A
        contentLength of -1 means don't send it (for when the content length is
        not known ahead of time).  A contentLength of -2 means used HTTP chunked
        transfer encoding
    */
    virtual void sendHttpResponseHeader(int responseCode,
                                        std::string contentType,
                                        ssize_t contentLength,
                                        RestParams headers = RestParams());
    
    /** Send a payload (or a chunk of a payload) for an HTTP connection. */
    virtual void sendPayload(std::string payload);

    /** Finish the response, recycling or closing the connection. */
    virtual void finishResponse();

    /** Send the given error string back on the connection. */
    virtual void sendErrorResponse(int responseCode,
                                   std::string error,
                                   std::string contentType);
    
    virtual void sendErrorResponse(int responseCode, const Json::Value & error);

    using RestConnection::sendErrorResponse;

    virtual bool responseSent() const
    {
        return responseSent_;
    }

    virtual bool isConnected() const;

    virtual std::shared_ptr<RestConnection>
    capture(std::function<void ()> onDisconnect);

    virtual std::shared_ptr<RestConnection>
    captureInConnection(std::shared_ptr<void> piggyBack);
};


/*****************************************************************************/
/* HTTP REST SERVICE                                                         */
/*****************************************************************************/

/** This class exposes an API for a given service via http.

    It doesn't do any service discovery, etc: it's simply for handling REST
    requests.
*/

struct HttpRestService {

    /** Start the service with the given parameters.  If the ports are given,
        then the service will bind to those specific ports for the given
        endpoints, and so no service discovery will need to be done.
    */
    HttpRestService(bool enableLogging);

    virtual ~HttpRestService();

    void shutdown();

    void init();

    /** Bind to TCP/IP port */
    std::string bindTcp(PortRange const & httpRange = PortRange(),
                        std::string host = "");

    /** Bind to a fixed URI for the HTTP endpoint.  This will throw an
        exception if it can't bind.

        example address: "*:4444", "localhost:8888"
    */
    std::string bindFixedHttpAddress(std::string host, int port);
    std::string bindFixedHttpAddress(std::string address);

    /// Request handler function type
    typedef std::function<void (RestConnection & connection,
                                const RestRequest & request)> OnHandleRequest;

    OnHandleRequest onHandleRequest;

    /** Handle a request.  Default implementation defers to onHandleRequest.
        Otherwise this method should be overridden.
    */
    virtual void handleRequest(RestConnection & connection,
                               const RestRequest & request) const;

    std::function<void (HttpRestConnection & conn, const RestRequest & req) > logRequest;
    std::function<void (HttpRestConnection & conn,
                        int code,
                        const std::string & resp,
                        const std::string & contentType) > logResponse;

    void doHandleRequest(HttpRestConnection & connection,
                         const RestRequest & request)
    {
        if (logRequest)
            logRequest(connection, request);

        handleRequest(connection, request);
    }
    
    // Create a random request ID for an HTTP request
    std::string getHttpRequestId() const;

    /** Log all requests and responses to the given stream.  This function
        will overwrite logRequest and logResponse with new handlers.
    */
    void logToStream(std::ostream & stream);

    std::unique_ptr<EventLoop> eventLoop;
    std::unique_ptr<AsioThreadPool> threadPool;
    std::unique_ptr<HttpRestEndpoint> httpEndpoint;
    std::shared_ptr<spdlog::logger> logger;
};

} // namespace MLDB
