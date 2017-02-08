/* json_service_endpoint.h                                         -*- C++ -*-
   Jeremy Barnes, 9 November 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/jml/utils/vector_utils.h"
#include "mldb/types/date.h"
#include "mldb/io/asio_thread_pool.h"
#include "mldb/io/event_loop.h"
#include "mldb/rest/http_rest_endpoint.h"
#include "mldb/io/port_range_service.h"
#include "mldb/rest/rest_connection.h"
#include "mldb/rest/rest_request.h"
#include "mldb/utils/log_fwd.h"

namespace MLDB {


/*****************************************************************************/
/* REST SERVICE ENDPOINT                                                     */
/*****************************************************************************/

/** This class exposes an API for a given service via:
    - http

    It allows both synchronous and asynchronous responses.
*/
struct RestServiceEndpoint {
    RestServiceEndpoint(bool enableLogging = false);

    virtual ~RestServiceEndpoint();

    void shutdown();

    /** Defines an http connection (identified by its
        connection handler object).
    */
    struct ConnectionId : public RestConnection {
     
        /// Initialize for http
        ConnectionId(std::shared_ptr<HttpRestEndpoint::RestConnectionHandler> http,
                     const std::string & requestId,
                     RestServiceEndpoint * endpoint)
            : itl(new Itl(http, requestId, endpoint))
        {
        }

        struct Itl {
            Itl(std::shared_ptr<HttpRestEndpoint::RestConnectionHandler> http,
                const std::string & requestId,
                RestServiceEndpoint * endpoint)
                : requestId(requestId),
                  http(http),
                  endpoint(endpoint),
                  responseSent(false),
                  startDate(Date::now()),
                  chunkedEncoding(false),
                  keepAlive(true)
            {
            }

            ~Itl()
            {
                if (!responseSent) {
                    ::fprintf(stderr, "warning: no response sent on connection");
                    // no terminate is this could happen when shutting down
                    // uncleanly.  If it is printed at another time, there is
                    // a bug.
                }
            }
            
            std::string requestId;
            std::shared_ptr<HttpRestEndpoint::RestConnectionHandler> http;
            RestServiceEndpoint * endpoint;
            bool responseSent;
            Date startDate;
            bool chunkedEncoding;
            bool keepAlive;

            /** Data that is maintained with the connection.  This is where control
                data required for asynchronous or long-running connections can be
                put.
            */
            std::vector<std::shared_ptr<void> > piggyBack;
        };

        std::shared_ptr<Itl> itl;

        void sendResponse(int responseCode,
                          const char * response,
                          std::string contentType)
        {
            return sendResponse(responseCode, std::string(response),
                                std::move(contentType));
        }

        /** Send the given response back on the connection. */
        void sendResponse(int responseCode,
                          std::string response,
                          std::string contentType);

        /** Send the given response back on the connection. */
        void sendResponse(int responseCode,
                          const Json::Value & response,
                          std::string contentType = "application/json");

        void sendResponse(int responseCode)
        {
            return sendResponse(responseCode, "", "");
        }

        void sendRedirect(int responseCode, std::string location);

        /** Send an HTTP-only response with the given headers.  If it's not
            an HTTP connection, this will fail.
        */
        void sendHttpResponse(int responseCode,
                              std::string response,
                              std::string contentType,
                              RestParams headers);

        enum {
            UNKNOWN_CONTENT_LENGTH = -1,
            CHUNKED_ENCODING = -2
        };

        /** Send an HTTP-only response header.  This will not close the connection.  A
            contentLength of -1 means don't send it (for when the content length is
            not known ahead of time).  A contentLength of -2 means used HTTP chunked
            transfer encoding
        */
        void sendHttpResponseHeader(int responseCode,
                                    std::string contentType,
                                    ssize_t contentLength,
                                    RestParams headers = RestParams());

        /** Send a payload (or a chunk of a payload) for an HTTP connection. */
        void sendPayload(std::string payload);

        /** Finish the response, recycling or closing the connection. */
        void finishResponse();

        /** Send the given error string back on the connection. */
        void sendErrorResponse(int responseCode,
                               std::string error,
                               std::string contentType);

        void sendErrorResponse(int responseCode, const char * error,
                               std::string contentType)
        {
            sendErrorResponse(responseCode, std::string(error), "application/json");
        }

        void sendErrorResponse(int responseCode, const Json::Value & error);

        bool responseSent() const
        {
            return itl->responseSent;
        }

        bool isConnected() const
        {
            return itl->http->isConnected();
        }

        virtual std::shared_ptr<RestConnection>
        capture(std::function<void ()> onDisconnect);

        virtual std::shared_ptr<RestConnection>
        captureInConnection(std::shared_ptr<void> piggyBack);
    };

    void init();

    /** Bind to TCP/IP ports. */
    std::string
    bindTcp(PortRange const & httpRange = PortRange(),
            std::string host = "");

    /** Bind to a fixed URI for the HTTP endpoint.  This will throw an
        exception if it can't bind.

        example address: "*:4444", "localhost:8888"
    */
    std::string bindFixedHttpAddress(std::string host, int port)
    {
        return httpEndpoint.bindTcpFixed(host, port);
    }

    std::string bindFixedHttpAddress(std::string address)
    {
        return httpEndpoint.bindTcpAddress(address);
    }

    /// Request handler function type
    typedef std::function<void (ConnectionId & connection,
                                const RestRequest & request)> OnHandleRequest;

    OnHandleRequest onHandleRequest;

    /** Handle a request.  Default implementation defers to onHandleRequest.
        Otherwise this method should be overridden.
    */
    virtual void handleRequest(ConnectionId & connection,
                               const RestRequest & request) const;

    std::function<void (ConnectionId & conn, const RestRequest & req) > logRequest;
    std::function<void (ConnectionId & conn,
                        int code,
                        const std::string & resp,
                        const std::string & contentType) > logResponse;

    void doHandleRequest(ConnectionId & connection,
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

    EventLoop eventLoop;
    AsioThreadPool threadPool;
    HttpRestEndpoint httpEndpoint;
    std::shared_ptr<spdlog::logger> logger;
};

} // namespace MLDB
