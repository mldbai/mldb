/** http_client.h                                                   -*- C++ -*-
    Wolfgang Sourdeau, January 2014
    This file is part of MLDB. Copyright 2014 mldb.ai inc. All rights reserved.

    An asynchronous HTTP client.

    HttpClient is meant to provide a featureful, generic and asynchronous HTTP
    client class. It supports strictly asynchronous (non-blocking) operations,
    HTTP pipelining and concurrent requests while enabling streaming responses
    via a callback mechanism. It is meant to be subclassed whenever a
    synchronous interface or a one-shot response mechanism is required. In
    general, the code should be complete enough that existing and similar
    classes could be subclassed gradually (HttpRestProxy, s3 internals). As a
    generic class, it does not make assumptions on the transferred contents.
    Finally, it is based on the interface of HttpRestProxy.

    Caveat:
    - since those require header interpretation, there is not support for
    cookies per se
*/

#pragma once

#include <memory>
#include <string>

#include "mldb/http/http_client_callbacks.h"
#include "mldb/http/http_header.h"
#include "mldb/http/http_request.h"


namespace MLDB {

/* Forward declarations */

struct HttpClientImpl;
struct LegacyEventLoop;


/****************************************************************************/
/* HTTP CLIENT                                                              */
/****************************************************************************/

struct HttpClient
{
    /** This sets the requested version of the underlying HttpClientImpl. By
     * default, this value is deduced from the "HTTP_CLIENT_VERSION"
     * environment variable. It not set, this falls back to 1. */
    static void setHttpClientImplVersion(int version);

    /** HttpClient constructor.
       "eventLoop": LegacyEventLoop to which the instance will be attached,
       the loop *must* be started when the destructor is called.
       "baseUrl": scheme, hostname and port (scheme://hostname[:port]) that
       will be used as base for all requests
       "numParallels": number of requests that can be handled simultaneously
       "queueSize": size of the backlog of pending requests, after which
       operations will be refused (0 = infinite)
       "implVersion": use version X of the HttpClientImpl, fallback
       to HTTP_CLIENT_IMPL
    */
    HttpClient(LegacyEventLoop & eventLoop,
               const std::string & baseUrl,
               int numParallel = 1024, int queueSize = 0,
               int implVersion = 0);
    HttpClient(HttpClient && other) noexcept;
    HttpClient(const HttpClient & other) = delete;

    ~HttpClient();

    /* AsyncEventSource interface */
    virtual int selectFd() const;
    virtual bool processOne();

    /** Enable debugging */
    void enableDebug(bool value);

    /** SSL checks */
    void enableSSLChecks(bool value);

    /** Enable the TCP_NODELAY option, also known as the Nagle's algorithm */
    void enableTcpNoDelay(bool value);

    /** Enable the requesting of "100 Continue" responses in preparation of
     * a PUT request */
    void sendExpect100Continue(bool value);

    /** Use with servers that support HTTP pipelining */
    void enablePipelining(bool value);

    /** Performs a GET request, with "resource" as the location of the
     *  resource on the server indicated in "baseUrl". Query parameters
     *  should preferably be passed via "queryParams".
     *
     *  Returns "true" when the request could successfully be enqueued.
     */
    bool get(const std::string & resource,
             const std::shared_ptr<HttpClientCallbacks> & callbacks,
             const RestParams & queryParams = RestParams(),
             const RestParams & headers = RestParams(),
             int timeout = -1)
    {
        return enqueueRequest("GET", resource, callbacks,
                              HttpRequestContent(),
                              queryParams, headers, timeout);
    }

    /** Performs a POST request, using similar parameters as get with the
     * addition of "content" which defines the contents body and type.
     *
     *  Returns "true" when the request could successfully be enqueued.
     */
    bool post(const std::string & resource,
              const std::shared_ptr<HttpClientCallbacks> & callbacks,
              const HttpRequestContent & content = HttpRequestContent(),
              const RestParams & queryParams = RestParams(),
              const RestParams & headers = RestParams(),
              int timeout = -1)
    {
        return enqueueRequest("POST", resource, callbacks, content,
                              queryParams, headers, timeout);
    }

    /** Performs a PUT request in a similar fashion to "post" above.
     *
     *  Returns "true" when the request could successfully be enqueued.
     */
    bool put(const std::string & resource,
             const std::shared_ptr<HttpClientCallbacks> & callbacks,
             const HttpRequestContent & content = HttpRequestContent(),
             const RestParams & queryParams = RestParams(),
             const RestParams & headers = RestParams(),
             int timeout = -1)
    {
        return enqueueRequest("PUT", resource, callbacks, content,
                              queryParams, headers, timeout);
    }

    /** Performs a DELETE request. Note that this method cannot be named
     * "delete", which is a reserved keyword in C++.
     *
     *  Returns "true" when the request could successfully be enqueued.
     */
    bool del(const std::string & resource,
             const std::shared_ptr<HttpClientCallbacks> & callbacks,
             const RestParams & queryParams = RestParams(),
             const RestParams & headers = RestParams(),
             int timeout = -1)
    {
        return enqueueRequest("DELETE", resource, callbacks,
                              HttpRequestContent(),
                              queryParams, headers, timeout);
    }

    /** Enqueue (or perform) the specified request */
    bool enqueueRequest(const std::string & verb,
                        const std::string & resource,
                        const std::shared_ptr<HttpClientCallbacks> & callbacks,
                        const HttpRequestContent & content,
                        const RestParams & queryParams,
                        const RestParams & headers,
                        int timeout = -1);

    /** Returns the number of requests in the queue */
    size_t queuedRequests() const;

    HttpClient & operator = (HttpClient && other) noexcept;

private:
    LegacyEventLoop & eventLoop_;
    std::shared_ptr<HttpClientImpl> impl_;
};


} // namespace MLDB
