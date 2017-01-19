/** http_client_impl.h                                              -*- C++ -*-
    Wolfgang Sourdeau, January 2014
    This file is part of MLDB. Copyright 2014 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <memory>
#include <string>

#include "mldb/io/async_event_source.h"
#include "mldb/http/http_header.h"


namespace MLDB {

/* Forward declarations */

struct HttpClientCallbacks;
struct HttpRequestContent;


/****************************************************************************/
/* HTTP CLIENT IMPL                                                         */
/****************************************************************************/

struct HttpClientImpl : public AsyncEventSource {
    HttpClientImpl(const std::string & baseUrl,
                   int numParallel = 1024, int queueSize = 0);
    HttpClientImpl(HttpClientImpl && other) = default;

    virtual ~HttpClientImpl();

    /** Enable debugging */
    virtual void enableDebug(bool value) = 0;

    /** SSL checks */
    virtual void enableSSLChecks(bool value) = 0;

    /** Enable the TCP_NODELAY option, also known as the Nagle's algorithm */
    virtual void enableTcpNoDelay(bool value) = 0;

    /** Use with servers that support HTTP pipelining */
    virtual void enablePipelining(bool value) = 0;

    /** Enqueue (or perform) the specified request */
    virtual bool enqueueRequest(const std::string & verb,
                                const std::string & resource,
                                const std::shared_ptr<HttpClientCallbacks> & callbacks,
                                const HttpRequestContent & content,
                                const RestParams & queryParams,
                                const RestParams & headers,
                                int timeout = -1) = 0;

    /** Returns the number of requests in the queue */
    virtual size_t queuedRequests() const = 0;
};

} // namespace MLDB
