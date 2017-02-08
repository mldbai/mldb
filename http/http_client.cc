/** http_client.cc
    Wolfgang Sourdeau, January 2014
    This file is part of MLDB. Copyright 2014 mldb.ai inc. All rights reserved.
*/

#include <string.h>

#include "mldb/io/legacy_event_loop.h"
#include "mldb/io/message_loop.h"
#include "mldb/http/http_client_impl.h"
#include "mldb/http/http_client_impl_v1.h"
#include "mldb/http/http_client.h"

using namespace std;
using namespace MLDB;


namespace {

int httpClientImplVersion;

struct AtInit {
    AtInit()
    {
        httpClientImplVersion = 1;

        char * value = ::getenv("HTTP_CLIENT_IMPL");
        if (!value) {
            return;
        }

        if (::strcmp(value, "1") == 0) {
            httpClientImplVersion = 1;
        }
        else {
            cerr << (string("HttpClient: no handling for HttpClientImpl")
                     + " version " + value + ", using default\n");
        }
    }
} atInit;

} // file scope


/****************************************************************************/
/* HTTP CLIENT                                                              */
/****************************************************************************/

void
HttpClient::
setHttpClientImplVersion(int version)
{
    if (version != 1) {
        throw MLDB::Exception("invalid value for 'version': "
                            + to_string(version));
    }
    httpClientImplVersion = version;
}

HttpClient::
HttpClient(LegacyEventLoop & eventLoop,
           const string & baseUrl, int numParallel, int queueSize,
           int implVersion)
    : eventLoop_(eventLoop)
{
    bool isHttps(baseUrl.compare(0, 8, "https://") == 0);

    if (baseUrl.compare(0, 7, "http://") != 0 && !isHttps) {
        throw MLDB::Exception("'url' has an invalid value: " + baseUrl);
    }
    if (numParallel < 1) {
        throw MLDB::Exception("'numParallel' must at least be equal to 1");
    }

    if (implVersion == 0) {
        implVersion = httpClientImplVersion;
    }

    if (implVersion == 1) {
        impl_.reset(new HttpClientImplV1(baseUrl, numParallel, queueSize));
    }
    else {
        throw MLDB::Exception("invalid httpclient impl version");
    }

    auto & msgLoop = eventLoop_.loop();
    msgLoop.addSource("client", impl_);

    /* centralize the default values */
    enableDebug(false);
    enableSSLChecks(true);
    enableTcpNoDelay(false);
    enablePipelining(false);
}

HttpClient::
HttpClient(HttpClient && other)
      noexcept
    : eventLoop_(other.eventLoop_), impl_(std::move(other.impl_))
{
}

HttpClient::
~HttpClient()
{
    auto ptr = impl_.get();
    if (ptr) {
        auto & msgLoop = eventLoop_.loop();
        msgLoop.removeSourceSync(impl_.get());
    }
}

int
HttpClient::
selectFd()
    const
{
    return impl_->selectFd();
}

bool
HttpClient::
processOne()
{
    return impl_->processOne();
}

void
HttpClient::
enableDebug(bool value)
{
    impl_->enableDebug(value);
}

void
HttpClient::
enableSSLChecks(bool value)
{
    impl_->enableSSLChecks(value);
}

void
HttpClient::
enableTcpNoDelay(bool value)
{
    impl_->enableTcpNoDelay(value);
}

void
HttpClient::
sendExpect100Continue(bool value)
{
    if (value) {
        throw MLDB::Exception("HttpClient has no support for"
                            " 'Expect: 100 Continue' requests");
    }
}

void
HttpClient::
enablePipelining(bool value)
{
    impl_->enablePipelining(value);
}

bool
HttpClient::
enqueueRequest(const std::string & verb,
               const std::string & resource,
               const std::shared_ptr<HttpClientCallbacks> & callbacks,
               const HttpRequestContent & content,
               const RestParams & queryParams,
               const RestParams & headers,
               int timeout)
{
    return impl_->enqueueRequest(verb, resource, callbacks, content,
                                 queryParams, headers, timeout);
}

size_t
HttpClient::
queuedRequests()
    const
{
    return impl_->queuedRequests();
}

HttpClient &
HttpClient::operator = (HttpClient && other) noexcept
{
    if (&other != this) {
        impl_ = std::move(other.impl_);
    }

    return *this;
}
