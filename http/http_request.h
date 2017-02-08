/** http_request.h                                                 -*- C++ -*-
    Wolfgang Sourdeau, January 2014
    This file is part of MLDB. Copyright 2014 mldb.ai inc. All rights reserved.

*/

#pragma once

#include <memory>
#include <string>

#include "mldb/http/http_header.h"


/* Forward declarations */

namespace Json {

struct Value;

}


namespace MLDB {

/* Forward declarations */

struct HttpClientCallbacks;


/****************************************************************************/
/* HTTP REQUEST CONTENT                                                     */
/****************************************************************************/

struct HttpRequestContent {
    /** Structure used to hold content for a POST request. */
    HttpRequestContent() = default;

    HttpRequestContent(const std::string & str,
                       const std::string & contentType = "");
    HttpRequestContent(const char * data, uint64_t size,
                       const std::string & contentType = "");
    HttpRequestContent(const Json::Value & content,
                       const std::string & contentType = "application/json");

    const std::string & body()
        const
    {
        return body_;
    }

    const std::string & contentType()
        const
    {
        return contentType_;
    }

private:
    std::string body_;
    std::string contentType_;
};


/****************************************************************************/
/* HTTP REQUEST                                                             */
/****************************************************************************/

/* Representation of an HTTP request. */

struct HttpRequest {
    HttpRequest();
    HttpRequest(const std::string & verb, const std::string & url,
                const std::shared_ptr<HttpClientCallbacks> & callbacks,
                const HttpRequestContent & content,
                const RestParams & headers,
                int timeout = -1) noexcept;

    void clear();
    operator bool () const;

    const std::string & verb()
        const
    {
        return verb_;
    }

    const std::string & url()
        const
    {
        return url_;
    }

    const RestParams & headers()
        const
    {
        return headers_;
    }

    const HttpRequestContent & content()
        const
    {
        return content_;
    }

    int timeout()
        const
    {
        return timeout_;
    }

    const std::shared_ptr<HttpClientCallbacks> & callbacks()
        const
    {
        return callbacks_;
    }

private:
    std::string verb_;
    std::string url_;
    RestParams headers_;
    HttpRequestContent content_;
    int timeout_;

    std::shared_ptr<HttpClientCallbacks> callbacks_;
};

} // namespace MLDB
