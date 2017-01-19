/** http_request.cc
    Wolfgang Sourdeau, January 2014
    This file is part of MLDB. Copyright 2014 mldb.ai inc. All rights reserved.
*/

#include "mldb/ext/jsoncpp/value.h"
#include "mldb/http/http_request.h"

using namespace std;
using namespace MLDB;


/****************************************************************************/
/* HTTP REQUEST CONTENT                                                     */
/****************************************************************************/

HttpRequestContent::
HttpRequestContent(const string & body, const string & contentType)
    : body_(body), contentType_(contentType)
{
}

HttpRequestContent::
HttpRequestContent(const char * data, uint64_t size,
                   const string & contentType)
    : body_(data, size), contentType_(contentType)
{
}

HttpRequestContent::
HttpRequestContent(const Json::Value & content, const string & contentType)
    : body_(content.toString()), contentType_(contentType)
{
}


/****************************************************************************/
/* HTTP REQUEST                                                             */
/****************************************************************************/

HttpRequest::
HttpRequest()
    : timeout_(-1)
{
}

HttpRequest::
HttpRequest(const string & verb, const string & url,
            const shared_ptr<HttpClientCallbacks> & callbacks,
            const HttpRequestContent & content,
            const RestParams & headers,
            int timeout)
      noexcept
    : verb_(verb), url_(url), headers_(headers),
      content_(content), timeout_(timeout),
      callbacks_(callbacks)
{
}

void
HttpRequest::
clear()
{
    verb_ = "";
    url_ = "";
    callbacks_ = nullptr;
    content_ = HttpRequestContent();
    headers_ = RestParams();
    timeout_ = -1;
}

HttpRequest::
operator bool ()
    const
{
    return !verb_.empty();
}
