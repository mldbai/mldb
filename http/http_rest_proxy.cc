/* http_rest_proxy.cc
   Jeremy Barnes, 10 April 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   REST proxy class for http.
*/

#include <curl/curl.h>
#include "mldb/arch/threads.h"
#include <chrono>
#include <thread>
#include "mldb/types/structure_description.h"
#include "curl_wrapper.h"
#include "mldb/base/exc_assert.h"
#include "mldb/ext/jsoncpp/json.h"

#include "http_rest_proxy.h"
#include "http_rest_proxy_impl.h"

using namespace std;


namespace MLDB {

static inline std::string lowercase(const std::string & str)
{
    string result = str;
    for (unsigned i = 0;  i < str.size();  ++i)
        result[i] = tolower(result[i]);
    return result;
}



/*****************************************************************************/
/* HTTP REST RESPONSE                                                        */
/*****************************************************************************/

HttpRestResponse::
HttpRestResponse()
    : code_(0), errorCode_(0)
{
}

Json::Value
HttpRestResponse::
jsonBody() const
{
    return Json::parse(body_);
}

const std::pair<const std::string, std::string> *
HttpRestResponse::
hasHeader(const std::string & name) const
{
    auto it = header_.headers.find(name);
    if (it == header_.headers.end())
        it = header_.headers.find(lowercase(name));
    if (it == header_.headers.end())
        return nullptr;
    return &(*it);
}

const std::string &
HttpRestResponse::
getHeader(const std::string & name) const
{
    auto p = hasHeader(name);
    if (!p)
        throw MLDB::Exception("required header " + name + " not found");
    return p->second;
}


/*****************************************************************************/
/* HTTP REST CONTENT                                                         */
/*****************************************************************************/

HttpRestContent::
HttpRestContent()
    : data(0), size(0), hasContent(false)
{
}

HttpRestContent::
HttpRestContent(const std::string & str,
                const std::string & contentType)
    : str(str), data(str.c_str()), size(str.size()),
      hasContent(true), contentType(contentType)
{
}

HttpRestContent::
HttpRestContent(const char * data, uint64_t size,
                const std::string & contentType)
    : data(data), size(size), hasContent(true),
      contentType(contentType)
{
}

HttpRestContent::
HttpRestContent(const Json::Value & content,
                const std::string & contentType)
    : str(content.toString()), data(str.c_str()),
      size(str.size()), hasContent(true),
      contentType(contentType)
{
}

HttpRestContent::
HttpRestContent(const RestParams & form)
{
    for (auto p: form) {
        if (!str.empty())
            str += "&";
        str += urlEncode(p.first) + "=" + urlEncode(p.second);
    }

    data = str.c_str();
    size = str.size();
    hasContent = true;
    contentType = "application/x-www-form-urlencoded";
}

std::string
HttpRestContent::
urlEncode(const std::string & str)
{
    std::string result;
    for (auto c: str) {
                
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~')
            result += c;
        else result += MLDB::format("%%%02X", c);
    }
    return result;
}
        
std::string
HttpRestContent::
urlEncode(const Utf8String & str)
{
    // Encode each character separately
    std::string result;
    for (auto c: str.rawString()) {
                
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~')
            result += c;
        else result += MLDB::format("%%%02X", c);
    }
    return result;
}


/*****************************************************************************/
/* HTTP REST PROXY                                                           */
/*****************************************************************************/

struct HttpRestProxy::Itl {
    Itl(std::string serviceUri, HttpRestProxy * owner)
        : serviceUri(std::move(serviceUri)), noSSLChecks(false), debug(false),
          owner(owner)
    {
    }

    /** URI that will be automatically prepended to resources passed in to
        the perform() methods
    */
    std::string serviceUri;

    /** SSL checks */
    bool noSSLChecks;

    /** Are we debugging? */
    bool debug;

    HttpRestProxy * owner;

    /** Lock for connection pool. */
    mutable std::mutex lock;

    /** List of inactive handles.  These can be selected from when a new
        connection needs to be made.
    */
    mutable std::vector<Connection> inactive;

    std::vector<std::string> cookies;

    HttpRestProxy::Connection
    getConnection() const
    {
        std::unique_lock<std::mutex> guard(lock);

        if (inactive.empty()) {
            return Connection(new ConnectionHandler, owner);
        }
        else {
            Connection res(std::move(inactive.back()));
            inactive.pop_back();

            // Set proxy to this, so when it's destroyed it's put on our list
            ExcAssert(res.proxy == nullptr);
            res.proxy = owner;
            return res;
        }
    }
    
    void doneConnection(ConnectionHandler * conn)
    {
        std::unique_lock<std::mutex> guard(lock);
        conn->reset();

        // Put a Connection with a null handler on the list so it's
        // destroyed when done
        inactive.emplace_back(conn, nullptr);
    }
};

HttpRestProxy::
HttpRestProxy(const std::string & serviceUri)
    : itl(new Itl(serviceUri, this))
{
}

HttpRestProxy::
~HttpRestProxy()
{
}

void
HttpRestProxy::
init(const std::string & serviceUri)
{
    itl->serviceUri = serviceUri;
}

void
HttpRestProxy::
setCookieFromResponse(const Response& r)
{
    itl->cookies.push_back("Set-Cookie: " + r.getHeader("set-cookie"));
}

void
HttpRestProxy::
setCookie(const std::string & value)
{
    itl->cookies.push_back("Set-Cookie: " + value);
}

HttpRestResponse
HttpRestProxy::
post(const std::string & resource,
     const Content & content,
     const RestParams & queryParams,
     const RestParams & headers,
     double timeout,
     bool exceptions,
     OnData onData,
     OnHeader onHeader) const
{
    return perform("POST", resource, content, queryParams, headers,
                   timeout, exceptions, onData, onHeader);
}

HttpRestResponse
HttpRestProxy::
put(const std::string & resource,
    const Content & content,
    const RestParams & queryParams,
    const RestParams & headers,
    double timeout,
    bool exceptions,
    OnData onData,
    OnHeader onHeader) const
{
    return perform("PUT", resource, content, queryParams, headers,
                   timeout, exceptions, onData, onHeader);
}

HttpRestResponse
HttpRestProxy::
get(const std::string & resource,
    const RestParams & queryParams,
    const RestParams & headers,
    double timeout,
    bool exceptions,
    OnData onData,
    OnHeader onHeader,
    bool followRedirect,
    bool abortOnSlowConnection) const
{
    return perform("GET", resource, Content(), queryParams, headers,
                   timeout, exceptions, onData, onHeader, followRedirect,
                   abortOnSlowConnection);
}

HttpRestResponse
HttpRestProxy::
perform(const std::string & verb,
        const std::string & resource,
        const Content & content,
        const RestParams & queryParams,
        const RestParams & headers,
        double timeout,
        bool exceptions,
        OnData onData,
        OnHeader onHeader,
        bool followRedirect,
        bool abortOnSlowConnection) const
{
    string responseHeaders;
    string body;
    string uri;
    RestParams curlHeaders = headers;
    
    try {
        responseHeaders.clear();
        body.clear();

        Connection connection = getConnection();

        CurlWrapper::Easy & myRequest = *connection;

        uri = itl->serviceUri + resource + queryParams.uriEscaped();

        myRequest.add_option(CURLOPT_CUSTOMREQUEST, verb);

        myRequest.add_option(CURLOPT_URL, uri);

        if (itl->debug)
            myRequest.add_option(CURLOPT_VERBOSE, 1L);

        if (timeout != -1)
            myRequest.add_option(CURLOPT_TIMEOUT, timeout);
        else myRequest.add_option(CURLOPT_TIMEOUT, 0L);

        myRequest.add_option(CURLOPT_NOSIGNAL, 1L);
        myRequest.add_option(CURLOPT_CONNECTTIMEOUT, 20L);

        if (abortOnSlowConnection) {
            // abortOnSlowConnection SPECIFICATION *** /
            myRequest.add_option(CURLOPT_LOW_SPEED_LIMIT, 1024 * 10);
            myRequest.add_option(CURLOPT_LOW_SPEED_TIME, 5);
        }

        if (itl->noSSLChecks) {
            myRequest.add_option(CURLOPT_SSL_VERIFYHOST, 0L);
            myRequest.add_option(CURLOPT_SSL_VERIFYPEER, 0L);
        }

        if (followRedirect) {
            myRequest.add_option(CURLOPT_FOLLOWLOCATION, 1L);
            myRequest.add_option(CURLOPT_MAXREDIRS, 20);
        }

        CurlWrapper::Easy::CurlCallback onWriteData = [&] (char * data, size_t ofs1, size_t ofs2) -> size_t
            {
                if (itl->debug)
                    cerr << "got data " << string(data, data + ofs1 * ofs2) << endl;

                if (onData) {
                    if (!onData(string(data, data + ofs1 * ofs2)))
                        return ofs1 * ofs2 + 1; //indicate an error
                    return ofs1 * ofs2;
                }

                body.append(data, ofs1 * ofs2);
                return ofs1 * ofs2;
            };

        bool afterContinue = false;

        Response response;
        bool headerParsed = false;

        CurlWrapper::Easy::CurlCallback onHeaderLine = [&] (char * data, size_t ofs1, size_t ofs2) -> size_t
            {
                if (headerParsed && followRedirect) {
                    responseHeaders.clear();
                    headerParsed = false;
                }
                ExcAssert(!headerParsed);

                string headerLine(data, ofs1 * ofs2);

                if (itl->debug)
                    cerr << "got header " << headerLine << endl;

                if (headerLine.find("HTTP/1.1 100 Continue") == 0) {
                    afterContinue = true;
                }
                else if (afterContinue) {
                    if (headerLine == "\r\n")
                        afterContinue = false;
                }
                else {
                    responseHeaders.append(headerLine);
                    if (headerLine == "\r\n") {
                        response.header_.parse(responseHeaders);
                        headerParsed = true;

                        if (onHeader)
                            if (!onHeader(response.header_))
                                return ofs1 * ofs2 + 1;  // indicate an error
                    }
                }
                return ofs1 * ofs2;
            };

        myRequest.add_callback_option(CURLOPT_HEADERFUNCTION, CURLOPT_HEADERDATA, onHeaderLine);
        myRequest.add_callback_option(CURLOPT_WRITEFUNCTION, CURLOPT_WRITEDATA, onWriteData);

        for (auto & cookie: itl->cookies)
            myRequest.add_option(CURLOPT_COOKIELIST, cookie);

        if (content.data) {
            myRequest.add_option(CURLOPT_POSTFIELDSIZE, content.size);
            myRequest.add_data_option(CURLOPT_POSTFIELDS, content.data);
            curlHeaders.emplace_back(make_pair("Content-Length", MLDB::format("%lld", content.size)));
            curlHeaders.emplace_back(make_pair("Content-Type", content.contentType));
        }
        else {
            myRequest.add_option(CURLOPT_POSTFIELDSIZE, 0L);
            myRequest.add_option(CURLOPT_POSTFIELDS, "");
        }

        myRequest.add_header_option(curlHeaders);

        CURLcode code = myRequest.perform();
        response.body_ = body;
        if (code != CURLE_OK) {
            if (!exceptions) {
                response.errorCode_ = code;
                response.errorMessage_ = curl_easy_strerror(code);
                return response;
            }
            else {
                throw CurlWrapper::RuntimeError("performing HTTP request",
                                                code);
            }
        }

        myRequest.get_info(CURLINFO_RESPONSE_CODE, response.code_);

        double bytesUploaded;
    
        myRequest.get_info(CURLINFO_SIZE_UPLOAD, bytesUploaded);

        //cerr << "uploaded " << bytesUploaded << " bytes" << endl;

        return response;
    } catch (const CurlWrapper::RuntimeError & exc) {
        if (exc.whatCode() == CURLE_OPERATION_TIMEDOUT)
            throw;
        cerr << "libCurl returned an error with code " << exc.whatCode()
             << endl;
        cerr << "error is " << curl_easy_strerror(exc.whatCode())
             << endl;
        cerr << "verb is " << verb << endl;
        cerr << "uri is " << uri << endl;
        //cerr << "query params are " << queryParams << endl;
        cerr << "headers are " << responseHeaders << endl;
        cerr << "body contains " << body.size() << " bytes" << endl;
        throw;
    }
}

HttpRestProxy::Connection
HttpRestProxy::
getConnection() const
{
    return itl->getConnection();
}

void
HttpRestProxy::
doneConnection(ConnectionHandler * conn)
{
    itl->doneConnection(conn);
}

std::ostream &
operator << (std::ostream & stream, const HttpRestProxy::Response & response)
{
    return stream << response.header() << "\n" << response.body() << "\n";
}

} // namespace MLDB
