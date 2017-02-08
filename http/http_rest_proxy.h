/* http_rest_proxy.h                                               -*- C++ -*-
   Jeremy Barnes, 10 April 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <mutex>
#include "mldb/http/http_header.h"

namespace Json {
struct Value;
} // namespace JSON

namespace MLDB {


/*****************************************************************************/
/* HTTP REST RESPONSE                                                        */
/*****************************************************************************/

/** The response of a request.  Has a return code and a body. */
struct HttpRestResponse {
    HttpRestResponse();

    /** Return code of the REST call. */
    int code() const
    {
        return code_;
    }

    /** Body of the REST call.  This is a binary string with no implied
        encoding.
    */
    const std::string & body() const
    {
        return body_;
    }

    /** Return the body of the request, parsed as a JSON value.  The encoding
        of body must be UTF-8; currently the content-encoding is not
        parsed and not respected.
    */
    Json::Value jsonBody() const;

    /** Return the whole set of headers returned with the HTTP rest request. */
    const HttpHeader & header() const
    {
        return header_;
    }

    /** Return the error message from the underlying layer.  This is only
        valid if errorCode() is non-zero.
    */
    const std::string & errorMessage() const
    {
        return errorMessage_;
    }

    /** Return the underlying error code from the underlying layer.  This
        is transport-dependent, and depends upon the implementation: it may
        be a CURL code, or a system error code, or something else.  It is
        mostly useful for debugging the lower level layer.  The only thing
        that you can rely on is that if it's zero, there was no lower-level
        error.
    */
    int errorCode() const
    {
        return errorCode_;
    }

    /** Get the given response header of the REST call. */
    const std::pair<const std::string, std::string> *
    hasHeader(const std::string & name) const;

    /** Get the given response header of the REST call. */
    const std::string & getHeader(const std::string & name) const;

private:        
    friend class HttpRestProxy;
    long code_;
    std::string body_;
    HttpHeader header_;

    /// Error code for request, normally a CURL code, 0 is OK
    int errorCode_;

    /// Error string for an error request, empty is OK
    std::string errorMessage_;
};


/*****************************************************************************/
/* HTTP REST CONTENT                                                         */
/*****************************************************************************/

/** Structure used to hold content for a POST request. */
struct HttpRestContent {
    /** Construct an object that says "we have no content". */
    HttpRestContent();

    /** Construct content from a binary string and a content type. */
    HttpRestContent(const std::string & str,
                    const std::string & contentType = "");

    /** Construct content from a binary blob and a content type.  Note
        that the data is *not* copied, it must outlive the lifetime of
        this object.
    */
    HttpRestContent(const char * data, uint64_t size,
                    const std::string & contentType = "");

    /** Construct content from a binary blob and JSON object with the
        content.  */
    HttpRestContent(const Json::Value & content,
                    const std::string & contentType = "application/json");
    
    /** Construct content from an HTTP form, contained in the REST
        parameters. */
    HttpRestContent(const RestParams & form);

    /** Url encode the given ASCII string. */
    static std::string urlEncode(const std::string & str);
        
    /** Url encode the given UTF-8 string. */
    static std::string urlEncode(const Utf8String & str);

private:
    friend class HttpRestProxy;
    std::string str;

    const char * data;
    uint64_t size;
    bool hasContent;

    std::string contentType;
};


/*****************************************************************************/
/* HTTP REST PROXY                                                           */
/*****************************************************************************/

/** A class that can be used to perform queries against an HTTP service.
    Normally used to consume REST-like APIs, hence the name.
*/

struct HttpRestProxy {
    typedef HttpRestResponse Response;
    typedef HttpRestContent Content;

    HttpRestProxy(const std::string & serviceUri = "");

    void init(const std::string & serviceUri);

    ~HttpRestProxy();


    /** Look at a response, and add the contents of any set-cookie parameter
        to the cookies for the connection.
    */
    void setCookieFromResponse(const Response& r);

    /** Add a cookie to the connection. */
    void setCookie(const std::string & value);

    /// Callback function for when data is received
    typedef std::function<bool (const std::string &)> OnData;

    /// Callback function for when a response header is received
    typedef std::function<bool (const HttpHeader &)> OnHeader;

    /** Perform a POST request from end to end. */
    Response post(const std::string & resource,
                  const Content & content = Content(),
                  const RestParams & queryParams = RestParams(),
                  const RestParams & headers = RestParams(),
                  double timeout = -1,
                  bool exceptions = true,
                  OnData onData = nullptr,
                  OnHeader onHeader = nullptr) const;

    /** Perform a PUT request from end to end. */
    Response put(const std::string & resource,
                 const Content & content = Content(),
                 const RestParams & queryParams = RestParams(),
                 const RestParams & headers = RestParams(),
                 double timeout = -1,
                 bool exceptions = true,
                 OnData onData = nullptr,
                 OnHeader onHeader = nullptr) const;

    /** Perform a synchronous GET request from end to end. */
    Response get(const std::string & resource,
                 const RestParams & queryParams = RestParams(),
                 const RestParams & headers = RestParams(),
                 double timeout = -1,
                 bool exceptions = true,
                 OnData onData = nullptr,
                 OnHeader onHeader = nullptr,
                 bool followRedirect = false,
                 bool abortOnSlowConnection = false) const;

    /** Perform a synchronous request from end to end. 
        Note that when followRedirect is set, the onHeader
        callback will be called for each redirect, that is, 
        any response with code in [300, 400), but onData
        is only called on the last request.
     **/
    Response perform(const std::string & verb,
                     const std::string & resource,
                     const Content & content = Content(),
                     const RestParams & queryParams = RestParams(),
                     const RestParams & headers = RestParams(),
                     double timeout = -1,
                     bool exceptions = true,
                     OnData onData = nullptr,
                     OnHeader onHeader = nullptr,
                     bool followRedirect = false,
                     bool abortOnSlowConnection = false) const;
    
public:
    /** Get a connection. */
    struct Connection;
    Connection getConnection() const;

private:
    /** Private type that implements a connection handler. */
    struct ConnectionHandler;
    struct Itl;
    std::unique_ptr<Itl> itl;

    void doneConnection(ConnectionHandler * conn);
};

std::ostream &
operator << (std::ostream & stream, const HttpRestProxy::Response & response);


} // namespace MLDB
