/* http_rest_proxy.h                                               -*- C++ -*-
   Jeremy Barnes, 10 April 2013
   Copyright (c) 2013 Datacratic Inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#pragma once

#include <mutex>
#include "mldb/http/http_header.h"

namespace Json {
struct Value;
} // namespace JSON

namespace Datacratic {


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

    /** Body of the REST call. */
    const std::string & body() const
    {
        return body_;
    }

    Json::Value jsonBody() const;

    /** Get the given response header of the REST call. */
    const std::pair<const std::string, std::string> *
    hasHeader(const std::string & name) const;

    /** Get the given response header of the REST call. */
    const std::string & getHeader(const std::string & name) const;
        
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
    HttpRestContent();

    HttpRestContent(const std::string & str,
                    const std::string & contentType = "");

    HttpRestContent(const char * data, uint64_t size,
                    const std::string & contentType = "",
                    const std::string & contentMd5 = "");

    HttpRestContent(const Json::Value & content,
                    const std::string & contentType = "application/json");

    HttpRestContent(const RestParams & form);

    static std::string urlEncode(const std::string & str);
        
    static std::string urlEncode(const Utf8String & str);
        
    std::string str;

    const char * data;
    uint64_t size;
    bool hasContent;

    std::string contentType;
    std::string contentMd5;
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
                 bool followRedirect = false) const;

    /** Perform a synchronous request from end to end. */
    Response perform(const std::string & verb,
                     const std::string & resource,
                     const Content & content = Content(),
                     const RestParams & queryParams = RestParams(),
                     const RestParams & headers = RestParams(),
                     double timeout = -1,
                     bool exceptions = true,
                     OnData onData = nullptr,
                     OnHeader onHeader = nullptr,
                     bool followRedirect = false) const;
    
    /** URI that will be automatically prepended to resources passed in to
        the perform() methods
    */
    std::string serviceUri;

    /** SSL checks */
    bool noSSLChecks;

    /** Are we debugging? */
    bool debug;

public:
    /** Get a connection. */
    struct Connection;
    Connection getConnection() const;

private:
    /** Private type that implements a connection handler. */
    struct ConnectionHandler;

    void doneConnection(ConnectionHandler * conn);

    /** Lock for connection pool. */
    mutable std::mutex lock;

    /** List of inactive handles.  These can be selected from when a new
        connection needs to be made.
    */
    mutable std::vector<Connection> inactive;

    std::vector<std::string> cookies;
};

std::ostream &
operator << (std::ostream & stream, const HttpRestProxy::Response & response);


} // namespace Datacratic
