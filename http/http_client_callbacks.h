/** http_client_callbacks.h                                        -*- C++ -*-
    Wolfgang Sourdeau, January 2014
    This file is part of MLDB. Copyright 2014 mldb.ai inc. All rights reserved.

*/

#pragma once

#include <functional>


namespace MLDB {

/* Forward declarations */
struct HttpRequest;

/****************************************************************************/
/* HTTP CLIENT ERROR                                                        */
/****************************************************************************/

enum struct HttpClientError {
    None,
    Unknown,
    Timeout,
    HostNotFound,
    CouldNotConnect,
    SendError,
    RecvError
};

std::ostream & operator << (std::ostream & stream, HttpClientError error);


/****************************************************************************/
/* HTTP CLIENT CALLBACKS                                                    */
/****************************************************************************/

struct HttpClientCallbacks {
    typedef std::function<void (const HttpRequest &,
                                const std::string &,
                                int code)> OnResponseStart;
    typedef std::function<void (const HttpRequest &,
                                const char * data, size_t size)> OnData;
    typedef std::function<void (const HttpRequest & rq,
                                HttpClientError errorCode)> OnDone;

    HttpClientCallbacks(OnResponseStart onResponseStart = nullptr,
                        OnData onHeader = nullptr,
                        OnData onData = nullptr,
                        OnDone onDone = nullptr)
        : onResponseStart_(onResponseStart),
          onHeader_(onHeader), onData_(onData),
          onDone_(onDone)
    {
    }

    virtual ~HttpClientCallbacks()
    {
    }

    static const std::string & errorMessage(HttpClientError errorCode);

    /** Callback invoked when the initial response line is received */
    virtual void onResponseStart(const HttpRequest & rq,
                                 const std::string & httpVersion,
                                 int code);

    /** Callback for header lines, invoked once per line */
    virtual void onHeader(const HttpRequest & rq,
                          const char * data, size_t size);

    /** Callback invoked everytime a body chunk has been received. This
        callback can be called multiple time per request. */
    virtual void onData(const HttpRequest & rq,
                        const char * data, size_t size);

    /** Callback for operation completions, implying that no other call will
        be performed for the same request */
    virtual void onDone(const HttpRequest & rq,
                        HttpClientError errorCode);

private:
    OnResponseStart onResponseStart_;
    OnData onHeader_;
    OnData onData_;
    OnDone onDone_;
};


/****************************************************************************/
/* HTTP CLIENT SIMPLE CALLBACKS                                             */
/****************************************************************************/

/* This class is a child of HttpClientCallbacks and offers a simplified
 * interface when support for progressive responses is not necessary. */

struct HttpClientSimpleCallbacks : public HttpClientCallbacks
{
    typedef std::function<void (const HttpRequest &,  /* request */
                                HttpClientError,      /* error code */
                                int,                  /* status code */
                                std::string &&,       /* headers */
                                std::string &&)>      /* body */
        OnResponse;
    HttpClientSimpleCallbacks(const OnResponse & onResponse = nullptr);

    /* HttpClientCallbacks overrides */
    virtual void onResponseStart(const HttpRequest & rq,
                                 const std::string & httpVersion, int code);
    virtual void onHeader(const HttpRequest & rq,
                          const char * data, size_t size);
    virtual void onData(const HttpRequest & rq,
                        const char * data, size_t size);
    virtual void onDone(const HttpRequest & rq, HttpClientError errorCode);

    /** Callback invoked once per response after it has been received in its
        entirery */
    virtual void onResponse(const HttpRequest & rq,
                            HttpClientError error,
                            int status,
                            std::string && headers,
                            std::string && body);

private:
    OnResponse onResponse_;

    int statusCode_;
    std::string headers_;
    std::string body_;
};

} // namespace MLDB
