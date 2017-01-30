/** http_client_callbacks.cc
    Wolfgang Sourdeau, January 2014                                      \
    This file is part of MLDB. Copyright 2014 mldb.ai inc. All rights reserved.
*/

#include <string.h>

#include "mldb/arch/exception.h"
#include "mldb/http/http_client_callbacks.h"

using namespace std;
using namespace MLDB;


/****************************************************************************/
/* HTTP CLIENT ERROR                                                        */
/****************************************************************************/

std::ostream &
MLDB::
operator << (std::ostream & stream, HttpClientError error)
{
    return stream << HttpClientCallbacks::errorMessage(error);
}


/****************************************************************************/
/* HTTP CLIENT CALLBACKS                                                    */
/****************************************************************************/

const string &
HttpClientCallbacks::
errorMessage(HttpClientError errorCode)
{
    static const string none = "No error";
    static const string unknown = "Unknown error";
    static const string hostNotFound = "Host not found";
    static const string couldNotConnect = "Could not connect";
    static const string timeout = "Request timed out";
    static const string sendError = "Failure sending network data";
    static const string recvError = "Failure receiving network data";

    switch (errorCode) {
    case HttpClientError::None:
        return none;
    case HttpClientError::Unknown:
        return unknown;
    case HttpClientError::Timeout:
        return timeout;
    case HttpClientError::HostNotFound:
        return hostNotFound;
    case HttpClientError::CouldNotConnect:
        return couldNotConnect;
    case HttpClientError::SendError:
        return sendError;
    case HttpClientError::RecvError:
        return recvError;
    default:
        throw MLDB::Exception("invalid error code");
    };
}

void
HttpClientCallbacks::
onResponseStart(const HttpRequest & rq,
                const string & httpVersion, int code)
{
    if (onResponseStart_)
        onResponseStart_(rq, httpVersion, code);
}

void
HttpClientCallbacks::
onHeader(const HttpRequest & rq, const char * data, size_t size)
{
    if (onHeader_)
        onHeader_(rq, data, size);
}

void
HttpClientCallbacks::
onData(const HttpRequest & rq, const char * data, size_t size)
{
    if (onData_)
        onData_(rq, data, size);
}

void
HttpClientCallbacks::
onDone(const HttpRequest & rq, HttpClientError errorCode)
{
    if (onDone_)
        onDone_(rq, errorCode);
}


/****************************************************************************/
/* HTTP CLIENT SIMPLE CALLBACKS                                             */
/****************************************************************************/

HttpClientSimpleCallbacks::
HttpClientSimpleCallbacks(const OnResponse & onResponse)
    : onResponse_(onResponse)
{
}

void
HttpClientSimpleCallbacks::
onResponseStart(const HttpRequest & rq,
                const string & httpVersion, int code)
{
    statusCode_ = code;
}

void
HttpClientSimpleCallbacks::
onHeader(const HttpRequest & rq, const char * data, size_t size)
{
    headers_.append(data, size);
}

void
HttpClientSimpleCallbacks::
onData(const HttpRequest & rq, const char * data, size_t size)
{
    body_.append(data, size);
}

void
HttpClientSimpleCallbacks::
onDone(const HttpRequest & rq, HttpClientError error)
{
    onResponse(rq, error, statusCode_, move(headers_), move(body_));
    statusCode_ = 0;
    headers_ = "";
    body_ = "";
}

void
HttpClientSimpleCallbacks::
onResponse(const HttpRequest & rq,
           HttpClientError error, int status,
           string && headers, string && body)
{
    if (onResponse_) {
        onResponse_(rq, error, status, move(headers), move(body));
    }
}
