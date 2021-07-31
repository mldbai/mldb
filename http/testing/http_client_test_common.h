#pragma once

#include "mldb/http/http_client.h"
#include "mldb/arch/wait_on_address.h"

using namespace std;

namespace MLDB {

typedef tuple<HttpClientError, int, string> ClientResponse;

#define CALL_MEMBER_FN(object, pointer)  (object.*(pointer))

/* sync request helpers */
template<typename Func>
ClientResponse
doRequest(LegacyEventLoop & legacyLoop,
          const string & baseUrl, const string & resource,
          Func func,
          const RestParams & queryParams, const RestParams & headers,
          int timeout = -1)
{
    ClientResponse response;

    HttpClient client(legacyLoop, baseUrl, 4);

    std::atomic<int> done(false);
    auto onResponse = [&] (const HttpRequest & rq,
                           HttpClientError error,
                           int status,
                           string && headers,
                           string && body) {
        int & code = get<1>(response);
        code = status;
        string & body_ = get<2>(response);
        body_ = move(body);
        HttpClientError & errorCode = get<0>(response);
        errorCode = error;
        done = true;
        MLDB::wake_by_address(done);
    };
    auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);

    CALL_MEMBER_FN(client, func)(resource, cbs, queryParams, headers,
                                 timeout);

    while (!done) {
        int oldDone = false;
        MLDB::wait_on_address(done, oldDone);
    }

    return response;
}

ClientResponse
doGetRequest(LegacyEventLoop & loop,
             const string & baseUrl, const string & resource,
             const RestParams & queryParams = RestParams(),
             const RestParams & headers = RestParams(),
             int timeout = -1)
{
    return doRequest(loop, baseUrl, resource, &HttpClient::get,
                     queryParams, headers, timeout);
}

ClientResponse
doDeleteRequest(LegacyEventLoop & loop,
                const string & baseUrl, const string & resource,
                const RestParams & queryParams = RestParams(),
                const RestParams & headers = RestParams(),
                int timeout = -1)
{
    return doRequest(loop, baseUrl, resource, &HttpClient::del,
                     queryParams, headers, timeout);
}

ClientResponse
doUploadRequest(LegacyEventLoop & loop,
                bool isPut,
                const string & baseUrl, const string & resource,
                const string & body, const string & type)
{
    ClientResponse response;

    HttpClient client(loop, baseUrl, 4);

    std::atomic<int> done(false);
    auto onResponse = [&] (const HttpRequest & rq,
                           HttpClientError error,
                           int status,
                           string && headers,
                           string && body) {
        int & code = get<1>(response);
        code = status;
        string & body_ = get<2>(response);
        body_ = move(body);
        HttpClientError & errorCode = get<0>(response);
        errorCode = error;
        done = true;
        MLDB::wake_by_address(done);
    };

    auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);
    HttpRequestContent content(body, type);
    if (isPut) {
        client.put(resource, cbs, content);
    }
    else {
        client.post(resource, cbs, content);
    }

    while (!done) {
        int oldDone = done;
        MLDB::wait_on_address(done, oldDone);
    }

    return response;
}

} // namespace MLDB

