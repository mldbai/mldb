// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** s3.cc
    Jeremy Barnes, 3 July 2012
    Copyright (c) 2012 mldb.ai inc.  All rights reserved.

    Code to talk to s3.
*/

#include <exception>
#include <future>

#include "mldb/arch/exception.h"
#include "mldb/base/exc_assert.h"
#include "mldb/http/http_client.h"
#include "mldb/io/legacy_event_loop.h"
#include "mldb/io/message_loop.h"
#include "mldb/jml/utils/file_functions.h"
#include "mldb/soa/credentials/credentials.h"
#include "mldb/soa/credentials/credential_provider.h"
#include "mldb/utils/testing/print_utils.h"
#include "mldb/soa/service/xml_helpers.h"
#include "mldb/types/structure_description.h"
#include "mldb/vfs/fs_utils.h"

#include "mldb/soa/service/s3.h"


using namespace std;
using namespace MLDB;


namespace {

/****************************************************************************/
/* S3 GLOBALS                                                               */
/****************************************************************************/

struct S3Globals {
    S3Globals()
        : baseRetryDelay(3), numRetries(-1)
    {
        if (numRetries == -1) {
            char * numRetriesEnv = getenv("S3_RETRIES");
            if (numRetriesEnv) {
                numRetries = atoi(numRetriesEnv);
            }
            else {
                numRetries = 45;
            }
        }

        loop.start();
    }

    /* This method return an HttpClient attached to the given hostname. The
       instances thereby created are kept alive until the death of the
       process. This method is thread-safe. */
    HttpClient & getClient(const string & bucket,
                           const string & baseHostname = "s3.amazonaws.com")
    {
        string hostname = bucket;
        if (hostname.size() > 0) {
            hostname += ".";
        }
        hostname += baseHostname;

        unique_lock<mutex> guard(clientsLock);
        auto it = clients.find(hostname);
        if (it == clients.end()) {
            HttpClient newClient(loop, "http://" + hostname, 30);

            /* By disabling "Expect: 100-Continue", we remove one round trip
               per put request. Since the chances to receive an error after
               PUT requests are low, this reduces latency for the great
               majority of our requests.
               https://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.1.1
            */
            newClient.sendExpect100Continue(false);
            it = clients.insert(std::make_pair(std::move(hostname),
                                               std::move(newClient)))
                        .first;
        }

        return it->second;
    }

    int baseRetryDelay;
    int numRetries;
    LegacyEventLoop loop;

private:
    mutex clientsLock;
    map<string, HttpClient> clients;
};

static S3Globals &
getS3Globals()
{
    static S3Globals s3Config;
    return s3Config;
}


/****************************************************************************/
/* S3 REQUEST STATE                                                         */
/****************************************************************************/

struct S3RequestState {
    S3RequestState(const shared_ptr<S3Api::SignedRequest> & rq,
                   const S3Api::OnResponse & onResponse)
        : rq(rq), onResponse(onResponse),
          range(rq->params.downloadRange), retries(0)
    {
    }

    RestParams makeHeaders()
        const
    {
        RestParams headers = rq->params.headers;
        headers.push_back({"Date", rq->params.date});
        headers.push_back({"Authorization", rq->auth});
        if (rq->params.useRange()) {
            headers.push_back({"Range", range.headerValue()});
        }

        return headers;
    }

    int makeTimeout()
        const
    {
        double expectedTimeSeconds
            = (range.size / 1000000.0) / rq->bandwidthToServiceMbps;
        return 15 + std::max<int>(30, expectedTimeSeconds * 6);
    }

    shared_ptr<S3Api::SignedRequest> rq;

    S3Api::OnResponse onResponse;

    string body;
    string requestBody;
    S3Api::Range range;
    int retries;
};

/****************************************************************************/
/* S3 REQUEST CALLBACKS                                                     */
/****************************************************************************/

struct S3RequestCallbacks : public HttpClientCallbacks {
    S3RequestCallbacks(const shared_ptr<S3RequestState> & state)
        : state_(state)
    {
    }

    virtual void onResponseStart(const HttpRequest & rq,
                                 const string & httpVersion,
                                 int code);
    virtual void onHeader(const HttpRequest & rq,
                          const char * data, size_t size);
    virtual void onData(const HttpRequest & rq,
                        const char * data, size_t size);
    virtual void onDone(const HttpRequest & rq,
                        HttpClientError errorCode);

    pair<string, string> detectXMLError() const;
    string httpErrorContext() const;
    void scheduleRestart() const;

    shared_ptr<S3RequestState> state_;

    S3Api::Response response_;
    string header_;
};

void
performStateRequest(const shared_ptr<S3RequestState> & state)
{
    auto & client = getS3Globals().getClient(state->rq->params.bucket);

    const S3Api::RequestParams & params = state->rq->params;
    auto callbacks = make_shared<S3RequestCallbacks>(state);
    RestParams headers = state->makeHeaders();
    int timeout = state->makeTimeout();

    if (!client.enqueueRequest(params.verb, state->rq->resource,
                                  callbacks,
                                  state->rq->params.content,
                                  /* query params already encoded in
                                     resource */
                                  {},
                                  headers,
                                  timeout)) {
        /* TODO: should invoke onResponse with "too many requests" */
        throw MLDB::Exception("the http client could not enqueue the request");
    }
}

void
S3RequestCallbacks::
onResponseStart(const HttpRequest & rq, const string & httpVersion,
                int code)
{
    response_.code_ = code;
}

void
S3RequestCallbacks::
onHeader(const HttpRequest & rq, const char * data, size_t size)
{
    header_.append(data, size);
}

void
S3RequestCallbacks::
onData(const HttpRequest & rq, const char * data, size_t size)
{
    state_->requestBody.append(data, size);
}

void
S3RequestCallbacks::
onDone(const HttpRequest & rq, HttpClientError errorCode)
{
    bool errorCondition(false);
    bool recoverable(false);
    string errorCause;
    string errorDetails;

    if (errorCode == HttpClientError::None) {
        auto xmlError = detectXMLError();
        if (!xmlError.first.empty()) {
            errorCondition = true;
            errorCause = "REST error code \"" + xmlError.first + "\"";
            errorDetails = ("http status: "
                            + to_string(response_.code_) + "\n"
                            + "message: " + xmlError.second);

            /* retry on "InternalError" */
            if (xmlError.first == "InternalError") {
                recoverable = true;
            }
        }
        else if (response_.code_ >= 300 && response_.code_ != 404) {
            errorCondition = true;
            errorCause = "HTTP status code " + to_string(response_.code_);
            errorDetails = httpErrorContext();

            /* retry on 50X range errors */
            if (response_.code_ >= 500 and response_.code_ < 505) {
                recoverable = true;
            }
        }
    }
    else {
        errorCondition = true;
        errorCause = "internal error \"" + errorMessage(errorCode) + "\"";
        recoverable = true;
        if (state_->rq->params.useRange()) {
            state_->range.adjust(state_->requestBody.size());
        }
        state_->body.append(state_->requestBody);
    }

    if (errorCondition) {
        string recoverability;
        if (recoverable) {
            if (state_->retries < getS3Globals().numRetries) {
                recoverability = "The operation will be retried.";
                state_->retries++;
            }
            else {
                recoverability = "The operation was retried too many times.";
                recoverable = false;
            }
        }
        else {
            recoverability = "The error is non recoverable.";
        }

        string message("S3 operation failed with " + errorCause);

        const S3Api::RequestParams & params = state_->rq->params;
        string diagnostic(message + "\n"
                          + "operation: " + params.verb
                          + " " + state_->rq->resource + "\n");
        if (!errorDetails.empty()) {
            diagnostic += errorDetails + "\n";
        }
        diagnostic += recoverability + "\n";

        cerr << diagnostic;

        header_.clear();
        state_->requestBody.clear();
        if (recoverable) {
            scheduleRestart();
        }
        else {
            auto excPtr = std::make_exception_ptr(MLDB::Exception(message));
            state_->onResponse(std::move(response_), excPtr);
        }
    }
    else {
        response_.header_.parse(header_, false);
        header_.clear();
        state_->body.append(state_->requestBody);
        state_->requestBody.clear();
        response_.body_ = std::move(state_->body);
        state_->onResponse(std::move(response_), nullptr);
    }
}

pair<string, string>
S3RequestCallbacks::
detectXMLError()
    const
{
    /* Detect so-called "REST error"
       (http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html)

       Some S3 methods may return an XML error AND still have a 200 HTTP
       status code:
       http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
       Explanation of the why:
       https://github.com/aws/aws-sdk-go/issues/501.
    */

    pair<string, string> xmlError; /* {code, message} */

    const S3Api::RequestParams & params = state_->rq->params;
    if (!(response_.code_ == 200
          && (params.verb == "GET" || params.verb == "HEAD"))
        && ((header_.find("Content-Type: application/xml")
             != string::npos)
            || (header_.find("content-type: application/xml")
                != string::npos))) {
        if (!state_->requestBody.empty()) {
            std::unique_ptr<tinyxml2::XMLDocument> localXml;
            localXml.reset(new tinyxml2::XMLDocument());
            localXml->Parse(state_->requestBody.c_str());
            auto element = tinyxml2::XMLHandle(*localXml)
                .FirstChildElement("Error")
                .ToElement();
            if (element) {
                xmlError.first = extract<string>(element, "Code");
                xmlError.second = extract<string>(element, "Message");
            }
        }
    }

    return xmlError;
}

string
S3RequestCallbacks::
httpErrorContext()
    const
{
    string context = "http status: " + to_string(response_.code_) + "\n";
    if (header_.size() > 0) {
        context += "response headers:\n" + header_;
    }
    if (!state_->requestBody.empty()) {
        context += (string("response body (")
                    + to_string(state_->requestBody.size())
                    + " bytes):\n" + state_->requestBody + "\n");
    }

    return context;
}

void
S3RequestCallbacks::
scheduleRestart()
    const
{
    S3Globals & globals = getS3Globals();

    // allow a maximum of 384 seconds for retry delays (1 << 7 * 3)
    int multiplier = (state_->retries < 8
                      ? (1 << state_->retries)
                      : state_->retries << 7);
    double numSeconds = ::random() % (globals.baseRetryDelay
                                      * multiplier);
    if (numSeconds == 0) {
        numSeconds = globals.baseRetryDelay * multiplier;
    }

    /* TODO: "numSeconds" is set to a fixed value here because the base
       multiplier for exponential backoff should be carefully tuned in order
       to reduce the chances of stalling transfers. */
    numSeconds = 0.05;

    const S3Api::RequestParams & params = state_->rq->params;

    cerr << ("S3 operation retry in" + to_string(numSeconds) + " seconds: "
             + params.verb + " " + params.resource + "\n");

    auto timer = make_shared<PeriodicEventSource>();

    auto state = state_;
    auto onTimeout = [&, timer, state] (uint64_t ticks) {
        S3Globals & globals = getS3Globals();
        performStateRequest(state);
        globals.loop.loop().removeSource(timer.get());
    };
    timer->init(numSeconds, std::move(onTimeout));
    globals.loop.loop().addSource("retry-timer-" + randomString(8), timer);
}

} // file scope


namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(S3Config);


S3ConfigDescription::
S3ConfigDescription()
{
    addField("accessKeyId", &S3Config::accessKeyId, "");
    addField("accessKey", &S3Config::accessKey, "");
}


/****************************************************************************/
/* S3 API :: RANGE                                                          */
/****************************************************************************/

S3Api::Range::
Range(uint64_t aSize)
    : offset(0), size(aSize)
{}

S3Api::Range::
Range(uint64_t aOffset, uint64_t aSize)
    : offset(aOffset), size(aSize)
{}

uint64_t
S3Api::Range::
endPos()
    const
{
    return (offset + size - 1);
}

void
S3Api::Range::
adjust(size_t downloaded)
{
    if (downloaded > size) {
        throw MLDB::Exception("excessive adjustment size: downloaded %d size %d",
                            downloaded, size);
    }
    offset += downloaded;
    size -= downloaded;
}

string
S3Api::Range::
headerValue()
    const
{
    return (string("bytes=")
            + std::to_string(offset) + "-" + std::to_string(endPos()));
}

bool
S3Api::Range::
operator == (const Range & other)
    const
{
    return offset == other.offset && size == other.size;
}

bool
S3Api::Range::
operator != (const Range & other)
    const
{
    return !(*this == other);
}


/****************************************************************************/
/* S3 API :: REQUEST PARAMS                                                 */
/****************************************************************************/

S3Api::RequestParams::
RequestParams()
    : downloadRange(0)
{
}

bool
S3Api::RequestParams::
useRange()
    const
{
    /* The "Range" header is only useful with GET and when the range
       is explicitly specified. The use of Range::Full means that we
       always request the full body, even during retries. This is
       mainly useful for requests on non-object urls, where that
       header is ignored by the S3 servers. */
    return (verb == "GET" && downloadRange != Range::Full);
}


/****************************************************************************/
/* S3 API :: RESPONSE                                                       */
/****************************************************************************/

S3Api::Response::
Response()
    : code_(0)
{
}

const string &
S3Api::Response::
body()
    const
{
    if (code_ < 200 || code_ >= 300)
        throw MLDB::Exception("invalid http code returned");
    return body_;
}

std::unique_ptr<tinyxml2::XMLDocument>
S3Api::Response::
bodyXml()
    const
{
    if (code_ != 200)
        throw MLDB::Exception("invalid http code returned");
    std::unique_ptr<tinyxml2::XMLDocument> result(new tinyxml2::XMLDocument());
    result->Parse(body_.c_str());
    return result;
}

S3Api::Response::
operator std::unique_ptr<tinyxml2::XMLDocument>()
    const
{
    return bodyXml();
}

string
S3Api::Response::
bodyXmlStr()
    const
{
    auto x = bodyXml();
    tinyxml2::XMLPrinter printer;
    x->Print(&printer);
    return printer.CStr();
}

string
S3Api::Response::
getHeader(const string & name)
    const
{
    auto it = header_.headers.find(name);
    if (it == header_.headers.end())
        throw MLDB::Exception("required header " + name + " not found");
    return it->second;
}


/****************************************************************************/
/* S3 API :: OBJECT METADATA                                                */
/****************************************************************************/

S3Api::ObjectMetadata::
ObjectMetadata()
    : redundancy(REDUNDANCY_DEFAULT),
      serverSideEncryption(SSE_NONE),
      numRequests(8)
{
}

S3Api::ObjectMetadata::
ObjectMetadata(Redundancy redundancy)
    : redundancy(redundancy),
      serverSideEncryption(SSE_NONE),
      numRequests(8)
{
}

RestParams
S3Api::ObjectMetadata::
getRequestHeaders()
    const
{
    RestParams result;
    Redundancy redundancy = this->redundancy;

    if (redundancy == REDUNDANCY_DEFAULT)
        redundancy = defaultRedundancy;

    if (redundancy == REDUNDANCY_REDUCED)
        result.push_back({"x-amz-storage-class", "REDUCED_REDUNDANCY"});
    else if(redundancy == REDUNDANCY_GLACIER)
        result.push_back({"x-amz-storage-class", "GLACIER"});
    if (serverSideEncryption == SSE_AES256)
        result.push_back({"x-amz-server-side-encryption", "AES256"});
    if (contentType != "")
        result.push_back({"Content-Type", contentType});
    if (contentEncoding != "")
        result.push_back({"Content-Encoding", contentEncoding});
    if (acl != "")
        result.push_back({"x-amz-acl", acl});
    for (auto md: metadata) {
        result.push_back({"x-amz-meta-" + md.first, md.second});
    }
    return result;
}


/****************************************************************************/
/* S3 API :: MULTI PART UPLOAD PART                                         */
/****************************************************************************/

S3Api::MultiPartUploadPart::
MultiPartUploadPart()
    : partNumber(0), done(false)
{
}

void
S3Api::MultiPartUploadPart::
fromXml(tinyxml2::XMLElement * element)
{
    partNumber = extract<int>(element, "PartNumber");
    lastModified = extract<string>(element, "LastModified");
    etag = extract<string>(element, "ETag");
    size = extract<uint64_t>(element, "Size");
    done = true;
}


/****************************************************************************/
/* S3 API :: OBJECT INFO                                                    */
/****************************************************************************/

S3Api::ObjectInfo::
ObjectInfo(tinyxml2::XMLNode * element)
{
    size = extract<uint64_t>(element, "Size");
    key  = extract<string>(element, "Key");
    string lastModifiedStr = extract<string>(element, "LastModified");
    lastModified = Date::parseIso8601DateTime(lastModifiedStr);
    etag = extract<string>(element, "ETag");

    if (pathExists(element, "Owner/ID")) {
        ownerId = extract<string>(element, "Owner/ID");
    }

    ownerName = extractDef<string>(element, "Owner/DisplayName", "");
    storageClass = extract<string>(element, "StorageClass");
    exists = true;
}

S3Api::ObjectInfo::
ObjectInfo(const S3Api::Response & response)
{
    exists = true;
    lastModified = Date::parse(response.getHeader("last-modified"),
            "%a, %e %b %Y %H:%M:%S %Z");
    size = response.header_.contentLength;
    etag = response.getHeader("etag");
    storageClass = ""; // Not available in headers
    ownerId = "";      // Not available in headers
    ownerName = "";    // Not available in headers
}


/****************************************************************************/
/* S3 API                                                                   */
/****************************************************************************/

string
S3Api::
s3EscapeResource(const string & str)
{
    if (str.size() == 0) {
        throw MLDB::Exception("empty str name");
    }

    if (str[0] != '/') {
        throw MLDB::Exception("resource name must start with a '/'");
    }

    string result;
    for (auto c: str) {

        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~' || c == '/')
            result += c;
        else result += MLDB::format("%%%02X", c);
    }

    return result;
}

double
S3Api::
defaultBandwidthToServiceMbps = 20.0;

S3Api::Range S3Api::Range::Full(0);

S3Api::
S3Api()
{
    bandwidthToServiceMbps = defaultBandwidthToServiceMbps;
}

S3Api::
S3Api(const string & accessKeyId,
      const string & accessKey,
      double bandwidthToServiceMbps,
      const string & defaultProtocol,
      const string & serviceUri)
    : accessKeyId(accessKeyId),
      accessKey(accessKey),
      defaultProtocol(defaultProtocol),
      serviceUri(serviceUri),
      bandwidthToServiceMbps(bandwidthToServiceMbps)
{
}

void
S3Api::
init(const string & accessKeyId,
     const string & accessKey,
     double bandwidthToServiceMbps,
     const string & defaultProtocol,
     const string & serviceUri)
{
    this->accessKeyId = accessKeyId;
    this->accessKey = accessKey;
    this->defaultProtocol = defaultProtocol;
    this->serviceUri = serviceUri;
    this->bandwidthToServiceMbps = bandwidthToServiceMbps;
}

void
S3Api::
perform(const OnResponse & onResponse, const shared_ptr<SignedRequest> & rq)
    const
{
    size_t spacePos = rq->resource.find(" ");
    if (spacePos != string::npos) {
        throw MLDB::Exception("url '" + rq->resource + "' contains an unescaped"
                            " space at position " + to_string(spacePos));
    }

    performStateRequest(make_shared<S3RequestState>(rq, onResponse));
}

S3Api::Response
S3Api::
performSync(const shared_ptr<SignedRequest> & rq)
    const
{
    std::promise<S3Api::Response> respPromise;

    auto onResponse = [&] (S3Api::Response && response,
                           std::exception_ptr excPtr) {
        if (excPtr) {
            respPromise.set_exception(excPtr);
        }
        else {
            respPromise.set_value(response);
        }
    };
    perform(onResponse, rq);

    auto respFuture = respPromise.get_future();
    respFuture.wait();

    return respFuture.get();
}

string
S3Api::
signature(const RequestParams & request) const
{
    string digest
        = S3Api::getStringToSignV2Multi(request.verb,
                                        request.bucket,
                                        request.resource, request.subResource,
                                        request.contentType, request.contentMd5,
                                        request.date, request.headers);

    //cerr << "digest = " << digest << endl;

    return signV2(digest, accessKey);
}

shared_ptr<S3Api::SignedRequest>
S3Api::
prepare(const RequestParams & request) const
{
    string protocol = defaultProtocol;
    if(protocol.length() == 0){
        throw MLDB::Exception("attempt to perform s3 request without a "
            "default protocol. (Could be caused by S3Api initialisation with "
            "the empty constructor.)");
    }

    auto result = make_shared<SignedRequest>();
    result->params = request;
    result->bandwidthToServiceMbps = bandwidthToServiceMbps;

    if (request.resource.find("//") != string::npos)
        throw MLDB::Exception("attempt to perform s3 request with double slash: "
                            + request.resource);

    result->resource += request.resource;
    if (request.subResource.size() > 0) {
        result->resource += "?" + request.subResource;
    }

    for (unsigned i = 0;  i < request.queryParams.size();  ++i) {
        if (i == 0 && request.subResource == "")
            result->resource += "?";
        else
            result->resource += "&";
        result->resource += (uriEncode(request.queryParams[i].first)
                            + "=" + uriEncode(request.queryParams[i].second));
    }

    string sig = signature(request);
    result->auth = "AWS " + accessKeyId + ":" + sig;

    //cerr << "result->resource = " << result->resource << endl;
    //cerr << "result->auth = " << result->auth << endl;

    return result;
}

S3Api::Response
S3Api::
head(const string & bucket,
     const string & resource,
     const string & subResource,
     const RestParams & headers,
     const RestParams & queryParams)
    const
{
    return headEscaped(bucket, s3EscapeResource(resource), subResource,
                       headers, queryParams);
}

S3Api::Response
S3Api::
headEscaped(const string & bucket,
            const string & resource,
            const string & subResource,
            const RestParams & headers,
            const RestParams & queryParams) const
{
    RequestParams request;
    request.verb = "HEAD";
    request.bucket = bucket;
    request.resource = resource;
    request.subResource = subResource;
    request.headers = headers;
    request.queryParams = queryParams;
    request.date = Date::now().printRfc2616();

    return performSync(prepare(request));;
}

S3Api::Response
S3Api::
get(const string & bucket,
    const string & resource,
    const Range & downloadRange,
    const string & subResource,
    const RestParams & headers,
    const RestParams & queryParams)
    const
{
    return getEscaped(bucket, s3EscapeResource(resource), downloadRange,
                      subResource, headers, queryParams);
}

S3Api::Response
S3Api::
getEscaped(const string & bucket,
           const string & resource,
           const Range & downloadRange,
           const string & subResource,
           const RestParams & headers,
           const RestParams & queryParams) const
{
    RequestParams request;
    request.verb = "GET";
    request.bucket = bucket;
    request.resource = resource;
    request.subResource = subResource;
    request.headers = headers;
    request.queryParams = queryParams;
    request.date = Date::now().printRfc2616();
    request.downloadRange = downloadRange;

    return performSync(prepare(request));;
}

void
S3Api:: 
getAsync(const OnResponse & onResponse,
         const string & bucket,
         const string & resource,
         const Range & downloadRange,
         const string & subResource,
         const RestParams & headers,
         const RestParams & queryParams)
        const
{
    getEscapedAsync(onResponse, bucket, s3EscapeResource(resource),
                    downloadRange, subResource, headers,
                    queryParams);
}

void
S3Api::
getEscapedAsync(const S3Api::OnResponse & onResponse,
                const string & bucket,
                const string & resource,
                const Range & downloadRange,
                const string & subResource,
                const RestParams & headers,
                const RestParams & queryParams) const
{
    RequestParams request;
    request.verb = "GET";
    request.bucket = bucket;
    request.resource = resource;
    request.subResource = subResource;
    request.headers = headers;
    request.queryParams = queryParams;
    request.date = Date::now().printRfc2616();
    request.downloadRange = downloadRange;

    perform(onResponse, prepare(request));
}

/** Perform a POST request from end to end. */
S3Api::Response
S3Api::
post(const string & bucket,
     const string & resource,
     const string & subResource,
     const RestParams & headers,
     const RestParams & queryParams,
     const HttpRequestContent & content)
    const
{
    return postEscaped(bucket, s3EscapeResource(resource), subResource,
                       headers, queryParams, content);
}

S3Api::Response
S3Api::
postEscaped(const string & bucket,
            const string & resource,
            const string & subResource,
            const RestParams & headers,
            const RestParams & queryParams,
            const HttpRequestContent & content)
    const
{
    RequestParams request;
    request.verb = "POST";
    request.bucket = bucket;
    request.resource = resource;
    request.subResource = subResource;
    request.headers = headers;
    request.queryParams = queryParams;
    request.date = Date::now().printRfc2616();
    request.content = content;

    return performSync(prepare(request));;
}

S3Api::Response
S3Api::
put(const string & bucket,
    const string & resource,
    const string & subResource,
    const RestParams & headers,
    const RestParams & queryParams,
    const HttpRequestContent & content)
    const
{
    return putEscaped(bucket, s3EscapeResource(resource), subResource,
                      headers, queryParams, content);
}

S3Api::Response
S3Api::
putEscaped(const string & bucket,
           const string & resource,
           const string & subResource,
           const RestParams & headers,
           const RestParams & queryParams,
           const HttpRequestContent & content)
    const
{
    RequestParams request;
    request.verb = "PUT";
    request.bucket = bucket;
    request.resource = resource;
    request.subResource = subResource;
    request.headers = headers;
    request.queryParams = queryParams;
    request.date = Date::now().printRfc2616();
    request.content = content;

    return performSync(prepare(request));;
}

void
S3Api::
putAsync(const OnResponse & onResponse,
         const string & bucket,
         const string & resource,
         const string & subResource,
         const RestParams & headers,
         const RestParams & queryParams,
         const HttpRequestContent & content)
    const
{
    putEscapedAsync(onResponse, bucket, s3EscapeResource(resource),
                    subResource, headers, queryParams, content);
}

void
S3Api::
putEscapedAsync(const OnResponse & onResponse,
                const string & bucket,
                const string & resource,
                const string & subResource,
                const RestParams & headers,
                const RestParams & queryParams,
                const HttpRequestContent & content)
    const
{
    RequestParams request;
    request.verb = "PUT";
    request.bucket = bucket;
    request.resource = resource;
    request.subResource = subResource;
    request.headers = headers;
    request.queryParams = queryParams;
    request.date = Date::now().printRfc2616();
    request.content = content;

    perform(onResponse, prepare(request));
}

S3Api::Response
S3Api::
erase(const string & bucket,
      const string & resource,
      const string & subResource,
      const RestParams & headers,
      const RestParams & queryParams)
    const
{
    return eraseEscaped(bucket, s3EscapeResource(resource), subResource,
                        headers, queryParams);
}

S3Api::Response
S3Api::
eraseEscaped(const string & bucket,
             const string & resource,
             const string & subResource,
             const RestParams & headers,
             const RestParams & queryParams)
    const
{
    RequestParams request;
    request.verb = "DELETE";
    request.bucket = bucket;
    request.resource = resource;
    request.subResource = subResource;
    request.headers = headers;
    request.queryParams = queryParams;
    request.date = Date::now().printRfc2616();

    return performSync(prepare(request));;
}

pair<bool,string>
S3Api::isMultiPartUploadInProgress(
    const string & bucket,
    const string & resource) const
{
    // Contains the resource without the leading slash
    string outputPrefix(resource, 1);

    // Check if there is already a multipart upload in progress
    auto inProgressReq = get(bucket, "/", Range::Full, "uploads", {},
                             { { "prefix", outputPrefix } });

    //cerr << inProgressReq.bodyXmlStr() << endl;

    auto inProgress = inProgressReq.bodyXml();

    using namespace tinyxml2;

    XMLHandle handle(*inProgress);

    auto upload
        = handle
        .FirstChildElement("ListMultipartUploadsResult")
        .FirstChildElement("Upload")
        .ToElement();

    string uploadId;
    vector<MultiPartUploadPart> parts;


    for (; upload; upload = upload->NextSiblingElement("Upload"))
    {
        XMLHandle uploadHandle(upload);

        auto key = extract<string>(upload, "Key");

        if (key != outputPrefix)
            continue;

        // Already an upload in progress
        string uploadId = extract<string>(upload, "UploadId");

        return make_pair(true,uploadId);
    }
    return make_pair(false,"");
}

S3Api::MultiPartUpload
S3Api::
obtainMultiPartUpload(const string & bucket,
                      const string & resource,
                      const ObjectMetadata & metadata,
                      UploadRequirements requirements) const
{
    string escapedResource = s3EscapeResource(resource);
    // Contains the resource without the leading slash
    string outputPrefix(resource, 1);

    string uploadId;
    vector<MultiPartUploadPart> parts;

    if (requirements != UR_FRESH) {

        // Check if there is already a multipart upload in progress
        auto inProgressReq = get(bucket, "/", Range::Full, "uploads", {},
                                 { { "prefix", outputPrefix } });

        //cerr << "in progress requests:" << endl;
        //cerr << inProgressReq.bodyXmlStr() << endl;

        auto inProgress = inProgressReq.bodyXml();

        using namespace tinyxml2;

        XMLHandle handle(*inProgress);

        auto upload
            = handle
            .FirstChildElement("ListMultipartUploadsResult")
            .FirstChildElement("Upload")
            .ToElement();

        // uint64_t partSize = 0;
        uint64_t currentOffset = 0;

        for (; upload; upload = upload->NextSiblingElement("Upload")) {
            XMLHandle uploadHandle(upload);

            auto key = extract<string>(upload, "Key");

            if (key != outputPrefix)
                continue;

            // Already an upload in progress
            string uploadId = extract<string>(upload, "UploadId");

            // From here onwards is only useful if we want to continue a half-finished
            // upload.  Instead, we will delete it to avoid problems with creating
            // half-finished files when we don't know what we're doing.

            auto deletedInfo = eraseEscaped(bucket, escapedResource,
                                            "uploadId=" + uploadId);

            continue;

            // TODO: check metadata, etc
            auto inProgressInfo = getEscaped(bucket, escapedResource, Range::Full,
                                             "uploadId=" + uploadId)
                .bodyXml();

            XMLHandle handle(*inProgressInfo);

            auto foundPart
                = handle
                .FirstChildElement("ListPartsResult")
                .FirstChildElement("Part")
                .ToElement();

            int numPartsDone = 0;
            uint64_t biggestPartSize = 0;
            for (; foundPart;
                 foundPart = foundPart->NextSiblingElement("Part"),
                     ++numPartsDone) {
                MultiPartUploadPart currentPart;
                currentPart.fromXml(foundPart);
                if (currentPart.partNumber != numPartsDone + 1) {
                    //cerr << "missing part " << numPartsDone + 1 << endl;
                    // from here we continue alone
                    break;
                }
                currentPart.startOffset = currentOffset;
                currentOffset += currentPart.size;
                biggestPartSize = std::max(biggestPartSize, currentPart.size);
                parts.push_back(currentPart);
            }

            // partSize = biggestPartSize;

            //cerr << "numPartsDone = " << numPartsDone << endl;
            //cerr << "currentOffset = " << currentOffset
            //     << "dataSize = " << dataSize << endl;
        }
    }

    if (uploadId.empty()) {
        //cerr << "getting new ID" << endl;

        RestParams headers = metadata.getRequestHeaders();
        auto result = postEscaped(bucket, escapedResource,
                                  "uploads", headers).bodyXml();
        //cerr << "result = " << result << endl;

        uploadId
            = extract<string>(result, "InitiateMultipartUploadResult/UploadId");

        //cerr << "new upload = " << uploadId << endl;
    }
        //return;

    MultiPartUpload result;
    result.parts.swap(parts);
    result.id = uploadId;
    return result;
}

string
S3Api::
finishMultiPartUpload(const string & bucket,
                      const string & resource,
                      const string & uploadId,
                      const std::vector<string> & etags) const
{
    using namespace tinyxml2;
    // Finally, send back a response to join the parts together
    ExcAssert(etags.size());

    XMLDocument joinRequest;
    auto r = joinRequest.InsertFirstChild(joinRequest.NewElement("CompleteMultipartUpload"));
    for (unsigned i = 0;  i < etags.size();  ++i) {
        auto n = r->InsertEndChild(joinRequest.NewElement("Part"));
        n->InsertEndChild(joinRequest.NewElement("PartNumber"))
            ->InsertEndChild(joinRequest.NewText(MLDB::format("%d", i + 1).c_str()));
        n->InsertEndChild(joinRequest.NewElement("ETag"))
            ->InsertEndChild(joinRequest.NewText(etags[i].c_str()));
    }

    string escapedResource = s3EscapeResource(resource);

    HttpRequestContent xmlReq(xmlDocumentAsString(joinRequest));
    auto joinResponse
        = postEscaped(bucket, escapedResource, "uploadId=" + uploadId,
                      {}, {}, xmlReq);

    //cerr << joinResponse.bodyXmlStr() << endl;

    auto joinResponseXml = joinResponse.bodyXml();

    try {

        string etag = extract<string>(joinResponseXml,
                                      "CompleteMultipartUploadResult/ETag");
        return etag;
    } catch (const std::exception & exc) {
        cerr << ("--- request is\n"
                 + xmlDocumentAsString(joinRequest) + "\n"
                 + "error completing multipart upload: "
                 + exc.what()
                 + "\n");
        throw;
    }
}

void
S3Api::
forEachObject(const string & bucket,
              const string & prefix,
              const OnObject & onObject,
              const OnSubdir & onSubdir,
              const string & delimiter,
              int depth,
              const string & startAt) const
{
    using namespace tinyxml2;

    string marker = startAt;
    // bool firstIter = true;
    do {
        //cerr << "Starting at " << marker << endl;

        RestParams queryParams;
        if (prefix != "")
            queryParams.push_back({"prefix", prefix});
        if (delimiter != "")
            queryParams.push_back({"delimiter", delimiter});
        if (marker != "")
            queryParams.push_back({"marker", marker});

        auto listingResult = get(bucket, "/", Range::Full, "",
                                 {}, queryParams);
        auto listingResultXml = listingResult.bodyXml();

        string foundPrefix
            = extractDef<string>(listingResult, "ListBucketResult/Prefix", "");
        string truncated
            = extract<string>(listingResult, "ListBucketResult/IsTruncated");
        bool isTruncated = truncated == "true";
        marker = "";

        auto foundObject
            = XMLHandle(*listingResultXml)
            .FirstChildElement("ListBucketResult")
            .FirstChildElement("Contents")
            .ToElement();

        bool stop = false;

        for (int i = 0; onObject && foundObject;
             foundObject = foundObject->NextSiblingElement("Contents"), ++i) {
            ObjectInfo info(foundObject);

            string key = info.key;
            ExcAssertNotEqual(key, marker);
            marker = key;

            ExcAssertEqual(info.key.find(foundPrefix), 0);
            // cerr << "info.key: " + info.key + "; foundPrefix: " +foundPrefix + "\n";
            string basename(info.key, foundPrefix.length());

            if (!onObject(foundPrefix, basename, info, depth)) {
                stop = true;
                break;
            }
        }

        if (stop) return;

        auto foundDir
            = XMLHandle(*listingResultXml)
            .FirstChildElement("ListBucketResult")
            .FirstChildElement("CommonPrefixes")
            .ToElement();

        for (; onSubdir && foundDir;
             foundDir = foundDir->NextSiblingElement("CommonPrefixes")) {
            string dirName = extract<string>(foundDir, "Prefix");

            // Strip off the delimiter
            if (dirName.rfind(delimiter) == dirName.size() - delimiter.size()) {
                dirName = string(dirName, 0, dirName.size() - delimiter.size());
                ExcAssertEqual(dirName.find(prefix), 0);
                dirName = string(dirName, prefix.size());
            }
            if (onSubdir(foundPrefix, dirName, depth)) {
                string newPrefix = foundPrefix + dirName + "/";
                //cerr << "newPrefix = " << newPrefix << endl;
                forEachObject(bucket, newPrefix, onObject, onSubdir, delimiter,
                              depth + 1);
            }
        }

        // firstIter = false;
        if (!isTruncated)
            break;
    } while (marker != "");

    //cerr << "done scanning" << endl;
}

void
S3Api::
forEachObject(const string & uriPrefix,
              const OnObjectUri & onObject,
              const OnSubdir & onSubdir,
              const string & delimiter,
              int depth,
              const string & startAt) const
{
    string bucket, objectPrefix;
    std::tie(bucket, objectPrefix) = parseUri(uriPrefix);

    auto onObject2 = [&] (const string & prefix,
                          const string & objectName,
                          const ObjectInfo & info,
                          int depth)
        {
            string uri = "s3://" + bucket + "/" + prefix;
            if (objectName.size() > 0) {
                uri += objectName;
            }
            return onObject(uri, info, depth);
        };

    forEachObject(bucket, objectPrefix, onObject2, onSubdir, delimiter, depth, startAt);
}

S3Api::ObjectInfo
S3Api::
getObjectInfo(const string & bucket, const string & object,
              S3ObjectInfoTypes infos)
    const
{
    return ((infos & int(S3ObjectInfoTypes::FULL_EXTRAS)) != 0
            ? getObjectInfoFull(bucket, object)
            : getObjectInfoShort(bucket, object));
}

S3Api::ObjectInfo
S3Api::
getObjectInfoFull(const string & bucket, const string & object)
    const
{
    RestParams queryParams;
    queryParams.push_back({"prefix", object});

    auto listingResult = getEscaped(bucket, "/", Range::Full, "", {}, queryParams);

    if (listingResult.code_ != 200) {
        cerr << listingResult.bodyXmlStr() << endl;
        throw MLDB::Exception("error getting object");
    }

    auto listingResultXml = listingResult.bodyXml();

    auto foundObject
        = tinyxml2::XMLHandle(*listingResultXml)
        .FirstChildElement("ListBucketResult")
        .FirstChildElement("Contents")
        .ToElement();

    if (!foundObject)
        throw MLDB::Exception("object " + object + " not found in bucket "
                            + bucket);

    ObjectInfo info(foundObject);

    if(info.key != object){
        throw MLDB::Exception("object " + object + " not found in bucket "
                            + bucket);
    }
    return info;
}

S3Api::ObjectInfo
S3Api::
getObjectInfoShort(const string & bucket, const string & object)
    const
{
    auto res = head(bucket, "/" + object);
    if (res.code_ == 404) {
        throw MLDB::Exception("object " + object + " not found in bucket "
                            + bucket);
    }
    if (res.code_ != 200) {
        throw MLDB::Exception("error getting object");
    }
    return ObjectInfo(res);
}

S3Api::ObjectInfo
S3Api::
tryGetObjectInfo(const string & bucket,
                 const string & object,
                 S3ObjectInfoTypes infos)
    const
{
    return ((infos & int(S3ObjectInfoTypes::FULL_EXTRAS)) != 0
            ? tryGetObjectInfoFull(bucket, object)
            : tryGetObjectInfoShort(bucket, object));
}

S3Api::ObjectInfo
S3Api::
tryGetObjectInfoFull(const string & bucket, const string & object)
    const
{
    RestParams queryParams;
    queryParams.push_back({"prefix", object});

    auto listingResult = get(bucket, "/", Range::Full, "", {}, queryParams);
    if (listingResult.code_ != 200) {
        cerr << listingResult.bodyXmlStr() << endl;
        throw MLDB::Exception("error getting object request: %d",
                            listingResult.code_);
    }
    auto listingResultXml = listingResult.bodyXml();

    auto foundObject
        = tinyxml2::XMLHandle(*listingResultXml)
        .FirstChildElement("ListBucketResult")
        .FirstChildElement("Contents")
        .ToElement();

    if (!foundObject)
        return ObjectInfo();

    ObjectInfo info(foundObject);

    if (info.key != object) {
        return ObjectInfo();
    }

    return info;
}

S3Api::ObjectInfo
S3Api::
tryGetObjectInfoShort(const string & bucket, const string & object)
    const
{
    auto res = head(bucket, "/" + object);
    if (res.code_ == 404) {
        return ObjectInfo();
    }
    if (res.code_ != 200) {
        throw MLDB::Exception("error getting object");
    }

    return ObjectInfo(res);
}

S3Api::ObjectInfo
S3Api::
getObjectInfo(const string & uri, S3ObjectInfoTypes infos)
    const
{
    string bucket, object;
    std::tie(bucket, object) = parseUri(uri);
    return getObjectInfo(bucket, object, infos);
}

S3Api::ObjectInfo
S3Api::
tryGetObjectInfo(const string & uri, S3ObjectInfoTypes infos)
    const
{
    string bucket, object;
    std::tie(bucket, object) = parseUri(uri);
    return tryGetObjectInfo(bucket, object, infos);
}

void
S3Api::
eraseObject(const string & bucket,
            const string & object)
{
    Response response = erase(bucket, object);

    if (response.code_ != 204) {
        cerr << response.bodyXmlStr() << endl;
        throw MLDB::Exception("error erasing object request: %d",
                            response.code_);
    }
}

bool
S3Api::
tryEraseObject(const string & bucket,
               const string & object)
{
    Response response = erase(bucket, object);

    if (response.code_ != 200) {
        return false;
    }

    return true;
}

void
S3Api::
eraseObject(const string & uri)
{
    string bucket, object;
    std::tie(bucket, object) = parseUri(uri);
    eraseObject(bucket, object);
}

bool
S3Api::
tryEraseObject(const string & uri)
{
    string bucket, object;
    std::tie(bucket, object) = parseUri(uri);
    return tryEraseObject(bucket, object);
}

string
S3Api::
getPublicUri(const string & uri,
             const string & protocol)
{
    string bucket, object;
    std::tie(bucket, object) = parseUri(uri);
    return getPublicUri(bucket, object, protocol);
}

string
S3Api::
getPublicUri(const string & bucket,
             const string & object,
             const string & protocol)
{
    return protocol + "://" + bucket + ".s3.amazonaws.com/" + object;
}

std::pair<string, string>
S3Api::
parseUri(const string & uri)
{
    if (uri.find("s3://") != 0)
        throw MLDB::Exception("wrong scheme (should start with s3://)");
    string pathPart(uri, 5);
    string::size_type pos = pathPart.find('/');
    if (pos == string::npos)
        throw MLDB::Exception("couldn't find bucket name");
    string bucket(pathPart, 0, pos);
    string object(pathPart, pos + 1);

    return make_pair(bucket, object);
}

bool
S3Api::
forEachBucket(const OnBucket & onBucket) const
{
    using namespace tinyxml2;

    //cerr << "forEachObject under " << prefix << endl;

    auto listingResult = get("", "/", Range::Full, "");
    auto listingResultXml = listingResult.bodyXml();

    auto foundBucket
        = XMLHandle(*listingResultXml)
        .FirstChildElement("ListAllMyBucketsResult")
        .FirstChildElement("Buckets")
        .FirstChildElement("Bucket")
        .ToElement();

    for (; onBucket && foundBucket;
         foundBucket = foundBucket->NextSiblingElement("Bucket")) {

        string foundName
            = extract<string>(foundBucket, "Name");
        if (!onBucket(foundName))
            return false;
    }

    return true;
}

void S3Api::setDefaultBandwidthToServiceMbps(double mbps){
    S3Api::defaultBandwidthToServiceMbps = mbps;
}

S3Api::Redundancy S3Api::defaultRedundancy = S3Api::REDUNDANCY_STANDARD;

void
S3Api::
setDefaultRedundancy(Redundancy redundancy)
{
    if (redundancy == REDUNDANCY_DEFAULT)
        throw MLDB::Exception("Can't set default redundancy as default");
    defaultRedundancy = redundancy;
}

S3Api::Redundancy
S3Api::
getDefaultRedundancy()
{
    return defaultRedundancy;
}

/** getEnv, but compatible with string.  Returns null string if not
    found.
*/
static string getEnv(const char * varName)
{
    const char * val = getenv(varName);
    return val ? val : "";
}

/** Provider of S3 credentials that are added explicitly for a subset of
    buckets.
*/

struct S3ExplicitCredentialProvider: public CredentialProvider {

    std::vector<string> buckets;
    StoredCredentials cred;

    S3ExplicitCredentialProvider()
    {
    }

    S3ExplicitCredentialProvider(string provider,
                                 string id,
                                 string secret,
                                 std::vector<string> buckets,
                                 double bandwidthToServiceMbps,
                                 const string & protocol,
                                 const string & serviceUri)
    {
        init(provider, id, secret, buckets, bandwidthToServiceMbps, protocol, serviceUri);
    }

    void init(string provider,
              string id,
              string secret,
              std::vector<string> buckets,
              double bandwidthToServiceMbps = S3Api::defaultBandwidthToServiceMbps,
              const string & protocol = "http",
              const string & serviceUri = "s3.amazonaws.com")
    {
        cred.resourceType = "aws:s3";
        cred.resource = "";
        cred.credential.provider = provider;
        cred.credential.protocol = protocol;
        cred.credential.location = serviceUri;
        cred.credential.id = id;
        cred.credential.secret = secret;
        cred.credential.extra["bandwithToServiceMbps"] = bandwidthToServiceMbps;
        this->buckets = buckets;
    }

    virtual std::vector<StoredCredentials>
    getCredentialsOfType(const string & resourceType) const
    {
        if (resourceType == "aws:s3")
            return { cred };
        return {};
    }
};

bool disableCloudCredentials = false;

/** Register S3 with the filter streams API so that a filter_stream can be
    used to treat an S3 object as a simple stream.
*/
struct RegisterS3CredProvider {
    /** Parse the ~/.cloud_credentials file and add those buckets in.

        The format of that file is as follows:
        1.  One entry per line
        2.  Tab separated
        3.  Comments are '#' in the first position
        4.  First entry is the name of the URI scheme (here, s3)
        5.  Second entry is the "version" of the configuration (here, 1)
            for forward compatibility
        6.  The rest of the entries depend upon the scheme; for s3 they are
            tab-separated and include the following:
            - Access key ID
            - Access key
            - Bandwidth from this machine to the server (MBPS)
            - Protocol (http)
            - S3 machine host name (s3.amazonaws.com)
    */
    struct CloudCredentialProvider: public CredentialProvider {

        CloudCredentialProvider()
        {
            // Parse cloud credentials file

            string filename = "";
            const char * home = getenv("HOME");
            if (home != NULL)
                filename = home + string("/.cloud_credentials");
            if (filename != "" && ML::fileExists(filename)) {
                std::ifstream stream(filename.c_str());
                int lineNum = 1;
                for (; stream;  ++lineNum) {
                    string line;

                    getline(stream, line);
                    if (line.empty() || line[0] == '#')
                        continue;
                    if (line.find("s3") != 0)
                        continue;

                    vector<string> fields = ML::split(line, '\t');

                    if (fields[0] != "s3")
                        continue;

                    if (fields.size() < 4) {
                        cerr << "warning: skipping invalid line in ~/.cloud_credentials at line "
                             << lineNum << endl;
                        continue;
                    }

                    fields.resize(7);

                    static constexpr auto V1 = "1";
                    string version = fields[1];
                    if (version != V1) {
                        cerr << "warning: ignoring unknown version at line "
                             << lineNum <<  " in ~/.cloud_credentials. "
                             << "Version " << V1 << " is supported." << endl;
                        continue;
                    }

                    string keyId = fields[2];
                    string key = fields[3];
                    string bandwidth = fields[4];
                    string protocol = fields[5];
                    string serviceUri = fields[6];

                    double bw = S3Api::defaultBandwidthToServiceMbps;
                    if (bandwidth != "")
                        bw = boost::lexical_cast<double>(bandwidth);
                    if (protocol == "")
                        protocol = "http";
                    if (serviceUri == "")
                        serviceUri = "s3.amazonaws.com";

                    Credential cred;
                    cred.provider = "S3CloudCredentials " + filename + ":" + std::to_string(lineNum);
                    cred.id = keyId;
                    cred.secret = key;
                    cred.protocol = protocol;
                    cred.location = serviceUri;
                    cred.extra["bandwidthToServiceMbps"] = bw;

                    StoredCredentials storedCred;
                    storedCred.resourceType = "aws:s3"; // only resource of that type is supported
                    storedCred.resource = ""; // credentials for all AWS S3
                    storedCred.credential = cred;
                    creds.push_back(storedCred);
                }
            }
        }

        std::vector<StoredCredentials> creds;

        virtual std::vector<StoredCredentials>
        getCredentialsOfType(const string & resourceType) const
        {
            if (disableCloudCredentials || resourceType != "aws:s3")
                return {};
            return creds;
        }
    };

    struct S3EnvironmentCredentialProvider: public S3ExplicitCredentialProvider {

        S3EnvironmentCredentialProvider()
        {
            init("S3EnvironmentCredentialProvider",
                 getEnv("S3_KEY_ID"),
                 getEnv("S3_KEY"),
                 ML::split(getEnv("S3_BUCKETS"), ','));
        }
    };

    RegisterS3CredProvider()
    {
        CredentialProvider::registerProvider
            (std::make_shared<CloudCredentialProvider>());
        if (getenv("S3_KEY_ID"))
            CredentialProvider::registerProvider
                (std::make_shared<S3EnvironmentCredentialProvider>());
    }

} registerS3CredProvider;

/** S3 support for filter_ostream opens.  Register the bucket name here, and
    you can open it directly from s3.
*/

void registerS3Bucket(const string & bucketName,
                      const string & accessKeyId,
                      const string & accessKey,
                      double bandwidthToServiceMbps,
                      const string & protocol,
                      const string & serviceUri)
{
    CredentialProvider::registerProvider
        (std::make_shared<S3ExplicitCredentialProvider>
         ("registerS3Bucket()", accessKeyId, accessKey,
          vector<string>({ bucketName }),
          bandwidthToServiceMbps, protocol, serviceUri));
}

void registerS3Buckets(const string & accessKeyId,
                       const string & accessKey,
                       double bandwidthToServiceMbps,
                       const string & protocol,
                       const string & serviceUri)
{
    auto api = std::make_shared<S3Api>(accessKeyId, accessKey,
                                       bandwidthToServiceMbps,
                                       protocol, serviceUri);

    vector<string> bucketNames;

    auto onBucket = [&] (const string & bucketName)
        {
            bucketNames.push_back(bucketName);
            return true;
        };

    api->forEachBucket(onBucket);

    if (bucketNames.empty()) {
        cerr << "warning: no bucket names registered";
    } else {
        CredentialProvider::registerProvider
            (std::make_shared<S3ExplicitCredentialProvider>
             ("registerS3Buckets()", accessKeyId, accessKey,
              bucketNames, bandwidthToServiceMbps, protocol, serviceUri));
    }
}

/** Extract the bandwidth to the service from a credentials object. */
static double getBandwidth(const Credential & cred)
{
    if (cred.extra.isMember("bandwidthToServiceMbps"))
        return cred.extra["bandwidthToServiceMbps"].asDouble();
    else return S3Api::defaultBandwidthToServiceMbps;
}

std::shared_ptr<S3Api> getS3ApiForUri(const string & uri)
{
    // Get the credentials
    auto creds = getCredential("aws:s3", uri);
    return std::make_shared<S3Api>(creds.id, creds.secret, getBandwidth(creds),
                                   creds.protocol, creds.location);
}

} // namespace MLDB
