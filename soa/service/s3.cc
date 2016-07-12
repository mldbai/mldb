// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** s3.cc
    Jeremy Barnes, 3 July 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Code to talk to s3.
*/

#include <atomic>
#include "mldb/soa/service/s3.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/types/date.h"
#include "mldb/types/url.h"
#include "mldb/soa/utils/print_utils.h"
#include "mldb/types/structure_description.h"
#include "mldb/arch/futex.h"
#include "mldb/arch/threads.h"
#include "mldb/base/exc_assert.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/jml/utils/ring_buffer.h"
#include "mldb/base/hash.h"
#include "mldb/jml/utils/file_functions.h"
#include "mldb/jml/utils/info.h"
#include "mldb/jml/utils/environment.h"
#include "xml_helpers.h"
#include "mldb/soa/credentials/credentials.h"
#include "mldb/soa/credentials/credential_provider.h"
#include "mldb/http/curl_wrapper.h"
#include "mldb/http/http_rest_proxy_impl.h"

#include <exception>
#include <thread>
#include <chrono>
#include <unordered_map>
#include <boost/filesystem.hpp>

#include "mldb/vfs/fs_utils.h"


using namespace std;
using namespace ML;
using namespace Datacratic;


namespace Datacratic {

DEFINE_STRUCTURE_DESCRIPTION(S3Config);


S3ConfigDescription::
S3ConfigDescription()
{
    addField("accessKeyId", &S3Config::accessKeyId, "");
    addField("accessKey", &S3Config::accessKey, "");
}

std::string
S3Api::
s3EscapeResource(const std::string & str)
{
    if (str.size() == 0) {
        throw ML::Exception("empty str name");
    }

    if (str[0] != '/') {
        throw ML::Exception("resource name must start with a '/'");
    }

    std::string result;
    for (auto c: str) {

        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~' || c == '/')
            result += c;
        else result += ML::format("%%%02X", c);
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
S3Api(const std::string & accessKeyId,
      const std::string & accessKey,
      double bandwidthToServiceMbps,
      const std::string & defaultProtocol,
      const std::string & serviceUri)
    : accessKeyId(accessKeyId),
      accessKey(accessKey),
      defaultProtocol(defaultProtocol),
      serviceUri(serviceUri),
      bandwidthToServiceMbps(bandwidthToServiceMbps)
{
}

void
S3Api::
init(const std::string & accessKeyId,
     const std::string & accessKey,
     double bandwidthToServiceMbps,
     const std::string & defaultProtocol,
     const std::string & serviceUri)
{
    this->accessKeyId = accessKeyId;
    this->accessKey = accessKey;
    this->defaultProtocol = defaultProtocol;
    this->serviceUri = serviceUri;
    this->bandwidthToServiceMbps = bandwidthToServiceMbps;
}

ML::Env_Option<int> S3_DEBUG("S3_DEBUG", 0);

S3Api::Response
S3Api::SignedRequest::
performSync() const
{
    static const int baseRetryDelay(3);
    static int numRetries(-1);

    if (numRetries == -1) {
        char * numRetriesEnv = getenv("S3_RETRIES");
        if (numRetriesEnv) {
            numRetries = atoi(numRetriesEnv);
        }
        else {
            numRetries = 45;
        }
    }

    size_t spacePos = uri.find(" ");
    if (spacePos != string::npos) {
        throw ML::Exception("url '" + uri + "' contains an unescaped space"
                            " at position " + to_string(spacePos));
    }

    Range currentRange = params.downloadRange;

    /* The "Range" header is only useful with GET and when the range is
       explicitly specified. The use of Range::Full means that we always
       request the full body, even during retries. This is mainly useful for
       requests on non-object urls, where that header is ignored by the S3
       servers. */
    bool useRange = (params.verb == "GET" && currentRange != Range::Full);

    string body;

    for (int i = 0; i < numRetries; ++i) {
        if (i > 0) {
            /* allow a maximum of 384 seconds for retry delays (1 << 7 * 3) */
            int multiplier = i < 8 ? (1 << i) : i << 7;
            double numSeconds = ::random() % (baseRetryDelay * multiplier);
            if (numSeconds == 0) {
                numSeconds = baseRetryDelay * multiplier;
            }

            numSeconds = 0.05;

            ::fprintf(stderr,
                      "S3 operation retry in %f seconds: %s %s\n",
                      numSeconds, params.verb.c_str(), uri.c_str());
            std::this_thread::sleep_for(std::chrono::milliseconds(int(numSeconds * 1000)));
        }

        string responseHeaders;
        string responseBody;
        long int responseCode(0);
        size_t received(0);

        auto connection = owner->proxy.getConnection();
        CurlWrapper::Easy & myRequest = (CurlWrapper::Easy &)(*connection);
        myRequest.reset();


        RestParams headers = params.headers;
        headers.emplace_back(make_pair("Date", params.date));
        headers.emplace_back(make_pair("Authorization", auth));

        if (useRange) {
            uint64_t end = currentRange.endPos();
            string range = ML::format("bytes=%zd-%zd",
                                      currentRange.offset, end);
            // ::fprintf(stderr, "%p: requesting %s\n", this, range.c_str());
            headers.emplace_back(make_pair("range", range));
        }

        // cerr << "getting " << uri << " " << params.headers << endl;

        double expectedTimeSeconds
            = (currentRange.size / 1000000.0) / bandwidthToServiceMbps;
        int timeout = 15 + std::max<int>(30, expectedTimeSeconds * 6);

#if 0
        cerr << "expectedTimeSeconds = " << expectedTimeSeconds << endl;
        cerr << "timeout = " << timeout << endl;
#endif

        //cerr << "!!!Setting params verb " << params.verb << endl;
        myRequest.add_option(CURLOPT_CUSTOMREQUEST, params.verb);

        myRequest.add_option(CURLOPT_URL, uri);
        myRequest.add_option(CURLOPT_TIMEOUT, timeout);
        myRequest.add_option(CURLOPT_NOSIGNAL, 1L);

        if (S3_DEBUG)
            myRequest.add_option(CURLOPT_VERBOSE, 1L);

        bool noBody = (params.verb == "HEAD");
        if (noBody) {
            myRequest.add_option(CURLOPT_NOBODY, noBody);
        }

        CurlWrapper::Easy::CurlCallback onWriteData
            = [&] (char * data, size_t ofs1, size_t ofs2) {
            size_t total = ofs1 * ofs2;
            received += total;
            responseBody.append(data, total);
            return total;
        };

        bool afterContinue = false;

        CurlWrapper::Easy::CurlCallback onHeader
            = [&] (char * data, size_t ofs1, size_t ofs2) {
            string headerLine(data, ofs1 * ofs2);

            if (headerLine.find("HTTP/1.1 100 Continue") == 0) {
                afterContinue = true;
            }
            else if (afterContinue) {
                if (headerLine == "\r\n")
                    afterContinue = false;
            }
            else {
                responseHeaders.append(headerLine);
                //cerr << "got header data " << headerLine << endl;
            }
            return ofs1 * ofs2;
        };

        myRequest.add_callback_option(CURLOPT_HEADERFUNCTION, CURLOPT_HEADERDATA, onHeader);
        myRequest.add_callback_option(CURLOPT_WRITEFUNCTION, CURLOPT_WRITEDATA, onWriteData);

        const string & s = params.content.body();

        if (!noBody) {
            myRequest.add_option(CURLOPT_POSTFIELDS, s);
            myRequest.add_option(CURLOPT_POSTFIELDSIZE, s.size());
            headers.emplace_back(make_pair("Content-Length",
                                           ML::format("%lld", s.size())));

            // This is needed for S3 to properly understand the request
            headers.emplace_back(make_pair("Content-Type", ""));
        }

        myRequest.add_header_option(headers);

        CURLcode res = myRequest.perform();

        if (res != CURLE_OK) {
            string message("S3 operation failed with a libCurl error: "
                           + string(curl_easy_strerror(res))
                           + " (" + to_string(res) + ")\n"
                           + params.verb + " " + uri + "\n");
            if (responseHeaders.size() > 0) {
                message += "headers:\n" + responseHeaders;
            }
            ::fprintf(stderr, "%s\n", message.c_str());

            if (useRange && received > 0) {
                body.append(responseBody);
                currentRange.adjust(received);
            }
            continue;
        }

        myRequest.get_info(CURLINFO_RESPONSE_CODE, responseCode);

        /* Detect so-called "REST error"
           (http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html)

           Some S3 methods may return an XML error AND still have a 200 HTTP
           status code:
           http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
           Explanation of the why:
           https://github.com/aws/aws-sdk-go/issues/501.
        */
        pair<string, string> xmlError; /* {code, message} */
        if (!(responseCode == 200
              && (params.verb == "GET" || params.verb == "HEAD"))
            && (responseHeaders.find("Content-Type: application/xml")
                != string::npos)) {
            unique_ptr<tinyxml2::XMLDocument> localXml(
                new tinyxml2::XMLDocument()
                );
            localXml->Parse(responseBody.c_str());
            auto element
                = tinyxml2::XMLHandle(*localXml).FirstChildElement("Error")
                .ToElement();
            if (element) {
                xmlError.first = extract<string>(element, "Code");
                xmlError.second = extract<string>(element, "Message");
            }
        }

        auto makeErrorMsg = [&] () {
            string message("S3 operation failed with HTTP code "
                           + to_string(responseCode) + "\n"
                           + params.verb + " " + uri + "\n");
            if (responseHeaders.size() > 0) {
                message += "headers:\n" + responseHeaders;
            }
            if (responseBody.size() > 0) {
                message += (string("body (") + to_string(responseBody.size())
                            + " bytes):\n" + responseBody + "\n");
            }
            if (!xmlError.first.empty()) {
                message += ("S3 REST error code: "
                            + xmlError.first
                            + "; message: "
                            + xmlError.second
                            +"\n");
            }

            return message;
        };

        if (!xmlError.first.empty()
            || (responseCode >= 300 && responseCode != 404)) {
            string message(makeErrorMsg());
            ::fprintf(stderr, "%s\n", message.c_str());

            /* retry on 50X range errors (recoverable) */
            if ((responseCode >= 500 && responseCode < 505)
                || xmlError.first == "InternalError") {
                continue;
            }
            else {
                string firstLine(responseHeaders, 0, responseHeaders.find('\n'));

                throw ML::Exception("S3 error loading '%s': %s",
                                    uri.c_str(), firstLine.c_str());
            }
        }

        Response response;
        response.code_ = responseCode;
        response.header_.parse(responseHeaders, !noBody);
        body.append(responseBody);
        response.body_ = body;

        return response;
    }

    throw ML::Exception("too many retries");
}

std::string
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

S3Api::SignedRequest
S3Api::
prepare(const RequestParams & request) const
{
    string protocol = defaultProtocol;
    if(protocol.length() == 0){
        throw ML::Exception("attempt to perform s3 request without a "
            "default protocol. (Could be caused by S3Api initialisation with "
            "the empty constructor.)");
    }

    SignedRequest result;
    result.params = request;
    result.bandwidthToServiceMbps = bandwidthToServiceMbps;
    result.owner = const_cast<S3Api *>(this);

    if (request.resource.find("//") != string::npos)
        throw ML::Exception("attempt to perform s3 request with double slash: "
                            + request.resource);

    if (request.bucket.empty()) {
        result.uri = protocol + "://" + serviceUri
            + request.resource
            + (request.subResource != "" ? "?" + request.subResource : "");
    }
    else {
        result.uri = protocol + "://" + request.bucket + "." + serviceUri
            + request.resource
            + (request.subResource != "" ? "?" + request.subResource : "");
    }

    for (unsigned i = 0;  i < request.queryParams.size();  ++i) {
        if (i == 0 && request.subResource == "")
            result.uri += "?";
        else result.uri += "&";
        result.uri += uriEncode(request.queryParams[i].first)
            + "=" + uriEncode(request.queryParams[i].second);
    }

    string sig = signature(request);
    result.auth = "AWS " + accessKeyId + ":" + sig;

    //cerr << "result.uri = " << result.uri << endl;
    //cerr << "result.auth = " << result.auth << endl;

    return result;
}

S3Api::Response
S3Api::
headEscaped(const std::string & bucket,
            const std::string & resource,
            const std::string & subResource,
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

    return prepare(request).performSync();
}

S3Api::Response
S3Api::
getEscaped(const std::string & bucket,
           const std::string & resource,
           const Range & downloadRange,
           const std::string & subResource,
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

    return prepare(request).performSync();
}

/** Perform a POST request from end to end. */
S3Api::Response
S3Api::
postEscaped(const std::string & bucket,
            const std::string & resource,
            const std::string & subResource,
            const RestParams & headers,
            const RestParams & queryParams,
            const HttpRequestContent & content) const
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

    return prepare(request).performSync();
}

S3Api::Response
S3Api::
putEscaped(const std::string & bucket,
           const std::string & resource,
           const std::string & subResource,
           const RestParams & headers,
           const RestParams & queryParams,
           const HttpRequestContent & content) const
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

    return prepare(request).performSync();
}

S3Api::Response
S3Api::
eraseEscaped(const std::string & bucket,
             const std::string & resource,
             const std::string & subResource,
             const RestParams & headers,
             const RestParams & queryParams,
             const HttpRequestContent & content) const
{
    RequestParams request;
    request.verb = "DELETE";
    request.bucket = bucket;
    request.resource = resource;
    request.subResource = subResource;
    request.headers = headers;
    request.queryParams = queryParams;
    request.date = Date::now().printRfc2616();
    request.content = content;

    return prepare(request).performSync();
}

RestParams
S3Api::ObjectMetadata::
getRequestHeaders() const
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

pair<bool,string>
S3Api::isMultiPartUploadInProgress(
    const std::string & bucket,
    const std::string & resource) const
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
obtainMultiPartUpload(const std::string & bucket,
                      const std::string & resource,
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

std::string
S3Api::
finishMultiPartUpload(const std::string & bucket,
                      const std::string & resource,
                      const std::string & uploadId,
                      const std::vector<std::string> & etags) const
{
    using namespace tinyxml2;
    // Finally, send back a response to join the parts together
    ExcAssert(etags.size());

    XMLDocument joinRequest;
    auto r = joinRequest.InsertFirstChild(joinRequest.NewElement("CompleteMultipartUpload"));
    for (unsigned i = 0;  i < etags.size();  ++i) {
        auto n = r->InsertEndChild(joinRequest.NewElement("Part"));
        n->InsertEndChild(joinRequest.NewElement("PartNumber"))
            ->InsertEndChild(joinRequest.NewText(ML::format("%d", i + 1).c_str()));
        n->InsertEndChild(joinRequest.NewElement("ETag"))
            ->InsertEndChild(joinRequest.NewText(etags[i].c_str()));
    }

    string escapedResource = s3EscapeResource(resource);

    HttpRequestContent xmlReq(xmlDocumentAsString(joinRequest),
                              "application/xml");
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
S3Api::MultiPartUploadPart::
fromXml(tinyxml2::XMLElement * element)
{
    partNumber = extract<int>(element, "PartNumber");
    lastModified = extract<string>(element, "LastModified");
    etag = extract<string>(element, "ETag");
    size = extract<uint64_t>(element, "Size");
    done = true;
}

S3Api::ObjectInfo::
ObjectInfo(tinyxml2::XMLNode * element)
{
    size = extract<uint64_t>(element, "Size");
    key  = extract<string>(element, "Key");
    string lastModifiedStr = extract<string>(element, "LastModified");
    lastModified = Date::parseIso8601DateTime(lastModifiedStr);
    etag = extract<string>(element, "ETag");
    ownerId = extract<string>(element, "Owner/ID");
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


void
S3Api::
forEachObject(const std::string & bucket,
              const std::string & prefix,
              const OnObject & onObject,
              const OnSubdir & onSubdir,
              const std::string & delimiter,
              int depth,
              const std::string & startAt) const
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
forEachObject(const std::string & uriPrefix,
              const OnObjectUri & onObject,
              const OnSubdir & onSubdir,
              const std::string & delimiter,
              int depth,
              const std::string & startAt) const
{
    string bucket, objectPrefix;
    std::tie(bucket, objectPrefix) = parseUri(uriPrefix);

    auto onObject2 = [&] (const std::string & prefix,
                          const std::string & objectName,
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
getObjectInfo(const std::string & bucket, const std::string & object,
              S3ObjectInfoTypes infos)
    const
{
    return ((infos & int(S3ObjectInfoTypes::FULL_EXTRAS)) != 0
            ? getObjectInfoFull(bucket, object)
            : getObjectInfoShort(bucket, object));
}

S3Api::ObjectInfo
S3Api::
getObjectInfoFull(const std::string & bucket, const std::string & object)
    const
{
    RestParams queryParams;
    queryParams.push_back({"prefix", object});

    auto listingResult = getEscaped(bucket, "/", Range::Full, "", {}, queryParams);

    if (listingResult.code_ != 200) {
        cerr << listingResult.bodyXmlStr() << endl;
        throw ML::Exception("error getting object");
    }

    auto listingResultXml = listingResult.bodyXml();

    auto foundObject
        = tinyxml2::XMLHandle(*listingResultXml)
        .FirstChildElement("ListBucketResult")
        .FirstChildElement("Contents")
        .ToElement();

    if (!foundObject)
        throw ML::Exception("object " + object + " not found in bucket "
                            + bucket);

    ObjectInfo info(foundObject);

    if(info.key != object){
        throw ML::Exception("object " + object + " not found in bucket "
                            + bucket);
    }
    return info;
}

S3Api::ObjectInfo
S3Api::
getObjectInfoShort(const std::string & bucket, const std::string & object)
    const
{
    auto res = head(bucket, "/" + object);
    if (res.code_ == 404) {
        throw ML::Exception("object " + object + " not found in bucket "
                            + bucket);
    }
    if (res.code_ != 200) {
        throw ML::Exception("error getting object");
    }
    return ObjectInfo(res);
}

S3Api::ObjectInfo
S3Api::
tryGetObjectInfo(const std::string & bucket,
                 const std::string & object,
                 S3ObjectInfoTypes infos)
    const
{
    return ((infos & int(S3ObjectInfoTypes::FULL_EXTRAS)) != 0
            ? tryGetObjectInfoFull(bucket, object)
            : tryGetObjectInfoShort(bucket, object));
}

S3Api::ObjectInfo
S3Api::
tryGetObjectInfoFull(const std::string & bucket, const std::string & object)
    const
{
    RestParams queryParams;
    queryParams.push_back({"prefix", object});

    auto listingResult = get(bucket, "/", Range::Full, "", {}, queryParams);
    if (listingResult.code_ != 200) {
        cerr << listingResult.bodyXmlStr() << endl;
        throw ML::Exception("error getting object request: %d",
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
tryGetObjectInfoShort(const std::string & bucket, const std::string & object)
    const
{
    auto res = head(bucket, "/" + object);
    if (res.code_ == 404) {
        return ObjectInfo();
    }
    if (res.code_ != 200) {
        throw ML::Exception("error getting object");
    }

    return ObjectInfo(res);
}

S3Api::ObjectInfo
S3Api::
getObjectInfo(const std::string & uri, S3ObjectInfoTypes infos)
    const
{
    string bucket, object;
    std::tie(bucket, object) = parseUri(uri);
    return getObjectInfo(bucket, object, infos);
}

S3Api::ObjectInfo
S3Api::
tryGetObjectInfo(const std::string & uri, S3ObjectInfoTypes infos)
    const
{
    string bucket, object;
    std::tie(bucket, object) = parseUri(uri);
    return tryGetObjectInfo(bucket, object, infos);
}

void
S3Api::
eraseObject(const std::string & bucket,
            const std::string & object)
{
    Response response = erase(bucket, object);

    if (response.code_ != 204) {
        cerr << response.bodyXmlStr() << endl;
        throw ML::Exception("error erasing object request: %d",
                            response.code_);
    }
}

bool
S3Api::
tryEraseObject(const std::string & bucket,
               const std::string & object)
{
    Response response = erase(bucket, object);

    if (response.code_ != 200) {
        return false;
    }

    return true;
}

void
S3Api::
eraseObject(const std::string & uri)
{
    string bucket, object;
    std::tie(bucket, object) = parseUri(uri);
    eraseObject(bucket, object);
}

bool
S3Api::
tryEraseObject(const std::string & uri)
{
    string bucket, object;
    std::tie(bucket, object) = parseUri(uri);
    return tryEraseObject(bucket, object);
}

std::string
S3Api::
getPublicUri(const std::string & uri,
             const std::string & protocol)
{
    string bucket, object;
    std::tie(bucket, object) = parseUri(uri);
    return getPublicUri(bucket, object, protocol);
}

std::string
S3Api::
getPublicUri(const std::string & bucket,
             const std::string & object,
             const std::string & protocol)
{
    return protocol + "://" + bucket + ".s3.amazonaws.com/" + object;
}

std::pair<std::string, std::string>
S3Api::
parseUri(const std::string & uri)
{
    if (uri.find("s3://") != 0)
        throw ML::Exception("wrong scheme (should start with s3://)");
    string pathPart(uri, 5);
    string::size_type pos = pathPart.find('/');
    if (pos == string::npos)
        throw ML::Exception("couldn't find bucket name");
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

HttpRestProxy S3Api::proxy;

S3Api::Redundancy S3Api::defaultRedundancy = S3Api::REDUNDANCY_STANDARD;

void
S3Api::
setDefaultRedundancy(Redundancy redundancy)
{
    if (redundancy == REDUNDANCY_DEFAULT)
        throw ML::Exception("Can't set default redundancy as default");
    defaultRedundancy = redundancy;
}

S3Api::Redundancy
S3Api::
getDefaultRedundancy()
{
    return defaultRedundancy;
}

/** getEnv, but compatible with std::string.  Returns null string if not
    found.
*/
static std::string getEnv(const char * varName)
{
    const char * val = getenv(varName);
    return val ? val : "";
}

/** Provider of S3 credentials that are added explicitly for a subset of
    buckets.
*/

struct S3ExplicitCredentialProvider: public CredentialProvider {

    std::vector<std::string> buckets;
    StoredCredentials cred;

    S3ExplicitCredentialProvider()
    {
    }

    S3ExplicitCredentialProvider(std::string provider,
                                 std::string id,
                                 std::string secret,
                                 std::vector<std::string> buckets,
                                 double bandwidthToServiceMbps,
                                 const std::string & protocol,
                                 const std::string & serviceUri)
    {
        init(provider, id, secret, buckets, bandwidthToServiceMbps, protocol, serviceUri);
    }

    void init(std::string provider,
              std::string id,
              std::string secret,
              std::vector<std::string> buckets,
              double bandwidthToServiceMbps = S3Api::defaultBandwidthToServiceMbps,
              const std::string & protocol = "http",
              const std::string & serviceUri = "s3.amazonaws.com")
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
    getCredentialsOfType(const std::string & resourceType) const
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
                        cerr << "warning: skipping invalid line in ~/.cloud_credentials: "
                             << line << endl;
                        continue;
                    }

                    fields.resize(7);

                    string version = fields[1];
                    if (version != "1") {
                        cerr << "warning: ignoring unknown version "
                             << version <<  " in ~/.cloud_credentials: "
                             << line << endl;
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
        getCredentialsOfType(const std::string & resourceType) const
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

void registerS3Bucket(const std::string & bucketName,
                      const std::string & accessKeyId,
                      const std::string & accessKey,
                      double bandwidthToServiceMbps,
                      const std::string & protocol,
                      const std::string & serviceUri)
{
    CredentialProvider::registerProvider
        (std::make_shared<S3ExplicitCredentialProvider>
         ("registerS3Bucket()", accessKeyId, accessKey,
          vector<string>({ bucketName }),
          bandwidthToServiceMbps, protocol, serviceUri));
}

void registerS3Buckets(const std::string & accessKeyId,
                       const std::string & accessKey,
                       double bandwidthToServiceMbps,
                       const std::string & protocol,
                       const std::string & serviceUri)
{
    auto api = std::make_shared<S3Api>(accessKeyId, accessKey,
                                       bandwidthToServiceMbps,
                                       protocol, serviceUri);

    vector<string> bucketNames;

    auto onBucket = [&] (const std::string & bucketName)
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

std::shared_ptr<S3Api> getS3ApiForUri(const std::string & uri)
{
    // Get the credentials
    auto creds = getCredential("aws:s3", uri);
    return std::make_shared<S3Api>(creds.id, creds.secret, getBandwidth(creds),
                                   creds.protocol, creds.location);
}

} // namespace Datacratic
