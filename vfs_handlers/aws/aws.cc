// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* aws.cc
   Jeremy Barnes, 8 August 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

*/

#include "aws.h"
#include "mldb/arch/format.h"
#include "mldb/utils/string_functions.h"
#include <iostream>
#include <algorithm> 
#include <cctype>
#include <locale>
#include "xml_helpers.h"
#include "mldb/utils/trim.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-function"
#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include "crypto++/sha.h"
#include "crypto++/md5.h"
#include "crypto++/hmac.h"
#include "crypto++/base64.h"
#include "crypto++/hex.h"
#pragma GCC diagnostic pop

using std::string;
using std::map;
using std::cout;
using std::cerr;
using std::endl;
using CryptoPP::byte;

namespace MLDB {

// Fix GCC error message about unused function
auto __fixGccError = &CryptoPP::StringNarrow;

template<class Hash>
std::string
AwsApi::
hmacDigest(const std::string & stringToSign,
           const std::string & accessKey)
{
    constexpr size_t digestLen = Hash::DIGESTSIZE;
    byte digest[digestLen];
    CryptoPP::HMAC<Hash> hmac((byte *)accessKey.c_str(), accessKey.length());
    hmac.CalculateDigest(digest,
                         (byte *)stringToSign.c_str(),
                         stringToSign.length());

    return std::string((const char *)digest,
                       digestLen);
}

std::string
AwsApi::
hmacSha1Digest(const std::string & stringToSign,
               const std::string & accessKey)
{
    return hmacDigest<CryptoPP::SHA1>(stringToSign, accessKey);
}

std::string
AwsApi::
hmacSha256Digest(const std::string & stringToSign,
                 const std::string & accessKey)
{
    return hmacDigest<CryptoPP::SHA256>(stringToSign, accessKey);
}

std::string
AwsApi::
sha256Digest(const std::string & stringToSign)
{
    typedef CryptoPP::SHA256 Hash;
    constexpr size_t digestLen = Hash::DIGESTSIZE;
    byte digest[digestLen];
    Hash h;
    h.CalculateDigest(digest,
                      (byte *)stringToSign.c_str(),
                      stringToSign.length());
    
    return std::string((const char *)digest,
                       digestLen);
}

template<typename Encoder>
std::string
AwsApi::
encodeDigest(const std::string & digest)
{
    char outBuf[256];

    Encoder encoder;
    encoder.Put((byte *)digest.c_str(), digest.size());
    encoder.MessageEnd();
    size_t got = encoder.Get((::byte *)outBuf, 256);
    outBuf[got] = 0;

    //cerr << "signing " << digest.size() << " characters" << endl;
    //cerr << "last character is " << (int)outBuf[got - 1] << endl;
    //cerr << "got " << got << " characters" << endl;

    std::string result(outBuf, outBuf + got);
    trim(result);
    return result;
}

std::string
AwsApi::
base64EncodeDigest(const std::string & digest)
{
    return encodeDigest<CryptoPP::Base64Encoder>(digest);
}

std::string
AwsApi::
hexEncodeDigest(const std::string & digest)
{
    return MLDB::lowercase(encodeDigest<CryptoPP::HexEncoder>(digest));
}

std::string
AwsApi::
getStringToSignV2Multi(const std::string & verb,
                       const Utf8String & bucket,
                       const Utf8String & resource,
                       const Utf8String & subResource,
                       const std::string & contentType,
                       const std::string & contentMd5,
                       const std::string & date,
                       const std::vector<std::pair<std::string, std::string> > & headers)
{
    std::map<std::string, std::string> canonHeaders;
    for (auto it = headers.begin(), end = headers.end();
         it != end;  ++it) {
        std::string key = MLDB::lowercase(it->first);
        if (key.find("x-amz") != 0) continue;

        std::string value = it->second;
        if (canonHeaders.count(key))
            canonHeaders[key] += ",";
        canonHeaders[key] += value;
    }
    
    return getStringToSignV2(verb, bucket, resource, subResource,
                             contentType, contentMd5, date, canonHeaders);
}

/*
Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature;

Signature = Base64( HMAC-SHA1( YourSecretAccessKeyID, UTF-8-Encoding-Of( StringToSign ) ) );

StringToSign = HTTP-Verb + "\n" +
    Content-MD5 + "\n" +
    Content-Type + "\n" +
    Date + "\n" +
    CanonicalizedAmzHeaders +
    CanonicalizedResource;

CanonicalizedResource = [ "/" + Bucket ] +
    <HTTP-Request-URI, from the protocol name up to the query string> +
    [ sub-resource, if present. For example "?acl", "?location", "?logging", or "?torrent"];

CanonicalizedAmzHeaders = <described below>
To construct the CanonicalizedAmzHeaders part of StringToSign, select all HTTP request headers that start with 'x-amz-' (using a case-insensitive comparison) and use the following process.

CanonicalizedAmzHeaders Process
1	Convert each HTTP header name to lower-case. For example, 'X-Amz-Date' becomes 'x-amz-date'.
2	Sort the collection of headers lexicographically by header name.
3	Combine header fields with the same name into one "header-name:comma-separated-value-list" pair as prescribed by RFC 2616, section 4.2, without any white-space between values. For example, the two metadata headers 'x-amz-meta-username: fred' and 'x-amz-meta-username: barney' would be combined into the single header 'x-amz-meta-username: fred,barney'.
4	"Unfold" long headers that span multiple lines (as allowed by RFC 2616, section 4.2) by replacing the folding white-space (including new-line) by a single space.
5	Trim any white-space around the colon in the header. For example, the header 'x-amz-meta-username: fred,barney' would become 'x-amz-meta-username:fred,barney'
6	Finally, append a new-line (U+000A) to each canonicalized header in the resulting list. Construct the CanonicalizedResource element by concatenating all headers in this list into a single string.


*/
std::string
AwsApi::
getStringToSignV2(const std::string & verb,
                  const Utf8String & bucket,
                  const Utf8String & resource,
                  const Utf8String & subResource,
                  const std::string & contentType,
                  const std::string & contentMd5,
                  const std::string & date,
                  const std::map<std::string, std::string> & headers)
{
    std::string canonHeaderString;

    for (auto it = headers.begin(), end = headers.end();
         it != end;  ++it) {
        std::string key = MLDB::lowercase(it->first);
        if (key.find("x-amz") != 0) continue;

        std::string value = it->second;

        canonHeaderString += key + ":" + value + "\n";
    }

    //cerr << "bucket = " << bucket << " resource = " << resource << endl;

    std::string canonResource
        = (bucket == "" ? "" : "/" + uriEncode(bucket))
        + uriEncode(resource)
        + (subResource.empty() ? "" : "?")
        + uriEncode(subResource);

    std::string stringToSign
        = verb + "\n"
        + contentMd5 + "\n"
        + contentType + "\n"
        + date + "\n"
        + canonHeaderString
        + canonResource;

    return stringToSign;
}

std::string
AwsApi::
signV2(const std::string & stringToSign,
       const std::string & accessKey)
{
    return base64EncodeDigest(hmacSha1Digest(stringToSign, accessKey));
}

std::string
AwsApi::
uriEncode(const std::string & str)
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
AwsApi::
uriEncode(const Utf8String & str)
{
    std::string result;
    for (unsigned c: str) {

        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~')
            result += c;
        else result += MLDB::format("%%%02X", c);
    }

    return result;
}

std::string
AwsApi::
escapeResource(const Utf8String & resource)
{
    if (resource.empty()) {
        throw MLDB::Exception("empty resource name");
    }

    if (!resource.startsWith("/")) {
        throw MLDB::Exception("resource name must start with a '/'");
    }

    return "/" + uriEncode(Utf8String(std::next(resource.begin()), resource.end()));
}

std::string
AwsApi::
signingKeyV4(const std::string & accessKey,
             const std::string & date,
             const std::string & region,
             const std::string & service,
             const std::string & signing)
{
    auto hmac = [&] (const std::string & key, const std::string & data)
        {
            return hmacSha256Digest(data, key);
        };
    
    std::string signingKey
        = hmac(hmac(hmac(hmac("AWS4" + accessKey,
                              date),
                         region),
                    service),
               signing);
    return signingKey;
}

std::string
AwsApi::
signV4(const std::string & stringToSign,
       const std::string & accessKey,
       const std::string & date,
       const std::string & region,
       const std::string & service,
       const std::string & signing)
{
    
    std::string signingKey = signingKeyV4(accessKey, date, region, service, signing);
    //cerr << "signingKey " << hexEncodeDigest(signingKey) << endl;
    return hexEncodeDigest(hmacSha256Digest(stringToSign, signingKey));
}

void
AwsApi::
addSignatureV4(BasicRequest & request,
               std::string service,
               std::string region,
               std::string accessKeyId,
               std::string accessKey,
               Date now)
{
    std::string dateStr = now.print("%Y%m%dT%H%M%SZ");

    //cerr << "dateStr = " << dateStr << endl;

    request.headers.push_back({"X-Amz-Date", dateStr});

    std::string canonicalHeaders;
    std::string signedHeaders;

    if (!request.headers.empty()) {
        RestParams headers = request.headers;
        for (auto & h: headers) {
            h.first = lowercase(h.first);
            trim(h.second);
        }
        std::sort(headers.begin(), headers.end());
        
        for (auto h: headers) {
            canonicalHeaders += h.first.rawString() + ":" + h.second.rawString() + "\n";
            signedHeaders += h.first.rawString() + ";";
        }

        signedHeaders.erase(signedHeaders.size() - 1);
    }

    std::string canonicalQueryParams;

    if (!request.queryParams.empty()) {
        RestParams queryParams = request.queryParams;
        std::sort(queryParams.begin(), queryParams.end());
        
        for (auto h: queryParams)
            canonicalQueryParams += uriEncode(h.first) + "=" + uriEncode(h.second) + "&";

        canonicalQueryParams.erase(canonicalQueryParams.size() - 1);
    }

    //cerr << "payload = " << request.payload << endl;

    std::string payloadHash = hexEncodeDigest(sha256Digest(request.payload));
    
    std::string canonicalRequest
        = request.method + "\n"
        + "/" + request.relativeUri + "\n"
        + canonicalQueryParams + "\n"
        + canonicalHeaders + "\n"
        + signedHeaders + "\n"
        + payloadHash;

    //cerr << "canonicalRequest = " << canonicalRequest << endl;

    RestParams authParams;

    std::string authHeader = "AWS4-HMAC-SHA256 ";

    auto addParam = [&] (std::string key, std::string value)
        {
            authHeader += key + "=" + value + ", ";

#if 0
            if (request.method == "POST") {
                authHeader

                if (!request.payload.empty())
                    request.payload += "&";
                request.payload += uriEncode(key) + "=" + uriEncode(value);
            }
            else if (request.method == "GET") {
                request.queryParams.push_back({key, value});
            }
#endif
        };



    std::string credentialScope = std::string(dateStr, 0, 8) + "/" + region + "/" + service + "/" + "aws4_request";
    
    addParam("Credential", accessKeyId + "/" + credentialScope);
    addParam("SignedHeaders", signedHeaders);
    
    //addParam("SignatureVersion", "4");
    //addParam("SignatureMethod", "AWS4-HMAC-SHA256");

    std::string hashedCanonicalRequest
        = hexEncodeDigest(sha256Digest(canonicalRequest));
    
    std::string stringToSign
        = "AWS4-HMAC-SHA256\n"
        + dateStr + "\n"
        + credentialScope + "\n"
        + hashedCanonicalRequest;

    //cerr << "stringToSign = " << stringToSign << endl;

    std::string signature = AwsApi::signV4(stringToSign, accessKey,
                                           std::string(dateStr, 0, 8),
                                           region, service);
    addParam("Signature", signature);

    authHeader.erase(authHeader.size() - 2);

    request.headers.push_back({"Authorization", authHeader});
}



/*****************************************************************************/
/* AWS BASIC API                                                             */
/*****************************************************************************/

#if 0

AwsBasicApi(const std::string & accessKeyId,
                    const std::string & accessKey,
                    const std::string & service,
                    const std::string & serviceUri = "",
                    const std::string & region = "us-east-1");

    void init(const std::string & accessKeyId,
              const std::string & accessKey,
              const std::string & service,
              const std::string & serviceUri = "",
              const std::string & region = "us-east-1");
              
    std::string accessKeyId;
    std::string accessKey;
    std::string serviceUri;
    std::string service;
    std::string region;

    HttpRestProxy proxy;


void
AwsBasicApi::
init(const std::string & accessKeyId,
     const std::string & accessKey,
     const std::string & serviceUri)
{
    this->serviceUri = serviceUri;

}

#endif

AwsBasicApi::
AwsBasicApi()
{
}

void
AwsBasicApi::
setService(const std::string & serviceName,
           const std::string & protocol,
           const std::string & region)
{
    this->serviceName = serviceName;
    this->protocol = protocol;
    this->region = region;

    this->serviceHost = serviceName + "." + region + ".amazonaws.com";
    this->serviceUri = protocol + "://" + serviceHost + "/";

    proxy.init(serviceUri);
    //proxy.debug = true;
}

void
AwsBasicApi::
setCredentials(const std::string & accessKeyId,
               const std::string & accessKey)
{
    this->accessKeyId = accessKeyId;
    this->accessKey = accessKey;
}

AwsBasicApi::BasicRequest
AwsBasicApi::
signPost(RestParams && params, const std::string & resource)
{
    BasicRequest result;
    result.method = "POST";
    result.relativeUri = resource;
    result.headers.push_back({"Host", serviceHost});
    result.headers.push_back({"Content-Type", "application/x-www-form-urlencoded; charset=utf-8"});

    std::string encodedPayload;

    for (auto p: params) {
        encodedPayload += uriEncode(p.first) + "=";
        encodedPayload += uriEncode(p.second) + "&";
    }

    if (!params.empty())
        encodedPayload.erase(encodedPayload.size() - 1);

    //cerr << "encodedPayload = " << encodedPayload << endl;

    result.payload = encodedPayload;
    
    addSignatureV4(result, serviceName, region, accessKeyId, accessKey);

    return result;

}

AwsBasicApi::BasicRequest
AwsBasicApi::
signGet(RestParams && params, const std::string & resource)
{
    BasicRequest result;
    result.method = "GET";
    result.relativeUri = resource;
    result.headers.push_back({"Host", serviceHost});
    result.queryParams = params;

    addSignatureV4(result, serviceName, region, accessKeyId, accessKey);

    return result;
}

std::unique_ptr<tinyxml2::XMLDocument>
AwsBasicApi::
performPost(RestParams && params,
            const std::string & resource,
            double timeoutSeconds)
{
    return perform(signPost(std::move(params), resource), timeoutSeconds, 3);
}

std::string
AwsBasicApi::
performPost(RestParams && params,
            const std::string & resource,
            const std::string & resultSelector,
            double timeoutSeconds)
{
    return extract<string>(*performPost(std::move(params), resource, timeoutSeconds),
                           resultSelector);
}

std::unique_ptr<tinyxml2::XMLDocument>
AwsBasicApi::
performGet(RestParams && params,
           const std::string & resource,
           double timeoutSeconds)
{
    return perform(signGet(std::move(params), resource), timeoutSeconds, 3);
}

std::unique_ptr<tinyxml2::XMLDocument>
AwsBasicApi::
perform(const BasicRequest & request,
        double timeoutSeconds,
        int retries)
{
    int retry = 0;
    for (; retry < retries;  ++retry) {
        HttpRestProxy::Response response;
        try {
            response = proxy.perform(request.method,
                                     request.relativeUri,
                                     HttpRestProxy::Content(request.payload),
                                     request.queryParams,
                                     request.headers,
                                     timeoutSeconds);

            if (response.code() == 200) {
                std::unique_ptr<tinyxml2::XMLDocument> body(new tinyxml2::XMLDocument());
                body->Parse(response.body().c_str());
                return body;
            }
            else if (response.code() == 503)
                continue;
            else {
                cerr << "request failed: " << response << endl;
                break;
            }
        } catch (const std::exception & exc) {
            cerr << "error on request: " << exc.what() << endl;
        }
    }

    throw MLDB::Exception("failed request after %d retries", retries);
}

std::string
AwsBasicApi::
performGet(RestParams && params,
           const std::string & resource,
           const std::string & resultSelector,
           double timeoutSeconds)
{
    return extract<std::string>(*performGet(std::move(params), resource, timeoutSeconds),
                           resultSelector);
}

} // namespace MLDB
