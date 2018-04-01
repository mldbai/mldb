/* aws.h                                                           -*- C++ -*-
   Jeremy Barnes, 8 August 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
   
   Amazon Web Services support code, especially signing of requests.
*/

#pragma once
#include <string>
#include "mldb/types/date.h"
#include "mldb/http/http_rest_proxy.h"

namespace tinyxml2 {
struct XMLDocument;
} // namespace tinyxml2

namespace MLDB {


/*****************************************************************************/
/* AWS API                                                                   */
/*****************************************************************************/

/** Base functionality for dealing with Amazon's APIs. */

struct AwsApi {
    template<typename Hash>
    static std::string hmacDigest(const std::string & stringToSign,
                                  const std::string & accessKey);

    template<typename Encoder>
    static std::string encodeDigest(const std::string & digest);

    static std::string hmacSha1Digest(const std::string & stringToSign,
                                      const std::string & accessKey);

    static std::string hmacSha256Digest(const std::string & stringToSign,
                                        const std::string & accessKey);
    
    static std::string sha256Digest(const std::string & stringToSign);

    static std::string base64EncodeDigest(const std::string & digest);

    static std::string hexEncodeDigest(const std::string & digest);
    
    /** URI encode the given string according to RFC 3986 */
    static std::string uriEncode(const std::string & str);

    /** URI encode the given string according to RFC 3986 */
    static std::string uriEncode(const Utf8String & str);

    /** Helper for url-escaping of resource names */
    static std::string escapeResource(const std::string & resource);

    /** See http://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html */
    static std::string signingKeyV4(const std::string & accessKey,
                                    const std::string & date,
                                    const std::string & region,
                                    const std::string & service,
                                    const std::string & signing = "aws4_request");
    
    /** See http://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html */
    static std::string signV4(const std::string & stringToSign,
                              const std::string & accessKey,
                              const std::string & date,
                              const std::string & region,
                              const std::string & service,
                              const std::string & signing = "aws4_request");

    struct BasicRequest {
        std::string method;
        std::string relativeUri;
        RestParams queryParams;
        RestParams headers;
        std::string payload;
    };

    enum PayloadDigest {
        PLD_IMPLICIT,   ///< Perform an implicit payload digest (old behavior)
        PLD_ON,         ///< Payload digest is on (default)
        PLD_OFF         ///< Payload digest is off (faster but less safe)
    };
    
    static void
    addSignatureV4(BasicRequest & request,
                   std::string service,
                   std::string region,
                   std::string accessKeyId,
                   std::string accessKey,
                   Date now = Date::now(),
                   PayloadDigest digest = PLD_ON);
};


/*****************************************************************************/
/* AWS BASIC API                                                             */
/*****************************************************************************/

/** Base class for basic services that use the Action API and signature
    V4.
*/

struct AwsBasicApi : public AwsApi {

    AwsBasicApi();

    void setService(const std::string & serviceName,
                    const std::string & protocol = "http",
                    const std::string & region = "us-east-1");

    void setCredentials(const std::string & accessKeyId,
                        const std::string & accessKey);
              
    std::string accessKeyId;
    std::string accessKey;

    std::string protocol;
    std::string serviceName;
    std::string serviceHost;
    std::string region;
    std::string serviceUri;

    std::unique_ptr<tinyxml2::XMLDocument>
    perform(const BasicRequest & request,
            double timeoutSeconds,
            int retries);

    std::string performPost(RestParams && params, const std::string & resource,
                            const std::string & resultSelector,
                            double timeoutSeconds = 10.0,
                            Date date = Date::now(),
                            PayloadDigest digest = PLD_ON);
    std::string performGet(RestParams && params, const std::string & resource,
                           const std::string & resultSelector,
                           double timeoutSeconds = 10.0,
                           Date date = Date::now(),
                           PayloadDigest digest = PLD_ON);

    std::unique_ptr<tinyxml2::XMLDocument>
    performPost(RestParams && params, const std::string & resource,
                double timeoutSeconds = 10.0,
                Date date = Date::now(),
                PayloadDigest digest = PLD_ON);

    std::unique_ptr<tinyxml2::XMLDocument>
    performGet(RestParams && params, const std::string & resource,
               double timeoutSeconds = 10.0,
               Date date = Date::now(),
               PayloadDigest digest = PLD_ON);
    
    BasicRequest signPost(RestParams && params,
                          const std::string & resource = "",
                          Date date = Date::now(),
                          PayloadDigest digest = PLD_ON);
    BasicRequest signGet(RestParams && params,
                         const std::string & resource = "",
                         Date date = Date::now(),
                         PayloadDigest digest = PLD_ON);
    
    HttpRestProxy proxy;
};

} // namespace MLDB
