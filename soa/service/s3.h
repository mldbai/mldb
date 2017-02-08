/* s3.h                                                            -*- C++ -*-
   Jeremy Barnes, 3 July 2012

   This file is part of MLDB.
   Copyright 2012-2015 mldb.ai inc. All rights reserved.

   Class to deal with doing s3.
   Note: Your access key must have the listallmybuckets permission on the aws side.
*/

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <map>
#include "mldb/ext/tinyxml2/tinyxml2.h"
#include "mldb/soa/service/aws.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/http/http_header.h"
#include "mldb/http/http_request.h"


namespace MLDB {

struct S3Config {
    std::string accessKeyId;
    std::string accessKey;
};

DECLARE_STRUCTURE_DESCRIPTION(S3Config);


/*****************************************************************************/
/* S3 OBJECTINFO TYPES                                                       */
/*****************************************************************************/

/* This enum contains the list of attributes that can be queried via the
 * S3Api::getObjectInfo functions */

enum S3ObjectInfoTypes {
    LASTMODIFIED  = 1 << 0,
    SIZE          = 1 << 1,
    ETAG          = 1 << 2,

    STORAGECLASS  = 1 << 3,
    OWNERID       = 1 << 4,
    OWNERNAME     = 1 << 5,

    SHORT_INFO = LASTMODIFIED | SIZE | ETAG,
    FULL_EXTRAS = STORAGECLASS | OWNERID | OWNERNAME,
    FULL_INFO = SHORT_INFO | FULL_EXTRAS
};


/*****************************************************************************/
/* S3 API                                                                    */
/*****************************************************************************/

/** Interface to Amazon's S3 service. */

struct S3Api : public AwsApi {
    /** Default value for bandwidth to service.  In mega*bytes* per second.
        Default value is 20.0 MBPS for ec2 instances in the same availability
        zone.
    */
    static double defaultBandwidthToServiceMbps;

    S3Api();

    /** Set up the API to called with the given credentials. */
    S3Api(const std::string & accessKeyId,
          const std::string & accessKey,
          double bandwidthToServiceMbps = defaultBandwidthToServiceMbps,
          const std::string & defaultProtocol = "http",
          const std::string & serviceUri = "s3.amazonaws.com");

    /** Set up the API to called with the given credentials. */
    void init(const std::string & accessKeyId,
              const std::string & accessKey,
              double bandwidthToServiceMbps = defaultBandwidthToServiceMbps,
              const std::string & defaultProtocol = "http",
              const std::string & serviceUri = "s3.amazonaws.com");

    std::string accessKeyId;
    std::string accessKey;
    std::string defaultProtocol;
    std::string serviceUri;
    double bandwidthToServiceMbps;

    struct Range {
        static Range Full;

        Range(uint64_t aSize);
        Range(uint64_t aOffset, uint64_t aSize);

        uint64_t endPos() const;

        void adjust(size_t downloaded);
        std::string headerValue() const;

        bool operator == (const Range & other) const;
        bool operator != (const Range & other) const;

        uint64_t offset;
        uint64_t size;
    };

    /** A set of parameters that specify a request. */
    struct RequestParams {
        RequestParams();

        bool useRange() const;

        std::string verb;
        std::string bucket;
        std::string resource;
        std::string subResource;
        std::string date;

        std::string contentType;
        std::string contentMd5;
        HttpRequestContent content;
        Range downloadRange;

        RestParams headers;
        RestParams queryParams;
    };

    /** The response of a request.  Has a return code and a body. */
    struct Response {
        Response();

        const std::string & body() const;
        std::unique_ptr<tinyxml2::XMLDocument> bodyXml() const;
        operator std::unique_ptr<tinyxml2::XMLDocument>() const;
        std::string bodyXmlStr() const;
        std::string getHeader(const std::string & name) const;

        long code_;
        std::string body_;
        HttpHeader header_;
    };

    enum Redundancy {
        REDUNDANCY_DEFAULT,
        REDUNDANCY_STANDARD,
        REDUNDANCY_REDUCED,
        REDUNDANCY_GLACIER
    };

    /** Set the meaning of REDUNDANCY_DEFAULT.  Default is REDUNDANCY_STANDARD.
     */
    static void setDefaultRedundancy(Redundancy redundancy);

    /** Get the meaning of REDUNDANCY_DEFAULT.  */
    static Redundancy getDefaultRedundancy();

    enum ServerSideEncryption {
        SSE_NONE,
        SSE_AES256
    };

    struct ObjectMetadata {
        ObjectMetadata();
        ObjectMetadata(Redundancy redundancy);

        RestParams getRequestHeaders() const;

        Redundancy redundancy;
        ServerSideEncryption serverSideEncryption;
        std::string contentType;
        std::string contentEncoding;
        std::map<std::string, std::string> metadata;
        std::string acl;

        /* maximum number of concurrent requests */
        unsigned int numRequests;
    };

    /** Signed request that can be executed. */
    struct SignedRequest {
        RequestParams params;
        std::string auth;
        std::string resource;
        double bandwidthToServiceMbps;
    };

    /** Calculate the signature for a given request. */
    std::string signature(const RequestParams & request) const;

    /** Prepare a request to be executed. */
    std::shared_ptr<SignedRequest> prepare(const RequestParams
                                           & request) const;

    typedef std::function<void (Response &&, std::exception_ptr)> OnResponse;

    /** Perform the request asynchronously. */
    void perform(const OnResponse & onResponse,
                 const std::shared_ptr<SignedRequest> & rq) const;

    /** Perform the request synchronously and return the result. */
    Response performSync(const std::shared_ptr<SignedRequest> & rq) const;

    /** Escape a resource used by S3; this in particular leaves a slash
        in place. */
    static std::string s3EscapeResource(const std::string & resource);

    /** Perform a HEAD request from end to end. */
    Response head(const std::string & bucket,
                  const std::string & resource,
                  const std::string & subResource = "",
                  const RestParams & headers = RestParams(),
                  const RestParams & queryParams = RestParams()) const;
    Response headEscaped(const std::string & bucket,
                         const std::string & resource,
                         const std::string & subResource = "",
                         const RestParams & headers = RestParams(),
                         const RestParams & queryParams = RestParams()) const;

    /** Perform a GET request from end to end. */
    Response get(const std::string & bucket,
                 const std::string & resource,
                 const Range & downloadRange,
                 const std::string & subResource = "",
                 const RestParams & headers = RestParams(),
                 const RestParams & queryParams = RestParams()) const;
    Response getEscaped(const std::string & bucket,
                        const std::string & resource,
                        const Range & downloadRange,
                        const std::string & subResource = "",
                        const RestParams & headers = RestParams(),
                        const RestParams & queryParams = RestParams()) const;
    void getAsync(const OnResponse & onResponse,
                  const std::string & bucket,
                  const std::string & resource,
                  const Range & downloadRange,
                  const std::string & subResource = "",
                  const RestParams & headers = RestParams(),
                  const RestParams & queryParams = RestParams()) const;
    void getEscapedAsync(const OnResponse & onResponse,
                         const std::string & bucket,
                         const std::string & resource,
                         const Range & downloadRange,
                         const std::string & subResource = "",
                         const RestParams & headers = RestParams(),
                         const RestParams & queryParams = RestParams()) const;

    /** Perform a POST request from end to end. */
    Response post(const std::string & bucket,
                  const std::string & resource,
                  const std::string & subResource = "",
                  const RestParams & headers = RestParams(),
                  const RestParams & queryParams = RestParams(),
                  const HttpRequestContent & content
                  = HttpRequestContent()) const;
    Response postEscaped(const std::string & bucket,
                         const std::string & resource,
                         const std::string & subResource = "",
                         const RestParams & headers = RestParams(),
                         const RestParams & queryParams = RestParams(),
                         const HttpRequestContent & content
                         = HttpRequestContent()) const;

    /** Perform a PUT request from end to end including data. */
    Response put(const std::string & bucket,
                 const std::string & resource,
                 const std::string & subResource = "",
                 const RestParams & headers = RestParams(),
                 const RestParams & queryParams = RestParams(),
                 const HttpRequestContent & content
                 = HttpRequestContent()) const;
    Response putEscaped(const std::string & bucket,
                        const std::string & resource,
                        const std::string & subResource = "",
                        const RestParams & headers = RestParams(),
                        const RestParams & queryParams = RestParams(),
                        const HttpRequestContent & content
                        = HttpRequestContent()) const;
    void putAsync(const OnResponse & onResponse,
                  const std::string & bucket,
                  const std::string & resource,
                  const std::string & subResource = "",
                  const RestParams & headers = RestParams(),
                  const RestParams & queryParams = RestParams(),
                  const HttpRequestContent & content
                  = HttpRequestContent()) const;
    void putEscapedAsync(const OnResponse & onResponse,
                         const std::string & bucket,
                         const std::string & resource,
                         const std::string & subResource = "",
                         const RestParams & headers = RestParams(),
                         const RestParams & queryParams = RestParams(),
                         const HttpRequestContent & content
                         = HttpRequestContent()) const;

    /** Perform a DELETE request from end to end including data. */
    Response erase(const std::string & bucket,
                   const std::string & resource,
                   const std::string & subResource = "",
                   const RestParams & headers = RestParams(),
                   const RestParams & queryParams = RestParams()) const;
    Response eraseEscaped(const std::string & bucket,
                          const std::string & resource,
                          const std::string & subResource = "",
                          const RestParams & headers = RestParams(),
                          const RestParams & queryParams
                          = RestParams()) const;


    enum CheckMethod {
        CM_SIZE,     ///< Check via the size of the content
        CM_MD5_ETAG, ///< Check via the md5 of the content vs the etag
        CM_ASSUME_INVALID  ///< Anything there is assumed invalid
    };

    struct ObjectInfo : public FsObjectInfo {
        ObjectInfo()
        {}

        ObjectInfo(tinyxml2::XMLNode * element);
        ObjectInfo(const S3Api::Response & response);

        std::string key;
    };

    typedef std::function<bool (const std::string & prefix,
                                const std::string & objectName,
                                const ObjectInfo & info,
                                int depth)>
        OnObject;

    typedef std::function<bool (const std::string & prefix,
                                const std::string & dirName,
                                int depth)>
        OnSubdir;

    /** For each file matching the given prefix in the given bucket, call
        the callback.
    */
    void forEachObject(const std::string & bucket,
                       const std::string & prefix = "",
                       const OnObject & onObject = OnObject(),
                       const OnSubdir & onSubdir = OnSubdir(),
                       const std::string & delimiter = "/",
                       int depth = 1,
                       const std::string & startAt = "") const;

    typedef std::function<bool (const std::string & uri,
                                const ObjectInfo & info,
                                int depth)>
        OnObjectUri;

    /** For each file matching the given prefix in the given bucket, call
        the callback.
    */
    void forEachObject(const std::string & uriPrefix,
                       const OnObjectUri & onObject,
                       const OnSubdir & onSubdir = OnSubdir(),
                       const std::string & delimiter = "/",
                       int depth = 1,
                       const std::string & startAt = "") const;

    /** Value for the "delimiter" parameter in forEachObject for when we
        don't want any subdirectories.  It is equal to the empty string.
    */
    static const std::string NO_SUBDIRS;

    /** Does the object exist? */
    ObjectInfo tryGetObjectInfo(const std::string & bucket,
                                const std::string & object,
                                S3ObjectInfoTypes infos = SHORT_INFO) const;
    ObjectInfo tryGetObjectInfo(const std::string & uri,
                                S3ObjectInfoTypes infos = SHORT_INFO) const;


    /** Return the ObjectInfo about the object.  Throws an exception if it
        doesn't exist.
    */
    ObjectInfo getObjectInfo(const std::string & bucket,
                             const std::string & object,
                             S3ObjectInfoTypes infos = SHORT_INFO) const;
    ObjectInfo getObjectInfo(const std::string & uri,
                             S3ObjectInfoTypes infos = SHORT_INFO) const;

    /** Erase the given object.  Throws an exception if it fails. */
    void eraseObject(const std::string & bucket,
                     const std::string & object);

    /** Erase the given object.  Throws an exception if it fails. */
    void eraseObject(const std::string & uri);

    /** Erase the given object.  Returns true if an object was erased or false
        otherwise.
    */
    bool tryEraseObject(const std::string & bucket,
                        const std::string & object);
    
    /** Erase the given object.  Returns true if an object was erased or false
        otherwise.
    */
    bool tryEraseObject(const std::string & uri);

    /** Return the public URI that should be used to access a public object. */
    static std::string getPublicUri(const std::string & uri,
                                    const std::string & protocol);

    static std::string getPublicUri(const std::string & bucket,
                                    const std::string & object,
                                    const std::string & protocol);

    typedef std::function<bool (std::string bucket)> OnBucket;

    /** Call the given callback on every bucket in the current
        account.
    */
    bool forEachBucket(const OnBucket & bucket) const;

    /** Turn a s3:// uri string into a bucket name and object. */
    static std::pair<std::string, std::string>
    parseUri(const std::string & uri);

    struct MultiPartUploadPart {
        MultiPartUploadPart();

        void fromXml(tinyxml2::XMLElement * element);

        int partNumber;
        uint64_t startOffset;
        uint64_t size;
        std::string lastModified;
        std::string etag;
        bool done;
    };

    struct MultiPartUpload {
        std::string id;
        std::vector<MultiPartUploadPart> parts;
    };

    enum UploadRequirements {
        UR_EXISTING,   ///< OK to return an existing one
        UR_FRESH,      ///< Must be a fresh one
        UR_EXCLUSIVE   ///< Must be a fresh one, and no other may exist
    };

    /** Obtain a multipart upload, either in progress or a new one. */
    MultiPartUpload
    obtainMultiPartUpload(const std::string & bucket,
                          const std::string & resource,
                          const ObjectMetadata & metadata,
                          UploadRequirements requirements) const;

    std::pair<bool,std::string>
    isMultiPartUploadInProgress(const std::string & bucket,
                                const std::string & resource) const;

    std::string
    finishMultiPartUpload(const std::string & bucket,
                          const std::string & resource,
                          const std::string & uploadId,
                          const std::vector<std::string> & etags) const;

    //easy handle for v8 wrapping
    void setDefaultBandwidthToServiceMbps(double mpbs);

private:
    ObjectInfo tryGetObjectInfoShort(const std::string & bucket,
                                     const std::string & object) const;
    ObjectInfo tryGetObjectInfoFull(const std::string & bucket,
                                    const std::string & object) const;
    ObjectInfo getObjectInfoShort(const std::string & bucket,
                                  const std::string & object) const;
    ObjectInfo getObjectInfoFull(const std::string & bucket,
                                 const std::string & object) const;

    /// Static variable to hold the default redundancy to be used
    static Redundancy defaultRedundancy;

};

/** S3 support for filter_ostream opens.  Register the bucket name here, and
    you can open it directly from s3.
*/

void registerS3Bucket(const std::string & bucketName,
                      const std::string & accessKeyId,
                      const std::string & accessKey,
                      double bandwidthToServiceMbps = S3Api::defaultBandwidthToServiceMbps,
                      const std::string & protocol = "http",
                      const std::string & serviceUri = "s3.amazonaws.com");

/** S3 support for filter_ostream opens.  Register the bucket name here, and
    you can open it directly from s3.  Queries and iterates over all
    buckets within the account.
*/

void registerS3Buckets(const std::string & accessKeyId,
                       const std::string & accessKey,
                       double bandwidthToServiceMbps = S3Api::defaultBandwidthToServiceMbps,
                       const std::string & protocol = "http",
                       const std::string & serviceUri = "s3.amazonaws.com");

/** Returns an S3Api constructed to access the given URI.  Will look up its
    own credentials using registered credential providers, or one which was
    registered using registerS3Bucket or registerS3Buckets, or
    ~/.cloud_credentials, or S3_KEY_ID, S3_KEY and S3_BUCKETS environment
    variables.
*/
std::shared_ptr<S3Api> getS3ApiForUri(const std::string & uri);

} // namespace MLDB
