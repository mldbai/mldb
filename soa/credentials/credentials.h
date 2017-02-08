// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* credentials.h                                                   -*- C++ -*-
   Jeremy Barnes, 5 November 2014

   A pluggable mechanism for getting credentials.
*/

#pragma once

#include "mldb/types/value_description_fwd.h"
#include "mldb/types/date.h"
#include "mldb/ext/jsoncpp/value.h"
#include "mldb/types/periodic_utils.h"

namespace MLDB {

struct Credential {
    std::string provider; ///< Path through which credential was obtained
    std::string protocol; ///< Protocol to use to get to service (e.g. http, https)
    std::string location; ///< URI to call to get resource (for AWS this is s3.amazonaws.com)
    std::string id;       ///< User ID (for AWS S3 this is the key)
    std::string secret;   ///< Password / secret / etc
    /** Extra parameters to control the use of the credentials.
     *  For example, the field `bandwidthToServiceMbps` can be used
     *  for S3 credentials to indicate the available bandwidth to the service.
     *  This affects the timeouts.
     */
    Json::Value extra;    ///< Other fields
    Date validUntil;
};

DECLARE_STRUCTURE_DESCRIPTION(Credential);

struct StoredCredentials {
    std::string resourceType;
    std::string resource;
    Date expiration;
    Json::Value extra;
    Credential credential;
};

DECLARE_STRUCTURE_DESCRIPTION(StoredCredentials);

/** Returns the best credential match for the given resource.
    If no credential is found an exception is thrown.
*/
Credential getCredential(const std::string & resourceType,
                         const std::string & resource);

} // namespace MLDB
