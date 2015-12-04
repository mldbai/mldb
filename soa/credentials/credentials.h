// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* credentials.h                                                   -*- C++ -*-
   Jeremy Barnes, 5 November 2014
   
   A pluggable mechanism for getting credentials.
*/

#pragma once

#include "mldb/types/value_description.h"
#include "mldb/types/date.h"
#include "mldb/types/periodic_utils.h"

namespace Datacratic {

struct Credential {
    std::string provider; ///< Path through which credential was obtained
    std::string protocol; ///< Protocol to use to get to service
    std::string location; ///< URI to call to get resource
    std::string id;       ///< User ID
    std::string secret;   ///< Password / secret / etc

    Json::Value extra;    ///< Other fields

    Date validUntil;
};

DECLARE_STRUCTURE_DESCRIPTION(Credential);

struct CredentialContext {
};

DECLARE_STRUCTURE_DESCRIPTION(CredentialContext);

/** Return credentials for the given resource of the given resource type.

    If none are available, then returns an empty list.
*/
std::vector<Credential>
getCredentials(const std::string & resourceType,
               const std::string & resource,
               const CredentialContext & context,
               Json::Value extraData = Json::Value());

Credential getCredential(const std::string & resourceType,
                         const std::string & resource,
                         const CredentialContext & context = CredentialContext(),
                         Json::Value extraData = Json::Value(),
                         TimePeriod validTime = "99999d");

} // namespace Datacratic
