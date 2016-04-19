/* credential_provider.h                                           -*- C++ -*-
   Jeremy Barnes, 5 November 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

   Credential provider structure and registration.
*/

#include "mldb/soa/credentials/credentials.h"

#pragma once

namespace Datacratic {


/*****************************************************************************/
/* CREDENTIAL PROVIDER                                                       */
/*****************************************************************************/

/** Base class that can provide credentials to access a given resource.

    Credentials are pluggable to allow for flexible scenarios.
*/
struct CredentialProvider {

    virtual ~CredentialProvider();

    /**
       Returns a set of resource types (e.g. aws:s3).  This is
       used by the look-up credential algorithm to find the
       right credential provider for the resource.

       If one of the resource type is a prefix of the resource,
       the credential look-up algorithm calls getSync.
    */
    virtual std::vector<std::string>
    getResourceTypePrefixes() const = 0;

    /**
       Returns a list of credentials matching the resource type
       and resource.  Currently, the context and extraData params
       are not used.
    */
    virtual std::vector<Credential>
    getSync(const std::string & resourceType,
            const std::string & resource,
            const CredentialContext & context,
            Json::Value extraData) const = 0;

    /**
       Adds a provider to the list of providers.  Currently, the
       name param is not used.
    */
    static void registerProvider(const std::string & name,
                                 std::shared_ptr<CredentialProvider> provider);
};


} // namespace Datacratic
