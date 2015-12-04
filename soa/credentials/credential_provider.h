// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* credential_provider.h                                           -*- C++ -*-
   Jeremy Barnes, 5 November 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.
   
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

    virtual std::vector<std::string>
    getResourceTypePrefixes() const = 0;

    virtual std::vector<Credential>
    getSync(const std::string & resourceType,
            const std::string & resource,
            const CredentialContext & context,
            Json::Value extraData) const = 0;
    
    static void registerProvider(const std::string & name,
                                 std::shared_ptr<CredentialProvider> provider);
};


} // namespace Datacratic
