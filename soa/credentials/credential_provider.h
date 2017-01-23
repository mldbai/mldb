/* credential_provider.h                                           -*- C++ -*-
   Jeremy Barnes, 5 November 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Credential provider structure and registration.
*/

#include "mldb/soa/credentials/credentials.h"

#pragma once

namespace MLDB {


/*****************************************************************************/
/* CREDENTIAL PROVIDER                                                       */
/*****************************************************************************/

/** Interface that can provide access to credentials to unlock private resources.

    Example of credential providers include file-based provider, command-line
    provider and in-memory provider.
*/
struct CredentialProvider {

    virtual ~CredentialProvider();

    /**
       Returns a list of credentials matching the resource type.

       This call must return <b>all</b> the available stored credentials
       matching  <b>exactly</b> the resourceType.  Additional logic is implemented
       on top of that call to find the best matching credential for the resource.
    */
    virtual std::vector<StoredCredentials>
    getCredentialsOfType(const std::string & resourceType) const = 0;

    /**
       Adds a provider to the list of providers.  This should be called at
       load time for all providers.
    */
    static void registerProvider(std::shared_ptr<CredentialProvider> provider);
};


} // namespace MLDB
