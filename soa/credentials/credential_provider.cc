// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* credential_provider.cc
   Jeremy Barnes, 5 November 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Basic functionality to get credentials.
*/

#include "credential_provider.h"
#include "mldb/types/optional.h"
#include "mldb/base/exc_assert.h"
#include <mutex>
#include <iostream>

using namespace std;

namespace MLDB {

/*****************************************************************************/
/* CREDENTIAL PROVIDER                                                       */
/*****************************************************************************/

CredentialProvider::
~CredentialProvider()
{
}

namespace {

std::mutex providersLock;
std::vector<std::shared_ptr<CredentialProvider> > providers;

} // file scope

void
CredentialProvider::
registerProvider(std::shared_ptr<CredentialProvider> provider)
{
    std::unique_lock<std::mutex> guard(providersLock);
    providers.push_back(provider);
}

Credential
getCredential(const std::string & resourceType,
              const std::string & resource)
{
    std::vector<StoredCredentials> candidates;
    {
        std::unique_lock<std::mutex> guard(providersLock);

        // find all credentials matching the resource type
        for (auto it = providers.begin(), end = providers.end();
             it != end;  ++it) {
            auto creds = (*it)->getCredentialsOfType(resourceType);
            if (!creds.empty()) {
                candidates.insert(candidates.end(), creds.begin(), creds.end());
            }
        }
    }

    // find the best match

    // This might be too simplistic but we assume here that longer
    // is the prefix matching a resource URI better is the match.
    // In particular, this logic has no understanding of URI structure
    // like folders, buckets, filename, extension and so on.
    Optional<StoredCredentials> bestMatch;
    for (const auto & storedCredential : candidates) {
        ExcAssertEqual(resourceType, storedCredential.resourceType);

        if (resource.find(storedCredential.resource) != 0)
            continue;  // not a prefix

        // better match on path
        if (!bestMatch || bestMatch->resource.find(storedCredential.resource)) {
            bestMatch.reset(new StoredCredentials(storedCredential));
        }
    }

    if (bestMatch) {
        // found credentials for the resource
        return bestMatch->credential;
    }

    throw MLDB::Exception("No credentials found for " + resourceType + " "
                        + resource);
}


} // namespace MLDB
