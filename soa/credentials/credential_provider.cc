// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* credential_provider.cc
   Jeremy Barnes, 5 November 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

   Basic functionality to get credentials.
*/

#include "credential_provider.h"
#include <mutex>

using namespace std;

namespace Datacratic {

/*****************************************************************************/
/* CREDENTIAL PROVIDER                                                       */
/*****************************************************************************/

CredentialProvider::
~CredentialProvider()
{
}

namespace {

std::mutex providersLock;
std::multimap<std::string, std::shared_ptr<CredentialProvider> > providers;

} // file scope

void
CredentialProvider::
registerProvider(const std::string & name,
                 std::shared_ptr<CredentialProvider> provider)
{
    std::unique_lock<std::mutex> guard(providersLock);

    auto prefixes = provider->getResourceTypePrefixes();

    for (string prefix: prefixes)
        providers.insert({ prefix, provider });
}

std::vector<Credential>
getCredentials(const std::string & resourceType,
               const std::string & resource,
               const CredentialContext & context,
               Json::Value extraData)
{
    std::unique_lock<std::mutex> guard(providersLock);

    std::vector<Credential> result;

    for (auto it = providers.lower_bound(resourceType);  it != providers.end();
           ++it) {
        if (resourceType.find(it->first) != 0)
            break;  // not a prefix
        auto creds = it->second->getSync(resourceType, resource, context,
                                         extraData);
        result.insert(result.end(), creds.begin(), creds.end());
    }

    return result;
}

Credential
getCredential(const std::string & resourceType,
              const std::string & resource,
              const CredentialContext & context,
              Json::Value extraData,
              TimePeriod validTime)
{
    std::unique_lock<std::mutex> guard(providersLock);

    cerr << "getCredential" << endl;

    for (auto it = providers.begin(), end = providers.end();
         it != end;  ++it) {
        // cerr << "testing " << it->first << " against " << resourceType
        //     << " " << resource << endl;
        if (resourceType.find(it->first) != 0)
            break;  // not a prefix
        // cerr << "FOUND" << endl;

        auto creds = it->second->getSync(resourceType, resource, context,
                                         extraData);
        if (!creds.empty()) {
            /**** THIS WILL OUTPUT CREDENTIALS IN CLEAR - KEEP COMMENTED OUT EXCEPT WHEN DEBUGGING ****/
            // cerr << "credentials for " << resourceType << " " << resource
            //      << " are " << endl << jsonEncode(creds[0]) << endl;
            return creds[0];
        }
    }

    for (auto it = providers.lower_bound(resourceType);  it != providers.end();
           ++it) {
        // cerr << "testing " << it->first << " against " << resourceType
        //     << endl;
        if (resourceType.find(it->first) != 0)
            break;  // not a prefix
        // cerr << "FOUND" << endl;

        auto creds = it->second->getSync(resourceType, resource, context,
                                         extraData);
        if (!creds.empty()) {
             /**** THIS WILL OUTPUT CREDENTIALS IN CLEAR - KEEP COMMENTED OUT EXCEPT WHEN DEBUGGING ****/
            // cerr << "credentials for " << resourceType << " " << resource
            //      << " are " << endl << jsonEncode(creds[0]) << endl;
            return creds[0];
        }
    }

    throw ML::Exception("No credentials found for " + resourceType + " "
                        + resource);
}


} // namespace Datacratic

