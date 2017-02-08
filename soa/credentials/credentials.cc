// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** credentials.cc
    Jeremy Barnes, 5 November 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "credentials.h"
#include "mldb/types/structure_description.h"

using namespace std;

namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(Credential);

CredentialDescription::
CredentialDescription()
{
    addField("provider", &Credential::provider,
             "Provider of credentials.  This is an identifier that could help "
             "locating where the credentials are stored. ");
    addField("protocol", &Credential::protocol,
             "Protocol to use to access the service");
    addField("location", &Credential::location,
             "Location of the service.  This is used where there are multiple "
             "places that the same service can be provided, but we want to access "
             "the most local one.");
    addField("id", &Credential::id,
             "User ID to use to access the service.  This is sometimes called the key ID "
             "or user name.");
    addField("secret", &Credential::secret,
             "Secret key to use to access the service.  This is sometimes called the "
             "access key or password.  It is secret and sensitive.");
    addField("extra", &Credential::extra,
             "Extra configuration needed to access the service.  Some service providers "
             "require extra information that can't be added to the rest of the fields, "
             "and that information should go here.  See the documentation for the "
             "service providers for more information.");
    addField("validUntil", &Credential::validUntil,
             "Time until which the credential is valid.  After this date the credentials "
             "should not be used; instead a new call should be made to get new "
             "credentials.");
}

DEFINE_STRUCTURE_DESCRIPTION(StoredCredentials);

StoredCredentialsDescription::
StoredCredentialsDescription()
{
    addField("resourceType", &StoredCredentials::resourceType,
             "Type of resource that this credential rule applies to.  Currently, "
             "resources of type aws:s3 are supported when accessing AWS S3.");
    addField("resource", &StoredCredentials::resource,
             "Resource that this credential rule applies to.  This is a URI style path. "
             "If the requested resource match this prefix, the credentials will be used. "
             "The most specific match is used.  If this is empty, the credentials will "
             "be used for all resources of type `resourceType`.");
    addField("expiration", &StoredCredentials::expiration,
             "Date on which credentials expire.  After this date they will no "
             "longer match.");
    addField("extra", &StoredCredentials::extra,
             "Extra credential parameters.  Some credentials types require extra "
             "information; that information can be put here.  See the documentation "
             "for the specific credentials type for more information.");
    addField("credential", &StoredCredentials::credential,
             "Credentials for when the pattern matches.  These will be returned "
             "to the caller if the above rules match.");
}


} // namespace MLDB
