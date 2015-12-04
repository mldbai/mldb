// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** credentials.cc
    Jeremy Barnes, 5 November 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

*/

#include "credentials.h"
#include "mldb/types/structure_description.h"

using namespace std;

namespace Datacratic {

DEFINE_STRUCTURE_DESCRIPTION(Credential);

CredentialDescription::
CredentialDescription()
{
    addField("provider", &Credential::provider,
             "Provider of credentials.  This is normally an address on the internet that "
             "can be connected to in order to request the provided service.");
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

DEFINE_STRUCTURE_DESCRIPTION(CredentialContext);

CredentialContextDescription::
CredentialContextDescription()
{
}

} // namespace Datacratic
