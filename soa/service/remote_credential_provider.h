// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** remote_credential_provider.h                                   -*- C++ -*-
    Jeremy Barnes, 12 November 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    Credentials provider that gets credentials from a remote source.
*/

    
#pragma once

#include "mldb/soa/credentials/credentials.h"
#include "mldb/soa/credentials/credential_provider.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/soa/credentials/credentials.h"
#include "mldb/types/periodic_utils.h"

namespace Datacratic {


/*****************************************************************************/
/* CREDENTIALS DAEMON CLIENT                                                 */
/*****************************************************************************/

/** A client for a credentials daemon.

    By writing a daemon, it is possible to implement logic like short-lived
    credentials.

    The daemon must respond to the following routes:
    1.  GET /interfaces/credentialsd
    2.  GET /v1/types/<resourceType>/resources/<resource>/credentials?role=...&operation=...&validity=...&extra=...

*/

struct CredentialsDaemonClient {
    CredentialsDaemonClient() = default;
    CredentialsDaemonClient(const std::string & uri);

    ~CredentialsDaemonClient();
    
    void connect(const std::string & uri);
    
    std::vector<Credential>
    getCredentials(const std::string & resourceType,
                   const std::string & resource,
                   const std::string & role,
                   const std::string & operation = "*",
                   const TimePeriod & validity = "1h",
                   const Json::Value & extra = Json::Value()) const;
    
protected:
    HttpRestProxy conn;
};


/*****************************************************************************/
/* REMOTE CREDENTIAL PROVIDER                                                */
/*****************************************************************************/

struct RemoteCredentialProvider: public CredentialProvider {

    RemoteCredentialProvider(const std::string & uri);

    CredentialsDaemonClient client;

    virtual std::vector<std::string>
    getResourceTypePrefixes() const;

    virtual std::vector<Credential>
    getSync(const std::string & resourceType,
            const std::string & resource,
            const CredentialContext & context,
            Json::Value extraData) const;

};

/*****************************************************************************/
/* REGISTRATION                                                              */
/*****************************************************************************/

/** Add the ability to get remote credentials from the given URI. */
void addRemoteCredentialProvider(const std::string & uri);

/** This function will scan the environment looking for any entries that
    start with REMOTE_CREDENTIAL_PROVIDER, and for each of them will set
    up the given URI as a place to look for remote credentials.
*/
void initRemoteCredentialsFromEnvironment();

} // namespace Datacratic

