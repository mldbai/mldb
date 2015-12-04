// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** remote_credential_provider.cc
    Jeremy Barnes, 12 November 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    Credentials provider that gets credentials from a remote source.
*/

#include <unistd.h>
#include <thread>
#include "remote_credential_provider.h"
#include "mldb/types/vector_description.h"

using namespace std;


namespace Datacratic {
    
/*****************************************************************************/
/* CREDENTIALS DAEMON CLIENT                                                 */
/*****************************************************************************/

CredentialsDaemonClient::
CredentialsDaemonClient(const std::string & uri)
{
    connect(uri);
}

CredentialsDaemonClient::
~CredentialsDaemonClient()
{
}

void
CredentialsDaemonClient::
connect(const std::string & uri)
{
    if (uri.find("http") != 0) {
        throw ML::Exception("'uri' parameter does not start with http: " + uri);
    }
    conn.init(uri);

    try {

        int tries = 10;

        HttpRestProxy::Response resp;

        do{
            if (tries != 10)
            {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
            tries--;

            //don't throw exceptions on curl error, return the error code
            resp = conn.get("/info", RestParams(), RestParams(), -1, false);

        } while (resp.code() != 200 && tries > 0);

        if (tries == 0)
        {
            throw ML::Exception("Failed to connect to the credentials daemon after 10 tries, %s", resp.errorMessage_.c_str());
        }
    }
    catch (const std::exception & exc) {
        cerr << "Failed to connect to the credentials daemon: " << exc.what()
             << endl;
        throw;
    }
}

std::vector<Credential>
CredentialsDaemonClient::
getCredentials(const std::string & resourceType,
               const std::string & resource,
               const std::string & role,
               const std::string & operation,
               const TimePeriod & validity,
               const Json::Value & extra) const
{
    cerr << "getCredentials " << resourceType << " " << resource << endl;

    string uri = "/v1/types/" + resourceType + "/resources/" + resource
        + "/credentials";
    RestParams params;
    if (role != "")
        params.emplace_back("role", role);
    if (operation != "")
        params.emplace_back("operation", operation);
    if (validity != TimePeriod())
        params.emplace_back("validity", validity.toString());
    if (!extra.isNull())
        params.emplace_back("extra", extra.toStringNoNewLine());
    
    cerr << "calling" << endl;

    auto res = conn.get(uri, params, {}, 5.0 /* seconds */);

    cerr << "res = " << res << endl;

    if (res.code() != 200)
        throw ML::Exception("couldn't get credentials: returned code %d",
                            res.code());

    return jsonDecodeStr<std::vector<Credential> >(res.body());
}


/*****************************************************************************/
/* REMOTE CREDENTIAL PROVIDER                                                */
/*****************************************************************************/

RemoteCredentialProvider::
RemoteCredentialProvider(const std::string & uri)
{
    client.connect(uri);
}

std::vector<std::string>
RemoteCredentialProvider::
getResourceTypePrefixes() const
{
    return {""};  // for all prefixes
}

std::vector<Credential>
RemoteCredentialProvider::
getSync(const std::string & resourceType,
        const std::string & resource,
        const CredentialContext & context,
        Json::Value extraData) const
{
    cerr << "Remote getSync for " << resourceType << " " << resource
         << endl;

    std::string role = "default";
    std::string operation = "*";
    TimePeriod validity = "10000d";
    
    return client.getCredentials(resourceType, resource, role, operation,
                                 validity, extraData);
}


/*****************************************************************************/
/* REGISTRATION                                                              */
/*****************************************************************************/

void addRemoteCredentialProvider(const std::string & uri)
{
    cerr << "Registering remote credential provider at " << uri << endl;

    auto provider = std::make_shared<RemoteCredentialProvider>(uri);

    CredentialProvider::registerProvider("Remote provider at " + uri,
                                         provider);

}

void initRemoteCredentialsFromEnvironment()
{
    for (const char * const * p = environ;  *p;  ++p) {
        string var(*p);
        if (var.find("REMOTE_CREDENTIAL_PROVIDER") == 0) {
            auto equalPos = var.find('=');
            if (equalPos == string::npos)
                continue;  // strange, no equals
            string val(var, equalPos + 1);
            
            addRemoteCredentialProvider(val);
        }
    }
}

namespace {

struct AtInit {

    AtInit()
    {
        initRemoteCredentialsFromEnvironment();
    }
    
} atInit;

} // file scope

} // namespace Datacratic
