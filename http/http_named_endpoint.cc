// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* http_named_endpoint.cc
   Jeremy Barnes, 11 November 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Named endpoint for http connections.
*/

#include "http_named_endpoint.h"
#include <sys/utsname.h>


using namespace std;


namespace MLDB {


/*****************************************************************************/
/* HTTP NAMED ENDPOINT                                                       */
/*****************************************************************************/

HttpNamedEndpoint::
HttpNamedEndpoint()
{
}

void
HttpNamedEndpoint::
init(std::shared_ptr<ConfigurationService> config,
          const std::string & endpointName)
{
    NamedEndpoint::init(config, endpointName);
}

std::string
HttpNamedEndpoint::
bindTcp(PortRange const & portRange, std::string host)
{
    using namespace std;

    // TODO: generalize this...
    if (host == "" || host == "*")
        host = "0.0.0.0";

    // TODO: really scan ports
    int port = HttpEndpoint::listen(portRange, host, false /* name lookup */);

    cerr << "bound tcp for http port " << port << endl;

    auto getUri = [&] (const std::string & host)
        {
            return "http://" + host + ":" + to_string(port);
        };

    Json::Value config;

    auto addEntry = [&] (const std::string & addr,
                         const std::string & hostScope,
                         const std::string & uri)
        {
            Json::Value & entry = config[config.size()];
            entry["httpUri"] = uri;

            Json::Value & transports = entry["transports"];
            transports[0]["name"] = "tcp";
            transports[0]["addr"] = addr;
            transports[0]["hostScope"] = hostScope;
            transports[0]["port"] = port;
            transports[1]["name"] = "http";
            transports[1]["uri"] = uri;
        };

    if (host == "0.0.0.0") {
        auto interfaces = getInterfaces({AF_INET});
        for (unsigned i = 0;  i < interfaces.size();  ++i) {
            addEntry(interfaces[i].addr,
                     interfaces[i].hostScope,
                     getUri(interfaces[i].addr));
        }
        publishAddress("tcp", config);
        return getUri(host);
    }
    else {
        string host2 = addrToIp(host);
        string uri = getUri(host2);
        // TODO: compute host scope
        addEntry(host2, "*", uri);
        publishAddress("tcp", config);
        return uri;
    }
}


} // namespace MLDB
