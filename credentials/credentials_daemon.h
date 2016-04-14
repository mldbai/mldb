// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** credentiald.h                                                  -*- C++ -*-
    Jeremy Barnes, 11 November 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

*/

#pragma once

#include "mldb/soa/service/event_service.h"
#include "mldb/rest/rest_request_router.h"
#include "mldb/rest/rest_service_endpoint.h"
#include "mldb/soa/credentials/credential_provider.h"
#include "mldb/rest/rest_collection.h"
#include "mldb/rest/poly_collection.h"
#include "mldb/utils/log_fwd.h"

namespace Datacratic {

struct CredentialsDaemon;

/*****************************************************************************/
/* CREDENTIAL RULE                                                           */
/*****************************************************************************/

struct StoredCredentials {
    std::string resourceType;
    std::string resource;
    std::string role;
    std::string operation;
    Date expiration;
    Json::Value extra;
    Credential credential;
};

DECLARE_STRUCTURE_DESCRIPTION(StoredCredentials);

struct CredentialRuleConfig {
    std::string id;
    std::shared_ptr<StoredCredentials> store;
};

DECLARE_STRUCTURE_DESCRIPTION(CredentialRuleConfig);

struct CredentialRuleStatus {
    StoredCredentials stored;
};

DECLARE_STRUCTURE_DESCRIPTION(CredentialRuleStatus);

struct CredentialRule {
    CredentialRule(CredentialRuleConfig config);
    std::shared_ptr<CredentialRuleConfig> config;
};

struct CredentialRuleCollection
    : public RestConfigurableCollection<std::string, CredentialRule,
                                        CredentialRuleConfig,
                                        CredentialRuleStatus> {

    typedef RestConfigurableCollection<std::string, CredentialRule,
                                       CredentialRuleConfig,
                                       CredentialRuleStatus> Base2;
    
    CredentialRuleCollection(CredentialsDaemon * owner);
    ~CredentialRuleCollection();
    
    void init(RestRequestRouter & parentNode);

    static void initRoutes(RouteManager & manager);

    virtual std::string getKey(CredentialRuleConfig & config);

    virtual void setKey(CredentialRuleConfig & config, std::string key);

    virtual CredentialRuleStatus
    getStatusLoading(std::string key, const BackgroundTask & task) const;

    virtual CredentialRuleStatus
    getStatusFinished(std::string key, const CredentialRule & value) const;

    /** Perform the actual work of loading the dataset given by the
        configuration.  This is a synchronous function that will call
        onProgress as it progresses.
    */
    virtual std::shared_ptr<CredentialRule>
    construct(CredentialRuleConfig config, const OnProgress & onProgress) const;

    virtual std::shared_ptr<CredentialRuleConfig>
    getConfig(std::string key, const CredentialRule & value) const;
};

extern template class RestCollection<std::string, CredentialRule>;
extern template class RestConfigurableCollection<std::string, CredentialRule,
                                                 CredentialRuleConfig,
                                                 CredentialRuleStatus>;


/*****************************************************************************/
/* CREDENTIAL DAEMON                                                         */
/*****************************************************************************/

struct CredentialsDaemon
    : public EventRecorder, public RestServiceEndpoint, public RestDirectory {

    CredentialsDaemon();

    // Initialize with an event recorder
    CredentialsDaemon(const std::string & eventPrefix,
                      std::shared_ptr<EventService> events);

    ~CredentialsDaemon();

    void init(std::shared_ptr<CollectionConfigStore> configStore);

    std::string bindTcp(const PortRange & portRange = PortRange(),
                        const std::string & host = "localhost");

    /** Main query method to return credentials. */
    std::vector<Credential>
    getCredentials(const std::string & resourceType,
                   const std::string & resource,
                   const std::string & role,
                   const std::string & operation,
                   const TimePeriod & validity,
                   const Json::Value & extra);

    /** ServicePeer method that allows the Location: header to be filled in
        for POSTs
    */
    Utf8String getUriForPath(ResourcePath path);

private:
    CredentialRuleCollection rules;
    RestRequestRouter router;    
    std::shared_ptr<CollectionConfigStore> config;
    std::shared_ptr<RestRouteManager> routeManager;
    std::shared_ptr<spdlog::logger> logger;
};

} // namespace Datacratic
