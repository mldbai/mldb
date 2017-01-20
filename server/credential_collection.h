/** credential_collection.h                                                  -*- C++ -*-
    Jeremy Barnes, 11 November 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/soa/service/event_service.h"
#include "mldb/rest/rest_request_router.h"
#include "mldb/rest/rest_service_endpoint.h"
#include "mldb/soa/credentials/credentials.h"
#include "mldb/rest/rest_collection.h"
#include "mldb/utils/log_fwd.h"



namespace MLDB {
struct MldbServer;

/*****************************************************************************/
/* CREDENTIAL RULE                                                           */
/*****************************************************************************/

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
                                       CredentialRuleStatus> Base;

    CredentialRuleCollection(MLDB::MldbServer * owner);
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

extern template class RestCollection<std::string, MLDB::CredentialRule>;

} // MLDB namespace



