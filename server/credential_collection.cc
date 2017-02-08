/** credential_collection.cc
    Jeremy Barnes, 11 November 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

     This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "credential_collection.h"
#include "mldb/server/mldb_server.h"
#include "mldb/rest/rest_collection_impl.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/pointer_description.h"
#include "mldb/soa/credentials/credential_provider.h"
#include "mldb/utils/log.h"
#include "mldb/utils/json_utils.h"
#include <signal.h>


using namespace std;


namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(CredentialRuleConfig);

CredentialRuleConfigDescription::
CredentialRuleConfigDescription()
{
    addField("id", &CredentialRuleConfig::id,
             "ID of rule");
    addField("store", &CredentialRuleConfig::store,
             "Credentials to be stored");
}

DEFINE_STRUCTURE_DESCRIPTION(CredentialRuleStatus);

CredentialRuleStatusDescription::
CredentialRuleStatusDescription()
{
    addField("stored", &CredentialRuleStatus::stored,
             "Credential rule (with the actual credentials blanked out) stored "
             "for this rule");
}



/*****************************************************************************/
/* CREDENTIAL RULE                                                           */
/*****************************************************************************/

CredentialRule::
CredentialRule(CredentialRuleConfig config)
{
    this->config.reset(new CredentialRuleConfig(config));
}

/*****************************************************************************/
/* COLLECTION CREDENTIAL PROVIDER                                            */
/*****************************************************************************/
struct CollectionCredentialProvider: public CredentialProvider {

    CollectionCredentialProvider(const std::shared_ptr<CredentialRuleCollection> rules)
        : rules(rules)
    {

    }

    // not owning because credential providers are stored in a global
    // object and we don't want to delay the destruction of a collection
    // object that late in the MLDB shutdown process
    std::weak_ptr<CredentialRuleCollection> rules;

    virtual std::vector<StoredCredentials>
    getCredentialsOfType(const std::string & resourceType) const
    {
        std::shared_ptr<CredentialRuleCollection> sharedRules =
            rules.lock();

        // getCredentialsOfType called after destruction of the collection!
        ExcAssert(sharedRules);

        vector<StoredCredentials> matchingCreds;
        if (sharedRules) {
            auto keys = sharedRules->getKeys();
            for (const auto & key : keys) {
                auto cred = sharedRules->tryGetEntry(key);
                ExcAssert(!cred.second); // still under construction!
                if (cred.first) {
                    auto stored = cred.first->config->store;
                    if (resourceType != stored->resourceType)
                        continue;
                    matchingCreds.push_back(*stored);
                }
            }
        }

        return matchingCreds;
    }
};

/*****************************************************************************/
/* CREDENTIAL RULE COLLECTION                                                */
/*****************************************************************************/

CredentialRuleCollection::
CredentialRuleCollection(MLDB::MldbServer * server)
    : Base("credential", "credentials", server)
{
    this->backgroundCreate = false;
}

CredentialRuleCollection::
~CredentialRuleCollection()
{
}

void
CredentialRuleCollection::
init(RestRequestRouter & parentNode)
{
}

void
CredentialRuleCollection::
initRoutes(RouteManager & manager)
{
    Base::initRoutes(manager);

    manager.addPutRoute();
    manager.addPostRoute();
    manager.addDeleteRoute();
}

std::string
CredentialRuleCollection::
getKey(CredentialRuleConfig & config)
{
    if (config.id == "") {
        // By default, it's a content hash of the credential contents
        uint64_t hash = jsonHash(jsonEncode(config));
        return MLDB::format("%016llx", (unsigned long long)hash);
    }
    return config.id;
}

void
CredentialRuleCollection::
setKey(CredentialRuleConfig & config, std::string key)
{
    if (config.id != "" && config.id != key)
        throw MLDB::Exception("attempt to put with a different key than created with");
    config.id = key;
}

CredentialRuleStatus
CredentialRuleCollection::
getStatusLoading(std::string key, const BackgroundTask & task) const
{
    return CredentialRuleStatus();
}

CredentialRuleStatus
CredentialRuleCollection::
getStatusFinished(std::string key, const CredentialRule & value) const
{
    CredentialRuleStatus status;
    status.stored = *value.config->store;
    status.stored.credential = Credential();
    status.stored.credential.secret = "<<credentials removed>>";
    return status;
}

std::shared_ptr<CredentialRule>
CredentialRuleCollection::
construct(CredentialRuleConfig config, const OnProgress & onProgress) const
{
    return std::make_shared<CredentialRule>(config);
}

std::shared_ptr<CredentialRuleConfig>
CredentialRuleCollection::
getConfig(std::string key, const CredentialRule & value) const
{
    return value.config;
}


std::shared_ptr<CredentialRuleCollection>
createCredentialCollection(MLDB::MldbServer * server, RestRouteManager & routeManager,
                           std::shared_ptr<CollectionConfigStore> configStore) {

    auto result = std::make_shared<CredentialRuleCollection>(server);

    // if the CollectionConfigStore is valid
    // ensure that credentials are persisted
    if (configStore) {
        result->attachConfig(configStore);
        result->loadConfig();
    }

    auto getCollection = [=] (const RestRequestParsingContext & context)
        {
            return result.get();
        };

    auto collectionRouteManager
        = std::make_shared<typename CredentialRuleCollection::RouteManager>
        (routeManager, *routeManager.parentNode,
         routeManager.resourceElementsMatched + 2,
         getCollection, "credential", "credentials");
    CredentialRuleCollection::initRoutes(*collectionRouteManager);

    // Save our child route
    routeManager.childRoutes["credentials"] = collectionRouteManager;

    server->addEntity("credentials", *result);

    CredentialProvider::
        registerProvider(std::make_shared<CollectionCredentialProvider>(result));
    return result;
}

template class RestCollection<std::string, CredentialRule>;

} // namespace MLDB


