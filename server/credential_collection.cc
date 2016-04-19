/** credential_collection.cc
    Jeremy Barnes, 11 November 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

     This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#include "credential_collection.h"
#include "mldb/server/mldb_server.h"
#include "mldb/rest/rest_collection_impl.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/pointer_description.h"
#include "mldb/soa/credentials/credential_provider.h"
#include "mldb/types/id.h"
#include "mldb/utils/log.h"
#include <signal.h>

using namespace std;


namespace Datacratic {

namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(StoredCredentials);

StoredCredentialsDescription::
StoredCredentialsDescription()
{
    addField("resourceType", &StoredCredentials::resourceType,
             "Type of resource that this credentials rule applies to.  This is "
             "matched by checking that the prefix matches the resource.  So a "
             "rule with a resourceType of 'aws' will match a resource request "
             "for a resource type of 'aws' and 'aws:s3', but not 'aw'.  If this "
             "field is empty, it will match all resource types.");
    addField("resource", &StoredCredentials::resource,
             "Resource that this credentials rule applies to.  Again, this is "
             "a prefix match.  If this is empty, it will match all resources.");
    addField("role", &StoredCredentials::role,
             "Role to match.  This is currently unused.");
    addField("operation", &StoredCredentials::operation,
             "Operation to perform on the credentials.  This is currently unused.");
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
/* COLLECITON CREDENTIAL PROVIDER                                            */
/*****************************************************************************/
struct CollectionCredentialProvider: public CredentialProvider {

    CollectionCredentialProvider(const std::shared_ptr<CredentialRuleCollection> rules)
        : rules(rules)
    {

    }

    std::shared_ptr<CredentialRuleCollection> rules;

    virtual std::vector<std::string>
    getResourceTypePrefixes() const
    {
        auto keys =  rules->getKeys();
        std::vector<std::string> result;
        for (const auto & key : keys) {
            auto cred = rules->tryGetEntry(key);
            ExcAssert(!cred.second); // still under construction
            if (cred.first)
                result.push_back(cred.first->config->store->resourceType);
        }
        return result;
    }

    virtual std::vector<Credential>
    getSync(const std::string & resourceType,
            const std::string & resource,
            const CredentialContext & context,
            Json::Value extraData) const
    {
        vector<Credential> result;
        auto keys =  rules->getKeys();
        for (const auto & key : keys) {
            auto cred = rules->tryGetEntry(key);
            ExcAssert(!cred.second); // still under construction
            if (cred.first) {
                auto stored = cred.first->config->store;
                if (resourceType != stored->resourceType)
                    continue;
                if (resource.find(stored->resource) != 0)
                    continue;
                result.push_back(stored->credential);
            }
        }
        return result;
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
    if (config.id == "")
        return ML::format("%016llx", (unsigned long long)Id(jsonEncodeStr(config)).hash());
    return config.id;
}

void
CredentialRuleCollection::
setKey(CredentialRuleConfig & config, std::string key)
{
    if (config.id != "" && config.id != key)
        throw ML::Exception("attempt to put with a different key than created with");
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

    CredentialProvider::registerProvider("collectionCredentials",
                                         std::make_shared<CollectionCredentialProvider>(result));
    return result;
}

/*****************************************************************************/
/* CREDENTIAL DAEMON                                                         */
/*****************************************************************************/
#if 0
CredentialsDaemon::
CredentialsDaemon()
    : EventRecorder("", nullptr),
      RestDirectory(this, "root"),
      rules(this),
      logger(MLDB::getMldbLog<CredentialsDaemon>())
{
}

CredentialsDaemon::
~CredentialsDaemon()
{
}

void
CredentialsDaemon::
init(std::shared_ptr<CollectionConfigStore> configStore)
{
    RestServiceEndpoint::init();

    onHandleRequest = router.requestHandler();

    router.description = "Datacratic Credentials Daemon";

    router.addHelpRoute("/", "GET");

    RestRequestRouter::OnProcessRequest serviceInfoRoute
        = [=] (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context) {
        Json::Value result;
        result["apiVersions"]["v1"] = "1.0.0";
        connection.sendResponse(200, result);
        return RestRequestRouter::MR_YES;
    };

    router.addRoute("/info", "GET", "Return service information (version, etc)",
                    serviceInfoRoute,
                    Json::Value());

    // Push our this pointer in to make sure that it's available to sub
    // routes
    auto addObject = [=] (RestConnection & connection,
                          const RestRequest & request,
                          RestRequestParsingContext & context)
        {
            context.addObject(this);
        };

    auto & versionNode = router.addSubRouter("/v1", "version 1 of API",
                                             addObject);

    RestRequestRouter::OnProcessRequest handleShutdown
        = [=] (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context) {

        kill(getpid(), SIGUSR2);

        Json::Value result;
        result["shutdown"] = true;
        connection.sendResponse(200, result);
        return RestRequestRouter::MR_YES;
    };

    versionNode.addRoute("/shutdown", "POST", "Shutdown the service",
                         handleShutdown,
                         Json::Value());


    // If we want persistent rules, then attach the config store
    if (configStore) {
        logger->debug() << "Attaching config store";
        rules.attachConfig(configStore);
        rules.loadConfig();
    }

    rules.init(versionNode);

    if (false) {
        logRequest = [&] (const ConnectionId & conn, const RestRequest & req)
            {
                this->recordHit("rest.request.count");
                this->recordHit("rest.request.verbs.%s", req.verb.c_str());
            };

        logResponse = [&] (const ConnectionId & conn,
                           int code,
                           const std::string & resp,
                           const std::string & contentType)
            {
                double processingTimeMs
                = Date::now().secondsSince(conn.itl->startDate) * 1000.0;
                this->recordOutcome(processingTimeMs,
                                    "rest.response.processingTimeMs");
                this->recordHit("rest.response.codes.%d", code);
            };
    }

    addEntity("rules", rules);

    auto getRuleCollection = [=] (const RestRequestParsingContext & context)
        {
            return &rules;
        };

    routeManager.reset(new RestRouteManager(versionNode,
                                            1 /* path length: /v1 */));
    (*routeManager).addChild<CredentialRuleCollection>
        ("rules", versionNode,
         3 /* path length: /v1/rules/<ruleName> */,
         getRuleCollection, "rule", "rules");
#if 0
    {
        auto collectionRouteManager
            = std::make_shared<CredentialRuleCollection::RouteManager>
            (versionNode, getRulesCollection, "rule", "rules");
        CredentialRuleCollection::initRoutes(*collectionRouteManager);

        // Save our child route
        routeManager->childRoutes["rules"] = collectionRouteManager;
    }
#endif

    auto & typesNode = versionNode.addSubRouter("/types", "Node for resource types");

    auto & typeNode = typesNode.addSubRouter(Rx("/([a-zA-Z0-9:]*)", "/<resourceType>"),
                                             "Node for resources of a type");

    RequestParam<std::string> typeParam(3, "/<resourceType>", "Type of resource");

    auto & resourcesNode = typeNode.addSubRouter("/resources", "Node for specific resources");

    //auto & resourceNode = resourcesNode.addSubRouter(Rx("/(.*)/credentials", "/<resource>"),
    //                                                 "Node for a given resource");

    RequestParam<std::string> resourceParam(6, "<resource>", "Name of resource");

    addRouteSyncJsonReturn(resourcesNode,
                           Rx("/(.*)/credentials", "/<resource>/credentials"),
                           {"GET"},
                           "Get credentials for the given resource",
                           "Credentials for the resource",
                           &CredentialsDaemon::getCredentials,
                           this,
                           typeParam,
                           resourceParam,
                           RestParamDefault<string>("role", "Role required", ""),
                           RestParamDefault<string>("operation", "Operations required", ""),
                           RestParamDefault<TimePeriod>("validity",
                                                        "Time credential is needed for",
                                                        "1h"),
                           RestParamDefault<Json::Value>("extra", "Extra parameters", Json::Value()));

}

std::vector<Credential>
CredentialsDaemon::
getCredentials(const std::string & resourceType,
               const std::string & resource,
               const std::string & role,
               const std::string & operation,
               const TimePeriod & validity,
               const Json::Value & extra)
{
    logger->debug() << "getCredentials: resourceType " << resourceType
                    << " resource " << resource
                    << " role " << role
                    << " operation " << operation
                    << " validity " << validity.toString()
                    << " extra " << extra;

    std::vector<Credential> result;

    auto onEntry = [&] (const std::string & ruleName,
                        const CredentialRule & rule)
        {
            logger->info() << "attempting to match rule " << rule.config->id;

            if (rule.config->store) {
                if (resourceType.find(rule.config->store->resourceType) != 0) {
                    logger->info() << "failed to match on resource type "
                                   << rule.config->store->resourceType;
                    return true;
                }
                if (resource.find(rule.config->store->resource) != 0) {
                    logger->info() << "failed to match on resource "
                                   << rule.config->store->resource;
                    return true;
                }
                logger->info() << "matched rule " << rule.config->id;
                result.emplace_back(rule.config->store->credential);
                return true;
            }

            logger->info() << "failed to matched rule " << rule.config->id;
            return true;
        };

    rules.forEachEntry(onEntry);

    return result;
}

std::string
CredentialsDaemon::
bindTcp(const PortRange & portRange,
        const std::string & host)
{
    return httpEndpoint.bindTcp(portRange, host);
}

Utf8String
CredentialsDaemon::
getUriForPath(ResourcePath path)
{
    Utf8String result = "/v1";
    for (auto & e: path)
        result += "/" + e;
    return result;
}
#endif

} // namespace MLDB

} // namespace datacratic
