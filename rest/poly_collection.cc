// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** poly_collection.cc                                             -*- C++ -*-
    Jeremy Barnes, 22 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Implementation of a polymorphic collection.
*/

#include "poly_collection.h"
#include "poly_collection_impl.h"
#include "mldb/rest/rest_collection_impl.h"
#include "mldb/types/structure_description.h"
#include "mldb/watch/watch_impl.h"
#include "mldb/utils/json_utils.h"


using namespace std;


namespace MLDB {


/*****************************************************************************/
/* PLUGIN                                                                    */
/*****************************************************************************/

bool operator==(const PolyConfig & lhs, const PolyConfig & rhs)
{
    return  lhs.id == rhs.id &&
        lhs.type == rhs.type &&
        lhs.persistent == rhs.persistent &&
        lhs.params == rhs.params &&
        lhs.deterministic == rhs.deterministic;
}

DEFINE_STRUCTURE_DESCRIPTION(PolyConfig);

PolyConfigDescription::
PolyConfigDescription()
{
    addField("id", &PolyConfig::id,
             "ID of the entity.  This indicates what name the entity will "
             "be called when accessed via the REST API.");
    addField("type", &PolyConfig::type,
             "Type of the entity.  This indicates what implementation type "
             "to use for the entity.");
    addFieldDesc("params", &PolyConfig::params,
                 "Entity configuration parameters.  This is always an object, and "
                 "the type of object accepted depends upon the 'type' field.",
                 getBareAnyDescription());
    addField("persistent", &PolyConfig::persistent,
             "If true, then this element will have its configuration stored "
             "and will be reloaded on startup", false);
    addField("deterministic", &PolyConfig::deterministic,
             "If true, the entity has no hidden state ", true);
}


DEFINE_STRUCTURE_DESCRIPTION(PolyStatus);

PolyStatusDescription::
PolyStatusDescription()
{
    nullAccepted = true;
    
    addField("id", &PolyStatus::id,
             "ID of entity");
    addField("state", &PolyStatus::state,
             "State of entity");
    addField("type", &PolyStatus::type,
             "Type of entity");
    addField("progress", &PolyStatus::progress,
             "Progress of construction of entity");
    addField("config", &PolyStatus::config,
             "Configuration of the entity");
    addFieldDesc("status", &PolyStatus::status,
                 "Status of the entity",
                 getBareAnyDescription());
}


/*****************************************************************************/
/* POLY COLLECTION                                                           */
/*****************************************************************************/

PolyCollectionBase::
PolyCollectionBase(const Utf8String & nounSingular,
                   const Utf8String & nounPlural,
                   RestDirectory * server)
    : RestConfigurableCollection<Utf8String, PolyEntity,
                                 PolyConfig, PolyStatus>
      (nounSingular, nounPlural, server),
      server(server)
{
}

PolyCollectionBase::
~PolyCollectionBase()
{
    shutdown();
}

void
PolyCollectionBase::
initRoutes(RouteManager & manager)
{
    RestConfigurableCollection<Utf8String, PolyEntity,
                               PolyConfig, PolyStatus>
        ::initRoutes(manager);

    ExcAssert(manager.getKey);

    manager.addPutRoute();
    manager.addPostRoute();
    manager.addDeleteRoute();
}

void
PolyCollectionBase::
init(std::shared_ptr<CollectionConfigStore> configStore)
{
    if (configStore)
        this->attachConfig(configStore);
}

Utf8String
PolyCollectionBase::
getKey(PolyConfig & config)
{
    if (!config.id.empty())
        return config.id;

    // Add a disambiguating element to distinguish between different things that
    // try to get the same key.
    // 1.  Newly seeded random number based on current time
    // 2.  Thread ID
    Utf8String disambig
        = MLDB::format("%d-%d", random())
        + Date::now().print(9)
        + std::to_string(std::hash<std::thread::id>()(std::this_thread::get_id()));
    
    // Create an auto hash that is cleary identified as one
    return config.id = MLDB::format("auto_%016llx_%016llx",
                                  (unsigned long long)jsonHash(jsonEncode(config)),
                                  (unsigned long long)jsonHash(disambig));
}

void
PolyCollectionBase::
setKey(PolyConfig & config, Utf8String key)
{
    if (!config.id.empty() && config.id != key) {
        throw HttpReturnException(400,
                                  "Ambiguous names between route and config "
                                  "for PUT to collection '" + nounPlural + "'.  "
                                  "In the resource of the PUT request, "
                                  "the entity is called '" + key + "' but in the "
                                  ".id field of the config, the entity is called '"
                                  + config.id + "'.  Either the two must match, "
                                  "or the 'id' value in the config must be empty.",
                                  "valueInUri", key,
                                  "valueInConfig", config.id,
                                  "config", config,
                                  "collection", nounPlural);
    }
    config.id = key;
}

PolyStatus
PolyCollectionBase::
getStatusLoading(Utf8String key, const BackgroundTask & task) const
{
    PolyStatus result;
    result.id = key;
    result.state = task.getState();
    result.progress = task.getProgress();

    auto realConfig = task.config.convert<PolyConfig>();

    result.type = realConfig.type;
    result.config = std::make_shared<PolyConfig>(std::move(realConfig));
    return result;
}

std::shared_ptr<PolyConfig>
PolyCollectionBase::
getConfig(Utf8String key, const PolyEntity & value) const
{
    return value.config_;
}

bool
PolyCollectionBase::
objectIsPersistent(const Utf8String & key, const PolyConfig & config) const
{
    return config.persistent;
}

DEFINE_REST_COLLECTION_INSTANTIATIONS(Utf8String, PolyEntity, PolyConfig, PolyStatus);

template class WatchT<PolyCollectionBase::ChildEvent>;

template class WatchT<Utf8String>;
template class WatchesT<Utf8String>;


} // namespace MLDB
