/** poly_entity.h                                                  -*- C++ -*-
    Jeremy Barnes, 22 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Polymorphic entity.  All entities in MLDB derive from this class, which
    provides a root for the type hierarchy.
*/

#pragma once

#include "mldb/rest/rest_request_fwd.h"
#include "mldb/types/any.h"
#include "mldb/ext/jsoncpp/value.h"
#include "mldb/base/exc_assert.h"

namespace MLDB {

/*****************************************************************************/
/* PACKAGE                                                                   */
/*****************************************************************************/

/** Entity that describes a package of functionality that is delivered
    into MLDB via a plugin.  Each new type has to belong to a
    package.
*/

struct Package {
    Package()
    {
    }

    Package(Utf8String packageName)
        : packageName_(std::move(packageName))
    {
    }

    const Utf8String & packageName() const
    {
        return packageName_;
    }

private:
    Utf8String packageName_;
};


/*****************************************************************************/
/* ENTITY TYPE                                                               */
/*****************************************************************************/

/** This represents an entity type, which is a factory for entities that
    can be constructed from a configuration and a MLDB instance.
*/

template<typename Entity>
struct EntityType {
};


struct RestDirectory;


/*****************************************************************************/
/* POLY CONFIG                                                               */
/*****************************************************************************/

/** Common class for the configuration of an entity.  This is a fully
    generic object from which any type can be constructed.  The params
    are interpreted depending upon the type.
*/

struct PolyConfig {
    PolyConfig()
        : persistent(false),
          deterministic(true)
    {
    }

    Utf8String id;        ///< Id (name) of the entity.  Must be unique
    Utf8String type;      ///< Type of the entity.
    bool persistent;      ///< Save this object's configuration for loading
    bool deterministic;   ///< The entity has no hidden state
    Any params;           ///< Creation parameters, per type
};

bool operator==(const PolyConfig & lhs, const PolyConfig & rhs);

DECLARE_STRUCTURE_DESCRIPTION(PolyConfig);


/*****************************************************************************/
/* POLY STATUS                                                               */
/*****************************************************************************/

/** Common class for the status of an entity. */

struct PolyStatus {
    Utf8String id;
    Utf8String state;
    Json::Value progress;
    std::shared_ptr<PolyConfig> config;
    Utf8String type;
    Any status;
};

DECLARE_STRUCTURE_DESCRIPTION(PolyStatus);


/*****************************************************************************/
/* POLY ENTITY                                                               */
/*****************************************************************************/


/** Base class of things that live in polymorphic containers.  Only exists
    because we can't take a reference to a void.
*/
struct PolyEntity {
    virtual ~PolyEntity()
    {
    }

    std::shared_ptr<const PolyConfig> getConfigPtr() const
    {
        return config_;
    }

    const PolyConfig & getConfig() const
    {
        if (!config_)
            throw MLDB::Exception("Entity has no configuration");
        return *config_;
    }

    const Utf8String & getId() const
    {
        if (!config_)
            throw MLDB::Exception("Entity has no configuration");
        return config_->id;
    }

    const Utf8String & getType() const
    {
        if (!config_)
            throw MLDB::Exception("Entity has no configuration");
        return config_->type;
    }

    std::shared_ptr<PolyConfig> config_;
    Utf8String type_;
};

template<typename Entity>
struct PolyConfigT: public PolyConfig {
    void operator = (const PolyConfig & config)
    {
        *(PolyConfig *)this = config;
    }

    PolyConfigT<Entity> & withId(Utf8String id) {
        this->id = id;
        return *this;
    }

    PolyConfigT<Entity> & withType(Utf8String type) {
        this->type = type;
        return *this;
    }
};

template<typename Entity>
bool operator==(const PolyConfigT<Entity> & lhs, const PolyConfigT<Entity> & rhs)
{
    return ((const PolyConfig &)lhs == (const PolyConfig &)rhs);
}

/** Type of a function used to handle a custom route on the type
    collection.
*/
typedef std::function<RestRequestMatchResult (RestDirectory *, RestConnection &, const RestRequest &, RestRequestParsingContext &)> TypeCustomRouteHandler;


} // namespace MLDB
