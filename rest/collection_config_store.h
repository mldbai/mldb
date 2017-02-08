// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** collection_config_store.h                                      -*- C++ -*-

    Store for configurations of collections to live.
*/

#pragma once

#include <vector>
#include <string>
#include "mldb/ext/jsoncpp/json.h"

namespace MLDB {

/*****************************************************************************/
/* COLLECTION CONFIG STORE                                                   */
/*****************************************************************************/

/** A place to store configuration permanently. */

struct CollectionConfigStore {
    virtual ~CollectionConfigStore();
    virtual std::vector<Utf8String> keys() const = 0;
    virtual void set(Utf8String key, const Json::Value & config) = 0;
    virtual Json::Value get(Utf8String key) const = 0;
    virtual std::vector<std::pair<Utf8String, Json::Value> > getAll() const = 0;
    virtual void clear() = 0;
    virtual void erase(Utf8String key) = 0;
};


/*****************************************************************************/
/* S3 COLLECTION CONFIG STORE                                                */
/*****************************************************************************/

/** Store configuration of a collection of objects in S3. */

struct S3CollectionConfigStore : public CollectionConfigStore {
    S3CollectionConfigStore()
    {
    }

    S3CollectionConfigStore(const std::string & baseUri);

    void init(const std::string & baseUri);

    virtual ~S3CollectionConfigStore();
    virtual std::vector<Utf8String> keys() const;
    virtual void set(Utf8String key, const Json::Value & config);
    virtual Json::Value get(Utf8String key) const;
    virtual std::vector<std::pair<Utf8String, Json::Value> > getAll() const;
    virtual void clear();
    virtual void erase(Utf8String key);

    std::string baseUri;  // UTF-8 encoded
};





} // namespace MLDB
