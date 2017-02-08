// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** collection_config_store.cc
    Jeremy Barnes, 8 March 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "mldb/rest/collection_config_store.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/base/exc_assert.h"
#include "mldb/vfs/filter_streams.h"


using namespace std;


namespace MLDB {

/*****************************************************************************/
/* COLLECTION CONFIG STORE                                                   */
/*****************************************************************************/

CollectionConfigStore::
~CollectionConfigStore()
{
}


/*****************************************************************************/
/* S3 COLLECTION CONFIG STORE                                                */
/*****************************************************************************/

S3CollectionConfigStore::
S3CollectionConfigStore(const std::string & baseUri)
{
    init(baseUri);
}

S3CollectionConfigStore::
~S3CollectionConfigStore()
{
}

void
S3CollectionConfigStore::
init(const std::string & baseUri)
{
    if (baseUri.empty())
        throw MLDB::Exception("can't do empty uri");

    // Strip off a trailing slash
    if (baseUri[baseUri.size() - 1] == '/')
        this->baseUri = string(baseUri, 0, baseUri.size() - 1);
    else this->baseUri = baseUri;

    makeUriDirectory(baseUri + "/");
}

std::vector<Utf8String>
S3CollectionConfigStore::
keys() const
{
    vector<Utf8String> result;

    auto onFile = [&] (const std::string & uri_,
                       const FsObjectInfo & info,
                       const OpenUriObject & open,
                       int depth)
        {
            Utf8String uri(uri_);

            if (!uri.startsWith(baseUri)) {
                cerr << "uri = " << uri << endl;
                cerr << "baseUri = " << baseUri << endl;
            }
            ExcAssert(uri.startsWith(baseUri));
            // Strip off the prefix and the / delimiter
            result.emplace_back(uri, baseUri.size() + 1);
            return true;
        };

    auto onDir = [&] (const Utf8String & dirName,
                      int depth)
        {
            return false;
        };

    forEachUriObject(baseUri + "/", onFile, onDir);

    return result;
}

void
S3CollectionConfigStore::
set(Utf8String key, const Json::Value & config)
{
    filter_ostream stream((baseUri + "/" + key).rawString());
    stream << config;
}

Json::Value
S3CollectionConfigStore::
get(Utf8String key) const
{
    filter_istream stream((baseUri + "/" + key).rawString());
    Json::Value val;
    stream >> val;
    return val;
}

std::vector<std::pair<Utf8String, Json::Value> >
S3CollectionConfigStore::
getAll() const
{
    vector<pair<Utf8String, Json::Value> > result;
    for (const Utf8String & key: keys())
        result.push_back(make_pair(key, get(key)));
    return result;
}

void
S3CollectionConfigStore::
clear()
{
    for (const Utf8String & key: keys())
        erase(key);
}

void
S3CollectionConfigStore::
erase(Utf8String key)
{
    tryEraseUriObject((baseUri + "/" + key).rawString());
}

} // namespace MLDB
