/* etcd_client.h                                                   -*- C++ -*-
   Jeremy Barnes, 4 March 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Client for etcd.
*/

#pragma once

#include "mldb/types/value_description_fwd.h"
#include "mldb/types/date.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/arch/exception.h"

namespace MLDB {

struct EtcdNode {
    EtcdNode()
        : createdIndex(0), modifiedIndex(0), dir(false),
          expiration(Date::positiveInfinity()), ttl(-1)
    {
    }

    int64_t createdIndex;
    int64_t modifiedIndex;
    bool dir;
    std::string key;
    std::string value;
    std::vector<EtcdNode> nodes;
    Date expiration;
    int ttl;
};

DECLARE_STRUCTURE_DESCRIPTION(EtcdNode);

struct EtcdResponse {
    EtcdResponse()
        : errorCode(0), index(0)
    {
    }

    // These are for a successful action
    std::string action;
    std::shared_ptr<EtcdNode> node;
    std::shared_ptr<EtcdNode> prevNode;

    // These are for an error
    std::string cause;
    int errorCode;
    int64_t index;
    std::string message;
};

DECLARE_STRUCTURE_DESCRIPTION(EtcdResponse);



/*****************************************************************************/
/* ETCD CLIENT                                                               */
/*****************************************************************************/

struct EtcdClient {

    EtcdClient()
    {
        timeout = 1.0;
        retries = 3;
        retryDelay = 0.1;
    }
    
    EtcdClient(const std::string & uri,
               std::string basePath)
    {
        timeout = 1.0;
        retries = 3;
        retryDelay = 0.1;

        connect(uri, basePath);
    }

    void connect(std::string uri,
                 std::string basePath = "")
    {
        if (uri.empty())
            throw MLDB::Exception("URI is empty");
        if (uri[uri.size() - 1] == '/')
            uri = std::string(uri, 0, uri.size() - 1);
        proxy.init(uri);
        if (!basePath.empty() && basePath[basePath.size() - 1] == '/')
            basePath = std::string(basePath, 0, basePath.size() - 1);
        this->basePath = basePath;
    }

    double timeout;
    int retries;
    double retryDelay;

    EtcdResponse set(const std::string & key,
                     const std::string & val);
    
    EtcdResponse setIfNotPresent(const std::string & key,
                                 const std::string & val);
    
    EtcdResponse get(const std::string & key);

    EtcdResponse createDir(const std::string & key,
                           int ttl = 0);
    
    EtcdResponse refreshDir(const std::string & key,
                            int ttl = 0);

    EtcdResponse erase(const std::string & key);

    EtcdResponse eraseDir(const std::string & key, bool recursive = true);
    
    EtcdResponse listDir(const std::string & key, bool recursive = false);

    std::string keyPath(const std::string & key)
    {
        if (key == "")
            return basePath;
        if (basePath == "")
            return key;
        return basePath + "/" + key;
    }

    EtcdResponse watch(const std::string & key,
                       bool recursive = false,
                       double timeout = -1,
                       int64_t prevIndex = -1);

    static EtcdResponse
    extractResponse(const HttpRestProxy::Response & response);

    HttpRestProxy proxy;
    std::string basePath;
};

} // namespace MLDB
