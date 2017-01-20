// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* etcd_client.cc
   Jeremy Barnes, 4 March 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "etcd_client.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/pointer_description.h"
#include <chrono>
#include <thread>
#include <curl/curl.h>

using namespace std;

namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(EtcdResponse);

EtcdResponseDescription::
EtcdResponseDescription()
{
    addField("action", &EtcdResponse::action,
             "Action that was successfully finished");
    addField("node", &EtcdResponse::node,
             "Node that was returned");
    addField("prevNode", &EtcdResponse::prevNode,
             "Previous node returned");
    addField("cause", &EtcdResponse::cause,
             "Cause of error");
    addField("errorCode", &EtcdResponse::errorCode,
             "Error code of response", 0);
    addField("index", &EtcdResponse::index,
             "Index value as at operation");
    addField("message", &EtcdResponse::message,
             "Error message");
}

DEFINE_STRUCTURE_DESCRIPTION(EtcdNode);

EtcdNodeDescription::
EtcdNodeDescription()
{
    addField("createdIndex", &EtcdNode::createdIndex,
             "Index at which node was created", int64_t(0));
    addField("modifiedIndex", &EtcdNode::modifiedIndex,
             "Index at which node was modified", int64_t(0));
    addField("dir", &EtcdNode::dir,
             "This node is a directory", false);
    addField("key", &EtcdNode::key,
             "Key of node");
    addField("value", &EtcdNode::value,
             "Value of node");
    addField("nodes", &EtcdNode::nodes,
             "Child nodes if directory");
    addField("expiration", &EtcdNode::expiration,
             "Date at which node expires", Date::positiveInfinity());
    addField("ttl", &EtcdNode::ttl,
             "TIme to live of node in seconds", -1);
}

EtcdResponse
EtcdClient::
set(const std::string & key,
    const std::string & val)
{
    RestParams form;
    form.push_back({ "value", val });

    for (unsigned i = 0;;  ++i) {

        try {
            auto res = proxy.put("/v2/keys/" + keyPath(key), form,
                                 {}, {}, timeout);
            return extractResponse(res);
        } catch (const std::exception & exc) {
            if (i == retries - 1)
                throw;
            cerr << "retrying etcd operation" << endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(
                int(retryDelay * 1000)));
        }
    }
}

EtcdResponse
EtcdClient::
setIfNotPresent(const std::string & key,
                const std::string & val)
{
    RestParams form;
    form.push_back({ "value", val });
    form.push_back({ "prevExist", "false" });
    
    for (unsigned i = 0;;  ++i) {
        
        try {
            //cerr << "setting key " << keyPath(key) << " to " << val << endl;
            auto res = proxy.put("/v2/keys/" + keyPath(key), form,
                                 {}, {}, timeout);
            return extractResponse(res);
        } catch (const std::exception & exc) {
            if (i == retries - 1)
                throw;
            cerr << "retrying etcd operation" << endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(
                int(retryDelay * 1000)));
        }
    }
}
    
EtcdResponse
EtcdClient::
get(const std::string & key)
{
    for (unsigned i = 0;;  ++i) {

        try {
            auto resp = proxy.get("/v2/keys/" + keyPath(key),
                                  {}, {}, timeout);
            return extractResponse(resp);

        } catch (const std::exception & exc) {
            if (i == retries - 1)
                throw;
            cerr << "retrying etcd operation" << endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(
                int(retryDelay * 1000)));
        }
    }
}

EtcdResponse
EtcdClient::
createDir(const std::string & key,
          int ttl)
{
    RestParams form = { { "dir", "true" } };
    if (ttl > 0)
        form.push_back( { "ttl", std::to_string(ttl) } );

    for (unsigned i = 0;;  ++i) {

        //cerr << "path is " << ("/v2/keys/" + keyPath(key)) << endl;

        try {
            auto resp = proxy.put("/v2/keys/" + keyPath(key), form,
                                  {}, {}, timeout);

#if 0
            using namespace std;
            if (resp.code() != 201 && resp.code() != 200) {
                cerr << "createDir returned wrong code: "
                     << resp << endl;
                throw MLDB::Exception("createDir");
            }
#endif
            return extractResponse(resp);
        } catch (const std::exception & exc) {
            if (i == retries - 1)
                throw;
            cerr << "retrying etcd operation" << endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(
                int(retryDelay * 1000)));
        }
    }
}

EtcdResponse
EtcdClient::
refreshDir(const std::string & key,
           int ttl)
{
    RestParams form = { { "dir", "true" },
                        { "prevExist", "true" } };
    if (ttl > 0)
        form.push_back( { "ttl", std::to_string(ttl) } );
        
    for (unsigned i = 0;;  ++i) {

        try {
            auto resp = proxy.put("/v2/keys/" + keyPath(key), form,
                                  {}, {}, timeout);
            return extractResponse(resp);
        } catch (const std::exception & exc) {
            if (i == retries - 1)
                throw;
            cerr << "retrying etcd operation" << endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(
                int(retryDelay * 1000)));
        }
    }
}
    
EtcdResponse
EtcdClient::
listDir(const std::string & key, bool recursive)
{
    RestParams queryParams;
    if (recursive)
        queryParams.push_back( { "recursive", "true" } );

    for (unsigned i = 0;;  ++i) {

        try {
            auto resp = proxy.get("/v2/keys/" + keyPath(key) + (keyPath(key).empty() ? "" : "/"),
                                  queryParams);

#if 0
            static std::mutex respMutex;
            std::unique_lock<std::mutex> guard(respMutex);
            cerr << "--- listdir response for " << key << " " << recursive << endl;
            cerr << resp.jsonBody() << endl;
#endif

            auto response = extractResponse(resp);

            if (response.errorCode == 100)
                return response;

            if (!response.node)
                throw MLDB::Exception("listDir didn't return node: %d %s",
                                    resp.code(), resp.body().c_str());

            return response;
        } catch (const std::exception & exc) {
            if (i == retries - 1)
                throw;
            cerr << "retrying etcd operation" << endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(
                int(retryDelay * 1000)));
        }
    }
}

EtcdResponse
EtcdClient::
erase(const std::string & key)
{
    for (unsigned i = 0;;  ++i) {

        try {
            auto resp = proxy.perform("DELETE",
                                      "/v2/keys/" + keyPath(key) + (keyPath(key).empty() ? "" : "/"),
                                      HttpRestProxy::Content(),
                                      {}, {}, timeout);
        
            auto response = extractResponse(resp);
            return response;
        } catch (const std::exception & exc) {
            if (i == retries - 1)
                throw;
            cerr << "retrying etcd operation" << endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(
                int(retryDelay * 1000)));
        }
    }
}

EtcdResponse
EtcdClient::
eraseDir(const std::string & key, bool recursive)
{
    for (unsigned i = 0;;  ++i) {

        try {
            auto resp = proxy.perform("DELETE",
                                      "/v2/keys/" + keyPath(key) + (keyPath(key).empty() ? "" : "/"),
                                      HttpRestProxy::Content(),
                                      { { "dir", "true" },
                                              { "recursive", recursive ? "true" : "false" } },
                                      {}, timeout);
            
            //cerr << "erase dir resp = " << resp << endl;
        
            auto response = extractResponse(resp);
            return response;
        } catch (const std::exception & exc) {
            if (i == retries - 1)
                throw;
            cerr << "retrying etcd operation" << endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(
                int(retryDelay * 1000)));
        }
    }
}

EtcdResponse
EtcdClient::
watch(const std::string & key,
      bool recursive,
      double timeout,
      int64_t prevIndex)
{
    RestParams queryParams = { { "wait", "true" } };
    if (recursive)
        queryParams.push_back( { "recursive", "true" } );
    if (prevIndex != -1)
        queryParams.push_back( { "waitIndex", std::to_string(prevIndex) } );

    for (unsigned i = 0;;  ++i) {

        auto resp = proxy.get("/v2/keys/" + keyPath(key) + (keyPath(key).empty() ? "" : "/"),
                              queryParams,
                              {}, timeout, false /* exceptions */);
        
        if (resp.errorCode() == CURLE_OPERATION_TIMEDOUT)
            return EtcdResponse();
        if (resp.errorCode()) {
            if (i == retries - 1)
                throw MLDB::Exception(resp.errorMessage());
            cerr << "retrying etcd operation" << endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(
                int(retryDelay * 1000)));
        }

        return extractResponse(resp);
    }
}

EtcdResponse
EtcdClient::
extractResponse(const HttpRestProxy::Response & resp)
{
    //cerr << "extracting from " << resp << endl;
    auto response = jsonDecodeStr<EtcdResponse>(resp.body());
    if (response.index == 0) {
        //cerr << "header = " << resp.getHeader("x-etcd-index") << endl;
        response.index = jsonDecodeStr<uint64_t>(resp.getHeader("x-etcd-index"));
    }
    return response;
}

} // namespace MLDB
