/* mldb_server.h                                                   -*- C++ -*-
   Jeremy Barnes, 12 December 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Server class for MLDB.
*/

#pragma once

#include "mldb/rest/service_peer.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "mldb/types/string.h"
#include "mldb/soa/service/event_service.h"
#include "mldb/utils/log_fwd.h"


namespace MLDB {

struct PolyConfig;
struct Utf8String;
struct Package;


struct PluginCollection;
struct DatasetCollection;
struct ProcedureCollection;
struct FunctionCollection;
struct CredentialRuleCollection;
struct TypeClassCollection;

struct Plugin;
struct Dataset;
struct Procedure;
struct Function;
struct CredentialRule;

struct MatrixNamedRow;


/*****************************************************************************/
/* MLDB SERVER                                                               */
/*****************************************************************************/

/** Server that runs MLDB.  This holds all of the collections of entities,
    the utility functions they require, and handles REST requests.
*/

struct MldbServer: public ServicePeer, public EventRecorder {

    MldbServer(const std::string & serviceName = "mldb",
               const std::string & etcdUri = "",
               const std::string & etcdPath = "",
               bool enableAccessLog = false,
               const std::string & httpBaseUrl = "");
    ~MldbServer();

    /** Scan the given directory for plugins.  These are not loaded;
        their metadata is simply parsed and a dependency graph
        created.  This allows them to be initialized in an order
        which allows for dependencies to be present later on.

        Note that dir includes a full URL; ie it requires file://
        in order to load from the local filesystem.

        It may be called multiple times.

        There are two possibilities for dir:

        1.  It has no mldb_plugin.json file, in which case it is
            assumed to be a directory full of plugins and each
            subdirectory will be scanned recursively.
        2.  It has a mldb_plugin.json file, in which case the
            plugin will be loaded from that directory.
    */
    void scanPlugins(const std::string & dir);

    /** Set up the SSD cache directory, where files that need memory
        mapping can be cached.
    */
    void setCacheDirectory(const std::string & dir);

    /** Initialize the server in standalone mode, with the given
        configuration path.  No remote
        discovery or message passing is supported in this configuration.
    */
    bool init(std::string configurationPath = "",
              std::string staticFilesPath = "file://mldb/container_files/public_html/resources",
              std::string staticDocPath = "file://mldb/container_files/public_html/doc",
              bool hideInternalEntities = false);

    void start();

    void shutdown();

    typedef std::function<bool (const Json::Value & progress)> OnProgress;

    /** Obtain the dataset with the given configuration. */

    std::shared_ptr<RestRouteManager> routeManager;

    std::shared_ptr<PluginCollection> plugins;
    std::shared_ptr<DatasetCollection> datasets;
    std::shared_ptr<ProcedureCollection> procedures;
    std::shared_ptr<FunctionCollection> functions;
    std::shared_ptr<CredentialRuleCollection> credentials;
    std::shared_ptr<TypeClassCollection> types;

    /** Parse and perform an SQL query. */
    std::vector<MatrixNamedRow> query(const Utf8String& query) const;

    /** Parse and perform an SQL query, returning the results
        on the given HTTP connection.
    */
    void runHttpQuery(const Utf8String& query,
                      RestConnection & connection,
                      const std::string & format,
                      bool createHeaders,
                      bool rowNames,
                      bool rowHashes,
                      bool sortColumns) const;

    /** Get a type info structure for the given type. */
    Json::Value
    getTypeInfo(const std::string & typeName);

    /** Get the documentation path for the given package.  This will look
        at the working directory of the package that loaded it.
    */
    Utf8String getPackageDocumentationPath(const Package & package) const;

    /** Get the SSD cache directory.  This can be used to cache files
        and as backing for memory-mappable datasets.
    */
    std::string getCacheDirectory() const;

    std::string httpBoundAddress;
    std::string httpBaseUrl;

    Utf8String prefixUrl(Utf8String url) const;
    std::string prefixUrl(std::string url) const;
    std::string prefixUrl(const char* url) const;
    
    InProcessRestConnection restPerform(
        const std::string & verb,
        const Utf8String & resource,
        const RestParams & params = RestParams(),
        Json::Value payload = Json::Value(),
        const RestParams & headers = RestParams()) const;
    InProcessRestConnection restGet(
        const Utf8String & resource,
        const RestParams & params = RestParams()) const;

    InProcessRestConnection restDelete(
        const Utf8String & resource,
        const RestParams & params = RestParams()) const;
    InProcessRestConnection restPut(
        const Utf8String & resource,
        const RestParams & params = RestParams(),
        const Json::Value payload = Json::Value()) const;
    InProcessRestConnection restPost(
        const Utf8String & resource,
        const RestParams & params = RestParams(),
        const Json::Value payload = Json::Value()) const;

private:
    void preInit();
    bool initRoutes();
    void initCollections(std::string credentialsPath,
                         std::string staticFilesPath,
                         std::string staticDocPath,
                         bool hideInternalEntities);
    RestRequestRouter * versionNode;
    std::string cacheDirectory_;
    std::shared_ptr<spdlog::logger> logger;
};

} // namespace MLDB

