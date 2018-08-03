/* mldb_engine.h                                                   -*- C++ -*-
   Jeremy Barnes, 12 December 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Server class for MLDB.
*/

#pragma once

#include <functional>
#include <memory>

// NOTE TO MLDB DEVELOPERS: This is an API header file.  No includes
// should be added, especially value_description.h.

namespace Json {
struct Value;
} // namespace Json

namespace MLDB {

struct PolyConfig;
struct Utf8String;
struct Package;

struct RestConnection;
struct RestRequest;
struct RestEntity;

struct Plugin;
struct Dataset;
struct Procedure;
struct Function;
struct CredentialRule;

struct MatrixNamedRow;

struct RestDirectory;

struct Date;
struct Utf8String;

template<typename... T> struct WatchT;

extern const Utf8String & EMPTY_UTF8;

/*****************************************************************************/
/* MLDB ENGINE                                                               */
/*****************************************************************************/

/** Server that runs MLDB.  This holds all of the collections of entities,
    the utility functions they require, and handles REST requests.
*/

struct MldbEngine {

    virtual ~MldbEngine() = default;
    
    typedef std::function<bool (const Json::Value & progress)> OnProgress;

    /*************************************************************************/
    /* TIMERS                                                                */
    /*************************************************************************/

    /** Return a timer, possibly periodic, which will trigger the given
        watch periodically.  If toBind is specified, then that function
        is automatically bound to the timer./
    */
    virtual WatchT<Date> getTimer(Date nextExpiry, double period = -0.0,
                                  std::function<void (Date)> toBind = nullptr) = 0;

    /** Return the RestDirectory at the root of the server.  Eventually,
        we will make this interface abstract away the details of the
        implementation of the directory implementation, and this method
        will disappear.
    */
    virtual RestDirectory * getDirectory() = 0;

    virtual void addEntity(Utf8String name,
                           std::shared_ptr<RestEntity> entity) = 0;
    
    virtual void handleRequest(RestConnection & connection,
                               const RestRequest & request) const = 0;

    /** Prefix the given relative path with whatever is necessary to
        enable an external entity to find it within this MLDB server.
        
        If URL is empty, then the base MLDB prefix URL will be returned.
    */
    virtual Utf8String prefixUrl(const Utf8String & url = EMPTY_UTF8)
        const = 0;

    /** Return the URL on which MLDB can be reached. */
    virtual std::string getHttpBoundAddress() const = 0;
    
    virtual std::shared_ptr<Plugin>
    obtainPluginSync(PolyConfig config,
                     const OnProgress & onProgress) = 0;

    virtual std::shared_ptr<Plugin>
    createPluginSync(PolyConfig config,
                     const OnProgress & onProgress, bool overwrite = false) = 0;

    virtual std::shared_ptr<Plugin>
    tryGetPlugin(const Utf8String & pluginName) const = 0;
    
    virtual std::shared_ptr<Plugin>
    getPlugin(const Utf8String & pluginName) const = 0;    
    
    virtual std::shared_ptr<Dataset>
    obtainDatasetSync(PolyConfig config,
                      const OnProgress & onProgress) = 0;
    
    virtual std::shared_ptr<Dataset>
    createDatasetSync(PolyConfig config,
                      const OnProgress & onProgress, bool overwrite = false)
        = 0;

    virtual std::shared_ptr<Dataset>
    tryGetDataset(const Utf8String & datasetName) const = 0;
    
    virtual std::shared_ptr<Dataset>
    getDataset(const Utf8String & datasetName) const = 0;
    
    virtual std::shared_ptr<Function>
    obtainFunctionSync(PolyConfig config,
                       const OnProgress & onProgress) = 0;
    
    virtual std::shared_ptr<Function>
    createFunctionSync(PolyConfig config,
                       const OnProgress & onProgress, bool overwrite = false)
        = 0;

    virtual std::shared_ptr<Function>
    tryGetFunction(const Utf8String & functionName) const = 0;
    
    virtual std::shared_ptr<Function>
    getFunction(const Utf8String & functionName) const = 0;
    
    virtual std::shared_ptr<Procedure>
    obtainProcedureSync(PolyConfig config,
                        const OnProgress & onProgress) = 0;
    
    virtual std::shared_ptr<Procedure>
    createProcedureSync(PolyConfig config,
                        const OnProgress & onProgress, bool overwrite = false)
        = 0;
    
    virtual std::shared_ptr<Procedure>
    tryGetProcedure(const Utf8String & procedureName) const = 0;
    
    virtual std::shared_ptr<Procedure>
    getProcedure(const Utf8String & procedureName) const = 0;

    virtual RestEntity *
    getProcedureCollection() const = 0;
    
#if 0    
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

    /** Redirect POST request as a GET with body.  
        This is for client that do not support GET with body.
    */
    void handleRedirectToGet(RestConnection & connection,
                             const RestRequest & request,
                             const std::string & uri,
                             const Json::Value & body) const;

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
#endif
};

} // namespace MLDB

