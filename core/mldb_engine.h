/* mldb_engine.h                                                   -*- C++ -*-
   Jeremy Barnes, 12 December 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Server class for MLDB.
*/

#pragma once

#include <functional>
#include <memory>
#include "mldb/rest/rest_request_fwd.h"

// NOTE TO MLDB DEVELOPERS: This is an API header file.  No includes
// should be added, especially value_description.h.

namespace Json {
struct Value;
} // namespace Json

namespace MLDB {

struct PolyConfig;
struct Utf8String;
struct Package;

struct Plugin;
struct Dataset;
struct Procedure;
struct Function;
struct Sensor;
struct Recorder;
struct CredentialRule;

struct MatrixNamedRow;

struct Date;
struct Utf8String;

template<typename... T> struct WatchT;

struct ProcedureRunCollection;

extern const Utf8String & EMPTY_UTF8;

/*****************************************************************************/
/* MLDB ENGINE                                                               */
/*****************************************************************************/

/** Server that runs MLDB.  This holds all of the collections of entities,
    the utility functions they require, and handles REST requests.
*/

struct MldbEngine {

    virtual ~MldbEngine();
    
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

    /** Get the SSD cache directory.  This can be used to cache files
        and as backing for memory-mappable datasets.

        If there is none, an empty directory is returned.
    */
    virtual std::string getCacheDirectory() const = 0;
    
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

    /** Return the python executable that is compatible with this
        MLDB server.
    */
    virtual std::string getPythonExecutable() const = 0;

    /** Get the documentation path for the given package.  This will look
        at the working directory of the package that loaded it.
    */
    virtual Utf8String
    getPackageDocumentationPath(const Package & package) const = 0;

    /** Get a handler for a static route that will serve up files from
        the given directory.
    */
    virtual OnProcessRestRequest
    getStaticRouteHandler(std::string dir,
                          bool hideInternalEntities = false) = 0;

    
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

    virtual std::shared_ptr<ProcedureRunCollection>
    createProcedureRunCollection(Procedure * owner) = 0;

    virtual std::shared_ptr<Sensor>
    obtainSensorSync(PolyConfig config,
                       const OnProgress & onProgress) = 0;
    
    virtual std::shared_ptr<Sensor>
    createSensorSync(PolyConfig config,
                       const OnProgress & onProgress, bool overwrite = false)
        = 0;

    virtual std::shared_ptr<Sensor>
    tryGetSensor(const Utf8String & sensorName) const = 0;
    
    virtual std::shared_ptr<Sensor>
    getSensor(const Utf8String & sensorName) const = 0;
};

} // namespace MLDB

