/**                                                   -*- C++ -*-
  plugin_resource.cc
  Francois Maillet, 18 fevrier 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Transparent resource getter for plugins
*/

#pragma once

#include "mldb/rest/poly_entity.h"
#include "mldb/types/any.h"
#include "mldb/core/plugin.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/types/url.h"
#include <boost/filesystem.hpp>


namespace MLDB {

/*****************************************************************************/
/* PLUGIN STRUCTURES                                                         */
/*****************************************************************************/

enum PackageElement {
    MAIN,
    ROUTES,
    STATUS
};

struct PackageElementSources {

    PackageElementSources() : writeSourceToFile(false)
    {
    }

    Utf8String main;     ///< Main (initialization) function source
    Utf8String routes;   ///< Source of function to handle a route
    Utf8String status;   ///< Source of function to return status
    
    bool writeSourceToFile;

    bool empty() const
    {
        return main.empty() && routes.empty() && status.empty();
    }

    bool operator==(const PackageElementSources & other) const
    {
        return main == other.main &&
            routes == other.routes &&
            status == other.status;
    }
    
    Utf8String getElement(PackageElement elem) const
    {
        switch(elem) {
            case MAIN:      return main;
            case ROUTES:    return routes;
            case STATUS:    return status;
        }
        throw MLDB::Exception("Unknown PackageElem");
    }
};

DECLARE_STRUCTURE_DESCRIPTION(PackageElementSources);

struct PluginResource {
    PluginResource()
    {
    }

    std::string address;
    PackageElementSources source;
    Any args;
    Any status;
};

DECLARE_STRUCTURE_DESCRIPTION(PluginResource);


struct ScriptResource {
    ScriptResource() : writeSourceToFile(false)
    {
    }

    PluginResource toPluginConfig() const
    {
        PluginResource res;
        res.address = address;
        res.source.main = source;
        res.source.writeSourceToFile = writeSourceToFile;
        res.args = args;
        res.status = status;
        return res;
    }

    std::string address;
    Utf8String source;
    bool writeSourceToFile;
    Any args;
    Any status;
};

DECLARE_STRUCTURE_DESCRIPTION(ScriptResource);


struct PluginVersion {
    std::string address;
    std::string revision;
    std::string branch;
    std::string message;
};

DECLARE_STRUCTURE_DESCRIPTION(PluginVersion);


enum ScriptLanguage {
    PYTHON,
    JAVASCRIPT
};

ScriptLanguage parseScriptLanguage(const std::string);

DECLARE_ENUM_DESCRIPTION(ScriptLanguage);


/*****************************************************************************/
/* PLUGIN RESSOURCE                                                          */
/*****************************************************************************/

struct LoadedPluginResource {
    
    enum ScriptType {
        PLUGIN,
        SCRIPT
    };

    enum Location {
        GIT,
        HTTP,
        LOCAL_FILE,
        SOURCE
    };


    // Make this object non-copyable since we're mananing the cleanup
    // of checkedout files on disk through the destructor. Use a pointer instead
    LoadedPluginResource(const LoadedPluginResource& other) = delete; // non construction-copyable
    LoadedPluginResource& operator=(const LoadedPluginResource&) = delete; // non copyable

    LoadedPluginResource(ScriptLanguage lang, ScriptType type,
                         Utf8String pluginId,
                         const PluginResource & resource);

    ~LoadedPluginResource();

    void cleanup();

    Url url;
    Any args;

    std::string getElementFilename(PackageElement elem) const;
    std::string getElementLocation(PackageElement elem) const;
    bool packageElementExists(PackageElement elem) const;
    Utf8String getScript(PackageElement elem) const;
    Utf8String getScriptUri(PackageElement elem) const;
    std::string getFilenameForErrorMessages() const;

    boost::filesystem::path getPluginDir() const;

    ScriptLanguage pluginLanguage;
    ScriptType scriptType;
    Location pluginLocation;

    PackageElementSources source;
    boost::filesystem::path plugin_working_dir;

    PluginVersion version;

    const boost::filesystem::path MLDB_ROOT;
};

} // namespace MLDB

