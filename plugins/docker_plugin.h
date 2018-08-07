// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** docker_plugin.h                                                -*- C++ -*-
    Jeremy Barnes, 9 September 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Docker support for loading plugins.
*/

#pragma once

#include "mldb/engine/external_plugin.h"


namespace MLDB {


/*****************************************************************************/
/* DOCKER PLUGIN STARTUP CONFIG                                              */
/*****************************************************************************/

struct DockerPluginStartupConfig {
    Utf8String repo;
    Utf8String sharedLibrary;
};

DECLARE_STRUCTURE_DESCRIPTION(DockerPluginStartupConfig);


/*****************************************************************************/
/* DOCKER PLUGIN STARTUP                                                     */
/*****************************************************************************/

struct DockerPluginStartup: public ExternalPluginStartup {

    DockerPluginStartup(MldbEngine * engine,
                        PolyConfig pconfig,
                        std::function<bool (const Json::Value & progress)> onProgress);

    DockerPluginStartupConfig config;
    MldbEngine * engine;

    virtual std::shared_ptr<ExternalPluginCommunication> start();
};

} // namespace MLDB



