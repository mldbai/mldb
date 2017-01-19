// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** docker_plugin.h                                                -*- C++ -*-
    Jeremy Barnes, 9 September 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Docker support for loading plugins.
*/

#pragma once

#include "mldb/server/external_plugin.h"


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

    DockerPluginStartup(MldbServer * server,
                        PolyConfig pconfig,
                        std::function<bool (const Json::Value & progress)> onProgress);

    DockerPluginStartupConfig config;
    MldbServer * server;

    virtual std::shared_ptr<ExternalPluginCommunication> start();
};

} // namespace MLDB



