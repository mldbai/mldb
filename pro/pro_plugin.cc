// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** pro_plugin.cc
    Jeremy Barnes, 24 November 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "pro_plugin.h"
#include <iostream>
#include "mldb/plugins/behavior/behavior_manager.h"
#include "mldb/server/mldb_server.h"

using namespace std;



namespace MLDB {

// Static instance of a behavior manager, shared between all beh datasets
extern BehaviorManager behManager;


const Package & proPackage()
{
    static const Package result("pro");
    return result;
}

} // namespace MLDB


// Plugin entry point.  This is called by MLDB once the plugin is loaded.
// Sets up the cache directory for beh files.
MLDB::Plugin *
mldbPluginEnterV100(MLDB::MldbServer * server)
{
    string cacheDir = server->getCacheDirectory();
    if (!cacheDir.empty()) {
        MLDB::behManager.setRemoteCacheDir(cacheDir);
    }
    return nullptr;
}
