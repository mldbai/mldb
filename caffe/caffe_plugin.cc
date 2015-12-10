/** caffe_plugin.cc
    Jeremy Barnes, 24 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#include "caffe_plugin.h"
#include <iostream>

using namespace std;


namespace Datacratic {
namespace MLDB {

const Package & caffePackage()
{
    static const Package result("caffe");
    return result;
}

} // namespace MLDB
} // namespace Datacratic

// Plugin entry point.  This is called by MLDB once the plugin is loaded.
Datacratic::MLDB::Plugin *
mldbPluginEnterV100(Datacratic::MLDB::MldbServer * server)
{
    return nullptr;
}
