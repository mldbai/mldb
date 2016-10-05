// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** plugin_manifest.cc
    Jeremy Barnes, 22 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "plugin_manifest.h"
#include "mldb/types/basic_value_descriptions.h"


namespace MLDB {


DEFINE_STRUCTURE_DESCRIPTION(PluginManifest);

PluginManifestDescription::
PluginManifestDescription()
{
    addField("config", &PluginManifest::config,
             "Configuration of plugin loading");
}


} // namespace MLDB

