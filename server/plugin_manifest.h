// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** plugin_manifest.h                                              -*- C++ -*-
    Jeremy Barnes, 20 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Manifest for an MLDB plugin.
*/

#pragma once

#include "mldb/types/value_description_fwd.h"
#include "plugin.h"

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* PLUGIN MANIFEST                                                           */
/*****************************************************************************/

struct PluginManifest {
    PolyConfig config;
};

DECLARE_STRUCTURE_DESCRIPTION(PluginManifest);

} // namespace MLDB
} // namespace Datacratic
