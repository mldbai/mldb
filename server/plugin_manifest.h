// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** plugin_manifest.h                                              -*- C++ -*-
    Jeremy Barnes, 20 November 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Manifest for an MLDB plugin.
*/

#pragma once

#include "mldb/types/value_description_fwd.h"
#include "mldb/core/plugin.h"


namespace MLDB {


/*****************************************************************************/
/* PLUGIN MANIFEST                                                           */
/*****************************************************************************/

struct PluginManifest {
    PolyConfig config;
};

DECLARE_STRUCTURE_DESCRIPTION(PluginManifest);

} // namespace MLDB

