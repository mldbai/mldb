/** sensors_plugin.h                                                   -*- C++ -*-
    Jeremy Barnes, 24 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    "Sensors" plugins for MLDB.
*/

#pragma once

#include "mldb/core/mldb_entity.h"

namespace MLDB {

/** Package that we register the sensors types under. */
const Package & sensorsPackage();

} // namespace MLDB



