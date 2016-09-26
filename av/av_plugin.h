/** av_plugin.h                                                   -*- C++ -*-
    Jeremy Barnes, 24 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    "Av" plugins for MLDB.
*/

#pragma once

#include "mldb/core/mldb_entity.h"

namespace Datacratic {
namespace MLDB {

/** Package that we register the av types under. */
const Package & avPackage();

} // namespace MLDB
} // namespace Datacratic



