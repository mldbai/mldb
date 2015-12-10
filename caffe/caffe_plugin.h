/** caffe_plugin.h                                                   -*- C++ -*-
    Jeremy Barnes, 24 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    "Caffe" plugins for MLDB.
*/

#pragma once

#include "mldb/core/mldb_entity.h"
#include "mldb/core/plugin.h"

namespace Datacratic {
namespace MLDB {

/** Package that we register the caffe types under. */
const Package & caffePackage();

} // namespace MLDB
} // namespace Datacratic
