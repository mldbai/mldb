/** tensorflow_plugin.h                                                   -*- C++ -*-
    Jeremy Barnes, 24 November 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    "Tensorflow" plugins for MLDB.
*/

#pragma once

#include "mldb/server/mldb_entity.h"


namespace MLDB {

/** Package that we register the tensorflow types under. */
const Package & tensorflowPackage();

} // namespace MLDB




