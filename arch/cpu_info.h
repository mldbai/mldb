/* cpu_info.h                                                      -*- C++ -*-
   Jeremy Barnes, 22 January 2010
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Information about CPUs.
*/

#pragma once

#include "mldb/compiler/compiler.h"

namespace MLDB {

/** Returns the number of CPU cores installed in the system. */

extern int num_cpus_result;

void init_num_cpus();

MLDB_ALWAYS_INLINE int num_cpus()
{
    if (MLDB_UNLIKELY(!num_cpus_result)) init_num_cpus();
    return num_cpus_result;
}

} // namespace MLDB
