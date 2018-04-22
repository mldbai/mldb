/* hex_dump.h                                                      -*- C++ -*-
   Jeremy Barnes, 6 October 2010
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Routine to dump memory in hex format.
*/

#pragma once
#include <stddef.h>
#include "mldb/compiler/string_view.h"

namespace MLDB {

/** Dump the given range of memory (up to a minimum of total_memory and
    max_size) as a hex/ascii dump to the screen.
*/
void hex_dump(const void * mem, size_t total_memory, size_t max_size = 1024);
void hex_dump(std::string_view mem, size_t max_size = 1024);

} // namespace MLDB
