/* exception_hook.cc
   Jeremy Barnes, 7 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Object file to install the exception tracer.
*/

#pragma once

namespace MLDB {

/** Hook for the function to call when we throw an exception.  The first
    argument is a pointer to the object thrown; the second is the type
    info block for the thrown object.

    Starts off at null which means that no hook is installed.
*/
extern bool (*exception_tracer) (void *, const std::type_info *);

} // namespace MLDB
