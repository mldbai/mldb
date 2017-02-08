/** spinlock.cc
    Jeremy Barnes, 23 December 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Spinlock support functions.
*/

#include "spinlock.h"
#include <thread>

namespace MLDB {

void
Spinlock::
yield()
{
    // This is here so we don't need to #include <thread> in the .h file
    std::this_thread::yield();
}

} // namespace MLDB
