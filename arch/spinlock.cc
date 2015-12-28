/** spinlock.cc
    Jeremy Barnes, 23 December 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Spinlock support functions.
*/

#include "spinlock.h"
#include <thread>

namespace ML {

void
Spinlock::
yield()
{
    std::this_thread::yield();
}

} // namespace ML
