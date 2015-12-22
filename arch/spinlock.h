/** spinlock.h                                                     -*- C++ -*-
    Jeremy Barnes, 13 December 2009.  All rights reserved.
    Implementation of a spinlock.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#pragma once

#include <sched.h>
#include <atomic>

namespace ML {


/*****************************************************************************/
/* SPINLOCK                                                                  */
/*****************************************************************************/

/** This is the simplest possible locking primitive, which spins on an
    atomic flag until it acquires it.  This version is extended to allow
    for a spinning thread to yield the CPU if it has been unsuccessful
    for a while; the number of times to spin before yielding is passed
    in to the constructor.
*/

struct Spinlock {
    Spinlock(int yieldAfter = 100)
        : value(ATOMIC_FLAG_INIT), yieldAfter(yieldAfter)
    {
    }

    void lock()
    {
        acquire();
    }

    void unlock()
    {
        release();
    }

    int acquire()
    {
        for (int tries = 0;  true;  ++tries) {
            if (!value.test_and_set(std::memory_order_acquire))
                return 0;
            if (tries == yieldAfter) {
                tries = 0;
                sched_yield();
            }
        }
    }

    int release()
    {
        value.clear(std::memory_order_release);
        return 0;
    }

    std::atomic_flag value;
    int yieldAfter;
};

} // namespace ML
