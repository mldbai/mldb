/* futex.h                                                         -*- C++ -*-
   Jeremy Barnes, 25 January 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Basic futex function wrappers.
*/

#pragma once

#include <atomic>
#include <chrono>
#include <cmath>

namespace MLDB {

// From https://raw.githubusercontent.com/apple/swift-corelibs-libdispatch/main/src/shims/lock.h, Apache 2.0 license
// Copyright Apple 
enum WaitOnAddressLockOptions: uint32_t {
    LOCK_NONE = 0,
    LOCK_DATA_CONTENTION = 0x00010000
};

int wait_on_address(std::atomic<uint32_t> & address, uint32_t value,
                    double timeout = INFINITY,
                    WaitOnAddressLockOptions options = LOCK_NONE);
inline int wait_on_address(std::atomic<int32_t> & address, uint32_t value,
                    double timeout = INFINITY,
                    WaitOnAddressLockOptions options = LOCK_NONE)
{
    return wait_on_address(reinterpret_cast<std::atomic<uint32_t> &>(address), value, timeout, options);
}

void wake_by_address(std::atomic<uint32_t> & address);
inline void wake_by_address(std::atomic<int32_t> & address)
{
    return wake_by_address(reinterpret_cast<std::atomic<uint32_t> &>(address));
}

} // namespace MLDB