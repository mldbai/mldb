/* wakeup_fd.h                                                     -*- C++ -*-
   Jeremy Barnes, 23 January 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Simple class that provides an FD that we can use to wake something
   up.  A generalization of the self-pipe trick.
*/

#pragma once

#include <unistd.h>
#include <memory>
#include "exception.h"

namespace MLDB {

enum WakeupFDOptions {
    WFD_NONE = 0,
    WFD_NONBLOCK = 1,
    WFD_CLOEXEC = 2
};

struct WakeupFD {
    WakeupFD(WakeupFDOptions options = WFD_NONE);

    template<typename... Options>
    WakeupFD(Options&&... options)
        : WakeupFD(collect(std::forward<Options>(options)...))
    {
    }

    static WakeupFDOptions collect()
    {
        return WFD_NONE;
    }

    template<typename... Rest>
    static WakeupFDOptions collect(WakeupFDOptions first, Rest&&... rest)
    {
        return WakeupFDOptions(collect(std::forward<Rest>(rest)...) | first);
    }

    WakeupFD(const WakeupFD & other) = delete;
    WakeupFD(WakeupFD && other) noexcept = default;

    ~WakeupFD();

    int fd() const;

    void signal();

    bool trySignal();  // guaranteed not to block

    uint64_t read();

    // Only works if it was constructed with WFD_NONBLOCK
    bool tryRead(uint64_t & val);

    // Only works if it was constructed with WFD_NONBLOCK
    bool tryRead();

    WakeupFD & operator = (const WakeupFD & other) = delete;
    WakeupFD & operator = (WakeupFD && other) noexcept = default;

    struct Itl;
    std::unique_ptr<Itl> itl_;
};


} // namespace MLDB
