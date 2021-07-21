// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#pragma once

#include <signal.h>

namespace MLDB {

struct BlockedSignals
{
    BlockedSignals(const sigset_t & newSet)
    {
        blockMask(newSet);
    }

    ~BlockedSignals()
    {
#if !defined(__APPLE__) // no sigtimedwait on OSX, TODO to see what to do instead of this....
        // Clear the pending signals before we unblock them
        // This avoids us getting spurious signals, especially SIGCHLD from
        // grandchildren, etc.
        struct timespec timeout = { 0, 0 };
        siginfo_t info;
        while (::sigtimedwait(&newSet_, &info, &timeout) != -1) ;
#endif // __APPLE__

        // Now unblock
        ::pthread_sigmask(SIG_UNBLOCK, &oldSet_, NULL);
    }

    BlockedSignals(int signum)
    {
        sigemptyset(&newSet_);
        sigaddset(&newSet_, signum);

        blockMask(newSet_);
    }

    void blockMask(const sigset_t & newSet)
    {
        newSet_ = newSet;
        ::pthread_sigmask(SIG_BLOCK, &newSet, &oldSet_);
    }

    sigset_t newSet_;
    sigset_t oldSet_;
};

}
