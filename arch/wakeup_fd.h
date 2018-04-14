/* wakeup_fd.h                                                     -*- C++ -*-
   Jeremy Barnes, 23 January 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Simple class that provides an FD that we can use to wake something
   up.  A generalization of the self-pipe trick.
*/

#pragma once

#include <sys/eventfd.h>
#include <unistd.h>
#include "exception.h"

namespace MLDB {

struct WakeupFd {
    WakeupFd(int flags = 0)
    {
        fd_ = ::eventfd(0, flags);
        if (fd_ == -1)
            throw MLDB::Exception(errno, "eventfd");
    }

    WakeupFd(const WakeupFd & other) = delete;
    WakeupFd(WakeupFd && other)
        noexcept
        : fd_(other.fd_)
    {
        other.fd_ = -1;
    }

    ~WakeupFd()
    {
        if (fd_ != -1)
            ::close(fd_);
    }

    int fd() const { return fd_; }

    void signal()
    {
        //cerr << "wakeup signal" << endl;
        eventfd_t val = 1;
        int res = eventfd_write(fd_, val);
        if (res == -1)
            throw MLDB::Exception(errno, "eventfd write()");
    }

    eventfd_t read()
    {
        eventfd_t val = 0;
        int res = eventfd_read(fd_, &val); 
        if (res == -1)
            throw MLDB::Exception(errno, "eventfd read()");
        return val;
    }

    // Only works if it was constructed with EFD_NONBLOCK
    bool tryRead(eventfd_t & val)
    {
        int res = ::read(fd_, &val, 8);
        if (res == -1 && errno == EWOULDBLOCK)
            return false;
        if (res == -1)
            throw MLDB::Exception(errno, "eventfd read()");
        if (res != sizeof(eventfd_t))
            throw MLDB::Exception("eventfd read() returned wrong num bytes");
        return true;
    }

    // Only works if it was constructed with EFD_NONBLOCK
    bool tryRead()
    {
        eventfd_t val = 0;
        return tryRead(val);
    }

    WakeupFd & operator = (const WakeupFd & other) = delete;
    WakeupFd & operator = (WakeupFd && other)
        noexcept
    {
        fd_ = other.fd_;
        other.fd_ = -1;

        return *this;
    }

    int fd_;
};


} // namespace MLDB
