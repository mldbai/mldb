/* epoller.h                                                       -*- C++ -*-
   Jeremy Barnes, 26 September 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Structure do allow multiplexing of FDs based upon epoll.
*/

#pragma once

#include <functional>
#include <vector>
#include <thread>
#include <mutex>
#include "mldb/arch/wakeup_fd.h"
#include "mldb/io/async_event_source.h"

namespace MLDB {

struct EpollEvent;

// Return the FD associated with a given epoll event
int getFd(const EpollEvent & event);
bool hasInput(const EpollEvent & event);
bool hasOutput(const EpollEvent & event);
bool hasHangup(const EpollEvent & event);
std::string getMaskStr(const EpollEvent & event);
void * getPtr(const EpollEvent & event);

enum {
    EPOLL_INPUT = 1,
    EPOLL_OUTPUT = 2,
    EPOLL_ONESHOT = 4
};

/*****************************************************************************/
/* EPOLLER                                                                   */
/*****************************************************************************/

/** Basic wrapper around the epoll interface to turn it into an async event
    source.
*/

struct Epoller: public AsyncEventSource {

    Epoller();

    ~Epoller();

    void init(int maxFds, int timeout = 0, bool closeOnExec = false);

    void close();

    /** Set the timeout value used when calling epoll_wait. */
    void setPollTimeout(int newTimeout)
    {
        timeout_ = newTimeout;
    }
    
    /** Add the given fd to multiplex fd.  It will repeatedly wake up the
        loop without being restarted.
    */
    void addFd(int fd, int flags, void * data = nullptr)
    {
        performAddFd(fd, data, flags, false /* restart */);
    }
    
    /** Add the given fd to multiplex fd.  It will repeatedly wake up the
        loop without being restarted.
    */
    void modifyFd(int fd, int flags, void * data = nullptr)
    {
        performAddFd(fd, data, flags, true /* restart */);
    }

    /** Add the given fd to wake up one a one-shot basis.  It will need to
        be restarted once the event is handled.
    */
    void addFdOneShot(int fd, int flags, void * data = 0)
    {
        performAddFd(fd, data, flags | EPOLL_ONESHOT, false /* restart */);
    }

    /** Restart a woken up one-shot fd. */
    void restartFdOneShot(int fd, int flags, void * data = 0)
    {
        performAddFd(fd, data, flags | EPOLL_ONESHOT, true /* restart */);
    }

    /** Remove the given fd from the multiplexer set. */
    void removeFd(int fd);
    
    enum HandleEventResult {
        DONE,
        SHUTDOWN
    };

    typedef std::function<HandleEventResult (EpollEvent & event)> HandleEvent;
    typedef std::function<void ()> OnEvent;

    /** Default event handler function to use. */
    HandleEvent handleEvent;

    OnEvent beforeSleep;
    OnEvent afterSleep;

    /** Wait up to the given number of microseconds and handle up to
        the given number of events.
        
        The handleEvent function should return true if the loop should exit
        immediately, or false if there are other events to continue.
        
        Returns the number of events handled or -1 if a handler forced the
        event handler to exit.

    */

    int handleEvents(int usToWait = 0, int nEvents = -1,
                     const HandleEvent & handleEvent = HandleEvent(),
                     const OnEvent & beforeSleep = OnEvent(),
                     const OnEvent & afterSleep = OnEvent());

    virtual int selectFd() const
    {
        return epoll_fd;
    }

    /** Poll for events.
     * Note that:
     * 1.  This function may return false positives, but will never return false negatives.
     * 2.  This function cannot be called from within an event handler.
     */
    virtual bool poll() const;

    virtual bool processOne();

private:
    /* Perform the fd addition and modification */
    void performAddFd(int fd, void * data, int flags, bool restart);

    /* Fd for the epoll mechanism. */
    int epoll_fd;

    /* Timeout value to use for epoll_wait */
    int timeout_;

    /* Number of registered file descriptors */
    size_t numFds_;

    WakeupFD shutdown_;

    std::mutex pollThreadsMutex;

    std::vector<std::thread> pollThreads;
};

} // namespace MLDB
