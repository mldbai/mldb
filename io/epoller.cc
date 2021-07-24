/* epoller.cc
   Jeremy Barnes, 26 September 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#include "mldb/io/epoller.h"

#ifdef __linux__
#include <sys/epoll.h>
#include <poll.h>
#include "mldb/arch/exception.h"
#include "mldb/arch/backtrace.h"
#include <string.h>
#include <iostream>
#include <unistd.h>
#include "mldb/types/date.h"

using namespace std;

namespace MLDB {

// Maximum number of events that we can handle
static constexpr int MaxEvents = 1024;

struct EpollEvent: public epoll_event {
};

int getFd(const EpollEvent & event)
{
    return event.data.fd;
}

bool hasInput(const EpollEvent & event)
{
    return event.events & EPOLLIN;
}

bool hasOutput(const EpollEvent & event)
{
    return event.events & EPOLLOUT;
}

bool hasHangup(const EpollEvent & event)
{
    return event.events & EPOLLHUP;
}

std::string getMaskStr(const EpollEvent & event)
{
    std::string result;
    if (hasInput(event)) result += "I";
    if (hasOutput(event)) result += "O";
    if (hasHangup(event)) result += "H";
    return result;
}

void * getPtr(const EpollEvent & event)
{
    return event.data.ptr;
}


/*****************************************************************************/
/* EPOLLER                                                                   */
/*****************************************************************************/

Epoller::
Epoller()
    : epoll_fd(-1), timeout_(0), numFds_(0)
{
}

Epoller::
~Epoller()
{
    close();
}

void
Epoller::
init(int maxFds, int timeout, bool closeOnExec)
{
    //cerr << "initializing epoller at " << this << endl;
    //backtrace();
    close();

    epoll_fd = epoll_create1(closeOnExec ? EPOLL_CLOEXEC : 0);
    if (epoll_fd == -1)
        throw MLDB::Exception(errno, "EndpointBase epoll_create()");

    timeout_ = timeout;
}

void
Epoller::
close()
{
    if (epoll_fd < 0)
        return;
    //cerr << "closing epoller at " << this << endl;
    //backtrace();
    ::close(epoll_fd);
    epoll_fd = -2;
}

void
Epoller::
removeFd(int fd)
{
    //cerr << Date::now().print(4) << "removed " << fd << endl;

    int res = epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, 0);
    
    if (res == -1) {
        if (errno != EBADF)
            throw MLDB::Exception("epoll_ctl DEL fd %d: %s", fd,
                                strerror(errno));
    }

    if (numFds_ > 0) {
        numFds_--;
    }
    else {
        throw MLDB::Exception("too many file descriptors removed");
    }
}

int
Epoller::
handleEvents(int usToWait, int nEvents,
             const HandleEvent & handleEvent_,
             const OnEvent & beforeSleep_,
             const OnEvent & afterSleep_)
{
    if (nEvents == -1) {
        nEvents = std::max<int>(numFds_, 1);
    }

    const HandleEvent & handleEvent
        = handleEvent_ ? handleEvent_ : this->handleEvent;
    const OnEvent & beforeSleep
        = beforeSleep_ ? beforeSleep_ : this->beforeSleep;
    const OnEvent & afterSleep
        = afterSleep_ ? afterSleep_ : this->afterSleep;

    if (nEvents <= 0)
        throw MLDB::Exception("can't wait for no events");

    if (nEvents > MaxEvents)
        nEvents = MaxEvents;

    for (;;) {
        EpollEvent events[nEvents];
                
        if (beforeSleep)
            beforeSleep();

        // Do the sleep with nanosecond resolution
        // Let's hope it doesn't busy-wait
        if (usToWait != 0) {
            pollfd fd[1] = { { epoll_fd, POLLIN, 0 } };
            timespec timeout = { 0, usToWait * 1000 };
            int res = ppoll(fd, 1, &timeout, 0);
            if (res == -1 && errno == EBADF) {
                cerr << "got bad FD on sleep" << endl;
                return -1;
            }
            if (res == -1 && errno == EINTR)
                continue;
            //if (debug) cerr << "handleEvents: res = " << res << endl;
            if (res == 0) return 0;
        }

        int res = epoll_wait(epoll_fd, events, nEvents, timeout_);

        if (afterSleep)
            afterSleep();

        // sys call interrupt
        if (res == -1 && errno == EINTR) continue;
        if (res == -1 && errno == EBADF) {
            cerr << "got bad FD" << endl;
            return -1;
        }
        if (res == 0) return 0;
        
        if (res == -1) {
            //cerr << "epoll_fd = " << epoll_fd << endl;
            //cerr << "timeout_ = " << timeout_ << endl;
            //cerr << "nEvents = " << nEvents << endl;
            throw Exception(errno, "epoll_wait");
        }
        nEvents = res;
        
        for (unsigned i = 0;  i < nEvents;  ++i) {
            if (handleEvent(events[i]) == SHUTDOWN) return -1;
        }
                
        return nEvents;
    }
}

bool
Epoller::
poll() const
{
    for (;;) {
        pollfd fds[1] = { { epoll_fd, POLLIN, 0 } };
        int res = ::poll(fds, 1, 0);

        //cerr << "poll res = " << res << endl;

        if (res == -1 && errno == EBADF)
            return false;
        if (res == -1 && errno == EINTR)
            continue;
        if (res == -1)
            throw MLDB::Exception("ppoll in Epoller::poll");

        return res > 0;
    }
}

void
Epoller::
performAddFd(int fd, void * data, int flags, bool restart)
{
    // cerr << (Date::now().print(4)
    //          + " performAddFd: epoll_fd=" + to_string(epoll_fd)
    //          + " fd=" + to_string(fd)
    //          + " one-shot=" + to_string(oneshot)
    //          + " restart=" + to_string(restart)
    //          + "\n");

    bool input = flags & EPOLL_INPUT;  flags &= ~EPOLL_INPUT;
    bool output = flags & EPOLL_OUTPUT;  flags &= ~EPOLL_OUTPUT;

    struct epoll_event event;
    ::memset(&event, 0, sizeof(event));
    event.events = (input ? EPOLLIN : 0) | (output ? EPOLLOUT : 0);
    if (data == nullptr)
	event.data.fd = fd;
    else
	event.data.ptr = data;

    auto doFlag = [&] (int inFlag, int outFlag) { if (flags & inFlag) event.events |= outFlag; flags &= ~inFlag; };
    doFlag(EPOLL_ONESHOT, EPOLLONESHOT);

    if (flags != 0) {
        throw MLDB::Exception("Unknown flags in Epoller add");
    }

    // cerr << (Date::now().print(4)
    //          + " performAddFd: epoll_fd=" + to_string(epoll_fd)
    //          + " fd=" + to_string(fd)
    //          + " one-shot=" + to_string((event.events & EPOLLONESHOT) != 0)
    //          + " input=" + to_string(input)
    //          + " output=" + to_string(output)
    //          + " restart=" + to_string(restart)
    //          + "\n");

    int action = restart ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;

    if (!restart) {
        numFds_++;
    }

    int res;
    do {
        res = epoll_ctl(epoll_fd, action, fd, &event);

        if (res == -1 && errno != EINTR && errno != EAGAIN)
            throw MLDB::Exception("epoll_ctl: %s (fd=%d, epollfd=%d,"
                                " restart=%d)",
                                strerror(errno), fd, epoll_fd, restart);
    }
    while (res == -1);
}

bool
Epoller::
processOne()
{
    int res = handleEvents();
    //cerr << "processOne res = " << res << endl;
    if (res == -1) return false;  // wakeup for shutdown
    return poll();
}

} // namespace MLDB

#elif __APPLE__

#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#include <sys/ioctl.h>
#include <poll.h>
#include "mldb/arch/exception.h"
#include "mldb/arch/backtrace.h"
#include "mldb/arch/wakeup_fd.h"
#include <string.h>
#include <iostream>
#include <unistd.h>
#include "mldb/types/date.h"
#include <libproc.h>
#include <sys/proc_info.h>

using namespace std;

namespace MLDB {

// Maximum number of events that we can handle
static constexpr int MaxEvents = 1024;


/*****************************************************************************/
/* EPOLLER                                                                   */
/*****************************************************************************/

struct EpollEvent: public kevent64_s {
};

int getFd(const EpollEvent & event)
{
    return event.ident;
}

bool hasInput(const EpollEvent & event)
{
    return event.filter == EVFILT_READ;
}

bool hasOutput(const EpollEvent & event)
{
    return event.filter == EVFILT_WRITE;
}

bool hasHangup(const EpollEvent & event)
{
    return event.flags & EV_EOF;
}

std::string getMaskStr(const EpollEvent & event)
{
    std::string result;
    if (hasInput(event)) result += "I";
    if (hasOutput(event)) result += "O";
    if (hasHangup(event)) result += "H";
    return result;
}

void * getPtr(const EpollEvent & event)
{
    return (void *)event.udata;
}

Epoller::
Epoller()
    : epoll_fd(-1), timeout_(0), numFds_(0)
{
}

Epoller::
~Epoller()
{
    close();
}

void
Epoller::
init(int maxFds, int timeout, bool closeOnExec)
{
    //cerr << "epoller " << this << " init" << endl;
    close();

    epoll_fd = kqueue();
    if (epoll_fd == -1)
        throw MLDB::Exception(errno, "Epoller epoll_create()");

    int res = ioctl(epoll_fd, closeOnExec ? FIOCLEX : FIONCLEX, nullptr);
    if (res == -1)
        throw MLDB::Exception(errno, "epoll fd ioctl FIOCLEX");

    timeout_ = timeout;

    cerr << "epoller " << this << " init: kqueue fd = " << epoll_fd << endl;
}

void
Epoller::
close()
{
    if (epoll_fd < 0)
        return;
    //cerr << "closing epoller at " << this << endl;
    //backtrace();
    ::close(epoll_fd);
    epoll_fd = -2;

    std::unique_lock<std::mutex> guard(pollThreadsMutex);

    for (auto & t: pollThreads) {
        shutdown_.trySignal();
        t.join();
    }

    pollThreads.clear();
}

void
Epoller::
removeFd(int fd)
{
    //cerr << Date::now().print(4) << "removed " << fd << endl;

    //cerr << "kevent64 removing fd " << fd << " from kqueue fd " << epoll_fd << endl;

    struct kevent64_s events[2];
    EV_SET64(&events[0], fd, EVFILT_READ, EV_DELETE,
             0 /* fflags */, 0 /* fdata */, 0 /* data */, 0 /* ext1 */, 0 /* ext2 */);
    EV_SET64(&events[1], fd, EVFILT_WRITE, EV_DELETE,
             0 /* fflags */, 0 /* fdata */, 0 /* data */, 0 /* ext1 */, 0 /* ext2 */);

    int res;
    int failures = 0;

    for (int i = 0;  i < 2;  ++i) {
        do {
            res = kevent64(epoll_fd, &events[i], 1, nullptr, 0, 0 /* flags */, nullptr);

            if (res == -1 && errno != EINTR && errno != EAGAIN) {
                ++failures;
                break;
            }
        }
        while (res == -1);
    }

    if (failures == 2) {
        throw MLDB::Exception("kevent64 remove: %s (fd=%d, epollfd=%d)",
                        strerror(errno), fd, epoll_fd);
    }

    if (numFds_ > 0) {
        numFds_--;
    }
    else {
        throw MLDB::Exception("too many file descriptors removed");
    }
}

int
Epoller::
handleEvents(int usToWait, int nEvents,
             const HandleEvent & handleEvent_,
             const OnEvent & beforeSleep_,
             const OnEvent & afterSleep_)
{
    if (nEvents == -1) {
        nEvents = std::max<int>(numFds_, 1);
    }

    const HandleEvent & handleEvent
        = handleEvent_ ? handleEvent_ : this->handleEvent;
    const OnEvent & beforeSleep
        = beforeSleep_ ? beforeSleep_ : this->beforeSleep;
    const OnEvent & afterSleep
        = afterSleep_ ? afterSleep_ : this->afterSleep;

    if (nEvents <= 0)
        throw MLDB::Exception("can't wait for no events");

    if (nEvents > MaxEvents)
        nEvents = MaxEvents;

    for (;;) {
        EpollEvent events[nEvents];
                
        if (beforeSleep)
            beforeSleep();

        // Do the sleep with nanosecond resolution
        // Let's hope it doesn't busy-wait
        if (usToWait != 0) {
            pollfd fd[1] = { { epoll_fd, POLLIN, 0 } };
#if defined(__linux__)
            timespec timeout = { 0, usToWait * 1000 };
            int res = ppoll(fd, 1, &timeout, 0);
#else
            int res = ::poll(fd, 1, usToWait * 1000);
#endif
            if (res == -1 && errno == EBADF) {
                cerr << "got bad FD on sleep" << endl;
                return -1;
            }
            if (res == -1 && errno == EINTR)
                continue;
            //if (debug) cerr << "handleEvents: res = " << res << endl;
            if (res == 0) return 0;
        }

        struct timespec ts = {0, 0};
        int res = kevent64(epoll_fd, nullptr, 0, events, nEvents, 0, &ts);

        if (afterSleep)
            afterSleep();

        // sys call interrupt
        if (res == -1 && errno == EINTR) continue;
        if (res == -1 && errno == EBADF) {
            cerr << "got bad FD" << endl;
            return -1;
        }
        if (res == 0) return 0;
        
        if (res == -1) {
            //cerr << "epoll_fd = " << epoll_fd << endl;
            //cerr << "timeout_ = " << timeout_ << endl;
            //cerr << "nEvents = " << nEvents << endl;
            throw Exception(errno, "epoll_wait");
        }
        nEvents = res;

        // Handle errors first
        for (unsigned i = 0;  i < nEvents;  ++i) {
            if (events[i].flags & EV_ERROR) {
                throw Exception(events[i].data, "kqueue wait");
            };
        }

        for (unsigned i = 0;  i < nEvents;  ++i) {
            if (events[i].flags & EV_ERROR) continue;  // handled below
            if (handleEvent(events[i]) == SHUTDOWN) return -1;
        }
                
        return nEvents;
    }
}

bool
Epoller::
poll() const
{
    for (;;) {
        pollfd fds[1] = { { epoll_fd, POLLIN, 0 } };
        int res = ::poll(fds, 1, 0);

        //cerr << "poll res = " << res << endl;

        if (res == -1 && errno == EBADF)
            return false;
        if (res == -1 && errno == EINTR)
            continue;
        if (res == -1)
            throw MLDB::Exception("ppoll in Epoller::poll");

        return res > 0;
    }
}

void
Epoller::
performAddFd(int fd, void * data, int flags, bool restart)
{
    // cerr << (Date::now().print(4)
    //          + " performAddFd: epoll_fd=" + to_string(epoll_fd)
    //          + " fd=" + to_string(fd)
    //          + " one-shot=" + to_string(oneshot)
    //          + " restart=" + to_string(restart)
    //          + "\n");

    bool input = flags & EPOLL_INPUT;  flags &= ~EPOLL_INPUT;
    bool output = flags & EPOLL_OUTPUT;  flags &= ~EPOLL_OUTPUT;

    int kevent_flags = (restart ? EV_ENABLE : EV_ADD);
    auto doFlag = [&] (int inFlag, int outFlag) { if (flags & inFlag) kevent_flags |= outFlag; flags &= ~inFlag; };
    doFlag(EPOLL_ONESHOT, EV_ONESHOT);

    if (flags != 0) {
        throw MLDB::Exception("Unknown flags in Epoller add");
    }

    auto isKqueueFd = [&] ()
    {
        std::vector<proc_fdinfo> infos;
        int bufferSize = proc_pidinfo(getpid(), PROC_PIDLISTFDS, 0, 0, 0);

        if (bufferSize == -1) {
            throw MLDB::Exception(errno, "proc_pidinfo");
        }
        infos.resize(bufferSize / sizeof(proc_fdinfo));
        int res = proc_pidinfo(getpid(), PROC_PIDLISTFDS, 0, infos.data(), bufferSize);
        if (res == -1) {
            throw MLDB::Exception(errno, "proc_pidinfo 2");
        }

        for (auto & fdinfo: infos) {
            //cerr << "fd " << fdinfo.proc_fd << " is of type " << fdinfo.proc_fdtype << endl;
            if (fdinfo.proc_fd == fd) {
                return fdinfo.proc_fdtype == PROX_FDTYPE_KQUEUE;
            }
        }

        throw MLDB::Exception("couldn't determine if fd type was kqueue");
    };

    int toAddFd = fd;

    if (isKqueueFd()) {
        // We need to wrap it in a wakeupfd as we can't add a kqueue to a kqueue
        auto wu = std::make_shared<WakeupFD>();

        auto pollWakeup = [this, wu, fd] ()
        {
            for (;;) {
                struct pollfd pollers[2];
                pollers[0].fd = fd;
                pollers[1].fd = shutdown_.fd();
                pollers[0].events = pollers[1].events = POLLIN | POLLHUP;

                int res = ::poll(pollers, 2, 100 /* ms */);

                if (res == 0 || (res == -1 && (errno == EINTR || errno == EAGAIN))) {
                    continue;
                }
                
                if (pollers[0].revents != 0)
                    wu->trySignal();

                if (pollers[1].revents)
                    break;
            }
        };

        toAddFd = wu->fd();

        std::unique_lock<std::mutex> guard(pollThreadsMutex);
        pollThreads.emplace_back(std::move(pollWakeup));
    }

    auto doAdd = [&] (auto kevent_filter)
    {
        struct kevent64_s event;
        EV_SET64(&event, toAddFd, kevent_filter, kevent_flags,
                0 /* fflags */, 0 /* fdata */, reinterpret_cast<uint64_t>(data), 0, 0);

        if (!restart) {
            ++numFds_;
        }

        int res;
        do {
            //cerr << "kevent64 adding fd " << fd << " to kqueue fd " << epoll_fd << " filter " << kevent_filter << " flags " << kevent_flags << endl;
            res = kevent64(epoll_fd, &event, 1, nullptr, 0, 0 /* flags */, nullptr);
            //cerr << "kevent64 adding fd " << fd << " to kqueue fd " << epoll_fd << ": res = " << res << endl;

            if (res == -1 && errno != EINTR && errno != EAGAIN) {
                --numFds_;

                throw MLDB::Exception("kevent64 add: %s (fd=%d, epollfd=%d, i=%d, o=%d, oneshot=%d, restart=%d)",
                                    strerror(errno), fd, epoll_fd, input, output, ((kevent_flags & EV_ONESHOT) != 0), restart);
            }
        }
        while (res == -1);
    };

    if (input) doAdd(EVFILT_READ);
    if (output) doAdd(EVFILT_WRITE);
}

bool
Epoller::
processOne()
{
    int res = handleEvents();
    //cerr << "processOne res = " << res << endl;
    if (res == -1) return false;  // wakeup for shutdown
    return poll();
}

} // namespace MLDB

#else
#error "tell us how your OS implements something like epoll"
#endif

