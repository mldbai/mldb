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
#include "mldb/base/scope.h"
#include "mldb/base/exc_assert.h"
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

    //cerr << "epoller " << this << " init: kqueue fd = " << epoll_fd << endl;
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
#if 0
        // If we're polling on a kqueue fd, trying to add to it with kqueue returns
        // EINVAL.  Instead, we need to use kevent for the polling.  So this block
        // is not active.
        if (usToWait != 0) {
            pollfd fd[1] = { { epoll_fd, POLLIN, 0 } };
            int res = ::poll(fd, 1, usToWait / 1000);
            if (res == -1 && errno == EBADF) {
                cerr << "got bad FD on sleep" << endl;
                return -1;
            }
            if (res == -1 && errno == EINTR)
                continue;
            //if (debug) cerr << "handleEvents: res = " << res << endl;
            if (res == 0) return 0;
        }
#endif

        // Set up timeout.  If usToWait is -1, we wait forever (null pointer passed).  If usToWait is zero,
        // we return immediately (zero timespec passed).  Otherwise we put the wait time in the timespec.
        struct timespec ts = {0, 0};
        if (usToWait > 0) {
            ts.tv_sec = usToWait / 1000000;
            ts.tv_nsec = (usToWait % 1000000) * 1000;
        }

        auto tsp = &ts;
        if (usToWait == -1)
            tsp = nullptr;
        
        int res = kevent64(epoll_fd, nullptr, 0, events, nEvents, 0 /* flags */, tsp);

        if (afterSleep)
            afterSleep();

        // sys call interrupt
        if (res == -1 && errno == EINTR) continue;
        if (res == -1 && errno == EBADF) {
            //cerr << format("got bad FD for Epoller fd %d; assuming it's shutting down", epoll_fd);
            return -1;
        }
        if (res == 0) return 0;
        
        if (res == -1) {
            //cerr << format("kevent64 in Epoller handleEvents: %s fd %d timeout %d us\n", strerror(errno), epoll_fd, usToWait);
            //cerr << "epoll_fd = " << epoll_fd << endl;
            //cerr << "timeout_ = " << timeout_ << endl;
            //cerr << "nEvents = " << nEvents << endl;
            throw Exception(errno, "kevent64 in Epoller handle events");
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
#if 0   
    for (;;) {
        // At least on OSX 11.4, when you call poll() on a kqueue fd it
        // (possibly depending upon which threads are doing what) can cause
        // kqueue64 to fail to add the file descriptor.  We use select instead.
        // This is however problematic for when we have too many FDs open; in
        // that case we may need to create a new kqueue FD just for the poll
        // operation.
        pollfd fds[1] = { { epoll_fd, POLLIN, 0 } };
        int res = ::poll(fds, 1, 0);

        //cerr << "epoller poll res on " << epoll_fd << " = " << res << endl;

        if (res == -1 && errno == EBADF)
            return false;
        if (res == -1 && errno == EINTR)
            continue;
        if (res == -1)
            throw MLDB::Exception("ppoll in Epoller::poll");

        return res > 0;
    }
#else
    if (epoll_fd < FD_SETSIZE) {
        for (;;) {
            ExcAssertLess(epoll_fd, FD_SETSIZE);
            struct fd_set toRead;
            FD_ZERO(&toRead);
            FD_SET(epoll_fd, &toRead);
            struct timeval timeout = { 0, 0 };
            int res = ::select(epoll_fd + 1, &toRead, nullptr, nullptr, &timeout);

            if (res == -1 && errno == EBADF)
                return false;
            if (res == -1 && errno == EINTR)
                continue;
            if (res == -1)
                throw MLDB::Exception("ppoll in Epoller::poll");

            return res > 0;
        }
    }
    else {
        // is there a better way?  This is pretty heavy
        int tmpfd;
        for (;;) {
            tmpfd = kqueue();
            if (tmpfd == -1) {
                if (errno == EAGAIN || errno == EINTR)
                    continue;
                throw MLDB::Exception(errno, "kqueue() for Epoller::poll()");
            }
            break;
        }
        Scope_Exit(::close(tmpfd));

        //cerr << "polling on " << tmpfd << endl;

        kevent64_s event;
        EV_SET64(&event, epoll_fd, EVFILT_READ, EV_ADD, 0 /* fflags */, 0 /* fdata */, 0 /* udata */, 0 /* ext1 */, 0 /* ext2 */);

        int res;
        for (;;) {

            res = kevent64(tmpfd, &event, 1, &event, 1, KEVENT_FLAG_IMMEDIATE, nullptr /* timeout */);
            //struct timespec timeout = { 0, 0 };
            //res = kevent64(epoll_fd, nullptr, 0, &event, 1, 0 /* flags */, &timeout);

            cerr << "done polling on " << epoll_fd << ": res = " << res << endl;

            if (res == -1) {
                if (errno == EINTR || errno == EAGAIN)
                    continue;
                throw MLDB::Exception(errno, "kevent64 for Epoller::poll()");
            }

            return res > 0;
        }
    }
#endif
}

std::string getFdType(int fd)
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
            switch (fdinfo.proc_fdtype) {
                case PROX_FDTYPE_ATALK: return "atalk";
                case PROX_FDTYPE_VNODE: return "vnode";
                case PROX_FDTYPE_SOCKET: return "socket";
                case PROX_FDTYPE_PSHM: return "shm";
                case PROX_FDTYPE_PSEM: return "sem";
                case PROX_FDTYPE_KQUEUE: return "kqueue";
                case PROX_FDTYPE_PIPE: return "pipe";
                case PROX_FDTYPE_FSEVENTS: return "fsevents";
                case PROX_FDTYPE_NETPOLICY: return "netpolicy";
                default: return "ftype(" + to_string(fdinfo.proc_fdtype) + ")";
            }
        }
    }

    throw MLDB::Exception("couldn't determine if fd type was kqueue");
};

void dumpKqueueState(int fd)
{
    struct kqueue_fdinfo info;
    memset(&info, 0, sizeof(info));
    errno = 0;
    int res = proc_pidfdinfo(getpid(), fd, PROC_PIDFDKQUEUEINFO, &info, sizeof(info));
    if (res == 0)
        throw MLDB::Exception("proc_pidfdinfo() error: " + string(strerror(errno)));
    cerr << "  *** kqueue info for fd " << fd << ":" << endl;
    cerr << "      - pfi.openFlags: " << info.pfi.fi_openflags << endl;
    cerr << "      - pfi.status:    " << info.pfi.fi_status << endl;
    cerr << "      - pfi.type:      " << info.pfi.fi_type << endl;
    cerr << "      - state:         " << info.kqueueinfo.kq_state << endl;
    cerr << "      - rfu_1:         " << info.kqueueinfo.rfu_1 << endl;
};


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

    int kevent_flags = EV_ADD;  //(restart ? EV_ENABLE : EV_ADD); // restart applies to both read and write, which are separate.  EV_ADD has the right semantics
    auto doFlag = [&] (int inFlag, int outFlag) { if (flags & inFlag) kevent_flags |= outFlag; flags &= ~inFlag; };
    doFlag(EPOLL_ONESHOT, EV_ONESHOT);

    if (flags != 0) {
        throw MLDB::Exception("Unknown flags in Epoller add");
    }

    struct kevent64_s events[2];
    int nevents = 0;

    auto doAdd = [&] (auto kevent_filter)
    {
        //cerr << format("kevent64 adding fd %d of type %s to kqueue fd %d filter %s flags %d kevent_flags: %s%s%s\n",
        //               fd, getFdType(fd).c_str(), epoll_fd,
        //               (kevent_filter == EVFILT_READ ? "EVFILT_READ": "EVFILT_WRITE"),
        //               flags,
        //               ((kevent_flags & EV_ENABLE) ? "EV_ENABLE " : ""),
        //               ((kevent_flags & EV_ADD) ? "EV_ADD " : ""),
        //               ((kevent_flags & EV_ONESHOT) ? "EV_ONESHOT ": ""));

        struct kevent64_s * event = &events[nevents++];
        EV_SET64(event, fd, kevent_filter, kevent_flags,
                0 /* fflags */, 0 /* fdata */, reinterpret_cast<uint64_t>(data), 0 /* ext1 */, 0 /* ext2 */);
    };

    if (input) doAdd(EVFILT_READ);
    if (output) doAdd(EVFILT_WRITE);

    if (!restart) {
        ++numFds_;
    }

    int res;
    do {
        //dumpKqueueState(epoll_fd);
        //if (getFdType(fd) == "kqueue") {
        //    cerr << "  CHILD KQUEUE" << endl;
        //    dumpKqueueState(fd);
        //}
        res = kevent64(epoll_fd, events, nevents, nullptr, 0, 0 /* flags */, nullptr /* timeout */);

        if (res == -1 && errno != EINTR && errno != EAGAIN) {
            --numFds_;

            throw MLDB::Exception("kevent64 add: %s (fd=%d, epollfd=%d, i=%d, o=%d, oneshot=%d, restart=%d)",
                                strerror(errno), fd, epoll_fd, input, output, ((kevent_flags & EV_ONESHOT) != 0), restart);
        }
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

#else
#error "tell us how your OS implements something like epoll"
#endif

