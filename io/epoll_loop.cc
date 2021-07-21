// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* epoll_loop.cc
   Wolfgang Sourdeau, 25 February 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   An alternative event loop to Epoller.
*/

#include <string>
#include <unistd.h>

#include "mldb/base/exc_assert.h"
#include "epoll_loop.h"

using namespace std;
using namespace MLDB;


EpollLoop::
EpollLoop(const OnException & onException)
    : AsyncEventSource(),
      onException_(onException)
{
    epoller.init(1024 /* max FDs */, 0 /* timeout */, true /* close on exec */);
}

EpollLoop:: 
~EpollLoop()
{
    closeEpollFd();
}
     
bool
EpollLoop::
processOne()
{
    loop(-1, 0);

    return false;
}

void
EpollLoop::
loop(int maxEvents, int timeout)
{
    try {
        auto handleEvent = [&] (EpollEvent & event)
        {
            try {
                auto * fn = static_cast<EpollCallback *>(getPtr(event));
                ExcAssert(fn != nullptr);
                (*fn)(event);
            } catch (const std::exception & exc) {
                handleException();
            }

            return Epoller::DONE;
        };

        epoller.handleEvents(timeout, maxEvents, handleEvent);

        map<int, OnUnregistered> delayedUnregistrations;
        {
            std::unique_lock<mutex> guard(callbackLock_);
            delayedUnregistrations = move(delayedUnregistrations_);
            delayedUnregistrations_.clear();
        }
        for (const auto & unreg: delayedUnregistrations) {
            unregisterFdCallback(unreg.first, false, unreg.second);
        }
    }
    catch (const std::exception & exc) {
        handleException();
    }
}

void
EpollLoop::
closeEpollFd()
{
    epoller.close();
}

void
EpollLoop::
performAddFd(int fd, bool readerFd, bool writerFd, bool modify, bool oneshot)
{
    EpollCallback & cb = fdCallbacks_.at(fd);

    int flags = (readerFd ? EPOLL_INPUT : 0) | (writerFd ? EPOLL_OUTPUT : 0);

    if (oneshot) {
        ExcAssert(!modify);
        epoller.addFdOneShot(fd, flags, &cb);
    }
    else if (modify) {
        epoller.modifyFd(fd, flags, &cb);
    }
    else {
        epoller.addFd(fd, flags, &cb);
    }
#if 0
    if (epollFd_ == -1)
        return;
    ExcAssert(fd > -1);

    struct epoll_event event;
    if (oneshot) {
        event.events = EPOLLONESHOT;
    }
    else {
        event.events = 0;
    }
    if (readerFd) {
        event.events |= EPOLLIN;
    }
    if (writerFd) {
        event.events |= EPOLLOUT;
    }

    EpollCallback & cb = fdCallbacks_.at(fd);
    event.data.ptr = &cb;

    int operation = modify ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;

    int res = epoll_ctl(epollFd_, operation, fd, &event);
    if (res == -1) {
        string message = (string("epoll_ctl:")
                          + " modify=" + to_string(modify)
                          + " fd=" + to_string(fd)
                          + " readerFd=" + to_string(readerFd)
                          + " writerFd=" + to_string(writerFd));
        throw MLDB::Exception(errno, message);
    }

    if (!modify) {
        numFds_++;
    }
#endif
}

void
EpollLoop::
removeFd(int fd, bool unregisterCallback)
{
    ExcAssertGreaterEqual(fd, 0);

    epoller.removeFd(fd);

    if (unregisterCallback) {
        unregisterFdCallback(fd, true);
    }

#if 0

    int res = epoll_ctl(epollFd_, EPOLL_CTL_DEL, fd, 0);
    if (res == -1) {
        throw MLDB::Exception(errno, "epoll_ctl DEL " + to_string(fd));
    }
    if (numFds_ == 0) {
        throw MLDB::Exception("inconsistent number of fds registered");
    }
    numFds_--;

#endif
}

void
EpollLoop::
registerFdCallback(int fd, const EpollCallback & cb)
{
    std::unique_lock<mutex> guard(callbackLock_);
    if (delayedUnregistrations_.count(fd) == 0) {
        if (fdCallbacks_.find(fd) != fdCallbacks_.end()) {
            throw MLDB::Exception("callback already registered for fd");
        }
    }
    else {
        delayedUnregistrations_.erase(fd);
    }
    fdCallbacks_[fd] = cb;
}

void
EpollLoop::
unregisterFdCallback(int fd, bool delayed,
                     const OnUnregistered & onUnregistered)
{
    std::unique_lock<mutex> guard(callbackLock_);
    if (fdCallbacks_.find(fd) == fdCallbacks_.end()) {
        throw MLDB::Exception("callback not registered for fd");
    }
    if (delayed) {
        ExcAssert(delayedUnregistrations_.count(fd) == 0);
        delayedUnregistrations_[fd] = onUnregistered;
    }
    else {
        delayedUnregistrations_.erase(fd);
        fdCallbacks_.erase(fd);
        if (onUnregistered) {
            onUnregistered();
        }
    }
}

void
EpollLoop::
handleException()
{
    onException(current_exception());
}

void
EpollLoop::
onException(const exception_ptr & excPtr)
{
    if (onException_) {
        onException_(excPtr);
    }
    else {
        rethrow_exception(excPtr);
    }
}
