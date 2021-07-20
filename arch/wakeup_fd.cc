#include "wakeup_fd.h"
#include "exception.h"
#include "mldb/base/scope.h"


#if defined(__linux__)

#include <sys/eventfd.h>
#include <unistd.h>

namespace MLDB {

struct WakeupFD::Itl {
    Itl(WakeupFDOptions options)
    {
        // Arrange for them to be closed in the case of an exception.
	auto guard = ScopeExit([&] () noexcept { ::close(fd_); });

        int flags = 0;
        if (options & WFD_NONBLOCK) {
            flags = flags | EFD_NONBLOCK;
        }
        if (options & WFD_CLOEXEC) {
            flags = flags | EFD_CLOEXEC;
        }

        fd_ = ::eventfd(0, flags);
        if (fd_ == -1)
            throw MLDB::Exception(errno, "eventfd");
	
        guard.clear();
    }

    ~Itl()
    {
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

    bool trySignal()
    {
        eventfd_t val = 1;
        int res = eventfd_write(fd_, val);
	if (res == -1 && errno == EAGAIN)
	    return false;
        if (res == -1)
            throw MLDB::Exception(errno, "eventfd write()");
	return true;
    }

    uint64_t read() const
    {
        eventfd_t val = 0;
        int res = eventfd_read(fd_, &val); 
        if (res == -1)
            throw MLDB::Exception(errno, "eventfd read()");
        return val;
    }

    bool tryRead(uint64_t & val)
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

    bool tryRead()
    {
        eventfd_t val = 0;
        return tryRead(val);
    }

    int fd_ = -1;
};

} // namespace MLDB
    
#elif defined(__APPLE__)

#include <array>
#include <fcntl.h>
#include <poll.h>

namespace MLDB {

struct WakeupFD::Itl {
    Itl(WakeupFDOptions options)
    {
        // Arrange for them to be closed in the case of an exception.
        auto guard = ScopeExit([&] () noexcept {
            if (fds_[0] != -1)
                ::close(fds_[0]);
            if (fds_[1] != -1)
                ::close(fds_[1]);
            });

        int res = pipe(fds_.data());
        if (res == -1)
            throw MLDB::Exception(errno, "pipe() for wakeupfd");

        int flags = 0;
        if (options & WFD_NONBLOCK) {
            flags = flags | O_NONBLOCK;
        }
        if (options & WFD_CLOEXEC) {
            flags = flags | O_CLOEXEC;
        }

        res = fcntl(fds_[0], F_SETFL, flags);
        if (res == -1)
            throw MLDB::Exception(errno, "fcntl() for wakeupfd read end");
        res = fcntl(fds_[1], F_SETFL, flags);
        if (res == -1)
            throw MLDB::Exception(errno, "fcntl() for wakeupfd write end");

        guard.clear();

    #if 0
        res = ioctl(childLaunchStatusFd[0], FIOCLEX, 0);
        if (res == -1)
            throw MLDB::Exception(errno, "status pipe() O_CLOEXEC 0");
        res = ioctl(childLaunchStatusFd[1], FIOCLEX, 0);
        if (res == -1)
            throw MLDB::Exception(errno, "status pipe() O_CLOEXEC 1");
    #endif
    }

    ~Itl()
    {
        ::close(fds_[0]);
        ::close(fds_[1]);
    }

    int fd() const { return fds_[0]; }

    void signal()
    {
        char buf[1] = { 0 };
        int res = ::write(fds_[1], buf, 1);
        if (res == -1)
            throw MLDB::Exception(errno, "wakeupfd signal write");

    }

    bool trySignal()
    {
        struct pollfd pollers[1];
        pollers[0].fd = fds_[1];
        pollers[0].events = POLL_OUT;
        int res = ::poll(pollers, 1, 0 /* don't wait */);
        if (res == 0)
            return false;
            
        char buf[1] = { 0 };
        res = ::write(fds_[1], buf, 1);
        if (res == -1 && errno == EWOULDBLOCK)
            return false;
        if (res == -1)
            throw MLDB::Exception(errno, "wakeupfd signal write");
        return true;
    }

    uint64_t read() const
    {
        uint64_t result = 0;

        for (;;) {
            char buf[1024];
            int res = ::read(fds_[0], buf, sizeof(buf));

            if (res == -1) {
                if (errno == EINTR)
                    continue;
                if (errno == EAGAIN)
                    break;
                throw MLDB::Exception(errno, "wakeupfd signal read");
            }

            result += res;
            if (res != sizeof(buf))
                break;
        } 

        return result;
    }

    bool tryRead(uint64_t & val)
    {
        val = 0;

        for (;;) {
            char buf[1024];
            int res = ::read(fds_[0], buf, sizeof(buf));

            if (res == -1) {
                if (errno == EINTR)
                    continue;
                if (errno == EAGAIN)
                    break;
                throw MLDB::Exception(errno, "wakeupfd signal read");
            }

            val += res;
            if (res != sizeof(buf))
                break;
        } 

        return val > 0;
    }

    bool tryRead()
    {
        uint64_t val;
        return this->tryRead(val);
    }

    std::array<int, 2> fds_ = { -1, -1 };
};

} // namespace MLDB

#else
#  error "Tell us how to do a wakeup FD on your OS"
#endif

namespace MLDB {

WakeupFD::WakeupFD(WakeupFDOptions options)
    : itl_(new WakeupFD::Itl(options))
{
}

WakeupFD::~WakeupFD()
{    
}

int WakeupFD::fd() const
{
    return itl_->fd();
}

void WakeupFD::signal()
{
    itl_->signal();
}

bool WakeupFD::trySignal()
{
    return itl_->trySignal();
}

uint64_t WakeupFD::read()
{
    return itl_->read();
}

bool
WakeupFD::tryRead(uint64_t & val)
{
    return itl_->tryRead(val);
}

bool WakeupFD::tryRead()
{
    return itl_->tryRead();
}

} // namespace MLDB

